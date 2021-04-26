/**************************************************************************************************
 * @(#)AggregationSQLGenerator.java
 *
 *
 *
 *************************************************************************************************/
package gov.epa.otaq.moves.master.framework;

import gov.epa.otaq.moves.common.ModelScale;
import gov.epa.otaq.moves.common.ModelDomain;
import gov.epa.otaq.moves.common.Model;
import gov.epa.otaq.moves.common.Models;
import gov.epa.otaq.moves.common.Logger;
import gov.epa.otaq.moves.common.LogMessageCategory;
import gov.epa.otaq.moves.common.TreeSetIgnoreCase;
import gov.epa.otaq.moves.master.runspec.OutputTimeStep;
import gov.epa.otaq.moves.master.runspec.GeographicOutputDetailLevel;
import gov.epa.otaq.moves.master.runspec.GeographicSelectionType;
import gov.epa.otaq.moves.master.runspec.GeographicSelection;
import gov.epa.otaq.moves.master.runspec.*;
import gov.epa.otaq.moves.common.SQLRunner;
import gov.epa.otaq.moves.common.MOVESDatabaseType;
import gov.epa.otaq.moves.common.TimeSpan;
import gov.epa.otaq.moves.common.OutputEmissionsBreakdownSelection;
import java.sql.*;
import java.util.*;
import gov.epa.otaq.moves.common.CompilationFlags;

/**
 * Generates SQL to perform aggregation at several stages of exeuction.  First, SQL is generated
 * for use by Workers.  This reduces the number of records that shipped between Master and
 * Worker.  Second, SQL is generated for the Master-side data.  Consider the case of a user
 * desiring National numbers.  Data is generated at the state and county level but must be
 * SUM'd to a single set of National numbers.  If MOVES were to wait until the last detail
 * had arrived from the Workers prior to doing any summing, there would be an unacceptably
 * large output table to sum over.  Instead, SQL is used to "collapse" the data in the output
 * table periodically.  Also, a final set of SQL is used to do any final aggregations.<br>
 * <br>
 * All SQL is generated, once, at the start of an execution run. The resulting SQL
 * is executed by the Workers or by the OutputProcessor after the calculated output
 * of a worker has been imported into the output database and the IntegratedPostProcessors
 * have been run.<br>
 * <br>
 * A word about aggregation concepts is essential to understanding this class.  To aggregate a table T
 * with detail columns A, B, and C and quantity column Q, one might use the following SQL:
 * <pre>
 * 	SELECT A, B, C, SUM(Q) As Q
 * 	FROM T
 * 	GROUP BY A, B, C
 * </pre>
 * If detail C is not required, then the following SQL could be used:
 * <pre>
 * 	SELECT A, B, null AS C, SUM(Q) AS Q
 * 	FROM T
 * 	GROUP BY A, B
 * </pre>
 * This form has the same column format as the original table T which is advantagous if the aggregated
 * output needs to be intermixed (even temporarily) with unaggregated output.  It also allows a single
 * database schema to be used regardless of the aggregation options selected.<br>
 * <br>
 * If neither detail B nor C is required, then the SQL becomes:
 * <pre>
 * 	SELECT A, null AS B, null AS C, SUM(Q) AS Q
 * 	FROM T
 * 	GROUP BY A
 * </pre>
 * Thus, the general rule for aggregation is to consider each detail column and contribute appropriately
 * to the SELECT'd columns clause and to the GROUP BY clause.<br>
 * <br>
 * Seldom is a mere SELECT appropriate.  In fact, the aggregated results in MOVES must end up back in the
 * original table (hence the use of "null as c" clauses) intermixed with results from other runs.
 * To accomplish this, the general structure is revised to include a new temporary table T2 and filtering
 * of T's data:
 * <pre>
 * 	CREATE TABLE T2 .....
 *
 * 	INSERT INTO T2 (<font color=Red>A, B, C, Q</font>)
 * 	SELECT <font color=Blue>A, B, null AS C, SUM(Q) AS Q</font>
 * 	FROM T
 * 	WHERE T.RunID = (desired subset of simulation runs in T)
 * 	GROUP BY <font color=Green>A, B</font>
 *
 * 	DELETE FROM T
 * 	WHERE T.RunID = (desired subset of simulation runs in T)
 *
 * 	INSERT INTO T (<font color=Red>A, B, C, Q</font>)
 * 	SELECT <font color=Red>A, B, C, Q</font>
 * 	FROM T2
 *
 * 	DROP TABLE T2
 * </pre>
 * The need for the INSERT statement to name columns adds a third clause that the detail column examination
 * must contribute to.  The 3 common clauses are highlighted above.  The generateBaseSQLForAggregation()
 * method is responsible for building these common clauses, which are then assembled into specialized
 * SQL for Worker, intermediate processing, and final processing by the generateSQLsForWorker(),
 * generateSQLsForOutputProcessor(), and generateSQLsForFinalProcessing() methods respectively.<br>
 * <br>
 * The issue of inflating the daily output up to monthly numbers must also be considered during aggregation.
 * SQL provides a convenient keyword for this: CASE.  In general, CASE is used to change the value SELECT'd
 * depending upon an expression.  For example:
 * <pre>
 * 	SELECT C, CASE
 * 			WHEN C = 2 THEN Q * 28 / 7
 * 			WHEN C = 12 THEN Q * 31 / 7
 * 			ELSE Q * 30 / 7
 * 		  END AS Q
 * 	FROM T
 * </pre>
 * The above SQL will return a modified value of Q for each row by examining C in the row and performing
 * different math for each value of C.  Aggregating with this tool changes our SQL example to:
 * <pre>
 * 	SELECT A, B, null AS C, SUM(CASE
 * 			WHEN C = 2 THEN Q * 28 / 7
 * 			WHEN C = 12 THEN Q * 31 / 7
 * 			ELSE Q * 30 / 7
 * 		  END) AS Q
 * 	FROM T
 * 	GROUP BY A, B
 * </pre>
 * Note that the "c" column used in the case statement is the "c" column from the table, not the null value
 * labeled as "c" in the SELECT'd output.<br>
 * <br>
 * This technique is used in the SQL sent to Workers to inflate data to a value consistent with the
 * number of days in each month.
 * @author		Wesley Faler
 * @version		2017-03-22
**/
public class AggregationSQLGenerator {
	/** true to display aggregation SQL statements. **/
	private static final boolean shouldDebug = false;

	/** true when Nonroad results can be aggregated, false during testing **/
	private static final boolean ENABLE_NONROAD_AGGREGATION = true;

	/**
	 * Partial SQL to create the WorkerOutputTemp table from an output table.
	**/
	private String createSQL = "create table workeroutputtemp("
			+ " movesrunid smallint unsigned not null,"
			+ " iterationid smallint unsigned default 1,"
			+ " yearid smallint unsigned null,"
			+ " monthid smallint unsigned null,"
			+ " dayid smallint unsigned null,"
			+ " hourid smallint unsigned null,"
			+ " stateid smallint unsigned null,"
			+ " countyid integer unsigned null,"
			+ " zoneid integer unsigned null,"
			+ " linkid integer unsigned null,"
			+ " pollutantid smallint unsigned null,"
			+ " roadtypeid smallint unsigned null,"
			+ " processid smallint unsigned null,"
			+ " fueltypeid smallint unsigned null,"
			+ " fuelsubtypeid smallint unsigned null,"
			+ " modelyearid smallint unsigned null,"
			+ " sourcetypeid smallint unsigned null,"
			+ " regclassid smallint unsigned null,"
			+ " scc char(10) null,"
			+ " engtechid smallint unsigned null default null,"
			+ " sectorid smallint unsigned null default null,"
			+ " hpid smallint unsigned null default null,"
			+ " emissionquant float null,"
			+ " emissionrate float null)";
	/**
	 * Partial SQL to create the WorkerOutputActivityTemp table from an output table.
	**/
	private String createActivitySQL = "create table workeractivityoutputtemp("
			+ " movesrunid smallint unsigned not null,"
			+ " iterationid smallint unsigned default 1,"
			+ " yearid smallint unsigned null,"
			+ " monthid smallint unsigned null,"
			+ " dayid smallint unsigned null,"
			+ " hourid smallint unsigned null,"
			+ " stateid smallint unsigned null,"
			+ " countyid integer unsigned null,"
			+ " zoneid integer unsigned null,"
			+ " linkid integer unsigned null,"
			+ " roadtypeid smallint unsigned null,"
			+ " fueltypeid smallint unsigned null,"
			+ " fuelsubtypeid smallint unsigned null,"
			+ " modelyearid smallint unsigned null,"
			+ " sourcetypeid smallint unsigned null,"
			+ " regclassid smallint unsigned null,"
			+ " scc char(10) null,"
			+ " engtechid smallint unsigned null default null,"
			+ " sectorid smallint unsigned null default null,"
			+ " hpid smallint unsigned null default null,"
			+ " activitytypeid smallint not null,"
			+ " activity float null"
			+ ") ";

	/**
	 * Partial SQL to create the WorkerBaseRateOutputTemp table from an output table.
	**/
	private String createBaseRateOutputSQL = "create table workerbaserateoutputtemp like baserateoutput";

	/**
	 * SQL to insert into a worker's WorkerOutputTemp table from an output table.
	**/
	private String workerInsertSQL;

	/**
	 * SQL to insert into master's WorkerOutputTemp table from an output table.
	**/
	private String masterInsertSQL;

	/**
	 * SQL to select from a master's output table. Insert into WorkerOutputTemp uses this select statement.
	**/
	private String masterSelectSQL;

	/**
	 * SQL to select from a worker's output table. Insert into WorkerOutputTemp uses this select statement.
	**/
	private String workerSelectSQL;

	/**
	 * Group by segment of the SQL for select statement.
	**/
	private String groupBy;

	/**
	 * SQL to insert into WorkerActivityOutputTemp table from an activity output table.
	**/
	private String insertActivitySQL;

	/**
	 * SQL to select from an activity output table. Insert into WorkerActivityOutputTemp uses this
	 * select statement.
	**/
	private String selectActivitySQL;

	/**
	 * Group by segment of the SQL for activity output select statement.
	**/
	private String groupByActivity;

	/** Group by segment of the SQL for population activity output **/
	private String groupByActivitySpatialOnly = "";
	/**
	 * SQL to select population from an activity output table. Insert into
	 * WorkerActivityOutputTemp uses this select statement.
	**/
	private String selectActivityNoScaleSQL = "";

	/**
	 * SQL to insert into WorkerBaseRateOutputTemp table.
	**/
	private String insertBaseRateOutputSQL;

	/**
	 * SQL to select from a BaseRateOutput table. Insert into WorkerBaseRateOutputTemp uses this
	 * select statement.
	**/
	private String selectBaseRateOutputSQL;

	/**
	 * Group by segment of the SQL for BaseRateOutput select statement.
	**/
	private String groupByBaseRateOutput;

	/** SQL executed by outputProcessor while processing worker files. **/
	public Vector<String> outputProcessorSQLs = new Vector<String>();

	/** SQL statements to be executed by the worker.  Each must end with a semicolon. **/
	public Vector<String> workerSQLs = new Vector<String>();

	/**
	 * Final aggregation SQL statements, executed by OutputProcessor while
	 * doing final processing.
	**/
	public Vector<String> finalProcessSQLs = new Vector<String>();

	/**
	 * Output fields on a master, to insert values into output table from the temporary aggregated table.
	**/
	private String masterOutputTableFields = "movesrunid,"
			+"iterationid,"
			+"yearid,"
			+"monthid,"
			+"dayid,"
			+"hourid,"
			+"stateid,"
			+"countyid,"
			+"zoneid,"
			+"linkid,"
			+"pollutantid,"
			+"roadtypeid,"
			+"processid,"
			+"sourcetypeid,"
			+"regclassid,"
			+"fueltypeid,"
			+(CompilationFlags.ALLOW_FUELSUBTYPE_OUTPUT? "fuelsubtypeid," : "")
			+"modelyearid,"
			+"scc,"
			+"engtechid,"
			+"sectorid,"
			+"hpid,"
			+"emissionquant";

	/**
	 * Output fields on worker, to insert values into output table from the temporary aggregated table.
	**/
	private String workerOutputTableFields = "movesrunid,"
			+"iterationid,"
			+"yearid,"
			+"monthid,"
			+"dayid,"
			+"hourid,"
			+"stateid,"
			+"countyid,"
			+"zoneid,"
			+"linkid,"
			+"pollutantid,"
			+"roadtypeid,"
			+"processid,"
			+"sourcetypeid,"
			+"regclassid,"
			+"fueltypeid,"
			+"fuelsubtypeid,"
			+"modelyearid,"
			+"scc,"
			+"engtechid,"
			+"sectorid,"
			+"hpid,"
			+"emissionquant,"
			+"emissionrate";

	/**
	 * Activity Output fields, to insert values into activity output table from the
	 * temporary aggregated table.
	**/
	private String outputActivityTableFields = "movesrunid,"
			+"iterationid,"
			+"yearid,"
			+"monthid,"
			+"dayid,"
			+"hourid,"
			+"stateid,"
			+"countyid,"
			+"zoneid,"
			+"linkid,"
			+"roadtypeid,"
			+"sourcetypeid,"
			+"regclassid,"
			+"fueltypeid,"
			+(CompilationFlags.ALLOW_FUELSUBTYPE_OUTPUT? "fuelsubtypeid," : "")
			+"modelyearid,"
			+"scc,"
			+"engtechid,"
			+"sectorid,"
			+"hpid,"
			+"activitytypeid,"
			+"activity";

	/**
	 * BaseRateOutput fields, to insert values into output table from the temporary aggregated table.
	**/
	private String outputBaseRateOutputTableFields = "movesrunid,"
			+"iterationid,"
			+"zoneid,"
			+"linkid,"
			+"sourcetypeid,"
			+"scc,"
			+"roadtypeid,"
			+"avgspeedbinid,"
			+"monthid,"
			+"hourdayid,"
			+"pollutantid,"
			+"processid,"
			+"modelyearid,"
			+"yearid,"
			+"fueltypeid,"
			+"regclassid,"
			+"meanbaserate,"
			+"emissionrate";

	/**
	 * Select statement for inserting values into a master Output table. Doesn't have FROM, WHERE, ORDER BY,
	 * GROUP BY. Just fields.
	**/
	private String selectSQLForMasterOutput = "select " + masterOutputTableFields;

	/**
	 * Select statement for inserting values into a worker Output table. Doesn't have FROM, WHERE, ORDER BY,
	 * GROUP BY. Just fields.
	**/
	private String selectSQLForWorkerOutput = "select " + workerOutputTableFields;

	/**
	 * Select statement for inserting values into Activity Output table. Doesn't have FROM, WHERE,
	 * ORDER BY, GROUP BY. Just fields.
	**/
	private String selectSQLForActivityOutput = "select " + outputActivityTableFields;

	/**
	 * Select statement for inserting values into BaseRateOutput table. Doesn't have FROM, WHERE,
	 * ORDER BY, GROUP BY. Just fields.
	**/
	private String selectSQLForBaseRateOutput = "select " + outputBaseRateOutputTableFields;

	/** The active run ID. This is significant in the output database. **/
	int activeRunID = 0;

	/** The active iteration ID. This is significant in the output database. **/
	int activeIterationID = 0;

	/** true after issues have already been reported **/
	boolean didReportIssues = false;

	/** true after report SQL has already been generated **/
	boolean didGenerateReportSQL = false;

	/** true when Nonroad activity outputs must be weighted before aggregation **/
	boolean nrNeedsActivityWeight = false;

	/** SQL statements that weight Nonroad activity output before aggregation **/
	ArrayList<String> nrActivityWeightSQL = new ArrayList<String>();
	
	TreeSetIgnoreCase nrActivitySummaryColumns = new TreeSetIgnoreCase();

	/**
	 * Default constructor
	**/
	public AggregationSQLGenerator() {

	}

	/**
	 * Builds the aggregation SQL using data from the ExecutionRunSpec singleton. Typically, this
	 * method is called once and the aggregated result will be stored in an output table.
	 * @return false if fails
	**/
	public boolean generateReportSQL() {
		if(MOVESEngine.theInstance != null) {
			activeRunID = MOVESEngine.theInstance.getActiveRunID();
			activeIterationID = MOVESEngine.theInstance.getActiveIterationID();
		}
		if(didGenerateReportSQL) {
			return true;
		}
		if(!validateAggregation()) {
			return false;
		}
		if(!generateSQLsForWorker()) {
			return false;
		}
		logSQL("worker",workerSQLs);

		if(!generateSQLsForOutputProcessor()) {
			return false;
		}
		logSQL("outputprocessor",outputProcessorSQLs);

		if(!generateSQLsForFinalProcessing()) {
			return false;
		}
		logSQL("final",finalProcessSQLs);

		didGenerateReportSQL = true;
		return true;
	}

	/**
	 * Display aggregation SQL statements.
	 * @param purpose use of the SQL, such as "worker" or "final".
	 * @param statements SQL statements, never null, may be empty.
	**/
	void logSQL(String purpose, Vector<String> statements) {
		if(!shouldDebug) {
			return;
		}
		Logger.log(LogMessageCategory.DEBUG,"aggregation statements for: " + purpose);
		for(String sql : statements) {
			if(sql.length() > 0) {
				Logger.log(LogMessageCategory.DEBUG,sql);
			}
		}
		Logger.log(LogMessageCategory.DEBUG,"end of aggregation statements for: " + purpose);
	}

	/**
	 * Validates the RunSpec's output parameters before aggregation.
	 * @return false if there is any problem during validation
	**/
	boolean validateAggregation() {
		try {
			// All the time periods required are present in the runspec.
			OutputTimeStep outputTimeStep =
					ExecutionRunSpec.theExecutionRunSpec.getOutputTimeStep();
			TimeSpan timeSpan = ExecutionRunSpec.theExecutionRunSpec.getTimeSpan();
			if(!ExecutionRunSpec.theExecutionRunSpec.getModels().contains(Model.NONROAD)) {
				if(outputTimeStep.requiresAllHours()) {
					if(!timeSpan.hasAllHours() && !didReportIssues) {
						/**
						 * @explain Hour is not a desired output dimension yet aggregation is
						 * called for.  When reporting results, be sure to include the fact that the
						 * totals do not include all hours.
						**/
						Logger.log(LogMessageCategory.WARNING,
								"warning: runspec doesn't have all the hours.");
					}
				}
			}
			Connection defaultDB = null;
			try {
				defaultDB = DatabaseConnectionManager.checkOutConnection(MOVESDatabaseType.DEFAULT);
				if(outputTimeStep.requiresAllDays()) {
					if(!timeSpan.hasAllDays(defaultDB) && !didReportIssues) {
						/**
						 * @explain Day is not a desired output dimension yet aggregation is
						 * called for.  When reporting results, be sure to include the fact that the
						 * totals do not include all days.
						**/
						Logger.log(LogMessageCategory.WARNING,
								"warning: runspec doesn't have all the days.");
					}
				}
			} catch(Exception e) {
				/**
				 * @explain A database error was encountered while checking for aggregation
				 * suitability.
				**/
				Logger.logError(e, "failed to validate data for aggregation");
				return false;
			} finally {
				if(defaultDB != null) {
					DatabaseConnectionManager.checkInConnection(MOVESDatabaseType.DEFAULT,
							defaultDB);
					defaultDB = null;
				}
			}
			if(outputTimeStep.requiresAllMonths()) {
				if(!timeSpan.hasAllMonths() && !didReportIssues) {
					/**
					 * @explain Month is not a desired output dimension yet aggregation is
					 * called for.  When reporting results, be sure to include the fact that the
					 * totals do not include all months.
					**/
					Logger.log(LogMessageCategory.WARNING,
							"warning: runspec doesn't have all the months.");
				}
			}

			// All the roadtypes required are present in the runspec.
			Connection executionDatabase = null;
			try {
				executionDatabase = DatabaseConnectionManager.checkOutConnection(
						MOVESDatabaseType.EXECUTION);
				// Size comparision, is to make sure runspec has all the roadtypes.
				if(!ExecutionRunSpec.theExecutionRunSpec.
						getOutputEmissionsBreakdownSelection().roadType) {
					int dbRoadTypeCount = 0;
					String sql = "select count(*) from roadtype where shoulddisplay=1 and roadtypeid not in (1,100)";
					Models.ModelCombination mc = ExecutionRunSpec.getRunSpec().getModelCombination();
					switch (mc) {
						case M1:
							sql += " and isaffectedbyonroad is true";
							break;
						case M2:
							sql += " and isaffectedbynonroad is true";
							break;
						default:
							break;
					}
					ResultSet result = SQLRunner.executeQuery(executionDatabase, sql);
					if(result.next()) {
						dbRoadTypeCount = result.getInt(1);
					}
					result.close();

					int runSpecRoadTypeCount = 0;					
					for(RoadType r : ExecutionRunSpec.theExecutionRunSpec.getRoadTypes()) {
						if(r.roadTypeID == 1 || r.roadTypeID == 100
								|| r.roadTypeID == 6 || r.roadTypeID == 7
								|| r.roadTypeID == 8 || r.roadTypeID == 9) {
							// Skip hidden roads, only counting road types
							// that matter when aggregated (non-1, non-100)
							// and that are shown to the user.
							continue;
						}
						if(ExecutionRunSpec.theExecutionRunSpec.getModels().containsAny(r.mc)) {
							runSpecRoadTypeCount++;
						}
					}
					if(runSpecRoadTypeCount != dbRoadTypeCount && !didReportIssues) {
						/**
						 * @explain RoadType is not a desired output dimension yet aggregation is
						 * called for.  When reporting results, be sure to include the fact that the
						 * totals do not include all road types.
						**/
						Logger.log(LogMessageCategory.WARNING,
								"warning: runspec doesn't have all the roadtypes.");
					}
				}
			} catch(Exception e) {
				/**
				 * @explain A database error was encountered while checking for aggregation
				 * suitability.
				**/
				Logger.logError(e, "failed to validate data for aggregation");
				return false;
			} finally {
				if(executionDatabase != null) {
					DatabaseConnectionManager.checkInConnection(
							MOVESDatabaseType.EXECUTION, executionDatabase);
					executionDatabase = null;
				}
			}

			// Can uncertainty estimation be performed.
			OutputEmissionsBreakdownSelection outputEmissionsBreakdownSelection =
				ExecutionRunSpec.theExecutionRunSpec.getOutputEmissionsBreakdownSelection();
			if(outputEmissionsBreakdownSelection.estimateUncertainty) {
				TreeSet geographicSelections =
						ExecutionRunSpec.theExecutionRunSpec.getGeographicSelections();
				GeographicSelectionType geoType = null;
				Iterator i = geographicSelections.iterator();
				if(i.hasNext()) {
					geoType = ((GeographicSelection) i.next()).type;
				}
				if(geoType != null) {
					if(geoType == GeographicSelectionType.NATION
							|| geoType == GeographicSelectionType.STATE) {
						if(!didReportIssues) {
							/**
							 * @explain Uncertainty cannot be done for NATION or STATE aggregation.
							**/
							Logger.log(LogMessageCategory.WARNING,
								"uncertainty cannot be estimated for aggregated data.");
							return false;
						}
					}
				}
			}
			return true;
		} finally {
			didReportIssues = true;
		}
	}

	/**
	 * Generates SQLs to be processed by worker, to aggregate worker output.
	 * <p>
	 * <b>NOTE: ALL SQL STATEMENTS CREATED FOR WORKERS *MUST* END WITH A SEMICOLON!!!</b>
	 * This allows parsing of the SQL file by the worker when the statements
	 * are multiple lines long.
	 * @return false if there is any problem while generating SQLs.
	**/
	boolean generateSQLsForWorker() {
		if(!ENABLE_NONROAD_AGGREGATION
				&& ExecutionRunSpec.theExecutionRunSpec.getModels().contains(Model.NONROAD)) {
			// Temporarily disable all aggregation for Nonroad runs.
			return true;
		}

		workerSQLs.add("starttimer generalaggregation;");

		// Populate the SCC fields for onroad. Do this after setting the road type
		// as SCC contains the road type.
		if(ExecutionRunSpec.theExecutionRunSpec.getOutputEmissionsBreakdownSelection().onRoadSCC
				&& !ExecutionRunSpec.theExecutionRunSpec.getModels().contains(Model.NONROAD)) {
			String[] tables = {
				"movesworkeroutput", // "movesworkeractivityoutput" Doesn't have processID, so can't make a SCC
				(CompilationFlags.DO_RATES_FIRST? "baserateoutput":"")
			};
			String sql = "";
			for(int i=0;i<tables.length;i++) {
				String tableName = tables[i];
				if(tableName.length() <= 0) {
					continue;
				}
				sql = "update " + tablename + " set scc=concat('22',lpad(fueltypeid,2,'00'),lpad(sourcetypeid,2,'00'),lpad(roadtypeid,2,'00'),lpad(processid,2,'00'));";
				workerSQLs.add(sql);
			}
			/*
			Activity table doesn't technically require a SCC but it would help future reporting.
			The process to be used is implied by the activity type:
				activityTypeID|activityType|activityTypeDesc         |Process to use
				1             |distance    |Distance traveled        |1
				2             |sourcehours |Source Hours             |1
				4             |sho         |Source Hours Operating   |1
				6             |population  |Population               |1
				5             |shp         |Source Hours Parked      |2
				7             |starts      |Starts                   |2
				3             |extidle     |Extended Idle Hours      |90
				8             |hotelling   |Hotelling Hours          |90
				9             |avghp       |Average Horsepower       |n/a
				10            |retrofrac   |Fraction Retrofitted     |n/a
				11            |retrocnt    |Number Units Retrofitted |n/a
				12            |loadfactor  |Load Factor              |n/a
				13            |hotelling   |Hotelling Diesel Aux     |90
				14            |hotelling   |Hotelling Battery or AC  |90
				15            |hotelling   |Hotelling All Engines Off|90
			*/
			sql = "update movesworkeractivityoutput set scc=concat('22',lpad(fueltypeid,2,'00'),lpad(sourcetypeid,2,'00'),lpad(roadtypeid,2,'00'),"
					+ " lpad(case"
					+ " when activitytypeid in (1,2,4,6) then 1"
					+ " when activitytypeid in (5,7) then 2"
					+ " when activitytypeid in (3,8,13,14,15) then 90"
					+ " else null end"
					+ " ,2,'00'));";
			workerSQLs.add(sql);
		}

		try {
			if(!generateBaseSQLForAggregation(true)) {
				return false;
			}
			workerSQLs.add("starttimer outputaggregation;");
			workerSQLs.add("drop table if exists workeroutputtemp;");
			workerSQLs.add(createSQL+";");
			//workerSQLs.add("flush tables;");
			workerSQLs.add(workerInsertSQL + ") " + workerselectsql + " from movesworkeroutput "
					+ groupBy + ";");
			workerSQLs.add("truncate movesworkeroutput;");
			//workerSQLs.add("flush tables;");
			String insertOutputSQL = "insert into movesworkeroutput (" + workeroutputtablefields + ") "
					+ selectSQLForWorkerOutput + " from workeroutputtemp;";
			workerSQLs.add(insertOutputSQL+";");
			//workerSQLs.add("flush tables;");
			workerSQLs.add("drop table if exists workeroutputtemp;");
			//workerSQLs.add("flush tables;");
		} catch(Exception e) {
			/**
			 * @explain A database error occurred while creating SQL statements for
			 * aggregating data.
			**/
			Logger.logError(e, "failed to generate sql for workers");
			return false;
		}

		try {
			if(nrNeedsActivityWeight) {
				workerSQLs.add("starttimer nonroadaggregation;");
				for(String s : nrActivityWeightSQL) {
					workerSQLs.add(s);
				}
			}

			workerSQLs.add("starttimer activityaggregation;");
			workerSQLs.add("drop table if exists workeractivityoutputtemp;");
			workerSQLs.add(createActivitySQL+";");
			//workerSQLs.add("flush tables;");

			if(ExecutionRunSpec.getRunSpec().outputPopulation) {
				workerSQLs.add(insertActivitySQL + ") " + selectActivitySQL
						+ " from movesworkeractivityoutput "
						+ " where activitytypeid <> 6 "
						+ groupByActivity + ";");
				workerSQLs.add(insertActivitySQL + ") " + selectActivityNoScaleSQL
						+ " from movesworkeractivityoutput "
						+ " where activitytypeid=6 "
						+ groupByActivitySpatialOnly + ";");
			} else {
				workerSQLs.add(insertActivitySQL + ") " + selectActivitySQL
						+ " from movesworkeractivityoutput "
						+ groupByActivity + ";");
			}

			workerSQLs.add("truncate movesworkeractivityoutput;");
			//workerSQLs.add("flush tables;");
			String insertActivityOutputSQL = "insert into movesworkeractivityoutput ("
					+ outputActivityTableFields + ") "
					+ selectSQLForActivityOutput + " from workeractivityoutputtemp;";
			workerSQLs.add(insertActivityOutputSQL+";");
			//workerSQLs.add("flush tables;");
			workerSQLs.add("drop table if exists workeractivityoutputtemp;");
			// Update all MOVESWorkerActivityOutput key fields to non-NULL values
			// This facilitates the overlay algorithm needed by the master.
			String sql = "update movesworkeractivityoutput set ";
			String[] affectedColumns = {
				"yearid", "monthid", "dayid", "hourid", "stateid", "countyid", "zoneid",
				"linkid", "sourcetypeid", "regclassid", "fueltypeid", "modelyearid", "roadtypeid",
				"engtechid", "sectorid", "hpid"
			};
			for(int i=0;i<affectedColumns.length;i++) {
				if(i > 0) {
					sql += ", ";
				}
				sql += affectedColumns[i] + "=coalesce(" + affectedcolumns[i] + ",0)";
			}
			sql += ", scc=coalesce(scc,'nothing');";
			workerSQLs.add(sql);
			//workerSQLs.add("flush tables;");
		} catch(Exception e) {
			/**
			 * @explain A database error occurred while creating SQL statements for
			 * aggregating data.
			**/
			Logger.logError(e, "failed to generate sql for workers activity");
			return false;
		}

		workerSQLs.add("starttimer generalaggregation;");

		// Aggregate base rates
		if(CompilationFlags.DO_RATES_FIRST) {
			try {
				workerSQLs.add("drop table if exists workerbaserateoutputtemp;");
				workerSQLs.add(createBaseRateOutputSQL+";");
				workerSQLs.add(insertBaseRateOutputSQL + ") " + selectBaseRateOutputSQL
						+ " from baserateoutput "
						+ groupByBaseRateOutput + ";");
				workerSQLs.add("truncate baserateoutput;");
				//workerSQLs.add("flush tables;");
				String insertBaseRateOutputSQL = "insert into baserateoutput ("
						+ outputBaseRateOutputTableFields + ") "
						+ selectSQLForBaseRateOutput + " from workerbaserateoutputtemp;";
				workerSQLs.add(insertBaseRateOutputSQL+";");
				//workerSQLs.add("flush tables;");
				workerSQLs.add("drop table if exists workerbaserateoutputtemp;");
				//workerSQLs.add("flush tables;");
			} catch(Exception e) {
				/**
				 * @explain A database error occurred while creating SQL statements for
				 * aggregating data.
				**/
				Logger.logError(e, "failed to generate sql for base rate output");
				return false;
			}
		}

		return true;
	}

	/**
	 * This method generates the base SQL for the aggregation and populates createSQL, 
	 * masterInsertSQL, workerInsertSQL, masterSelectSQL, and workerSelectSQL.
	 * @param isWorkerSQL true if the SQL is being created for use on the workers
	 * @return false if there is any problem creating the SQL
	**/
	boolean generateBaseSQLForAggregation(boolean isWorkerSQL) {
		boolean convertDays = false;
		boolean convertWeeks = false;
		masterInsertSQL = "insert into workeroutputtemp ( movesrunid, iterationid";
		workerInsertSQL = "insert into workeroutputtemp ( movesrunid, iterationid";
		masterSelectSQL = " select movesrunid, iterationid";
		workerSelectSQL = " select movesrunid, iterationid";
		groupBy = "group by movesrunid, iterationid";

		insertActivitySQL = "insert into workeractivityoutputtemp ( movesrunid, iterationid";
		selectActivitySQL = " select movesrunid, iterationid";
		groupByActivity = "group by movesrunid, iterationid";
		groupByActivitySpatialOnly = "group by movesrunid, iterationid";

		insertBaseRateOutputSQL = "insert into workerbaserateoutputtemp ( movesrunid, iterationid";
		selectBaseRateOutputSQL = " select movesrunid, iterationid";
		groupByBaseRateOutput = "group by movesrunid, iterationid";

		nrNeedsActivityWeight = false;
		nrActivityWeightSQL.clear();
		nrActivitySummaryColumns.clear();

		try {
			// Time period
			masterInsertSQL += ", yearid, monthid, dayid, hourid";
			workerInsertSQL += ", yearid, monthid, dayid, hourid";
			insertActivitySQL += ", yearid, monthid, dayid, hourid";
			insertBaseRateOutputSQL += ", yearid, monthid, hourdayid";
			OutputTimeStep outputTimeStep = ExecutionRunSpec.theExecutionRunSpec.getOutputTimeStep();

			groupByActivitySpatialOnly += ", yearid, monthid, dayid, hourid";
			if(outputTimeStep.requiresAllHours() && outputTimeStep.requiresAllDays()
					&& outputTimeStep.requiresAllMonths()) {
				// Output results in years.
				masterSelectSQL += ", yearid, null as monthid, null as dayid, null as hourid";
				workerSelectSQL += ", yearid, null as monthid, null as dayid, null as hourid";
				groupBy += ", yearid";
				convertWeeks = true;
				selectActivitySQL += ", yearid, null as monthid, null as dayid, null as hourid";
				groupByActivity += ", yearid";
				selectBaseRateOutputSQL += ", yearid, 0 as monthid, 0 as hourdayid";
				groupByBaseRateOutput += ", yearid";

				nrActivitySummaryColumns.add("yearid");
				nrNeedsActivityWeight = true;
			} else if(outputTimeStep.requiresAllHours() &&
					outputTimeStep.requiresAllDays()) {
				// Output results in months
				masterSelectSQL += ", yearid, monthid, null as dayid, null as hourid";
				workerSelectSQL += ", yearid, monthid, null as dayid, null as hourid";
				groupBy += ", yearid, monthid";
				convertWeeks = true;
				selectActivitySQL += ", yearid, monthid, null as dayid, null as hourid";
				groupByActivity += ", yearid, monthid";
				selectBaseRateOutputSQL += ", yearid, monthid, 0 as hourdayid";
				groupByBaseRateOutput += ", yearid, monthid";

				nrActivitySummaryColumns.add("yearid");
				nrActivitySummaryColumns.add("monthid");
				nrNeedsActivityWeight = true;
			} else if(outputTimeStep.requiresAllHours()) {
				// Output results in days.
				convertDays = outputTimeStep.usesClassicalDay();
				masterSelectSQL += ", yearid, monthid, dayid, null as hourid";
				workerSelectSQL += ", yearid, monthid, dayid, null as hourid";
				groupBy += ", yearid, monthid, dayid";
				selectActivitySQL += ", yearid, monthid, dayid, null as hourid";
				groupByActivity += ", yearid, monthid, dayid";

				selectBaseRateOutputSQL += ", yearid, monthid, hourdayid";
				groupByBaseRateOutput += ", yearid, monthid, hourdayid";

				nrActivitySummaryColumns.add("yearid");
				nrActivitySummaryColumns.add("monthid");
				nrActivitySummaryColumns.add("dayid");
			} else {
				// Output results in hours.
				convertDays = outputTimeStep.usesClassicalDay();
				masterSelectSQL += ", yearid, monthid, dayid, hourid";
				workerSelectSQL += ", yearid, monthid, dayid, hourid";
				groupBy += ", yearid, monthid, dayid, hourid";
				selectActivitySQL += ", yearid, monthid, dayid, hourid";
				groupByActivity += ", yearid, monthid, dayid, hourid";

				selectBaseRateOutputSQL += ", yearid, monthid, hourdayid";
				groupByBaseRateOutput += ", yearid, monthid, hourdayid";

				nrActivitySummaryColumns.add("yearid");
				nrActivitySummaryColumns.add("monthid");
				nrActivitySummaryColumns.add("dayid");
				//nrActivitySummaryColumns.add("hourid");
			}

			// PollutantID
			masterInsertSQL += ", pollutantid";
			workerInsertSQL += ", pollutantid";
			masterSelectSQL += ", pollutantid";
			workerSelectSQL += ", pollutantid";
			groupBy += ", pollutantid";

			// AvgSpeedBinID and PollutantID
			insertBaseRateOutputSQL +=", avgspeedbinid, pollutantid";
			selectBaseRateOutputSQL += ", avgspeedbinid, pollutantid";
			groupByBaseRateOutput += ", avgspeedbinid, pollutantid";

			// activityTypeID
			insertActivitySQL +=", activitytypeid";
			selectActivitySQL += ", activitytypeid";
			groupByActivity += ", activitytypeid";
			groupByActivitySpatialOnly += ", activitytypeid";
			nrActivitySummaryColumns.add("activitytypeid");

			// Geographic detail
			GeographicOutputDetailLevel geographicOutputDetail = ExecutionRunSpec.theExecutionRunSpec.getGeographicOutputDetailLevel();
			ModelScale scale = ExecutionRunSpec.theExecutionRunSpec.getModelScale();
			ModelDomain domain = ExecutionRunSpec.theExecutionRunSpec.getModelDomain();

			/*
			System .out.println("geographicoutputdetail=" + geographicOutputDetail.toString());
			System .out.println("scale=" + scale.toString());
			System .out.println("domain=" + domain.toString());
			*/
			if(scale == null || scale == ModelScale.MACROSCALE) {
				if(geographicOutputDetail == GeographicOutputDetailLevel.NATION) {
					// This is ok
				} else if(geographicOutputDetail == GeographicOutputDetailLevel.STATE) {
					// This is ok
				} else if(geographicOutputDetail == GeographicOutputDetailLevel.COUNTY) {
					// This is ok
				} else if(geographicOutputDetail == GeographicOutputDetailLevel.ZONE) {
					// This is ok
				} else { // Link detail
					if(domain == ModelDomain.PROJECT) {
						// This is ok
					} else {
						// Force this change.  Macroscale, Non-Project cannot use the link level.
						geographicOutputDetail = GeographicOutputDetailLevel.COUNTY;
					}
				}
			}

			if(geographicOutputDetail == GeographicOutputDetailLevel.NATION) {
				masterSelectSQL += ", null as stateid, null as countyid, null as zoneid, null as linkid";
				workerSelectSQL += ", null as stateid, null as countyid, null as zoneid, null as linkid";
				masterInsertSQL += ", stateid, countyid, zoneid, linkid";
				workerInsertSQL += ", stateid, countyid, zoneid, linkid";
				selectActivitySQL += ", null as stateid, null as countyid, null as zoneid, "
						+ "null as linkid";
				insertActivitySQL += ", stateid, countyid, zoneid, linkid";

				selectBaseRateOutputSQL += ", 0 as zoneid, 0 as linkid";
				insertBaseRateOutputSQL += ", zoneid, linkid";

				nrNeedsActivityWeight = true;
			} else if(geographicOutputDetail == GeographicOutputDetailLevel.STATE) {
				if(ExecutionRunSpec.theExecutionRunSpec.getModels().contains(Model.NONROAD) && isWorkerSQL) {
					// In the NR worker case, countyID is not aggregated over because this detail is needed
					// on the master side to correctly aggregate LF and avgHP over multiple counties
					masterSelectSQL += ", stateid, countyid, null as zoneid, null as linkid";
					workerSelectSQL += ", stateid, countyid, null as zoneid, null as linkid";
					groupBy += ", stateid";
					masterInsertSQL += ", stateid, countyid, zoneid, linkid";
					workerInsertSQL += ", stateid, countyid, zoneid, linkid";
					selectActivitySQL += ", stateid, countyid, null as zoneid, null as linkid";
					groupByActivity += ", stateid, countyid";
					groupByActivitySpatialOnly += ", stateid, countyid";
					insertActivitySQL += ", stateid, countyid, zoneid, linkid";

					selectBaseRateOutputSQL += ", 0 as zoneid, 0 as linkid";
					insertBaseRateOutputSQL += ", zoneid, linkid";

					nrActivitySummaryColumns.add("stateid");
					nrActivitySummaryColumns.add("countyid");
					nrNeedsActivityWeight = true;					
				} else {
					// In the onroad case or NR master case, countyID is aggregated over as expected
					masterSelectSQL += ", stateid, null as countyid, null as zoneid, null as linkid";
					workerSelectSQL += ", stateid, null as countyid, null as zoneid, null as linkid";
					groupBy += ", stateid";
					masterInsertSQL += ", stateid, countyid, zoneid, linkid";
					workerInsertSQL += ", stateid, countyid, zoneid, linkid";
					selectActivitySQL += ", stateid, null as countyid, null as zoneid, null as linkid";
					groupByActivity += ", stateid";
					groupByActivitySpatialOnly += ", stateid";
					insertActivitySQL += ", stateid, countyid, zoneid, linkid";

					selectBaseRateOutputSQL += ", 0 as zoneid, 0 as linkid";
					insertBaseRateOutputSQL += ", zoneid, linkid";

					nrActivitySummaryColumns.add("stateid");
					nrNeedsActivityWeight = true;
				}
			} else if(geographicOutputDetail == GeographicOutputDetailLevel.COUNTY) {
				masterSelectSQL += ", stateid, countyid, null as zoneid, null as linkid";
				workerSelectSQL += ", stateid, countyid, null as zoneid, null as linkid";
				groupBy += ", stateid, countyid";
				masterInsertSQL += ", stateid, countyid, zoneid, linkid";
				workerInsertSQL += ", stateid, countyid, zoneid, linkid";
				selectActivitySQL += ", stateid, countyid, null as zoneid, null as linkid";
				groupByActivity += ", stateid, countyid";
				groupByActivitySpatialOnly += ", stateid, countyid";
				insertActivitySQL += ", stateid, countyid, zoneid, linkid";

				selectBaseRateOutputSQL += ", 0 as zoneid, 0 as linkid";
				insertBaseRateOutputSQL += ", zoneid, linkid";

				nrActivitySummaryColumns.add("stateid");
				nrActivitySummaryColumns.add("countyid");
			} else if(geographicOutputDetail == GeographicOutputDetailLevel.ZONE) {
				// Zone is the lowest level of geographic detail that can be specified at the
				// Macroscale level. However, at this level there is only one link of a given
				// road type in a given zone. The linkID will be automatically included if and
				// only if the road type is requested below.
				if(scale == null || scale == ModelScale.MACROSCALE) {
					masterSelectSQL += ", stateid, countyid, zoneid";
					workerSelectSQL += ", stateid, countyid, zoneid";
					groupBy += ", stateid, countyid, zoneid";
					masterInsertSQL += ", stateid, countyid, zoneid";
					workerInsertSQL += ", stateid, countyid, zoneid";
					selectActivitySQL += ", stateid, countyid, zoneid";
					groupByActivity += ", stateid, countyid, zoneid";
					groupByActivitySpatialOnly += ", stateid, countyid, zoneid";
					insertActivitySQL += ", stateid, countyid, zoneid";

					selectBaseRateOutputSQL += ", zoneid";
					insertBaseRateOutputSQL += ", zoneid";
					groupByBaseRateOutput += ", zoneid";

					nrActivitySummaryColumns.add("stateid");
					nrActivitySummaryColumns.add("countyid");
					nrActivitySummaryColumns.add("zoneid");
				} else {
					masterSelectSQL += ", stateid, countyid, zoneid, null as linkid";
					workerSelectSQL += ", stateid, countyid, zoneid, null as linkid";
					groupBy += ", stateid, countyid, zoneid";
					masterInsertSQL += ", stateid, countyid, zoneid, linkid";
					workerInsertSQL += ", stateid, countyid, zoneid, linkid";
					selectActivitySQL += ", stateid, countyid, zoneid, null as linkid";
					groupByActivity += ", stateid, countyid, zoneid";
					groupByActivitySpatialOnly += ", stateid, countyid, zoneid";
					insertActivitySQL += ", stateid, countyid, zoneid, linkid";

					selectBaseRateOutputSQL += ", zoneid, 0 as linkid";
					insertBaseRateOutputSQL += ", zoneid, linkid";
					groupByBaseRateOutput += ", zoneid";

					nrActivitySummaryColumns.add("stateid");
					nrActivitySummaryColumns.add("countyid");
					nrActivitySummaryColumns.add("zoneid");
				}
			} else { // Link detail
				if(scale == null || scale == ModelScale.MACROSCALE) {
					if(domain == ModelDomain.PROJECT) {
						masterSelectSQL += ", stateid, countyid, zoneid, linkid";
						workerSelectSQL += ", stateid, countyid, zoneid, linkid";
						groupBy += ", stateid, countyid, zoneid, linkid";
						masterInsertSQL += ", stateid, countyid, zoneid, linkid";
						workerInsertSQL += ", stateid, countyid, zoneid, linkid";
						selectActivitySQL += ", stateid, countyid, zoneid, linkid";
						groupByActivity += ", stateid, countyid, zoneid, linkid";
						groupByActivitySpatialOnly += ", stateid, countyid, zoneid, linkid";
						insertActivitySQL += ", stateid, countyid, zoneid, linkid";

						selectBaseRateOutputSQL += ", zoneid, linkid";
						insertBaseRateOutputSQL += ", zoneid, linkid";
						groupByBaseRateOutput += ", zoneid, linkid";

						nrActivitySummaryColumns.add("stateid");
						nrActivitySummaryColumns.add("countyid");
						nrActivitySummaryColumns.add("zoneid");
						nrActivitySummaryColumns.add("linkid");
					} else {
						// This should not happen
						/**
						 * @issue The Link geographic output detail level is not allowed for Macroscale simulations.
						 * @explain An internal inconsistency within the RunSpec was detected.  Macroscale,
						 * that is simulations involving inventory data, do not support link-level output.
						**/
						Logger.log(LogMessageCategory.ERROR,"the link geographic output detail "
								+ "level is not allowed for macroscale simulations.");
						// Provide some default behavior (County-level aggregation)
						masterSelectSQL += ", stateid, countyid, null as zoneid, null as linkid";
						workerSelectSQL += ", stateid, countyid, null as zoneid, null as linkid";
						groupBy += ", stateid, countyid";
						masterInsertSQL += ", stateid, countyid, zoneid, linkid";
						workerInsertSQL += ", stateid, countyid, zoneid, linkid";
						selectActivitySQL += ", stateid, countyid, null as zoneid, null as linkid";
						groupByActivity += ", stateid, countyid";
						groupByActivitySpatialOnly += ", stateid, countyid";
						insertActivitySQL += ", stateid, countyid, zoneid, linkid";

						selectBaseRateOutputSQL += ", zoneid, 0 as linkid";
						insertBaseRateOutputSQL += ", zoneid, linkid";
						groupByBaseRateOutput += ", zoneid";

						nrActivitySummaryColumns.add("stateid");
						nrActivitySummaryColumns.add("countyid");
					}
				} else {
					masterSelectSQL += ", stateid, countyid, zoneid, linkid";
					workerSelectSQL += ", stateid, countyid, zoneid, linkid";
					groupBy += ", stateid, countyid, zoneid, linkid";
					masterInsertSQL += ", stateid, countyid, zoneid, linkid";
					workerInsertSQL += ", stateid, countyid, zoneid, linkid";
					selectActivitySQL += ", stateid, countyid, zoneid, linkid";
					groupByActivity += ", stateid, countyid, zoneid, linkid";
					groupByActivitySpatialOnly += ", stateid, countyid, zoneid, linkid";
					insertActivitySQL += ", stateid, countyid, zoneid, linkid";

					selectBaseRateOutputSQL += ", zoneid, linkid";
					insertBaseRateOutputSQL += ", zoneid, linkid";
					groupByBaseRateOutput += ", zoneid, linkid";

					nrActivitySummaryColumns.add("stateid");
					nrActivitySummaryColumns.add("countyid");
					nrActivitySummaryColumns.add("zoneid");
					nrActivitySummaryColumns.add("linkid");
				}
			}

			// Emission Breakdown selections
			OutputEmissionsBreakdownSelection outputEmissionsBreakdownSelection =
					ExecutionRunSpec.theExecutionRunSpec.getOutputEmissionsBreakdownSelection();
			//System.out.println("outputemissionsbreakdownselection.roadtype=" + outputEmissionsBreakdownSelection.roadType);

			if(scale == null || scale == ModelScale.MACROSCALE) {
				// Handle Macroscale, roadTypeID, and linkID.  Specifically, at the
				// Macroscale level, knowing roadTypeID and zoneID, implies knowing
				// the linkID.
				if(!outputEmissionsBreakdownSelection.roadType) {
					// Road type is not desired
					if(geographicOutputDetail == GeographicOutputDetailLevel.ZONE) {
						// Road type is not desired but the zone is selected, the linkID is
						// unknown and not already present in the SQL statements
						masterSelectSQL += ", null as linkid, null as roadtypeid";
						workerSelectSQL += ", null as linkid, null as roadtypeid";
						masterInsertSQL += ", linkid, roadtypeid";
						workerInsertSQL += ", linkid, roadtypeid";
						selectActivitySQL += ", null as linkid, null as roadtypeid";
						insertActivitySQL += ", linkid, roadtypeid";

						selectBaseRateOutputSQL += ", 0 as linkid, 0 as roadtypeid";
						insertBaseRateOutputSQL += ", linkid, roadtypeid";
					} else {
						// Road type is not desired and zone is unknown, so linkID
						// is unknown and already present in the SQL statements
						masterSelectSQL += ", null as roadtypeid";
						workerSelectSQL += ", null as roadtypeid";
						masterInsertSQL += ", roadtypeid";
						workerInsertSQL += ", roadtypeid";
						selectActivitySQL += ", null as roadtypeid";
						insertActivitySQL += ", roadtypeid";

						selectBaseRateOutputSQL += ", 0 as roadtypeid";
						insertBaseRateOutputSQL += ", roadtypeid";
					}
				} else {
					// Road type is desired
					if(geographicOutputDetail == GeographicOutputDetailLevel.ZONE) {
						// Road type and zone are known, so linkID is known
						// and not already present in the SQL statements
						masterSelectSQL += ", linkid, roadtypeid";
						workerSelectSQL += ", linkid, roadtypeid";
						groupBy += ", linkid, roadtypeid";
						masterInsertSQL += ", linkid, roadtypeid";
						workerInsertSQL += ", linkid, roadtypeid";
						selectActivitySQL += ", linkid, roadtypeid";
						groupByActivity += ", linkid, roadtypeid";
						groupByActivitySpatialOnly += ", linkid, roadtypeid";
						insertActivitySQL += ", linkid, roadtypeid";

						selectBaseRateOutputSQL += ", linkid, roadtypeid";
						insertBaseRateOutputSQL += ", linkid, roadtypeid";
						groupByBaseRateOutput += ", linkid, roadtypeid";

						nrActivitySummaryColumns.add("linkid");
						//nrActivitySummaryColumns.add("roadtypeid");
					} else {
						// Road type is desired, but the zone is unknown, so linkID is unknown
						// and already present in the SQL statements
						masterSelectSQL += ", roadtypeid";
						workerSelectSQL += ", roadtypeid";
						groupBy += ", roadtypeid";
						masterInsertSQL += ", roadtypeid";
						workerInsertSQL += ", roadtypeid";
						selectActivitySQL += ", roadtypeid";
						groupByActivity += ", roadtypeid";
						groupByActivitySpatialOnly += ", roadtypeid";
						insertActivitySQL += ", roadtypeid";

						selectBaseRateOutputSQL += ", roadtypeid";
						insertBaseRateOutputSQL += ", roadtypeid";
						groupByBaseRateOutput += ", roadtypeid";

						//nrActivitySummaryColumns.add("roadtypeid");
					}
				}
			} else { // All other scales can assume linkID is already in the SQL statements
				masterInsertSQL += ", roadtypeid";
				workerInsertSQL += ", roadtypeid";
				insertActivitySQL += ", roadtypeid";

				if(!outputEmissionsBreakdownSelection.roadType) {
					masterSelectSQL += ", null as roadtypeid";
					workerSelectSQL += ", null as roadtypeid";
					selectActivitySQL += ", null as roadtypeid";

					selectBaseRateOutputSQL += ", 0 as roadtypeid";
					insertBaseRateOutputSQL += ", roadtypeid";
				} else {
					masterSelectSQL += ", roadtypeid";
					workerSelectSQL += ", roadtypeid";
					groupBy += ", roadtypeid";
					selectActivitySQL += ", roadtypeid";
					groupByActivity += ", roadtypeid";
					groupByActivitySpatialOnly += ", roadtypeid";

					selectBaseRateOutputSQL += ", roadtypeid";
					insertBaseRateOutputSQL += ", roadtypeid";
					groupByBaseRateOutput += ", roadtypeid";
				}
			}

			// Process ID
			masterInsertSQL += ", processid";
			workerInsertSQL += ", processid";
			insertBaseRateOutputSQL += ", processid";
			if(!outputEmissionsBreakdownSelection.emissionProcess) {
				masterSelectSQL += ", null as processid";
				workerSelectSQL += ", null as processid";
			} else {
				masterSelectSQL += ", processid";
				workerSelectSQL += ", processid";
				groupBy += ", processid";
			}
			// Base rates always include the process
			selectBaseRateOutputSQL += ", processid";
			groupByBaseRateOutput += ", processid";

			// Fuel Type ID
			masterInsertSQL += ", fueltypeid";
			workerInsertSQL += ", fueltypeid";
			insertActivitySQL += ", fueltypeid";
			insertBaseRateOutputSQL += ", fueltypeid";
			if(!outputEmissionsBreakdownSelection.fuelType) {
				masterSelectSQL += ", null as fueltypeid";
				workerSelectSQL += ", null as fueltypeid";
				selectActivitySQL += ", null as fueltypeid";
				selectBaseRateOutputSQL += ", 0 as fueltypeid";

				nrNeedsActivityWeight = true;
			} else {
				masterSelectSQL += ", fueltypeid";
				workerSelectSQL += ", fueltypeid";
				groupBy += ", fueltypeid";
				selectActivitySQL += ", fueltypeid";
				groupByActivity += ", fueltypeid";
				groupByActivitySpatialOnly += ", fueltypeid";
				selectBaseRateOutputSQL += ", fueltypeid";
				groupByBaseRateOutput += ", fueltypeid";

				nrActivitySummaryColumns.add("fueltypeid");
			}

			if(CompilationFlags.ALLOW_FUELSUBTYPE_OUTPUT) {
				// Fuel SubType ID
				masterInsertSQL += ", fuelsubtypeid";
				workerInsertSQL += ", fuelsubtypeid";
				insertActivitySQL += ", fuelsubtypeid";
				//insertBaseRateOutputSQL += ", fuelsubtypeid";
				if(!outputEmissionsBreakdownSelection.fuelSubType) {
					masterSelectSQL += ", null as fuelsubtypeid";
					workerSelectSQL += ", null as fuelsubtypeid";
					selectActivitySQL += ", null as fuelsubtypeid";
					//selectBaseRateOutputSQL += ", 0 as fuelsubtypeid";
					// nrNeedsActivityWeight = true; Not needed.
				} else {
					masterSelectSQL += ", fuelsubtypeid";
					workerSelectSQL += ", fuelsubtypeid";
					groupBy += ", fuelsubtypeid";
					selectActivitySQL += ", fuelsubtypeid";
					groupByActivity += ", fuelsubtypeid";
					groupByActivitySpatialOnly += ", fuelsubtypeid";
					//selectBaseRateOutputSQL += ", fuelsubtypeid";
					//groupByBaseRateOutput += ", fuelsubtypeid";
	
					nrActivitySummaryColumns.add("fuelsubtypeid");
				}
			}

			// Model year ID
			masterInsertSQL += ", modelyearid";
			workerInsertSQL += ", modelyearid";
			insertActivitySQL += ", modelyearid";
			insertBaseRateOutputSQL += ", modelyearid";
			if(!outputEmissionsBreakdownSelection.modelYear) {
				masterSelectSQL += ", null as modelyearid";
				workerSelectSQL += ", null as modelyearid";
				selectActivitySQL += ", null as modelyearid";
				selectBaseRateOutputSQL += ", 0 as modelyearid";

				nrNeedsActivityWeight = true;
			} else {
				masterSelectSQL += ", modelyearid";
				workerSelectSQL += ", modelyearid";
				groupBy += ", modelyearid";
				selectActivitySQL += ", modelyearid";
				groupByActivity += ", modelyearid";
				groupByActivitySpatialOnly += ", modelyearid";
				selectBaseRateOutputSQL += ", modelyearid";
				groupByBaseRateOutput += ", modelyearid";

				nrActivitySummaryColumns.add("modelyearid");
			}

			// Source Type ID
			masterInsertSQL += ", sourcetypeid";
			workerInsertSQL += ", sourcetypeid";
			insertActivitySQL += ", sourcetypeid";
			insertBaseRateOutputSQL += ", sourcetypeid";
			if(!outputEmissionsBreakdownSelection.sourceUseType) {
				masterSelectSQL += ", null as sourcetypeid";
				workerSelectSQL += ", null as sourcetypeid";
				selectActivitySQL += ", null as sourcetypeid";
				selectBaseRateOutputSQL += ", 0 as sourcetypeid";
			} else {
				masterSelectSQL += ", sourcetypeid";
				workerSelectSQL += ", sourcetypeid";
				groupBy += ", sourcetypeid";
				selectActivitySQL += ", sourcetypeid";
				groupByActivity += ", sourcetypeid";
				groupByActivitySpatialOnly += ", sourcetypeid";
				selectBaseRateOutputSQL += ", sourcetypeid";
				groupByBaseRateOutput += ", sourcetypeid";
			}

			// Regulatory Class ID
			masterInsertSQL += ", regclassid";
			workerInsertSQL += ", regclassid";
			insertActivitySQL += ", regclassid";
			insertBaseRateOutputSQL += ", regclassid";
			if(!outputEmissionsBreakdownSelection.regClassID) {
				masterSelectSQL += ", null as regclassid";
				workerSelectSQL += ", null as regclassid";
				selectActivitySQL += ", null as regclassid";
			} else {
				masterSelectSQL += ", regclassid";
				workerSelectSQL += ", regclassid";
				groupBy += ", regclassid";
				selectActivitySQL += ", regclassid";
				groupByActivity += ", regclassid";
				groupByActivitySpatialOnly += ", regclassid";
			}
			// Reg. class is not always included in base rates
			if(!outputEmissionsBreakdownSelection.regClassID) {
				selectBaseRateOutputSQL += ", 0 as regclassid";
			} else {
				selectBaseRateOutputSQL += ", regclassid";
				groupByBaseRateOutput += ", regclassid";
			}

			// SCC
			masterInsertSQL += ", scc";
			workerInsertSQL += ", scc";
			insertActivitySQL +=", scc";
			insertBaseRateOutputSQL += ", scc";
			if(!outputEmissionsBreakdownSelection.onRoadSCC) {
				masterSelectSQL += ", null as scc";
				workerSelectSQL += ", null as scc";
				selectActivitySQL += ", null as scc";
				selectBaseRateOutputSQL += ", '' as scc";

				nrNeedsActivityWeight = true;
			} else {
				masterSelectSQL += ", scc";
				workerSelectSQL += ", scc";
				groupBy += ", scc";
				selectActivitySQL += ", scc";
				groupByActivity += ", scc";
				groupByActivitySpatialOnly += ", scc";
				selectBaseRateOutputSQL += ", scc";
				groupByBaseRateOutput += ", scc";

				nrActivitySummaryColumns.add("scc");
			}

			// engTechID
			masterInsertSQL += ", engtechid";
			workerInsertSQL += ", engtechid";
			insertActivitySQL +=", engtechid";
			if(!ExecutionRunSpec.theExecutionRunSpec.getModels().contains(Model.NONROAD)
					|| !outputEmissionsBreakdownSelection.engTechID) {
				masterSelectSQL += ", null as engtechid";
				workerSelectSQL += ", null as engtechid";
				selectActivitySQL += ", null as engtechid";

				nrNeedsActivityWeight = true;
			} else {
				masterSelectSQL += ", engtechid";
				workerSelectSQL += ", engtechid";
				groupBy += ", engtechid";
				selectActivitySQL += ", engtechid";
				groupByActivity += ", engtechid";
				groupByActivitySpatialOnly += ", engtechid";

				nrActivitySummaryColumns.add("engtechid");
			}

			// sectorID
			masterInsertSQL += ", sectorid";
			workerInsertSQL += ", sectorid";
			insertActivitySQL +=", sectorid";
			if(!ExecutionRunSpec.theExecutionRunSpec.getModels().contains(Model.NONROAD)
					|| !outputEmissionsBreakdownSelection.sector) {
				masterSelectSQL += ", null as sectorid";
				workerSelectSQL += ", null as sectorid";
				selectActivitySQL += ", null as sectorid";

				nrNeedsActivityWeight = true;
			} else {
				masterSelectSQL += ", sectorid";
				workerSelectSQL += ", sectorid";
				groupBy += ", sectorid";
				selectActivitySQL += ", sectorid";
				groupByActivity += ", sectorid";
				groupByActivitySpatialOnly += ", sectorid";

				nrActivitySummaryColumns.add("sectorid");
			}

			// hpID
			masterInsertSQL += ", hpid";
			workerInsertSQL += ", hpid";
			insertActivitySQL +=", hpid";
			if(!ExecutionRunSpec.theExecutionRunSpec.getModels().contains(Model.NONROAD)
					|| !outputEmissionsBreakdownSelection.hpClass) {
				masterSelectSQL += ", null as hpid";
				workerSelectSQL += ", null as hpid";
				selectActivitySQL += ", null as hpid";

				nrNeedsActivityWeight = true;
			} else {
				masterSelectSQL += ", hpid";
				workerSelectSQL += ", hpid";
				groupBy += ", hpid";
				selectActivitySQL += ", hpid";
				groupByActivity += ", hpid";
				groupByActivitySpatialOnly += ", hpid";

				nrActivitySummaryColumns.add("hpid");
			}

			// Summed emission quantity and associated uncertainty values. If the output time period
			// excludes the day of the week, multiply the day's results by the average number of
			// times that the day occurs in the month to get a total results for the month.
			masterInsertSQL += ", emissionquant";
			workerInsertSQL += ", emissionquant, emissionrate";
			insertActivitySQL += ", activity";

			insertBaseRateOutputSQL += ", meanbaserate, emissionrate";

			selectActivityNoScaleSQL = selectActivitySQL.replace("select movesrunid,","select distinct movesrunid,") + ", sum(activity) as activity";

			if(ExecutionRunSpec.theExecutionRunSpec.getModels().contains(Model.NONROAD)) {
				// Nonroad runs natively with classical 24-hour days and is restricted
				// to such in the GUI. Therefore, no time aggregation should be done for Nonroad.
				masterSelectSQL += ", sum(emissionquant) as emissionquant";
				workerSelectSQL += ", sum(emissionquant) as emissionquant, sum(emissionrate) as emissionrate";
				selectActivitySQL += ", sum(activity) as activity";

				// If outputEmissionsBreakdownSelection.hpClass is off and
				// load factor is not output and retroFrac is not output, then force nrNeedsActivityWeight=false
				// because nothing would be output that needs to be weighted.

				// If things that could need weighting (load factor and retroFrac are always created)
				// must be weighted, then make the SQL for doing so.
				if(nrNeedsActivityWeight) {
					String keyColumnNames = "";
					for(String c : nrActivitySummaryColumns) {
						if(keyColumnNames.length() > 0) {
							keyColumnNames += ",";
						}
						keyColumnNames += c;
					}
					ArrayList<String> detailMatchColumns = new ArrayList<String>(Arrays.asList("scc","modelyearid",
						"engtechid","sectorid","hpid","fueltypeid","yearid","monthid","dayid","stateid","countyid"));
					if(CompilationFlags.ALLOW_FUELSUBTYPE_OUTPUT && outputEmissionsBreakdownSelection.fuelSubType) {
						detailMatchColumns.add("fuelsubtypeid");
					}
					String updateWhere = "";
					String detailKey = "";
					String detailSelect = "";
					for(int i=0;i<detailMatchColumns.size();i++) {
						String c = detailMatchColumns.get(i);
						if(detailKey.length() > 0) {
							detailKey += ",";
							detailSelect += ",";
							updateWhere += " and ";
						}
						detailKey += c;
						detailSelect += "a." + c;
						updateWhere += "movesworkeractivityoutput." + c + "=nractivityweightdetail." + c;
					}

					nrActivityWeightSQL.add("drop table if exists nractivityweightsummary;");
					nrActivityWeightSQL.add("drop table if exists nractivityweightdetail;");
					nrActivityWeightSQL.add("create table nractivityweightsummary like movesworkeractivityoutput;");
					nrActivityWeightSQL.add("alter table nractivityweightsummary add primary key (" + keyColumnNames + ");");
					nrActivityWeightSQL.add("insert into nractivityweightsummary (" + keyColumnNames + ",activity)"
							+ " select " + keyColumnNames + ",sum(activity) as activity"
							+ " from movesworkeractivityoutput where activitytypeid=2" // weight by source hours (activity type 2)
							+ " group by " + keyColumnNames
							+ " order by null;");
					nrActivityWeightSQL.add("create table nractivityweightdetail like movesworkeractivityoutput;");
					nrActivityWeightSQL.add("alter table nractivityweightdetail add primary key (" + detailKey + ");");
					nrActivityWeightSQL.add("insert into nractivityweightdetail(" + detailKey + ",activity,activitytypeid)"
							+ " select " + detailSelect + ","
							+ " case when s.activity>0 then a.activity/s.activity else 0.0 end as activity,2 as activitytypeid"
							+ " from movesworkeractivityoutput a"
							+ " inner join nractivityweightsummary s using (" + keyColumnNames + ")"
							+ " where a.activitytypeid=2;"); // weight by source hours (activity type 2)
					nrActivityWeightSQL.add("update movesworkeractivityoutput, nractivityweightdetail set movesworkeractivityoutput.activity=nractivityweightdetail.activity*movesworkeractivityoutput.activity"
							+ " where " + updateWhere
							+ " and movesworkeractivityoutput.activitytypeid in (9,10,12);"); // avgHP, retroFrac, LF load factor
					nrActivityWeightSQL.add("drop table if exists nractivityweightsummary;");
					nrActivityWeightSQL.add("drop table if exists nractivityweightdetail;");
				}
			} else if(convertWeeks && isWorkerSQL) {
				// Only scale to monthly in one place (on the worker)
				// in the calculation pipeline
				WeeksInMonthHelper weekHelper = new WeeksInMonthHelper();
				/*
				String daysPerMonthClause =
						weekHelper.getDaysPerMonthSQLClause("yearid","monthid","dayid");
				masterSelectSQL += ", sum(emissionquant*" + daysPerMonthClause
						+ ") as emissionquant";
				workerSelectSQL += ", sum(emissionquant*" + daysPerMonthClause
						+ ") as emissionquant";
				workerSelectSQL += ", sum(emissionrate*" + daysPerMonthClause
						+ ") as emissionrate";
				selectActivitySQL += ", sum(activity*" + daysPerMonthClause
						+ ") as activity";
				*/
				String weeksPerMonthClause =
						weekHelper.getWeeksPerMonthSQLClause("yearid","monthid");
				masterSelectSQL += ", sum(emissionquant*" + weeksPerMonthClause
						+ ") as emissionquant";
				workerSelectSQL += ", sum(emissionquant*" + weeksPerMonthClause
						+ ") as emissionquant";
				workerSelectSQL += ", sum(emissionrate*" + weeksPerMonthClause
						+ ") as emissionrate";
				selectActivitySQL += ", sum(activity*" + weeksPerMonthClause
						+ ") as activity";
			} else if(convertDays && isWorkerSQL) {
				// Only scale to classical days in one place (on the worker)
				// in the calculation pipeline
				WeeksInMonthHelper weekHelper = new WeeksInMonthHelper();
				String portionOfWeekPerDayClause =
						weekHelper.getPortionOfWeekPerDayClause("dayid");
				masterSelectSQL += ", sum(emissionquant*" + portionOfWeekPerDayClause
						+ ") as emissionquant";
				workerSelectSQL += ", sum(emissionquant*" + portionOfWeekPerDayClause
						+ ") as emissionquant";
				workerSelectSQL += ", sum(emissionrate*" + portionOfWeekPerDayClause
						+ ") as emissionrate";
				selectActivitySQL += ", sum(activity*" + portionOfWeekPerDayClause
						+ ") as activity";
			} else {
				masterSelectSQL += ", sum(emissionquant) as emissionquant";
				workerSelectSQL += ", sum(emissionquant) as emissionquant, sum(emissionrate) as emissionrate";
				selectActivitySQL += ", sum(activity) as activity";
			}

			selectBaseRateOutputSQL += ", sum(meanbaserate) as meanbaserate, sum(emissionrate) as emissionrate";
		} catch(Exception e) {
			/**
			 * @explain A database error occurred while creating SQL statements for
			 * aggregating data.
			**/
			Logger.logError(e, "failed to create base sql");
			return false;
		}

		/*
		System .out.println("masterselectsql=" + masterSelectSQL);
		System .out.println("workerselectsql=" + workerSelectSQL);
		System .out.println("groupby=" + groupBy);
		System .out.println("selectactivitysql=" + selectActivitySQL);
		System .out.println("selectactivitynoscalesql=" + selectActivityNoScaleSQL);
		System .out.println("groupbyactivity=" + groupByActivity);
		System .out.println("groupbyactivityspatialonly=" + groupByActivitySpatialOnly);
		System .out.println("masterinsertsql=" + masterInsertSQL);
		System .out.println("workerinsertsql=" + workerInsertSQL);
		System .out.println("insertactivitysql=" + insertActivitySQL);
		if(isWorkerSQL) {
			Logger .log(LogMessageCategory.INFO,"nrneedsactivityweight="+nrNeedsActivityWeight);
			if(nrNeedsActivityWeight) {
				Logger .log(LogMessageCategory.INFO,"nractivityweightsql=");
				for(String s : nrActivityWeightSQL) {
					Logger.log(LogMessageCategory.INFO,s);
				}
			}
		}
		*/

		return true;
	}

	/**
	 * Generates SQLs to be processed by OutputProcessor periodically while receiving data
	 * from workers.
	 * @return false if there is any problem while generating SQLs.
	**/
	boolean generateSQLsForOutputProcessor() {
		if(!generateBaseSQLForAggregation(false)) {
			return false;
		}
		if(!ENABLE_NONROAD_AGGREGATION
				&& ExecutionRunSpec.theExecutionRunSpec.getModels().contains(Model.NONROAD)) {
			// Temporarily disable all aggregation for Nonroad runs.
			return true;
		}
		try {
			outputProcessorSQLs.add("drop table if exists workeroutputtemp");
			outputProcessorSQLs.add(createSQL);
			outputProcessorSQLs.add(masterInsertSQL + ") " + masterSelectSQL
					+ " from " + ExecutionRunSpec.getEmissionOutputTable()
					+ " where movesrunid = " + activerunid + " and iterationid = "
					+ activeIterationID + " " + groupBy);
			outputProcessorSQLs.add("optimize table " + ExecutionRunSpec.getEmissionOutputTable());
			outputProcessorSQLs.add("delete from " + ExecutionRunSpec.getEmissionOutputTable()
					+ " where movesrunid = " + activeRunID
					+ " and iterationid = " + activeIterationID);
			String insertOutputSQL = "insert into " + ExecutionRunSpec.getEmissionOutputTable()
					+ "(" + masteroutputtablefields + ") "
					+ selectSQLForMasterOutput + " from workeroutputtemp";
			outputProcessorSQLs.add(insertOutputSQL);
			outputProcessorSQLs.add("drop table if exists workeroutputtemp");
		} catch(Exception e) {
			/**
			 * @explain A database error occurred while creating SQL statements for
			 * aggregating data.
			**/
			Logger.logError(e, "failed to generate sqls for output processor");
			return false;
		}
		try {
			outputProcessorSQLs.add("drop table if exists workeractivityoutputtemp");
			outputProcessorSQLs.add(createActivitySQL);

			if(ExecutionRunSpec.getRunSpec().outputPopulation) {
				outputProcessorSQLs.add(insertActivitySQL + ") " + selectActivitySQL
						+ " from " + ExecutionRunSpec.getActivityOutputTable()
						+ " where movesrunid = " + activerunid + " and iterationid = "
						+ activeIterationID + " and activitytypeid <> 6 " + groupByActivity);
				outputProcessorSQLs.add(insertActivitySQL + ") " + selectActivityNoScaleSQL
						+ " from " + ExecutionRunSpec.getActivityOutputTable()
						+ " where movesrunid = " + activerunid + " and iterationid = "
						+ activeIterationID + " and activitytypeid = 6 " + groupByActivitySpatialOnly);
			} else {
				outputProcessorSQLs.add(insertActivitySQL + ") " + selectActivitySQL
						+ " from " + ExecutionRunSpec.getActivityOutputTable()
						+ " where movesrunid = " + activerunid + " and iterationid = "
						+ activeIterationID + " " + groupByActivity);
			}

			outputProcessorSQLs.add("delete from " + ExecutionRunSpec.getActivityOutputTable()
					+ " where movesrunid = " + activeRunID
					+ " and iterationid = " + activeIterationID);
			String insertActivityOutputSQL = "insert into "
					+ ExecutionRunSpec.getActivityOutputTable() + " ("
					+ outputActivityTableFields + ") "
					+ selectSQLForActivityOutput + " from workeractivityoutputtemp";
			outputProcessorSQLs.add(insertActivityOutputSQL);
			outputProcessorSQLs.add("drop table if exists workeractivityoutputtemp");
		} catch(Exception e) {
			/**
			 * @explain A database error occurred while creating SQL statements for
			 * aggregating data.
			**/
			Logger.logError(e, "failed to generate sqls for output processor");
			return false;
		}

		// Aggregation of BaseRateOutput is not possible without supporting
		// population and activity data.
		if(false && CompilationFlags.DO_RATES_FIRST) {
//		if(CompilationFlags.DO_RATES_FIRST) {
			try {
				outputProcessorSQLs.add("drop table if exists workerbaserateoutputtemp");
				outputProcessorSQLs.add(createBaseRateOutputSQL);
				outputProcessorSQLs.add(insertBaseRateOutputSQL + ") " + selectBaseRateOutputSQL
						+ " from baserateoutput"
						+ " where movesrunid = " + activerunid + " and iterationid = "
						+ activeIterationID + " " + groupByBaseRateOutput);
				outputProcessorSQLs.add("delete from baserateoutput"
						+ " where movesrunid = " + activeRunID
						+ " and iterationid = " + activeIterationID);
				outputProcessorSQLs.add("insert into baserateoutput ("
						+ outputBaseRateOutputTableFields + ") "
						+ selectSQLForBaseRateOutput + " from workerbaserateoutputtemp");
				outputProcessorSQLs.add("drop table if exists workerbaserateoutputtemp");
			} catch(Exception e) {
				/**
				 * @explain A database error occurred while creating SQL statements for
				 * aggregating data.
				**/
				Logger.logError(e, "failed to generate sqls for output processor");
				return false;
			}
		}

		return true;
	}

	/**
	 * Generates SQLs to be processed by output processor, after all other processing has been
	 * completed but before unit conversions have been done.
	 * @return false if there is any problem while generating SQLs.
	**/
	boolean generateSQLsForFinalProcessing() {
		if(!generateBaseSQLForAggregation(false)) {
			return false;
		}
		if(!ENABLE_NONROAD_AGGREGATION
				&& ExecutionRunSpec.theExecutionRunSpec.getModels().contains(Model.NONROAD)) {
			// Temporarily disable all aggregation for Nonroad runs.
			return true;
		}
		try {
			finalProcessSQLs.add("drop table if exists workeroutputtemp");
			finalProcessSQLs.add(createSQL);
			finalProcessSQLs.add(masterInsertSQL + ") " + masterSelectSQL
					+ " from " + ExecutionRunSpec.getEmissionOutputTable()
					+ " where movesrunid = " + activerunid + " and iterationid = "
					+ activeIterationID + " " + groupBy);
			finalProcessSQLs.add("delete from " + ExecutionRunSpec.getEmissionOutputTable()
					+ " where movesrunid = " + activeRunID
					+ " and iterationid = " + activeIterationID);
			String insertOutputSQL = "insert into " + ExecutionRunSpec.getEmissionOutputTable()
					+ "(" + masteroutputtablefields + ") "
					+ selectSQLForMasterOutput + " from workeroutputtemp";
			finalProcessSQLs.add(insertOutputSQL);
			finalProcessSQLs.add("drop table if exists workeroutputtemp");
		} catch(Exception e) {
			/**
			 * @explain A database error occurred while creating SQL statements for
			 * aggregating data.
			**/
			Logger.logError(e, "failed to generate sqls for final processing");
			return false;
		}
		
		try {
			if(ExecutionRunSpec.theExecutionRunSpec.getModels().contains(Model.NONROAD) 
				&& ExecutionRunSpec.theExecutionRunSpec.getGeographicOutputDetailLevel() == GeographicOutputDetailLevel.STATE) {
				// Weight LF and avgHP during final aggregation if output is reported at state level (because all NR workers return county level data).
				// This is the same weighting code as run on the worker side, with the addition of the MOVESRunID and iterationID columns.
				// (Note: countyID has not been aggregated over by the worker in this specific case--it is aggregated over in the next try block)
				String keyColumnNames = "movesrunid,iterationid";
				for(String c : nrActivitySummaryColumns) {
					keyColumnNames += "," + c;
				}
				String[] detailMatchColumns = {
					"scc","modelyearid","engtechid","sectorid","hpid","fueltypeid",
					"yearid","monthid","dayid","stateid","countyid","movesrunid","iterationid"
				};
				String updateWhere = "";
				String detailKey = "";
				String detailSelect = "";
				for(int i=0;i<detailMatchColumns.length;i++) {
					String c = detailMatchColumns[i];
					if(detailKey.length() > 0) {
						detailKey += ",";
						detailSelect += ",";
						updateWhere += " and ";
					}
					detailKey += c;
					detailSelect += "a." + c;
					updateWhere += "a." + c + "=nractivityweightdetail." + c;
				}

				finalProcessSQLs.add("drop table if exists nractivityweightsummary;");
				finalProcessSQLs.add("drop table if exists nractivityweightdetail;");
				finalProcessSQLs.add("create table nractivityweightsummary like " + executionrunspec.getactivityoutputtable() + ";");
				finalProcessSQLs.add("alter table nractivityweightsummary add primary key (" + keyColumnNames + ");");
				finalProcessSQLs.add("insert into nractivityweightsummary (" + keyColumnNames + ",activity)"
						+ " select " + keyColumnNames + ",sum(activity) as activity"
						+ " from " + executionrunspec.getactivityoutputtable() + " where activitytypeid=2" // weight by source hours (activity type 2)
						+ " and movesrunid = "+ activeRunID
						+ " and iterationid = " + activeIterationID
						+ " group by " + keyColumnNames
						+ " order by null;");
				finalProcessSQLs.add("create table nractivityweightdetail like " + executionrunspec.getactivityoutputtable() + ";");
				finalProcessSQLs.add("alter table nractivityweightdetail add primary key (" + detailKey + ");");
				finalProcessSQLs.add("insert into nractivityweightdetail(" + detailKey + ",activity,activitytypeid)"
						+ " select " + detailSelect + ","
						+ " case when s.activity>0 then a.activity/s.activity else 0.0 end as activity,2 as activitytypeid"
						+ " from " + executionrunspec.getactivityoutputtable() + " a"
						+ " inner join nractivityweightsummary s using (" + keyColumnNames + ")"
						+ " where a.activitytypeid=2" // weight by source hours (activity type 2)
						+ " and movesrunid = "+ activeRunID
						+ " and iterationid = " + activeiterationid + ";");
				finalProcessSQLs.add("update " + executionrunspec.getactivityoutputtable() + " a, nractivityweightdetail set a.activity=nractivityweightdetail.activity*a.activity"
						+ " where " + updateWhere
						+ " and a.activitytypeid in (9,10,12);"); // avgHP, retroFrac, LF load factor
				finalProcessSQLs.add("drop table if exists nractivityweightsummary;");
				finalProcessSQLs.add("drop table if exists nractivityweightdetail;");
			}
		} catch(Exception e) {
			/**
			 * @explain A database error occurred while creating SQL statements for
			 * aggregating data.
			**/
			Logger.logError(e, "failed to generate sqls for final processing");
			return false;
		}
		
		try {
			finalProcessSQLs.add("drop table if exists workeractivityoutputtemp");
			finalProcessSQLs.add(createActivitySQL);

			if(ExecutionRunSpec.getRunSpec().outputPopulation) {
				finalProcessSQLs.add(insertActivitySQL + ") " + selectActivitySQL
						+ " from " + ExecutionRunSpec.getActivityOutputTable()
						+ " where movesrunid = " + activeRunID
						+ " and iterationid = " + activeIterationID
						+ " and activitytypeid <> 6 " + groupByActivity);
				finalProcessSQLs.add(insertActivitySQL + ") " + selectActivityNoScaleSQL
						+ " from " + ExecutionRunSpec.getActivityOutputTable()
						+ " where movesrunid = " + activeRunID
						+ " and iterationid = " + activeIterationID
						+ " and activitytypeid = 6 " + groupByActivitySpatialOnly);
			} else {
				finalProcessSQLs.add(insertActivitySQL + ") " + selectActivitySQL
						+ " from " + ExecutionRunSpec.getActivityOutputTable()
						+ " where movesrunid = " + activeRunID
						+ " and iterationid = " + activeiterationid + " " + groupByActivity);
			}

			finalProcessSQLs.add("delete from " + ExecutionRunSpec.getActivityOutputTable()
					+ " where movesrunid = " + activeRunID
					+ " and iterationid = " + activeIterationID);
			String insertActivityOutputSQL = "insert into "
					+ ExecutionRunSpec.getActivityOutputTable() + " ("
					+ outputActivityTableFields + ") " + selectSQLForActivityOutput
					+ " from workeractivityoutputtemp";
			finalProcessSQLs.add(insertActivityOutputSQL);
			finalProcessSQLs.add("drop table if exists workeractivityoutputtemp");
		} catch(Exception e) {
			/**
			 * @explain A database error occurred while creating SQL statements for
			 * aggregating data.
			**/
			Logger.logError(e, "failed to generate sqls for final processing");
			return false;
		}

		// Aggregation of BaseRateOutput is not possible without supporting
		// population and activity data.
		if(false && CompilationFlags.DO_RATES_FIRST) {
			try {
				finalProcessSQLs.add("drop table if exists workerbaserateoutputtemp");
				finalProcessSQLs.add(createBaseRateOutputSQL);
				finalProcessSQLs.add(insertBaseRateOutputSQL + ") " + selectBaseRateOutputSQL
						+ " from baserateoutput"
						+ " where movesrunid = " + activeRunID
						+ " and iterationid = " + activeiterationid + " " + groupByBaseRateOutput);
				finalProcessSQLs.add("delete from baserateoutput"
						+ " where movesrunid = " + activeRunID
						+ " and iterationid = " + activeIterationID);
				finalProcessSQLs.add("insert into baserateoutput ("
						+ outputBaseRateOutputTableFields + ") " + selectSQLForBaseRateOutput
						+ " from workerbaserateoutputtemp");
				finalProcessSQLs.add("drop table if exists workerbaserateoutputtemp");
			} catch(Exception e) {
				/**
				 * @explain A database error occurred while creating SQL statements for
				 * aggregating data.
				**/
				Logger.logError(e, "failed to generate sqls for final processing");
				return false;
			}
		}

		return true;
	}
}
