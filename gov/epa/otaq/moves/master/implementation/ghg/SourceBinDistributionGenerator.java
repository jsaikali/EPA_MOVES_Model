 /**************************************************************************************************
 * @(#)SourceBinDistributionGenerator.java
 *
 *************************************************************************************************/
package gov.epa.otaq.moves.master.implementation.ghg;

import gov.epa.otaq.moves.common.*;
import gov.epa.otaq.moves.master.runspec.*;
import gov.epa.otaq.moves.master.framework.*;
import java.util.*;
import java.sql.*;

/**
 * @algorithm
 * @owner SourceBin Distribution Generator
 * @generator
**/

/**
 * This builds source bin distribution data.
 *
 * @author		Wesley Faler
 * @author		Mitch Cumberworth
 * @version		2017-05-16
**/
public class SourceBinDistributionGenerator extends Generator {
	/** Default constructor **/
	public SourceBinDistributionGenerator() {
	}

	/**
	 * Database connection used by all methods of this class.  Setup by executeLoop and
	 * cleanDataLoop
	**/
	Connection db;
	/* Flag indicating that this is the first execution of executeLoop */
	boolean isFirstExecuteLoop = true;
	/* First and Last Model Years Implied by target RunSpec */
	int firstModelYearNeeded;
	int lastModelYearNeeded;
	/* Variables used to embed SQL declared here for use by all methods in class */
	String sql = "";
	//PreparedStatement statement = null;
	//ResultSet results = null;
	/** milliseconds spent during one time operations **/
	long setupTime = 0;
	/** milliseconds spent during non-one-time operations **/
	long totalTime = 0;
	/** CSV String objects holding fuelTypeIDs, keyed by sourceTypeID as an Integer **/
	TreeMap<Integer,String> fuelTypesBySourceType = new TreeMap<Integer,String>();
	/** Processes that have already been calculated **/
	TreeSet<Integer> processesDone = new TreeSet<Integer>();
	/** County/Year combinations already processed **/
	TreeSet<String> countyYearsDone = new TreeSet<String>();
	/** EmissionProcess used prior to the current context **/
	int priorProcessID = 0;
	/** Tables created that need to be dropped **/
	TreeSet<String> tablesToDrop = new TreeSet<String>();
	/** Model-year specific rolling and drag terms **/
	SourceTypePhysics modelYearPhysics = new SourceTypePhysics();

	/**
	 * Requests that this object subscribe to the given loop at desired looping points.
	 * Objects can assume that all necessary MasterLoopable objects have been instantiated.
	 *
	 * @param targetLoop The loop to subscribe to.
	 * This generator signs up for all "real" GHG processes at process level.
	 * Generator won't be called by MasterLoop if process not in RunSpec
	**/
	public void subscribeToMe(MasterLoop targetLoop) {
		/**
		 * @algorithm
		 * @signup Year
		**/
		String[] processNames = {
			"Running Exhaust",
			"Start Exhaust",
			"Extended Idle Exhaust",
			"Auxiliary Power Exhaust",
			"Evap Permeation",
			"Evap Fuel Vapor Venting",
			"Evap Fuel Leaks",
			"Evap Non-Fuel Vapors",
			"Brakewear",
			"Tirewear"
		};
		for(int i=0;i<processNames.length;i++) {
			EmissionProcess process = EmissionProcess.findByName(processNames[i]);
			if(process != null) {
				targetLoop.subscribe(this, process, MasterLoopGranularity.YEAR,
						MasterLoopPriority.GENERATOR+1); // execute ahead of operating mode generators that
														 // require the sourceBinDistribution
			}
		}
	}

	/**
	 * Called during each process level iteration of the MasterLoop.
	 *
	 * @param context The current context of the loop.
	**/
	public void executeLoop(MasterLoopContext context) {
		try {
			db = DatabaseConnectionManager.checkOutConnection(MOVESDatabaseType.EXECUTION);
			long start;
			Integer processID = Integer.valueOf(context.iterProcess.databaseKey);
			boolean isNewProcess = !processesDone.contains(processID);
			if(isNewProcess) {
				processesDone.add(processID);
				Logger.log(LogMessageCategory.DEBUG,"newSBD Generator called for process: " +
					 context.iterProcess.toString());
			}
			String countyYearKey = "" + context.iterProcess.databaseKey + "|" + context.iterLocation.countyRecordID + "|" + context.year;
			boolean isNewCountyYear = !countyYearsDone.contains(countyYearKey);
			if(isNewCountyYear) {
				countyYearsDone.add(countyYearKey);
			}

			if(priorProcessID != 0 && priorProcessID != context.iterProcess.databaseKey) {
				cleanupPriorProcess();
				priorProcessID = context.iterProcess.databaseKey;
			}

			if (isFirstExecuteLoop) {
				start = System.currentTimeMillis();
				isFirstExecuteLoop=false;
				doFirstTime(db);
				setupTime += System.currentTimeMillis() - start;
			}
			/* Find next pollutant which is in the RunSpec and exists for this Process */
			start = System.currentTimeMillis();
			if(isNewProcess) {
				for (Iterator i = ExecutionRunSpec.theExecutionRunSpec.pollutantProcessAssociations.iterator();
						i.hasNext();) {
					PollutantProcessAssociation polProcess = (PollutantProcessAssociation) i.next();
					if (polProcess.emissionProcess == context.iterProcess ) {
						doPollutantProcess(polProcess, db); // steps 200-299
					}
				}
				ExecutionRunSpec.refreshMacroExpanderSourceBins(db);
			}
			if(isNewCountyYear) {
				doCountyYear(context.iterProcess.databaseKey, context.iterLocation.countyRecordID, context.year); // TODO steps 300-399
			}
			if(isNewProcess && ExecutionRunSpec.theExecutionRunSpec.getModelDomain() == ModelDomain.PROJECT) {
				modelYearPhysics.updateEmissionRateTables(db,context.iterProcess.databaseKey);
			}
			totalTime = System.currentTimeMillis() - start;
		} catch (Exception e) {
			Logger.logError(e,"Unable to generate new Source Bin Distribution.");
		} finally {
			DatabaseConnectionManager.checkInConnection(MOVESDatabaseType.EXECUTION, db);
			db = null;
		}

		Logger.log(LogMessageCategory.INFO,"newSBDG setupTime=" + setupTime + " bundleTime=" + totalTime);
	}

	/**
	 * Removes data from the execution database that was created by this object within executeLoop
	 * for the same context. This is only called after all other loopable objects that might use
	 * data created by executeLoop have had their executeLoop and cleanDataLoop functions called.
	 * @param context The MasterLoopContext that applies to this execution.
	**/
	public void cleanDataLoop(MasterLoopContext context) {
		// Nothing to do at the year level here.  Process level cleanup is done during executeLoop.
	}

	/** Remove data from SourceBinDistribution associated with the Emission Process that was previous run. **/
	void cleanupPriorProcess() {
		if(ExecutionRunSpec.shouldSaveData(this)) {
			return;
		}

		SQLRunner.Query query = new SQLRunner.Query();
		String sql = "SELECT polprocessid from pollutantprocessassoc "
					+ "WHERE processid = " + priorProcessID;
		try {
			db = DatabaseConnectionManager.checkOutConnection(MOVESDatabaseType.EXECUTION);
			query.open(db,sql);
			String pollutantProcessID = "";
			boolean isFirst = true;
			while(query.rs.next()) {
				if(!isFirst) {
					pollutantProcessID += ",";
				}
				isFirst = false;
				pollutantProcessID += query.rs.getString(1);
			}
			query.close();
			if (pollutantProcessID != "") {
				sql = "DELETE FROM sourcebindistribution where polprocessid in (" +
						pollutantProcessID + ")";
			} else {
				sql = "DELETE FROM sourcebindistribution where isuserinput = 'N'";
			}
			SQLRunner.executeSQL(db, sql);

			for(Iterator<String> i=tablesToDrop.iterator();i.hasNext();) {
				String tableName = i.next();
				sql = "drop table if exists " + tableName;
				SQLRunner.executeSQL(db, sql);
			}
			tablesToDrop.clear();
		} catch(Exception e) {
			Logger.logSqlError(e,"Could not remove Source Bin Distribution data from previous process.",sql);
		} finally {
			query.onFinally();

			DatabaseConnectionManager.checkInConnection(MOVESDatabaseType.EXECUTION,db);
			db=null;
		}
	}

	/**
	 * Called for each pollutant-process which is in the RunSpec and in the Process
	 *
	 * @param pProc the pollutant process association
	 * @param db a connection to the execution database
	**/
	void doPollutantProcess(PollutantProcessAssociation pProc, Connection db){
		Logger.log(LogMessageCategory.DEBUG,"Processing pollutant-process: " + pProc.pollutant.toString()
			 + " - " + pProc.emissionProcess.toString());

		Statement statement = null;
		ResultSet rs = null;

		try {

			/* Simply return if not in SourceTypePolProcessTable */
			sql = "SELECT stpp.polprocessid "
				 + " from sourcetypepolprocess as stpp, pollutantprocessassoc as ppa "
				 + " where stpp.polprocessid = ppa.polprocessid and "
				 + " ppa.processid = " + pProc.emissionProcess.databaseKey
				 + " AND ppa.pollutantid = " + pProc.pollutant.databaseKey;
			statement=db.createStatement();
			rs = SQLRunner.executeQuery(statement,sql);
			if(!rs.next()) {
				Logger.log(LogMessageCategory.DEBUG,
						"No SourceBinDistribution for pollutant-process "
						 + pProc.pollutant.toString()
						 + " - " + pProc.emissionProcess.toString()
						 + " because there is no SourceTypePolProcess information for it.");
				rs.close();
				rs = null;
				statement.close();
				statement = null;
				return;
			}
			int polProcessID = rs.getInt(1);
			rs.close();
			rs = null;
			statement.close();
			statement = null;

			/* Determine the set of SourceTypes which should be blocked
			   because they already have output data for this pollutant process
			   (ref paragraph h. of Task description). */
			sql = "DROP TABLE IF EXISTS blocksourcetype" ;
			SQLRunner.executeSQL(db, sql);

			/**
			 * @step 200
			 * @algorithm Determine the set of SourceTypes which should be blocked
			 * because they already have output data for the current polProcessID.
			 * @output BlockSourceType
			 * @input SourceBinDistribution
			 * @input SourceTypeModelYear
			 * @input current polProcessID
			**/
			sql = "CREATE TABLE blocksourcetype " +
				"select sourcetypeid " +
				"from sourcebindistribution inner join sourcetypemodelyear " +
				"using (sourcetypemodelyearid) where polprocessid = " + polProcessID +
				" GROUP BY sourcetypeid ";
			SQLRunner.executeSQL(db, sql);

			/**
			 * @step 200
			 * @algorithm Determine the set of SourceTypes which should be used,
			 * namely those that exist in SourceTypePolProcess for the current
			 * polProcessID and which are not listed in BlockSourceType.
			 * @output list of sourceTypeIDs
			 * @input SourceTypePolProcess
			 * @input BlockSourceType
			 * @input current polProcessID
			**/
			TreeSet<Integer> sourceTypesToUse =
					new TreeSet<Integer>(); // sourceTypeID Integer objects
			sql = "SELECT sourcetypepolprocess.sourcetypeid " +
				"from sourcetypepolprocess left join blocksourcetype " +
				"using (sourcetypeid) " +
				"where polprocessid = " + polProcessID +
				" AND ISNULL(blocksourcetype.sourcetypeid) " +
				"GROUP BY sourcetypeid ";
			statement = db.createStatement();
			rs = SQLRunner.executeQuery(statement,sql);
			while(rs.next()) {
				int id = rs.getInt(1);
				sourceTypesToUse.add(Integer.valueOf(id));
			}
			rs.close();
			rs = null;
			statement.close();
			statement = null;

			/**
			 * @step 200
			 * @algorithm Perform the following steps for each source type is the list of sourceTypeIDs
			 * and for the current polProcessID.
			 * @input list of sourceTypeIDs
			 * @input current polProcessID
			**/
			for(Iterator<Integer> i=sourceTypesToUse.iterator();i.hasNext();) {
				Integer intSourceTypeID = (Integer)i.next();
				int sourceTypeID = intSourceTypeID.intValue();
				String fuelTypeIDs = (String)fuelTypesBySourceType.get(intSourceTypeID);
				if(fuelTypeIDs == null || fuelTypeIDs.length() <= 0) {
					continue;
				}

				/**
				 * @step 200
				 * @algorithm Lookup isRegClassRequired and isMYGroupRequired.
				 * @input SourceTypePolProcess
				 * @input current sourceTypeID
				 * @input current polProcessID
				**/
				sql = "select * from sourcetypepolprocess"
						+ " where sourcetypeid=" + sourceTypeID
						+ " and polprocessid=" + polProcessID;
				statement = db.createStatement();
				rs = SQLRunner.executeQuery(statement,sql);
				boolean found = false;
				boolean isRegClassRequired = false;
				boolean isMYGroupRequired = false;
				if(rs.next()) {
					found = true;
					isRegClassRequired = StringUtilities.stringToBoolean(rs.getString("isRegClassReqd"));
					isMYGroupRequired = StringUtilities.stringToBoolean(rs.getString("isMYGroupReqd"));
				}
				rs.close();
				rs = null;
				statement.close();
				statement = null;
				if(!found) {
					Logger.log(LogMessageCategory.ERROR,
							"Unable to find SourceTypePolProcess for sourceTypeID="
							+ sourceTypeID + ", polProcessID=" + polProcessID);
				}

				/* Example coreSQL where all data is required:
				insert into SBDGSVP (sourceTypeModelYearID, fuelTypeID, engTechID,
					regClassID,
					stmyFraction,
					modelYearGroupID, shortModYrGroupID)
				select
					svp.sourceTypeModelYearID, fuelTypeID, engTechID,
					regClassID,
					sum(stmyFraction),
					myg.modelYearGroupID, shortModYrGroupID
				from sampleVehiclePopulation svp
				inner join pollutantProcessModelYear ppmy on (ppmy.modelYearID=svp.modelYearID)
				inner join modelYearGroup myg on (myg.modelYearGroupID=ppmy.modelYearGroupID)
				where sourceTypeID=21
					and ppmy.polProcessID=9101
					and svp.modelYearID <= 2030
					and svp.modelYearID >= 2000
					and fuelTypeID in (1,2)
					and stmyFraction > 0.0
				group by sourceTypeModelYearID, fuelTypeID, engTechID,
					regClassID,
					shortModYrGroupID
				order by null
				*/

				/**
				 * @step 200
				 * @algorithm Aggregate sampleVehiclePopulation.stmyFraction based upon
				 * the need for regulatory class and model year group in the source bin.
				 * stmyFraction[sourceTypeModelYearID,fuelTypeID,engTechID,optionally regClassID,optionally shortModYrGroupID] = sum(SampleVehiclePopulation.stmyFraction).
				 * @output SBDGSVP
				 * @input sampleVehiclePopulation
				 * @input pollutantProcessModelYear
				 * @input modelYearGroup
				 * @input isRegClassRequired
				 * @input isMYGroupRequired
				**/
				String coreSQL =
					"insert into sbdgsvp (sourcetypemodelyearid, fueltypeid, engtechid, "
					+ " regclassid, "
					+ " stmyfraction, "
					+ " modelyeargroupid, shortmodyrgroupid)"
					+ " select"
						+ " svp.sourcetypemodelyearid, fueltypeid, engtechid,";
				coreSQL += (isRegClassRequired?"":"0 as") + " regclassid,";
				coreSQL += " sum(stmyfraction),";
				coreSQL += (isMYGroupRequired?" myg.":" 0 as ") + "modelyeargroupid,";
				coreSQL += (isMYGroupRequired?"":"0 as") + " shortmodyrgroupid";
				coreSQL +=
					" from samplevehiclepopulation svp"
					+ " inner join pollutantprocessmodelyear ppmy on (ppmy.modelyearid=svp.modelyearid)"
					+ " inner join modelyeargroup myg on (myg.modelyeargroupid=ppmy.modelyeargroupid)"
					+ " where svp.sourcetypeid=" + sourceTypeID
					+ " and ppmy.polprocessid=" + polProcessID
					+ " and svp.modelyearid <= " + lastModelYearNeeded
					+ " and svp.modelyearid >= " + firstModelYearNeeded
					+ " and fueltypeid in (" + fuelTypeIDs + ")"
					+ " and stmyfraction > 0.0"
					+ " group by sourcetypemodelyearid, fueltypeid, engtechid";
				coreSQL += isRegClassRequired?",regclassid":"";
				coreSQL += isMYGroupRequired?",shortmodyrgroupid":"";
				coreSQL += " order by null";

				String[] sqlBlock = {
					"drop table if exists sbdgsvp",

					// Since done for only one source type and pollutant/process at a time,
					// those fields are not needed.
					"create table if not exists sbdgsvp ("
							+ "	sourcetypemodelyearid int(10) unsigned not null,"
							+ "	fueltypeid smallint(5) unsigned not null,"
							+ "	engtechid smallint(6) not null,"
							+ "	regclassid smallint(5) unsigned not null,"
							+ "	stmyfraction double not null,"
							+ "	modelyeargroupid int(11) not null,"
							+ "	shortmodyrgroupid smallint not null,"
							+ "	sourcebinid bigint not null default 0,"
							+ "	index (sourcebinid),"
							+ "	index (sourcetypemodelyearid, sourcebinid)"
							+ ")",

					coreSQL,

					/**
					 * @step 200
					 * @algorithm Assign sourceBinID by combining other columns.
					 * sourceBinID= 1000000000000000000+ fuelTypeID*10000000000000000 + engTechID*100000000000000 + regClassID*1000000000000 + shortModYrGroupID*10000000000.
					 * @output SBDGSVP
					**/
					"update sbdgsvp set sourcebinid= 1000000000000000000"
							+ " + fueltypeid		*10000000000000000"
							+ " + engtechid			*100000000000000"
							+ " + regclassid		*1000000000000"
							+ " + shortmodyrgroupid	*10000000000"
							+ " + 0      			*1000000"
							+ " + 0					*100",

					"drop table if exists newsourcebin2",

					/**
					 * @step 200
					 * @algorithm Find sourceBinIDs that do not already exist within SourceBin.
					 * @input SBDGSVP
					 * @input SourceBin
					 * @output NewSourceBin2
					**/
					"create table if not exists newsourcebin2"
							+ " select s.sourcebinid, 0 as engsizeid, s.fueltypeid, s.engtechid,"
							+ " s.regclassid, s.modelyeargroupid, 0 as weightclassid"
							+ " from sbdgsvp s"
							+ " left join sourcebin sb using (sourcebinid)"
							+ " where isnull(sb.sourcebinid)"
							+ " group by s.sourcebinid"
							+ " order by null",

					/**
					 * @step 200
					 * @algorithm Add sourceBinIDs that did not previously exist within SourceBin.
					 * @output SourceBin
					 * @input NewSourceBin2
					**/
					"insert into sourcebin (sourcebinid, engsizeid, fueltypeid, engtechid, "
							+ " regclassid, modelyeargroupid, weightclassid)"
							+ " select sourcebinid, engsizeid, fueltypeid, engtechid, regclassid,"
							+ " modelyeargroupid, weightclassid"
							+ " from newsourcebin2",

					/** @output SourceBinDistribution **/

					/**
					 * @step 200
					 * @algorithm sourceBinActivityFraction[sourceTypeModelYearID,sourceBinID] = sum(stmyFraction)
					 * of SampleVehiclePopulation records that have been aggregated
					 * according to sourcebin needs.
					 * @output SourceBinDistribution
					 * @input SBDGSVP
					**/
					"insert into sourcebindistribution (sourcetypemodelyearid,"
							+ " polprocessid, sourcebinid, sourcebinactivityfraction)"
							+ " select sourcetypemodelyearid, " + polProcessID + " as polprocessid,"
							+ " sourcebinid, sum(stmyfraction)"
							+ " from sbdgsvp"
							+ " group by sourcetypemodelyearid, sourcebinid"
							+ " order by null"
				};
				for(int si=0;si < sqlBlock.length;si++) {
					sql = sqlBlock[si];
					SQLRunner.executeSQL(db,sql);
				}
			}
			/**
			 * @step 200
			 * @algorithm Done iterating over the list of sourceTypeIDs
			 * for the current polProcessID.
			 * @input list of sourceTypeIDs
			**/
		} catch (SQLException e) {
			Logger.logSqlError(e,"Unable to perform Source Bin Distribution for pollutant/"
					+"process," + pProc.pollutant.toString() + "/" +
					pProc.emissionProcess.toString(),sql);
		} finally {
			if(rs != null) {
				try {
					rs.close();
				} catch(Exception e) {
					// Nothing to do here
				}
				rs = null;
			}
			if(statement!=null) {
				try {
					statement.close();
				} catch (SQLException e) {
					// Failure to close on a preparedStatment should not be an issue.
				}
			}
		}
	}

	/** Called Once Globally
	*   1. Calculates TreeSet of SourceTypeIDs in RunSpec
	*   2. Calculates first year needed equal to
	*   earliest calendar year in runspec minus largest ageid value
	*   Calulates latest model year needed equal to latest calendar year
	*
	*   @param db The connection to execution database to use
	**/
	void doFirstTime(Connection db) {
		//Calculate first and last model years needed
		TreeSet<Integer> yrs = ExecutionRunSpec.theExecutionRunSpec.years;
		int firstCalendarYear = ((Integer)(yrs.first())).intValue();
		lastModelYearNeeded = ((Integer)(yrs.last())).intValue();
		Statement statement = null;
		ResultSet rs = null;
		try {
			/** @input AgeCategory **/
			sql="SELECT MAX(ageid) AS maxage from agecategory";
			statement=db.createStatement();
			rs=SQLRunner.executeQuery(statement,sql);
			rs.next(); //we can assume there is one result
			firstModelYearNeeded = firstCalendarYear - rs.getInt("maxAge");
			rs.close();
			rs = null;
			statement.close();
			statement = null;

			// Build list of fuels used by each source type.  This will help with
			// optimizations later in the process.
			sql = "select sourcetypeid, fueltypeid"
					+ " from runspecsourcefueltype"
					+ " order by sourcetypeid, fueltypeid";
			statement=db.createStatement();
			rs = SQLRunner.executeQuery(statement,sql);
			while(rs.next()) {
				int sourceTypeID = rs.getInt(1);
				int fuelTypeID = rs.getInt(2);
				Integer id = Integer.valueOf(sourceTypeID);
				String csv = (String)fuelTypesBySourceType.get(id);
				if(csv == null) {
					csv = "";
				}
				if(csv.length() > 0) {
					csv += ",";
				}
				csv += fuelTypeID;
				fuelTypesBySourceType.put(id,csv);
			}
			rs.close();
			rs = null;
			statement.close();
			statement = null;
		} catch (SQLException e) {
			Logger.logSqlError(e,"Unable to calculate model year range needed", sql);
			firstModelYearNeeded = 1966;
			lastModelYearNeeded = 2060;
		} finally {
			if(rs != null) {
				try {
					rs.close();
				} catch(Exception e) {
					// Nothing to do here
				}
				rs = null;
			}
			if(statement != null) {
				try {
					statement.close();
				} catch(Exception e) {
					// Nothing to do here
				}
				statement = null;
			}
		}
	}

	/**
	 * Create a source bin distribution specific to a process, county, and year.  The new
	 * distribution is associated with the fuels actually used in county/year rather than
	 * the equipped fuels given generically in SourceBinDistribution.
	 * @param processID emission process identifier
	 * @param countyID county identifier
	 * @param year calendar year
	**/
	void doCountyYear(int processID, int countyID, int year) {
		String newSBDTable = "sourcebindistributionfuelusage_" + processID + "_" + countyID + "_" + year;

		String[] statements = {
			"drop table if exists sourcebinfuelusage",
			"create table sourcebinfuelusage ("
					+ " 	equippedsourcebinid bigint(20) not null,"
					+ " 	usedsourcebinid bigint(20) not null,"
					+ " 	usagefraction double not null,"
					+ " 	key (equippedsourcebinid, usedsourcebinid),"
					+ " 	key (usedsourcebinid, equippedsourcebinid)"
					+ ")",

			/**
			 * @step 300
			 * @algorithm equippedSourceBinID=sourceBin[fuelTypeID=sourceBinFuelTypeID].
			 * usedSourceBinID=sourceBin[fuelTypeID=fuelSupplyFuelTypeID].
			 * @output sourceBinFuelUsage
			 * @input fuelUsageFraction
			 * @input sourceBin
			**/
			(CompilationFlags.USE_FUELUSAGEFRACTION?
				"insert into sourcebinfuelusage (equippedsourcebinid, usedsourcebinid, usagefraction)"
						+ " select e.sourcebinid, u.sourcebinid, usagefraction"
						+ " from fuelusagefraction f"
						+ " inner join sourcebin e on ("
						+ " 	e.fueltypeid=f.sourcebinfueltypeid"
						+ "	and (f.modelyeargroupid=0 or e.modelyeargroupid=f.modelyeargroupid))"
						+ " inner join sourcebin u on ("
						+ " 	u.fueltypeid=f.fuelsupplyfueltypeid"
						+ " 	and u.engtechid=e.engtechid"
						+ " 	and u.regclassid=e.regclassid"
						+ " 	and u.modelyeargroupid=e.modelyeargroupid"
						+ " 	and u.engsizeid=e.engsizeid"
						+ " 	and u.weightclassid=e.weightclassid)"
						+ " where f.countyid = " + countyID
						+ " and f.fuelyearid = (select fuelyearid from year where yearid=" + year + ")"
				:
				null
			),
			"drop table if exists " + newSBDTable,
			"create table " + newSBDTable + " like sourcebindistribution",

			/**
			 * @step 300
			 * @algorithm Convert sourceBinDistribution from equipped fuels to used fuels basis.
			 * Use a new output table with a name that includes the process, county, and calendar year.
			 * sourceBinActivityFraction[sourceBinID=usedSourceBinID]=sum(usageFraction * sourceBinActivityFraction).
			 * @output sourceBinDistributionFuelUsage_[processID]_[countyID]_[year]
			 * @input sourceBinDistribution
			 * @input sourceBinFuelUsage
			 * @input pollutantProcessAssoc
			**/
			(CompilationFlags.USE_FUELUSAGEFRACTION?
				"insert into " + newSBDTable + " ("
						+ " 	sourcetypemodelyearid, polprocessid, sourcebinid,"
						+ " 	sourcebinactivityfraction,"
						+ " 	sourcebinactivityfractioncv, isuserinput)"
						+ " select sourcetypemodelyearid, d.polprocessid, u.usedsourcebinid,"
						+ " 	sum(u.usagefraction * sourcebinactivityfraction),"
						+ " 	null, 'N'"
						+ " from sourcebindistribution d"
						+ " inner join sourcebinfuelusage u on (u.equippedsourcebinid = d.sourcebinid)"
						+ " inner join pollutantprocessassoc ppa on ("
						+ " 	ppa.processid = " + processID
						+ " 	and ppa.polprocessid = d.polprocessid)"
						+ " group by sourcetypemodelyearid, d.polprocessid, u.usedsourcebinid"
				:
				"insert into " + newSBDTable + " ("
						+ " 	sourcetypemodelyearid, polprocessid, sourcebinid,"
						+ " 	sourcebinactivityfraction,"
						+ " 	sourcebinactivityfractioncv, isuserinput)"
						+ " select sourcetypemodelyearid, d.polprocessid, sourcebinid,"
						+ " 	sourcebinactivityfraction,"
						+ " 	null, 'N'"
						+ " from sourcebindistribution d"
						+ " inner join pollutantprocessassoc ppa on ("
						+ " 	ppa.processid = " + processID
						+ " 	and ppa.polprocessid = d.polprocessid)"
			),

			"drop table if exists sourcebinfuelusage"
		};
		String sql = "";
		try {
			for(int i=0;i<statements.length;i++) {
				sql = statements[i];
				if(sql != null && sql.length() > 0) {
					SQLRunner.executeSQL(db,sql);
				}
			}
			tablesToDrop.add(newSBDTable);
		} catch(Exception e) {
			Logger.logSqlError(e,"Unable to create county/year-specific source bin distribution",sql);
		}
	}
}
