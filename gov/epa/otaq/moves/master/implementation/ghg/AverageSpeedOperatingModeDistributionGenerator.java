/**************************************************************************************************
 * @(#)AverageSpeedOperatingModeDistributionGenerator.java
 *
 *
 *
 *************************************************************************************************/
package gov.epa.otaq.moves.master.implementation.ghg;

import gov.epa.otaq.moves.common.*;
import gov.epa.otaq.moves.master.runspec.*;
import gov.epa.otaq.moves.master.framework.*;
import java.sql.*;

/**
 * This builds "Operating Mode Distribution" records for Running Exhaust and Tirewear
 * based upon average speed distributions.
 *
 * @author		Wesley Faler
 * @version		2014-03-04
**/
public class AverageSpeedOperatingModeDistributionGenerator extends Generator {
	/**
	 * @algorithm
	 * @owner Average Speed Operating Mode Distribution Generator
	 * @generator
	**/

	/** Database connection used by all functions.  Setup by executeLoop and cleanDataLoop. **/
	Connection db;
	/** Running Exhaust process **/
	EmissionProcess runningProcess = null;
	/** Tirewear process **/
	EmissionProcess tireProcess = null;
	/** true if executeLoop and cleanDataLoop are being execute for the Tirewear process **/
	boolean isTirewear = false;
	/** comma-separated list of polProcessIDs used by the most recent run of executeLoop() **/
	String polProcessIDs = "";
	/** case-clause for assigning operating modes **/
	String opModeAssignmentSQL = null;

	/** Default constructor **/
	public AverageSpeedOperatingModeDistributionGenerator() {
	}

	/**
	 * Requests that this object subscribe to the given loop at desired looping points.
	 * Objects can assume that all necessary MasterLoopable objects have been instantiated.
	 *
	 * @param targetLoop The loop to subscribe to.
	**/
	public void subscribeToMe(MasterLoop targetLoop) {
		// Subscribe at the LINK level because testing revealed it to be substantially faster to
		// perform the queries for a single link than once for an entire zone.
		tireProcess = EmissionProcess.findByName("Tirewear");
		if(CompilationFlags.DO_RATES_FIRST) {
			if(ExecutionRunSpec.getRunSpec().domain == ModelDomain.PROJECT) {
				targetLoop.subscribe(this, tireProcess, MasterLoopGranularity.LINK,
						MasterLoopPriority.GENERATOR);
			} else {
				targetLoop.subscribe(this, tireProcess, MasterLoopGranularity.PROCESS,
						MasterLoopPriority.GENERATOR);
			}
		} else {
			targetLoop.subscribe(this, tireProcess, MasterLoopGranularity.LINK,
					MasterLoopPriority.GENERATOR);
		}
	}

	/**
	 * Called each time the link changes.
	 *
	 * @param inContext The current context of the loop.
	**/
	public void executeLoop(MasterLoopContext inContext) {
		try {
			db = DatabaseConnectionManager.checkOutConnection(MOVESDatabaseType.EXECUTION);
			if(inContext.iterProcess.databaseKey == tireProcess.databaseKey) {
				isTirewear = true;
				polProcessIDs = "11710";
			} else {
				isTirewear = false;
				polProcessIDs = "";
				Logger.log(LogMessageCategory.ERROR,"AvgSpeedOMDG called for unknown process");
				return;
			}
			if(CompilationFlags.DO_RATES_FIRST) {
				if(ExecutionRunSpec.getRunSpec().domain == ModelDomain.PROJECT) {
					if(isTirewear) {
						calculateTireProjectOpModeFractions(inContext.iterLocation.linkRecordID);
					}
				} else {
					calculateRatesFirstOpModeFractions();
				}
			} else {
				if(ExecutionRunSpec.getRunSpec().domain == ModelDomain.PROJECT) {
					if(isTirewear) {
						calculateTireProjectOpModeFractions(inContext.iterLocation.linkRecordID);
					}
				} else {
					calculateOpModeFractions(inContext.iterLocation.linkRecordID);
				}
			}
		} catch (Exception e) {
			Logger.logError(e,"Operating Mode Distribution Generation failed.");
		} finally {
			DatabaseConnectionManager.checkInConnection(MOVESDatabaseType.EXECUTION, db);
			db = null;
		}
	}

	/**
	 * Removes data from the execution database that was created by this object within executeLoop
	 * for the same context. This is only called after all other loopable objects that might use
	 * data created by executeLoop have had their executeLoop and cleanDataLoop functions called.
	 * @param context The MasterLoopContext that applies to this execution.
	**/
	public void cleanDataLoop(MasterLoopContext context) {
		if(CompilationFlags.DO_RATES_FIRST) {
			// Nothing to do in Rates First mode.
			return;
		}
		String sql = "";
		try {
			if(polProcessIDs.length() > 0) {
				db = DatabaseConnectionManager.checkOutConnection(MOVESDatabaseType.EXECUTION);
				sql = "DELETE FROM opmodedistribution WHERE isuserinput='N' AND linkid = "
						+ context.iterLocation.linkRecordID
						+ " AND polprocessid IN (" + polProcessIDs + ")";
				SQLRunner.executeSQL(db, sql);
			}
		} catch(Exception e) {
			Logger.logSqlError(e,"Unable to delete Operating Mode Distribution data from a previous"
					+ " run", sql);
		} finally {
			if(db != null) {
				DatabaseConnectionManager.checkInConnection(MOVESDatabaseType.EXECUTION,db);
				db=null;
			}
		}
	}

	/**
	 * Calculate operating mode fractions based upon the link average speed.
	 * @param linkID The link being processed.
	**/
	void calculateTireProjectOpModeFractions(int linkID) {
		if(opModeAssignmentSQL == null) {
			opModeAssignmentSQL = buildOpModeClause();
		}
		String[] statements = null;
		if(CompilationFlags.DO_RATES_FIRST) {
			String[] t = {
				/**
				 * @step 010
				 * @algorithm Remove all tirewear pollutant/processes on any link from the operating mode distribution.
				 * @output RatesOpModeDistribution
				 * @condition Project domain
				 * @condition Tirewear process
				**/
				"delete from ratesopmodedistribution where polprocessid=11710",

				/**
				 * @step 020
				 * @algorithm Assign operating mode based upon a single link's linkAvgSpeed, one operating mode entry per link with opModeFraction=1.0.
				 * @input link
				 * @input operatingMode
				 * @output RatesOpModeDistribution
				 * @condition Project domain
				 * @condition Tirewear process
				**/
				"insert ignore into ratesopmodedistribution (sourcetypeid, roadtypeid, avgspeedbinid, hourdayid,"
				+ " polprocessid, opmodeid, opmodefraction, avgbinspeed)"
				+ " select sourcetypeid, roadtypeid, 0 as avgspeedbinid, hourdayid, "
				+ " 	11710 as polprocessid,"
				+ " 	(case " + opModeAssignmentSQL + " else -1 end) as opmodeid,"
				+ " 	1 as opmodefraction,"
				+ " 	linkavgspeed avgbinspeed"
				+ " from link"
				+ " inner join runspecsourcetype"
				+ " inner join runspechourday"
				+ " where linkid=" + linkID
			};
			statements = t;
		} else {
			String[] t = {
				"insert ignore into opmodedistribution (sourcetypeid, hourdayid, linkid, polprocessid,"
				+ " 	opmodeid, opmodefraction, opmodefractioncv, isuserinput)"
				+ " select sourcetypeid, hourdayid, linkid, 11710 as polprocessid,"
				+ " 	(case " + opModeAssignmentSQL + " else -1 end) as opmodeid,"
				+ " 	1 as opmodefraction,"
				+ " 	0 as opmodefractioncv,"
				+ " 	'N' as isuserinput"
				+ " from link"
				+ " inner join runspecsourcetype"
				+ " inner join runspechourday"
			};
			statements = t;
		}
		String sql = "";
		try {
			for(int i=0;i<statements.length;i++) {
				sql = statements[i];
				if(sql != null && sql.length() > 0) {
					SQLRunner.executeSQL(db,sql);
				}
			}
		} catch (SQLException e) {
			Logger.logSqlError(e,"Could not create tirewear opmode distribution.",sql);
		}
	}

	/**
	 * Calculate operating mode fractions based upon the average speed distribution.
	 * @param linkID The link being processed.
	**/
	void calculateOpModeFractions(int linkID) {
		String opModeIDColumn = "";
		if(isTirewear) {
			opModeIDColumn = "opmodeidtirewear";
		//} else if(isRunningExhaust) {
		//	opModeIDColumn = "opModeIDRunning";
		}

		String[] statements = {
			"insert ignore into Opmodedistribution (sourcetypeid, linkid, hourdayid,"
				+ " polprocessid, opmodeid, opmodefraction)"
				+ " select sourcetypeid, linkid, hourdayid, polprocessid, "
				+ opModeIDColumn + ", sum(avgspeedfraction)"
				+ " from avgspeedbin bin"
				+ " inner join avgspeeddistribution dist on dist.avgspeedbinid=bin.avgspeedbinid"
				+ " inner join link on link.roadtypeid=dist.roadtypeid,"
				+ " pollutantprocessassoc"
				+ " where polprocessid in (" + polProcessIDs + ")"
				+ " and linkid=" + linkID
				+ " group by sourcetypeid, linkid, hourdayid, polprocessid, " + opModeIDColumn
				+ " having sum(avgspeedfraction) > 0"
				+ " order by sourcetypeid, linkid, hourdayid, polprocessid, " + opModeIDColumn,

			"ANALYZE TABLE opmodedistribution"
		};
		String sql = "";
		try {
			for(int i=0;i<statements.length;i++) {
				sql = statements[i];
				SQLRunner.executeSQL(db,sql);
			}
		} catch (SQLException e) {
			Logger.logSqlError(e,"Could not determine final Operating Mode Distribution.",sql);
		}
	}

	/**
	 * Calculate operating mode fractions for each speed bin.
	**/
	void calculateRatesFirstOpModeFractions() {
		String opModeIDColumn = "";
		if(isTirewear) {
			opModeIDColumn = "opmodeidtirewear";
		//} else if(isRunningExhaust) {
		//	opModeIDColumn = "opModeIDRunning";
		}

		String[] statements = null;
		if(CompilationFlags.USE_2010B_TIREWEAR_RATE_METHOD) {
			// This method is known to make incorrect rates. It does match
			// the rates produced by 2010B. However, those rates cannot be
			// combined with activity to match the 2010B tirewear inventory.
			String[] t = {
				"drop table if exists tavgspeedromd",
	
				"create table tavgspeedromd"
					+ " select sourcetypeid, roadtypeid, avgspeedbinid, hourdayid, "
					+ " polprocessid, avgbinspeed"
					+ " from avgspeedbin bin,"
					+ " runspecsourcetype sut,"
					+ " runspecroadtype rt,"
					+ " runspechourday hd,"
					+ " pollutantprocessassoc ppa"
					+ " where polprocessid in (" + polProcessIDs + ")",
	
				"alter table tavgspeedromd add key (hourdayid, roadtypeid, sourcetypeid)",
	
				"insert ignore into ratesopmodedistribution (sourcetypeid, roadtypeid, avgspeedbinid, hourdayid,"
					+ " polprocessid, opmodeid, opmodefraction, avgbinspeed)"
					+ " select r.sourcetypeid, r.roadtypeid, r.avgspeedbinid, r.hourdayid, "
					+ " 	r.polprocessid, " + opModeIDColumn + ", dist.avgspeedfraction as opmodefraction, r.avgbinspeed"
					+ " from avgspeedbin bin"
					+ " inner join avgspeeddistribution dist on (dist.avgspeedbinid=bin.avgspeedbinid)"
					+ " inner join tavgspeedromd r on ("
					+ " 	r.sourcetypeid = dist.sourcetypeid"
					+ " 	and r.roadtypeid = dist.roadtypeid"
					+ " 	and r.hourdayid = dist.hourdayid)",
	
				"ANALYZE TABLE ratesopmodedistribution"
			};
			statements = t;
		} else {
			String[] t = {
				/**
				 * @step 100
				 * @algorithm Create a single operating mode entry, with opModeFraction=1.0, for every combination of speed bin, source type, 
				 * road type, hour day, and tirewear pollutant in the runspec. Each speed bin has an associated tirewear operating mode in 
				 * its opModeIDTirewear column.
				 * @input avgSpeedBin
				 * @output RatesOpModeDistribution
				 * @condition Non-Project domain
				 * @condition Tirewear process
				**/
				"insert ignore into ratesopmodedistribution (sourcetypeid, roadtypeid, avgspeedbinid, hourdayid,"
					+ " polprocessid, opmodeid, opmodefraction, avgbinspeed)"
					+ " select sourcetypeid, roadtypeid, avgspeedbinid, hourdayid, "
					+ " polprocessid, " + opModeIDColumn + ", 1 as opmodefraction, avgbinspeed"
					+ " from avgspeedbin bin,"
					+ " runspecsourcetype sut,"
					+ " runspecroadtype rt,"
					+ " runspechourday hd,"
					+ " pollutantprocessassoc ppa"
					+ " where polprocessid in (" + polProcessIDs + ")",
	
				"ANALYZE TABLE ratesopmodedistribution"
			};
			statements = t;
		}
		String sql = "";
		try {
			for(int i=0;i<statements.length;i++) {
				sql = statements[i];
				if(sql != null && sql.length() > 0) {
					SQLRunner.executeSQL(db,sql);
				}
			}
		} catch (SQLException e) {
			Logger.logSqlError(e,"Could not create RatesOpModeDistribution for tire wear.",sql);
		}
	}

	/**
	 * Build case-centric clause for assigning operating modes.  The clause will be used within
	 * a SQL CASE statement.
	 * Example lines:
	 * 	when (VSP < 1 and speed >= 5) then 314
	 *  when (VSP <= 2 and speed <= 17) then 999
	 * @return case-centric clause for assigning operating modes
	**/
	String buildOpModeClause() {
		String clause = "when linkAvgSpeed<0.1 then 400\n";
		String sql = "select speedlower, speedupper, opmodeid "
				+ " from operatingmode"
				+ " where opmodeid >= 401 and opmodeid <= 499"
				+ " and opmodename like 'tirewear%'"
				+ " order by speedlower";
		SQLRunner.Query query = new SQLRunner.Query();
		try {
			query.open(db,sql);
			while(query.rs.next()) {
				boolean hasCondition = false;
				String line = "when (";

				float speedLower = query.rs.getFloat("speedlower");
				if(!query.rs.wasNull()) {
					if(hasCondition) {
						line += " and ";
					}
					hasCondition = true;
					line += "" + speedLower + " <= linkavgspeed";
				}

				float speedUpper = query.rs.getFloat("speedupper");
				if(!query.rs.wasNull()) {
					if(hasCondition) {
						line += " and ";
					}
					hasCondition = true;
					line += "linkavgspeed < " + speedUpper;
				}

				line += ") then " + query.rs.getInt("opmodeid");
				clause += line + "\n";
			}
		} catch(SQLException e) {
			query.onException(e,"Unable to build operating mode clause",sql);
		} finally {
			query.onFinally();
		}
		return clause;
	}
}
