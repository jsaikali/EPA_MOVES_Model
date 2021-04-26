/**************************************************************************************************
 * @(#)LinkOperatingModeDistributionGenerator.java
 *
 *
 *
 *************************************************************************************************/
package gov.epa.otaq.moves.master.implementation.ghg;

import gov.epa.otaq.moves.common.*;
import gov.epa.otaq.moves.master.runspec.*;
import gov.epa.otaq.moves.master.framework.*;
import java.sql.*;
import java.util.*;

/**
 * This builds "Operating Mode Distribution" records for project domains.
 *
 * @author		Wesley Faler
 * @author		W. Aikman
 * @version		2017-05-16
**/
public class LinkOperatingModeDistributionGenerator extends Generator {
	/**
	 * @algorithm
	 * @owner Link Operating Mode Distribution Generator
	 * @generator
	**/

	/** Flags for tasks already done, used to prevent duplicate execution **/
	TreeSet<String> alreadyDoneFlags = new TreeSet<String>();
	/** String objects representing tables and time/location keys that have been generated **/
	TreeSet<String> itemsGenerated = new TreeSet<String>();
	/** Database connection used by all functions.  Setup by executeLoop and cleanDataLoop. **/
	Connection db;
	/** milliseconds spent during one time operations **/
	long setupTime = 0;
	/** milliseconds spent during non-one-time operations **/
	long totalTime = 0;
	/** comma-separated list of polProcessIDs used by this generator **/
	String polProcessIDs = "";
	/** case-clause for assigning operating modes **/
	String opModeAssignmentSQL = null;
	/** true if one-time setup activities have already occurred **/
	boolean didSetup = false;
	/**
	 * Messages generated when driving cycles are not available from average speeds.
	 * Used here to prevent duplicate messages.
	**/
	TreeSet<String> outOfBoundMessagesGenerated = new TreeSet<String>();
	/** Model-year specific rolling and drag terms **/
	SourceTypePhysics modelYearPhysics = new SourceTypePhysics();
	/** road type of the previous link **/
	int previousRoadTypeID = 0;
	
	/** Default constructor **/
	public LinkOperatingModeDistributionGenerator() {
	}

	/**
	 * Requests that this object subscribe to the given loop at desired looping points.
	 * Objects can assume that all necessary MasterLoopable objects have been instantiated.
	 *
	 * @param targetLoop The loop to subscribe to.
	**/
	public void subscribeToMe(MasterLoop targetLoop) {
		EmissionProcess runningProcess = EmissionProcess.findByName("Running Exhaust");
		// Signup at the Link level but only for the purpose of populating OperatingModeDistribution
		// in small steps which are faster for subsequent joins by calculators.  Grade data is
		// used for the links as given by the driving cycle.

		// GENERATOR+1 priority is used because this generator fills Link.linkAvgSpeed based
		// on drive schedules and Link.linkAvgSpeed is needed by ProjectTAG to do SHO calculations.

		targetLoop.subscribe(this, runningProcess, MasterLoopGranularity.LINK,
				MasterLoopPriority.GENERATOR+1);

		EmissionProcess brakeProcess = EmissionProcess.findByName("Brakewear");
		targetLoop.subscribe(this, brakeProcess, MasterLoopGranularity.LINK,
				MasterLoopPriority.GENERATOR+1);
	}

	/**
	 * Called each time the link changes.
	 *
	 * @param inContext The current context of the loop.
	**/
	public void executeLoop(MasterLoopContext inContext) {
		try {
			db = DatabaseConnectionManager.checkOutConnection(MOVESDatabaseType.EXECUTION);

			long start, detailStart, detailEnd;

			if(!didSetup) {
				start = System.currentTimeMillis();
				didSetup = true;
				/**
				 * @step 010
				 * @algorithm Setup for Model Year Physics effects.
				**/
				modelYearPhysics.setup(db);
				setup();
				setupTime = System.currentTimeMillis() - start;
			}

			start = System.currentTimeMillis();
			if(inContext.iterLocation.roadTypeRecordID != 1) {
				String alreadyKey = "calc|" + inContext.iterProcess.databaseKey + "|" + inContext.iterLocation.linkRecordID;
				if(!alreadyDoneFlags.contains(alreadyKey)) {
					alreadyDoneFlags.add(alreadyKey);
					detailStart = System.currentTimeMillis();
					calculateOpModeFractions(inContext.iterLocation.linkRecordID); // steps 100-199
					detailEnd = System.currentTimeMillis();
					Logger.log(LogMessageCategory.DEBUG,"LinkOperatingModeDistributionGenerator.calculateOpModeFractions ms="+(detailEnd-detailStart));
					if(CompilationFlags.DO_RATES_FIRST) {
						detailStart = System.currentTimeMillis();
						populateRatesOpModeDistribution(inContext.iterLocation.linkRecordID, inContext.iterLocation.roadTypeRecordID); // steps 200-299
						detailEnd = System.currentTimeMillis();
						Logger.log(LogMessageCategory.DEBUG,"LinkOperatingModeDistributionGenerator.populateRatesOpModeDistribution ms="+(detailEnd-detailStart));
					}
				}

				alreadyKey = "rates|" + inContext.iterProcess.databaseKey;
				if(!alreadyDoneFlags.contains(alreadyKey)) {
					alreadyDoneFlags.add(alreadyKey);
					/**
					 * @step 900
					 * @algorithm Update emission rate tables for Model Year Physics effects.
					**/
					//detailStart = System.currentTimeMillis();
					//modelYearPhysics.updateEmissionRateTables(db,inContext.iterProcess.databaseKey);
					//detailEnd = System.currentTimeMillis();
					//Logger.log(LogMessageCategory.DEBUG,"LinkOperatingModeDistributionGenerator.modelYearPhysics.updateEmissionRateTables processID=" + inContext.iterProcess.databaseKey + " ms="+(detailEnd-detailStart));
				}
			}
			totalTime += System.currentTimeMillis() - start;
		} catch (Exception e) {
			Logger.logError(e,"Operating Mode Distribution Generation failed.");
		} finally {
			DatabaseConnectionManager.checkInConnection(MOVESDatabaseType.EXECUTION, db);
			db = null;
		}

		Logger.log(LogMessageCategory.INFO,"LOMDG setupTime=" + setupTime + " bundleTime=" + totalTime);
	}

	/**
	 * Removes data from the execution database that was created by this object within executeLoop
	 * for the same context. This is only called after all other loopable objects that might use
	 * data created by executeLoop have had their executeLoop and cleanDataLoop functions called.
	 * @param context The MasterLoopContext that applies to this execution.
	**/
	public void cleanDataLoop(MasterLoopContext context) {
		// Don't do any cleanup.  All data created herein could be needed across multiple processes.
		/*
		String sql = "";
		try {
			if(polProcessIDs.length() > 0) {
				db = DatabaseConnectionManager.checkOutConnection(MOVESDatabaseType.EXECUTION);

				sql = "DELETE FROM OpModeDistribution WHERE isUserInput='N' AND linkID = "
						+ context.iterLocation.linkRecordID
						+ " AND polProcessID IN (" + polProcessIDs + ")";
				//System.out.println("########## DELETING OPD ###### : " +
				//		context.iterLocation.linkRecordID);
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
		*/
	}

	/** Perform one-time setup operations **/
	void setup() {
		String sql = "";
		String[] statements = {
			// Get the table of sourceTypeIDs and polProcessIDs only once before we write
			// anything to opModeDistrubtion
			"drop table if exists tempExistingOpMode",

			"create table tempExistingOpMode"
					+ " select distinct sourceTypeID, polProcessID, linkID"
					+ " from opModeDistribution",

			"drop table if exists tempLinkBracket",
		};
		try {
			for(int i=0;i<statements.length;i++) {
				sql = statements[i];
				SQLRunner.executeSQL(db,sql);
			}
		} catch (SQLException e) {
			Logger.logSqlError(e,"Could not do link operating mode setup", sql);
		}
	}

	/**
	 * Determine if a section of data, identified by a key, within a table has already
	 * been generated.  If it has, true is returned.  If it has not, false is returned and
	 * the data is marked as having been generated.
	 * @param tableName table to hold the generated data
	 * @param key key to the data, such as the year or link
	 * @return true if the data has already been generated, false otherwise.
	**/
	boolean hasGenerated(String tableName, int key) {
		return hasGenerated(tableName,key,0);
	}

	/**
	 * Determine if a section of data, identified by two keys, within a table has already
	 * been generated.  If it has, true is returned.  If it has not, false is returned and
	 * the data is marked as having been generated.
	 * @param tableName table to hold the generated data
	 * @param key1 key to the data, such as the year or link
	 * @param key2 key to the data, such as the year or link
	 * @return true if the data has already been generated, false otherwise.
	**/
	boolean hasGenerated(String tableName, int key1, int key2) {
		String t = tableName + "|" + key1 + "|" + key2;
		if(itemsGenerated.contains(t)) {
			return true;
		}
		itemsGenerated.add(t);
		return false;
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
		String clause = "";
		String sql = "select VSPLower, VSPUpper, speedLower, speedUpper, opModeID "
				+ " from operatingMode"
				+ " where opModeID >= 1 and opModeID <= 99"
				+ " and opModeID not in (26,36)" // These operating modes are redundant with others
				+ " order by opModeID";
		SQLRunner.Query query = new SQLRunner.Query();
		try {
			query.open(db,sql);
			while(query.rs.next()) {
				boolean hasCondition = false;
				String line = "when (";

				float vspLower = query.rs.getFloat("VSPLower");
				if(!query.rs.wasNull()) {
					if(hasCondition) {
						line += " and ";
					}
					hasCondition = true;
					line += "" + vspLower + " <= VSP";
				}

				float vspUpper = query.rs.getFloat("VSPUpper");
				if(!query.rs.wasNull()) {
					if(hasCondition) {
						line += " and ";
					}
					hasCondition = true;
					line += "VSP < " + vspUpper;
				}

				float speedLower = query.rs.getFloat("speedLower");
				if(!query.rs.wasNull()) {
					if(hasCondition) {
						line += " and ";
					}
					hasCondition = true;
					line += "" + speedLower + " <= speed";
				}

				float speedUpper = query.rs.getFloat("speedUpper");
				if(!query.rs.wasNull()) {
					if(hasCondition) {
						line += " and ";
					}
					hasCondition = true;
					line += "speed < " + speedUpper;
				}

				line += ") then " + query.rs.getInt("opModeID");
				clause += line + "\n";
			}
		} catch(SQLException e) {
			query.onException(e,"Unable to build operating mode clause",sql);
		} finally {
			query.onFinally();
		}
		return clause;
	}

	/**
	 * OMDG-7: Calculate overall operating mode fractions.
	 * <p>The overall operating mode fractions are calculated by weighting the operating mode
	 * fractions of each drive schedule by the drive schedule fractions. This is done for each
	 * source type, road type, day of the week, hour of the day and operating mode.</p>
	 * @param linkID The link being processed.
	**/
	void calculateOpModeFractions(int linkID) {
		if(hasGenerated("driveScheduleSecondLink",linkID)) {
			return;
		}
		
		if(hasGenerated("opModeDistribution",linkID)) {
			return;
		}
		
		
		String sql = "";
		try {
			boolean hasDriveSchedule = false;
			boolean hasRunningOpModeDistribution = false;
			double averageSpeed = 0.0;

			sql = "select count(*) from driveScheduleSecondLink where linkID=" + linkID;
			if(SQLRunner.executeScalar(db,sql) > 0) {
				hasDriveSchedule = true;
			} 
			
			sql = "select count(*) from opModeDistribution where linkID=" + linkID + " and (polProcessID % 100) = 1";
			if(SQLRunner.executeScalar(db,sql) > 0) {
				hasRunningOpModeDistribution = true;
			}
			
			if (!(hasDriveSchedule || hasRunningOpModeDistribution)) {
				/**
				 * @step 100
				 * @algorithm Lookup linkAvgSpeed for the current link.
				 * @input link
				**/
				sql = "select linkAvgSpeed from link where linkID=" + linkID;
				averageSpeed = SQLRunner.executeScalar(db,sql);
				if(averageSpeed <= 0) {
					/**
					 * @step 100
					 * @algorithm For links with zero average speed, provide a default
					 * drive schedule of 30 seconds of idling. Use 0 grade because with 0 speed, 
					 * brakes are likely applied rather than using the engine to counteract 
					 * any grade.  This removes the grade's effect on VSP.
					 * @output driveScheduleSecondLink
					 * @condition linkAvgSpeed <= 0
					**/

					// Provide a default drive schedule of all idling
					sql = "insert into driveScheduleSecondLink"
							+ " (linkID, secondID, speed, grade) values ";
					for(int i=1;i<=30;i++) { // 30 seconds
						if(i > 1) {
							sql += ",";
						}
						sql += "(" + linkID + "," + i + ", 0, 0)";
						// Use 0 grade because with 0 speed, brakes are likely applied rather
						// than using the engine to counteract any grade.  This removes the
						// grade's effect on VSP.
					}
					SQLRunner.executeSQL(db,sql);
					hasDriveSchedule = true;
				}
			}
			
			if(hasDriveSchedule && !hasRunningOpModeDistribution) {
				sql = "";
				/**
				 * @step 109
				 * @algorithm Perform the steps to calculate a link's operating mode distribution from its drive schedule.
				 * @condition The current link has a drive schedule not just an average speed.
				**/
				calculateOpModeFractionsCore(linkID,null); // steps 110-149
				
				// if averageSpeed = 0, then the code above creates an idle cycle and runs it like a drive schedule
			} else if (!hasRunningOpModeDistribution && averageSpeed > 0) {
				sql = "";
				interpolateOpModeFractions(linkID,averageSpeed); // steps 101-105
			} else if (!hasRunningOpModeDistribution) {
				Logger.log(LogMessageCategory.ERROR,"Link " + linkID + " has no drive schedule or average speed or operating mode distribution.");
			} else {
				Logger.log(LogMessageCategory.DEBUG,"Link " + linkID + " is using input operating mode distribution.");
			}
			
			modelYearPhysics.updateOperatingModeDistribution(db,"OpModeDistribution","linkID=" + linkID);
			// offset the opModeIDs to account for user-provided operating mode distribtuion inputs
			modelYearPhysics.offsetUserInputOpModeIDs(db);
		} catch(SQLException e) {
			Logger.logSqlError(e,"Could not calculate operating mode distributions", sql);
		}
	}

	/**
	 * Populate the RatesOpModeDistribution table with data from the OpModeDistribution
	 * table for a single link. RatesOpModeDistribution has data only by road type, not
	 * per link, and needs to be updated for each project link.
	 * @param linkID link to be used.
	 * @param roadTypeID road type of the link to be used.
	**/
	void populateRatesOpModeDistribution(int linkID, int roadTypeID) {
		String sql = "";
		try {
			// Cleanup previous data
			if(previousRoadTypeID > 0) {
				/**
				 * @step 200
				 * @algorithm Delete the previous road type's data from ratesOpModeDistribution.
				 * @output ratesOpModeDistribution
				 * @condition A previous road type has been used in the run.
				**/
				sql = "delete from ratesopmodedistribution where roadtypeid=" + previousRoadTypeID;
				SQLRunner.executeSQL(db,sql);
			}

			// Remove current data that might overlap

			/**
			 * @step 200
			 * @algorithm Delete the current road type's data from ratesOpModeDistribution.
			 * @output ratesOpModeDistribution
			**/
			sql = "delete from ratesopmodedistribution where roadtypeid=" + roadTypeID;
			SQLRunner.executeSQL(db,sql);

			// Copy opmode data to RatesOpModeDistribution

			/**
			 * @step 200
			 * @algorithm Lookup the current link's linkAvgSpeed.
			 * @input linkAvgSpeed
			**/
			sql = "select linkavgspeed from link where linkid=" + linkID;
			double averageSpeed = SQLRunner.executeScalar(db,sql);

			/**
			 * @step 200
			 * @algorithm Copy the current link's opModeDistribution entries to ratesOpModeDistribution,
			 * providing the current link's road type and average speed.
			 * Do not copy any generic polProcessID entries (polProcessID <= 0) as these are just to speedup
			 * internal opModeDistribution calculations.
			 * @input opModeDistribution
			 * @output ratesOpModeDistribution
			**/
			sql = "insert into ratesopmodedistribution (sourcetypeid, hourdayid, polprocessid, opmodeid, opmodefraction,"
					+ " 	roadtypeid, avgspeedbinid, avgbinspeed)"
					+ " select sourcetypeid, hourdayid, polprocessid, opmodeid, opmodefraction,"
					+ " 	" + roadTypeID + " as roadtypeid, "
					+ " 	0 as avgspeedbinid, "
					+ " 	" + averageSpeed + " as avgbinspeed"
					+ " from opmodedistribution omd"
					+ " inner join linksourcetypehour lsth"
					+ "		using (sourcetypeid,linkid)"
					+ " where polprocessid > 0" // don't copy the generic polprocess entries, these are just to speed up OMD population itself
					+ " and linkid = " + linkID;
			SQLRunner.executeSQL(db,sql);
			previousRoadTypeID = roadTypeID;
		} catch(SQLException e) {
			Logger.logSqlError(e,"Could not populate rates operating mode distributions", sql);
		}
	}

	/**
	 * @param linkID The link being processed.
	 * @param averageSpeed the average speed on the link
	**/
	void interpolateOpModeFractions(int linkID, double averageSpeed) throws SQLException {
		String sql = "";
		int roadTypeID = 0;
		double averageGrade = 0;
		SQLRunner.Query query = new SQLRunner.Query();
		try {
			/**
			 * @step 105
			 * @algorithm Lookup the road type and average grade for the current link.
			 * @input link
			 * @condition The current link has no drive schedule
			**/
			sql = "select roadtypeid from link where linkid=" + linkID;
			roadTypeID = (int)SQLRunner.executeScalar(db,sql);

			sql = "select ifnull(linkavggrade,0.0) from link where linkid=" + linkID;
			averageGrade = SQLRunner.executeScalar(db,sql);

			// Find brackets for each source type given the road type and average speed
			String[] statements = {
				"create table if not exists templinkbracket ("
						+ " 	linkid int not null,"
						+ " 	sourcetypeid smallint(6) not null,"
						+ " 	roadtypeid smallint(6) not null,"
						+ " 	drivescheduleidlow int null,"
						+ " 	drivescheduleidhigh int null,"
						+ "     isoutofboundslow int not null default 0,"
						+ "     isoutofboundshigh int not null default 0,"
						+ " 	unique key (linkid, sourcetypeid, roadtypeid),"
						+ " 	key (linkid, roadtypeid, sourcetypeid)"
						+ " )",

				/**
				 * @step 105
				 * @algorithm Find the drive schedule with the greatest average speed that is still <= the link's average speed.
				 * Use this as the lower bracketing schedule.
				 * Such a drive schedule may not exist.
				 * @output tempLinkBracket
				 * @input driveScheduleAssoc
				 * @input driveSchedule
				 * @condition The current link has no drive schedule
				**/
				"insert into templinkbracket (linkid, sourcetypeid, roadtypeid, drivescheduleidlow)"
						+ " select " + linkID + " as linkid,"
						+ " 	dsal2.sourcetypeid, dsal2.roadtypeid, max(dsal2.drivescheduleid) as drivescheduleidlow"
						+ " from drivescheduleassoc dsal2"
						+ " inner join driveschedule dsl2 using (drivescheduleid)"
						+ " where dsl2.averagespeed=("
						+ " 	select max(averagespeed)"
						+ " 	from drivescheduleassoc dsal"
						+ " 	inner join driveschedule dsl using (drivescheduleid)"
						+ " 	where dsl.averagespeed <= " + averageSpeed
						+ " 	and dsal.roadtypeid=" + roadTypeID
						+ " 	and dsal.sourcetypeid=dsal2.sourcetypeid"
						+ " 	and dsal.roadtypeid=dsal2.roadtypeid"
						+ " )"
						+ " group by dsal2.sourcetypeid, dsal2.roadtypeid"
						+ " order by null",

				// Do an insert ignore set to isOutOfBoundsLow=1

				/**
				 * @step 105
				 * @algorithm Find the drive schedule with the lowest average speed that is still > the link's average speed.
				 * Use this as the lower bracketing schedule if one was not previously found. Flag the bracket as out of bounds
				 * on the low side.
				 * Such a drive schedule may not exist.
				 * @output tempLinkBracket
				 * @input driveScheduleAssoc
				 * @input driveSchedule
				 * @condition The current link has no drive schedule
				**/
				"insert ignore into templinkbracket (linkid, sourcetypeid, roadtypeid, drivescheduleidlow, isoutofboundslow)"
						+ " select " + linkID + " as linkid,"
						+ " 	dsal2.sourcetypeid, dsal2.roadtypeid, max(dsal2.drivescheduleid) as drivescheduleidlow,"
						+ "     1 as isoutofboundslow"
						+ " from drivescheduleassoc dsal2"
						+ " inner join driveschedule dsl2 using (drivescheduleid)"
						+ " where dsl2.averagespeed=("
						+ " 	select min(averagespeed)"
						+ " 	from drivescheduleassoc dsal"
						+ " 	inner join driveschedule dsl using (drivescheduleid)"
						+ " 	where dsl.averagespeed > " + averageSpeed
						+ " 	and dsal.roadtypeid=" + roadTypeID
						+ " 	and dsal.sourcetypeid=dsal2.sourcetypeid"
						+ " 	and dsal.roadtypeid=dsal2.roadtypeid"
						+ " )"
						+ " group by dsal2.sourcetypeid, dsal2.roadtypeid"
						+ " order by null",

				"drop table if exists templinkbrackethigh",

				"create table if not exists templinkbrackethigh ("
						+ " 	linkid int not null,"
						+ " 	sourcetypeid smallint(6) not null,"
						+ " 	roadtypeid smallint(6) not null,"
						+ " 	drivescheduleidhigh int null,"
						+ "     isoutofboundshigh int not null default 0,"
						+ " 	unique key (linkid, sourcetypeid, roadtypeid),"
						+ " 	key (linkid, roadtypeid, sourcetypeid)"
						+ " )",

				/**
				 * @step 105
				 * @algorithm Find the drive schedule with the lowest average speed that is still >= the link's average speed.
				 * Use this as the upper bracketing schedule.
				 * Such a drive schedule may not exist.
				 * @output tempLinkBracketHigh
				 * @input driveScheduleAssoc
				 * @input driveSchedule
				 * @condition The current link has no drive schedule
				**/
				"insert into templinkbrackethigh (linkid, sourcetypeid, roadtypeid, drivescheduleidhigh)"
						+ " select " + linkID + " as linkid,"
						+ " 	dsal2.sourcetypeid, dsal2.roadtypeid, max(dsal2.drivescheduleid) as drivescheduleidhigh"
						+ " from drivescheduleassoc dsal2"
						+ " inner join driveschedule dsl2 using (drivescheduleid)"
						+ " where dsl2.averagespeed=("
						+ " 	select min(averagespeed)"
						+ " 	from drivescheduleassoc dsal"
						+ " 	inner join driveschedule dsl using (drivescheduleid)"
						+ " 	where dsl.averagespeed >= " + averageSpeed
						+ " 	and dsal.roadtypeid=" + roadTypeID
						+ " 	and dsal.sourcetypeid=dsal2.sourcetypeid"
						+ " 	and dsal.roadtypeid=dsal2.roadtypeid"
						+ " )"
						+ " group by dsal2.sourcetypeid, dsal2.roadtypeid"
						+ " order by null",

				// Do an insert ignore set to isOutOfBoundsHigh=1

				/**
				 * @step 105
				 * @algorithm Find the drive schedule with the highest average speed that is still < the link's average speed.
				 * Use this as the upper bracketing schedule if one was not previously found. Flag the bracket as out of bounds
				 * on the high side.
				 * Such a drive schedule may not exist.
				 * @output tempLinkBracketHigh
				 * @input driveScheduleAssoc
				 * @input driveSchedule
				 * @condition The current link has no drive schedule
				**/
				"insert ignore into templinkbrackethigh (linkid, sourcetypeid, roadtypeid, drivescheduleidhigh, isoutofboundshigh)"
						+ " select " + linkID + " as linkid,"
						+ " 	dsal2.sourcetypeid, dsal2.roadtypeid, max(dsal2.drivescheduleid) as drivescheduleidhigh,"
						+ "     1 as isoutofboundshigh"
						+ " from drivescheduleassoc dsal2"
						+ " inner join driveschedule dsl2 using (drivescheduleid)"
						+ " where dsl2.averagespeed=("
						+ " 	select max(averagespeed)"
						+ " 	from drivescheduleassoc dsal"
						+ " 	inner join driveschedule dsl using (drivescheduleid)"
						+ " 	where dsl.averagespeed < " + averageSpeed
						+ " 	and dsal.roadtypeid=" + roadTypeID
						+ " 	and dsal.sourcetypeid=dsal2.sourcetypeid"
						+ " 	and dsal.roadtypeid=dsal2.roadtypeid"
						+ " )"
						+ " group by dsal2.sourcetypeid, dsal2.roadtypeid"
						+ " order by null",

				/**
				 * @step 105
				 * @algorithm Note the upper bracket link and its out of bounds flag, if any.
				 * @output tempLinkBracket
				 * @input tempLinkBracketHigh
				 * @condition The current link has no drive schedule
				**/
				"update templinkbracket, templinkbrackethigh set templinkbracket.drivescheduleidhigh=templinkbrackethigh.drivescheduleidhigh,"
						+ " templinkbracket.isoutofboundshigh=templinkbrackethigh.isoutofboundshigh"
						+ " where templinkbracket.sourcetypeid=templinkbrackethigh.sourcetypeid"
						+ " and templinkbracket.roadtypeid=templinkbrackethigh.roadtypeid"
						+ " and templinkbracket.linkid=templinkbrackethigh.linkid"
						+ " and templinkbracket.linkid=" + linkID,

				"drop table if exists templinkbrackethigh"
			};
			for(int i=0;i<statements.length;i++) {
				sql = statements[i];
				SQLRunner.executeSQL(db,sql);
			}
			// Build operating mode distributions for each drive schedule, using its -driveScheduleID as the pseudo-linkID
			int priorDriveScheduleID = 0;
			String sourceTypes = "";
			/**
			 * @step 105
			 * @algorithm Get the set of distinct driveScheduleID and sourceTypeID combinations
			 * that require operating mode calculations.
			 * @input tempLinkBracket
			 * @output list of bracketing drive schedules that need operating mode distributions
			 * @condition The current link has no drive schedule
			**/
			sql = "select distinct drivescheduleidlow as drivescheduleid, sourcetypeid"
					+ " from templinkbracket"
					+ " where drivescheduleidlow is not null and drivescheduleidhigh is not null"
					+ " and linkid=" + linkID
					+ " union"
					+ " select distinct drivescheduleidhigh as drivescheduleid, sourcetypeid"
					+ " from templinkbracket"
					+ " where drivescheduleidlow is not null and drivescheduleidhigh is not null"
					+ " and linkid=" + linkID
					+ " order by drivescheduleid, sourcetypeid";
			query.open(db,sql);
			while(query.rs.next()) {
				int driveScheduleID = query.rs.getInt(1);
				int sourceTypeID = query.rs.getInt(2);
				if(driveScheduleID != priorDriveScheduleID) {
					if(priorDriveScheduleID != 0) {
						// Copy drive schedule into driveScheduleSecondLink using averageGrade
						sql = "delete from driveschedulesecondlink where linkid=" + -priorDriveScheduleID;
						SQLRunner.executeSQL(db,sql);

						/**
						 * @step 105
						 * @algorithm Copy each second of each bracketing drive schedule into driveScheduleSecondLink.
						 * Use the link's average grade as each entry's grade.
						 * Use -driveScheduleID as the linkID field in driveScheduleSecondLink, denoting it as a
						 * bracketing link.
						 * @input driveScheduleSecond
						 * @output driveScheduleSecondLink
						 * @condition The current link has no drive schedule
						**/
						sql = "insert into driveschedulesecondlink (linkid, secondid, speed, grade)"
								+ " select " + -priorDriveScheduleID + ", second, speed, " + averageGrade
								+ " from driveschedulesecond"
								+ " where drivescheduleid=" + priorDriveScheduleID;
						SQLRunner.executeSQL(db,sql);
						/**
						 * @step 105
						 * @algorithm Perform the steps to calculate a link's operating mode distribution from its drive schedule.
						 * Use -driveScheduleID as the linkID to calculate.
						 * @condition The current link has a drive schedule not just an average speed.
						**/
						calculateOpModeFractionsCore(-priorDriveScheduleID,sourceTypes);
						sourceTypes = "";
					}
					priorDriveScheduleID = driveScheduleID;
				}
				if(sourceTypes.length() > 0) {
					sourceTypes += "," + sourceTypeID;
				} else {
					sourceTypes = "" + sourceTypeID;
				}
			}
			query.close();
			if(priorDriveScheduleID != 0 && sourceTypes.length() > 0) {
				// Copy drive schedule into driveScheduleSecondLink using averageGrade
				sql = "delete from driveschedulesecondlink where linkid=" + -priorDriveScheduleID;
				SQLRunner.executeSQL(db,sql);
				sql = "insert into driveschedulesecondlink (linkid, secondid, speed, grade)"
						+ " select " + -priorDriveScheduleID + ", second, speed, " + averageGrade
						+ " from driveschedulesecond"
						+ " where drivescheduleid=" + priorDriveScheduleID;
				SQLRunner.executeSQL(db,sql);
				// Build the operating mode distribution
				calculateOpModeFractionsCore(-priorDriveScheduleID,sourceTypes);
				sourceTypes = "";
			}
			// Build the interpolated opModeDistribution for each source type given its bounding drive schedules
			ArrayList<Double> tempValues = new ArrayList<Double>();
			sql = "select sourcetypeid, drivescheduleidlow, dsl.averagespeed, drivescheduleidhigh, dsh.averagespeed,"
					+ " 	isoutofboundslow, isoutofboundshigh"
					+ " from templinkbracket"
					+ " inner join driveschedule dsl on (dsl.drivescheduleid=drivescheduleidlow)"
					+ " inner join driveschedule dsh on (dsh.drivescheduleid=drivescheduleidhigh)"
					+ " where linkid=" + linkID
					+ " order by sourcetypeid";
			query.open(db,sql);
			while(query.rs.next()) {
				int sourceTypeID = query.rs.getInt(1);
				tempValues.add(Double.valueOf(sourceTypeID));
				tempValues.add(Double.valueOf(query.rs.getInt(2)));
				tempValues.add(Double.valueOf(query.rs.getFloat(3)));
				tempValues.add(Double.valueOf(query.rs.getInt(4)));
				tempValues.add(Double.valueOf(query.rs.getFloat(5)));

				boolean isTooLow = query.rs.getInt(6) > 0;
				boolean isTooHigh = query.rs.getInt(7) > 0;
				if(isTooLow || isTooHigh) {
					String message = "Driving cycles for average speed " + averageSpeed
							+ " for sourcetype " + sourceTypeID
							+ " on roadtype " + roadTypeID
							+ " were not available.  MOVES results for this speed were extrapolated from the closest available driving cycles.";
					if(!outOfBoundMessagesGenerated.contains(message)) {
						outOfBoundMessagesGenerated.add(message);
						if(CompilationFlags.ALLOW_DRIVE_CYCLE_EXTRAPOLATION) {
							Logger.log(LogMessageCategory.WARNING,message);
						} else {
							Logger.log(LogMessageCategory.ERROR,message);
							MOVESEngine.terminalErrorFound();
						}
					}
				}
			}
			query.close();

			/**
			 * @step 105
			 * @algorithm Create expanded operating mode tables for Model Year physics effects.
			 * @condition The current link has a drive schedule not just an average speed.
			**/
			modelYearPhysics.createExpandedOperatingModesTable(db);

			for(int ti=0;ti<tempValues.size();ti+=5) {
				int sourceTypeID = (int)(tempValues.get(ti+0).doubleValue());
				int lowScheduleID = (int)(tempValues.get(ti+1).doubleValue());
				double lowAverageSpeed = tempValues.get(ti+2).doubleValue();
				int highScheduleID = (int)(tempValues.get(ti+3).doubleValue());
				double highAverageSpeed = tempValues.get(ti+4).doubleValue();

				/**
				 * @step 105
				 * @algorithm interpolationFactor = (link average speed - low bracket schedule average speed) / (high bracket schedule average speed - low bracket schedule average speed).
				 * @condition The current link has a drive schedule not just an average speed.
				**/
				double factor = 0.0;
				if(lowAverageSpeed < highAverageSpeed) {
					factor = (averageSpeed - lowAverageSpeed) / (highAverageSpeed - lowAverageSpeed);
				}

				String[] interpolateStatements = {
					"create table if not exists tempopmodedistribution like opmodedistribution",
					"create table if not exists tempopmodedistribution2 like opmodedistribution",
					
					"truncate tempopmodedistribution",
					"truncate tempopmodedistribution2",
					
					"insert into tempopmodedistribution2 (sourcetypeid, hourdayid, linkid, polprocessid, opmodeid, opmodefraction, isuserinput)"
						+ " select sourcetypeid, hourdayid, linkid, polprocessid, opmodeid, coalesce(opmodefraction, 0) as opmodefraction, 'N' as isuserinput"
						+ " from physicsoperatingmode pom"
						+ " join sourceusetypephysicsmapping sutpm"
						+ " on (realsourcetypeid = " + sourceTypeID + " and pom.opmodeid div 100 = sutpm.opmodeidoffset div 100)"
						+ " join (select distinct sourcetypeid,hourdayid,linkid,polprocessid from opmodedistribution" 
							+ " where linkid in (" + -lowScheduleID + "," + -highScheduleID + ") and sourcetypeid = " + sourceTypeID + ") lp"
						+ " left join opmodedistribution omd"
						+ " using (opmodeid,linkid,polprocessid,sourcetypeid,hourdayid)",
						
	
					"insert into tempopmodedistribution (sourcetypeid, hourdayid, linkid, polprocessid, opmodeid, opmodefraction, isuserinput)"
							+ " select omdlow.sourcetypeid, omdlow.hourdayid, " + linkID + " as linkid,"
							+ " 	omdlow.polprocessid, omdlow.opmodeid,"
							+ " 	(omdlow.opmodefraction+(omdhigh.opmodefraction-omdlow.opmodefraction)*" + factor + ") as opmodefraction,"
							+ " 	'N' as isuserinput"
							+ " from tempopmodedistribution2 omdlow"
							+ " inner join tempopmodedistribution2 omdhigh on ("
							+ " 	omdhigh.linkid= " + -highScheduleID
							+ " 	and omdhigh.sourcetypeid = omdlow.sourcetypeid"
							+ " 	and omdhigh.hourdayid = omdlow.hourdayid"
							+ " 	and omdhigh.polprocessid = omdlow.polprocessid"
							+ " 	and omdhigh.opmodeid = omdlow.opmodeid"
							+ " )"
							+ " where omdlow.sourcetypeid=" + sourceTypeID
							+ " and omdlow.linkid=" + -lowScheduleID,

					"delete from tempopmodedistribution where opmodefraction <= 0",

					/**
					 * @step 105
					 * @algorithm Interpolate the operating mode distribution from the low and high bracket's operating mode distribution.
					 * opModeFraction = opModeFraction[low]+(opModeFraction[high]-opModeFraction[low])*interpolationFactor.
					 * @output opModeDistribution
					 * @condition The current link has a drive schedule not just an average speed.
					**/
					"insert ignore into opmodedistribution (sourcetypeid, hourdayid, linkid, polprocessid, opmodeid, opmodefraction, isuserinput)"
							+ " select tomd.sourcetypeid, tomd.hourdayid, tomd.linkid, tomd.polprocessid, tomd.opmodeid, tomd.opmodefraction, tomd.isuserinput"
							+ " from tempopmodedistribution tomd"
							+ " left join tempexistingopmode eom on ("
							+ " 	eom.sourcetypeid=tomd.sourcetypeid"
							+ " 	and eom.linkid=tomd.linkid"
							+ " 	and eom.polprocessid=tomd.polprocessid)"
							+ " where eom.sourcetypeid is null"
							+ " and eom.polprocessid is null"
							+ " and eom.linkid is null"

				};
				for(int i=0;i<interpolateStatements.length;i++) {
					sql = interpolateStatements[i];
					//System.out.println(sql);
					SQLRunner.executeSQL(db,sql);
				}
				/*
				sql = " select count(*)"
						+ " from tempOpModeDistribution tomd"
						+ " left join tempExistingOpMode eom on ("
						+ " 	eom.sourceTypeID=tomd.sourceTypeID"
						+ " 	and eom.linkID=tomd.linkID"
						+ " 	and eom.polProcessID=tomd.polProcessID)"
						+ " where eom.sourceTypeID is null"
						+ " and eom.polProcessID is null"
						+ " and eom.linkID is null";
				int countAvailable = (int)SQLRunner.executeScalar(db,sql);
				System.out.println("***** countAvailable=" + countAvailable + " for linkID=" + linkID);
				*/
			}
			//System.out.println("***** Built opModeDistribution for linkID=" + linkID);
			// Remove temporary data from driveScheduleSecondLink

			/**
			 * @step 105
			 * @algorithm Remove bracketing schedules from driveScheduleSecondLink.
			 * @input tempLinkBracket
			 * @output driveScheduleSecondLink
			 * @condition The current link has a drive schedule not just an average speed.
			**/
			sql = "select distinct drivescheduleidlow as drivescheduleid"
					+ " from templinkbracket"
					+ " where drivescheduleidlow is not null and drivescheduleidhigh is not null"
					+ " and linkid=" + linkID
					+ " union"
					+ " select distinct drivescheduleidhigh as drivescheduleid"
					+ " from templinkbracket"
					+ " where drivescheduleidlow is not null and drivescheduleidhigh is not null"
					+ " and linkid=" + linkID
					+ " order by drivescheduleid";
			query.open(db,sql);
			while(query.rs.next()) {
				int driveScheduleID = query.rs.getInt(1);
				sql = "delete from driveschedulesecondlink where linkid=" + -driveScheduleID;
				SQLRunner.executeSQL(db,sql);
			}
			query.close();
		} catch(SQLException e) {
			Logger.logSqlError(e,"Could not interpolate operating mode distributions", sql);
			throw e;
		} finally {
			query.onFinally();
			String[] statements = {
				"drop table if exists templinkbrackethigh"
				//,"drop table if exists tempLinkBracket" Leave tempLinkBracket in MOVESExecution for debugging
			};
			for(int i=0;i<statements.length;i++) {
				try {
					sql = statements[i];
					SQLRunner.executeSQL(db,sql);
				} catch(Exception e) {
					// Nothing to do here
				}
			}
		}
	}

	/**
	 * Core routine for calculating operating mode distribution for a link based on its
	 * drive schedule.  This routine may be called multiple times while establishing the
	 * distributions bracketing a link.
	 * <p>The overall operating mode fractions are calculated by weighting the operating mode
	 * fractions of each drive schedule by the drive schedule fractions. This is done for each
	 * source type, road type, day of the week, hour of the day and operating mode.</p>
	 * @param linkID The link being processed.  Bracketing links are given negative number IDs.
	 * @param sourceTypes comma-separated list of source types to be used.  If null or empty,
	 * all source types in the RunSpec will be used.
	**/
	void calculateOpModeFractionsCore(int linkID, String sourceTypes) throws SQLException {
		String sql = "";
		try {
			// Update the link's average speed and grade to be consistent with the drive schedule for
			// real links.  Bracketing links are given negative number IDs.
			if(linkID > 0) {
				/**
				 * @step 110
				 * @algorithm linkAvgSpeed=average(speed).
				 * linkAvgGrade=average(grade).
				 * @input driveScheduleSecondLink
				 * @output link
				 * @condition Calculate a link's operating mode distribution from its drive schedule.
				 * @condition A real linkID is provided (linkID > 0)
				**/
				sql = "select avg(speed) from driveschedulesecondlink where linkid=" + linkID;
				double averageSpeed = SQLRunner.executeScalar(db,sql);
				sql = "update link set linkavgspeed=" + averageSpeed
						+ " where linkid=" + linkID;
						//+ " and (linkAvgSpeed is null or linkAvgSpeed <= 0)";
				SQLRunner.executeSQL(db,sql);

				sql = "select avg(grade) from driveschedulesecondlink where linkid=" + linkID;
				double averageGrade = SQLRunner.executeScalar(db,sql);
				sql = "update link set linkavggrade=" + averageGrade
						+ " where linkid=" + linkID;
						//+ " and linkAvgGrade is null";
				SQLRunner.executeSQL(db,sql);
			}
		} catch (SQLException e) {
			Logger.logSqlError(e,"Could not check driveScheduleSecondLink", sql);
			return;
		}
		if(opModeAssignmentSQL == null) {
			opModeAssignmentSQL = buildOpModeClause();
		}
		String sourceTypeIDClause = "";
		if(sourceTypes != null && sourceTypes.trim().length() > 0) {
			sourceTypeIDClause = " and rst.sourcetypeid in (" + sourceTypes + ")";
		}
		String[] statements = {
			"drop table if exists tempdriveschedulesecondlink",

			// Accelerations are in units of miles/(hour*sec)
			// Speeds is miles/hour
			// VSP is kW/tonne
			// 0.44704 (meter*hour)/(miles*sec)

			/**
			 * @step 110
			 * @algorithm Calculate accelerations in units of miles/(hour*second).
			 * The acceleration of the 1st second = the acceleration of the 2nd second.
			 * Acceleration includes the effect of gravity due to grade.
			 * Speeds are given in miles/hour.
			 * VSP is kW/tonne.
			 * There are 0.44704 (meter*hours)/(miles*second).
			 * at0 = coalesce(
			 * (speed[t]-speed[t-1])+(9.81/0.44704*sin(atan(grade[t]/100.0))),
			 * (speed[t+1]-speed[t])+(9.81/0.44704*sin(atan(grade[t]/100.0))),
			 * 0.0).
			 * at1 = coalesce((speed[t-1]-speed[t-2])+(9.81/0.44704*sin(atan(grade[t-1]/100.0))),0.0).
			 * at2 = coalesce((speed[t-2]-speed[t-3])+(9.81/0.44704*sin(atan(grade[t-2]/100.0))),0.0).
			 * VSP = (((speed[t]*0.44704)*(rollingTermA+(speed[t]*0.44704)*(rotatingTermB+dragTermC*(speed[t]*0.44704)))
			 * +sourceMass*(speed[t]*0.44704)*coalesce(speed[t]-speed[t-1],speed[t+1]-speed[t],0.0)*0.44704
			 * +sourceMass*9.81*sin(atan(grade[t]/100.0))*(speed[t]*0.44704)))/fixedMassFactor.
			 * @input driveScheduleSecondLink at time t
			 * @input driveScheduleSecondLink at time t-1 seconds
			 * @input driveScheduleSecondLink at time t-2 seconds
			 * @input driveScheduleSecondLink at time t-3 seconds
			 * @input driveScheduleSecondLink at time t+1 seconds
			 * @output tempDriveScheduleSecondLink
			 * @condition Calculate a link's operating mode distribution from its drive schedule.
			**/
			"create table tempdriveschedulesecondlink"
					+ " select "
					+ " sut.tempsourcetypeid as sourcetypeid, "
					+ "     a.linkid, a.secondid, a.speed,"
					+ " 	coalesce("
					+ "			(a.speed-b.speed)+(9.81/0.44704*sin(atan(a.grade/100.0))),"
					+ "			0.0) as at0,"
					+ "		coalesce((b.speed-c.speed)+(9.81/0.44704*sin(atan(b.grade/100.0))),0.0) as at1,"
					+ "		coalesce((c.speed-d.speed)+(9.81/0.44704*sin(atan(c.grade/100.0))),0.0) as at2,"
				 	+ " 	(((a.speed*0.44704)*(rollingterma+(a.speed*0.44704)*(rotatingtermb+dragtermc*(a.speed*0.44704)))"
				 	+ "		+sourcemass*(a.speed*0.44704)*coalesce(a.speed-b.speed,0.0)*0.44704"
			 		+ " 	+sourcemass*9.81*sin(atan(a.grade/100.0))*(a.speed*0.44704)))/fixedmassfactor as vsp,"
					+ " 	-1 as opmodeid"
					+ " from driveschedulesecondlink a"
					+ " left join driveschedulesecondlink b on (b.linkid=a.linkid and b.secondid=a.secondid-1)"
					+ " left join driveschedulesecondlink c on (c.linkid=b.linkid and c.secondid=b.secondid-1)"
					+ " left join driveschedulesecondlink d on (d.linkid=c.linkid and d.secondid=c.secondid-1)"
					+ ","
					+ " sourceusetypephysicsmapping sut"
					+ " inner join runspecsourcetype rst on (rst.sourcetypeid = sut.realsourcetypeid)"
					+ " where a.linkid=" + linkID
					+ sourceTypeIDClause,

			/**
			 * @step 110
			 * @algorithm Assign operating modes.
			 * Assign a stopped mode 501 when speed = 0, assign mode 501. This mode will be converted to 1 based on polProcessID later.
			 * Otherwise, assign the idle mode 1 when speed < 1.
			 * Otherwise, assign the braking mode 0 when At0 <= -2 or (At0 < -1 and At1 < -1 and At2 < -1).
			 * Otherwise, assign operating modes by their data-drive speed and VSP ranges.
			 * @output tempDriveScheduleSecondLink
			 * @condition Calculate a link's operating mode distribution from its drive schedule.
			**/
			"update tempdriveschedulesecondlink set opmodeid= case"
					+ " when (speed = 0) then 501" // force special bin for stopped, will be converted to bin 1 based on polprocessid later
					+ " when (speed < 1) then 1" // force idle if speed < 1, just like omdg and mesoscaleomdg
					+ "	when (at0 <= -2 or (at0 < -1 and at1 < -1 and at2 < -1)) then 0" // braking
					+ " " + opModeAssignmentSQL
					+ "	else -1"
					+ " end",

			"drop table if exists tempdriveschedulesecondlinktotal",

			/**
			 * @step 110
			 * @algorithm secondTotal = Count of the number of entries in the drive schedule. This is the number
			 * of seconds of driving, even if there are gaps within the data.
			 * @output tempDriveScheduleSecondLinkTotal
			 * @input tempDriveScheduleSecondLink
			 * @condition Calculate a link's operating mode distribution from its drive schedule.
			**/
			"create table tempdriveschedulesecondlinktotal"
					+ " select sourcetypeid, linkid, count(*) as secondtotal"
					+ " from tempdriveschedulesecondlink"
					+ " group by sourcetypeid, linkid"
					+ " order by null",

			"drop table if exists tempdriveschedulesecondlinkcount",

			/**
			 * @step 110
			 * @algorithm secondCount = Count of the number of entries in the drive schedule for each operating mode.
			 * @output tempDriveScheduleSecondLinkCount
			 * @input tempDriveScheduleSecondLink
			 * @condition Calculate a link's operating mode distribution from its drive schedule.
			**/
			"create table tempdriveschedulesecondlinkcount"
					+ " select sourcetypeid, linkid, opmodeid, count(*) as secondcount"
					+ " from tempdriveschedulesecondlink"
					+ " group by sourcetypeid, linkid, opmodeid"
					+ " order by null",

			"drop table if exists tempdriveschedulesecondlinkfraction",

			/**
			 * @step 110
			 * @algorithm opModeFraction = secondCount/secondTotal.
			 * @output tempDriveScheduleSecondLinkFraction
			 * @input tempDriveScheduleSecondLinkCount
			 * @input tempDriveScheduleSecondLinkTotal
			 * @condition Calculate a link's operating mode distribution from its drive schedule.
			**/
			"create table tempdriveschedulesecondlinkfraction"
					+ " select sourcetypeid, linkid, opmodeid, (secondcount*1.0/secondtotal) as opmodefraction"
					+ " from tempdriveschedulesecondlinkcount sc"
					+ " inner join tempdriveschedulesecondlinktotal st using (sourcetypeid, linkid)",

			"drop table if exists opmodedistributiontemp",

			"CREATE TABLE opmodedistributiontemp ("
					+ "   sourcetypeid smallint(6) not null default '0',"
					+ "   hourdayid smallint(6) not null default '0',"
					+ "   linkid int(11) not null default '0',"
					+ "   polprocessid int not null default '0',"
					+ "   opmodeid smallint(6) not null default '0',"
					+ "   opmodefraction float default null,"
					+ "   opmodefractioncv float default null,"
					+ "   isuserinput char(1) not null default 'N',"
					+ "   key allcolumns (hourdayid,linkid,opmodeid,polprocessid,sourcetypeid),"
					+ "   key sourcetypeid (sourcetypeid),"
					+ "   key hourdayid (hourdayid),"
					+ "   key linkid (linkid),"
					+ "   key polprocessid (polprocessid),"
					+ "   key opmodeid (opmodeid)"
					+ " )",

			/**
			 * @step 110
			 * @algorithm Copy the operating mode distribution to all polProcessIDs that share it.
			 * For polProcessID 11609, retain opMode 501 and convert opMode 501 to opMode 1 for all
			 * other polProcessIDs.
			 * @output opModeDistributionTemp
			 * @input tempDriveScheduleSecondLinkFraction
			 * @input opModePolProcAssoc
			 * @condition Calculate a link's operating mode distribution from its drive schedule.
			**/
			"insert into opmodedistributiontemp (sourcetypeid, hourdayid, linkid, polprocessid, opmodeid, opmodefraction)"
					+ " select lf.sourcetypeid, hourdayid, lf.linkid, omppa.polprocessid, "
					+ " if(lf.opmodeid=501,if(omppa.polprocessid=11609,501,1),lf.opmodeid) as opmodeid,"
					+ " opmodefraction"
					+ " from tempdriveschedulesecondlinkfraction lf"
					+ " inner join opmodepolprocassoc omppa using (opmodeid)"
					+ " left join tempexistingopmode eom on ("
					+ "		eom.sourcetypeid=lf.sourcetypeid"
					+ "		and eom.linkid=lf.linkid"
					+ "		and eom.polprocessid=omppa.polprocessid),"
					+ " runspechourday rshd"
					+ " where eom.sourcetypeid is null"
					+ " and eom.polprocessid is null"
					+ " and eom.linkid is null",

			/**
			 * @step 110
			 * @algorithm opModeFraction=sum(temporary opModeFraction).
			 * other polProcessIDs.
			 * @output opModeDistribution
			 * @output opModeDistributionTemp
			 * @condition Calculate a link's operating mode distribution from its drive schedule.
			**/
			"insert ignore into opmodedistribution (sourcetypeid, hourdayid, linkid, polprocessid, opmodeid, opmodefraction)"
					+ " select sourcetypeid, hourdayid, linkid, polprocessid, opmodeid, sum(opmodefraction)"
					+ " from opmodedistributiontemp"
					+ " group by hourdayid, linkid, opmodeid, polprocessid, sourcetypeid"
			/*
			"insert ignore into opModeDistribution (sourceTypeID, hourDayID, linkID, polProcessID, opModeID, opModeFraction)"
					+ " select lf.sourceTypeID, hourDayID, lf.linkID, omppa.polProcessID, lf.opModeID, opModeFraction"
					+ " from tempDriveScheduleSecondLinkFraction lf"
					+ " inner join opModePolProcAssoc omppa using (opModeID)"
					+ " left join tempExistingOpMode eom on ("
					+ "		eom.sourceTypeID=lf.sourceTypeID"
					+ "		and eom.linkID=lf.linkID"
					+ "		and eom.polProcessID=omppa.polProcessID),"
					+ " RunSpecHourDay rshd"
					+ " where eom.sourceTypeID is null"
					+ " and eom.polProcessID is null"
					+ " and eom.linkID is null"
			*/
		};
		try {
			if(linkID < 0) {
				/**
				 * @step 110
				 * @algorithm Remove data for bracketing links first, just in case they were left from a prior link's settings.
				 * @output opModeDistribution
				 * @condition Calculate a link's operating mode distribution from its drive schedule.
				 * @condition A bracketing link is provided (linkID < 0)
				**/
				sql = "delete from opmodedistribution where linkid=" + linkID;
				SQLRunner.executeSQL(db,sql);
			}
			for(int i=0;i<statements.length;i++) {
				sql = statements[i];
				if(sql != null && sql.length() > 0) {
					SQLRunner.executeSQL(db,sql);
				}
			}
			//System.out.println("**** Built opModes from drive schedule for link " + linkID);
			/**
			 * @step 110
			 * @algorithm Update OpModeDistribution for Source Type Physics effects.
			 * @output opModeDistribution
			 * @condition Calculate a link's operating mode distribution from its drive schedule.
			**/
			modelYearPhysics.updateOperatingModeDistribution(db,"opmodedistribution", "linkid=" + linkID);
		} catch(SQLException e) {
			Logger.logSqlError(e,"Could not calculate operating mode distributions", sql);
			throw e;
		}
	}
}
