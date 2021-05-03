/**************************************************************************************************
 * @(#)MesoscaleLookupOperatingModeDistributionGenerator.java
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
 * This builds "operating mode distribution" records for ELDB data.
 * ELDB is the Execution Location Database explained in TotalActivityGenerator
 *
 * @author		Wesley Faler
 * @author		W. Aikman
 * @version		2014-05-28
**/
public class MesoscaleLookupOperatingModeDistributionGenerator extends Generator {
	/** @notused **/

	/** Flags for tasks already done, used to prevent duplicate execution **/
	TreeSet<String> alreadyDoneFlags = new TreeSet<String>();
	/** Flag for whether the data tables have been cleared/setup **/
	boolean hasBeenSetup = false;
	/** Flag to validate the data before determining drive schedule distribution **/
	boolean isValid = true;
	/** Database connection used by all functions.  Setup by executeLoop and cleanDataLoop. **/
	Connection db;
	/** milliseconds spent during one time operations **/
	long setupTime = 0;
	/** milliseconds spent during non-one-time operations **/
	long totalTime = 0;
	/** comma-separated list of polProcessIDs used by this generator **/
	String polProcessIDs = "";
	/** Model-year specific rolling and drag terms **/
	SourceTypePhysics modelYearPhysics = new SourceTypePhysics();

	/** Default constructor **/
	public MesoscaleLookupOperatingModeDistributionGenerator() {
	}

	/**
	 * Requests that this object subscribe to the given loop at desired looping points.
	 * Objects can assume that all necessary MasterLoopable objects have been instantiated.
	 *
	 * @param targetLoop The loop to subscribe to.
	**/
	public void subscribeToMe(MasterLoop targetLoop) {
		EmissionProcess process = EmissionProcess.findByName("running exhaust");

		targetLoop.subscribe(this, process, MasterLoopGranularity.YEAR, // LINK. Year level for source bins from SBDG.
				MasterLoopPriority.GENERATOR);

		EmissionProcess brakeProcess = EmissionProcess.findByName("brakewear");
		targetLoop.subscribe(this, brakeProcess, MasterLoopGranularity.YEAR, // LINK. Year level for source bins from SBDG.
				MasterLoopPriority.GENERATOR);
	}

	/**
	 * Called each time the link changes.
	 *
	 * @param inContext The current context of the loop.
	**/
	public void executeLoop(MasterLoopContext inContext) {
		try {
			db = DatabaseConnectionManager.checkOutConnection(MOVESDatabaseType.EXECUTION);

			long start;

			// The following only has to be done once for each run.
			if(!hasBeenSetup) {
				start = System.currentTimeMillis();
				modelYearPhysics.setup(db);
				bracketAverageSpeedBins();
				determineDriveScheduleProportions();
				if(!validateDriveScheduleDistribution()) {
					isValid = false;
				}
				if(isValid) {
					determineDriveScheduleDistributionNonRamp();
					calculateEnginePowerBySecond();
					determineOpModeIDPerSecond();
					calculateOpModeFractionsPerDriveSchedule();
					preliminaryCalculateOpModeFractions();
					hasBeenSetup = true;
				}
				setupTime += System.currentTimeMillis() - start;
			}

			start = System.currentTimeMillis();
			if(isValid) {
				String alreadyKey = "calc|" + incontext.iterprocess.databasekey + "|" + inContext.iterLocation.linkRecordID;
				if(!alreadyDoneFlags.contains(alreadyKey)) {
					alreadyDoneFlags.add(alreadyKey);
					calculateOpModeFractions(inContext.iterLocation.linkRecordID);
				}
				alreadyKey = "rates|" + inContext.iterProcess.databaseKey;
				if(!alreadyDoneFlags.contains(alreadyKey)) {
					alreadyDoneFlags.add(alreadyKey);
					modelYearPhysics.updateEmissionRateTables(db,inContext.iterProcess.databaseKey);
				}
			} else {
				Logger.log(LogMessageCategory.ERROR, "error while validating drive schedule "
						+ "distribution, operating mode computation cannot continue");
			}
			totalTime += System.currentTimeMillis() - start;
		} catch (Exception e) {
			Logger.logError(e,"operating mode distribution generation failed.");
		} finally {
			DatabaseConnectionManager.checkInConnection(MOVESDatabaseType.EXECUTION, db);
			db = null;
		}

		Logger.log(LogMessageCategory.INFO,"meslkomdg setuptime=" + setuptime + " totaltime=" + totalTime);
	}

	/**
	 * Removes data from the execution database that was created by this object within executeLoop
	 * for the same context. This is only called after all other loopable objects that might use
	 * data created by executeLoop have had their executeLoop and cleanDataLoop functions called.
	 * @param context The MasterLoopContext that applies to this execution.
	**/
	public void cleanDataLoop(MasterLoopContext context) {
		// Do not remove data since it is needed across multiple processes
		// (Running Exhaust and Brakewear).
		/*
		String sql = "";
		try {
			if(polProcessIDs.length() > 0) {
				db = DatabaseConnectionManager.checkOutConnection(MOVESDatabaseType.EXECUTION);

				sql = "delete from opmodedistribution where isuserinput='n' and linkid = "
						+ context.iterLocation.linkRecordID
						+ " and polprocessid in (" + polprocessids + ")";
				//System.out.println("########## deleting meslkopd ###### : " +
				//		context.iterLocation.linkRecordID);
				SQLRunner.executeSQL(db, sql);
			}
		} catch(Exception e) {
			Logger.logSqlError(e,"unable to delete operating mode distribution data from a previous"
					+ " run", sql);
		} finally {
			if(db != null) {
				DatabaseConnectionManager.checkInConnection(MOVESDatabaseType.EXECUTION,db);
				db=null;
			}
		}
		*/
	}

	/**
	 * OMDG-1: Determine the drive schedules that bracket each Average Speed Bin value.
	 * <p>Each average speed bin lies between (is bracketed) by the average speeds of two drive
	 * schedules. Determine which two drive schedules bracket the average speed bin and store the
	 * identity and average speeds of the two bins.  This is done for each source type, roadway
	 * type, day of week and hour of day for each average speed bin.</p>
	**/
	void bracketAverageSpeedBins() {
		String sql = "";

		ResultSet rs = null;
		try {
			// The documentation doesn't mention this but, going from the spreadsheet, speed bins
			// with values below and above the lowest and highest drive schedule values are bound
			// to those values. The following query determines these bounded values.
			sql = "create table if not exists driveschedulebounds ("+
						"sourcetypeid     smallint,"+
						"roadtypeid       smallint,"+
						"scheduleboundlo  float,"+
						"scheduleboundhi  float,"+
						"unique index xpkdriveschedulebounds ("+
							"sourcetypeid, roadtypeid))";
			SQLRunner.executeSQL(db, sql);

			sql = "truncate driveschedulebounds";
			SQLRunner.executeSQL(db, sql);

			sql = "insert into driveschedulebounds ("+
						"sourcetypeid,"+
						"roadtypeid,"+
						"scheduleboundlo,"+
						"scheduleboundhi) "+
					"select "+
						"dsa.sourcetypeid,"+
						"dsa.roadtypeid,"+
						"min(ds.averagespeed),"+
						"max(ds.averagespeed) "+
					"from "+
						"runspecroadtype rsrt,"+
						"runspecsourcetype rsst,"+
						"driveschedule ds,"+
						"drivescheduleassoc dsa "+
					"where "+
						"rsrt.roadtypeid = dsa.roadtypeid and "+
						"rsst.sourcetypeid = dsa.sourcetypeid and "+
						"ds.drivescheduleid = dsa.drivescheduleid "+
					"group by "+
						"dsa.sourcetypeid,"+
						"dsa.roadtypeid";
			SQLRunner.executeSQL(db, sql);

			SQLRunner.executeSQL(db,"analyze table driveschedulebounds");

			sql = "create table if not exists bracketschedulelo2 ("+
						"sourcetypeid     smallint,"+
						"roadtypeid       smallint,"+
						"avgspeedbinid    smallint,"+
						"drivescheduleid  smallint,"+
						"loschedulespeed  float,"+
						"unique index xpkbracketschedulelo2 ("+
							"sourcetypeid, roadtypeid, avgspeedbinid, drivescheduleid))";
			SQLRunner.executeSQL(db, sql);

			sql = "truncate bracketschedulelo2";
			SQLRunner.executeSQL(db, sql);

			sql = "insert into bracketschedulelo2 ("+
						"sourcetypeid,"+
						"roadtypeid,"+
						"avgspeedbinid,"+
						"loschedulespeed) "+
					"select "+
						"dsa.sourcetypeid,"+
						"dsa.roadtypeid,"+
						"asb.avgspeedbinid,"+
						"max(ds.averagespeed) "+
					"from "+
						"runspecroadtype rsrt,"+
						"runspecsourcetype rsst,"+
						"driveschedule ds,"+
						"drivescheduleassoc dsa,"+
						"avgspeedbin asb "+
					"where "+
						"rsrt.roadtypeid = dsa.roadtypeid and "+
						"rsst.sourcetypeid = dsa.sourcetypeid and "+
						"ds.drivescheduleid = dsa.drivescheduleid and "+
						"ds.averagespeed <= asb.avgbinspeed "+
					"group by "+
						"dsa.sourcetypeid,"+
						"dsa.roadtypeid,"+
						"asb.avgbinspeed";
			SQLRunner.executeSQL(db, sql);

			SQLRunner.executeSQL(db,"analyze table bracketschedulelo2");
				// changed to INSERT IGNORE to work properly with MySQL 4
			sql = "insert ignore into bracketschedulelo2 ("+
						"sourcetypeid,"+
						"roadtypeid,"+
						"avgspeedbinid,"+
						"loschedulespeed) "+
					"select "+
						"dsa.sourcetypeid,"+
						"dsa.roadtypeid,"+
						"asb.avgspeedbinid,"+
						"dsb.scheduleboundlo "+
					"from "+
						"runspecroadtype rsrt,"+
						"runspecsourcetype rsst,"+
						"driveschedule ds,"+
						"drivescheduleassoc dsa,"+
						"driveschedulebounds dsb,"+
						"avgspeedbin asb "+
					"where "+
						"rsrt.roadtypeid = dsa.roadtypeid and "+
						"rsst.sourcetypeid = dsa.sourcetypeid and "+
						"ds.drivescheduleid = dsa.drivescheduleid and "+
						"dsb.sourcetypeid = dsa.sourcetypeid and "+
						"dsb.roadtypeid = dsa.roadtypeid and "+
						"asb.avgbinspeed < dsb.scheduleboundlo";
			SQLRunner.executeSQL(db, sql);

			sql = "create table if not exists bracketschedulelo ("+
					"sourcetypeid    smallint,"+
					"roadtypeid      smallint,"+
					"avgspeedbinid   smallint,"+
					"drivescheduleid smallint,"+
					"loschedulespeed float,"+
					"unique index xpkbracketschedulelo ("+
							"sourcetypeid, roadtypeid, avgspeedbinid, drivescheduleid))";
			SQLRunner.executeSQL(db, sql);

			sql = "truncate bracketschedulelo";
			SQLRunner.executeSQL(db, sql);
			// changed to INSERT IGNORE to work with MySQL 4
			sql = "insert ignore into bracketschedulelo ("+
						"sourcetypeid,"+
						"roadtypeid,"+
						"avgspeedbinid,"+
						"drivescheduleid,"+
						"loschedulespeed) "+
					"select "+
						"bsl.sourcetypeid,"+
						"bsl.roadtypeid,"+
						"bsl.avgspeedbinid,"+
						"ds.drivescheduleid,"+
						"bsl.loschedulespeed "+
					"from "+
						"bracketschedulelo2 bsl,"+
						"drivescheduleassoc dsa,"+
						"driveschedule ds "+
					"where "+
						"dsa.drivescheduleid = ds.drivescheduleid and "+
						"dsa.sourcetypeid = bsl.sourcetypeid and "+
						"dsa.roadtypeid = bsl.roadtypeid and "+
						"ds.averagespeed = bsl.loschedulespeed";
			SQLRunner.executeSQL(db, sql);

			SQLRunner.executeSQL(db,"analyze table bracketschedulelo");

			//
			// Delete intermediate results for large tables. Normally, intermediate
			// results are kept when possible for debugging purposes.
			sql = "truncate bracketschedulelo2";
			SQLRunner.executeSQL(db, sql);

			sql = "create table if not exists bracketschedulehi2 ("+
					"sourcetypeid      smallint,"+
					"roadtypeid        smallint,"+
					"avgspeedbinid     smallint,"+
					"drivescheduleid   smallint,"+
					"hischedulespeed   float,"+
					"unique index xpkbracketschedulehi2 ("+
							"sourcetypeid, roadtypeid, avgspeedbinid, drivescheduleid))";
			SQLRunner.executeSQL(db, sql);

			sql = "truncate bracketschedulehi2";
			SQLRunner.executeSQL(db, sql);

			sql = "insert into bracketschedulehi2 ("+
						"sourcetypeid,"+
						"roadtypeid,"+
						"avgspeedbinid,"+
						"hischedulespeed) "+
					"select "+
						"dsa.sourcetypeid,"+
						"dsa.roadtypeid,"+
						"asb.avgspeedbinid,"+
						"min(ds.averagespeed) "+
					"from "+
						"runspecroadtype rsrt,"+
						"runspecsourcetype rsst,"+
						"driveschedule ds,"+
						"drivescheduleassoc dsa,"+
						"avgspeedbin asb "+
					"where "+
						"rsrt.roadtypeid = dsa.roadtypeid and "+
						"rsst.sourcetypeid = dsa.sourcetypeid and "+
						"ds.drivescheduleid = dsa.drivescheduleid and "+
						"ds.averagespeed > asb.avgbinspeed "+
					"group by "+
						"dsa.sourcetypeid,"+
						"dsa.roadtypeid,"+
						"asb.avgbinspeed";
			SQLRunner.executeSQL(db, sql);

			SQLRunner.executeSQL(db,"analyze table bracketschedulehi2");
				// changed to INSERT IGNORE to work with MySQL 4.0
			sql = "insert ignore into bracketschedulehi2 ("+
						"sourcetypeid,"+
						"roadtypeid,"+
						"avgspeedbinid,"+
						"hischedulespeed) "+
					"select "+
						"dsa.sourcetypeid,"+
						"dsa.roadtypeid,"+
						"asb.avgspeedbinid,"+
						"dsb.scheduleboundhi "+
					"from "+
						"runspecroadtype rsrt,"+
						"runspecsourcetype rsst,"+
						"driveschedule ds,"+
						"drivescheduleassoc dsa,"+
						"driveschedulebounds dsb,"+
						"avgspeedbin asb "+
					"where "+
						"rsrt.roadtypeid = dsa.roadtypeid and "+
						"rsst.sourcetypeid = dsa.sourcetypeid and "+
						"ds.drivescheduleid = dsa.drivescheduleid and "+
						"dsb.sourcetypeid = dsa.sourcetypeid and "+
						"dsb.roadtypeid = dsa.roadtypeid and "+
						"asb.avgbinspeed > dsb.scheduleboundhi";
			SQLRunner.executeSQL(db, sql);

			SQLRunner.executeSQL(db,"analyze table bracketschedulehi2");

			sql = "create table if not exists bracketschedulehi ("+
					"sourcetypeid      smallint,"+
					"roadtypeid        smallint,"+
					"avgspeedbinid     smallint,"+
					"drivescheduleid   smallint,"+
					"hischedulespeed   float,"+
					"unique index xpkbracketschedulehi ("+
							"sourcetypeid, roadtypeid, avgspeedbinid, drivescheduleid))";
			SQLRunner.executeSQL(db, sql);

			sql = "truncate bracketschedulehi";
			SQLRunner.executeSQL(db, sql);
			// changed to INSERT IGNORE to work with MySQL 4.
			sql = "insert ignore into bracketschedulehi ("+
						"sourcetypeid,"+
						"roadtypeid,"+
						"avgspeedbinid,"+
						"drivescheduleid,"+
						"hischedulespeed) "+
					"select "+
						"bsl.sourcetypeid,"+
						"bsl.roadtypeid,"+
						"bsl.avgspeedbinid,"+
						"ds.drivescheduleid,"+
						"bsl.hischedulespeed "+
					"from "+
						"bracketschedulehi2 bsl,"+
						"drivescheduleassoc dsa,"+
						"driveschedule ds "+
					"where "+
						"dsa.drivescheduleid = ds.drivescheduleid and "+
						"dsa.sourcetypeid = bsl.sourcetypeid and "+
						"dsa.roadtypeid = bsl.roadtypeid and "+
						"ds.averagespeed = bsl.hischedulespeed";
			SQLRunner.executeSQL(db, sql);

			SQLRunner.executeSQL(db,"analyze table bracketschedulehi");

			//
			// Delete intermediate results for potentially large tables. Normally, intermediate
			// results are kept when possible for debugging purposes.
			sql = "truncate bracketschedulehi2";
			SQLRunner.executeSQL(db, sql);

		} catch (SQLException e) {
			Logger.logSqlError(e,"could not determine brackets for average speed bins.", sql);
		} finally {
			if(rs != null) {
				try {
					rs.close();
				} catch (SQLException e) {
					// Failure to close on a preparedStatment should not be an issue.
				}
				rs = null;
			}
		}
	}

	/**
	 * OMDG-2: Determine proportions for bracketing drive schedules.
	 * <p>This step determines the proportion of each of the bracketing drive schedules such that
	 * the combination of the average speeds of drive schedules equals the nominal average speed
	 * of each average speed bin. The results are then weighted by the fraction of all operating
	 * time that are represented by the time spent in that average speed bin. This is done for each
	 * source type, roadway type, day of week and hour of day.</p>
	**/
	void determineDriveScheduleProportions() {
		String[] statements = {
			"create table if not exists loschedulefraction ("+
					"sourcetypeid       smallint,"+
					"roadtypeid         smallint,"+
					"avgspeedbinid      smallint,"+
					"loschedulefraction float,"+
					"unique index xpkloschedulefraction ("+
							"sourcetypeid, roadtypeid, avgspeedbinid))",
			"truncate loschedulefraction",
			"create table if not exists hischedulefraction ("+
					"sourcetypeid       smallint,"+
					"roadtypeid         smallint,"+
					"avgspeedbinid      smallint,"+
					"hischedulefraction float,"+
					"unique index xpkhischedulefraction ("+
							"sourcetypeid, roadtypeid, avgspeedbinid))",
			"truncate hischedulefraction",

			"insert into loschedulefraction ("+
						"sourcetypeid,"+
						"roadtypeid,"+
						"avgspeedbinid,"+
						"loschedulefraction) "+
					"select "+
						"bsl.sourcetypeid,"+
						"bsl.roadtypeid,"+
						"bsl.avgspeedbinid,"+
						"(bsh.hischedulespeed - asb.avgbinspeed) / (bsh.hischedulespeed -"+
								"bsl.loschedulespeed) "+
					"from "+
						"bracketschedulelo bsl,"+
						"bracketschedulehi bsh,"+
						"avgspeedbin asb "+
					"where "+
						"bsl.sourcetypeid = bsh.sourcetypeid and "+
						"bsl.roadtypeid = bsh.roadtypeid and "+
						"bsl.avgspeedbinid = bsh.avgspeedbinid and "+
						"bsl.avgspeedbinid = asb.avgspeedbinid and "+
						"bsh.hischedulespeed <> bsl.loschedulespeed",
			"analyze table loschedulefraction",
			"insert into loschedulefraction ("+
						"sourcetypeid,"+
						"roadtypeid,"+
						"avgspeedbinid,"+
						"loschedulefraction) "+
					"select "+
						"bsl.sourcetypeid,"+
						"bsl.roadtypeid,"+
						"bsl.avgspeedbinid,"+
						"1 "+
					"from "+
						"bracketschedulelo bsl,"+
						"bracketschedulehi bsh "+
					"where "+
						"bsl.sourcetypeid = bsh.sourcetypeid and "+
						"bsl.roadtypeid = bsh.roadtypeid and "+
						"bsl.avgspeedbinid = bsh.avgspeedbinid and "+
						"bsh.hischedulespeed = bsl.loschedulespeed",
			"analyze table loschedulefraction",
			"insert into hischedulefraction ("+
						"sourcetypeid,"+
						"roadtypeid,"+
						"avgspeedbinid,"+
						"hischedulefraction) "+
					"select "+
						"bsh.sourcetypeid,"+
						"bsh.roadtypeid,"+
						"bsh.avgspeedbinid,"+
						"(1 - lsf.loschedulefraction) "+
					"from "+
						"bracketschedulehi bsh,"+
						"loschedulefraction lsf "+
					"where "+
						"lsf.sourcetypeid = bsh.sourcetypeid and "+
						"lsf.roadtypeid = bsh.roadtypeid and "+
						"lsf.avgspeedbinid = bsh.avgspeedbinid",
			"analyze table hischedulefraction"
		};
		String sql = "";
		try {
			for(int i=0;i<statements.length;i++) {
				sql = statements[i];
				SQLRunner.executeSQL(db, sql);
			}
		} catch (SQLException e) {
			Logger.logSqlError(e,"could not determine fraction of drive schedules in each "
					+ "speed bin.", sql);
		}
	}

	/**
	 * Validate drive schedule proportions befor determining drive schedule distribution.
	 * @return true on success
	**/
	boolean validateDriveScheduleDistribution() {
		String sql = "";
		PreparedStatement statement = null;
		ResultSet result = null;
		int lastSourceType = -1;
		int lastRoadType = -1;
		int lastDriveScheduleID = -1;
		try {
			sql = "select distinct dsa.sourcetypeid, dsa.roadtypeid "
					+ " from drivescheduleassoc dsa, runspecroadtype rsrt, "
					+ " runspecsourcetype rsst "
					+ " where dsa.roadtypeid = rsrt.roadtypeid"
					+ " and dsa.sourcetypeid = rsst.sourcetypeid"
					+ " order by dsa.sourcetypeid, dsa.roadtypeid";
			statement = db.prepareStatement(sql);
			result = SQLRunner.executeQuery(statement, sql);
			lastSourceType = -1;
			lastRoadType = -1;
			boolean hasNonRamp = false;
			while(result.next()) {
				int sourceType = result.getInt(1);
				int roadType = result.getInt(2);
				if(lastSourceType != sourceType || lastRoadType != roadType) {
					if(lastSourceType >= 0 && !hasNonRamp) {
						Logger.log(LogMessageCategory.ERROR,
								"no drive schedule for road type " + lastroadtype + " and "
								+ "source type " + lastSourceType);
						return false;
					}
					lastSourceType = sourceType;
					lastRoadType = roadType;
					hasNonRamp = true;
				} else {
					hasNonRamp = true;
				}
			}
			if(lastSourceType >= 0 && !hasNonRamp) {
				Logger.log(LogMessageCategory.ERROR,
						"no drive schedule for road type " + lastroadtype + " and "
						+ "source type " + lastSourceType);
				return false;
			}
		} catch(Exception e) {
			Logger.logError(e, "error while validating drive schedule distribution");
			return false;
		} finally {
			if(statement!=null) {
				try {
					statement.close();
				} catch (SQLException e) {
					// Failure to close on a preparedStatment should not be an issue.
				}
			}
		}
		return true;
	}

	/**
	 * OMDG-3 (Non-Ramp) : Determine Distribution of Non Ramp Drive Schedules.
	 * <p>This step determines the distribution of drive schedules which represents the sum of
	 * all of the average speed bins. This is done for each source type, roadway type, day of
	 * week and hour of day.</p>
	**/
	void determineDriveScheduleDistributionNonRamp() {
		String sql = "";
		try {
			sql = "create table if not exists driveschedulefractionlo ("+
					"sourcetypeid          smallint,"+
					"roadtypeid            smallint,"+
					"avgspeedbinid         smallint,"+
					"drivescheduleid       smallint,"+
					"driveschedulefraction float,"+
					"unique index xpkdriveschedulefractionlo ("+
							"sourcetypeid, roadtypeid, avgspeedbinid, drivescheduleid))";
			SQLRunner.executeSQL(db, sql);

			sql = "truncate driveschedulefractionlo";
			SQLRunner.executeSQL(db, sql);

			sql = "insert into driveschedulefractionlo ("+
						"sourcetypeid,"+
						"roadtypeid,"+
						"avgspeedbinid,"+
						"drivescheduleid,"+
						"driveschedulefraction) "+
					"select "+
						"bsl.sourcetypeid,"+
						"bsl.roadtypeid,"+
						"lsf.avgspeedbinid,"+
						"bsl.drivescheduleid,"+
						"sum(lsf.loschedulefraction) "+
					"from "+
						"bracketschedulelo bsl,"+
						"loschedulefraction lsf "+
					"where "+
						"bsl.sourcetypeid = lsf.sourcetypeid and "+
						"bsl.roadtypeid = lsf.roadtypeid and "+
						"bsl.avgspeedbinid = lsf.avgspeedbinid "+
					"group by "+
						"bsl.sourcetypeid,"+
						"bsl.roadtypeid,"+
						"lsf.avgspeedbinid,"+
						"bsl.drivescheduleid";
			SQLRunner.executeSQL(db, sql);

			SQLRunner.executeSQL(db,"analyze table driveschedulefractionlo");

			sql = "create table if not exists driveschedulefractionhi ( "+
					"sourcetypeid          smallint, "+
					"roadtypeid            smallint, "+
					"avgspeedbinid         smallint,"+
					"drivescheduleid       smallint, "+
					"driveschedulefraction float, "+
					"unique index xpkdriveschedulefractionhi ( "+
					"sourcetypeid, roadtypeid, avgspeedbinid, drivescheduleid))";
			SQLRunner.executeSQL(db, sql);

			sql = "truncate driveschedulefractionhi";
			SQLRunner.executeSQL(db, sql);

			sql = "insert into driveschedulefractionhi ( "+
						"sourcetypeid, "+
						"roadtypeid, "+
						"avgspeedbinid, "+
						"drivescheduleid, "+
						"driveschedulefraction) "+
					"select "+
						"bsh.sourcetypeid, "+
						"bsh.roadtypeid, "+
						"hsf.avgspeedbinid, "+
						"bsh.drivescheduleid, "+
						"sum(hsf.hischedulefraction) "+
					"from "+
						"bracketschedulehi bsh, "+
						"hischedulefraction hsf "+
					"where "+
						"bsh.sourcetypeid = hsf.sourcetypeid and "+
						"bsh.roadtypeid = hsf.roadtypeid and "+
						"bsh.avgspeedbinid = hsf.avgspeedbinid "+
					"group by "+
						"bsh.sourcetypeid, "+
						"bsh.roadtypeid, "+
						"hsf.avgspeedbinid, "+
						"bsh.drivescheduleid";
			SQLRunner.executeSQL(db, sql);

			SQLRunner.executeSQL(db,"analyze table driveschedulefractionhi");

			sql = "create table if not exists driveschedulefraction ("+
					"sourcetypeid          smallint,"+
					"roadtypeid            smallint,"+
					"avgspeedbinid         smallint,"+
					"drivescheduleid       smallint,"+
					"driveschedulefraction float,"+
					"unique index xpkdriveschedulefraction ("+
							"sourcetypeid, roadtypeid, avgspeedbinid, drivescheduleid))";
			SQLRunner.executeSQL(db, sql);
			// changed to INSERT IGNORE to work with MySQL 4, 
			// because DriveScheduleFractionLo and Hi tables have multiple speed bins
			sql = "insert ignore into driveschedulefraction ( "+
						"sourcetypeid, "+
						"roadtypeid, "+
						"avgspeedbinid, "+
						"drivescheduleid, "+
						"driveschedulefraction) "+
					"select  "+
						"bsh.sourcetypeid, "+
						"bsh.roadtypeid, "+
						"dsfh.avgspeedbinid, "+
						"bsh.drivescheduleid, "+
						"(dsfl.driveschedulefraction + dsfh.driveschedulefraction) "+
					"from  "+
						"bracketschedulehi bsh, "+
						"driveschedulefractionlo dsfl, "+
						"driveschedulefractionhi dsfh, "+
						"roadtype rt, "+
						"drivescheduleassoc dsa "+
					"where  "+
						"bsh.sourcetypeid = dsfl.sourcetypeid and "+
						"bsh.roadtypeid = dsfl.roadtypeid and  "+
						"rt.roadtypeid = dsa.roadtypeid and  "+
						"bsh.drivescheduleid = dsfl.drivescheduleid and  "+
						"bsh.drivescheduleid = dsa.drivescheduleid and "+
						"bsh.sourcetypeid = dsfh.sourcetypeid and "+
						"bsh.roadtypeid = dsfh.roadtypeid and  "+
						"bsh.drivescheduleid = dsfh.drivescheduleid and "+ 
						"bsh.sourcetypeid = dsa.sourcetypeid and "+
						"bsh.roadtypeid = dsa.roadtypeid and "+
						"bsh.roadtypeid = rt.roadtypeid and "+
						"dsfl.avgspeedbinid = dsfh.avgspeedbinid";
			SQLRunner.executeSQL(db, sql);

			SQLRunner.executeSQL(db,"analyze table driveschedulefraction");

			sql = "insert ignore into driveschedulefraction ( "+
						"sourcetypeid, "+
						"roadtypeid, "+
						"avgspeedbinid, "+
						"drivescheduleid, "+
						"driveschedulefraction) "+
					"select  "+
						"bsl.sourcetypeid, "+
						"bsl.roadtypeid, "+
						"dsfl.avgspeedbinid, "+
						"bsl.drivescheduleid, "+
						"dsfl.driveschedulefraction "+
					"from  "+
						"bracketschedulelo bsl, "+
						"driveschedulefractionlo dsfl, "+
						"roadtype rt, "+
						"drivescheduleassoc dsa "+
					"where  "+
						"bsl.sourcetypeid = dsfl.sourcetypeid and "+
						"bsl.roadtypeid = dsfl.roadtypeid and  "+
						"bsl.avgspeedbinid = dsfl.avgspeedbinid and "+
						"bsl.roadtypeid = rt.roadtypeid and "+
						"bsl.roadtypeid = dsa.roadtypeid and "+
						"bsl.drivescheduleid = dsa.drivescheduleid and "+
						"rt.roadtypeid = dsa.roadtypeid and  "+
						"bsl.drivescheduleid = dsfl.drivescheduleid and "+
						"bsl.drivescheduleid = dsa.drivescheduleid ";
			SQLRunner.executeSQL(db, sql);

			SQLRunner.executeSQL(db,"analyze table driveschedulefraction");

			sql = "insert ignore into driveschedulefraction ( "+
						"sourcetypeid, "+
						"roadtypeid, "+
						"avgspeedbinid, "+
						"drivescheduleid, "+
						"driveschedulefraction) "+
					"select  "+
						"bsh.sourcetypeid, "+
						"bsh.roadtypeid, "+
						"dsfh.avgspeedbinid, "+
						"bsh.drivescheduleid, "+
						"dsfh.driveschedulefraction "+
					"from  "+
						"bracketschedulehi bsh, "+
						"driveschedulefractionhi dsfh, "+
						"roadtype rt, "+
						"drivescheduleassoc dsa "+
					"where  "+
						"bsh.sourcetypeid = dsfh.sourcetypeid and "+
						"bsh.roadtypeid = dsfh.roadtypeid and "+
						"bsh.avgspeedbinid = dsfh.avgspeedbinid and "+
						"bsh.roadtypeid = rt.roadtypeid and "+
						"bsh.roadtypeid = dsa.roadtypeid and "+
						"rt.roadtypeid = dsa.roadtypeid and  "+
						"bsh.sourcetypeid = dsa.sourcetypeid and "+
						"bsh.drivescheduleid = dsfh.drivescheduleid and "+
						"bsh.drivescheduleid = dsa.drivescheduleid ";
			SQLRunner.executeSQL(db, sql);

			sql = "insert ignore into driveschedulefraction (sourcetypeid, roadtypeid, avgspeedbinid, drivescheduleid, driveschedulefraction)"
					+ " select tempsourcetypeid, roadtypeid, avgspeedbinid, drivescheduleid, driveschedulefraction"
					+ " from driveschedulefraction"
					+ " inner join sourceusetypephysicsmapping on (realsourcetypeid=sourcetypeid)"
					+ " where tempsourcetypeid <> realsourcetypeid";
			SQLRunner.executeSQL(db,sql);

			SQLRunner.executeSQL(db,"analyze table driveschedulefraction");
		} catch(SQLException e) {
			Logger.logSqlError(e,"could not determine the distribution of drive schedules for "
					+ "a drive cycle.", sql);
		}
	}

	/**
	 *	OMDG-4: Calculate the second-by-second engine specific power.
	 * <p>This step calculates the engine specific power for each drive schedule for each source
	 * type.  This step could be limited to only those drive schedules needed for the specified
	 * source types and roadway types and indicated by the non-zero values for
	 * DriveScheduleFraction.
	**/
	void calculateEnginePowerBySecond() {
		String sql="";
		try {
			sql = "create table if not exists sourcetypedriveschedule ("+
					"sourcetypeid     smallint,"+
					"drivescheduleid  smallint,"+
					"unique index xpksourcetypedriveschedule ("+
							"sourcetypeid, drivescheduleid))";
			SQLRunner.executeSQL(db, sql);

			sql = "truncate sourcetypedriveschedule";
			SQLRunner.executeSQL(db, sql);

			sql = "insert into sourcetypedriveschedule ("+
						"sourcetypeid,"+
						"drivescheduleid) "+
					"select "+
						"dsf.sourcetypeid,"+
						"dsf.drivescheduleid "+
					"from "+
						"driveschedulefraction dsf "+
					"group by "+
						"dsf.sourcetypeid,"+
						"dsf.drivescheduleid "+
					"having "+
						"sum(dsf.driveschedulefraction) <> 0";
			SQLRunner.executeSQL(db, sql);

			sql = "insert ignore into sourcetypedriveschedule ( sourcetypeid, drivescheduleid)"
					+ " select tempsourcetypeid, drivescheduleid"
					+ " from sourcetypedriveschedule"
					+ " inner join sourceusetypephysicsmapping on (realsourcetypeid=sourcetypeid)"
					+ " where tempsourcetypeid <> realsourcetypeid";
			SQLRunner.executeSQL(db, sql);

			SQLRunner.executeSQL(db,"analyze table sourcetypedriveschedule");

			sql = "create table if not exists driveschedulefirstsecond ("+
					"drivescheduleid	smallint,"+
					"second			smallint,"+
					"unique index xpkdriveschedulefirstsecond ("+
							"drivescheduleid))";
			SQLRunner.executeSQL(db, sql);

			sql = "truncate driveschedulefirstsecond";
			SQLRunner.executeSQL(db, sql);

			sql = "insert into driveschedulefirstsecond ("+
						"drivescheduleid,"+
						"second) "+
					"select "+
						"dss.drivescheduleid,"+
						"min(dss.second) "+
					"from "+
						"driveschedulesecond dss "+
					"group by "+
						"dss.drivescheduleid";
			SQLRunner.executeSQL(db, sql);

			SQLRunner.executeSQL(db,"analyze table driveschedulefirstsecond");

			sql = "create table if not exists driveschedulesecond2 ("+
					"drivescheduleid smallint,"+
					"second          smallint,"+
					"speed           float,"+
					"acceleration    float,"+
					"unique index xpkdriveschedulesecond2 ("+
							"drivescheduleid, second))";
			SQLRunner.executeSQL(db, sql);

			sql = "truncate driveschedulesecond2";
			SQLRunner.executeSQL(db, sql);

			sql = "insert into driveschedulesecond2 ("+
						"drivescheduleid,"+
						"second,"+
						"speed,"+
						"acceleration) "+
					"select "+
						"dsfs.drivescheduleid,"+
						"dsfs.second,"+
						"dss.speed * 0.44704,"+
						"0 "+
					"from "+
						"driveschedulefirstsecond dsfs,"+
						"driveschedulesecond dss "+
					"where "+
						"dsfs.drivescheduleid = dss.drivescheduleid and "+
						"dsfs.second = dss.second";
			SQLRunner.executeSQL(db, sql);

			SQLRunner.executeSQL(db,"analyze table driveschedulesecond2");

			sql = "insert into driveschedulesecond2 ("+
						"drivescheduleid,"+
						"second,"+
						"speed,"+
						"acceleration) "+
					"select "+
						"dss.drivescheduleid,"+
						"dss.second,"+
						"dss.speed * 0.44704,"+
						"(dss.speed - dss2.speed) * 0.44704 "+
					"from "+
						"driveschedulesecond dss,"+
						"driveschedulesecond dss2 "+
					"where "+
						"dss.drivescheduleid = dss2.drivescheduleid and "+
						"dss2.second = dss.second - 1";
			SQLRunner.executeSQL(db, sql);

			SQLRunner.executeSQL(db,"analyze table driveschedulesecond2");

			sql = "create table if not exists vsp ("+
					"sourcetypeid    smallint,"+
					"drivescheduleid smallint,"+
					"second          smallint,"+
					"vsp             float,"+
					"unique index xpkesp ("+
							"sourcetypeid, drivescheduleid, second))";
			SQLRunner.executeSQL(db, sql);

			sql = "truncate vsp";
			SQLRunner.executeSQL(db, sql);

			sql = "insert into vsp ("+
						"sourcetypeid,"+
						"drivescheduleid,"+
						"second,"+
						"vsp) "+
					"select "+
						"stds.sourcetypeid,"+
						"stds.drivescheduleid,"+
						"dss2.second,"+
						"(sut.rollingterma * dss2.speed +"+
								"sut.rotatingtermb * pow(dss2.speed,2) + "+
								"sut.dragtermc * pow(dss2.speed,3) + "+
								"sut.sourcemass * dss2.speed * "+
							// old line	"dss2.acceleration) / sut.sourcemass "+
								"dss2.acceleration) / sut.fixedmassfactor "+
					"from "+
						"sourcetypedriveschedule stds,"+
						"driveschedulesecond2 dss2,"+
						"sourceusetypephysicsmapping sut "+
					"where "+
						"dss2.drivescheduleid = stds.drivescheduleid and "+
						"sut.tempsourcetypeid = stds.sourcetypeid and "+
						"sut.sourcemass <> 0 and "+
						"dss2.second > 0";
			SQLRunner.executeSQL(db, sql);

			SQLRunner.executeSQL(db,"analyze table vsp");
		} catch (SQLException e) {
			Logger.logSqlError(e,"could not calculate engine power distribution.",sql);
		}
	}

	/**
	 * OMDG-5: Determine the operating mode bin for each second.
	 * <p>The ESP value for each second is compared to the upper and lower bounds for the
	 * operating mode bins and a bin ID is assigned to each second. This is done for each
	 * source type, drive schedule and second.</p>
	**/
	void determineOpModeIDPerSecond() {
		String sql = "";
		PreparedStatement statement = null;
		try {
			sql = "create table if not exists driveschedulesecond3 ("+
					"drivescheduleid smallint,"+
					"second          smallint,"+
					"speed           float,"+
					"acceleration    float,"+
					"unique index xpkdriveschedulesecond3 ("+
							"drivescheduleid, second))";
			SQLRunner.executeSQL(db, sql);
			//System.out.println("######## creating drive schedule second 3 ##########");

			sql = "truncate driveschedulesecond3";
			SQLRunner.executeSQL(db, sql);

			sql = "insert into driveschedulesecond3 ("+
						"drivescheduleid,"+
						"second,"+
						"speed,"+
						"acceleration) "+
					"select "+
						"dss.drivescheduleid,"+
						"dss.second,"+
						"dss.speed,"+
						"dss.speed - dss2.speed "+
					"from "+
						"driveschedulesecond dss,"+
						"driveschedulesecond dss2 "+
					"where "+
						"dss.drivescheduleid = dss2.drivescheduleid and "+
						"dss2.second = dss.second - 1";
			SQLRunner.executeSQL(db, sql);

			SQLRunner.executeSQL(db,"analyze table driveschedulesecond3");

			sql = "create table if not exists opmodeidbysecond ("+
					"sourcetypeid    smallint,"+
					"drivescheduleid smallint,"+
					"second          smallint,"+
					"opmodeid        smallint,"+
					"polprocessid    int,"+
					"speed           float,"+
					"acceleration    float,"+
					"vsp             float,"+
					"unique index xpkopmodeidbysecond ("+
							"sourcetypeid, drivescheduleid, second, polprocessid))";
			SQLRunner.executeSQL(db, sql);

			sql = "truncate opmodeidbysecond";
			SQLRunner.executeSQL(db, sql);
/*
			sql = "insert into opmodeidbysecond ("+
						"sourcetypeid,"+
						"drivescheduleid,"+
						"second,"+
						"opmodeid,"+
						"speed,"+
						"acceleration,"+
						"vsp) "+
					"select "+
						"v.sourcetypeid,"+
						"v.drivescheduleid,"+
						"v.second,"+
						"null,"+
						"dss.speed,"+
						"dss.acceleration,"+
						"v.vsp "+
					"from "+
						"runspecsourcetype rsst,"+
						"driveschedulesecond3 dss,"+
						"vsp v "+
					"where "+
						"v.sourcetypeid = rsst.sourcetypeid and "+
						"dss.drivescheduleid = v.drivescheduleid and "+
						"dss.second = v.second";
*/
			sql = "create table omdgpollutantprocess ("
					+ " polprocessid int not null,"
					+ " unique index pkxomdgpollutantprocess (polprocessid) )";
			SQLRunner.executeSQL(db, sql);

			sql = "insert into omdgpollutantprocess (polprocessid)"
					+ " select distinct polprocessid"
					+ " from opmodepolprocassoc"; // RunSpecPollutantProcess
			SQLRunner.executeSQL(db, sql);

			// Note: We cannot remove anything that already has an operating mode distribution
			// because not all required links may have been provided.

			// Remove anything from OMDGPollutantProcess that has a representing pollutant/process.  Only
			// its representing item should be calculated.
			sql = "delete from omdgpollutantprocess"
					+ " using omdgpollutantprocess"
					+ " inner join omdgpolprocessrepresented on (omdgpollutantprocess.polprocessid = omdgpolprocessrepresented.polprocessid)";
			SQLRunner.executeSQL(db, sql);

			SQLRunner.executeSQL(db,"analyze table omdgpollutantprocess");

			sql = "create table if not exists opmodepolprocassoctrimmed ("
					+ " 	polprocessid int not null default '0',"
					+ " 	opmodeid smallint(6) not null default '0',"
					+ " 	primary key (opmodeid,polprocessid),"
					+ " 	key (polprocessid),"
					+ " 	key (opmodeid),"
					+ " 	key (opmodeid,polprocessid)"
					+ " )";
			SQLRunner.executeSQL(db, sql);

			sql = "truncate opmodepolprocassoctrimmed";
			SQLRunner.executeSQL(db, sql);

			sql = "insert into opmodepolprocassoctrimmed (polprocessid, opmodeid)"
					+ " select omp.polprocessid, omp.opmodeid"
					+ " from opmodepolprocassoc omp"
					+ " inner join omdgpollutantprocess pp on pp.polprocessid=omp.polprocessid";
			SQLRunner.executeSQL(db, sql);

			sql = "insert into opmodeidbysecond ("+
						"sourcetypeid,"+
						"drivescheduleid,"+
						"second,"+
						"opmodeid,"+
						"polprocessid,"+
						"speed,"+
						"acceleration,"+
						"vsp) "+
					"select "+
						"v.sourcetypeid,"+
						"v.drivescheduleid,"+
						"v.second,"+
						"null,"+
						"rspp.polprocessid,"+
						"dss.speed,"+
						"dss.acceleration,"+
						"v.vsp "+
					"from "+
						"driveschedulesecond3 dss,"+
						"vsp v,"+
						"omdgpollutantprocess rspp "+
					"where "+
						"dss.drivescheduleid = v.drivescheduleid and "+
						"dss.second = v.second";
			SQLRunner.executeSQL(db, sql);

			SQLRunner.executeSQL(db,"analyze table opmodeidbysecond");

			sql = "drop table if exists opmodeidbysecond_temp";
			SQLRunner.executeSQL(db, sql);

			sql = "create table opmodeidbysecond_temp "+
					"select " +
						"opid3.sourcetypeid, " +
						"opid3.drivescheduleid, " +
						"opid3.second, " +
						"0 as opmodeid, " +
						"opid3.polprocessid, "+
						"opid3.speed, " +
						"opid3.acceleration, " +
						"opid3.vsp " +
					"from " +
						"opmodeidbysecond opid1, " +
						"opmodeidbysecond opid2, " +
						"opmodeidbysecond opid3 " +
					"where " +
						"opid1.sourcetypeid = opid2.sourcetypeid and " +
						"opid2.sourcetypeid = opid3.sourcetypeid and " +
						"opid1.drivescheduleid = opid2.drivescheduleid and " +
						"opid2.drivescheduleid = opid3.drivescheduleid and " +
						"opid1.polprocessid = opid2.polprocessid and " +
						"opid2.polprocessid = opid3.polprocessid and " +
						"opid1.second = opid2.second-1 and " +
						"opid2.second = opid3.second-1 and " +
						"(opid3.acceleration <= -2 or (opid1.acceleration<-1 and " +
						"opid2.acceleration<-1 and " +
						"opid3.acceleration<-1))";
			SQLRunner.executeSQL(db, sql);

			sql = "replace into opmodeidbysecond ( "+
						"sourcetypeid, "+
						"drivescheduleid, "+
						"second, "+
						"opmodeid, "+
						"polprocessid, "+
						"speed, "+
						"acceleration, "+
						"vsp "+
						") "+
					"select "+
						"sourcetypeid, "+
						"drivescheduleid, "+
						"second, "+
						"opmodeid, "+
						"polprocessid, "+
						"speed, "+
						"acceleration, "+
						"vsp "+
					"from "+
						"opmodeidbysecond_temp";
			SQLRunner.executeSQL(db, sql);

			SQLRunner.executeSQL(db,"analyze table opmodeidbysecond");

			sql = "update opmodeidbysecond set opmodeid=0 where acceleration <= -2";
			SQLRunner.executeSQL(db, sql);

			sql = "select om.opmodeid, polprocessid, vsplower, vspupper, speedlower, speedupper " +
					"from operatingmode om inner join opmodepolprocassoc using (opmodeid) " +
					"where om.opmodeid > 1 and om.opmodeid < 100";
			statement = db.prepareStatement(sql);
			ResultSet result = SQLRunner.executeQuery(statement, sql);
			while(result.next()) {
				int opModeID = result.getInt(1);
				int polProcessID = result.getInt(2);
				float vspLower = result.getFloat(3);
				boolean isVSPLowerNull = result.wasNull();
				float vspUpper = result.getFloat(4);
				boolean isVSPUpperNull = result.wasNull();
				float speedLower = result.getFloat(5);
				boolean isSpeedLowerNull = result.wasNull();
				float speedUpper = result.getFloat(6);
				boolean isSpeedUpperNull = result.wasNull();
				sql = "update opmodeidbysecond set opmodeid = " + opModeID;
				String whereClause = "";
				String vspClause = "";
				String speedClause = "";

				if(!isVSPLowerNull) {
					vspClause += "vsp >= " + vspLower;
				}
				if(!isVSPUpperNull) {
					if(vspClause.length() > 0) {
						vspClause += " and ";
					}
					vspClause += "vsp < " + vspUpper;
				}
				if(!isSpeedLowerNull) {
					speedClause += "speed >= " + speedLower;
				}
				if(!isSpeedUpperNull) {
					if(speedClause.length() > 0) {
						speedClause += " and ";
					}
					speedClause += "speed < " + speedUpper;
				}
				if(vspClause.length() > 0) {
					whereClause += "(" + vspclause + ")";
				}
				if(speedClause.length() > 0) {
					if(whereClause.length() > 0) {
						whereClause += " and ";
					}
					whereClause += "(" + speedclause + ")";
				}
				sql += " where " + whereclause + " and polprocessid = " + polProcessID + 
						" and opmodeid is null";
				SQLRunner.executeSQL(db, sql);
			}
			statement.close();
			statement = null;
			result.close();

			sql = "update opmodeidbysecond set opmodeid=if(speed=0 and polprocessid=11609,501,if(speed<1.0,1,opmodeid))";
			SQLRunner.executeSQL(db,sql);
		} catch (SQLException e) {
			Logger.logSqlError(e, "could not determine operating mode id distribution.", sql);
		} finally {
			if(statement!=null) {
				try {
					statement.close();
				} catch (SQLException e) {
					// Failure to close on a preparedStatement should not be an issue.
				}
			}
		}
	}

	/**
	 * OMDG-6: Calculate operating mode fractions for each drive schedule.
	 * <p>Once all the seconds in each operating mode bin are known, the distribution of the bins
	 * can be determined. The sum of the operating mode fractions will add to one for each source
	 * type and drive schedule combination.  This is done for each source type and drive schedule.
	 * </p>
	**/
	void calculateOpModeFractionsPerDriveSchedule() {
		String sql = "";
		try {
			sql = "create table if not exists opmodefractionbyschedule2 ("+
					"sourcetypeid       smallint,"+
					"drivescheduleid    smallint,"+
					"polprocessid       int,"+
					"secondsum          smallint,"+
					"unique index xpkopmodefractionbyschedule2 ("+
							"sourcetypeid, drivescheduleid, polprocessid))";
			SQLRunner.executeSQL(db, sql);

			sql = "truncate opmodefractionbyschedule2";
			SQLRunner.executeSQL(db, sql);

			sql = "insert into opmodefractionbyschedule2 ("+
						"sourcetypeid,"+
						"drivescheduleid,"+
						"polprocessid,"+
						"secondsum) "+
					"select "+
						"omis.sourcetypeid,"+
						"omis.drivescheduleid,"+
						"omis.polprocessid,"+
						"count(*) "+
					"from "+
						" opmodeidbysecond omis "+
					"group by "+
						"omis.sourcetypeid,"+
						"omis.drivescheduleid,"+
						"omis.polprocessid";
			SQLRunner.executeSQL(db, sql);

			SQLRunner.executeSQL(db,"analyze table opmodefractionbyschedule2");

			sql = "create table if not exists opmodefractionbyschedule ("+
					"sourcetypeid      smallint,"+
					"drivescheduleid   smallint,"+
					"polprocessid      int,"+
					"opmodeid          smallint,"+
					"modefraction      float,"+
					"unique index xpkopmodefractionbyschedule ("+
							"sourcetypeid, drivescheduleid, polprocessid, opmodeid))";
			SQLRunner.executeSQL(db, sql);

			sql = "truncate opmodefractionbyschedule";
			SQLRunner.executeSQL(db, sql);

			sql = "insert into opmodefractionbyschedule ("+
						"sourcetypeid,"+
						"drivescheduleid,"+
						"polprocessid,"+
						"opmodeid,"+
						"modefraction) "+
					"select "+
						"omis.sourcetypeid,"+
						"omis.drivescheduleid,"+
						"omis.polprocessid,"+
						"omis.opmodeid,"+
						"count(*) / omfs2.secondsum "+
					"from "+
						"opmodeidbysecond omis,"+
						"opmodefractionbyschedule2 omfs2 "+
					"where "+
						"omis.sourcetypeid = omfs2.sourcetypeid and "+
						"omis.drivescheduleid = omfs2.drivescheduleid and "+
						"omis.polprocessid = omfs2.polprocessid and "+
						"omfs2.secondsum <> 0 "+
					"group by "+
						"omis.sourcetypeid,"+
						"omis.drivescheduleid,"+
						"omis.polprocessid,"+
						"omis.opmodeid";
			SQLRunner.executeSQL(db, sql);

			SQLRunner.executeSQL(db,"analyze table opmodefractionbyschedule");

			sql = "truncate opmodefractionbyschedule2";
			SQLRunner.executeSQL(db, sql);
		} catch (SQLException e) {
			Logger.logSqlError(e, "could not determine fractions of operating modes per drive"
					+ " schedule", sql);
		}
	}

	/**
	 * Preliminary steps for OMDG-7: Calculate overall operating mode fractions.
	 * <p>The overall operating mode fractions are calculated by weighting the operating mode
	 * fractions of each drive schedule by the drive schedule fractions. This is done for each
	 * source type, road type, day of the week, hour of the day and operating mode. This generator
	 * only applies to the running process of the total energy pollutant.</p>
	**/
	void preliminaryCalculateOpModeFractions() {
		String sql = "";
		try {
			String[] statements = {
				"drop table if exists opmodefraction2",
				"drop table if exists opmodefraction2a",
				"drop table if exists opmodefraction2b",

				"create table if not exists opmodefraction2 ("+
					"sourcetypeid      smallint,"+
					"roadtypeid        smallint,"+
					"avgspeedbinid     smallint,"+
					"hourdayid         smallint,"+
					"opmodeid          smallint,"+
					"polprocessid      int,"+
					"opmodefraction    float,"+
					"unique index xpkopmodefraction2 ("+
							"roadtypeid, avgspeedbinid, sourcetypeid, hourdayid, opmodeid, polprocessid))",
				// NOTE: above, roadTypeID is the first in the index so that it will be used
				// in calculateOpModeFractions which joins based on roadTypeID only.

				"create table if not exists opmodefraction2a ("+
					"sourcetypeid      smallint,"+
					"roadtypeid        smallint,"+
					"avgspeedbinid     smallint,"+
					"hourdayid         smallint,"+
					"opmodeid          smallint,"+
					"polprocessid      int,"+
					"opmodefraction    float,"+
					"index idxopmodefraction2a ("+
							"roadtypeid, avgspeedbinid, sourcetypeid, hourdayid, opmodeid, polprocessid))",

				"create table if not exists opmodefraction2b ("+
					"sourcetypeid      smallint,"+
					"roadtypeid        smallint,"+
					"avgspeedbinid     smallint,"+
					"opmodeid          smallint,"+
					"polprocessid      int,"+
					"opmodefraction    float)",

				"truncate opmodefraction2",
				"truncate opmodefraction2a",
				"truncate opmodefraction2b",

				// Add road information
				"insert into opmodefraction2b ("+
						"sourcetypeid,"+
						"roadtypeid,"+
						"avgspeedbinid,"+
						"opmodeid,"+
						"polprocessid,"+
						"opmodefraction) "+
					"select "+
						"dsf.sourcetypeid,"+
						"dsf.roadtypeid,"+
						"dsf.avgspeedbinid,"+
						"omfs.opmodeid,"+
						"omppa.polprocessid,"+
						"sum(omfs.modefraction * dsf.driveschedulefraction) as opmodefraction "+
					"from "+
						"driveschedulefraction dsf,"+
						"opmodefractionbyschedule omfs, "+
						"opmodepolprocassoctrimmed omppa " +
					"where "+
						"dsf.sourcetypeid = omfs.sourcetypeid and "+
						"dsf.drivescheduleid = omfs.drivescheduleid and "+
						"omfs.polprocessid = omppa.polprocessid and "+
						"omppa.opmodeid = omfs.opmodeid and " +
						"omppa.polprocessid not in (11710) " +
					"group by "+
						"dsf.sourcetypeid,"+
						"dsf.roadtypeid,"+
						"dsf.avgspeedbinid,"+
						"omppa.polprocessid,"+
						"omfs.opmodeid",

				"insert into opmodefraction2a ("+
						"sourcetypeid,"+
						"roadtypeid,"+
						"avgspeedbinid,"+
						"hourdayid,"+
						"opmodeid,"+
						"polprocessid,"+
						"opmodefraction) "+
					"select sourcetypeid, roadtypeid, avgspeedbinid, hourdayid, opmodeid, polprocessid, opmodefraction "+
					"from opmodefraction2b,"+
						"runspechourday",

				"analyze table opmodefraction2a",

				// Aggregate data from OpModeFraction2a
				"insert into opmodefraction2 ("+
						"sourcetypeid,"+
						"roadtypeid,"+
						"avgspeedbinid,"+
						"hourdayid,"+
						"opmodeid,"+
						"polprocessid,"+
						"opmodefraction) "+
					"select "+
						"sourcetypeid,"+
						"roadtypeid,"+
						"avgspeedbinid,"+
						"hourdayid,"+
						"opmodeid,"+
						"polprocessid,"+
						"sum(opmodefraction) as opmodefraction "+
					"from "+
						"opmodefraction2a "+
					"group by "+
						"roadtypeid, avgspeedbinid, sourcetypeid, hourdayid, opmodeid, polprocessid",

				"analyze table opmodefraction2"
			};

			for(int i=0;i<statements.length;i++) {
				sql = statements[i];
				SQLRunner.executeSQL(db,sql);
			}
		} catch (SQLException e) {
			Logger.logSqlError(e,"could not determine final operating mode distribution.",sql);
		}
	}

	/**
	 * OMDG-7: Calculate overall operating mode fractions.
	 * <p>The overall operating mode fractions are calculated by weighting the operating mode
	 * fractions of each drive schedule by the drive schedule fractions. This is done for each
	 * source type, road type, day of the week, hour of the day and operating mode. This generator
	 * only applies to the running process of the total energy pollutant.</p>
	 * @param linkID The link being processed.
	**/
	void calculateOpModeFractions(int linkID) {
		//System.out.println("calculateopmodefractions(" + linkid + ")");
		String sql = "";
		SQLRunner.Query query = new SQLRunner.Query();
		try {
			// Find [sourceTypeID, linkID, hourDayID, polProcessID] combinations
			// that already exist within OpModeDistribution.
			sql = "create table if not exists omdgomdkeys ( "+
					"	sourcetypeid smallint(6) not null default '0', "+
					"	hourdayid smallint(6) not null default '0', "+
					"	polprocessid int not null default '0', "+
					"	primary key (hourdayid, polprocessid, sourcetypeid) "+
					")";
			SQLRunner.executeSQL(db, sql);

			sql = "truncate table omdgomdkeys";
			SQLRunner.executeSQL(db, sql);

			sql = "insert into omdgomdkeys (sourcetypeid, hourdayid, polprocessid) "+
					"select distinct sourcetypeid, hourdayid, polprocessid "+
					"from opmodedistribution "+
					"where linkid = " + linkID;
			SQLRunner.executeSQL(db, sql);

			// Add to OpModeDistribution those entries that don't already have records
			sql = "insert ignore into opmodedistribution ("+
						"sourcetypeid,"+
						"linkid,"+
						"hourdayid,"+
						"polprocessid,"+
						"opmodeid,"+
						"opmodefraction) "+
					"select "+
						"omf2.sourcetypeid,"+
						"l.linkid,"+
						"omf2.hourdayid,"+
						"omf2.polprocessid,"+
						"omf2.opmodeid,"+
						"omf2.opmodefraction "+
					"from "+
						"opmodefraction2 omf2 "+
						"inner join link l on ("+
							"l.roadtypeid=omf2.roadtypeid "+
							"and omf2.avgspeedbinid = mod(l.linkid,100) "+
							") "+
						"left outer join omdgomdkeys k on ( "+
							"k.hourdayid=omf2.hourdayid "+
							"and k.polprocessid=omf2.polprocessid "+
							"and k.sourcetypeid=omf2.sourcetypeid "+
						") "+
					"where "+
						"k.hourdayid is null "+
						"and k.polprocessid is null "+
						"and k.sourcetypeid is null "+
						"and l.linkid =" + linkID;
			SQLRunner.executeSQL(db, sql);

			// Copy representing entries to those being represented, but only if those
			// being represented are not already present.
			sql = "truncate table omdgomdkeys";
			SQLRunner.executeSQL(db, sql);

			sql = "insert into omdgomdkeys (sourcetypeid, hourdayid, polprocessid) "+
					"select distinct sourcetypeid, hourdayid, polprocessid "+
					"from opmodedistribution "+
					"where linkid = " + linkID;
			SQLRunner.executeSQL(db, sql);

			ArrayList<String> ppaList = new ArrayList<String>();
			ArrayList<String> repPPAList = new ArrayList<String>();
			sql = "select polprocessid, representingpolprocessid"
					+ " from omdgpolprocessrepresented";
			query.open(db,sql);
			while(query.rs.next()) {
				ppaList.add(query.rs.getString(1));
				repPPAList.add(query.rs.getString(2));
			}
			query.close();
			for(int i=0;i<ppaList.size();i++) {
				String ppa = ppaList.get(i);
				String repPPA = repPPAList.get(i);

				sql = "insert ignore into opmodedistribution ("+
							"sourcetypeid,"+
							"linkid,"+
							"hourdayid,"+
							"polprocessid,"+
							"opmodeid,"+
							"opmodefraction) "+
						"select "+
							"omf2.sourcetypeid,"+
							"l.linkid,"+
							"omf2.hourdayid,"+
							ppa + " as polprocessid,"+
							"omf2.opmodeid,"+
							"omf2.opmodefraction "+
						"from "+
							"opmodefraction2 omf2 "+
							"inner join link l on ("+
								"l.roadtypeid=omf2.roadtypeid "+
								"and omf2.avgspeedbinid = mod(l.linkid,100) "+
								") "+
							"left outer join omdgomdkeys k on ( "+
								"k.hourdayid=omf2.hourdayid "+
								"and k.polprocessid=" + ppa + " "+
								"and k.sourcetypeid=omf2.sourcetypeid "+
							") "+
						"where "+
							"k.hourdayid is null "+
							"and k.polprocessid is null "+
							"and k.sourcetypeid is null "+
							"and l.linkid =" + linkid+ " "+
							"and omf2.polprocessid =" + repPPA;
				SQLRunner.executeSQL(db, sql);
			}

			modelYearPhysics.updateOperatingModeDistribution(db,"opmodedistribution");
			SQLRunner.executeSQL(db,"analyze table opmodedistribution");

			// Get distinct polProcessID in OpModeDistributionTemp as these are the ones to 
			// be cleaned out of OpModeDistribution
			sql = "select distinct polprocessid from opmodefraction2";
			polProcessIDs = "";
			query.open(db,sql);
			while(query.rs.next()) {
				if(polProcessIDs.length() > 0) {
					polProcessIDs += ",";
				}
				polProcessIDs += query.rs.getString(1);
			}
			query.close();
		} catch (SQLException e) {
			Logger.logSqlError(e,"could not determine final operating mode distribution.",sql);
		} finally {
			query.onFinally();
		}
	}
}
