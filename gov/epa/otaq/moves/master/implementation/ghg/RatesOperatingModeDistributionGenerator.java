/**************************************************************************************************
 * @(#)RatesOperatingModeDistributionGenerator.java
 *
 *
 *
 *************************************************************************************************/
package gov.epa.otaq.moves.master.implementation.ghg;

import gov.epa.otaq.moves.common.*;
import gov.epa.otaq.moves.master.runspec.*;
import gov.epa.otaq.moves.master.framework.*;
import java.util.*;
import java.sql.*;

/**
 * This builds "Operating Mode Distribution" records for ELDB data.
 * ELDB is the Execution Location Database explained in TotalActivityGenerator
 *
 * @author		Wesley Faler
 * @version		2017-07-04
**/
public class RatesOperatingModeDistributionGenerator extends Generator {
	// true when the external generator should be invoked for some of the algorithms
	public static final boolean USE_EXTERNAL_GENERATOR = true;
	public static final boolean USE_EXTERNAL_GENERATOR_FOR_DRIVE_CYCLES = true;

	/**
	 * @algorithm
	 * @owner Rates Operating Mode Distribution Generator
	 * @generator
	**/

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
	/** true when a Project simulation is being used **/
	boolean isProjectDomain = false;
	/**
	 * True when the BaseRateGenerator's MasterLoop subscription has been checked.
	 * Significant speedups are possible by combining the RatesOpModeDistributionGenerator
	 * and the BaseRateGenerator in the external generator code.
	**/
	boolean didCheckForBaseRateGenerator = false;
	/** True when external generator steps should be combined with the BaseRateGenerator's steps. **/
	boolean shouldDeferForBaseRateGenerator = false;
	/** MasterLoop object that owns and executes this generator **/
	MasterLoop ownerLoop;

	/** Default constructor **/
	public RatesOperatingModeDistributionGenerator() {
	}

	/**
	 * Requests that this object subscribe to the given loop at desired looping points.
	 * Objects can assume that all necessary MasterLoopable objects have been instantiated.
	 *
	 * @param targetLoop The loop to subscribe to.
	**/
	public void subscribeToMe(MasterLoop targetLoop) {
		ownerLoop = targetLoop;
		isProjectDomain = ExecutionRunSpec.theExecutionRunSpec.getModelDomain() == ModelDomain.PROJECT;
		String[] processNames = {
			(isProjectDomain? null : "Running Exhaust"), // Don't do Running Exhaust in project domain.
			"Extended Idle Exhaust",
			"Auxiliary Power Exhaust"
		};
		for(int i=0;i<processNames.length;i++) {
			if(processNames[i] == null) {
				continue;
			}
			EmissionProcess process = EmissionProcess.findByName(processNames[i]);
			if(process != null) {
				targetLoop.subscribe(this, process, MasterLoopGranularity.YEAR, // Year level for source bins from SBDG.
						MasterLoopPriority.GENERATOR);
			}
		}
	}

	/**
	 * Called each time the link changes.
	 *
	 * @param inContext The current context of the loop.
	**/
	public void executeLoop(MasterLoopContext inContext) {
		// Look ahead to see if the BaseRateGenerator will be used as well.
		// If it will, and if it too will use the external generator, then
		// performance is improved by combining steps in this generator with
		// steps in BaseRateGenerator.
		if(!didCheckForBaseRateGenerator) {
			didCheckForBaseRateGenerator = true;
			shouldDeferForBaseRateGenerator = false;
			if(USE_EXTERNAL_GENERATOR && BaseRateGenerator.USE_EXTERNAL_GENERATOR) {
				ArrayList<MasterLoopable> loopables = ownerLoop.getSubscribers();
				for(MasterLoopable ml : loopables) {
					if(ml instanceof BaseRateGenerator) {
						shouldDeferForBaseRateGenerator = true;
						break;
					}
				}
			}
		}

		try {
			db = DatabaseConnectionManager.checkOutConnection(MOVESDatabaseType.EXECUTION);

			long start, detailStart, detailEnd;

			// The following only has to be done once for each run.
			if(!hasBeenSetup) {
				start = System.currentTimeMillis();
				/**
				 * @step 010
				 * @algorithm Setup for Model Year Physics effects.
				**/
				modelYearPhysics.setup(db);
				if(isProjectDomain || USE_EXTERNAL_GENERATOR_FOR_DRIVE_CYCLES) {
					hasBeenSetup = true;
				} else {
					detailStart = System.currentTimeMillis();
					bracketAverageSpeedBins(); // steps 100-109
					detailEnd = System.currentTimeMillis();
					Logger.log(LogMessageCategory.DEBUG,"RatesOpModeDistributionGenerator.bracketAverageSpeedBins ms="+(detailEnd-detailStart));
					
					detailStart = System.currentTimeMillis();
					determineDriveScheduleProportions(); // steps 110-119
					detailEnd = System.currentTimeMillis();
					Logger.log(LogMessageCategory.DEBUG,"RatesOpModeDistributionGenerator.determineDriveScheduleProportions ms="+(detailEnd-detailStart));

					if(isValid) {
						detailStart = System.currentTimeMillis();
						determineDriveScheduleDistributionNonRamp(); // steps 130-139
						detailEnd = System.currentTimeMillis();
						Logger.log(LogMessageCategory.DEBUG,"RatesOpModeDistributionGenerator.determineDriveScheduleDistributionNonRamp ms="+(detailEnd-detailStart));

						detailStart = System.currentTimeMillis();
						calculateEnginePowerBySecond(); // steps 140-149
						detailEnd = System.currentTimeMillis();
						Logger.log(LogMessageCategory.DEBUG,"RatesOpModeDistributionGenerator.calculateEnginePowerBySecond ms="+(detailEnd-detailStart));

						detailStart = System.currentTimeMillis();
						determineOpModeIDPerSecond(); // steps 150-159
						detailEnd = System.currentTimeMillis();
						Logger.log(LogMessageCategory.DEBUG,"RatesOpModeDistributionGenerator.determineOpModeIDPerSecond ms="+(detailEnd-detailStart));

						detailStart = System.currentTimeMillis();
						calculateOpModeFractionsPerDriveSchedule(); // steps 160-169
						detailEnd = System.currentTimeMillis();
						Logger.log(LogMessageCategory.DEBUG,"RatesOpModeDistributionGenerator.calculateOpModeFractionsPerDriveSchedule ms="+(detailEnd-detailStart));

						detailStart = System.currentTimeMillis();
						preliminaryCalculateOpModeFractions(); // steps 170-179
						detailEnd = System.currentTimeMillis();
						Logger.log(LogMessageCategory.DEBUG,"RatesOpModeDistributionGenerator.preliminaryCalculateOpModeFractions ms="+(detailEnd-detailStart));

						hasBeenSetup = true;
					}
				}
				setupTime += System.currentTimeMillis() - start;
			}

			start = System.currentTimeMillis();
			if(isValid) {
				String alreadyKey = "calc|" + inContext.iterProcess.databaseKey;
				if(!alreadyDoneFlags.contains(alreadyKey)) {
					alreadyDoneFlags.add(alreadyKey);

					detailStart = System.currentTimeMillis();
					calculateOpModeFractions(inContext); // steps 200-299
					detailEnd = System.currentTimeMillis();
					Logger.log(LogMessageCategory.DEBUG,"RatesOpModeDistributionGenerator.calculateOpModeFractions ms="+(detailEnd-detailStart));
				}
				
				alreadyKey = "rates|" + inContext.iterProcess.databaseKey;
				if(!alreadyDoneFlags.contains(alreadyKey)) {
					alreadyDoneFlags.add(alreadyKey);
					if(!isProjectDomain && !USE_EXTERNAL_GENERATOR_FOR_DRIVE_CYCLES) {
						/**
						 * @step 900
						 * @algorithm Update emission rate tables for Model Year Physics effects.
						 * In Project domain, only Extended Idle and APU are used in this generator.
						 * Since neither is affected by aerodynamics, only do this for Non-Project domain.
						 * @condition Non-Project domain
						**/
						detailStart = System.currentTimeMillis();
						modelYearPhysics.updateEmissionRateTables(db,inContext.iterProcess.databaseKey);
						detailEnd = System.currentTimeMillis();
						Logger.log(LogMessageCategory.DEBUG,"RatesOpModeDistributionGenerator.modelYearPhysics.updateEmissionRateTables ms="+(detailEnd-detailStart));
					}
				}
			} else {
				Logger.log(LogMessageCategory.ERROR, "Error while validating drive schedule "
						+ "distribution, rates operating mode computation cannot continue");
			}
			totalTime += System.currentTimeMillis() - start;
		} catch (Exception e) {
			Logger.logError(e,"Rates Operating Mode Distribution Generation failed.");
		} finally {
			DatabaseConnectionManager.checkInConnection(MOVESDatabaseType.EXECUTION, db);
			db = null;
		}

		Logger.log(LogMessageCategory.INFO,"ROMDG setupTime=" + setupTime + " bundleTime=" + totalTime);
	}

	/**
	 * Removes data from the execution database that was created by this object within executeLoop
	 * for the same context. This is only called after all other loopable objects that might use
	 * data created by executeLoop have had their executeLoop and cleanDataLoop functions called.
	 * @param context The MasterLoopContext that applies to this execution.
	**/
	public void cleanDataLoop(MasterLoopContext context) {
		// Do not remove data since it is needed across multiple processes
	}

	/**
	 * Determine the drive schedules that bracket each Average Speed Bin value.
	 * <p>Each average speed bin lies between (is bracketed) by the average speeds of two drive
	 * schedules. Determine which two drive schedules bracket the average speed bin and store the
	 * identity and average speeds of the two bins.  This is done for each source type, roadway
	 * type, day of week and hour of day for each average speed bin.</p>
	**/
	void bracketAverageSpeedBins() {
		String sql = "";

		ResultSet rs = null;
		try {
			//
			// The documentation doesn't mention this but, going from the spreadsheet, speed bins
			// with values below and above the lowest and highest drive schedule values are bound
			// to those values. The following query determines these bounded values.
			sql = "CREATE TABLE IF NOT EXISTS driveschedulebounds ("+
						"sourcetypeid     smallint,"+
						"roadtypeid       smallint,"+
						"scheduleboundlo  float,"+
						"scheduleboundhi  float,"+
						"unique index xpkdriveschedulebounds ("+
							"sourcetypeid, roadtypeid))";
			SQLRunner.executeSQL(db, sql);

			sql = "TRUNCATE driveschedulebounds";
			SQLRunner.executeSQL(db, sql);

			/**
			 * @step 100
			 * @algorithm Speed bins with values below and above the lowest and highest drive schedule values are bound
			 * to those values.
			 * scheduleBoundLo = min(averageSpeed). scheduleBoundHi = max(averageSpeed).
			 * @output DriveScheduleBounds
			 * @input DriveSchedule
			 * @input DriveScheduleAssoc
			 * @input RunSpecRoadType
			 * @input RunSpecSourceType
			**/
			sql = "INSERT INTO driveschedulebounds ("+
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

			SQLRunner.executeSQL(db,"ANALYZE TABLE driveschedulebounds");

			sql = "CREATE TABLE IF NOT EXISTS bracketschedulelo2 ("+
						"sourcetypeid     smallint,"+
						"roadtypeid       smallint,"+
						"avgspeedbinid    smallint,"+
						"drivescheduleid  smallint,"+
						"loschedulespeed  float,"+
						"isoutofbounds	  smallint,"+
						"unique index xpkbracketschedulelo2 ("+
							"sourcetypeid, roadtypeid, avgspeedbinid, drivescheduleid))";
			SQLRunner.executeSQL(db, sql);

			sql = "TRUNCATE bracketschedulelo2";
			SQLRunner.executeSQL(db, sql);

			/**
			 * @step 100
			 * @algorithm loScheduleSpeed = max(averageSpeed). isOutOfBounds = 0.
			 * @condition Non-ramps, Drive schedules with averageSpeed <= avgBinSpeed
			 * @output BracketScheduleLo2
			 * @input RunSpecRoadType
			 * @input RunSpecSourceType
			 * @input DriveSchedule
			 * @input DriveScheduleAssoc
			 * @input AvgSpeedBin
			**/
			sql = "INSERT INTO bracketschedulelo2 ("+
						"sourcetypeid,"+
						"roadtypeid,"+
						"avgspeedbinid,"+
						"loschedulespeed,"+
						"isoutofbounds) "+
					"select "+
						"dsa.sourcetypeid,"+
						"dsa.roadtypeid,"+
						"asb.avgspeedbinid,"+
						"max(ds.averagespeed),"+
						"0 as isoutofbounds "+
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

			SQLRunner.executeSQL(db,"ANALYZE TABLE bracketschedulelo2");

			/**
			 * @step 100
			 * @algorithm loScheduleSpeed = scheduleBoundLo. isOutOfBounds = 1.
			 * @condition Non-ramps, avgBinSpeed < scheduleBoundLo.
			 * @output BracketScheduleLo2
			 * @input RunSpecRoadType
			 * @input RunSpecSourceType
			 * @input DriveSchedule
			 * @input DriveScheduleAssoc
			 * @input AvgSpeedBin
			 * @input DriveScheduleBounds
			**/
			sql = "INSERT IGNORE INTO bracketschedulelo2 ("+
						"sourcetypeid,"+
						"roadtypeid,"+
						"avgspeedbinid,"+
						"loschedulespeed,"+
						"isoutofbounds) "+
					"select "+
						"dsa.sourcetypeid,"+
						"dsa.roadtypeid,"+
						"asb.avgspeedbinid,"+
						"dsb.scheduleboundlo,"+
						"1 as isoutofbounds "+
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

			sql = "CREATE TABLE IF NOT EXISTS bracketschedulelo ("+
					"sourcetypeid    smallint,"+
					"roadtypeid      smallint,"+
					"avgspeedbinid   smallint,"+
					"drivescheduleid smallint,"+
					"loschedulespeed float,"+
					"unique index xpkbracketschedulelo ("+
							"sourcetypeid, roadtypeid, avgspeedbinid, drivescheduleid))";
			SQLRunner.executeSQL(db, sql);

			sql = "TRUNCATE bracketschedulelo";
			SQLRunner.executeSQL(db, sql);

			/**
			 * @step 100
			 * @algorithm Find the drive cycle with an averageSpeed = loScheduleSpeed.
			 * @output BracketScheduleLo
			 * @input BracketScheduleLo2
			 * @input DriveSchedule
			 * @input DriveScheduleAssoc
			**/
			sql = "INSERT IGNORE INTO Bracketschedulelo ("+
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

			SQLRunner.executeSQL(db,"ANALYZE TABLE bracketschedulelo");

			sql = "CREATE TABLE IF NOT EXISTS bracketschedulehi2 ("+
					"sourcetypeid      smallint,"+
					"roadtypeid        smallint,"+
					"avgspeedbinid     smallint,"+
					"drivescheduleid   smallint,"+
					"hischedulespeed   float,"+
					"isoutofbounds     smallint,"+
					"unique index xpkbracketschedulehi2 ("+
							"sourcetypeid, roadtypeid, avgspeedbinid, drivescheduleid))";
			SQLRunner.executeSQL(db, sql);

			sql = "TRUNCATE bracketschedulehi2";
			SQLRunner.executeSQL(db, sql);

			/**
			 * @step 100
			 * @algorithm hiScheduleSpeed = min(averageSpeed). isOutOfBounds = 0.
			 * @condition Non-ramps, Drive schedules with averageSpeed > avgBinSpeed
			 * @output BracketScheduleHi2
			 * @input RunSpecRoadType
			 * @input RunSpecSourceType
			 * @input DriveSchedule
			 * @input DriveScheduleAssoc
			 * @input AvgSpeedBin
			**/
			sql = "INSERT INTO bracketschedulehi2 ("+
						"sourcetypeid,"+
						"roadtypeid,"+
						"avgspeedbinid,"+
						"hischedulespeed,"+
						"isoutofbounds) "+
					"select "+
						"dsa.sourcetypeid,"+
						"dsa.roadtypeid,"+
						"asb.avgspeedbinid,"+
						"min(ds.averagespeed),"+
						"0 as isoutofbounds "+
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

			SQLRunner.executeSQL(db,"ANALYZE TABLE bracketschedulehi2");

			/**
			 * @step 100
			 * @algorithm hiScheduleSpeed = scheduleBoundHi. isOutOfBounds = 1.
			 * @condition Non-ramps, avgBinSpeed > scheduleBoundHi.
			 * @output BracketScheduleHi2
			 * @input RunSpecRoadType
			 * @input RunSpecSourceType
			 * @input DriveSchedule
			 * @input DriveScheduleAssoc
			 * @input AvgSpeedBin
			 * @input DriveScheduleBounds
			**/
			sql = "INSERT IGNORE INTO bracketschedulehi2 ("+
						"sourcetypeid,"+
						"roadtypeid,"+
						"avgspeedbinid,"+
						"hischedulespeed,"+
						"isoutofbounds) "+
					"select "+
						"dsa.sourcetypeid,"+
						"dsa.roadtypeid,"+
						"asb.avgspeedbinid,"+
						"dsb.scheduleboundhi,"+
						"1 as isoutofbounds "+
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

			SQLRunner.executeSQL(db,"ANALYZE TABLE bracketschedulehi2");

			sql = "CREATE TABLE IF NOT EXISTS bracketschedulehi ("+
					"sourcetypeid      smallint,"+
					"roadtypeid        smallint,"+
					"avgspeedbinid     smallint,"+
					"drivescheduleid   smallint,"+
					"hischedulespeed   float,"+
					"unique index xpkbracketschedulehi ("+
							"sourcetypeid, roadtypeid, avgspeedbinid, drivescheduleid))";
			SQLRunner.executeSQL(db, sql);

			sql = "TRUNCATE bracketschedulehi";
			SQLRunner.executeSQL(db, sql);

			/**
			 * @step 100
			 * @algorithm Find the drive cycle with an averageSpeed = hiScheduleSpeed.
			 * @output BracketScheduleHi
			 * @input BracketScheduleHi2
			 * @input DriveSchedule
			 * @input DriveScheduleAssoc
			**/
			sql = "INSERT IGNORE INTO bracketschedulehi ("+
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

			SQLRunner.executeSQL(db,"ANALYZE TABLE bracketschedulehi");

			// Look for BracketScheduleLo2.isOutOfBounds=1 entries and complain.
			// Look for BracketScheduleHi2.isOutOfBounds=1 entries and complain.
			sql = "select distinct sourcetypeid, roadtypeid, avgspeedbinid, 1 as islow"
					+ " from bracketschedulelo2"
					+ " where isoutofbounds=1"
					+ " union"
					+ " select distinct sourcetypeid, roadtypeid, avgspeedbinid, 0 as islow"
					+ " from bracketschedulehi2"
					+ " where isoutofbounds=1";
			SQLRunner.Query query = new SQLRunner.Query();
			try {
				query.open(db,sql);
				while(query.rs.next()) {
					int sourceTypeID = query.rs.getInt(1);
					int roadTypeID = query.rs.getInt(2);
					int avgSpeedBinID = query.rs.getInt(3);
					boolean isLow = query.rs.getInt(4) > 0;

					String message = "";
					if(isLow) {
						message = "All driving cycles for avgSpeedBinID " + avgSpeedBinID
							+ " for sourcetype " + sourceTypeID
							+ " on roadtype " + roadTypeID
							+ " were too fast.";
					} else {
						message = "All driving cycles for avgSpeedBinID " + avgSpeedBinID
							+ " for sourcetype " + sourceTypeID
							+ " on roadtype " + roadTypeID
							+ " were too slow.";
					}
					if(CompilationFlags.ALLOW_DRIVE_CYCLE_EXTRAPOLATION) {
						message += " MOVES results for this speed were extrapolated from the closest available driving cycles.";
						Logger.log(LogMessageCategory.WARNING,message);
					} else {
						message += " MOVES cannot proceed.";
						Logger.log(LogMessageCategory.ERROR,message);
						MOVESEngine.terminalErrorFound();
					}
				}
			} finally {
				query.onFinally();
			}

			// Delete intermediate results for large tables. Normally, intermediate
			// results are kept when possible for debugging purposes.
			sql = "TRUNCATE bracketschedulelo2";
			SQLRunner.executeSQL(db, sql);

			// Delete intermediate results for potentially large tables. Normally, intermediate
			// results are kept when possible for debugging purposes.
			sql = "TRUNCATE bracketschedulehi2";
			SQLRunner.executeSQL(db, sql);
		} catch (SQLException e) {
			Logger.logSqlError(e,"Could not determine brackets for Average Speed Bins.", sql);
		} finally {
			if(rs != null) {
				try {
					rs.close();
				} catch (SQLException e) {
					// Failure to close on a ResultSet should not be an issue.
				}
				rs = null;
			}
		}
	}

	/**
	 * Determine proportions for bracketing drive schedules.
	**/
	void determineDriveScheduleProportions() {
		String sql = "";

		try {
			sql = "CREATE TABLE IF NOT EXISTS loschedulefraction ("+
					"sourcetypeid       smallint,"+
					"roadtypeid         smallint,"+
					"avgspeedbinid      smallint,"+
					"loschedulefraction float,"+
					"unique index xpkloschedulefraction ("+
							"sourcetypeid, roadtypeid, avgspeedbinid))";
			SQLRunner.executeSQL(db, sql);

			sql = "TRUNCATE loschedulefraction";
			SQLRunner.executeSQL(db, sql);

			/**
			 * @step 110
			 * @algorithm loScheduleFraction=(hiScheduleSpeed - avgBinSpeed) / (hiScheduleSpeed - loScheduleSpeed).
			 * @condition hiScheduleSpeed <> loScheduleSpeed
			 * @output LoScheduleFraction
			 * @input BracketScheduleLo
			 * @input BracketScheduleHi
			 * @input AvgSpeedBin
			**/
			sql = "INSERT INTO loschedulefraction ("+
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
						"bsh.hischedulespeed <> bsl.loschedulespeed";
			SQLRunner.executeSQL(db, sql);

			SQLRunner.executeSQL(db,"ANALYZE TABLE loschedulefraction");

			/**
			 * @step 110
			 * @algorithm loScheduleFraction=1.
			 * @condition hiScheduleSpeed = loScheduleSpeed
			 * @output LoScheduleFraction
			 * @input BracketScheduleLo
			 * @input BracketScheduleHi
			**/
			sql = "INSERT INTO loschedulefraction ("+
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
						"bsh.hischedulespeed = bsl.loschedulespeed";
			SQLRunner.executeSQL(db, sql);

			SQLRunner.executeSQL(db,"ANALYZE TABLE loschedulefraction");

			sql = "CREATE TABLE IF NOT EXISTS hischedulefraction ("+
					"sourcetypeid       smallint,"+
					"roadtypeid         smallint,"+
					"avgspeedbinid      smallint,"+
					"hischedulefraction float,"+
					"unique index xpkhischedulefraction ("+
							"sourcetypeid, roadtypeid, avgspeedbinid))";
			SQLRunner.executeSQL(db, sql);

			sql = "TRUNCATE hischedulefraction";
			SQLRunner.executeSQL(db, sql);

			/**
			 * @step 110
			 * @algorithm hiScheduleFraction = 1-loScheduleFraction.
			 * @output HiScheduleFraction
			 * @input BracketScheduleHi
			 * @input loScheduleFraction
			**/
			sql = "INSERT INTO hischedulefraction ("+
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
						"lsf.avgspeedbinid = bsh.avgspeedbinid";
			SQLRunner.executeSQL(db, sql);

			SQLRunner.executeSQL(db,"ANALYZE TABLE hischedulefraction");
		} catch (SQLException e) {
			Logger.logSqlError(e,"Could not determine fraction of drive schedules in each "
					+ "speed bin.", sql);
		}
	}

	/**
	 * Determine Distribution of Non Ramp Drive Schedules.
	 * <p>This step determines the distribution of drive schedules which represents the sum of
	 * all of the average speed bins. This is done for each source type, roadway type, day of
	 * week and hour of day.</p>
	**/
	void determineDriveScheduleDistributionNonRamp() {
		String sql = "";

		try {
			sql = "CREATE TABLE IF NOT EXISTS driveschedulefractionlo ("+
					"sourcetypeid          smallint,"+
					"roadtypeid            smallint,"+
					"avgspeedbinid         smallint,"+
					"drivescheduleid       smallint,"+
					"driveschedulefraction float,"+
					"unique index xpkdriveschedulefractionlo ("+
							"sourcetypeid, roadtypeid, avgspeedbinid, drivescheduleid))";
			SQLRunner.executeSQL(db, sql);

			sql = "TRUNCATE driveschedulefractionlo";
			SQLRunner.executeSQL(db, sql);

			/**
			 * @step 130
			 * @algorithm driveScheduleFraction=sum(loScheduleFraction).
			 * @output DriveScheduleFractionLo
			 * @input BracketScheduleLo
			 * @input LoScheduleFraction
			**/
			sql = "INSERT INTO driveschedulefractionlo ("+
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

			SQLRunner.executeSQL(db,"ANALYZE TABLE driveschedulefractionlo");

			sql = "CREATE TABLE IF NOT EXISTS driveschedulefractionhi ( "+
					"sourcetypeid          smallint, "+
					"roadtypeid            smallint, "+
					"avgspeedbinid         smallint, "+
					"drivescheduleid       smallint, "+
					"driveschedulefraction float, "+
					"unique index xpkdriveschedulefractionhi ( "+
					"sourcetypeid, roadtypeid, avgspeedbinid, drivescheduleid))";
			SQLRunner.executeSQL(db, sql);

			sql = "TRUNCATE driveschedulefractionhi";
			SQLRunner.executeSQL(db, sql);

			/**
			 * @step 130
			 * @algorithm driveScheduleFraction=sum(hiScheduleFraction).
			 * @output DriveScheduleFractionHi
			 * @input BracketScheduleHi
			 * @input HiScheduleFraction
			**/
			sql = "INSERT INTO driveschedulefractionhi ( "+
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

			SQLRunner.executeSQL(db,"ANALYZE TABLE driveschedulefractionhi");

			sql = "CREATE TABLE IF NOT EXISTS driveschedulefraction ("+
					"sourcetypeid          smallint,"+
					"roadtypeid            smallint,"+
					"avgspeedbinid         smallint,"+
					"drivescheduleid       smallint,"+
					"driveschedulefraction float,"+
					"unique index xpkdriveschedulefraction ("+
							"sourcetypeid, roadtypeid, avgspeedbinid, drivescheduleid))";
			SQLRunner.executeSQL(db, sql);

			/**
			 * @step 130
			 * @algorithm driveScheduleFraction = DriveScheduleFractionLo.driveScheduleFraction + DriveScheduleFractionHi.driveScheduleFraction.
			 * @output DriveScheduleFraction
			 * @input BracketScheduleHi
			 * @input DriveScheduleFractionLo
			 * @input DriveScheduleFractionHi
			 * @input RoadType
			 * @input DriveScheduleAssoc
			**/
			sql = "INSERT IGNORE INTO driveschedulefraction ( "+
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

			SQLRunner.executeSQL(db,"ANALYZE TABLE driveschedulefraction");

			sql = "INSERT IGNORE INTO driveschedulefraction ( "+
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
						"bsl.roadtypeid = rt.roadtypeid and "+
						"bsl.roadtypeid = dsa.roadtypeid and "+
						"bsl.drivescheduleid = dsa.drivescheduleid and "+
						"rt.roadtypeid = dsa.roadtypeid and  "+
						"bsl.drivescheduleid = dsfl.drivescheduleid and "+
						"bsl.drivescheduleid = dsa.drivescheduleid ";
			SQLRunner.executeSQL(db, sql);

			SQLRunner.executeSQL(db,"ANALYZE TABLE driveschedulefraction");

			sql = "INSERT IGNORE INTO driveschedulefraction ( "+
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

			SQLRunner.executeSQL(db,"ANALYZE TABLE driveschedulefraction");
		} catch(SQLException e) {
			Logger.logSqlError(e,"Could not determine the distribution of drive schedules for "
					+ "non ramp drive cycle.", sql);
		}
	}

	/**
	 * Calculate the second-by-second engine specific power.
	 * <p>This step calculates the engine specific power for each drive schedule for each source
	 * type.  This step could be limited to only those drive schedules needed for the specified
	 * source types and roadway types and indicated by the non-zero values for
	 * DriveScheduleFraction.
	**/
	void calculateEnginePowerBySecond() {
		String sql="";

		try {
			sql = "CREATE TABLE IF NOT EXISTS sourcetypedriveschedule ("+
					"sourcetypeid     smallint,"+
					"drivescheduleid  smallint,"+
					"unique index xpksourcetypedriveschedule ("+
							"sourcetypeid, drivescheduleid))";
			SQLRunner.executeSQL(db, sql);

			sql = "TRUNCATE sourcetypedriveschedule";
			SQLRunner.executeSQL(db, sql);

			/**
			 * @step 140
			 * @algorithm Get unique combinations of sourceTypeID and driveScheduleID.
			 * Only these combinations require VSP and operating mode computations.
			 * @condition driveScheduleFraction <> 0
			 * @output SourceTypeDriveSchedule
			 * @input DriveScheduleFraction
			**/
			sql = "INSERT INTO sourcetypedriveschedule ("+
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

			SQLRunner.executeSQL(db,"ANALYZE TABLE sourcetypedriveschedule");

			sql = "CREATE TABLE IF NOT EXISTS driveschedulefirstsecond ("+
					"drivescheduleid	smallint,"+
					"second			smallint,"+
					"unique index xpkdriveschedulefirstsecond ("+
							"drivescheduleid))";
			SQLRunner.executeSQL(db, sql);

			sql = "TRUNCATE driveschedulefirstsecond";
			SQLRunner.executeSQL(db, sql);

			/**
			 * @step 140
			 * @algorithm Find the first recorded second in each drive schedule.
			 * second=min(second).
			 * @output DriveScheduleFirstSecond
			 * @input DriveScheduleSecond
			**/
			sql = "INSERT INTO driveschedulefirstsecond ("+
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

			SQLRunner.executeSQL(db,"ANALYZE TABLE driveschedulefirstsecond");

			sql = "CREATE TABLE IF NOT EXISTS driveschedulesecond2 ("+
					"drivescheduleid smallint,"+
					"second          smallint,"+
					"speed           float,"+
					"acceleration    float,"+
					"unique index xpkdriveschedulesecond2 ("+
							"drivescheduleid, second))";
			SQLRunner.executeSQL(db, sql);

			sql = "TRUNCATE driveschedulesecond2";
			SQLRunner.executeSQL(db, sql);

			/**
			 * @step 140
			 * @algorithm Calculate speed and acceleration during the first second.
			 * The acceleration of the 1st second = the acceleration of the 2nd second.
			 * The acceleration calculation assumes that the grade = 0.
			 * speed[t] = speed[t] * 0.44704.
			 * acceleration[t] = (speed[t+1]-speed[t]) * 0.44704.
			 * @output DriveScheduleSecond2
			 * @input DriveScheduleSecond at time t
			 * @input DriveScheduleSecond at time t+1
			 * @input DriveScheduleFirstSecond
			**/
			sql = "INSERT INTO driveschedulesecond2 ("+
						"drivescheduleid,"+
						"second,"+
						"speed,"+
						"acceleration) "+
					"select "+
						"dsfs.drivescheduleid,"+
						"dsfs.second,"+
						"dss.speed * 0.44704,"+
						"(dss2.speed - dss.speed) * 0.44704 "+ // the accel of the 1st second = accel of 2nd second
					"from "+
						"driveschedulefirstsecond dsfs,"+
						"driveschedulesecond dss, "+
						"driveschedulesecond dss2 "+
					"where "+
						"dsfs.drivescheduleid = dss.drivescheduleid and "+
						"dss2.drivescheduleid = dss.drivescheduleid and "+
						"dsfs.second = dss.second and "+
						"dss2.second = dss.second+1";
			SQLRunner.executeSQL(db, sql);

			SQLRunner.executeSQL(db,"ANALYZE TABLE driveschedulesecond2");

			/**
			 * @step 140
			 * @algorithm Calculate speed and acceleration during the remaining seconds.
			 * speed[t] = speed[t] * 0.44704.
			 * acceleration[t] = (speed[t]-speed[t-1]) * 0.44704.
			 * @output DriveScheduleSecond2
			 * @input DriveScheduleSecond at time t
			 * @input DriveScheduleSecond at time t-1
			**/
			sql = "INSERT INTO driveschedulesecond2 ("+
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

			SQLRunner.executeSQL(db,"ANALYZE TABLE driveschedulesecond2");

			sql = "CREATE TABLE IF NOT EXISTS vsp ("+
					"sourcetypeid    smallint,"+
					"drivescheduleid smallint,"+
					"second          smallint,"+
					"vsp             float,"+
					"unique index xpkesp ("+
							"sourcetypeid, drivescheduleid, second))";
			SQLRunner.executeSQL(db, sql);

			sql = "TRUNCATE vsp";
			SQLRunner.executeSQL(db, sql);

			/**
			 * @step 140
			 * @algorithm Calculate VSP for each second of each drive schedule, using sourceTypeID-specific terms.
			 * VSP=(rollingTermA * speed + sut.rotatingTermB * POW(dss2.speed,2) + dragTermC * POW(speed,3) + sourceMass * speed * acceleration) / fixedMassFactor.
			 * @output VSP
			 * @input SourceTypeDriveSchedule
			 * @input DriveScheduleSecond2
			 * @input sourceUseTypePhysicsMapping
			**/
			sql = "INSERT INTO vsp ("+
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

			SQLRunner.executeSQL(db,"ANALYZE TABLE vsp");
		} catch (SQLException e) {
			Logger.logSqlError(e,"Could not calculate Engine Power Distribution.",sql);
		}
	}

	/**
	 * Determine the operating mode bin for each second.
	 * <p>The VSP value for each second is compared to the upper and lower bounds for the
	 * operating mode bins and a bin ID is assigned to each second. This is done for each
	 * source type, drive schedule and second.</p>
	**/
	void determineOpModeIDPerSecond() {
		String sql = "";
		PreparedStatement statement = null;
		try {
			sql = "CREATE TABLE IF NOT EXISTS driveschedulesecond3 ("+
					"drivescheduleid smallint,"+
					"second          smallint,"+
					"speed           float,"+
					"acceleration    float,"+
					"unique index xpkdriveschedulesecond3 ("+
							"drivescheduleid, second))";
			SQLRunner.executeSQL(db, sql);
			//System.out.println("######## Creating Drive Schedule Second 3 ##########");

			sql = "TRUNCATE driveschedulesecond3";
			SQLRunner.executeSQL(db, sql);

			// The following statement drops the 1st second of a drive cycle
			// because it does not join to any prior second.

			/**
			 * @step 150
			 * @algorithm Get the acceleration of every second beyond the first.
			 * acceleration[t] = speed[t] - speed[t-1].
			 * @output DriveScheduleSecond3
			 * @input DriveScheduleSecond for time t
			 * @input DriveScheduleSecond for time t-1
			**/
			sql = "INSERT INTO driveschedulesecond3 ("+
						"drivescheduleid,"+
						"second,"+
						"speed,"+
						"acceleration) "+
					"select "+
						"dss.drivescheduleid,"+
						"dss.second,"+
						"dss.speed,"+
						"dss.speed - dss2.speed "+ // current speed - previous second's speed
					"from "+
						"driveschedulesecond dss,"+
						"driveschedulesecond dss2 "+
					"where "+
						"dss.drivescheduleid = dss2.drivescheduleid and "+
						"dss2.second = dss.second - 1"; // dss2 is 1 second in the past
			SQLRunner.executeSQL(db, sql);

			// Add the 1st second using the acceleration of the 2nd second.
			// Use INSERT IGNORE so only the 1st second entries will be
			// altered. All other seconds already exist and will be ignored.

			/**
			 * @step 150
			 * @algorithm Get the acceleration of the first second.
			 * Use INSERT IGNORE so only the 1st second entries will be
			 * altered. All other seconds already exist and will be ignored.
			 * acceleration[t] = speed[t+1] - speed[t].
			 * @output DriveScheduleSecond3
			 * @input DriveScheduleSecond for time t
			 * @input DriveScheduleSecond for time t+1
			**/
			sql = "INSERT IGNORE INTO driveschedulesecond3 ("+
						"drivescheduleid,"+
						"second,"+
						"speed,"+
						"acceleration) "+
					"select "+
						"dss.drivescheduleid,"+
						"dss.second,"+
						"dss.speed,"+
						"dss2.speed - dss.speed "+ // future second's speed - current second's speed
					"from "+
						"driveschedulesecond dss,"+
						"driveschedulesecond dss2 "+
					"where "+
						"dss.drivescheduleid = dss2.drivescheduleid and "+
						"dss2.second = dss.second + 1"; // dss2 is 1 second in the future
			SQLRunner.executeSQL(db, sql);

			SQLRunner.executeSQL(db,"ANALYZE TABLE driveschedulesecond3");

			sql = "CREATE TABLE IF NOT EXISTS opmodeidbysecond ("+
					"sourcetypeid    smallint,"+
					"drivescheduleid smallint,"+
					"second          smallint,"+
					"opmodeid        smallint,"+
					"polprocessid    int,"+
					"speed           float,"+
					"acceleration    float,"+
					"vsp             float,"+
					"unique index xpkopmodeidbysecond ("+
							"sourcetypeid, drivescheduleid, second, polprocessid),"+
					"unique index xpkopmodeidbysecond2 ("+
							"sourcetypeid, drivescheduleid, polprocessid, second)"+
					//",key speed1 (opmodeid,polprocessid,vsp,speed)"+ // slower!
					")";
			SQLRunner.executeSQL(db, sql);

			sql = "TRUNCATE opmodeidbysecond";
			SQLRunner.executeSQL(db, sql);

			sql = "CREATE TABLE omdgpollutantprocess ("
					+ " polprocessid int not null,"
					+ " unique index pkxomdgpollutantprocess (polprocessid) )";
			SQLRunner.executeSQL(db, sql);

			/**
			 * @step 150
			 * @algorithm Get the distinct polProcessIDs that require operating modes.
			 * @output OMDGPollutantProcess
			 * @input OpModePolProcAssoc
			**/
			sql = "INSERT INTO omdgpollutantprocess (polprocessid)"
					+ " select distinct polprocessid"
					+ " from opmodepolprocassoc";
			SQLRunner.executeSQL(db, sql);

			// Note: We cannot remove anything that already has an operating mode distribution
			// because not all required links may have been provided.

			/**
			 * @step 150
			 * @algorithm Remove anything from OMDGPollutantProcess that has a representing pollutant/process.  Only
			 * its representing item should be calculated.
			 * @output OMDGPollutantProcess
			 * @input OMDGPolProcessRepresented
			**/
			sql = "delete from omdgpollutantprocess"
					+ " using omdgpollutantprocess"
					+ " inner join omdgpolprocessrepresented on (omdgpollutantprocess.polprocessid = omdgpolprocessrepresented.polprocessid)";
			SQLRunner.executeSQL(db, sql);

			SQLRunner.executeSQL(db,"ANALYZE TABLE omdgpollutantprocess");

			sql = "CREATE TABLE IF NOT EXISTS opmodepolprocassoctrimmed ("
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

			/**
			 * @step 150
			 * @algorithm Get the set of polProcessID and opModeID combinations that must
			 * be calculated.
			 * @output OpModePolProcAssocTrimmed
			 * @input OMDGPollutantProcess
			 * @input OpModePolProcAssoc
			**/
			sql = "insert into opmodepolprocassoctrimmed (polprocessid, opmodeid)"
					+ " select omp.polprocessid, omp.opmodeid"
					+ " from opmodepolprocassoc omp"
					+ " inner join omdgpollutantprocess pp on pp.polprocessid=omp.polprocessid";
			SQLRunner.executeSQL(db, sql);

			/**
			 * @step 150
			 * @algorithm Associate each second of each drive schedule with a polProcessID.
			 * Obtain the speed, acceleration, and VSP at each point. This facilitates easy
			 * opModeID assignment.
			 * @output OpModeIDBySecond
			 * @input DriveScheduleSecond3
			 * @input VSP
			 * @input OMDGPollutantProcess
			**/
			sql = "INSERT INTO opmodeidbysecond ("+
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

			SQLRunner.executeSQL(db,"ANALYZE TABLE opmodeidbysecond");

			sql = "DROP TABLE IF EXISTS opmodeidbysecond_temp";
			SQLRunner.executeSQL(db, sql);

			/**
			 * @step 150
			 * @algorithm Find braking events, copying information from time t that meet braking conditions
			 * and setting opModeID=0.
			 * @condition acceleration[t] <= -2 or (acceleration[t] < -1 and acceleration[t-1] < -1 and acceleration[t-2] < -1)
			 * @output OpModeIDBySecond_Temp
			 * @input OpModeIDBySecond for time t
			 * @input OpModeIDBySecond for time t-1
			 * @input OpModeIDBySecond for time t-2
			**/
			sql = "CREATE TABLE opmodeidbysecond_temp "+
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

			/**
			 * @step 150
			 * @algorithm Copy braking events back into OpModeIDBySecond.
			 * @output OpModeIDBySecond
			 * @input OpModeIDBySecond_Temp
			**/
			sql = "REPLACE INTO Opmodeidbysecond ( "+
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

			SQLRunner.executeSQL(db,"ANALYZE TABLE opmodeidbysecond");

			/**
			 * @step 150
			 * @algorithm opModeID=0, braking.
			 * @condition Any second in which the acceleration <= -2.
			 * @output OpModeIDBySecond
			**/
			sql = "UPDATE opmodeidbysecond SET opmodeid=0 WHERE acceleration <= -2";
			SQLRunner.executeSQL(db, sql);

			sql = "SELECT om.opmodeid, polprocessid, vsplower, vspupper, speedlower, speedupper " +
					"from operatingmode om inner join opmodepolprocassoctrimmed using (opmodeid) " +
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
				
				/**
				 * @step 150
				 * @algorithm Assign an opModeID to each second based upon operating mode VSP and speed
				 * information.
				 * @output OpModeIDBySecond
				 * @input OperatingMode
				 * @input OpModePolProcAssocTrimmed
				 * @condition 1 < opModeID < 100, opModeID not previously assigned
				**/
				sql = "UPDATE opmodeidbysecond SET opmodeid = " + opModeID;
				String whereClause = "";
				String vspClause = "";
				String speedClause = "";

				if(!isVSPLowerNull) {
					vspClause += "vsp >= " + vspLower;
				}
				if(!isVSPUpperNull) {
					if(vspClause.length() > 0) {
						vspClause += " AND ";
					}
					vspClause += "vsp < " + vspUpper;
				}
				if(!isSpeedLowerNull) {
					speedClause += "speed >= " + speedLower;
				}
				if(!isSpeedUpperNull) {
					if(speedClause.length() > 0) {
						speedClause += " AND ";
					}
					speedClause += "speed < " + speedUpper;
				}
				if(vspClause.length() > 0) {
					whereClause += "(" + vspClause + ")";
				}
				if(speedClause.length() > 0) {
					if(whereClause.length() > 0) {
						whereClause += " AND ";
					}
					whereClause += "(" + speedClause + ")";
				}
				sql += " WHERE " + whereClause + " AND polprocessid = " + polProcessID +
						" AND opmodeid IS NULL";
				SQLRunner.executeSQL(db, sql);
			}
			statement.close();
			statement = null;
			result.close();

			// Assign Idle to speed=0

			/**
			 * @step 150
			 * @algorithm Assign the Idle operating mode to speed=0.
			 * OpModeID=IF(speed=0 and polProcessID=11609,501,if(speed<1.0,1,opModeID)).
			 * @output OpModeIDBySecond
			**/
			sql = "UPDATE opmodeidbysecond SET opmodeid=IF(speed=0 and polprocessid=11609,501,if(speed<1.0,1,opmodeid))";
//					+ " where (speed=0 and polProcessID=11609) or (speed<1.0 and not (speed=0 and polProcessID=11609))";
			SQLRunner.executeSQL(db, sql);
		} catch (SQLException e) {
			Logger.logSqlError(e, "Could not determine Operating Mode ID distribution.", sql);
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
	 * Calculate operating mode fractions for each drive schedule.
	 * <p>Once all the seconds in each operating mode bin are known, the distribution of the bins
	 * can be determined. The sum of the operating mode fractions will add to one for each source
	 * type and drive schedule combination.  This is done for each source type and drive schedule.
	 * </p>
	**/
	void calculateOpModeFractionsPerDriveSchedule() {
		String sql = "";

		try {
			sql = "CREATE TABLE IF NOT EXISTS opmodefractionbyschedule2 ("+
					"sourcetypeid       smallint,"+
					"drivescheduleid    smallint,"+
					"polprocessid       int,"+
					"secondsum          smallint,"+
					"unique index xpkopmodefractionbyschedule2 ("+
							"sourcetypeid, drivescheduleid, polprocessid))";
			SQLRunner.executeSQL(db, sql);

			sql = "TRUNCATE opmodefractionbyschedule2";
			SQLRunner.executeSQL(db, sql);

			/**
			 * @step 160
			 * @algorithm secondSum=count(seconds in each drive schedule).
			 * @output OpModeFractionBySchedule2
			 * @input OpModeIDBySecond
			**/
			sql = "INSERT INTO opmodefractionbyschedule2 ("+
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

			SQLRunner.executeSQL(db,"ANALYZE TABLE opmodefractionbyschedule2");

			sql = "CREATE TABLE IF NOT EXISTS opmodefractionbyschedule ("+
					"sourcetypeid      smallint,"+
					"drivescheduleid   smallint,"+
					"polprocessid      int,"+
					"opmodeid          smallint,"+
					"modefraction      float,"+
					"unique index xpkopmodefractionbyschedule ("+
							"sourcetypeid, drivescheduleid, polprocessid, opmodeid))";
			SQLRunner.executeSQL(db, sql);

			sql = "TRUNCATE opmodefractionbyschedule";
			SQLRunner.executeSQL(db, sql);

			/**
			 * @step 160
			 * @algorithm modeFraction = count(seconds in each opModeID)/secondSum.
			 * @output OpModeFractionBySchedule
			 * @input OpModeFractionBySchedule2
			 * @input OpModeIDBySecond
			**/
			sql = "INSERT INTO opmodefractionbyschedule ("+
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

			SQLRunner.executeSQL(db,"ANALYZE TABLE opmodefractionbyschedule");

			sql = "TRUNCATE opmodefractionbyschedule2";
			SQLRunner.executeSQL(db, sql);
		} catch (SQLException e) {
			Logger.logSqlError(e, "Could not determine fractions of Operating Modes per Drive"
					+ " Schedule", sql);
		}
	}

	/**
	 * Calculate overall operating mode fractions.
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

				"CREATE TABLE IF NOT EXISTS opmodefraction2 ("+
					"sourcetypeid      smallint,"+
					"roadtypeid        smallint,"+
					"avgspeedbinid     smallint,"+
					"opmodeid          smallint,"+
					"polprocessid      int,"+
					"opmodefraction    float,"+
					"avgbinspeed       float default '0',"+
					"unique index xpkopmodefraction2 ("+
							"roadtypeid, sourcetypeid, avgspeedbinid, opmodeid, polprocessid))",
				// NOTE: above, roadTypeID is the first in the index so that it will be used
				// in calculateOpModeFractions which joins based on roadTypeID only.

				"CREATE TABLE IF NOT EXISTS opmodefraction2a ("+
					"sourcetypeid      smallint,"+
					"roadtypeid        smallint,"+
					"avgspeedbinid     smallint,"+
					"opmodeid          smallint,"+
					"polprocessid      int,"+
					"opmodefraction    float,"+
					"index idxopmodefraction2a ("+
							"roadtypeid, sourcetypeid, avgspeedbinid, opmodeid, polprocessid))",

				"truncate opmodefraction2",
				"truncate opmodefraction2a",

				/**
				 * @step 170
				 * @algorithm Add non-ramp-based information.
				 * opModeFraction = sum(modeFraction * driveScheduleFraction).
				 * @output OpModeFraction2a
				 * @input DriveScheduleFraction
				 * @input OpModeFractionBySchedule
				 * @input OpModePolProcAssocTrimmed
				 * @condition Not polProcessID 11710
				**/
				"INSERT INTO opmodefraction2a ("+
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

				"updateopmodefraction2a",

				"ANALYZE TABLE opmodefraction2a",

				/**
				 * @step 170
				 * @algorithm Aggregate ramp and non-ramp data.
				 * opModeFraction = sum(opModeFraction).
				 * @output OpModeFraction2
				 * @input OpModeFraction2a
				**/
				"INSERT INTO opmodefraction2 ("+
						"sourcetypeid,"+
						"roadtypeid,"+
						"avgspeedbinid,"+
						"opmodeid,"+
						"polprocessid,"+
						"opmodefraction) "+
					"select "+
						"sourcetypeid,"+
						"roadtypeid,"+
						"avgspeedbinid,"+
						"opmodeid,"+
						"polprocessid,"+
						"sum(opmodefraction) as opmodefraction "+
					"from "+
						"opmodefraction2a "+
					"group by "+
						"roadtypeid, sourcetypeid, avgspeedbinid, opmodeid, polprocessid",

				/**
				 * @step 170
				 * @algorithm Add running operating mode 300 to all source types.
				 * opModeFraction[opModeID=300]=1.
				 * @output OpModeFraction2
				 * @input OpModeFraction2a
				 * @input sourceUseTypePhysicsMapping
				**/
				"INSERT IGNORE INTO opmodefraction2 ("+
						"sourcetypeid,"+
						"roadtypeid,"+
						"avgspeedbinid,"+
						"opmodeid,"+
						"polprocessid,"+
						"opmodefraction) "+
					"select "+
						"sourcetypeid,"+
						"roadtypeid,"+
						"avgspeedbinid,"+
						"300 as opmodeid,"+
						"polprocessid,"+
						"1 as opmodefraction "+
					"from "+
						"(select distinct roadtypeid, avgspeedbinid, polprocessid from opmodefraction2a) t, "+
						"(select distinct realsourcetypeid as sourcetypeid from sourceusetypephysicsmapping union select distinct tempsourcetypeid as sourcetypeid from sourceusetypephysicsmapping) t2",
//					"GROUP BY "+
//						"roadTypeID, sourceTypeID, avgSpeedBinID, polProcessID",

				"update opmodefraction2, avgspeedbin set opmodefraction2.avgbinspeed=avgspeedbin.avgbinspeed where opmodefraction2.avgspeedbinid=avgspeedbin.avgspeedbinid",

				"ANALYZE TABLE opmodefraction2"
			};

			for(int i=0;i<statements.length;i++) {
				sql = statements[i];
				if(sql != null && sql.length() > 0) {
					if(sql.equalsIgnoreCase("updateopmodefraction2a")) {
						modelYearPhysics.updateOpModes(db,"opmodefraction2a");
					} else {
						SQLRunner.executeSQL(db,sql);
					}
				}
			}
		} catch (SQLException e) {
			Logger.logSqlError(e,"Could not determine final Operating Mode Distribution.",sql);
		}
	}

	/**
	 * Calculate overall operating mode fractions.
	 * @param inContext current iteration context
	**/
	void calculateOpModeFractions(MasterLoopContext inContext) {
		int processID = inContext.iterProcess.databaseKey;
		if(processID == 90) {
			calculateExtendedIdleOpModeFractions(); // steps 200-209
			return;
		} else if(processID == 91) {
			calculateAuxiliaryPowerOpModeFractions(); // steps 210-219
			return;
		} else if(processID == 1 && isProjectDomain) {
			return;
		}
		Logger.log(LogMessageCategory.DEBUG,"ROMD calculateOpModeFractions processID=" + processID);
		String sql = "";
		SQLRunner.Query query = new SQLRunner.Query();
		try {
			if(!USE_EXTERNAL_GENERATOR_FOR_DRIVE_CYCLES) {
				/**
				 * @step 220
				 * @algorithm Find [sourceTypeID, roadTypeID, avgSpeedBinID, polProcessID] combinations
				 * that already exist within RatesOpModeDistribution.
				 * @condition Non-Project domain Running Exhaust
				 * @output OMDGOMDKeys
				 * @input RatesOpModeDistribution
				 * @input PollutantProcessAssoc
				**/
				sql = "create table if not exists omdgomdkeys ( "+
						"	sourcetypeid smallint(6) not null default '0', "+
						"   avgspeedbinid smallint(6) not null default '0', "+
						"	roadtypeid smallint(6) not null default '0', "+
						"	polprocessid int not null default '0', "+
						"	primary key (avgspeedbinid, roadtypeid, polprocessid, sourcetypeid) "+
						")";
				SQLRunner.executeSQL(db, sql);
	
				sql = "truncate table omdgomdkeys";
				SQLRunner.executeSQL(db, sql);
	
				sql = "insert into omdgomdkeys (sourcetypeid, avgspeedbinid, roadtypeid, polprocessid) "+
						"select distinct romd.sourcetypeid, romd.avgspeedbinid, romd.roadtypeid, romd.polprocessid "+
						"from ratesopmodedistribution romd "+
						"inner join pollutantprocessassoc ppa on (ppa.polprocessid=romd.polprocessid) "+
						"where ppa.processid = " + processID;
				SQLRunner.executeSQL(db, sql);
	
				/**
				 * @step 220
				 * @algorithm Add to RatesOpModeDistribution those entries that don't already have records.
				 * Add records that do not appear within OMDGOMDKeys.
				 * @condition Non-Project domain Running Exhaust
				 * @output RatesOpModeDistribution
				 * @input OpModeFraction2
				 * @input OMDGOMDKeys
				 * @input runSpecHourDay
				**/
				sql = "INSERT IGNORE INTO ratesopmodedistribution ("+
							"sourcetypeid,"+
							"roadtypeid,"+
							"avgspeedbinid,"+
							"avgbinspeed,"+
							"polprocessid,"+
							"hourdayid,"+
							"opmodeid,"+
							"opmodefraction) "+
						"SELECT "+
							"omf2.sourcetypeid,"+
							"omf2.roadtypeid,"+
							"omf2.avgspeedbinid,"+
							"omf2.avgbinspeed,"+
							"omf2.polprocessid,"+
							"rshd.hourdayid,"+
							"omf2.opmodeid,"+
							"omf2.opmodefraction "+
						"FROM "+
							"opmodefraction2 omf2 "+
							"left outer join omdgomdkeys k on ( "+
								"k.roadtypeid=omf2.roadtypeid "+
								"and k.avgspeedbinid=omf2.avgspeedbinid "+
								"and k.polprocessid=omf2.polprocessid "+
								"and k.sourcetypeid=omf2.sourcetypeid "+
							") "+
							", runspechourday rshd "+
						"WHERE "+
							"k.roadtypeid is null "+
							"and k.polprocessid is null "+
							"and k.sourcetypeid is null "+
							"and k.avgspeedbinid is null";
				SQLRunner.executeSQL(db, sql);
	
				// Copy representing entries to those being represented, but only if those
				// being represented are not already present.
	
				/**
				 * @step 220
				 * @algorithm Clear all entries from OMDGOMDKeys.
				 * @condition Non-Project domain Running Exhaust
				 * @output OMDGOMDKeys
				**/
				sql = "truncate table omdgomdkeys";
				SQLRunner.executeSQL(db, sql);
	
				/**
				 * @step 220
				 * @algorithm Find [sourceTypeID, roadTypeID, avgSpeedBinID, polProcessID] combinations
				 * that already exist within RatesOpModeDistribution.
				 * @condition Non-Project domain Running Exhaust
				 * @output OMDGOMDKeys
				 * @input RatesOpModeDistribution
				 * @input PollutantProcessAssoc
				**/
				sql = "insert into Omdgomdkeys (sourcetypeid, avgspeedbinid, roadtypeid, polprocessid) "+
						"select distinct romd.sourcetypeid, romd.avgspeedbinid, romd.roadtypeid, romd.polprocessid "+
						"from ratesopmodedistribution romd "+
						"inner join pollutantprocessassoc ppa on (ppa.polprocessid=romd.polprocessid) "+
						"where ppa.processid = " + processID;
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
	
					/**
					 * @step 220
					 * @algorithm Copy representing entries to those being represented, but only if those
					 * being represented are not already present. Add to RatesOpModeDistribution those 
					 * entries that don't already have records. Add records that do not appear within OMDGOMDKeys.
					 * @condition Non-Project domain Running Exhaust
					 * @output RatesOpModeDistribution
					 * @input OpModeFraction2
					 * @input OMDGOMDKeys
					 * @input runSpecHourDay
					**/
					sql = "INSERT IGNORE INTO ratesopmodedistribution ("+
								"sourcetypeid,"+
								"roadtypeid,"+
								"avgspeedbinid,"+
								"avgbinspeed,"+
								"polprocessid,"+
								"hourdayid,"+
								"opmodeid,"+
								"opmodefraction) "+
							"select "+
								"omf2.sourcetypeid,"+
								"omf2.roadtypeid,"+
								"omf2.avgspeedbinid,"+
								"omf2.avgbinspeed,"+
								ppa + " as polprocessid,"+
								"rshd.hourdayid,"+
								"omf2.opmodeid,"+
								"omf2.opmodefraction "+
							"from "+
								"opmodefraction2 omf2 "+
								"left outer join omdgomdkeys k on ( "+
									"k.avgspeedbinid=omf2.avgspeedbinid "+
									"and k.polprocessid=" + ppa + " "+
									"and k.sourcetypeid=omf2.sourcetypeid "+
									"and k.roadtypeid=omf2.roadtypeid "+
								") "+
								", runspechourday rshd "+
							"where "+
								"k.avgspeedbinid is null "+
								"and k.polprocessid is null "+
								"and k.sourcetypeid is null "+
								"and k.roadtypeid is null "+
								"and omf2.polprocessid =" + repPPA;
					SQLRunner.executeSQL(db, sql);
				}
			}

			if(processID == 1) {
				if(USE_EXTERNAL_GENERATOR) {
					if(!shouldDeferForBaseRateGenerator) {
						if(runLocalExternalGenerator(inContext,"SourceTypePhysics.updateOperatingModeDistribution.RatesOpModeDistribution",null,null,null)) {
							Logger.log(LogMessageCategory.DEBUG,"Success running the external generator in RatesOpModeDistribution");
						} else {
							Logger.log(LogMessageCategory.ERROR,"Unable to run external generator in RatesOpModeDistribution");
						}
					}
				} else {
					/*
					Logger.log(LogMessageCategory.DEBUG,"Making backup copy of RatesOpModeDistribution into RatesOpModeDistributionSQL...");
					SQLRunner.executeSQL(db, "drop table if exists RatesOpModeDistributionSQLBackup");
					SQLRunner.executeSQL(db, "create table RatesOpModeDistributionSQLBackup like RatesOpModeDistribution");
					SQLRunner.executeSQL(db, "insert into RatesOpModeDistributionSQLBackup select * from RatesOpModeDistribution");
					*/
					Logger.log(LogMessageCategory.DEBUG,"Using SQL-based SourceTypePhysics.updateOperatingModeDistribution for RatesOpModeDistribution...");
					modelYearPhysics.updateOperatingModeDistribution(db,"ratesopmodedistribution");
					Logger.log(LogMessageCategory.DEBUG,"Done using SQL-based SourceTypePhysics.updateOperatingModeDistribution for RatesOpModeDistribution.");
				}
			}
			polProcessIDs = "";

			if(!USE_EXTERNAL_GENERATOR_FOR_DRIVE_CYCLES) {
				Logger.log(LogMessageCategory.DEBUG,"Analyzing RatesOpModeDistribution...");
				SQLRunner.executeSQL(db,"ANALYZE TABLE ratesopmodedistribution");
				Logger.log(LogMessageCategory.DEBUG,"Done analyzing RatesOpModeDistribution.");

				// Get distinct polProcessID in OpModeDistributionTemp as these are the ones to
				// be cleaned out of OpModeDistribution
				sql = "SELECT DISTINCT polprocessid from opmodefraction2";
				polProcessIDs = "";
				query.open(db,sql);
				while(query.rs.next()) {
					if(polProcessIDs.length() > 0) {
						polProcessIDs += ",";
					}
					polProcessIDs += query.rs.getString(1);
				}
				query.close();
			}
		} catch (SQLException e) {
			Logger.logSqlError(e,"Could not determine final Rates Operating Mode Distribution.",sql);
		} finally {
			query.onFinally();
		}
	}

	/**
	 * Calculate operating mode fractions for Extended Idle Exhaust (90).
	**/
	void calculateExtendedIdleOpModeFractions() {
		String sql = "";
		SQLRunner.Query query = new SQLRunner.Query();
		try {
			/**
			 * @step 200
			 * @algorithm opModeFraction=1.
			 * @condition Extended Idle
			 * @condition sourceTypeID=62 only
			 * @output RatesOpModeDistribution
			 * @input pollutantProcessAssoc
			 * @input sourceTypePolProcess
			 * @input opModePolProcessAssoc
			 * @input runSpecHourDay
			**/
			sql = "INSERT IGNORE INTO ratesopmodedistribution ("+
						"sourcetypeid,"+
						"roadtypeid,"+
						"avgspeedbinid,"+
						"avgbinspeed,"+
						"polprocessid,"+
						"hourdayid,"+
						"opmodeid,"+
						"opmodefraction) "+
					"select "+
						"stpp.sourcetypeid,"+
						"1 as roadtypeid,"+
						"0 as avgspeedbinid,"+
						"0 as avgbinspeed,"+
						"omppa.polprocessid,"+
						"rshd.hourdayid,"+
						"omppa.opmodeid,"+
						"1 as opmodefraction "+
					"from "+
						"pollutantprocessassoc ppa "+
						"inner join sourcetypepolprocess stpp on (stpp.polprocessid = ppa.polprocessid) "+
						"inner join opmodepolprocassoc omppa on (omppa.polprocessid = stpp.polprocessid) "+
						", runspechourday rshd "+
					"where "+
						"ppa.processid = 90 and stpp.sourcetypeid=62";
			SQLRunner.executeSQL(db, sql);

			/**
			 * @step 200
			 * @algorithm Add data for all extended idling (opModeID 200).
			 * opModeFraction[opModeID=200]=1.
			 * @condition Extended Idle
			 * @condition sourceTypeID=62 only
			 * @condition opModeFraction[opModeID=200] not already specified
			 * @output RatesOpModeDistribution
			 * @input pollutantProcessAssoc
			 * @input sourceTypePolProcess
			 * @input opModePolProcessAssoc
			 * @input runSpecHourDay
			**/
			sql = "INSERT IGNORE INTO ratesopmodedistribution ("+
						"sourcetypeid,"+
						"roadtypeid,"+
						"avgspeedbinid,"+
						"avgbinspeed,"+
						"polprocessid,"+
						"hourdayid,"+
						"opmodeid,"+
						"opmodefraction) "+
					"select "+
						"sourcetypeid,"+
						"1 as roadtypeid,"+
						"0 as avgspeedbinid,"+
						"0 as avgbinspeed,"+
						"polprocessid,"+
						"rshd.hourdayid,"+
						"200 as opmodeid,"+
						"1 as opmodefraction "+
					"from "+
						"pollutantprocessassoc,"+
						"runspecsourcetype "+
						", runspechourday rshd "+
					"where "+
						"processid = 90 and sourcetypeid=62";
			SQLRunner.executeSQL(db, sql);

			//modelYearPhysics.updateOperatingModeDistribution(db,"RatesOpModeDistribution");
			SQLRunner.executeSQL(db,"ANALYZE TABLE ratesopmodedistribution");
		} catch (SQLException e) {
			Logger.logSqlError(e,"Could not determine final Rates Operating Mode Distribution.",sql);
		} finally {
			query.onFinally();
		}
	}

	/**
	 * Calculate operating mode fractions for Auxiliary Power Exhaust (91).
	**/
	void calculateAuxiliaryPowerOpModeFractions() {
		String sql = "";
		SQLRunner.Query query = new SQLRunner.Query();
		try {
			/**
			 * @step 210
			 * @algorithm opModeFraction=1.
			 * @condition Auxiliary Power Exhaust
			 * @condition sourceTypeID=62 only
			 * @condition Not opModeID 200 (Extended Idle)
			 * @output RatesOpModeDistribution
			 * @input pollutantProcessAssoc
			 * @input sourceTypePolProcess
			 * @input opModePolProcessAssoc
			 * @input runSpecHourDay
			**/
			sql = "INSERT IGNORE INTO ratesopmodedistribution ("+
						"sourcetypeid,"+
						"roadtypeid,"+
						"avgspeedbinid,"+
						"avgbinspeed,"+
						"polprocessid,"+
						"hourdayid,"+
						"opmodeid,"+
						"opmodefraction) "+
					"select "+
						"stpp.sourcetypeid,"+
						"1 as roadtypeid,"+
						"0 as avgspeedbinid,"+
						"0 as avgbinspeed,"+
						"omppa.polprocessid,"+
						"rshd.hourdayid,"+
						"omppa.opmodeid,"+
						"1 as opmodefraction "+
					"from "+
						"pollutantprocessassoc ppa "+
						"inner join sourcetypepolprocess stpp on (stpp.polprocessid = ppa.polprocessid) "+
						"inner join opmodepolprocassoc omppa on (omppa.polprocessid = stpp.polprocessid) "+
						", runspechourday rshd "+
					"where "+
						"ppa.processid = 91 and omppa.opmodeid<>200 and stpp.sourcetypeid=62";
			SQLRunner.executeSQL(db, sql);

			/**
			 * @step 210
			 * @algorithm Add data for all hotelling except extended idling (opModeID 200).
			 * opModeFraction=1.
			 * @condition Auxiliary Power Exhaust
			 * @condition sourceTypeID=62 only
			 * @condition Not opModeID 200 (Extended Idle)
			 * @output RatesOpModeDistribution
			 * @input pollutantProcessAssoc
			 * @input runSpecSourceType
			 * @input runSpecHourDay
			 * @input hotellingActivityDistribution
			**/
			sql = "INSERT IGNORE INTO ratesopmodedistribution ("+
						"sourcetypeid,"+
						"roadtypeid,"+
						"avgspeedbinid,"+
						"avgbinspeed,"+
						"polprocessid,"+
						"hourdayid,"+
						"opmodeid,"+
						"opmodefraction) "+
					"select distinct "+
						"sourcetypeid,"+
						"1 as roadtypeid,"+
						"0 as avgspeedbinid,"+
						"0 as avgbinspeed,"+
						"polprocessid,"+
						"rshd.hourdayid,"+
						"opmodeid,"+
						"1 as opmodefraction "+
					"from "+
						"pollutantprocessassoc,"+
						"runspecsourcetype,"+
						"runspechourday rshd,"+
						"hotellingactivitydistribution "+
					"where "+
						"processid = 91 and opmodeid<>200 and sourcetypeid=62";
			SQLRunner.executeSQL(db, sql);

			SQLRunner.executeSQL(db,"ANALYZE TABLE ratesopmodedistribution");
		} catch (SQLException e) {
			Logger.logSqlError(e,"Could not determine final Rates Operating Mode Distribution.",sql);
		} finally {
			query.onFinally();
		}
	}
}
