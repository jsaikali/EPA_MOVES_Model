/**************************************************************************************************
 * @(#)TotalActivityGenerator.java  
 *
 *
 *
 *************************************************************************************************/
package gov.epa.otaq.moves.master.implementation.ghg;

import gov.epa.otaq.moves.common.*;
import gov.epa.otaq.moves.master.runspec.*;
import gov.epa.otaq.moves.master.framework.*;
import java.sql.SQLException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.*;
import java.io.*;

/**
 * This Class builds "Total Activity" records for ELDB data.
 *
 * ELDB is the Execution Location Database shown logically in the MOVES
 * data flow diagram.  The ELDB is implemented physically as tables in
 * MOVESExecution which have locational identifiers (stateID,
 * countyID, zoneID, linkID, gridID, or roadtypeID) in their primary keys.
 * In some contexts the ELDB may also be considered to consist of all
 * tables in MOVESExecution which are not in EERDB.
 *
 * The EERDB is The Execution Emission Rate Database shown logically in the
 * MOVES data flow diagram, physically implemented as the EmissionRate,
 * GREETWellToPump, and (eventually) the GREETManfAndDisposal tables in MOVESExecution.
 *
 * Finds the year with base population data that is closest to the analysis year.
 * Calculates the base year vehicle population by age.
 * Grows vehicle population from base year to analysis year.
 * Calculates the fraction of vehicle travel by HPMS type.
 * Grows VMT from the base year to the analysis year.
 * Allocates VMT by road type, source type, and age.
 * Temporarlly Allocates VMT to Hours.
 * Converts VMT to Total Activity Basis.
 * Calculates Starts and Source Hours Parked.
 * Allocates Total Activity Basis, Starts, SHP and Source Hours.
 * Calculates distance traveled corresponding to SourceHours Operating, when some
 * pollutant has been selected for the Running process.
 *
 * @author		Wesley Faler
 * @author    Chiu Foong, EPA
 * @author		Sarah Luo, ERG
 * @author		William Aikman (minor correction to scappage rate calculation)
 * @author		Mitch C. (minor mods for Tasks 128,133,135,Task 18 Item 169)
 * @version		2014-05-25
 * @author 		John Covey - Task 1806 changes
 * @version 	2018-03-20
**/
public class TotalActivityGenerator extends Generator {
	/**
	 * @algorithm
	 * @owner Total Activity Generator
	 * @generator
	**/

	/** Flag for whether the data tables have been cleared/setup **/
	boolean initialLoop = true;
	/** Database connection used by all functions.  Setup by executeLoop and cleanDataLoop. **/
	Connection db;
	/** Base year used for calculations. **/
	int baseYear = 0;
	/** Current year of result set **/
	int resultsYear = 0;
	/** ID of the last zone that Total Activity was generated for **/
	int currentZoneID = 0;
	/** Comma separated list of links performed. **/
	String linksInZone;
	/** ID of the last link that Total Activity was generated for **/
	int currentLinkID = 0;
	/** The current analysis year for the zone being processed **/
	int currentYearForZone = 0;
	/** The current emission process **/
	EmissionProcess currentProcess = null;
	/** The Running Exhaust emission process **/
	EmissionProcess runningExhaustProcess = EmissionProcess.findByName("Running Exhaust");
	/** The Start Exhaust emission process **/
	EmissionProcess startExhaustProcess = EmissionProcess.findByName("Start Exhaust");
	/** The Extended Idle emission process **/
	EmissionProcess extendedIdleProcess = EmissionProcess.findByName("Extended Idle Exhaust");
	/** The Auxiliary Power Exhaust process **/
	EmissionProcess auxiliaryPowerProcess = EmissionProcess.findByName("Auxiliary Power Exhaust");
	/** The Evap Permeation emission process **/
	EmissionProcess evapPermeationProcess = EmissionProcess.findByName("Evap Permeation");
	/** The Evap Fuel Vapor Venting emission process **/
	EmissionProcess evapFuelVaporVentingProcess = EmissionProcess.findByName("Evap Fuel Vapor Venting");
	/** The Evap Fuel Leaks emission process **/
	EmissionProcess evapFuelLeaksProcess = EmissionProcess.findByName("Evap Fuel Leaks");
	/** The Evap Non-Fuel Vapors emission process **/
	EmissionProcess evapNonFuelVaporsProcess = EmissionProcess.findByName("Evap Non-Fuel Vapors");
	/** The Brakewear emission process **/
	EmissionProcess brakeWearProcess = EmissionProcess.findByName("Brakewear");
	/** The Tirewear emission process **/
	EmissionProcess tireWearProcess = EmissionProcess.findByName("Tirewear");
	/** milliseconds spent during one time operations **/
	long setupTime = 0;
	/** milliseconds spent during growth operations **/
	long growthTime = 0;
	/** milliseconds spent during non-one-time and non-growth operations **/
	long totalTime = 0;
	/** milliseconds spent during a single operation as set during debugging/testing **/
	long focusTime = 0;
	/**
	 * Flags for tables, regions, and years that have been calcualted already.
	 * Data is formated as "table|regionid|year".
	**/
	TreeSetIgnoreCase calculationFlags = new TreeSetIgnoreCase();

	/** Default constructor **/
	public TotalActivityGenerator() {
	}

	/**
	 * Requests that this object subscribe to the given loop at desired looping points.
	 * Objects can assume that all necessary MasterLoopable objects have been instantiated.
	 *
	 * @param targetLoop The loop to subscribe to.
	**/
	public void subscribeToMe(MasterLoop targetLoop) {
		if(runningExhaustProcess != null
				&& ExecutionRunSpec.theExecutionRunSpec.doesHavePollutantAndProcess(
				null,"Running Exhaust")) {
			targetLoop.subscribe(this, runningExhaustProcess, MasterLoopGranularity.YEAR,
					MasterLoopPriority.GENERATOR-3); // Run after BaseRateGenerator
		}
		if(startExhaustProcess != null
				&& ExecutionRunSpec.theExecutionRunSpec.doesHavePollutantAndProcess(
				null,"Start Exhaust")) {
			targetLoop.subscribe(this, startExhaustProcess, MasterLoopGranularity.YEAR,
					MasterLoopPriority.GENERATOR);
		}
		if(extendedIdleProcess != null
				&& ExecutionRunSpec.theExecutionRunSpec.doesHavePollutantAndProcess(
				null,"Extended Idle Exhaust")) {
			targetLoop.subscribe(this, extendedIdleProcess, MasterLoopGranularity.YEAR,
					MasterLoopPriority.GENERATOR);
		}
		if(CompilationFlags.ENABLE_AUXILIARY_POWER_EXHAUST && auxiliaryPowerProcess != null
				&& ExecutionRunSpec.theExecutionRunSpec.doesHavePollutantAndProcess(
				null,"Auxiliary Power Exhaust")) {
			targetLoop.subscribe(this, auxiliaryPowerProcess, MasterLoopGranularity.YEAR,
					MasterLoopPriority.GENERATOR);
		}
		if(evapPermeationProcess != null
				&& ExecutionRunSpec.theExecutionRunSpec.doesHavePollutantAndProcess(
				null,"Evap Permeation")) {
			targetLoop.subscribe(this, evapPermeationProcess, MasterLoopGranularity.YEAR,
					MasterLoopPriority.GENERATOR);
		}
		if(evapFuelVaporVentingProcess != null
				&& ExecutionRunSpec.theExecutionRunSpec.doesHavePollutantAndProcess(
				null,"Evap Fuel Vapor Venting")) {
			targetLoop.subscribe(this, evapFuelVaporVentingProcess, MasterLoopGranularity.YEAR,
					MasterLoopPriority.GENERATOR);
		}
		if(evapFuelLeaksProcess != null
				&& ExecutionRunSpec.theExecutionRunSpec.doesHavePollutantAndProcess(
				null,"Evap Fuel Leaks")) {
			targetLoop.subscribe(this, evapFuelLeaksProcess, MasterLoopGranularity.YEAR,
					MasterLoopPriority.GENERATOR);
		}
		if(evapNonFuelVaporsProcess != null
				&& ExecutionRunSpec.theExecutionRunSpec.doesHavePollutantAndProcess(
				null,"Evap Non-Fuel Vapors")) {
			targetLoop.subscribe(this, evapNonFuelVaporsProcess, MasterLoopGranularity.YEAR,
					MasterLoopPriority.GENERATOR);
		}
		if(brakeWearProcess != null
				&& ExecutionRunSpec.theExecutionRunSpec.doesHavePollutantAndProcess(
				null,"Brakewear")) {
			targetLoop.subscribe(this, brakeWearProcess, MasterLoopGranularity.YEAR,
					MasterLoopPriority.GENERATOR);
		}
		if(tireWearProcess != null
				&& ExecutionRunSpec.theExecutionRunSpec.doesHavePollutantAndProcess(
				null,"Tirewear")) {
			targetLoop.subscribe(this, tireWearProcess, MasterLoopGranularity.YEAR,
					MasterLoopPriority.GENERATOR);
		}
	}

	/**
	 * Called each time the year changes.
	 *
	 * @param inContext The current context of the loop.
	**/
	public void executeLoop(MasterLoopContext inContext) {
		try {
			db = DatabaseConnectionManager.checkOutConnection(MOVESDatabaseType.EXECUTION);

			long start, focusStart;

			if(initialLoop) {
				start = System.currentTimeMillis();
				setup(inContext.iterProcess); // steps 100-109
				setupTime += System.currentTimeMillis() - start;
			}

			if(inContext.year > resultsYear) {
				start = System.currentTimeMillis();
				baseYear = determineBaseYear(inContext.year); // step 110
				if(baseYear > resultsYear) {
					calculateBaseYearPopulation(); // steps 120-129
				}

				growPopulationToAnalysisYear(inContext.year); // steps 130-139
				calculateFractionOfTravelUsingHPMS(inContext.year); // steps 140-149
				growVMTToAnalysisYear(inContext.year); // steps 150-159
				allocateVMTByRoadTypeSourceAge(inContext.year); // steps 160-169
				calculateVMTByRoadwayHour(inContext.year); // steps 170-179
				focusStart = System.currentTimeMillis();
				convertVMTToTotalActivityBasis(); // steps 180-189
				focusTime += System.currentTimeMillis() - focusStart;
				resultsYear = inContext.year;
				growthTime += System.currentTimeMillis() - start;
			}

			start = System.currentTimeMillis();
			allocateTotalActivityBasis(inContext); // steps 190-199
			calculateDistance(inContext); // steps 200-209
			initialLoop = false;
			totalTime += System.currentTimeMillis() - start;
		} catch (Exception e) {
			Logger.logError(e,"Total Activity Generation failed for year "+inContext.year);
		} finally {
			DatabaseConnectionManager.checkInConnection(MOVESDatabaseType.EXECUTION, db);
			db = null;
		}

		Logger.log(LogMessageCategory.INFO,"TAG setupTime=" + setupTime + " growthTime=" + growthTime
				+ " bundleTime=" + totalTime + " focusTime=" + focusTime);
	}

	/**
	 * Removes data from the execution database that was created by this object within executeLoop
	 * for the same context. This is only called after all other loopable objects that might use
	 * data created by executeLoop have had their executeLoop and cleanDataLoop functions called.
	 * @param context The MasterLoopContext that applies to this execution.
	**/
	public void cleanDataLoop(MasterLoopContext context) {
		// NOTE: Due to the data caching this object does, all cleanup is performed incrementally
		// ----- before writing new data.  The cleanup is done by clearActivityTables() and is
		// called by allocateTotalActivityBasis(...).
	}

	/**
	 * Create all the tables needed by the Total Activity Generator and purge any data left over
	 * in them from a previous run.
	 * @param initialProcess The emission process at the start of the loop.
	 * @throws SQLException If setup cannot be completed.
	**/
	void setup(EmissionProcess initialProcess) throws SQLException {
		String sql = "";

		// Keep track of the current emission process
		currentProcess = initialProcess;

		setupAnalysisYearVMTTables(db);
		setupAgeTables(db);

		//
		// The following tables contain data that are used during every loop.
		sql = "CREATE TABLE IF NOT EXISTS shobyageroadwayhour ("+
				"yearid         smallint not null,"+
				"roadtypeid     smallint not null,"+
				"sourcetypeid   smallint not null,"+
				"ageid          smallint not null,"+
				"monthid        smallint not null,"+
				"dayid          smallint not null,"+
				"hourid         smallint not null,"+
				"hourdayid      smallint not null default 0,"+
				"sho            double not null,"+
				"vmt            double not null,"+
				"unique index xpkshobyageroadwayhour ("+
					"yearid, roadtypeid, sourcetypeid, ageid, monthid, dayid, hourid))";
		SQLRunner.executeSQL(db, sql);

		sql = "TRUNCATE shobyageroadwayhour";
		SQLRunner.executeSQL(db, sql);

		sql = "CREATE TABLE IF NOT EXISTS startsbyagehour ("+
				"yearid         smallint not null,"+
				"sourcetypeid   smallint not null,"+
				"ageid          smallint not null,"+
				"monthid        smallint not null,"+
				"dayid          smallint not null,"+
				"hourid         smallint not null,"+
				"starts         double not null,"+
				"unique index xpkstartsbyagehour ("+
					"yearid, sourcetypeid, ageid, monthid, dayid, hourid))";
		SQLRunner.executeSQL(db,sql);

		sql = "TRUNCATE startsbyagehour";
		SQLRunner.executeSQL(db, sql);

		sql = "CREATE TABLE IF NOT EXISTS idlehoursbyagehour ("+
				"yearid         smallint not null,"+
				"sourcetypeid   smallint not null,"+
				"ageid          smallint not null,"+
				"monthid        smallint not null,"+
				"dayid          smallint not null,"+
				"hourid         smallint not null,"+
				"idlehours      double not null,"+
				"unique index xpkidlehoursbyagehour ("+
					"yearid, sourcetypeid, ageid, monthid, dayid, hourid))";
		SQLRunner.executeSQL(db,sql);

		sql = "TRUNCATE idlehoursbyagehour";
		SQLRunner.executeSQL(db,sql);

		//
		// The following tables contain data that should be cleaned out each loop.
		sql = "CREATE TABLE IF NOT EXISTS vmtbyageroadwayhour ("+
					"yearid        smallint not null,"+
					"roadtypeid    smallint not null,"+
					"sourcetypeid  smallint not null,"+
					"ageid         smallint not null,"+
					"monthid       smallint not null,"+
					"dayid         smallint not null,"+
					"hourid        smallint not null,"+
					"vmt           double not null,"+
					"hourdayid     smallint not null default 0,"+
					"unique index xpkvmtbyageroadwayhour("+
						"yearid, roadtypeid, sourcetypeid, ageid, monthid, dayid, hourid))";
		SQLRunner.executeSQL(db,sql);

		sql = "TRUNCATE vmtbyageroadwayhour";
		SQLRunner.executeSQL(db,sql);

		sql = "create table vmtbymyroadhourfraction ("
				+ " 	yearid smallint not null,"
				+ " 	roadtypeid smallint not null,"
				+ " 	sourcetypeid smallint not null,"
				+ " 	modelyearid smallint not null,"
				+ " 	monthid smallint not null,"
				+ " 	dayid smallint not null,"
				+ " 	hourid smallint not null,"
				+ " 	hourdayid smallint not null,"
				+ " 	vmtfraction double,"
				+ " 	unique key (yearid, roadtypeid, sourcetypeid, modelyearid, monthid, hourid, dayid),"
				+ " 	unique key (yearid, roadtypeid, sourcetypeid, modelyearid, monthid, hourdayid)"
				+ " )";
		SQLRunner.executeSQL(db,sql);

		sql = "TRUNCATE vmtbymyroadhourfraction";
		SQLRunner.executeSQL(db,sql);

		sql = "CREATE TABLE IF NOT EXISTS hpmsvtypepopulation ("+
					"yearid       smallint not null,"+
					"hpmsvtypeid  smallint not null,"+
					"population   float not null,"+
					"unique index xpkhpmsvtypepopulation("+
						"yearid, hpmsvtypeid))";
		SQLRunner.executeSQL(db,sql);

		sql = "TRUNCATE hpmsvtypepopulation";
		SQLRunner.executeSQL(db,sql);

		sql = "CREATE TABLE IF NOT EXISTS fractionwithinhpmsvtype ("+
					"yearid       smallint not null,"+
					"sourcetypeid smallint not null,"+
					"ageid        smallint not null,"+
					"fraction     float not null,"+
					"unique index xpkfractionwithinhpmsvtype ("+
						"yearid, sourcetypeid, ageid))";
		SQLRunner.executeSQL(db,sql);

		sql = "TRUNCATE fractionwithinhpmsvtype";
		SQLRunner.executeSQL(db,sql);

		sql = "CREATE TABLE IF NOT EXISTS hpmstravelfraction ("+
				"yearid      smallint not null,"+
				"hpmsvtypeid smallint not null,"+
				"fraction    float not null,"+
				"unique index xpkhpmstravelfraction ("+
					"yearid, hpmsvtypeid))";
		SQLRunner.executeSQL(db,sql);

		sql = "TRUNCATE hpmstravelfraction";
		SQLRunner.executeSQL(db,sql);

		sql = "CREATE TABLE IF NOT EXISTS travelfraction ("+
					"yearid        smallint not null,"+
					"sourcetypeid  smallint not null,"+
					"ageid         smallint not null,"+
					"fraction      float not null,"+
					"unique index xpktravelfraction("+
						"yearid, sourcetypeid, ageid))";
		SQLRunner.executeSQL(db,sql);

		sql = "TRUNCATE travelfraction";
		SQLRunner.executeSQL(db,sql);

		sql = "CREATE TABLE IF NOT EXISTS annualvmtbyageroadway ("+
				"yearid        smallint not null,"+
				"roadtypeid    smallint not null,"+
				"sourcetypeid  smallint not null,"+
				"ageid         smallint not null,"+
				"vmt           float not null,"+
				"unique index xpkannualvmtbyageroadway("+
					"yearid, roadtypeid, sourcetypeid, ageid))";
		SQLRunner.executeSQL(db,sql);

		sql = "TRUNCATE annualvmtbyageroadway";
		SQLRunner.executeSQL(db,sql);

		sql = "CREATE TABLE IF NOT EXISTS averagespeed ("+
				"roadtypeid    smallint not null,"+
				"sourcetypeid  smallint not null,"+
				"dayid         smallint not null,"+
				"hourid        smallint not null,"+
				"averagespeed  float not null,"+
				"unique index xpkaveragespeed ("+
					"roadtypeid, sourcetypeid, dayid, hourid))";
		SQLRunner.executeSQL(db,sql);

		sql = "TRUNCATE averagespeed";
		SQLRunner.executeSQL(db,sql);

		sql = "CREATE TABLE IF NOT EXISTS shobyageday ("+
					"yearid         smallint not null,"+
					"sourcetypeid   smallint not null,"+
					"ageid          smallint not null,"+
					"monthid        smallint not null,"+
					"dayid          smallint not null,"+
					"sho            double not null,"+
					"vmt            double not null,"+
					"unique index xpkshobyageday("+
						"yearid, sourcetypeid, ageid, monthid, dayid))";
		SQLRunner.executeSQL(db,sql);

		sql = "TRUNCATE shobyageday";
		SQLRunner.executeSQL(db,sql);

		sql = "CREATE TABLE IF NOT EXISTS vmtbyageroadwayday ("+
					"yearid         smallint not null,"+
					"roadtypeid     smallint not null,"+
					"sourcetypeid   smallint not null,"+
					"ageid          smallint not null,"+
					"monthid        smallint not null,"+
					"dayid          smallint not null,"+
					"vmt            double not null,"+
					"hotellinghours double not null,"+
					"primary key (yearid, roadtypeid, sourcetypeid, ageid, monthid, dayid))";
		SQLRunner.executeSQL(db,sql);

		sql = "TRUNCATE vmtbyageroadwayday";
		SQLRunner.executeSQL(db,sql);
/*
		sql = "CREATE TABLE IF NOT EXISTS SH ("+
					"hourDayID      SMALLINT NOT NULL,"+
					"monthID        SMALLINT NOT NULL,"+
					"yearID         SMALLINT NOT NULL,"+
					"ageID          SMALLINT NOT NULL,"+
					"zoneID         INTEGER NOT NULL,"+
					"sourceTypeID   SMALLINT NOT NULL,"+
					"roadTypeID     SMALLINT NOT NULL,"+
					"SH             FLOAT NOT NULL,"+
					"SHCV           FLOAT NOT NULL,"+
					"UNIQUE INDEX XPKSH("+
						"hourDayID,monthID,yearID,ageID,zoneID,sourceTypeID,roadTypeID))";
		SQLRunner.executeSQL(db,sql);

		sql = "TRUNCATE SH";
		SQLRunner.executeSQL(db,sql);
*/
		sql = "CREATE TABLE IF NOT EXISTS shp ("+
				"hourdayid         smallint not null,"+
				"monthid              smallint not null,"+
				"yearid               smallint not null,"+
				"ageid                smallint not null,"+
				"zoneid               integer not null,"+
				"sourcetypeid         smallint not null,"+
				"shp                  double null,"+
				"unique index xpksph("+
				"hourdayid, monthid, yearid, ageid, zoneid, sourcetypeid))";
		SQLRunner.executeSQL(db,sql);

		sql = "TRUNCATE shp";
		SQLRunner.executeSQL(db,sql);
	}

	/**
	 * Create and truncate the AnalysisYearVMT, and related, tables.
	 * @param db database to hold the tables
	 * @throws SQLException if anything goes wrong
	**/
	public static void setupAnalysisYearVMTTables(Connection db) throws SQLException {
		String sql = "";

		sql = "CREATE TABLE IF NOT EXISTS analysisyearvmt ("+
					"yearid      smallint not null,"+
					"hpmsvtypeid smallint not null,"+
					"vmt         float not null,"+
					"unique index xpkanalysisyearvmt ("+
						"yearid, hpmsvtypeid))";
		SQLRunner.executeSQL(db,sql);

		sql = "TRUNCATE analysisyearvmt";
		SQLRunner.executeSQL(db,sql);

		sql = "CREATE TABLE IF NOT EXISTS analysisyearvmt2 ("+
					"yearid      smallint not null,"+
					"hpmsvtypeid smallint not null,"+
					"vmt         float not null,"+
					"unique index analysisyearvmt2("+
						"yearid, hpmsvtypeid))";
		SQLRunner.executeSQL(db,sql);

		sql = "TRUNCATE analysisyearvmt2";
		SQLRunner.executeSQL(db,sql);
	}

	/**
	 * Remove AnalysisYearVMT, and related, tables.
	 * @param db database to hold the tables
	 * @throws SQLException if anything goes wrong
	**/
	public static void removeAnalysisYearVMTTables(Connection db) throws SQLException {
		String sql = "";

		sql = "drop table if exists analysisyearvmt";
		SQLRunner.executeSQL(db,sql);

		sql = "drop table if exists analysisyearvmt2";
		SQLRunner.executeSQL(db,sql);
	}

	/**
	 * Create and truncate the SourceTypeAgePopulation, and related, tables.
	 * @param db database to hold the tables
	 * @throws SQLException if anything goes wrong
	**/
	public static void setupAgeTables(Connection db) throws SQLException {
		String sql = "";

		//
		// Succeeding years may(if there is not an intervening base year) grow values from the
		// following tables to the current year so the data in these tables must be kept for
		// the full run.
		sql = "CREATE TABLE IF NOT EXISTS sourcetypeagepopulation ("+
					"yearid         smallint not null,"+
					"sourcetypeid   smallint not null,"+
					"ageid          smallint not null,"+
					"population     float not null,"+
					"unique index xpksourcetypeagepopulation ("+
						"yearid, sourcetypeid, ageid))";
		SQLRunner.executeSQL(db,sql);

		sql = "TRUNCATE sourcetypeagepopulation";
		SQLRunner.executeSQL(db,sql);

		sql = "CREATE TABLE IF NOT EXISTS sourcetypeagepopulation2 ("+
					"yearid         smallint not null,"+
					"sourcetypeid   smallint not null,"+
					"ageid          smallint not null,"+
					"population     float not null,"+
					"unique index xpksourcetypeagepopulation2 ("+
						"yearid, sourcetypeid, ageid))";
		SQLRunner.executeSQL(db,sql);

		sql = "TRUNCATE sourcetypeagepopulation2";
		SQLRunner.executeSQL(db,sql);
	}

	/**
	 * Remove SourceTypeAgePopulation, and related, tables.
	 * @param db database to hold the tables
	 * @throws SQLException if anything goes wrong
	**/
	public static void removeAgeTables(Connection db) throws SQLException {
		String sql = "";

		sql = "drop table if exists sourcetypeagepopulation";
		SQLRunner.executeSQL(db,sql);

		sql = "drop table if exists sourcetypeagepopulation2";
		SQLRunner.executeSQL(db,sql);
	}

	/**
	 * Tag-0: Find the year with base population data that is closest to the analysis year.
	 * @param analysisYear The year being analyzed.
	 * @return The base year for the year being analyzed.
	 * @throws Exception If the base year cannot be determined.
	**/
	int determineBaseYear(int analysisYear) throws Exception {
		return determineBaseYear(db, analysisYear);
	}

	/**
	 * Tag-0: Find the year with base population data that is closest to the analysis year.
	 * @param db database to be examined
	 * @param analysisYear The year being analyzed.
	 * @return The base year for the year being analyzed.
	 * @throws Exception If the base year cannot be determined.
	**/
	public static int determineBaseYear(Connection db, int analysisYear) throws Exception {
		String sql = "";
		ResultSet results = null;
		PreparedStatement statement = null;
		try {
			/**
			 * @step 110
			 * @algorithm baseYear = max(year) where year <= analysisYear and year is a base year.
			 * @input year
			**/
			sql = "SELECT "+
						"MAX(yearid) "+
					"FROM "+
						"year "+
					"WHERE "+
						"yearid <= ? AND "+
						"(isbaseyear = 'Y' OR isbaseyear = 'y')";
			statement=db.prepareStatement(sql);
			statement.setInt(1,analysisYear);

			results = SQLRunner.executeQuery(statement,sql);
			int maxYearID = 0;
			if(results!=null) {
				if(results.next()) {
					maxYearID = results.getInt(1);
					if(results.wasNull()) {
						maxYearID = 0;
					}
				}
			}
			if (maxYearID!=0) {
				//System.out.println("Base Year for " + analysisYear + " is " + maxYearID);
				return maxYearID;
			} else {
				throw new Exception("No base year found for analysis year " + analysisYear);
			}
		} finally {
			if(results != null) {
				try {
					results.close();
				} catch(SQLException e) {
					// Nothing to do here
				}
				results = null;
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

	/**
	 * Populate SourceTypeAgePopulation for a range of years
	 * @param db database to populate
	 * @param firstYear first year, inclusive, of the range to be populated
	 * @param lastYear last year, inclusive, of the range to be populated
	 * @throws Exception if the population cannot be grown or if there is no base year available
	**/
	public static void growPopulation(Connection db, int firstYear, int lastYear) throws Exception {
		int baseYear = 0;
		int resultsYear = 0;
		for(int y=firstYear;y<=lastYear;y++) {
			baseYear = determineBaseYear(db,y);
			if(baseYear > resultsYear) {
				calculateBaseYearPopulation(db,baseYear);
			}
			growPopulationToAnalysisYear(db,y,baseYear,resultsYear);
			resultsYear = y;
		}
	}

	/**
	 * Tag-1: Calculate the base year vehicle population by age.
	 * @throws SQLException If base year population cannot be determined.
	**/
	void calculateBaseYearPopulation() throws SQLException {
		calculateBaseYearPopulation(db,baseYear);
	}

	/**
	 * Calculate the base year vehicle population by age.
	 * @param db database to use
	 * @param baseYear year that is the basis for the population
	 * @throws SQLException If base year population cannot be determined.
	**/
	public static void calculateBaseYearPopulation(Connection db, int baseYear)
			throws SQLException {
		String sql = "";

		sql = "delete from sourcetypeagepopulation"
					+ " where yearid >= " + baseYear;
		SQLRunner.executeSQL(db,sql);

		/**
		 * @step 120
		 * @algorithm population = sourceTypePopulation * ageFraction.
		 * @output SourceTypeAgePopulation
		 * @input SourceTypeYear
		 * @input SourceTypeAgeDistribution
		**/
		sql = "INSERT INTO sourcetypeagepopulation ("+
					"yearid,"+
					"sourcetypeid,"+
					"ageid, "+
					"population) "+
				"select "+
					"sty.yearid,"+
					"sty.sourcetypeid,"+
					"stad.ageid, "+
					"sty.sourcetypepopulation * stad.agefraction "+
				"from "+
					"sourcetypeyear sty,"+
					"sourcetypeagedistribution stad "+
				"where "+
					"sty.sourcetypeid = stad.sourcetypeid and "+
					"sty.yearid = stad.yearid and "+
					"sty.yearid = " + baseYear;
		SQLRunner.executeSQL(db,sql);
	}

	/**
	 * Tag-2: Grow vehicle population from base year to analysis year.
	 * @param analysisYear the year to which the population data should be grown.
	 * @throws SQLException If population cannot be grown to the analysis year.
	**/
	void growPopulationToAnalysisYear(int analysisYear) throws SQLException {
		growPopulationToAnalysisYear(db,baseYear,resultsYear,analysisYear);
	}

	/**
	 * Grow vehicle population from base year to analysis year.
	 * @param db database to use
	 * @param baseYear year holding the population basis
	 * @param resultsYear any year between baseYear and analysisYear for which
	 * the population has already been calculated.  Set to 0 to indicate there has
	 * been no calculations since the base year.
	 * @param analysisYear the year to which the population data should be grown.
	 * @throws SQLException If population cannot be grown to the analysis year.
	**/
	public static void growPopulationToAnalysisYear(Connection db, int baseYear,
			int resultsYear, int analysisYear) throws SQLException {
		if(resultsYear < baseYear) {
			resultsYear = baseYear;
		}

		String sql = "";
		PreparedStatement statement = null;
		PreparedStatement copyStatement = null;
		PreparedStatement purgeStatement = null;
		PreparedStatement age0Statement = null;
		PreparedStatement ageXStatement = null;
		PreparedStatement age30PlusStatement = null;
		try {
			sql = "TRUNCATE sourcetypeagepopulation2";
			SQLRunner.executeSQL(db,sql);

			//
			// Setup the SQL strings used in the loops

			/**
			 * @step 130
			 * @algorithm Grow the age 0 population.
			 * population[ageID=0,y] = (population[y-1]/migrationRate[y-1])*salesGrowthFactor[y]*migrationRate[y].
			 * @output SourceTypeAgePopulation2
			 * @input SourceTypeYear for year y
			 * @input SourceTypeYear for year y-1
			 * @input SourceTypeAgePopulation for year y-1
			**/
			String age0Sql =
					"INSERT INTO sourcetypeagepopulation2 ("+
						"yearid,"+
						"sourcetypeid,"+
						"ageid,"+
						"population) "+
					"select "+
						"sty2.yearid,"+
						"sty.sourcetypeid,"+
						"stap.ageid,"+
						"(stap.population/sty.migrationrate)*sty2.salesgrowthfactor*"+
								"sty2.migrationrate "+
					"from "+
						"sourcetypeyear sty,"+
						"sourcetypeyear sty2,"+
						"sourcetypeagepopulation stap "+
					"where "+
						"sty.yearid = sty2.yearid-1 and "+
						"sty.sourcetypeid = stap.sourcetypeid and "+
						"sty2.yearid = ? and "+
						"sty2.sourcetypeid = stap.sourcetypeid and "+
						"stap.yearid = sty.yearid and "+
						"stap.ageid = 0 and "+
						"sty.migrationrate <> 0";
			age0Statement = db.prepareStatement(age0Sql);

			/**
			 * @step 130
			 * @algorithm Move the newly grown population to the main population table.
			 * @input SourceTypeAgePopulation2
			 * @output SourceTypeAgePopulation
			**/
			String copySql =
					"INSERT INTO sourcetypeagepopulation ("+
						"yearid,"+
						"sourcetypeid,"+
						"ageid,"+
						"population) "+
					"select "+
						"yearid,"+
						"sourcetypeid,"+
						"ageid,"+
						"population "+
					"from "+
						"sourcetypeagepopulation2";
			copyStatement = db.prepareStatement(copySql);

			String purgeSql = "TRUNCATE sourcetypeagepopulation2";
			purgeStatement = db.prepareStatement(purgeSql);

			/**
			 * @step 130
			 * @algorithm Grow the population for 1 <= ageID < 30.
			 * population[ageID,y] = population[y-1,ageID-1]*survivalRate[ageID]*migrationRate[y].
			 * @output SourceTypeAgePopulation2
			 * @input SourceTypeYear for year y
			 * @input SourceTypeAge
			 * @input SourceTypeAgePopulation for year y-1
			**/
			String ageXSql =
					"INSERT INTO sourcetypeagepopulation2 ("+
						"yearid,"+
						"sourcetypeid,"+
						"ageid,"+
						"population) "+
					"select "+
						"sty.yearid,"+
						"sty.sourcetypeid,"+
					 	"sta.ageid+0,"+
						"stap.population * sta.survivalrate * sty.migrationrate "+
					"from "+
						"sourcetypeyear sty, "+
						"sourcetypeage sta, "+
						"sourcetypeagepopulation stap "+
					"where "+
						"sty.yearid = ? and "+
						"sty.sourcetypeid = stap.sourcetypeid and "+
					 	"sta.ageid = ? and "+
						"sta.sourcetypeid = stap.sourcetypeid and "+
						"stap.yearid = sty.yearid-1 and "+
						"stap.ageid = sta.ageid-1";
			ageXStatement = db.prepareStatement(ageXSql);

			/**
			 * @step 130
			 * @algorithm Grow the population ageID >= 30.
			 * population[ageID,y] = population[ageID=29,y-1]*survivalRate[ageID=29]*migrationRate[y] + population[ageID=30,y]*survivalRate[ageID=30]*migrationRate[y].
			 * @output SourceTypeAgePopulation2
			 * @input sty SourceTypeYear for year y
			 * @input sta SourceTypeAge for ageID=29
			 * @input sta2 SourceTypeAge for ageID=30
			 * @input stap SourceTypeAgePopulation for year y-1 and ageID=29
			 * @input stap2 SourceTypeAgePopulation for year y and ageID=30
			**/
			String age30PlusSql =
					"INSERT INTO sourcetypeagepopulation2 ("+
						"yearid,"+
						"sourcetypeid,"+
						"ageid,"+
						"population) "+
					"select "+
						"sty.yearid,"+
						"sty.sourcetypeid,"+
						"sta2.ageid,"+
						"stap.population*sta.survivalrate*sty.migrationrate + stap2.population*"+
								"sta2.survivalrate*sty.migrationrate "+
					"from "+
						"sourcetypeyear sty,"+
						"sourcetypeage sta,"+
						"sourcetypeage sta2,"+
						"sourcetypeagepopulation stap,"+
						"sourcetypeagepopulation stap2 "+
					"where "+
						"sty.yearid = ? and "+
						"sty.sourcetypeid = stap.sourcetypeid and "+
						"sta.ageid = 29 and "+
						"sta.sourcetypeid = stap.sourcetypeid and "+
						"sta2.ageid = 30 and "+
						"sta2.sourcetypeid = stap.sourcetypeid and "+
						"sta.sourcetypeid = stap.sourcetypeid and "+
						"stap.yearid = sty.yearid-1 and "+
						"stap.ageid = 29 and "+
						"stap2.sourcetypeid = stap.sourcetypeid and "+
						"stap2.yearid = stap.yearid and "+
						"stap2.ageid = 30";
			age30PlusStatement = db.prepareStatement(age30PlusSql);

			int newYear = resultsYear;
			if(resultsYear<baseYear) {
				newYear = baseYear;
			}

			for (newYear=newYear+1;newYear<=analysisYear;newYear++) {
				age0Statement.setInt(1,newYear);
				SQLRunner.executeSQL(age0Statement,age0Sql);

				SQLRunner.executeSQL(copyStatement,copySql);

				SQLRunner.executeSQL(purgeStatement,purgeSql);

				for (int sourceAge=1;sourceAge<30;sourceAge++) {
					ageXStatement.setInt(1,newYear);
					ageXStatement.setInt(2,sourceAge);
					SQLRunner.executeSQL(ageXStatement,ageXSql);
				}

				age30PlusStatement.setInt(1,newYear);
				SQLRunner.executeSQL(age30PlusStatement,age30PlusSql);

				SQLRunner.executeSQL(copyStatement, copySql);

				SQLRunner.executeSQL(purgeStatement, purgeSql);
			}

			//
			// Source population data for years prior to the analysis year are no longer needed.
// Previous years cannot be deleted until everything above the year level has been run.
//			sql = "DELETE FROM SourceTypeAgePopulation WHERE "+
//					"	yearID<?";
//			statement.close();
//			statement = db.prepareStatement(sql);
//			statement.setInt(1,analysisYear);
//			SQLRunner.executeSQL(statement,sql);

			// Populate sourceTypeAgeDistribution with the distribution
			// within the analysis year.
			String[] statements = {
				"drop table if exists sourcetypeagepopulationtotal",
				
				"create table sourcetypeagepopulationtotal ("
				+ " 	sourcetypeid smallint not null,"
				+ " 	yearid smallint not null,"
				+ " 	totalpopulation double not null,"
				+ " 	primary key (yearid, sourcetypeid),"
				+ " 	unique key (sourcetypeid, yearid)"
				+ " )",

				/**
				 * @step 130
				 * @algorithm totalPopulation = sum(population).
				 * @output sourceTypeAgePopulationTotal
				 * @input sourceTypeagePopulation
				**/
				"insert into sourcetypeagepopulationtotal (sourcetypeid, yearid, totalpopulation)"
				+ " select sourcetypeid, yearid, sum(population)"
				+ " from sourcetypeagepopulation"
				+ " where yearid=" + analysisYear
				+ " group by yearid, sourcetypeid"
				+ " order by null",

				/**
				 * @step 130
				 * @algorithm ageFraction = population/totalPopulation.
				 * @output sourceTypeAgeDistribution
				 * @input sourceTypeAgePopulation
				 * @input sourceTypeAgePopulationTotal
				**/
				"insert ignore into sourcetypeagedistribution ("
				+ " 	sourcetypeid, yearid, ageid, agefraction)"
				+ " select detail.sourcetypeid, detail.yearid, detail.ageid, detail.population / total.totalpopulation"
				+ " from sourcetypeagepopulation detail"
				+ " inner join sourcetypeagepopulationtotal total on ("
				+ " 	total.sourcetypeid = detail.sourcetypeid"
				+ " 	and total.yearid = detail.yearid)"
				+ " where detail.yearid=" + analysisYear,

				"drop table if exists sourcetypeagepopulationtotal"
			};
			for(int i=0;i<statements.length;i++) {
				sql = statements[i];
				SQLRunner.executeSQL(db,sql);
			}
		} finally {
			if(statement!=null) {
				try {
					statement.close();
				} catch (SQLException e) {
					// Failure to close on a preparedStatment should not be an issue.
				}
			}
			if(copyStatement!=null) {
				try {
					copyStatement.close();
				} catch (SQLException e) {
					// Failure to close on a preparedStatment should not be an issue.
				}
			}
			if(purgeStatement!=null) {
				try {
					purgeStatement.close();
				} catch (SQLException e) {
					// Failure to close on a preparedStatment should not be an issue.
				}
			}
			if(age0Statement!=null) {
				try {
					age0Statement.close();
				} catch (SQLException e) {
					// Failure to close on a preparedStatment should not be an issue.
				}
			}
			if(ageXStatement!=null) {
				try {
					ageXStatement.close();
				} catch (SQLException e) {
					// Failure to close on a preparedStatment should not be an issue.
				}
			}
			if(age30PlusStatement!=null) {
				try {
					age30PlusStatement.close();
				} catch (SQLException e) {
					// Failure to close on a preparedStatment should not be an issue.
				}
			}
		}
	}

	/**
	 * Tag-3: Calculate the fraction of vehicle travel by HPMS type.
	 * @param analysisYear The current year being analyzed.
	 * @throws SQLException if Fraction of Travel by HPMS cannot be determined.
	**/
	void calculateFractionOfTravelUsingHPMS(int analysisYear) throws SQLException {
		String sql = "";

		sql = "TRUNCATE hpmsvtypepopulation";
		SQLRunner.executeSQL(db,sql);

		/**
		 * @step 140
		 * @algorithm population[HPMSVTypeID] = sum(population[sourceTypeID]).
		 * @output HPMSVTypePopulation
		 * @input SourceTypeAgePopulation
		 * @input SourceUseType
		**/
		sql = "INSERT INTO hpmsvtypepopulation ("+
					"yearid,"+
					"hpmsvtypeid,"+
					"population) "+
				"select "+
					"stap.yearid,"+
					"sut.hpmsvtypeid,"+
					"sum(stap.population) "+
				"from "+
					"sourcetypeagepopulation stap,"+
					"sourceusetype sut "+
				"where "+
					"stap.sourcetypeid = sut.sourcetypeid and "+
					"stap.yearid = " + analysisYear +
				" GROUP BY "+
					"stap.yearid,"+
					"sut.hpmsvtypeid";
		SQLRunner.executeSQL(db,sql);

		sql = "TRUNCATE fractionwithinhpmsvtype";
		SQLRunner.executeSQL(db,sql);

		/**
		 * @step 140
		 * @algorithm FractionWithinHPMSVType = population[sourceTypeID]/population[HPMSVTypeID].
		 * @output FractionWithinHPMSVType
		 * @input HPMSVTypePopulation
		 * @input SourceTypeAgePopulation
		 * @input SourceUseType
		**/
		sql = "INSERT INTO fractionwithinhpmsvtype ("+
					"yearid,"+
					"sourcetypeid,"+
					"ageid,"+
					"fraction) "+
				"select "+
					"stap.yearid,"+
					"stap.sourcetypeid,"+
					"stap.ageid,"+
					"stap.population / hvtp.population "+
				"from "+
					"sourcetypeagepopulation stap,"+
					"sourceusetype sut,"+
					"hpmsvtypepopulation hvtp "+
				"where "+
					"stap.sourcetypeid = sut.sourcetypeid and "+
					"sut.hpmsvtypeid = hvtp.hpmsvtypeid and "+
					"stap.yearid = hvtp.yearid and "+
					"hvtp.population <> 0";
		SQLRunner.executeSQL(db,sql);
									//
									// Since this table is joined with HPMSVTypePopulation
									// and HPMSVTypePopulation only contains data for the
									// analysisYear, there is no need to specify the
									// analysisYear here.

		sql = "TRUNCATE hpmstravelfraction";
		SQLRunner.executeSQL(db,sql);

		/**
		 * @step 140
		 * @algorithm HPMSTravelFraction = sum(relativeMAR * FractionWithinHPMSVType).
		 * @output HPMSTravelFraction
		 * @input FractionWithinHPMSVType
		 * @input SourceUseType
		 * @input SourceTypeAge
		**/
		sql = "INSERT INTO hpmstravelfraction ("+
					"yearid,"+
					"hpmsvtypeid,"+
					"fraction) "+
				"select "+
					"fwhvt.yearid,"+
					"sut.hpmsvtypeid,"+
					"sum(fwhvt.fraction * sta.relativemar) "+
				"from "+
					"fractionwithinhpmsvtype fwhvt,"+
					"sourceusetype sut,"+
					"sourcetypeage sta "+
				"where "+
					"sta.sourcetypeid = fwhvt.sourcetypeid and "+
					"sta.ageid = fwhvt.ageid and "+
					"fwhvt.sourcetypeid = sut.sourcetypeid "+
				"group by "+
					"fwhvt.yearid,"+
					"sut.hpmsvtypeid";
		SQLRunner.executeSQL(db,sql);

		sql = "TRUNCATE travelfraction";
		SQLRunner.executeSQL(db,sql);

		/**
		 * @step 140
		 * @algorithm TravelFraction = (FractionWithinHPMSVType * relativeMAR) / HPMSTravelFraction.
		 * @output TravelFraction
		 * @input HPMSTravelFraction
		 * @input FractionWithinHPMSVType
		 * @input SourceUseType
		 * @input SourceTypeAge
		**/
		sql = "INSERT INTO travelfraction ("+
					"yearid,"+
					"sourcetypeid,"+
					"ageid,"+
					"fraction) "+
				"select "+
					"fwhvt.yearid,"+
					"fwhvt.sourcetypeid,"+
					"fwhvt.ageid,"+
					"(fwhvt.fraction*sta.relativemar)/hpmstf.fraction "+
				"from "+
					"fractionwithinhpmsvtype fwhvt,"+
					"sourceusetype sut,"+
					"sourcetypeage sta,"+
					"hpmstravelfraction hpmstf "+
				"where "+
					"sta.sourcetypeid = fwhvt.sourcetypeid and "+
					"sta.ageid = fwhvt.ageid and "+
					"fwhvt.sourcetypeid = sut.sourcetypeid and "+
					"hpmstf.yearid = fwhvt.yearid and "+
					"hpmstf.hpmsvtypeid = sut.hpmsvtypeid and "+
					"hpmstf.fraction <> 0";
		SQLRunner.executeSQL(db,sql);

		// If VMT by source type has been provided, instead of by HPMSVType, then
		// normalize TravelFraction by year and sourcetype.
		sql = "select (select count(*) as howmany from sourcetypedayvmt)+(select count(*) as howmany from sourcetypeyearvmt)";
		if(SQLRunner.executeScalar(db,sql) > 0) {
			sql = "drop table if exists travelfractionsourcetypesum";
			SQLRunner.executeSQL(db,sql);
	
			/**
			 * @step 140
			 * @algorithm totalTravelFraction(yearID,sourceTypeID) = Sum(TravelFraction).
			 * @output TravelFractionSourceTypeSum
			 * @input TravelFraction
			 * @condition VMT provided by sourcetype not HPMSVType
			**/
			sql = "create table travelfractionsourcetypesum"
					+ " select yearid, sourcetypeid, sum(fraction) as totaltravelfraction"
					+ " from travelfraction"
					+ " group by yearid, sourcetypeid"
					+ " order by null";
			SQLRunner.executeSQL(db,sql);
	
			/**
			 * @step 140
			 * @algorithm When VMT by source type has been provided, normalize TravelFraction by year and source type.
			 * normalized TravelFraction = TravelFraction / totalTravelFraction.
			 * @output TravelFraction
			 * @input TravelFractionSourceTypeSum
			 * @condition VMT provided by sourcetype not HPMSVType
			**/
			sql = "update travelfraction, travelfractionsourcetypesum"
					+ " set fraction = case when totaltravelfraction > 0 then fraction / totaltravelfraction else 0 end"
					+ " where travelfraction.yearid = travelfractionsourcetypesum.yearid"
					+ " and travelfraction.sourcetypeid = travelfractionsourcetypesum.sourcetypeid";
			SQLRunner.executeSQL(db,sql);
		}
	}

	/**
	 * Tag-4: Grow VMT from the base year to the analysis year.
	 * @param analysisYear The year we are doing the analysis for
	 * @throws SQLException if the VMT cannot be grown to the analysis year.
	**/
	void growVMTToAnalysisYear(int analysisYear) throws SQLException {
		growVMTToAnalysisYear(db, analysisYear, baseYear, resultsYear, true);
	}

	/**
	 * Populate AnalysisYearVMT for each year in a range of years
	 * @param db database to populate
	 * @param firstYear first year, inclusive, of the range to be populated
	 * @param lastYear last year, inclusive, of the range to be populated
	 * @throws Exception if the VMT cannot be grown or if there is no base year available
	**/
	public static void growVMT(Connection db, int firstYear, int lastYear) throws Exception {
		int baseYear = 0;
		int resultsYear = 0;
		for(int y=firstYear;y<=lastYear;y++) {
			baseYear = determineBaseYear(db,y);
			growVMTToAnalysisYear(db,y,baseYear,resultsYear,false);
			resultsYear = y;
		}
	}

	/**
	 * Tag-4: Grow VMT from the base year to the analysis year.
	 * @param db database to use
	 * @param analysisYear The year we are doing the analysis for
	 * @param baseYear The base year for the year the analysis is for
	 * @param resultsYear the latest year yet calculated and already stored,
	 * 0 upon initial entry
	 * @throws SQLException if the VMT cannot be grown to the analysis year.
	**/
	public static void growVMTToAnalysisYear(Connection db, int analysisYear, int baseYear,
			int resultsYear, boolean shouldDeletePriorYears) throws SQLException {
		String sql = "";
		PreparedStatement statement = null;
		PreparedStatement copyStatement = null;
		PreparedStatement purgeStatement = null;
		try {
			if(baseYear > resultsYear) {
				sql = "DELETE FROM analysisyearvmt WHERE yearid >=" + baseYear;
				SQLRunner.executeSQL(db,sql);

				/**
				 * @step 150
				 * @algorithm VMT = HPMSBaseYearVMT.
				 * @output AnalysisYearVMT
				 * @input RunSpecSourceType
				 * @input SourceUseType
				 * @input HPMSVTypeYear
				**/
				sql = "INSERT IGNORE INTO analysisyearvmt ("+
							"yearid,"+
							"hpmsvtypeid,"+
							"vmt) "+
						"select "+
							"hvty.yearid,"+
							"hvty.hpmsvtypeid,"+
							"hvty.hpmsbaseyearvmt "+
						"from "+
							"runspecsourcetype rsst,"+
							"sourceusetype sut,"+
							"hpmsvtypeyear hvty "+
						"where "+
							"rsst.sourcetypeid = sut.sourcetypeid and "+
							"sut.hpmsvtypeid = hvty.hpmsvtypeid and "+
							"hvty.yearid = ?";
				statement = db.prepareStatement(sql);
				statement.setInt(1,baseYear);
				SQLRunner.executeSQL(statement,sql);
				statement.close();
			}

			sql = "TRUNCATE analysisyearvmt2";
			statement = db.prepareStatement(sql);
			SQLRunner.executeSQL(statement,sql);

			/**
			 * @step 150
			 * @algorithm Grow VMT one year.
			 * VMT[y] = VMT[y-1] * VMTGrowthFactor[y].
			 * @output AnalysisYearVMT2
			 * @input AnalysisYearVMT for year y-1
			 * @input HPMSVTypeYear for year y
			**/
			sql = "INSERT INTO analysisyearvmt2 ("+
						"yearid,"+
						"hpmsvtypeid,"+
						"vmt) "+
					"select "+
						"hvty.yearid,"+
						"ayv.hpmsvtypeid,"+
						"ayv.vmt * hvty.vmtgrowthfactor "+
					"from "+
						"analysisyearvmt ayv,"+
						"hpmsvtypeyear hvty "+
					"where "+
						"ayv.yearid = hvty.yearid-1 and "+
						"ayv.hpmsvtypeid = hvty.hpmsvtypeid and "+
						"hvty.yearid = ?";
			statement.close();
			statement = db.prepareStatement(sql);

			/**
			 * @step 150
			 * @algorithm Copy AnalysisYearVMT2 data into AnalysisYearVMT.
			 * @input AnalysisYearVMT2
			 * @output AnalysisYearVMT
			**/
			String copySql =
					"INSERT INTO analysisyearvmt ("+
						"yearid,"+
						"hpmsvtypeid,"+
						"vmt) "+
					"select "+
						"ayv2.yearid,"+
						"ayv2.hpmsvtypeid,"+
						"ayv2.vmt "+
					"from "+
						"analysisyearvmt2 ayv2";
			copyStatement = db.prepareStatement(copySql);

			String purgeSql = "TRUNCATE analysisyearvmt2";
			purgeStatement = db.prepareStatement(purgeSql);

			int newYear = resultsYear;
			if(newYear < baseYear) {
				newYear = baseYear;
			}

			for (newYear=newYear+1;newYear<=analysisYear;newYear++) {
				statement.setInt(1,newYear);
				SQLRunner.executeSQL(statement,sql);

				SQLRunner.executeSQL(copyStatement, copySql);
				SQLRunner.executeSQL(purgeStatement, purgeSql);
			}
			if(shouldDeletePriorYears) {
				//
				// VMT for years prior to the analysis year are no longer needed.
				sql = "DELETE FROM analysisyearvmt WHERE "+
							"yearid<?";
				statement.close();
				statement = db.prepareStatement(sql);
				statement.setInt(1,analysisYear);
				SQLRunner.executeSQL(statement,sql);
			}
		} finally {
			if(statement!=null) {
				try {
					statement.close();
				} catch (SQLException e) {
					// Failure to close on a preparedStatment should not be an issue.
				}
			}
			if(copyStatement!=null) {
				try {
					copyStatement.close();
				} catch (SQLException e) {
					// Failure to close on a preparedStatment should not be an issue.
				}
			}
			if(purgeStatement!=null) {
				try {
					purgeStatement.close();
				} catch (SQLException e) {
					// Failure to close on a preparedStatment should not be an issue.
				}
			}
		}
	}

	/**
	 * Tag-5: Allocate VMT by road type, source type, and age.
	 * @param yearID calendar year to be used
	 * @throws SQLException If VMT cannot be allocated by road type, source, and age.
	**/
	void allocateVMTByRoadTypeSourceAge(int yearID) throws SQLException {
		String sql = "";

		sql = "TRUNCATE annualvmtbyageroadway";
		SQLRunner.executeSQL(db,sql);

		/**
		 * @step 160
		 * @algorithm VMT = VMT * roadTypeVMTFraction * TravelFraction.
		 * @output AnnualVMTByAgeRoadway
		 * @input RoadType
		 * @input TravelFraction
		 * @input AnalysisYearVMT
		 * @input RoadTypeDistribution
		 * @input SourceUseType
		 * @condition VMT provided by HPMSVType
		**/
		sql = "INSERT INTO annualvmtbyageroadway ("+
					"yearid,"+
					"roadtypeid,"+
					"sourcetypeid,"+
					"ageid,"+
					"vmt) "+
				"select "+
					"tf.yearid,"+
					"rtd.roadtypeid,"+
					"tf.sourcetypeid,"+
					"tf.ageid,"+
					"ayv.vmt*rtd.roadtypevmtfraction*tf.fraction "+
				"from "+
					"roadtype rsrt,"+ // was runspecroadtype
					"travelfraction tf,"+
					"analysisyearvmt ayv,"+
					"roadtypedistribution rtd,"+
					"sourceusetype sut "+
				"where "+
					"rsrt.roadtypeid = rtd.roadtypeid and "+
					"ayv.yearid = tf.yearid and "+
					"tf.sourcetypeid = sut.sourcetypeid and "+
					"sut.hpmsvtypeid = ayv.hpmsvtypeid and "+
					"rtd.sourcetypeid = tf.sourcetypeid";
		SQLRunner.executeSQL(db,sql);

		/**
		 * @step 160
		 * @algorithm VMT = VMT * roadTypeVMTFraction * TravelFraction.
		 * @output AnnualVMTByAgeRoadway
		 * @input RoadType
		 * @input TravelFraction
		 * @input SourceTypeYearVMT
		 * @input RoadTypeDistribution
		 * @condition VMT provided by source type
		**/
		sql = "INSERT INTO annualvmtbyageroadway ("
				+ " 	yearid,"
				+ " 	roadtypeid,"
				+ " 	sourcetypeid,"
				+ " 	ageid,"
				+ " 	vmt)"
				+ " select"
				+ " 	tf.yearid,"
				+ " 	rtd.roadtypeid,"
				+ " 	tf.sourcetypeid,"
				+ " 	tf.ageid,"
				+ " 	v.vmt*rtd.roadtypevmtfraction*tf.fraction"
				+ " from"
				+ " 	roadtype rsrt,"
				+ " 	travelfraction tf,"
				+ " 	sourcetypeyearvmt v,"
				+ " 	roadtypedistribution rtd"
				+ " where"
				+ " 	rsrt.roadtypeid = rtd.roadtypeid and"
				+ " 	v.yearid = tf.yearid and"
				+ " 	tf.sourcetypeid = v.sourcetypeid and"
				+ " 	rtd.sourcetypeid = tf.sourcetypeid and"
				+ " 	v.yearid = " + yearID;
		SQLRunner.executeSQL(db,sql);
	}

	/**
	 * Tag-6: Temporarlly Allocate VMT to Hours
	 * @param analysisYear The year we are doing the analysis for
	 * @throws SQLException If VMT cannot be allocated to hours.
	**/
	void calculateVMTByRoadwayHour(int analysisYear) throws SQLException {
		WeeksInMonthHelper weekHelper = new WeeksInMonthHelper();
		String weeksPerMonthClause =
				weekHelper.getWeeksPerMonthSQLClause("avar.yearid","avar.monthid");
		String sql = "";

		sql = "TRUNCATE vmtbyageroadwayhour";
		SQLRunner.executeSQL(db,sql);

		sql = "DROP TABLE IF EXISTS avarmonth ";
		SQLRunner.executeSQL(db,sql);

		sql = "DROP TABLE IF EXISTS avarmonthday ";
		SQLRunner.executeSQL(db,sql);

		/**
		 * @step 170
		 * @algorithm Append monthVMTFraction to AnnualVMTByAgeRoadway.
		 * @output AvarMonth
		 * @input AnnualVMTByAgeRoadway
		 * @input MonthVMTFraction
		**/
		sql = "CREATE TABLE avarmonth " +
				"select avar.*, monthid, monthvmtfraction " +
				"from annualvmtbyageroadway as avar " +
				"inner join monthvmtfraction as mvf using (sourcetypeid)";
		SQLRunner.executeSQL(db,sql);

		sql = "CREATE INDEX index1 on avarmonth (sourcetypeid, monthid, roadtypeid) ";
		SQLRunner.executeSQL(db,sql);

		/**
		 * @step 170
		 * @algorithm Append dayVMTFraction to AvarMonth.
		 * @output AvarMonthDay
		 * @input AvarMonth
		 * @input DayVMTFraction
		**/
		sql = "CREATE TABLE avarmonthday " +
				"select avarm.*, dayid, dayvmtfraction, monthvmtfraction*dayvmtfraction as monthdayfraction " +
				"from avarmonth as avarm inner join dayvmtfraction as dvf " +
				"using (sourcetypeid, monthid, roadtypeid) ";
		SQLRunner.executeSQL(db,sql);

		sql = "CREATE INDEX index1 ON avarmonthday(sourcetypeid, roadtypeid, dayid) ";
		SQLRunner.executeSQL(db,sql);

		/**
		 * @step 170
		 * @algorithm Hourly VMT = Annual VMT * monthVMTFraction * dayVMTFraction * hourVMTFraction / weeksPerMonth.
		 * @output VMTByAgeRoadwayHour
		 * @input AvarMonthDay
		 * @input HourVMTFraction
		 * @input HourDay
		 * @condition Annual VMT provided, either by HPMSVType or sourceTypeID
		**/
		sql = "INSERT INTO vmtbyageroadwayhour (yearid, roadtypeid, sourcetypeid, " +
					"ageid, monthid, dayid, hourid, vmt, hourdayid) " +
				"select avar.yearid, avar.roadtypeid, avar.sourcetypeid, " +
					"avar.ageid, avar.monthid, avar.dayid, hvf.hourid, " +
//					"avar.vmt*avar.monthvmtfraction*avar.dayvmtfraction*hvf.hourvmtfraction " +
					"avar.vmt*avar.monthdayfraction*hvf.hourvmtfraction " +
					" / " + weeksPerMonthClause + ", "+
					"hd.hourdayid " +
				"from avarmonthday as avar inner join hourvmtfraction as hvf " +
				"using(sourcetypeid, roadtypeid, dayid) " +
				"inner join hourday hd on (hd.hourid=hvf.hourid and hd.dayid=avar.dayid)";
		SQLRunner.executeSQL(db,sql);

		/**
		 * @step 170
		 * @algorithm Hourly VMT = Daily VMT * hourVMTFraction * roadTypeVMTFraction * TravelFraction * NumberOfRealDays.
		 * @output VMTByAgeRoadwayHour
		 * @input SourceTypeDayVMT
		 * @input RoadTypeDistribution
		 * @input HourDay
		 * @input HourVMTFraction
		 * @input TravelFraction
		 * @input DayOfAnyWeek
		 * @condition Daily VMT provided by sourceTypeID
		**/
		sql = "insert ignore into vmtbyageroadwayhour (yearid, roadtypeid, sourcetypeid,"
				+ " 	ageid, monthid, dayid, hourid, vmt, hourdayid)"
				+ " select vmt.yearid, rtd.roadtypeid, vmt.sourcetypeid,"
				+ " 	tf.ageid, vmt.monthid, vmt.dayid, h.hourid,"
				+ " 	vmt.vmt*h.hourvmtfraction*rtd.roadtypevmtfraction*tf.fraction*dow.noofrealdays as vmt,"
				+ " 	hd.hourdayid"
				+ " from sourcetypedayvmt vmt"
				+ " inner join roadtypedistribution rtd on (rtd.sourcetypeid=vmt.sourcetypeid)"
				+ " inner join hourday hd on (hd.dayid=vmt.dayid)"
				+ " inner join hourvmtfraction h on (h.hourid=hd.hourid and h.roadtypeid=rtd.roadtypeid and h.sourcetypeid=rtd.sourcetypeid)"
				+ " inner join travelfraction tf on (tf.yearid=vmt.yearid and tf.sourcetypeid=rtd.sourcetypeid)"
				+ " inner join dayofanyweek dow on (dow.dayid=vmt.dayid)"
				+ " where vmt.yearid=" + analysisYear;
		SQLRunner.executeSQL(db,sql);

		/**
		 * @step 170
		 * @algorithm Hourly VMT = Daily VMT * hourVMTFraction * roadTypeVMTFraction * TravelFraction * NumberOfRealDays.
		 * @output VMTByAgeRoadwayHour
		 * @input HPMSVTypeDay
		 * @input SourceUseType
		 * @input RoadTypeDistribution
		 * @input HourDay
		 * @input HourVMTFraction
		 * @input TravelFraction
		 * @input DayOfAnyWeek
		 * @condition Daily VMT provided by HPMSVType
		**/
		sql = "insert ignore into vmtbyageroadwayhour (yearid, roadtypeid, sourcetypeid,"
				+ " 	ageid, monthid, dayid, hourid, vmt, hourdayid)"
				+ " select vmt.yearid, rtd.roadtypeid, sut.sourcetypeid,"
				+ " 	tf.ageid, vmt.monthid, vmt.dayid, h.hourid,"
				+ " 	vmt.vmt*h.hourvmtfraction*rtd.roadtypevmtfraction*tf.fraction*dow.noofrealdays as vmt,"
				+ " 	hd.hourdayid"
				+ " from hpmsvtypeday vmt"
				+ " inner join sourceusetype sut on (sut.hpmsvtypeid=vmt.hpmsvtypeid)"
				+ " inner join roadtypedistribution rtd on (rtd.sourcetypeid=sut.sourcetypeid)"
				+ " inner join hourday hd on (hd.dayid=vmt.dayid)"
				+ " inner join hourvmtfraction h on (h.hourid=hd.hourid and h.roadtypeid=rtd.roadtypeid and h.sourcetypeid=rtd.sourcetypeid)"
				+ " inner join travelfraction tf on (tf.yearid=vmt.yearid and tf.sourcetypeid=rtd.sourcetypeid)"
				+ " inner join dayofanyweek dow on (dow.dayid=vmt.dayid)"
				+ " where vmt.yearid=" + analysisYear;
		SQLRunner.executeSQL(db,sql);

		sql = "DROP TABLE IF EXISTS avarmonth ";
		SQLRunner.executeSQL(db,sql);

		sql = "DROP TABLE IF EXISTS avarmonthday ";
		SQLRunner.executeSQL(db,sql);

		sql = "drop table if exists vmtbymyroadhoursummary";
		SQLRunner.executeSQL(db,sql);

		sql = "create table vmtbymyroadhoursummary ("
				+ " 	yearid smallint not null,"
				+ " 	roadtypeid smallint not null,"
				+ " 	sourcetypeid smallint not null,"
				+ " 	monthid smallint not null,"
				+ " 	dayid smallint not null,"
				+ " 	hourid smallint not null,"
				+ " 	hourdayid smallint not null,"
				+ " 	totalvmt double,"
				+ " 	unique key (yearid, roadtypeid, sourcetypeid, monthid, hourid, dayid),"
				+ " 	unique key (yearid, roadtypeid, sourcetypeid, monthid, hourdayid)"
				+ " )";
		SQLRunner.executeSQL(db,sql);

		/**
		 * @step 170
		 * @algorithm totalVMT = sum(VMT).
		 * @output vmtByMYRoadHourSummary
		 * @input vmtByAgeRoadwayHour
		**/
		sql = "insert into vmtbymyroadhoursummary (yearid, roadtypeid, sourcetypeid,"
				+ " 	monthid, hourid, dayid, hourdayid, totalvmt)"
				+ " select yearid, roadtypeid, sourcetypeid,"
				+ " 	monthid, hourid, dayid, hourdayid,"
				+ " 	sum(vmt) as totalvmt"
				+ " from vmtbyageroadwayhour"
				+ " where yearid = " + analysisYear
				+ " group by yearid, roadtypeid, sourcetypeid, monthid, hourid, dayid"
				+ " having sum(vmt) > 0";
		SQLRunner.executeSQL(db,sql);

		/**
		 * @step 170
		 * @algorithm Find VMT fraction by model year.
		 * vmtFraction = VMT/totalVMT.
		 * @output vmtByMYRoadHourFraction
		 * @input vmtByMYRoadHourSummary
		 * @input vmtByAgeRoadwayHour
		**/
		sql = "insert into vmtbymyroadhourfraction (yearid, roadtypeid, sourcetypeid,"
				+ " 	modelyearid, monthid, hourid, dayid, hourdayid, vmtfraction)"
				+ " select s.yearid, s.roadtypeid, s.sourcetypeid,"
				+ " 	(v.yearid-v.ageid) as modelyearid, s.monthid, s.hourid, s.dayid, s.hourdayid, "
				+ " 	(vmt/totalvmt) as vmtfraction"
				+ " from vmtbymyroadhoursummary s"
				+ " inner join vmtbyageroadwayhour v using ("
				+ " 	yearid, roadtypeid, sourcetypeid, monthid, dayid, hourid)";
		SQLRunner.executeSQL(db,sql);
	}

	/**
	 * Tag-7: Convert VMT to Total Activity Basis
	 * Calculate Starts and Source Hours Parked.
	 * @throws SQLException If VMT cannot be converted to Total Activity Basis.
	**/
	void convertVMTToTotalActivityBasis() throws SQLException {
		long start = 0;

		String sql = "";

		start = System.currentTimeMillis();
		sql = "DROP TABLE IF EXISTS sourcetypehour2";
		SQLRunner.executeSQL(db,sql);

		/**
		 * @step 180
		 * @algorithm Remove unwanted days from SourceTypeHour.
		 * @output SourceTypeHour
		 * @input SourceTypeHour
		 * @input HourDay
		 * @input RunSpecDay
		**/
		sql = "delete from sourcetypehour"
				+ " where hourdayid not in (select hourdayid from hourday inner join runspecday using (dayid))";
		SQLRunner.executeSQL(db,sql);

		/**
		 * @step 180
		 * @algorithm Append dayID and hourID to SourceTypeHour.
		 * @output SourceTypeHour2
		 * @input SourceTypeHour
		 * @input HourDay
		**/
		sql= "CREATE TABLE sourcetypehour2 " +
				"select sth.sourcetypeid, hd.dayid, hd.hourid, sth.idleshofactor, sth.hotellingdist " +
				"from sourcetypehour as sth inner join hourday as hd using (hourdayid)";
		SQLRunner.executeSQL(db,sql);

		sql = "alter table sourcetypehour2 add key (sourcetypeid, dayid)";
		SQLRunner.executeSQL(db,sql);

		Logger.log(LogMessageCategory.INFO,"TAG SourceTypeHour2 ms=" + (System.currentTimeMillis()-start));

		sql = "TRUNCATE averagespeed";
		SQLRunner.executeSQL(db,sql);

		/**
		 * @step 180
		 * @algorithm averageSpeed = sum(avgBinSpeed * avgSpeedFraction).
		 * @output AverageSpeed
		 * @input RoadType
		 * @input RunSpecSourceType
		 * @input RunSpecDay
		 * @input HourOfAnyDay
		 * @input AvgSpeedBin
		 * @input AvgSpeedDistribution
		 * @input HourDay
		**/
		start = System.currentTimeMillis();
		sql = "INSERT INTO averagespeed ("+
					"roadtypeid,"+
					"sourcetypeid,"+
					"dayid,"+
					"hourid,"+
					"averagespeed) "+
				"select "+
					"asd.roadtypeid,"+
					"asd.sourcetypeid,"+
					"hd.dayid,"+
					"hd.hourid,"+
					"sum(asb.avgbinspeed*asd.avgspeedfraction) "+
				"from "+
					"roadtype rsrt,"+ // was runspecroadtype
					"runspecsourcetype rsst,"+
					"runspecday rsd,"+
					"hourofanyday rsh,"+ // "runspechour rsh,"+
					"avgspeedbin asb,"+
					"avgspeeddistribution asd,"+
					"hourday hd "+
				"where "+
					"rsrt.roadtypeid = asd.roadtypeid and "+
					"rsst.sourcetypeid = asd.sourcetypeid and "+
					"hd.dayid = rsd.dayid and "+
					"hd.hourid = rsh.hourid and "+
					"asb.avgspeedbinid = asd.avgspeedbinid and "+
					"asd.hourdayid = hd.hourdayid "+
				"group by "+
					"asd.roadtypeid,"+
					"asd.sourcetypeid,"+
					"hd.dayid,"+
					"hd.hourid";
		SQLRunner.executeSQL(db,sql);

		Logger.log(LogMessageCategory.INFO,"TAG averagespeed ms=" + (System.currentTimeMillis()-start));

		/**
		 * @step 180
		 * @algorithm SHO = VMT/averageSpeed where averageSpeed > 0, 0 otherwise.
		 * @output SHOByAgeRoadwayHour
		 * @input VMTByAgeRoadwayHour
		 * @input AverageSpeed
		**/
		start = System.currentTimeMillis();
		DatabaseUtilities.insertSelect(false,db,"shobyageroadwayhour",
					"yearid,"+
					"roadtypeid,"+
					"sourcetypeid,"+
					"ageid,"+
					"monthid,"+
					"dayid,"+
					"hourid,"+
					"hourdayid,"+
					"sho,"+
					"vmt",
				"select "+
					"varh.yearid,"+
					"varh.roadtypeid,"+
					"varh.sourcetypeid,"+
					"varh.ageid,"+
					"varh.monthid,"+
					"varh.dayid,"+
					"varh.hourid,"+
					"varh.hourdayid,"+
					"if(asp.averagespeed<>0,"+
					"coalesce(varh.vmt/asp.averagespeed,0.0),0.0),"+
					"varh.vmt "+
				"from vmtbyageroadwayhour varh "+
				"left join averagespeed asp on ("+
					"asp.roadtypeid = varh.roadtypeid and "+
					"asp.sourcetypeid = varh.sourcetypeid and "+
					"asp.dayid = varh.dayid and "+
					"asp.hourid = varh.hourid)");

		// Not needed, is only insert, so indexes will be all setup
		//SQLRunner.executeSQL(db,"analyze table SHOByAgeRoadwayHour");
		Logger.log(LogMessageCategory.INFO,"TAG shobyageroadwayhour ms=" + (System.currentTimeMillis()-start));

		// Calculate idle hours
		start = System.currentTimeMillis();

		/**
		 * @step 180
		 * @algorithm Find total VMT by day on Rural Restricted Access roads (roadTypeID=2)
		 * and Urban Restricted Access roads (roadTypeID=4) for Combination Long Haul Trucks (sourceTypeID=62).
		 * Daily VMT = sum(hourly VMT).
		 * hotellingHours = 0.
		 * @output VMTByAgeRoadwayDay
		 * @input VMTByAgeRoadwayHour
		**/
		sql = "insert ignore into vmtbyageroadwayday ("
				+ " 	yearid, roadtypeid, sourcetypeid, ageid, monthid, dayid, vmt, hotellinghours)"
				+ " select yearid, roadtypeid, sourcetypeid, ageid, monthid, dayid, sum(vmt), 0 as hotellinghours"
				+ " from vmtbyageroadwayhour"
				+ " where roadtypeid in (2,4) and sourcetypeid=62"
				+ " group by yearid, roadtypeid, sourcetypeid, ageid, monthid, dayid"
				+ " order by null";
		SQLRunner.executeSQL(db,sql);

		/**
		 * @step 180
		 * @algorithm hotellingHours = Daily VMT * hotellingRate.
		 * @output VMTByAgeRoadwayDay
		 * @input hotellingCalendarYear
		 * VMT = VMT * shoallocfactor from zoneroadtype table (join)
		**/
		sql = "update vmtbyageroadwayday, hotellingcalendaryear, zoneroadtype"
				+ " set hotellinghours = vmt * zoneroadtype.shoallocfactor * hotellingrate"
				+ " where vmtbyageroadwayday.yearid = hotellingcalendaryear.yearid"
				+ " and vmtbyageroadwayday.roadtypeid = zoneroadtype.roadtypeid"
				+ " and vmtbyageroadwayday.roadtypeid in (2,4)";
		SQLRunner.executeSQL(db,sql);

		/**
		 * @step 180
		 * @algorithm idleHours = hotellingHours * hotellingDist.
		 * @output IdleHoursByAgeHour
		 * @input VMTByAgeRoadwayDay
		 * @input SourceTypeHour2
		**/
		sql = "insert ignore into idlehoursbyagehour ("
				+ " 	yearid,sourcetypeid,ageid,"
				+ " 	monthid,dayid,hourid,idlehours)"
				+ " select v.yearid,v.sourcetypeid,v.ageid,"
				+ " 	v.monthid,sth.dayid,sth.hourid,sum(v.hotellinghours*sth.hotellingdist)"
				+ " from vmtbyageroadwayday as v"
				+ " inner join sourcetypehour2 as sth using (sourcetypeid, dayid)"
				+ " where v.roadtypeid in (2,4)"
				+ " group by v.yearid,v.sourcetypeid,v.ageid,"
				+ " 	v.monthid,sth.dayid,sth.hourid";
		SQLRunner.executeSQL(db,sql);

		SQLRunner.executeSQL(db,"analyze table idlehoursbyagehour");
		Logger.log(LogMessageCategory.INFO,"TAG IdleHoursByAgeHour ms=" + (System.currentTimeMillis()-start));

		sql = "DROP TABLE IF EXISTS sourcetypehour2";
		SQLRunner.executeSQL(db,sql);

		// Calculate starts
		if(initialLoop) {
			start = System.currentTimeMillis();
			sql = "DROP TABLE IF EXISTS startspersamplevehicle";
			SQLRunner.executeSQL(db,sql);

			/**
			 * @step 180
			 * @algorithm Find the number of starts for each sample vehicle.
			 * starts = count(trips) * noOfRealDays.
			 * @output StartsPerSampleVehicle
			 * @input SampleVehicleDay
			 * @input SampleVehicleTrip
			 * @input HourDay
			 * @input DayOfAnyWeek
			 * @condition Ignore marker trips
			**/
			sql = "CREATE TABLE startspersamplevehicle " +
					"select sv.sourcetypeid, hd.hourdayid, " +
					"(count(*)*noofrealdays) as starts, hd.dayid " +
					"from samplevehicleday sv " +
					"inner join samplevehicletrip svt using (vehid) " +
					"inner join hourday hd on (hd.dayid=svt.dayid and hd.hourid=svt.hourid) " +
					"inner join dayofanyweek d on (d.dayid=hd.dayid) " +
					"where svt.keyontime is not null " + // ignore marker trips
					"group by sv.sourcetypeid, hd.hourdayid " +
					"order by null";
			SQLRunner.executeSQL(db,sql);

			Logger.log(LogMessageCategory.INFO,"TAG StartsPerSampleVehicle ms=" + (System.currentTimeMillis()-start));

			start = System.currentTimeMillis();
			sql = "DROP TABLE IF EXISTS sourcetypesinstartspervehicle";
			SQLRunner.executeSQL(db,sql);

			sql = "CREATE TABLE sourcetypesinstartspervehicle select sourcetypeid from " +
					"startspervehicle group by sourcetypeid order by null";
			SQLRunner.executeSQL(db,sql);

			Logger.log(LogMessageCategory.INFO,"TAG SourceTypesInStartsPerVehicle ms=" + (System.currentTimeMillis()-start));

			/**
			 * @step 180
			 * @algorithm startsPerVehicle = starts / count(sample vehicles).
			 * @output StartsPerVehicle
			 * @input SampleVehicleDay
			 * @input StartsPerSampleVehicle
			**/
			start = System.currentTimeMillis();
			sql = "INSERT INTO startspervehicle(sourcetypeid, hourdayid, startspervehicle, " +
					"startspervehiclecv) " +
					"select sv.sourcetypeid, ssv.hourdayid, " +
					"starts/count(vehid) as startspervehicle,0 " +
					"from samplevehicleday sv " +
					"inner join startspersamplevehicle ssv on (ssv.sourcetypeid = " +
					"sv.sourcetypeid and ssv.dayid=sv.dayid) " +
					"left join sourcetypesinstartspervehicle stsv on " +
					"(stsv.sourcetypeid =  sv.sourcetypeid) " +
					"where stsv.sourcetypeid is null " +
					"group by sv.sourcetypeid, ssv.hourdayid " +
					"order by null";
			int rows = SQLRunner.executeSQL(db,sql);
			Logger.log(LogMessageCategory.INFO,"TAG StartsPerVehicle ms=" + (System.currentTimeMillis()-start));

			sql = "DROP TABLE IF EXISTS startspersamplevehicle";
			SQLRunner.executeSQL(db,sql);
		}

		start = System.currentTimeMillis();
		sql = "DROP TABLE IF EXISTS startsbyagehour";
		SQLRunner.executeSQL(db,sql);

		/**
		 * @step 180
		 * @algorithm starts = population * startsPerVehicle.
		 * @output StartsByAgeHour
		 * @input SourceTypeAgePopulation
		 * @input StartsPerVehicle
		**/
		sql = "CREATE TABLE IF NOT EXISTS startsbyagehour " +
				"select stap.sourcetypeid, yearid, " +
				"hourdayid, ageid, population*startspervehicle as starts " +
				"from sourcetypeagepopulation stap " +
				"inner join startspervehicle using (sourcetypeid)";
		int rows = SQLRunner.executeSQL(db,sql);

		SQLRunner.executeSQL(db,"analyze table startsbyagehour");

		Logger.log(LogMessageCategory.INFO,"TAG StartsByAgeHour ms=" + (System.currentTimeMillis()-start));

		start = System.currentTimeMillis();
		// Calculate source hours parked by age hour
		sql = "DROP TABLE IF EXISTS shpbyagehour";
		SQLRunner.executeSQL(db,sql);

		/**
		 * @step 180
		 * @algorithm SHP = (population*noOfRealDays) - SUM(sho).
		 * @output SHPByAgeHour
		 * @input ShoByAgeRoadwayHour
		 * @input SourceTypeAgePopulation
		 * @input DayOfAnyWeek
		**/
		sql = "CREATE TABLE IF NOT EXISTS shpbyagehour " +
				"select sarh.yearid, sarh.sourcetypeid, sarh.ageid, monthid, " +
				"sarh.dayid, hourid, (population*noofrealdays) - sum(sho) as shp " +
				"from shobyageroadwayhour sarh " +
				"inner join sourcetypeagepopulation stap using (yearid, sourcetypeid, " +
				"ageid) " +
				"inner join dayofanyweek d on (d.dayid=sarh.dayid) " +
				"where vmt > 0 " +
				"group by sarh.yearid, sarh.sourcetypeid, sarh.ageid, monthid, " +
				"sarh.dayid, hourid " +
				"order by null";
		SQLRunner.executeSQL(db,sql);

		Logger.log(LogMessageCategory.INFO,"TAG SHPByAgeHour ms=" + (System.currentTimeMillis()-start));
	}

	/**
	 * Remove records from SHO, ExtendedIdleHours, hotellingHours, and Starts based upon the
	 * currentZoneID, and currentLinkID member variables.  This is done anytime new data is
	 * generated for these activity output tables (which is currently whenever a new year or
	 * new zone or new process is requested).
	 * @throws SQLException If failed to allocate Total Activity Basis.
	**/
	void clearActivityTables() throws SQLException {
		if(currentZoneID == 0 || currentYearForZone == 0) {
			return;
		}
		if(ExecutionRunSpec.shouldSaveData(this)) {
			return;
		}
		String sql = "";
		try {
			/*
			sql = "DELETE FROM SHO WHERE isUserInput='N' AND linkID IN ("
					+ linksInZone.substring(1) + ")";
			SQLRunner.executeSQL(db, sql);
			clearFlags("SHO");

			sql = "DELETE FROM SourceHours WHERE isUserInput='N' "
					+ "AND linkID IN (" + linksInZone.substring(1) + ")";
			SQLRunner.executeSQL(db, sql);
			clearFlags("SourceHours");
			*/

			sql = "DELETE FROM extendedidlehours WHERE isuserinput='N' "
					+ "AND zoneid = " + currentZoneID;
			SQLRunner.executeSQL(db, sql);
			clearFlags("extendedidlehours");

			if(CompilationFlags.ENABLE_AUXILIARY_POWER_EXHAUST) {
				sql = "DELETE FROM hotellinghours WHERE isuserinput='N' "
						+ "AND zoneid = " + currentZoneID;
				SQLRunner.executeSQL(db, sql);
				clearFlags("hotellinghours");
			}

			sql = "DELETE FROM starts WHERE isuserinput='N' "
					+ "AND zoneid = " + currentZoneID;
			SQLRunner.executeSQL(db, sql);
			clearFlags("starts");
		} catch(Exception e) {
			Logger.logSqlError(e,"Could not delete Total Activity data from previous run.",sql);
		}
	}

	/**
	 * Tag-8: Allocate Total Activity Basis, Starts, SHP and Source Hours.
	 * @param inContext Current loop context being run.
	 * @throws SQLException If failed to allocate Total Activity Basis.
	**/
	void allocateTotalActivityBasis(MasterLoopContext inContext) throws SQLException {
		String sql = "";

		int analysisYear = inContext.year;
		int zoneID = inContext.iterLocation.zoneRecordID;
		int stateID = inContext.iterLocation.stateRecordID;

		// See if this is a new year for the current zone.
		boolean newYearForZone = false;
		if(inContext.iterProcess.compareTo(currentProcess)!=0) {
			clearActivityTables(); // do this before changing last known IDs
			currentProcess = inContext.iterProcess;
			newYearForZone = true;
			linksInZone = "";
			currentZoneID = zoneID;
			currentYearForZone = analysisYear;
		} else if(zoneID==currentZoneID) {
			if(currentYearForZone<analysisYear) {
				currentYearForZone = analysisYear;
				newYearForZone = true;
				linksInZone = "";
			}
		} else {
			clearActivityTables(); // do this before changing last known IDs
			currentZoneID = zoneID;
			currentYearForZone = analysisYear;
			newYearForZone = true;
			linksInZone = "";
		}
		currentLinkID = inContext.iterLocation.linkRecordID;
		linksInZone += "," + currentLinkID;

		boolean needSHO = false;
		boolean makeSH = false;

		if((evapPermeationProcess!=null &&
				inContext.iterProcess.compareTo(evapPermeationProcess)==0) ||
				(evapFuelVaporVentingProcess!=null &&
				inContext.iterProcess.compareTo(evapFuelVaporVentingProcess)==0) ||
		   		(evapFuelLeaksProcess!=null &&
	   			inContext.iterProcess.compareTo(evapFuelLeaksProcess)==0) ||
		   		(evapNonFuelVaporsProcess!=null &&
		   		inContext.iterProcess.compareTo(evapNonFuelVaporsProcess)==0)) {
			makeSH = true;
			if(inContext.iterLocation.roadTypeRecordID!=1 || newYearForZone) {
				needSHO = true;
			}
		}
		if((runningExhaustProcess!=null &&
				inContext.iterProcess.compareTo(runningExhaustProcess)==0) ||
				(brakeWearProcess!=null &&
				inContext.iterProcess.compareTo(brakeWearProcess)==0) ||
				(tireWearProcess!=null &&
				inContext.iterProcess.compareTo(tireWearProcess)==0)) {
			needSHO = true;
		}

		// Don't update the activity tables unless the zone changes
		if(newYearForZone) {
			if(needSHO && !checkAndMark("sho",zoneID,analysisYear)) {
				if(!checkAndMark("zoneroadtypelinktemp",zoneID,0)) {
					sql = "drop table if exists zoneroadtypelinktemp";
					SQLRunner.executeSQL(db,sql);
	
					sql = "create table if not exists zoneroadtypelinktemp ("
							+ " roadtypeid smallint not null,"
							+ " linkid int(11) not null,"
							+ " shoallocfactor double,"
							+ " unique index xpkzoneroadtypelinktemp ("
							+ "     roadtypeid, linkid))";
					SQLRunner.executeSQL(db,sql);
	
					/**
					 * @step 190
					 * @algorithm Append SHOAllocFactor to link information.
					 * @output ZoneRoadTypeLinkTemp
					 * @input ZoneRoadType
					 * @input Link
					**/
					DatabaseUtilities.insertSelect(false,db,"zoneroadtypelinktemp",
							"roadtypeid, linkid, shoallocfactor",
							"select zrt.roadtypeid, linkid, shoallocfactor"
								+ " from zoneroadtype zrt"
								+ " inner join link l on l.roadtypeid=zrt.roadtypeid"
								+ " where zrt.zoneid=" + zoneID
								+ " and l.zoneid=" + zoneID);
				}
				if(ExecutionRunSpec.getRunSpec().scale == ModelScale.MESOSCALE_LOOKUP) {
					/**
					 * @step 190
					 * @algorithm SHO = SHO * averageSpeed * SHOAllocFactor.
					 * @output SHO
					 * @input SHOByAgeRoadwayHour
					 * @input RunSpecHourDay
					 * @input ZoneRoadTypeLinkTemp
					 * @input LinkAverageSpeed
					 * @input AverageSpeed
					 * @condition Rates
					**/
					DatabaseUtilities.insertSelect(false,db,"sho",
								"hourdayid,"+
								"monthid,"+
								"yearid,"+
								"ageid,"+
								"linkid,"+
								"sourcetypeid,"+
								"sho",
							"select "+
								"sarh.hourdayid,"+
								"sarh.monthid,"+
								"sarh.yearid,"+
								"sarh.ageid,"+
								"zrt.linkid,"+
								"sarh.sourcetypeid,"+
								"sarh.sho*zrt.shoallocfactor "+ //em - there used to be a coalesce on averagespeed, but that writes vmt not sho
							"from "+
								"shobyageroadwayhour sarh "+
								"inner join runspechourday rshd on (rshd.hourdayid=sarh.hourdayid) "+
								"inner join zoneroadtypelinktemp zrt "+
									"on zrt.roadtypeid=sarh.roadtypeid "+
								"inner join linkaveragespeed las "+
									"on las.linkid=zrt.linkid "+
								"left join averagespeed asp on ("+
									"asp.roadtypeid = zrt.roadtypeid and "+
									"asp.sourcetypeid = sarh.sourcetypeid and "+
									"asp.dayid = sarh.dayid and "+
									"asp.hourid = sarh.hourid) "+
							"where "+
								"sarh.yearid = " + analysisYear
							);
				} else {
					/**
					 * @step 190
					 * @algorithm SHO = SHO * SHOAllocFactor.
					 * @output SHO
					 * @input SHOByAgeRoadwayHour
					 * @input RunSpecHourDay
					 * @input ZoneRoadTypeLinkTemp
					 * @condition Inventory
					**/
					DatabaseUtilities.insertSelect(false,db,"sho",
								"hourdayid,"+
								"monthid,"+
								"yearid,"+
								"ageid,"+
								"linkid,"+
								"sourcetypeid,"+
								"sho",
							"select "+
								"sarh.hourdayid,"+
								"sarh.monthid,"+
								"sarh.yearid,"+
								"sarh.ageid,"+
								"zrt.linkid,"+
								"sarh.sourcetypeid,"+
								"sarh.sho*zrt.shoallocfactor "+
							"from "+
								"shobyageroadwayhour sarh "+
								"inner join runspechourday rshd on (rshd.hourdayid=sarh.hourdayid) "+
								"inner join zoneroadtypelinktemp zrt " +
								"on zrt.roadtypeid=sarh.roadtypeid "+
							"where "+
								"sarh.yearid = " + analysisYear
							);
				}

				// Adjust, or create, TotalIdleFraction one time only.
				if(!checkAndMark("totalidlefraction",0,0)) {
					adjustTotalIdleFraction();
				}

				/**
				 * @step 190
				 * @algorithm ONI = SHO on roadtype 1=Sum(SHO[not road type 1] * 
				 *                  (totalIdlingFraction-(sum(sho*drivingIdleFraction)/sum(sho)))/
				 *                  (1-totalIdlingFraction)) 
				 * @output SHO
				 * @input SHO
				 * @input Link
				 * @input County
				 * @input State
				 * @input HourDay
				 * @input TotalIdleFraction
				 * @input DrivingIdleFraction
				**/
				
				//EM - we want different behaviour for ONI in Rates vs Inventory
				// 		In rates, we need to divide SHO by 16 becuase each road type has 16 rows for each avgSpeedBin
				//		Inventory is left how it was originally written
				if(ExecutionRunSpec.getRunSpec().scale == ModelScale.MESOSCALE_LOOKUP) {
					DatabaseUtilities.insertSelect(false,db,"sho", // ONI=Off network idle=SHO on road type 1
								"hourdayid,"+
								"monthid,"+
								"yearid,"+
								"ageid,"+
								"linkid,"+
								"sourcetypeid,"+
								"sho",
							"select s.hourdayid,s.monthid,s.yearid,s.ageid, lo.linkid,s.sourcetypeid, "+
							"(" + 
							"		case when totalidlefraction <> 1 then "+
							"			greatest(sum((s.sho/16))*(totalidlefraction-sum((s.sho/16)*drivingidlefraction) /sum((s.sho/16)))/(1-totalidlefraction),0) "+
							"		else 0 "+
							"		end "+
							"	) as sho "+
							"from sho s "+
							"inner join link l on ( "+
							"	l.linkid = s.linkid "+
							"	and l.roadtypeid <> 1) "+
							"inner join link lo on ( "+
							"	l.zoneid = lo.zoneid "+
							"	and lo.roadtypeid = 1) "+
							"inner join county c on (lo.countyid = c.countyid) "+
							"inner join state st using (stateid) "+
							"inner join hourday hd on (s.hourdayid = hd.hourdayid) "+
							"inner join totalidlefraction tif on ( "+
							"	tif.idleregionid = st.idleregionid "+
							"	and tif.countytypeid = c.countytypeid "+
							"	and tif.sourcetypeid = s.sourcetypeid "+
							"	and tif.monthid = s.monthid "+
							"	and tif.dayid = hd.dayid "+
							"	and tif.minmodelyearid <= s.yearid - s.ageid "+
							"	and tif.maxmodelyearid >= s.yearid - s.ageid) "+
							"inner join drivingidlefraction dif on ( "+
							"	dif.hourdayid = s.hourdayid "+
							"	and dif.yearid = s.yearid "+
							"	and dif.roadtypeid = l.roadtypeid "+
							"	and dif.sourcetypeid = s.sourcetypeid) "+
							"where s.yearid = " + analysisYear + " "+
							"and lo.zoneid = " + zoneID + " "+
							"group by s.hourdayid,s.monthid,s.yearid,s.ageid, lo.linkid,s.sourcetypeid"
							);
				} else {
					DatabaseUtilities.insertSelect(false,db,"sho", // ONI=Off network idle=SHO on road type 1
								"hourdayid,"+
								"monthid,"+
								"yearid,"+
								"ageid,"+
								"linkid,"+
								"sourcetypeid,"+
								"sho",
							"select s.hourdayid,s.monthid,s.yearid,s.ageid, lo.linkid,s.sourcetypeid, "+
							"(" + 
							"		case when totalidlefraction <> 1 then "+
							"			greatest(sum((s.sho))*(totalidlefraction-sum((s.sho)*drivingidlefraction) /sum((s.sho)))/(1-totalidlefraction),0) "+
							"		else 0 "+
							"		end "+
							"	) as sho "+
							"from sho s "+
							"inner join link l on ( "+
							"	l.linkid = s.linkid "+
							"	and l.roadtypeid <> 1) "+
							"inner join link lo on ( "+
							"	l.zoneid = lo.zoneid "+
							"	and lo.roadtypeid = 1) "+
							"inner join county c on (lo.countyid = c.countyid) "+
							"inner join state st using (stateid) "+
							"inner join hourday hd on (s.hourdayid = hd.hourdayid) "+
							"inner join totalidlefraction tif on ( "+
							"	tif.idleregionid = st.idleregionid "+
							"	and tif.countytypeid = c.countytypeid "+
							"	and tif.sourcetypeid = s.sourcetypeid "+
							"	and tif.monthid = s.monthid "+
							"	and tif.dayid = hd.dayid "+
							"	and tif.minmodelyearid <= s.yearid - s.ageid "+
							"	and tif.maxmodelyearid >= s.yearid - s.ageid) "+
							"inner join drivingidlefraction dif on ( "+
							"	dif.hourdayid = s.hourdayid "+
							"	and dif.yearid = s.yearid "+
							"	and dif.roadtypeid = l.roadtypeid "+
							"	and dif.sourcetypeid = s.sourcetypeid) "+
							"where s.yearid = " + analysisYear + " "+
							"and lo.zoneid = " + zoneID + " "+
							"group by s.hourdayid,s.monthid,s.yearid,s.ageid, lo.linkid,s.sourcetypeid"
							);
				}
				
				
			}
			if(startExhaustProcess!=null
					&& inContext.iterProcess.compareTo(startExhaustProcess)==0
					&& !checkAndMark("starts",zoneID,analysisYear)) {
				/**
				 * @step 190
				 * @algorithm starts = starts * startAllocFactor.
				 * @output Starts
				 * @input StartsByAgeHour
				 * @input RunSpecMonth
				 * @condition Inventory
				**/
				// Starts = StartsPerDay * monthAdjustment * ageFraction * allocationFraction						
//				DatabaseUtilities.insertSelect(false,db,"Starts",
//						"hourDayID, monthID, yearID, ageID, "
//						+ "zoneID, sourceTypeID, starts, startsCV, isUserInput",
//						"SELECT sah.hourDayID, rsm.monthID, sah.yearID, sah.ageID, "
//						+ "z.zoneID, sah.sourceTypeID, sah.starts * z.startAllocFactor, "
//						+ "z.zoneID, sah.sourceTypeID, sah.starts * 1, "
//						+ " 0, 'N' "
//						+ "FROM StartsByAgeHour sah INNER JOIN Zone z "
//						+ "CROSS JOIN RunSpecMonth rsm "
//						+ "WHERE sah.yearID = " + analysisYear + " AND z.zoneID = " + zoneID
//						);
				adjustStarts(zoneID,analysisYear);
				// Remove Start entries that are for hours outside of the user's selections.
				// Filter to analysisYear and zoneID.
				// Don't do this before adjusting the starts though as StartsPerDay requires
				// a full 24-hour distribution.
				sql = "delete from starts"
						+ " where hourdayid not in (select hourdayid from runspechourday)"
						+ " and zoneid=" + zoneID
						+ " and yearid=" + analysisYear;
//				SQLRunner.executeSQL(db,sql);
			} else if((extendedIdleProcess!=null && inContext.iterProcess.compareTo(extendedIdleProcess)==0)
					|| (CompilationFlags.ENABLE_AUXILIARY_POWER_EXHAUST && auxiliaryPowerProcess!=null && inContext.iterProcess.compareTo(auxiliaryPowerProcess)==0)) {
				if(extendedIdleProcess!=null && inContext.iterProcess.compareTo(extendedIdleProcess)==0
						&& !checkAndMark("extendedidlehours",zoneID,analysisYear)) {
					if(CompilationFlags.ENABLE_AUXILIARY_POWER_EXHAUST) {
						int hotellingActivityZoneID = findHotellingActivityDistributionZoneIDToUse(db,stateID,zoneID);
						/*
						 *The TotalActivityGenerator class performs inserts into the ExtendedIdleHours 
						 *and hotellingHours tables within its allocateTotalActivityBasis() function. 
						 *These inserts should be filtered to just sourceTypeID 62 as only that type of 
						 *vehicle idles overnight.
						 */

						// Apply user-supplied hotelling hours to extendedIdleHours.
						
						/**
						 * @step 190
						 * @algorithm extendedIdleHours = hotellingHours * opModeFraction[opModeID=200 extended idling].
						 * @output extendedIdleHours
						 * @input hotellingHours
						 * @input hotellingActivityDistribution
						 * @condition HotellingHours contains user-supplied hotelling information
						**/
						DatabaseUtilities.insertSelect(false,db,"extendedidlehours",
								"hourdayid,"+
								"monthid,"+
								"yearid,"+
								"ageid,"+
								"zoneid,"+
								"sourcetypeid,"+
								"extendedidlehours",
							"select"
								+ " 	hourdayid,"
								+ " 	monthid,"
								+ " 	yearid,"
								+ " 	ageid,"
								+ " 	h.zoneid,"
								+ " 	sourcetypeid,"
								+ " 	hotellinghours*opmodefraction as extendedidlehours"
								+ " from hotellinghours h"
								+ " inner join hotellingactivitydistribution a"
								+ " where a.zoneid=" + hotellingActivityZoneID + " and h.zoneid=" + zoneID
								+ " and h.yearid=" + analysisYear
								+ " and a.beginmodelyearid <= h.yearid - h.ageid"
								+ " and a.endmodelyearid >= h.yearid - h.ageid"
								+ " and opmodeid=200");

						/**
						 * @step 190
						 * @algorithm extendedIdleHours = idleHours * SHOAllocFactor * opModeFraction[opModeID=200 extended idling].
						 * @output extendedIdleHours
						 * @input IdleHoursByAgeHour
						 * @input runSpecHourDay
						 * @input ZoneRoadType
						 * @input HourDay
						 * @input hotellingActivityDistribution
						 * take out the shoallocfactor
						**/
						DatabaseUtilities.insertSelect(false,db,"extendedidlehours",
							"hourdayid,"+
							"monthid,"+
							"yearid,"+
							"ageid,"+
							"zoneid,"+
							"sourcetypeid,"+
							"extendedidlehours",
						"select "+
							"hd.hourdayid,"+
							"ihah.monthid,"+
							"ihah.yearid,"+
							"ihah.ageid,"+
							"z.zoneid,"+
							"ihah.sourcetypeid,"+
							"sum(ihah.idlehours*hac.opmodefraction) "+
						"from "+
							"idlehoursbyagehour ihah,"+
							"zone z,"+
							"hourday hd, "+
							"hotellingactivitydistribution hac "+
						"where "+
							"hd.hourid = ihah.hourid and "+
							"hd.dayid = ihah.dayid and "+
							"hac.opmodeid = 200 and "+
							"hac.beginmodelyearid <= ihah.yearid - ihah.ageid and "+
							"hac.endmodelyearid >= ihah.yearid - ihah.ageid and "+
							"ihah.yearid = " + analysisYear + " AND "+
							"hac.zoneid = " + hotellingActivityZoneID + " AND z.zoneid = " + zoneID  + " AND "+
							"ihah.sourcetypeid = 62 "+
						"group by "+
							"hd.hourdayid,"+
							"ihah.monthid,"+
							"ihah.yearid,"+
							"ihah.ageid,"+
							"z.zoneid,"+
							"ihah.sourcetypeid"
						);
						adjustExtendedIdle(zoneID,analysisYear,hotellingActivityZoneID);
						// Remove ExtendedIdleHours entries that are for hours outside of the user's selections.
						// Filter to analysisYear and zoneID.
						// Don't do this before adjusting the hours though as HotellingHoursPerDay requires
						// a full 24-hour distribution.
						sql = "delete from extendedidlehours"
								+ " where hourdayid not in (select hourdayid from runspechourday)"
								+ " and zoneid=" + zoneID
								+ " and yearid=" + analysisYear;
						SQLRunner.executeSQL(db,sql);
					} else {
						DatabaseUtilities.insertSelect(false,db,"extendedidlehours",
							"hourdayid,"+
							"monthid,"+
							"yearid,"+
							"ageid,"+
							"zoneid,"+
							"sourcetypeid,"+
							"extendedidlehours",
						"select "+
							"hd.hourdayid,"+
							"ihah.monthid,"+
							"ihah.yearid,"+
							"ihah.ageid,"+
							"z.zoneid,"+
							"ihah.sourcetypeid,"+
							"sum(ihah.idlehours) "+
						"from "+
							"idlehoursbyagehour ihah,"+
							"runspechourday rshd,"+
							"zone z,"+
							"hourday hd "+
						"where "+
							"hd.hourdayid = rshd.hourdayid and "+
							"hd.hourid = ihah.hourid and "+
							"hd.dayid = ihah.dayid and "+
							"ihah.yearid = " + analysisYear + " AND "+
							"z.zoneid = " + zoneID  + " AND "+
							"ihah.sourcetypeid = 62 "+
						"group by "+
							"hd.hourdayid,"+
							"ihah.monthid,"+
							"ihah.yearid,"+
							"ihah.ageid,"+
							"z.zoneid,"+
							"ihah.sourcetypeid"
						);
					}
				} else if(CompilationFlags.ENABLE_AUXILIARY_POWER_EXHAUST
						&& auxiliaryPowerProcess!=null && inContext.iterProcess.compareTo(auxiliaryPowerProcess)==0) {
					if(!checkAndMark("hotellinghours",zoneID,analysisYear)) {
						DatabaseUtilities.insertSelect(false,db,"hotellinghours",
									"hourdayid,"+
									"monthid,"+
									"yearid,"+
									"ageid,"+
									"zoneid,"+
									"sourcetypeid,"+
									"hotellinghours",
								"select "+
									"hd.hourdayid,"+
									"ihah.monthid,"+
									"ihah.yearid,"+
									"ihah.ageid,"+
									"z.zoneid,"+
									"ihah.sourcetypeid,"+
									"sum(ihah.idlehours) "+ // this must be total hotelling hours, including extended idle
								"from "+
									"idlehoursbyagehour ihah,"+
									"zone z,"+
									"hourday hd "+
								"where "+
									"hd.hourid = ihah.hourid and "+
									"hd.dayid = ihah.dayid and "+
									"ihah.yearid = " + analysisYear + " AND "+
									"z.zoneid = " + zoneID  + " AND "+
									"ihah.sourcetypeid = 62 "+
								"GROUP BY "+
									"hd.hourdayid,"+
									"ihah.monthid,"+
									"ihah.yearid,"+
									"ihah.ageid,"+
									"z.zoneid,"+
									"ihah.sourcetypeid"
								);
						int hotellingActivityZoneID = findHotellingActivityDistributionZoneIDToUse(db,stateID,zoneID);
						adjustHotelling(zoneID,analysisYear,hotellingActivityZoneID);
						// Remove HotellingHours entries that are for hours outside of the user's selections.
						// Filter to analysisYear and zoneID.
						// Don't do this before adjusting the hours though as HotellingHoursPerDay requires
						// a full 24-hour distribution.
						sql = "delete from hotellinghours"
								+ " where hourdayid not in (select hourdayid from runspechourday)"
								+ " and zoneid=" + zoneID
								+ " and yearid=" + analysisYear;
						SQLRunner.executeSQL(db,sql);
					}
				}
			}
		}

		// Allocate SHP and SHO to SourceHours
		if(makeSH && !checkAndMark("sourcehours",currentLinkID,analysisYear)) {
			if(inContext.iterLocation.roadTypeRecordID==1) {
				if(newYearForZone) {
					/**
					 * @step 190
					 * @algorithm SHP = SHP * SHPAllocFactor.
					 * @output SHP
					 * @input SHPByAgeHour
					 * @input RunSpecHourDay
					 * @input Zone
					**/
					DatabaseUtilities.insertSelect(false,db,"shp",
							"hourdayid, monthid, yearid, ageid, zoneid, " +
							"sourcetypeid, shp ",
							"select hd.hourdayid, monthid, yearid, ageid, zoneid, sourcetypeid, " +
							"shp * shpallocfactor " +
							"from shpbyagehour sah inner join hourday hd using (hourid, dayid) " +
							"inner join runspechourday rshd on (rshd.hourdayid=hd.hourdayid) "+
							"inner join zone z where sah.yearid = " + analysisYear +
							" AND z.zoneid = " + zoneID
							);
				}

				/**
				 * @step 190
				 * @algorithm sourceHours = SHP.
				 * @output SourceHours
				 * @input SHP
				 * @input Link
				**/
				DatabaseUtilities.insertSelect(false,db,"sourcehours",
						"hourdayid, monthid, yearid, ageid, linkid, " +
						"sourcetypeid, sourcehours, sourcehourscv, isuserinput ",
						"select hourdayid, " +
						"monthid, yearid, ageid, linkid, sourcetypeid, shp, 0, 'N' " +
						"from shp inner join link on (link.zoneid = shp.zoneid) " +
						"where roadtypeid = 1 and yearid = " + analysisYear +
						" AND linkid = " + currentLinkID
						);
			} else {
				/**
				 * @step 190
				 * @algorithm sourceHours = SHO.
				 * @output SourceHours
				 * @input SHO
				**/
				DatabaseUtilities.insertSelect(false,db,"sourcehours",
						"hourdayid, monthid, yearid, ageid, linkid, " +
						"sourcetypeid, sourcehours, sourcehourscv, isuserinput",
						"select hourdayid, " +
						"monthid, yearid, ageid, linkid, sourcetypeid, sho, shocv, 'N' " +
						"from sho sho where sho.yearid = " + analysisYear +
						" AND linkid = " + currentLinkID
						);
			}
		}
	}

	/**
	 * Tag-9: Calculate distance
	 * @throws SQLException If failed to calculate distance.
	**/
	void calculateDistance(MasterLoopContext inContext) throws SQLException {
		// System.out.println("TAG Calc Distance called, process = " + inContext.iterProcess.processName);
		// If the user did NOT choose distance output, then return now.
		if(!ExecutionRunSpec.theExecutionRunSpec.getOutputVMTData()) {
			return;
		}
		// Only calculate distance if process is exhaust running
		// We could also insure roadtype not equal off network but this would
		//     be less general.
		if(inContext.iterProcess.processName.equals(runningExhaustProcess.processName)) {
			String sql = " ";

			if(ExecutionRunSpec.getRunSpec().scale == ModelScale.MESOSCALE_LOOKUP) {
				/**
				 * @step 200
				 * @algorithm distance = SHO * averageSpeed.
				 * @output SHO
				 * @input LinkAverageSpeed
				 * @condition Rates
				**/
				sql = "update sho, linkaveragespeed"
						+ " set sho.distance=sho.sho*averagespeed"
						+ " where linkaveragespeed.linkid=sho.linkid"
						+ " and sho.distance is null";
				SQLRunner.executeSQL(db,sql);
				return;
			}

			sql = "DROP TABLE IF EXISTS shotemp ";
			SQLRunner.executeSQL(db,sql);

			/**
			 * @step 200
			 * @algorithm Append link and SHO information.
			 * @output SHOTemp
			 * @input SHO
			 * @input Link
			 * @condition Inventory
			**/
			sql = "CREATE TABLE shotemp " +
					"select hourdayid, monthid, yearid, ageid, sho.linkid, " +
					"sourcetypeid, roadtypeid, sho.shocv, sho.sho " +
					"from sho as sho inner join link using (linkid) ";
			SQLRunner.executeSQL(db,sql);

			sql = "CREATE INDEX index1 ON shotemp (hourdayid, sourcetypeid, roadtypeid) ";
			SQLRunner.executeSQL(db,sql);

			sql = "DROP TABLE IF EXISTS averagespeedtemp ";
			SQLRunner.executeSQL(db,sql);

			/**
			 * @step 200
			 * @algorithm Append AverageSpeed and HourDay information.
			 * @output AverageSpeedTemp
			 * @input AverageSpeed
			 * @input HourDay
			 * @condition Inventory
			**/
			sql = "CREATE TABLE averagespeedtemp " +
					"select hourdayid, sourcetypeid, roadtypeid, averagespeed " +
					"from averagespeed inner join hourday  using(dayid, hourid) ";
			SQLRunner.executeSQL(db,sql);

			sql = "CREATE INDEX index1 " +
					"ON averagespeedtemp(hourdayid, sourcetypeid, roadtypeid) ";
			SQLRunner.executeSQL(db,sql);

			sql = "CREATE TABLE IF NOT EXISTS sho2 " +
					"(hourdayid smallint, monthid smallint, yearid smallint, " +
					"ageid smallint,linkid integer, sourcetypeid smallint, " +
					"sho float, shocv float, distance float) ";
			SQLRunner.executeSQL(db,sql);

			/**
			 * @step 200
			 * @algorithm distance = SHO * averageSpeed.
			 * @output SHO2
			 * @input SHOTemp
			 * @input AverageSpeedTemp
			 * @condition Inventory
			**/
			sql = "INSERT INTO sho2 " +
					"select shot.hourdayid, shot.monthid, shot.yearid, shot.ageid, " +
					"shot.linkid, shot.sourcetypeid, shot.sho, shot.shocv, " +
					"(shot.sho * avsp.averagespeed) " +
					"from shotemp as shot inner join averagespeedtemp as avsp " +
					"using(hourdayid, sourcetypeid, roadtypeid) ";
			SQLRunner.executeSQL(db,sql);

			/**
			 * @step 200
			 * @algorithm Remove all items from the SHO table.
			 * @output SHO
			 * @condition Inventory
			**/
			sql = "TRUNCATE sho";
			SQLRunner.executeSQL(db,sql);

			/**
			 * @step 200
			 * @algorithm Copy the SHO2 information, which now includes distance, to the SHO table.
			 * @output SHO
			 * @input SHO2
			 * @condition Inventory
			**/
			sql = "INSERT INTO sho (hourdayid, monthid, yearid, ageid,"+
					"linkid, sourcetypeid, sho, shocv, distance) "+
					"select hourdayid, monthid,yearid, ageid, linkid,"+
					"sourcetypeid, sho, shocv, distance from sho2 ";
			SQLRunner.executeSQL(db,sql);

			/**
			 * @step 200
			 * @algorithm Copy back to SHO any SHOTemp information that was ignored because it
			 * was on off-network roads without an average speed. This is ONI data.
			 * @output SHO
			 * @input SHOTemp
			 * @condition Inventory
			**/
			sql = "INSERT IGNORE INTO sho (hourdayid, monthid, yearid, ageid,"+
					"linkid, sourcetypeid, sho, shocv, distance) "+
					"select hourdayid, monthid,yearid, ageid, linkid,"+
					"sourcetypeid, sho, shocv, 0 as distance from shotemp ";
			SQLRunner.executeSQL(db,sql);

			sql = "DROP TABLE IF EXISTS sho2 ";
			SQLRunner.executeSQL(db,sql);
			sql = "DROP TABLE IF EXISTS shotemp ";
			SQLRunner.executeSQL(db,sql);
			sql = "DROP TABLE IF EXISTS averagespeedtemp ";
			SQLRunner.executeSQL(db,sql);
		}
	}

	/**
	 * Check the markers for table, region, and year combinations that have already
	 * been calculated.  If not already calculated, mark the passed combination as
	 * calculated.
	 * @param tableName table to be checked
	 * @param regionID zone or link to be checked
	 * @param year year to be checked
	 * @return true if the passed combination has already been calculated
	**/
	boolean checkAndMark(String tableName, int regionID, int year) {
		String key = tableName + "|" + regionID + "|" + year;
		if(calculationFlags.contains(key)) {
			return true;
		}
		calculationFlags.add(key);
		return false;
	}

	/**
	 * Clear all calculation markers for a table.
	 * @param tableName table to be cleared
	**/
	void clearFlags(String tableName) {
		String prefix = tableName + "|";
		prefix = prefix.toLowerCase();
		String key;
		TreeSet<String> keysToRemove = new TreeSet<String>();
		for(Iterator i=calculationFlags.iterator();i.hasNext();) {
			key = (String)i.next();
			key = key.toLowerCase();
			if(key.startsWith(prefix)) {
				keysToRemove.add(key);
			}
		}
		for(Iterator<String> i=keysToRemove.iterator();i.hasNext();) {
			calculationFlags.remove(i.next());
		}
	}

	/**
	 * Apply user-supplied adjustments to Starts.
	 * @param zoneID affected zone
	 * @param yearID affected calendar year
	**/	
	void adjustStarts(int zoneID, int yearID) {
		String mainDatabaseName = SystemConfiguration.getTheSystemConfiguration().databaseSelections[MOVESDatabaseType.EXECUTION.getIndex()].databaseName;

		TreeMapIgnoreCase replacements = new TreeMapIgnoreCase();
		replacements.put("##zoneid##","" + zoneID);
		replacements.put("##yearid##","" + yearID);
		
		String sql = "";
		SQLRunner.Query query = new SQLRunner.Query();
		try {
			sql = "create table if not exists tempmessages "
					+ "( message varchar(1000) not null )";
			SQLRunner.executeSQL(db,sql);
			sql = "truncate table tempmessages";
			SQLRunner.executeSQL(db,sql);

			try {
				DatabaseUtilities.executeScript(db,new File("database/AdjustStarts.sql"),replacements,false);
			} catch(Exception e) {
				Logger.logError(e,"Unable to adjust starts" + e);
			}

			// Retrieve the results from tempMessages
			sql = "select message from tempmessages";
			query.open(db,sql);
			String m = "";
			while(query.rs.next()) {
				m = query.rs.getString(1);
			}
			query.close();
			if (m != null && m.length() > 0) {
				Logger.log(LogMessageCategory.WARNING,m);
			}
			sql = "drop table if exists tempmessages";
			SQLRunner.executeSQL(db,sql);
		} catch(Exception e) {
			// Nothing to do here
		} finally {
			query.onFinally();
		}
	}

	/**
	 * Apply user-supplied adjustments to HotellingHours.
	 * @param zoneID affected zone
	 * @param yearID affected calendar year
	 * @param hotellingActivityZoneID zoneID to be used for hotellingActivityDistribution.zoneID
	**/	
	void adjustHotelling(int zoneID, int yearID, int hotellingActivityZoneID) {
		TreeMapIgnoreCase replacements = new TreeMapIgnoreCase();
		replacements.put("##zoneid##","" + zoneID);
		replacements.put("##yearid##","" + yearID);
		replacements.put("##activityzoneid##","" + hotellingActivityZoneID);
		try {
			DatabaseUtilities.executeScript(db,new File("database/AdjustHotelling.sql"),replacements,false);
		} catch(Exception e) {
			Logger.logError(e,"Unable to adjust HotellingHours");
		}
	}

	/**
	 * Apply user-supplied adjustments to ExtendedIdleHours.
	 * @param zoneID affected zone
	 * @param yearID affected calendar year
	 * @param hotellingActivityZoneID zoneID to be used for hotellingActivityDistribution.zoneID
	**/	
	void adjustExtendedIdle(int zoneID, int yearID, int hotellingActivityZoneID) {
		TreeMapIgnoreCase replacements = new TreeMapIgnoreCase();
		replacements.put("##zoneid##","" + zoneID);
		replacements.put("##yearid##","" + yearID);
		replacements.put("##activityzoneid##","" + hotellingActivityZoneID);
		try {
			DatabaseUtilities.executeScript(db,new File("database/AdjustExtendedIdle.sql"),replacements,false);
		} catch(Exception e) {
			Logger.logError(e,"Unable to adjust ExtendedIdleHours");
		}
	}

	/**
	 * Using the wildcard system for zoneIDs within the hotellingActivityDistribution table,
	 * select the value for hotellingActivityDistribution.zoneID that should be used for a
	 * given real state and zone.
	 * @param db database connection to use
	 * @param stateID affected state
	 * @param zoneID affected zone
	 * @return zoneID to be used for hotellingActivityDistribution.zoneID
	 * @throws SQLException if something goes wrong finding the zone
	**/	
	public static int findHotellingActivityDistributionZoneIDToUse(Connection db, int stateID, int zoneID) throws SQLException {
		String sql = "select zoneid"
				+ " from ("
				+ " select distinct zoneid,"
				+ " 	case when zoneid=" + zoneID + " then 1" // The best match is the actual zone
				+ " 	when zoneid=" + (stateID*10000) + " then 2" // The second best match is the zone's state
				+ " 	when zoneid=990000 then 3" // The third best match is the national default
				+ " 	else 4 end as zonemerit" // Anything else is tied for last place
				+ " from hotellingactivitydistribution"
				+ " where zoneid in ("+zoneID+","+(stateID*10000)+",990000)"
				+ " ) t"
				+ " order by zonemerit" // Order by best (1) to worst (4)
				+ " limit 1"; // Only get the best scoring
		int resultZoneID = 0;
		SQLRunner.Query query = new SQLRunner.Query();
		try {
			query.open(db,sql);
			if(query.rs.next()) {
				resultZoneID = query.rs.getInt("zoneid");
			}
			query.close();
		} catch(SQLException e) {
			query.onException(e,"Unable to find hotellingActivityDistribution wildcard zone",sql);
		} finally {
			query.onFinally();
		}
		return resultZoneID;
	}

	/**
	 * Apply user-supplied adjustments to TotalIdleFraction.
	**/	
	void adjustTotalIdleFraction() {
		TreeMapIgnoreCase replacements = new TreeMapIgnoreCase();
		try {
			DatabaseUtilities.executeScript(db,new File("database/AdjustTotalIdleFraction.sql"),replacements,false);
		} catch(Exception e) {
			Logger.logError(e,"Unable to adjust TotalIdleFraction");
		}
	}
}
