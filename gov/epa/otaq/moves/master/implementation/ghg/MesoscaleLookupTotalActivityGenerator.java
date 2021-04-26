/**************************************************************************************************
 * @(#)MesoscaleLookupTotalActivityGenerator.java
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
 * This Class builds "Total Activity" records for the Mesoscale Lookup process.
 * Refer to the TotalActivityGenerator class and Task 224 for the basis of the calculations
 * herein.
 *
 * @author		Wesley Faler
 * @version		2013-09-17
**/
public class MesoscaleLookupTotalActivityGenerator extends Generator {
	/** @notused **/

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

	/** Default constructor **/
	public MesoscaleLookupTotalActivityGenerator() {
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

			if(initialLoop) {
				setup(inContext.iterProcess);
			}

			if(inContext.year > resultsYear) {
				baseYear = determineBaseYear(inContext.year);
				if(baseYear > resultsYear) {
					calculateBaseYearPopulation();
				}

				growPopulationToAnalysisYear(inContext.year);
				calculateFractionOfTravelUsingHPMS(inContext.year);
				allocateVMTByRoadTypeSourceAge();
				calculateVMTByRoadwayHour();
				convertVMTToTotalActivityBasis();
				resultsYear = inContext.year;
			}

			allocateTotalActivityBasis(inContext);
			calculateDistance(inContext);
			initialLoop = false;
		} catch (Exception e) {
			Logger.logError(e,"Total Activity Generation failed for year "+inContext.year);
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

		//
		// Succeeding years may(if there is not an intervening base year) grow values from the
		// following tables to the current year so the data in these tables must be kept for
		// the full run.
		sql = "create table if not exists sourcetypeagepopulation ("+
					"yearid         smallint not null,"+
					"sourcetypeid   smallint not null,"+
					"ageid          smallint not null,"+
					"population     float not null,"+
					"unique index xpksourcetypeagepopulation ("+
						"yearid, sourcetypeid, ageid))";
		SQLRunner.executeSQL(db,sql);

		sql = "TRUNCATE sourcetypeagepopulation";
		SQLRunner.executeSQL(db,sql);

		sql = "CREATE TABLE IF NOT EXISTS analysisyearvmt ("+
					"yearid      smallint not null,"+
					"hpmsvtypeid smallint not null,"+
					"vmt         float not null,"+
					"unique index xpkanalysisyearvmt ("+
						"yearid, hpmsvtypeid))";
		SQLRunner.executeSQL(db,sql);

		sql = "TRUNCATE analysisyearvmt";
		SQLRunner.executeSQL(db,sql);

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
				"sho            float not null,"+
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
				"starts         float not null,"+
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
				"idlehours      float not null,"+
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
					"vmt           float not null,"+
					"unique index xpkvmtbyageroadwayhour("+
						"yearid, roadtypeid, sourcetypeid, ageid, monthid, dayid, hourid))";
		SQLRunner.executeSQL(db,sql);

		sql = "TRUNCATE vmtbyageroadwayhour";
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

		sql = "CREATE TABLE IF NOT EXISTS analysisyearvmt2 ("+
					"yearid      smallint not null,"+
					"hpmsvtypeid smallint not null,"+
					"vmt         float not null,"+
					"unique index analysisyearvmt2("+
						"yearid, hpmsvtypeid))";
		SQLRunner.executeSQL(db,sql);

		sql = "TRUNCATE analysisyearvmt2";
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
					"sho            float not null,"+
					"unique index xpkshobyageday("+
						"yearid, sourcetypeid, ageid, monthid, dayid))";
		SQLRunner.executeSQL(db,sql);

		sql = "TRUNCATE shobyageday";
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
				"shp                  float null,"+
				"unique index xpksph("+
				"hourdayid, monthid, yearid, ageid, zoneid, sourcetypeid))";
		SQLRunner.executeSQL(db,sql);
		
		sql = "TRUNCATE shp";
		SQLRunner.executeSQL(db,sql);
	}

	/**
	 * Tag-0: Find the year with base population data that is closest to the analysis year.
	 * @param analysisYear The year being analyzed.
	 * @return The base year for the year being analyzed.
	 * @throws Exception If the base year cannot be determined.
	**/
	int determineBaseYear(int analysisYear) throws Exception {
		String sql = "";
		PreparedStatement statement = null;
		ResultSet results = null;
		try {
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
				return maxYearID;
			} else {
				throw new Exception("No base year found for specified analysis year.");
			}
		} finally {
			if(results != null) {
				try {
					results.close();
				} catch(Exception e) {
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
	 * Tag-1: Calculate the base year vehicle population by age.
	 * @throws SQLException If base year population cannot be determined.
	**/
	void calculateBaseYearPopulation() throws SQLException {
		String sql = "";
		PreparedStatement statement = null;
		try {
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
						"sty.yearid = ?";
			statement = db.prepareStatement(sql);
			statement.setInt(1,baseYear);
			int rows = SQLRunner.executeSQL(statement,sql);
		} finally {
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
	 * Tag-2: Grow vehicle population from base year to analysis year.
	 * @param analysisYear the year to which the population data should be grown.
	 * @throws SQLException If population cannot be grown to the analysis year.
	**/
	void growPopulationToAnalysisYear(int analysisYear) throws SQLException {
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

			String ageXSql =
					"INSERT INTO sourcetypeagepopulation2 ("+
						"yearid,"+
						"sourcetypeid,"+
						"ageid,"+
						"population) "+
					"select "+
						"sty.yearid,"+
						"sty.sourcetypeid,"+
						"sta.ageid+1,"+
						"stap.population * sta.survivalrate * sty.migrationrate "+
					"from "+
						"sourcetypeyear sty, "+
						"sourcetypeage sta, "+
						"sourcetypeagepopulation stap "+
					"where "+
						"sty.yearid = ? and "+
						"sty.sourcetypeid = stap.sourcetypeid and "+
						"sta.ageid = ?-1 and "+
						"sta.sourcetypeid = stap.sourcetypeid and "+
						"stap.yearid = sty.yearid-1 and "+
						"stap.ageid = sta.ageid";
			ageXStatement = db.prepareStatement(ageXSql);

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
//			statement = db.prepareStatement(sql);
//			statement.setInt(1,analysisYear);
//			SQLRunner.executeSQL(statement,sql);
//			statement.close();
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
									//
									// Same as previous table except limiting table is
									// the previous table.

		sql = "TRUNCATE travelfraction";
		SQLRunner.executeSQL(db,sql);

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
									//
									// Same as previous table except limiting table is
									// the previous table.
	}

	/**
	 * Tag-5: Allocate VMT by road type, source type, and age.
	 * @throws SQLException If VMT cannot be allocated by road type, source, and age.
	**/
	void allocateVMTByRoadTypeSourceAge( ) throws SQLException {
		String sql = "";
		sql = "TRUNCATE annualvmtbyageroadway";
		SQLRunner.executeSQL(db,sql);

		sql = "INSERT INTO annualvmtbyageroadway ("
				+ " yearid,"
				+ " roadtypeid,"
				+ " sourcetypeid,"
				+ " ageid,"
				+ " vmt) "
				+ " select" 
				+ " tf.yearid,"
				+ " rt.roadtypeid,"
				+ " tf.sourcetypeid,"
				+ " tf.ageid,"
				+ " tf.fraction "
				+ " from "
				+ " roadtype rt,"
				+ " travelfraction tf";
		SQLRunner.executeSQL(db,sql);
	}

	/**
	 * Tag-6: Temporarlly Allocate VMT to Hours
	 * @throws SQLException If VMT cannot be allocated to hours.
	**/
	void calculateVMTByRoadwayHour() throws SQLException {
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

		sql = "CREATE TABLE avarmonth " +
				"select avar.*, monthid, monthvmtfraction " +
				"from annualvmtbyageroadway as avar " +
				"inner join monthvmtfraction as mvf using (sourcetypeid)";
		SQLRunner.executeSQL(db,sql);

		sql = "CREATE INDEX index1 on avarmonth (sourcetypeid, monthid, roadtypeid) ";
		SQLRunner.executeSQL(db,sql);

		sql = "CREATE TABLE avarmonthday " +
				"select avarm.*, dayid, dayvmtfraction " +
				"from avarmonth as avarm inner join dayvmtfraction as dvf " +
				"using (sourcetypeid, monthid, roadtypeid) ";
		SQLRunner.executeSQL(db,sql);

		sql = "CREATE INDEX index1 on avarmonthday(sourcetypeid, roadtypeid, dayid) ";
		SQLRunner.executeSQL(db,sql);

		sql = "INSERT INTO vmtbyageroadwayhour (yearid, roadtypeid, sourcetypeid, " +
					"ageid, monthid, dayid, hourid, vmt) " +
				"select avar.yearid, avar.roadtypeid, avar.sourcetypeid, " +
					"avar.ageid, avar.monthid, avar.dayid, hvf.hourid, " +
					"avar.vmt*avar.monthvmtfraction*avar.dayvmtfraction*hvf.hourvmtfraction " +
					" / " + weeksPerMonthClause + " "+
					"from avarmonthday as avar inner join hourvmtfraction as hvf " +
				"using(sourcetypeid, roadtypeid, dayid) ";
		SQLRunner.executeSQL(db,sql);
		
		sql = "DROP TABLE IF EXISTS avarmonth ";
		SQLRunner.executeSQL(db,sql);

		sql = "DROP TABLE IF EXISTS avarmonthday ";
		SQLRunner.executeSQL(db,sql);
		// end of rewritten statement
	}

	/**
	 * Tag-7: Convert VMT to Total Activity Basis
	 * @throws SQLException If VMT cannot be converted to Total Activity Basis.
	**/
	void convertVMTToTotalActivityBasis() throws SQLException {
		String sql = "";

		// Because Distance is calculated from SHO and is divided out in the end, 
		// the actual SHO doesn't matter. But the proportional distribution of SHO 
		// among ages, sourcetypes and times must be preserved. This step sets SHO = VMT.
		sql = "INSERT INTO shobyageroadwayhour ("+
					"yearid,"+
					"roadtypeid,"+
					"sourcetypeid,"+
					"ageid,"+
					"monthid,"+
					"dayid,"+
					"hourid,"+
					"sho) "+
				"select "+
					"varh.yearid,"+
					"varh.roadtypeid,"+
					"varh.sourcetypeid,"+
					"varh.ageid,"+
					"varh.monthid,"+
					"varh.dayid,"+
					"varh.hourid,"+
					"varh.vmt "+
				"from vmtbyageroadwayhour varh ";
		SQLRunner.executeSQL(db,sql);
		
		sql = "TRUNCATE shobyageday";
		SQLRunner.executeSQL(db,sql);

		sql = "INSERT INTO shobyageday ("+
					"yearid,"+
					"sourcetypeid,"+
					"ageid,"+
					"monthid,"+
					"dayid,"+
					"sho) "+
				"select "+
					"sho.yearid,"+
					"sho.sourcetypeid,"+
					"sho.ageid,"+
					"sho.monthid,"+
					"sho.dayid,"+
					"sum(sho.sho) "+
				"from "+
					"shobyageroadwayhour sho "+
				"group by "+
					"sho.yearid,"+
					"sho.sourcetypeid,"+
					"sho.ageid,"+
					"sho.monthid,"+
					"sho.dayid";
		SQLRunner.executeSQL(db,sql);
	}

	/**
	 * Remove records from SHO, ExtendedIdleHours, and Starts based upon the 
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
			sql = "DELETE FROM sho WHERE isuserinput='N' AND linkid IN ("
					+ linksInZone.substring(1) + ")";
			SQLRunner.executeSQL(db, sql);
		
			sql = "DELETE FROM sourcehours WHERE isuserinput='N' "
					+ "AND linkid IN (" + linksInZone.substring(1) + ")";
			SQLRunner.executeSQL(db, sql);

			sql = "DELETE FROM extendedidlehours WHERE isuserinput='N' "
					+ "AND zoneid = " + currentZoneID;
			SQLRunner.executeSQL(db, sql);

			sql = "DELETE FROM starts WHERE isuserinput='N' "
					+ "AND zoneid = " + currentZoneID;
			SQLRunner.executeSQL(db, sql);
		} catch(Exception e) {
			Logger.logSqlError(e,"Could not delete Total Activity data from previous run.",sql);
		}
	}

	/**
	 * Tag-8: Allocate Total Activity Basis and Source Hours.
	 * @param inContext Current loop context being run.
	 * @throws SQLException If failed to allocate Total Activity Basis.
	**/
	void allocateTotalActivityBasis(MasterLoopContext inContext) throws SQLException {
		String sql = "";

		int analysisYear = inContext.year;
		int zoneID = inContext.iterLocation.zoneRecordID;

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
			if(needSHO) {
				// TAG8a is modified to assign SHO to multiple links of the same zone/roadtype.
				// Because geographic aggregation is forbidden for Lookup Output, allocation to
				// Zones can be uniform. Similarly, allocation to links is not important as 
				// long as the distribution among ages, sourcetypes & times is preserved. So we
				// set SHO(link) = SHO(roadType).  SHOCV is ignored.
				sql = "INSERT IGNORE INTO sho ("+
							"hourdayid,"+
							"monthid,"+
							"yearid,"+
							"ageid,"+
							"linkid,"+
							"sourcetypeid,"+
							"sho) "+
						"select "+
							"hd.hourdayid,"+
							"sarh.monthid,"+
							"sarh.yearid,"+
							"sarh.ageid,"+
							"l.linkid,"+
							"sarh.sourcetypeid,"+
							"sarh.sho "+
						"from "+
							"shobyageroadwayhour sarh,"+
							"link l,"+
							"hourday hd "+
						"where "+
							"l.roadtypeid = sarh.roadtypeid and "+
							"hd.hourid = sarh.hourid and "+
							"hd.dayid = sarh.dayid and "+
							"sarh.yearid = " + analysisYear + " AND "+
							"l.zoneid = " + zoneID;
				SQLRunner.executeSQL(db,sql);
			}
		}

		// Allocate SHO to SourceHours
		if(makeSH) {
			if(inContext.iterLocation.roadTypeRecordID!=1) {
				sql= "INSERT IGNORE INTO sourcehours (hourdayid, monthid, yearid, ageid, linkid, " +
						"sourcetypeid, sourcehours, sourcehourscv, isuserinput) select hourdayid, " +
						"monthid, yearid, ageid, linkid, sourcetypeid, sho, shocv, 'N' " +
						"FROM sho sho where sho.yearid = " + analysisYear +
						" AND linkid = " + currentLinkID;
				SQLRunner.executeSQL(db,sql);
			}
		}
	}

	/**
	 * Tag-9: Calculate distance
	 * @throws SQLException If failed to calculate distance.
	**/
	void calculateDistance(MasterLoopContext inContext) throws SQLException {
		// Only calculate distance if process is exhaust running
		// We could also insure roadtype not equal off network but this would
		//     be less general.
		if(!inContext.iterProcess.processName.equals(runningExhaustProcess.processName)) {
			return;
		}
		String sql = "update sho, linkaveragespeed"
				+ " set sho.distance=sho.sho*averagespeed"
				+ " where linkaveragespeed.linkid=sho.linkid"
				+ " and sho.distance is null";
		SQLRunner.executeSQL(db,sql);
	}
}
