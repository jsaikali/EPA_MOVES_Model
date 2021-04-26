/**************************************************************************************************
 * @(#)StartOperatingModeDistributionGenerator.java
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
import java.util.Iterator;

/**
 * This builds "Start Operating Mode Distribution" records for ELDB data.
 * ELDB is the Execution Location Database explained in TotalActivityGenerator
 *
 * @author		Wes Faler
 * @author      EPA Mitch C. (assumed existence of all HC pollutants)
 * @author      EPA Gwo S. (Added NH3 ammonia)
 * @version		2016-08-30
**/
public class StartOperatingModeDistributionGenerator extends Generator {
	/**
	 * @algorithm
	 * @owner Start Operating Mode Distribution Generator
	 * @generator
	**/

	/** Flag for whether the data tables have been cleared/setup **/
	boolean hasBeenSetup = false;
	/** Database connection used by all functions.  Setup by executeLoop and cleanDataLoop. **/
	Connection db;
	/** milliseconds spent during one time operations **/
	long setupTime = 0;
	/** milliseconds spent during non-one-time operations **/
	long totalTime = 0;
	/** Comma delimited list of pollutant process ids in the runspec. **/
	String polProcessIDs = "";
	/** true when running for a project domain **/
	boolean isProject = false;
	/** The Total Gaseous Hydrocarbons pollutant **/
	private static Pollutant totalGaseousHydrocarbons
			= Pollutant.findByName("Total Gaseous Hydrocarbons");
	/** The Non-Methane Organic Gases pollutant **/
	private static Pollutant nonMethaneOrganicGases	= Pollutant.findByName(
			"Non-Methane Organic Gases");
	/** The Non-Methane Hydrocarbons pollutant **/
	private static Pollutant nonMethaneHydrocarbons	= Pollutant.findByName(
			"Non-Methane Hydrocarbons");
	/** The Total Organic Gases pollutant **/
	private static Pollutant totalOrganicGases = Pollutant.findByName("Total Organic Gases");
	/** The Volatile Organic Compounds pollutant **/
	private static Pollutant volatileOrganicCompounds = Pollutant.findByName(
			"Volatile Organic Compounds");
	/** The Carbon Monoxide (CO) pollutant **/
	private static Pollutant carbonMonoxideCO = Pollutant.findByName("Carbon Monoxide (CO)");
	/** The Oxides of Nitrogen pollutant **/
	private static Pollutant oxidesOfNitrogen = Pollutant.findByName("Oxides of Nitrogen (NOx)");
	/** The Nitrous Oxide (N2O) pollutant **/
	private static Pollutant nitrousOxide = Pollutant.findByName("Nitrous Oxide (N2O)");
	/** The Ammonia (NH3) pollutant **/
	private static Pollutant nH3 = Pollutant.findByName("Ammonia (NH3)");
	/** The ECarbon PM Size 2.5 pollutant **/
	private static Pollutant eCarbon25 = Pollutant.findByName("Elemental Carbon");
	/** The NonECPM Size 2.5 pollutant **/
	private static Pollutant nonECPM25 = Pollutant.findByName("Composite - NonECPM");
	/** The Total Energy Consumption pollutant **/
	private static Pollutant totalEnergyConsumption =
				Pollutant.findByName("Total Energy Consumption");
	/** The Start Exhaust Emission Process **/
	private static EmissionProcess startExhaust = EmissionProcess.findByName("Start Exhaust");

	/** Default constructor **/
	public StartOperatingModeDistributionGenerator() {

	}

	/**
	 * Requests that this object subscribe to the given loop at desired looping points.
	 * Objects can assume that all necessary MasterLoopable objects have been instantiated.
	 * @param targetLoop The loop to subscribe to.
	**/
	public void subscribeToMe(MasterLoop targetLoop) {
		if(startExhaust != null) {
			if(CompilationFlags.DO_RATES_FIRST) {
				targetLoop.subscribe(this, startExhaust, MasterLoopGranularity.PROCESS,
						MasterLoopPriority.GENERATOR);
			} else {
				targetLoop.subscribe(this, startExhaust, MasterLoopGranularity.LINK,
						MasterLoopPriority.GENERATOR);
			}
		}
	}

	/**
	 * Called each time the zone changes.
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
				isProject = ExecutionRunSpec.theExecutionRunSpec.getModelDomain() == ModelDomain.PROJECT;
				getPollutantProcessIDs();
				if(!isProject) {
					calculateSoakTime(); // steps 100-199
					calculateStartOpMode(); // steps 200-299
					calculateOpModeFraction(); // steps 300-399
				}
				setupTime = System.currentTimeMillis() - start;
				hasBeenSetup = true;
			}

			start = System.currentTimeMillis();
			populateOperatingModeDistribution(inContext); // steps 400-499
			totalTime += System.currentTimeMillis() - start;
		} catch (Exception e) {
			Logger.logError(e,"Start Operating Mode Distribution Generation failed.");
		} finally {
			if(db != null) {
				DatabaseConnectionManager.checkInConnection(MOVESDatabaseType.EXECUTION, db);
				db = null;
			}
		}

		Logger.log(LogMessageCategory.INFO,"StartOperatingModeDistributionGenerator setupTime=" + setupTime + " totalTime=" + totalTime);
	}

	/**
	 * Removes data from the execution database that was created by this object within executeLoop
	 * for the same context. This is only called after all other loopable objects that might use
	 * data created by executeLoop have had their executeLoop and cleanDataLoop functions called.
	 * @param context The MasterLoopContext that applies to this execution.
	**/
	public void cleanDataLoop(MasterLoopContext context) {
		if(CompilationFlags.DO_RATES_FIRST) {
			// Skip cleanup when doing rates first.
			return;
		}
		String sql = "";
		try {
			if(context.iterLocation.roadTypeRecordID == 1) {
				db = DatabaseConnectionManager.checkOutConnection(MOVESDatabaseType.EXECUTION);
				sql = "DELETE FROM opmodedistribution WHERE isuserinput='N' AND linkid IN ("
						+ context.iterLocation.linkRecordID + ")";
				SQLRunner.executeSQL(db, sql);
			}
		} catch(Exception e) {
			Logger.logSqlError(e,"Unable to delete Start Operating Mode Distribution data"
					+ " from a previous run", sql);
		} finally {
			if(db != null) {
				DatabaseConnectionManager.checkInConnection(MOVESDatabaseType.EXECUTION,db);
				db=null;
			}
		}
	}

	/**
	 * Build a list of pollutant process ids for the pollutants in the runspec.
	**/
	private void getPollutantProcessIDs() {
		for (Iterator i = ExecutionRunSpec.theExecutionRunSpec.pollutantProcessAssociations.iterator();
				i.hasNext();) {
			PollutantProcessAssociation polProcess = (PollutantProcessAssociation) i.next();
			if (startExhaust != null && polProcess.emissionProcess.compareTo(startExhaust)==0) {
				if ((totalGaseousHydrocarbons != null && polProcess.pollutant.compareTo(totalGaseousHydrocarbons)==0) ||
						(nonMethaneOrganicGases != null && polProcess.pollutant.compareTo(nonMethaneOrganicGases)==0) ||
						(nonMethaneHydrocarbons != null && polProcess.pollutant.compareTo(nonMethaneHydrocarbons)==0) ||
						(totalOrganicGases != null && polProcess.pollutant.compareTo(totalOrganicGases)==0) ||
						(volatileOrganicCompounds != null && polProcess.pollutant.compareTo(volatileOrganicCompounds)==0) ||
						(carbonMonoxideCO != null && polProcess.pollutant.compareTo(carbonMonoxideCO)==0) ||
						(oxidesOfNitrogen != null && polProcess.pollutant.compareTo(oxidesOfNitrogen)==0) ||
						(nitrousOxide != null && polProcess.pollutant.compareTo(nitrousOxide)==0) ||
						(nH3 != null && polProcess.pollutant.compareTo(nH3)==0) ||
						(eCarbon25 != null && polProcess.pollutant.compareTo(eCarbon25)==0) ||
						(nonECPM25 != null && polProcess.pollutant.compareTo(nonECPM25)==0) ||
						(totalEnergyConsumption != null && polProcess.pollutant.compareTo(totalEnergyConsumption)==0)) {
					polProcessIDs += "," + polProcess.getDatabaseKey(db);
				}
			}
		}
	}

	/**
	 * Determines the soak time for each sample vehicle trip by subtracking the keyontime of the trip
	 * from the keyofftime of the previous trip.
	**/
	void calculateSoakTime() {
		String sql = "";

		try {
			sql = "DROP TABLE IF EXISTS soaktime";
			SQLRunner.executeSQL(db, sql);

			/**
			 * @step 100
			 * @algorithm Determine the soak time for each sample vehicle trip by subtracking the keyontime of the trip
			 * from the keyofftime of the previous trip.
			 * soakTime = keyOnTime[trip t] - keyOffTime[trip t-1].
			 * @output SoakTime
			 * @input SampleVehicleTrip
			**/
			sql = "CREATE TABLE soaktime "
					+ "select svt2.vehid, svt2.tripid, svt2.keyontime - svt1.keyofftime as soaktime "
					+ "from samplevehicletrip svt1 "
					+ "inner join samplevehicletrip svt2 on (svt2.vehid = svt1.vehid "
					+ "and svt2.priortripid = svt1.tripid)";
			SQLRunner.executeSQL(db, sql);
		} catch (SQLException e) {
			Logger.logSqlError(e,"Could not determine soak times for sample vehicle trips.", sql);
		}
	}

	/**
	 * Calculates the starting operating mode for each sample vehicle trip.
	**/
	void calculateStartOpMode() {
		String sql = "";

		try {
			sql = "DROP TABLE IF EXISTS startopmode";
			SQLRunner.executeSQL(db, sql);

			/**
			 * @step 200
			 * @algorithm Find the operating mode for each soaking vehicle.
			 * @condition (minSoakTime <= soakTime OR (minSoakTime IS NULL AND maxSoakTime
			 * IS NOT NULL)) AND (maxSoakTime > soakTime OR (maxSoakTime IS NULL AND
			 * minSoakTime IS NOT NULL)).
			 * @output StartOpMode
			 * @input OperatingMode
			 * @input SoakTime
			**/
			sql = "CREATE TABLE IF NOT EXISTS startopmode "
					+ "select vehid, tripid, opmodeid from soaktime "
					+ "inner join operatingmode "
					+ "where (minsoaktime <= soaktime or (minsoaktime is null and maxsoaktime "
					+ "is not null)) and (maxsoaktime > soaktime or (maxsoaktime is null and "
					+ "minsoaktime is not null)) order by vehid, tripid";
			SQLRunner.executeSQL(db, sql);
		} catch (SQLException e) {
			Logger.logSqlError(e,"Could not determine operating mode for sample vehicle trips.", sql);
		}
	}

	/**
	 * Calculates the fraction of times a vehicle is in a particular operating mode each day.
	**/
	void calculateOpModeFraction() {
		String sql = "";

		try {
			sql = "DROP TABLE IF EXISTS startspervehicleday";
			SQLRunner.executeSQL(db, sql);

			// Number of vehicle starts on day
			
			/**
			 * @step 300
			 * @algorithm Find the number of vehicle starts per day.
			 * starts = count(keyOnTime).
			 * @output StartsPerVehicleDay
			 * @input SampleVehicleDay
			 * @input SampleVehicleTrip
			 * @input StartOpMode
			 * @input HourDay
			 * @input RunSpecHourDay
			**/
			sql = "CREATE TABLE IF NOT EXISTS startspervehicleday "
					+ "select sourcetypeid, hd.hourdayid, count(keyontime) as starts, "
					+ "hd.dayid, hd.hourid "
					+ "from samplevehicleday sv "
					+ "inner join samplevehicletrip svt on (svt.vehid=sv.vehid) "
					+ "inner join startopmode som on (som.vehid=svt.vehid "
					+ "and som.tripid = svt.tripid) "
					+ "inner join hourday hd on (hd.dayid=svt.dayid and hd.hourid=svt.hourid) "
					+ "inner join runspechourday rshd on (rshd.hourdayid=hd.hourdayid)"
					+ "group by sourcetypeid, hourdayid "
					+ "order by sourcetypeid, hourdayid";
			SQLRunner.executeSQL(db, sql);

			sql = "DROP TABLE IF EXISTS startopmodedistribution";
			SQLRunner.executeSQL(db, sql);

			// Number of times in opmode on day

			/**
			 * @step 300
			 * @algorithm Calculate the operating mode fraction.
			 * opModeFraction = count(opModeID)/starts.
			 * @output StartOpModeDistribution
			 * @input SampleVehicleDay
			 * @input SampleVehicleTrip
			 * @input StartOpMode
			 * @input StartsPerVehicleDay
			**/
			sql = "CREATE TABLE IF NOT EXISTS startopmodedistribution "
					+ "select sv.sourcetypeid, svd.hourdayid, opmodeid, "
					+ "count(opmodeid)/starts as opmodefraction "
					+ "from samplevehicleday sv "
					+ "inner join samplevehicletrip svt on (svt.vehid=sv.vehid) "
					+ "inner join startopmode som on (som.vehid=svt.vehid "
					+ "and som.tripid=svt.tripid) "
					+ "inner join startspervehicleday svd on (svd.sourcetypeid= "
					+ "sv.sourcetypeid and svd.dayid=svt.dayid "
					+ "and svd.hourid=svt.hourid) "
					+ "group by sv.sourcetypeid, svd.hourdayid, opmodeid "
					+ "order by sv.sourcetypeid, svd.hourdayid, opmodeid";
			SQLRunner.executeSQL(db, sql);

			sql = "drop table if exists somdgopmodes";
			SQLRunner.executeSQL(db, sql);

			/**
			 * @step 300
			 * @algorithm Find the set of unique opModeID values within StartOpModeDistribution.
			 * @output SOMDGOpModes
			 * @input StartOpModeDistribution
			**/
			sql = "create table somdgopmodes select distinct opmodeid from startopmodedistribution";
			SQLRunner.executeSQL(db, sql);

			sql = "alter table somdgopmodes add primary key (opmodeid)";
			SQLRunner.executeSQL(db, sql);

			sql = "drop table if exists existingstartomd";
			SQLRunner.executeSQL(db, sql);
			
			sql = "create table existingstartomd like opmodedistribution";
			SQLRunner.executeSQL(db, sql);

			sql = "alter table opmodedistribution add key somdg_key (sourcetypeid, linkid, polprocessid, opmodeid)";
			SQLRunner.executeSQL(db, sql);

			sql = "analyze table opmodedistribution";
			SQLRunner.executeSQL(db, sql);

			sql = "alter table existingstartomd add key somdg_key (sourcetypeid, linkid, polprocessid, opmodeid)";
			SQLRunner.executeSQL(db, sql);

			/**
			 * @step 300
			 * @algorithm Find the set of operating mode distribution entries that already exist.
			 * @output existingStartOMD
			 * @input SOMDGOpModes
			 * @input opModeDistribution
			**/
			sql = "insert into existingstartomd (sourcetypeid, hourdayid, linkid, polprocessid, opmodeid, opmodefraction)"
					+ " select sourcetypeid, hourdayid, linkid, polprocessid, omd.opmodeid, opmodefraction"
					+ " from somdgopmodes soms"
					+ " inner join opmodedistribution omd using (opmodeid)"
					+ " group by sourcetypeid, linkid, polprocessid";
			SQLRunner.executeSQL(db, sql);
		} catch (SQLException e) {
			Logger.logSqlError(e,"Could not determine operating mode fraction.", sql);
		}
	}

	/**
	 * Copies entries from fractions table to OpModeDistribution table
	**/
	void populateOperatingModeDistribution(MasterLoopContext inContext) {
		String sql = "";

		try {
			if(CompilationFlags.DO_RATES_FIRST) {
				if(isProject) {
					/**
					 * @step 400
					 * @algorithm Populate RatesOpModeDistribution.
					 * @output RatesOpModeDistribution
					 * @input OpModeDistribution
					 * @input sourceTypePolProcess
					 * @input opModePolProcAssoc
					 * @condition Project domain
					**/
					sql = "INSERT IGNORE INTO ratesopmodedistribution ("+
								"sourcetypeid,"+
								"roadtypeid,"+
								"avgspeedbinid,"+
								"hourdayid,"+
								"polprocessid,"+
								"opmodeid,"+
								"opmodefraction) "+
							"select "+
								"somd.sourcetypeid,"+
								"1 as roadtypeid,"+
								"0 as avgspeedbinid,"+
								"somd.hourdayid,"+
								"omppa.polprocessid,"+
								"somd.opmodeid,"+
								"somd.opmodefraction "+
							"from "+
								"opmodedistribution somd "+
								"inner join sourcetypepolprocess stpp on (stpp.sourcetypeid = somd.sourcetypeid) "+
								"inner join opmodepolprocassoc omppa on ("+
									"omppa.polprocessid = stpp.polprocessid "+
									"and omppa.opmodeid = somd.opmodeid) "+
							"where "+
								"stpp.polprocessid in (" + polProcessIDs.substring(1) + ")";
					SQLRunner.executeSQL(db, sql);
				} else {
					/**
					 * @step 400
					 * @algorithm Populate RatesOpModeDistribution.
					 * @output RatesOpModeDistribution
					 * @input StartOpModeDistribution
					 * @input sourceTypePolProcess
					 * @input opModePolProcAssoc
					 * @condition Non-Project domain
					**/
					sql = "insert ignore into ratesopmodedistribution (avgspeedbinid, roadtypeid, "
					+ " sourcetypeid, hourdayid, polprocessid, opmodeid, opmodefraction, opmodefractioncv)"
					+ " select distinct 0 as avgspeedbinid, 1 as roadtypeid, "
					+ " somd.sourcetypeid, (somd.hourid*10+somd.dayid) as hourdayid, ppa.polprocessid, somd.opmodeid, somd.opmodefraction, null as opmodefractioncv"
					+ " from startsopmodedistribution somd "
					+ " cross join pollutantprocessassoc ppa"
					+ " where ppa.processid in (2,16)";
					SQLRunner.executeSQL(db, sql);
				}

				/**
				 * @step 400
				 * @algorithm Add information for All Starts using operating mode 100 and polProcessID 602.
				 * opModeFraction[opModeID=100,polProcessID=602]=1.0.
				 * @output RatesOpModeDistribution
				 * @input runSpecHourDay
				 * @input runSpecSourceType
				**/
				sql = "INSERT IGNORE INTO ratesopmodedistribution ("+
							"sourcetypeid,"+
							"roadtypeid,"+
							"avgspeedbinid,"+
							"hourdayid,"+
							"polprocessid,"+
							"opmodeid,"+
							"opmodefraction) "+
						"select distinct "+
							"sourcetypeid,"+
							"1 as roadtypeid,"+
							"0 as avgspeedbinid,"+
							"hourdayid,"+
							"602 as polprocessid,"+
							"100 as opmodeid,"+
							"1 as opmodefraction "+
						"from "+
							"runspechourday, "+
							"runspecsourcetype";
				SQLRunner.executeSQL(db, sql);
			} else {
				// For zone emissions, only work with the off-network road type.
				if(inContext.iterLocation.roadTypeRecordID != 1) {
					return;
				}
				sql = "insert ignore into opmodedistribution (sourcetypeid, hourdayid, linkid, polprocessid, opmodeid, opmodefraction, opmodefractioncv)"
					+ " select distinct somd.sourcetypeid, (somd.hourid*10+somd.dayid) as hourdayid, l.linkid, ppa.polprocessid, somd.opmodeid, somd.opmodefraction, null as opmodefractioncv"
					+ " from startsopmodedistribution somd "
					+ " cross join pollutantprocessassoc ppa "
					+ " cross join link l"
					+ " where ppa.processid in (2,16)";
				SQLRunner.executeSQL(db, sql);
			}
		} catch (SQLException e) {
			Logger.logSqlError(e,"Could not populate operating mode distribution for starts.", sql);
		}
	}
}
