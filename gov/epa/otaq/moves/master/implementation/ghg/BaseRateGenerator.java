/**************************************************************************************************
 * @(#)BaseRateGenerator.java
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
 * This builds the BaseRate and BaseRateByAge tables
 *
 * @author		Wesley Faler
 * @version		2017-09-17
**/
public class BaseRateGenerator extends Generator {
	// true when the external generator should be invoked for some of the algorithms
	public static final boolean USE_EXTERNAL_GENERATOR = true;

	/**
	 * @algorithm
	 * @owner Base Rate Generator
	 * @generator
	**/

	/**
	 * true to use BaseRateByAgeHelper to perform direct calculations instead of SQL.
	 * This flag is ignored when USE_EXTERNAL_GENERATOR is true.
	**/
	static final boolean useBaseRateByAgeHelper = true;

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
	/** Processes that have already been calculated **/
	TreeSet<Integer> processesDone = new TreeSet<Integer>();
	/** Links that have already been calculated **/
	TreeSet<Integer> linksDone = new TreeSet<Integer>();
	/** SourceBinDistribution tables that have been processed. **/
	TreeSet<String> sourceBinTablesDone = new TreeSet<String>();
	/** true when used for a Project domain run **/
	boolean isProjectDomain = false;
	/** road type used in a previous run of generateBaseRates() **/
	int previousRoadTypeID = 0;

	/** Default constructor **/
	public BaseRateGenerator() {
	}

	/**
	 * Requests that this object subscribe to the given loop at desired looping points.
	 * Objects can assume that all necessary MasterLoopable objects have been instantiated.
	 *
	 * @param targetLoop The loop to subscribe to.
	**/
	public void subscribeToMe(MasterLoop targetLoop) {
		isProjectDomain = ExecutionRunSpec.theExecutionRunSpec.getModelDomain() == ModelDomain.PROJECT;

		String[] processNames = {
			"Running Exhaust",
			"Start Exhaust",
			"Extended Idle Exhaust",
			"Auxiliary Power Exhaust",
			"Brakewear",
			"Tirewear"
		};
		for(int i=0;i<processNames.length;i++) {
			EmissionProcess process = EmissionProcess.findByName(processNames[i]);
			if(process != null) {
				// Signup at the YEAR level to be compatible with the SBDG but we only do our work
				// once per process.
				targetLoop.subscribe(this, process, MasterLoopGranularity.YEAR,
						MasterLoopPriority.GENERATOR-2); // Run after SBDG, OMDG, StartOMDG, and FuelEffectsGenerator.
			}
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

			long start, detailStart, detailEnd;

			// The following only has to be done once for each run.
			if(!hasBeenSetup) {
				start = System.currentTimeMillis();
				// Do any setup items here.
				isValid = true;
				hasBeenSetup = true;
				addIndexes();
				setupTime += System.currentTimeMillis() - start;
			}

			start = System.currentTimeMillis();

			Integer processID = Integer.valueOf(inContext.iterProcess.databaseKey);
			boolean isNewProcess = !processesDone.contains(processID);
			if(isNewProcess) {
				processesDone.add(processID);
				linksDone.clear();
				Logger.log(LogMessageCategory.DEBUG,"Base Rate Generator called for process: " +
					 inContext.iterProcess.toString());
			}
			Integer linkID = Integer.valueOf(inContext.iterLocation.linkRecordID);
			boolean isNewLink = !linksDone.contains(linkID);
			if(isProjectDomain && isNewLink) {
				linksDone.add(linkID);
				Logger.log(LogMessageCategory.DEBUG,"Base Rate Generator called for link: " + linkID);
			}

			if(isProjectDomain) {
				boolean roadTypeIsUseful = true;
				if(inContext.iterLocation.roadTypeRecordID != 1
						&& (inContext.iterProcess.databaseKey == 2
							|| inContext.iterProcess.databaseKey == 90
							|| inContext.iterProcess.databaseKey == 91)) {
					roadTypeIsUseful = false;
				} else if(inContext.iterProcess.databaseKey == 1 && inContext.iterLocation.roadTypeRecordID == 1) {
					roadTypeIsUseful = false;
				}
				boolean madeNewRates = false;
				if(isValid) {
					madeNewRates = generateSBWeightedEmissionRates(inContext.iterProcess.databaseKey,inContext.iterLocation.countyRecordID,inContext.year);
				}
				if((madeNewRates && roadTypeIsUseful) || (isValid && isNewLink && roadTypeIsUseful)) {
					generateBaseRates(inContext,inContext.iterProcess.databaseKey,inContext.iterLocation.roadTypeRecordID,inContext.year);
				}
			} else {
				boolean madeNewRates = false;
				if(isValid) {
					detailStart = System.currentTimeMillis();
					madeNewRates = generateSBWeightedEmissionRates(inContext.iterProcess.databaseKey,inContext.iterLocation.countyRecordID,inContext.year);
					detailEnd = System.currentTimeMillis();
					Logger.log(LogMessageCategory.DEBUG,"BaseRateGenerator.generateSBWeightedEmissionRates ms="+(detailEnd-detailStart));
				}
				if(madeNewRates || (isValid && isNewProcess)) {
					detailStart = System.currentTimeMillis();
					generateBaseRates(inContext,inContext.iterProcess.databaseKey,0,inContext.year);
					detailEnd = System.currentTimeMillis();
					Logger.log(LogMessageCategory.DEBUG,"BaseRateGenerator.generateBaseRates ms="+(detailEnd-detailStart));
				}
			}
			totalTime += System.currentTimeMillis() - start;
		} catch (Exception e) {
			Logger.logError(e,"Base Rate Generation failed.");
		} finally {
			DatabaseConnectionManager.checkInConnection(MOVESDatabaseType.EXECUTION, db);
			db = null;
		}

		Logger.log(LogMessageCategory.INFO,"BRG setupTime=" + setupTime + " bundleTime=" + totalTime);
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

	/** Add indexes to key tables. **/
	void addIndexes() {
		String[] statements = {
// TODO 
// Put back after T1506 testing			//"alter table ratesOpModeDistribution add key speed1 (sourceTypeID,polProcessID,roadTypeID,hourDayID,opModeID,avgSpeedBinID)",
// Put back after T1506 testing			//"analyze table ratesOpModeDistribution",
// TODO 

			"alter table avgspeedbin add key speed1 (avgspeedbinid, avgbinspeed)",
			"alter table avgspeedbin add key speed2 (avgbinspeed, avgspeedbinid)",
			"analyze table avgspeedbin"
		};
		long detailStart, detailEnd;
		for(int i=0;i<statements.length;i++) {
			try {
				Logger.log(LogMessageCategory.DEBUG,"BaseRateGenerator.addIndexes: " + statements[i]);
				detailStart = System.currentTimeMillis();
				SQLRunner.executeSQL(db,statements[i]);
				detailEnd = System.currentTimeMillis();
				Logger.log(LogMessageCategory.DEBUG,"BaseRateGenerator.addIndexes ms="+(detailEnd-detailStart));
			} catch(Exception e) {
				// Ignore these exceptions as they are likely due to the index already existing,
				// which is acceptable.
			}
		}
	}

	/**
	 * Create and fill SBWeightedEmissionRate and SBWeightedEmissionRateByAge tables.
	 * @param processID emission process
	 * @param countyID county identifier
	 * @param year calendar year
	 * @return true if new rates were created
	**/
	boolean generateSBWeightedEmissionRates(int processID, int countyID, int year) {
		String sbdKey = "";
		String sbdTable = "";
		if(CompilationFlags.USE_FUELUSAGEFRACTION) {
			sbdTable = "sourcebindistributionfuelusage_" + processID + "_" + countyID + "_" + year;
			sbdKey = sbdTable;
		} else {
			sbdTable = "sourcebindistribution";
			sbdKey = "sourcebindistribution" + processID;
		}
		if(sourceBinTablesDone.contains(sbdKey)) {
			return false;
		}
		sourceBinTablesDone.add(sbdKey);

		String normalize = "";

		boolean isStartsOrExtIdleOrAPU = (processID == 2 || processID == 90 || processID == 91);
		if(ExecutionRunSpec.theExecutionRunSpec.getModelScale() == ModelScale.MESOSCALE_LOOKUP) {
			/**
			 * @step 008
			 * @algorithm normalizationFactor = sum(sourceBinActivityFraction).
			 * @condition Rates creation
			**/
			Logger.log(LogMessageCategory.DEBUG,"Normalizing sourcebin-weighted emission rates");
			normalize = "/ SUM(sbd.sourcebinactivityfraction)";
		} else {
			/**
			 * @step 008
			 * @algorithm normalizationFactor = 1.
			 * @condition Inventory creation
			**/
			Logger.log(LogMessageCategory.DEBUG,"Not normalizing sourcebin-weighted emission rates");
		}
		
		// at project scale, we need to join on the offsets so we don't duplicate the opMode distribution for each row of sourceusetypephysics
		String projectStringJoin, projectStringWhere;
		if (isProjectDomain && (processID == 9 || processID == 1)) {
			projectStringJoin = "		left join sourceusetypephysicsmapping sutpm on ("
				+ "		    sutpm.realsourcetypeid = stmy.sourcetypeid"
				+ "         and sutpm.regclassid = sb.regclassid"
				+ "         and left(sutpm.opmodeidoffset, 2) = left(er.opmodeid, 2))";
			projectStringWhere = "  	(ppmy.modelyearid BETWEEN sutpm.beginmodelyearid and sutpm.endmodelyearid or er.opmodeid = 501) AND";
		} else  {
			projectStringJoin = "";
			projectStringWhere = "";
		}

		//boolean applyHotelling = processID == 91; OLD as of T1702
		
		String[] statements = {
			"#CORE",
			"TRUNCATE TABLE baseratebyage",
			"TRUNCATE TABLE baserate",

			"create table if not exists baseratebyage_" + processID + "_" + year + " like baseratebyage",
			"create table if not exists baserate_" + processID + "_" + year + " like baserate",
			"truncate table baseratebyage_" + processID + "_" + year,
			"truncate table baserate_" + processID + "_" + year,

			"#sbweightedemissionratebyage",
			"TRUNCATE TABLE sbweightedemissionratebyage",
			"#sbweightedemissionrate",
			"TRUNCATE TABLE sbweightedemissionrate",
			"#sbweighteddistancerate",
			"TRUNCATE TABLE sbweighteddistancerate",

			/**
			 * @step 010
			 * @algorithm Weight age-based rates by sourcebin distribution.
			 * MeanBaseRate=sum(SourceBinActivityFraction * MeanBaseRate)/normalizationFactor.
			 * MeanBaseRateIM=sum(SourceBinActivityFraction * MeanBaseRateIM)/normalizationFactor.
			 * MeanBaseRateACAdj=sum(SourceBinActivityFraction * MeanBaseRate * (coalesce(fullACAdjustment,1.0)-1.0))/normalizationFactor.
			 * MeanBaseRateIMACAdj=sum(SourceBinActivityFraction * MeanBaseRateIM * (coalesce(fullACAdjustment,1.0)-1.0))/normalizationFactor.
			 * sumSBD=sum(SourceBinActivityFraction)/normalizationFactor.
			 * sumSBDRaw=sum(SourceBinActivityFraction).
			 * @output SBWeightedEmissionRateByAge
			 * @input EmissionRateByAge
			 * @input PollutantProcessModelYear
			 * @input SourceBin
			 * @input SourceBinDistribution
			 * @input SourceTypeModelYear
			 * @input RunspecModelYearAgeGroup
			 * @input PollutantProcessAssoc
			 * @input fullACAdjustment
			**/
			"#sbweightedemissionratebyage",
			"INSERT INTO sbweightedemissionratebyage ("
			+ " 	sourcetypeid,"
			+ " 	polprocessid,"
			+ " 	modelyearid,"
			+ " 	fueltypeid,"
			+ " 	opmodeid,"
			+ " 	agegroupid,"
			+ "		regclassid,"
			+ " 	meanbaserate,"
			+ " 	meanbaserateim,"
			+ " 	meanbaserateacadj,"
			+ " 	meanbaserateimacadj,"
			+ "		sumsbd, sumsbdraw)"
			+ " select"
			+ " 	stmy.sourcetypeid,"
			+ " 	er.polprocessid,"
			+ " 	stmy.modelyearid,"
			+ " 	sb.fueltypeid,"
			+ " 	er.opmodeid,"
			+ " 	er.agegroupid,"
			+ "		sb.regclassid,"
			+ " 	SUM(sbd.sourcebinactivityfraction * er.meanbaserate)" + normalize + ","
			+ " 	SUM(sbd.sourcebinactivityfraction * er.meanbaserateim)" + normalize + ","
			+ " 	SUM(sbd.sourcebinactivityfraction * er.meanbaserate * (coalesce(fullacadjustment,1.0)-1.0))" + normalize + ","
			+ " 	SUM(sbd.sourcebinactivityfraction * er.meanbaserateim * (coalesce(fullacadjustment,1.0)-1.0))" + normalize + ","
			+ "		SUM(sbd.sourcebinactivityfraction)" + normalize + ","
			+ "		SUM(sbd.sourcebinactivityfraction)"
			+ " FROM"
			+ " 	emissionratebyage er"
			+ " 	inner join pollutantprocessmodelyear ppmy"
			+ " 	inner join sourcebin sb"
			+ " 	inner join " + sbdTable + " sbd"
			+ " 	inner join sourcetypemodelyear stmy"
			+ " 	inner join runspecmodelyearagegroup rsmy"
			+ " 	inner join pollutantprocessassoc ppa"
			+ " 	left outer join fullacadjustment fac on ("
			+ " 		fac.sourcetypeid = stmy.sourcetypeid"
			+ " 		and fac.polprocessid = er.polprocessid"
			+ " 		and fac.opmodeid = er.opmodeid)"
			+ " WHERE"
			+ " 	ppmy.modelyeargroupid = sb.modelyeargroupid and"
			+ " 	ppmy.modelyearid = stmy.modelyearid and"
			+ " 	er.polprocessid = ppmy.polprocessid and"
			+ " 	er.polprocessid = sbd.polprocessid and"
			+ " 	ppmy.polprocessid = sbd.polprocessid and"
			+ " 	er.sourcebinid = sb.sourcebinid and"
			+ " 	er.sourcebinid = sbd.sourcebinid and"
			+ " 	sb.sourcebinid = sbd.sourcebinid and"
			+ " 	sbd.sourcetypemodelyearid = stmy.sourcetypemodelyearid and"
			+ " 	stmy.modelyearid = rsmy.modelyearid and"
			+ " 	ppa.polprocessid = er.polprocessid and"
			+ " 	er.agegroupid = rsmy.agegroupid and"
			+ " 	ppa.processid = " + processID + " AND"
			+ " 	rsmy.yearid = " + year
			+ " GROUP BY"
			+ "		stmy.sourcetypeid,"
			+ " 	er.polprocessid,"
			+ " 	stmy.modelyearid,"
			+ " 	sb.fueltypeid,"
			+ " 	er.opmodeid,"
			+ " 	er.agegroupid,"
			+ "		sb.regclassid"
			+ " HAVING SUM(sbd.sourcebinactivityfraction) > 0",

			/**
			 * @step 010
			 * @algorithm Weight non-age-based rates by sourcebin distribution.
			 * MeanBaseRate=sum(SourceBinActivityFraction * MeanBaseRate)/normalizationFactor.
			 * MeanBaseRateIM=sum(SourceBinActivityFraction * MeanBaseRateIM)/normalizationFactor.
			 * MeanBaseRateACAdj=sum(SourceBinActivityFraction * MeanBaseRate * (coalesce(fullACAdjustment,1.0)-1.0))/normalizationFactor.
			 * MeanBaseRateIMACAdj=sum(SourceBinActivityFraction * MeanBaseRateIM * (coalesce(fullACAdjustment,1.0)-1.0))/normalizationFactor.
			 * sumSBD=sum(SourceBinActivityFraction)/normalizationFactor.
			 * sumSBDRaw=sum(SourceBinActivityFraction).
			 * @output SBWeightedEmissionRate
			 * @input EmissionRate
			 * @input PollutantProcessModelYear
			 * @input SourceBin
			 * @input SourceBinDistribution
			 * @input SourceTypeModelYear
			 * @input RunspecModelYear
			 * @input PollutantProcessAssoc
			 * @input fullACAdjustment
			**/
			"#sbweightedemissionrate",
			"INSERT INTO sbweightedemissionrate ("
			+ " 	sourcetypeid,"
			+ " 	polprocessid,"
			+ " 	modelyearid,"
			+ " 	fueltypeid,"
			+ " 	opmodeid,"
			+ " 	regclassid,"
			+ " 	meanbaserate,"
			+ " 	meanbaserateim,"
			+ " 	meanbaserateacadj,"
			+ " 	meanbaserateimacadj,"
			+ "		sumsbd, sumsbdraw)"
			+ " SELECT"
			+ " 	stmy.sourcetypeid,"
			+ " 	er.polprocessid,"
			+ " 	stmy.modelyearid,"
			+ " 	sb.fueltypeid,"
			+ " 	er.opmodeid,"
			+ "		sb.regclassid,"
			+ " 	sum(sbd.sourcebinactivityfraction * er.meanbaserate)" + normalize + ","
			+ " 	sum(sbd.sourcebinactivityfraction * er.meanbaserateim)" + normalize + ","
			+ " 	sum(sbd.sourcebinactivityfraction * er.meanbaserate * (coalesce(fullacadjustment,1.0)-1.0))" + normalize + ","
			+ " 	sum(sbd.sourcebinactivityfraction * er.meanbaserateim * (coalesce(fullacadjustment,1.0)-1.0))" + normalize + ","
			+ "		sum(sbd.sourcebinactivityfraction)" + normalize + ","
			+ "		sum(sbd.sourcebinactivityfraction)"
			+ " FROM"
			+ " 	emissionrate er"
			+ " 	inner join pollutantprocessmodelyear ppmy"
			+ " 	inner join sourcebin sb"
			+ " 	inner join " + sbdTable + " sbd"
			+ " 	inner join sourcetypemodelyear stmy"
			+ " 	inner join runspecmodelyear rsmy"
			+ " 	inner join pollutantprocessassoc ppa"
			+ 		projectStringJoin
			+ " 	left outer join fullacadjustment fac on ("
			+ " 		fac.sourcetypeid = stmy.sourcetypeid"
			+ " 		and fac.polprocessid = er.polprocessid"
			+ " 		and fac.opmodeid = er.opmodeid)"
			+ " WHERE"
			+ " 	ppmy.modelyeargroupid = sb.modelyeargroupid and"
			+ " 	ppmy.modelyearid = stmy.modelyearid and"
			+ 		projectStringWhere
			+ " 	er.polprocessid = ppmy.polprocessid and"
			+ " 	er.polprocessid = sbd.polprocessid and"
			+ " 	ppmy.polprocessid = sbd.polprocessid and"
			+ " 	er.sourcebinid = sb.sourcebinid and"
			+ " 	er.sourcebinid = sbd.sourcebinid and"
			+ " 	sb.sourcebinid = sbd.sourcebinid and"
			+ " 	sbd.sourcetypemodelyearid = stmy.sourcetypemodelyearid and"
			+ " 	stmy.modelyearid = rsmy.modelyearid and"
			+ " 	ppa.polprocessid = er.polprocessid and"
			+ " 	ppa.processid = " + processID
			+ " GROUP BY"
			+ "		stmy.sourcetypeid,"
			+ " 	er.polprocessid,"
			+ " 	stmy.modelyearid,"
			+ " 	sb.fueltypeid,"
			+ " 	er.opmodeid,"
			+ "		sb.regclassid"
			+ " HAVING SUM(sbd.sourcebinactivityfraction) > 0",

			/**
			 * @step 020
			 * @algorithm Weight distance-based rates by sourcebin distribution.
			 * Use fullACAdjustment for opModeID=300 (All Running).
			 * Use the SourceBinDistribution for Running Exhaust Total Gaseous Hydrocarbons (pol/proc 101).
			 * MeanBaseRate=sum(SourceBinActivityFraction * ratePerSHO)/normalizationFactor.
			 * MeanBaseRateIM=sum(SourceBinActivityFraction * ratePerSHO)/normalizationFactor.
			 * MeanBaseRateACAdj=sum(SourceBinActivityFraction * ratePerSHO * (coalesce(fullACAdjustment,1.0)-1.0))/normalizationFactor.
			 * MeanBaseRateIMACAdj=sum(SourceBinActivityFraction * ratePerSHO * (coalesce(fullACAdjustment,1.0)-1.0))/normalizationFactor.
			 * sumSBD=sum(SourceBinActivityFraction)/normalizationFactor.
			 * sumSBDRaw=sum(SourceBinActivityFraction).
			 * @output SBWeightedDistanceRate
			 * @input distanceEmissionRate
			 * @input PollutantProcessModelYear
			 * @input SourceBin
			 * @input SourceBinDistribution
			 * @input SourceTypeModelYear
			 * @input PollutantProcessAssoc
			 * @input modelYearGroup
			 * @input fullACAdjustment
			 * @condition Running Exhaust
			**/
			"#sbweighteddistancerate",
			(processID == 1?
			"INSERT INTO sbweighteddistancerate ("
			+ " 	sourcetypeid,"
			+ " 	polprocessid,"
			+ " 	modelyearid,"
			+ " 	fueltypeid,"
			+ " 	regclassid,"
			+ " 	avgspeedbinid,"
			+ " 	meanbaserate,"
			+ " 	meanbaserateim,"
			+ " 	meanbaserateacadj,"
			+ " 	meanbaserateimacadj,"
			+ "		sumsbd, sumsbdraw)"
			+ " SELECT"
			+ " 	er.sourcetypeid,"
			+ " 	er.polprocessid,"
			+ " 	er.modelyearid,"
			+ " 	er.fueltypeid,"
			+ "		sb.regclassid,"
			+ " 	er.avgspeedbinid,"
			+ " 	sum(sbd.sourcebinactivityfraction * er.ratepersho)" + normalize + ","
			+ " 	sum(sbd.sourcebinactivityfraction * er.ratepersho)" + normalize + ","
			+ " 	sum(sbd.sourcebinactivityfraction * er.ratepersho * (coalesce(fullacadjustment,1.0)-1.0))" + normalize + ","
			+ " 	sum(sbd.sourcebinactivityfraction * er.ratepersho * (coalesce(fullacadjustment,1.0)-1.0))" + normalize + ","
			+ "		sum(sbd.sourcebinactivityfraction)" + normalize + ","
			+ "		sum(sbd.sourcebinactivityfraction)"
			+ " FROM"
			+ " 	distanceemissionrate er"
			+ " 	inner join sourcebin sb"
			+ " 	inner join " + sbdTable + " sbd"
			+ " 	inner join sourcetypemodelyear stmy"
			+ " 	inner join pollutantprocessassoc ppa"
			+ "		inner join modelyeargroup myg on ("
			+ "			myg.modelyeargroupid = sb.modelyeargroupid"
			+ "			and myg.modelyeargroupstartyear <= er.modelyearid"
			+ "			and myg.modelyeargroupendyear >= er.modelyearid)"
			+ " 	left outer join fullacadjustment fac on ("
			+ " 		fac.sourcetypeid = stmy.sourcetypeid"
			+ " 		and fac.polprocessid = er.polprocessid"
			+ " 		and fac.opmodeid = 300)" // opModeID=300 is All Running
			+ " WHERE"
			+ " 	er.modelyearid = stmy.modelyearid and"
			+ " 	er.fueltypeid = sb.fueltypeid and"
			+ " 	sbd.polprocessid = 101 and" // use running exhaust total gaseous hydrocarbons (101)
			+ " 	sb.sourcebinid = sbd.sourcebinid and"
			+ " 	sbd.sourcetypemodelyearid = stmy.sourcetypemodelyearid and"
			+ " 	stmy.modelyearid = er.modelyearid and"
			+ "		stmy.sourcetypeid = er.sourcetypeid and"
			+ " 	ppa.polprocessid = er.polprocessid and"
			+ " 	ppa.processid = " + processID
			+ " GROUP BY"
			+ "		er.sourcetypeid,"
			+ " 	er.polprocessid,"
			+ " 	er.modelyearid,"
			+ " 	er.fueltypeid,"
			+ "		sb.regclassid,"
			+ " 	er.avgspeedbinid"
			+ " HAVING SUM(sbd.sourcebinactivityfraction) > 0"
			:
			""),

			/**
			 * @step 025
			 * @algorithm Apply hotelling activity distribution to age-weighted rates.
			 * MeanBaseRate = MeanBaseRate * opModeFraction, for MeanBaseRate, MeanBaseRateIM, MeanBaseRateACAdj, and MeanBaseRateIMACAdj.
			 * @output SBWeightedEmissionRateByAge
			 * @input hotellingActivityDistribution
			 * @condition Auxiliary Power Exhaust
			**/
			/* OLD as of T1702
			"#SBWeightedEmissionRateByAge",
			applyHotelling?
				"update SBWeightedEmissionRateByAge, hotellingActivityDistribution"
				+ " set MeanBaseRate = MeanBaseRate * opModeFraction,"
				+ " 	MeanBaseRateIM = MeanBaseRateIM * opModeFraction,"
				+ " 	MeanBaseRateACAdj = MeanBaseRateACAdj * opModeFraction,"
				+ " 	MeanBaseRateIMACAdj = MeanBaseRateIMACAdj * opModeFraction"
				+ " where beginModelYearID <= modelYearID"
				+ " and endModelYearID >= modelYearID"
				+ " and hotellingActivityDistribution.opModeID = SBWeightedEmissionRateByAge.opModeID"
				: "",
			*/

			/**
			 * @step 025
			 * @algorithm Apply hotelling activity distribution to non-age-weighted rates.
			 * MeanBaseRate = MeanBaseRate * opModeFraction, for MeanBaseRate, MeanBaseRateIM, MeanBaseRateACAdj, and MeanBaseRateIMACAdj.
			 * @output SBWeightedEmissionRate
			 * @input hotellingActivityDistribution
			 * @condition Auxiliary Power Exhaust
			**/
			/* OLD as of T1702
			"#SBWeightedEmissionRate",
			applyHotelling?
				"update SBWeightedEmissionRate, hotellingActivityDistribution"
				+ " set MeanBaseRate = MeanBaseRate * opModeFraction,"
				+ " 	MeanBaseRateIM = MeanBaseRateIM * opModeFraction,"
				+ " 	MeanBaseRateACAdj = MeanBaseRateACAdj * opModeFraction,"
				+ " 	MeanBaseRateIMACAdj = MeanBaseRateIMACAdj * opModeFraction"
				+ " where beginModelYearID <= modelYearID"
				+ " and endModelYearID >= modelYearID"
				+ " and hotellingActivityDistribution.opModeID = SBWeightedEmissionRate.opModeID"
				: ""
			*/
		};
		String sql = "";
		TaggedSQLRunner concurrentSQL = null;
		try {
			if(processID == 1) {
				makeDistanceRates();
			}
			String context = "#CORE";
			for(int i=0;i<statements.length;i++) {
				sql = statements[i];
				if(sql != null && sql.length() > 0) {
					if(sql.startsWith("#")) {
						if(context.length() <= 0 || context.equalsIgnoreCase("#CORE")) {
							if(concurrentSQL != null) {
								concurrentSQL.execute();
								concurrentSQL.clear();
								concurrentSQL.close();
							}
						}
						context = sql;
						continue;
					}
					if(concurrentSQL == null) {
						concurrentSQL = getSQLRunner();
					}
					if(concurrentSQL == null) {
						Logger.log(LogMessageCategory.INFO,"Directly Running: " + context + ": " + StringUtilities.substring(sql,0,100));
						SQLRunner.executeSQL(db,sql);
					} else {
						concurrentSQL.add(context,sql);
					}
				}
			}
			if(concurrentSQL != null) {
				Logger.log(LogMessageCategory.INFO,"BRG SBW running concurrent statements...");
				concurrentSQL.execute();
				Logger.log(LogMessageCategory.INFO,"BRG SBW done running concurrent statements.");
			}
		} catch(Exception e) {
			Logger.logSqlError(e,"Could not generate SBWeightedEmissionRate",sql);
		} finally {
			if(concurrentSQL != null) {
				concurrentSQL.stats.print();
				concurrentSQL.onFinally();
			}
		}
		return true;
	}

	/**
	 * Populate BaseRateByAge and BaseRate tables.
	 * @param loopContext The MasterLoopContext that applies to this execution.
	 * @param processID emission process
	 * @param roadTypeID road type to be processed, 0 for all road types
	 * @param yearID calendar year, may be 0
	**/
	void generateBaseRates(MasterLoopContext loopContext, int processID, int roadTypeID, int yearID) {
		BaseRateByAgeHelper.Flags brbaFlags = new BaseRateByAgeHelper.Flags();
		boolean applyAvgSpeedDistribution = 
				(processID == 1 || processID == 9 || processID == 10)
				&& 
				(ExecutionRunSpec.theExecutionRunSpec.getModelScale() == ModelScale.MACROSCALE);
		boolean useAvgSpeedBin = (!applyAvgSpeedDistribution) && (processID == 1 || processID == 9 || processID == 10);
		boolean applySourceBinDistribution = ExecutionRunSpec.theExecutionRunSpec.getModelScale() == ModelScale.MACROSCALE;
		boolean keepOpModeID = false;

		String quantAdjust = "";
		if(ExecutionRunSpec.theExecutionRunSpec.getModelScale() == ModelScale.MESOSCALE_LOOKUP) {
			if(processID == 2 || processID == 90 || processID == 91) {
				quantAdjust = "* sumsbdraw";
				brbaFlags.useSumSBDRaw = true;
			}
		}

		if(processID == 2 || processID == 90 || processID == 91) {
			applySourceBinDistribution = true;
		}

		keepOpModeID = processID == 2;

		if(isProjectDomain && (processID == 1 || processID == 9 || processID == 10)) {
			applyAvgSpeedDistribution = false;
			useAvgSpeedBin = false;
			keepOpModeID = false;

			brbaFlags.useAvgSpeedBin = false;
		}

		String avgSpeedFraction = "";
		if(applyAvgSpeedDistribution) {
			avgSpeedFraction = "*coalesce(avgspeedfraction,0)";
			brbaFlags.useAvgSpeedFraction = true;
		}

		String sumSBD = "";
		if(applySourceBinDistribution) {
			sumSBD = "*sumSBD";
			brbaFlags.useSumSBD = true;
		}

		brbaFlags.keepOpModeID = keepOpModeID;
		brbaFlags.useAvgSpeedBin = useAvgSpeedBin;

		/**
		 * @step 101
		 * @algorithm avgSpeedFractionClause=coalesce(avgSpeedFraction,0) when conditions are met, 1 otherwise.
		 * @condition Non-Project domain; Inventory; Running exhaust, Brakewear, or Tirewear.
		**/

		/**
		 * @step 101
		 * @algorithm sumSBDClause=sumSBD when conditions are met, 1 otherwise.
		 * @condition Inventory or Starts or Extended Idling or Auxiliary Power.
		**/
		
		/**
		 * @step 101
		 * @algorithm quantAdjustClause=sumSBDRaw when conditions are met, 1 otherwise.
		 * @condition Rates for Starts, Extended Idle, or Auxiliary Power.
		**/

		Logger.log(LogMessageCategory.DEBUG,"BaseRateGenerator: processID=" + processID
				+ ", applyAvgSpeedDistribution=" + applyAvgSpeedDistribution
				+ ", useAvgSpeedBin=" + useAvgSpeedBin
				+ ", applySourceBinDistribution=" + applySourceBinDistribution
				+ ", keepOpModeID=" + keepOpModeID
				+ ", avgSpeedFraction=\"" + avgSpeedFraction + "\""
				+ ", sumSBD=\"" + sumSBD + "\"");

		String[] noOpModeStatements = {
			/**
			 * @step 110
			 * @algorithm Calculate BaseRateByAge without operating mode, retaining average speed bin.
			 * opModeFraction=sum(opModeFraction * avgSpeedFractionClause * sumSBDClause).
			 * opModeFractionRate=sum(opModeFraction * avgSpeedFractionClause * sumSBDClause).
			 * MeanBaseRate=sum(MeanBaseRate * opModeFraction * avgSpeedFractionClause * quantAdjustClause).
			 * MeanBaseRateIM=sum(MeanBaseRateIM * opModeFraction * avgSpeedFractionClause * quantAdjustClause).
			 * MeanBaseRateACAdj=sum(MeanBaseRateACAdj * opModeFraction * avgSpeedFractionClause * quantAdjustClause).
			 * MeanBaseRateIMACAdj=sum(MeanBaseRateIMACAdj * opModeFraction * avgSpeedFractionClause * quantAdjustClause).
			 * emissionRate=case when avgBinSpeed>0 then sum(MeanBaseRate * opModeFraction * avgSpeedFractionClause) / avgBinSpeed else null end.
			 * emissionRateIM=case when avgBinSpeed>0 then sum(MeanBaseRateIM * opModeFraction * avgSpeedFractionClause) / avgBinSpeed else null end.
			 * emissionRateACAdj=case when avgBinSpeed>0 then sum(MeanBaseRateACAdj * opModeFraction * avgSpeedFractionClause) / avgBinSpeed else null end.
			 * emissionRateIMACAdj=case when avgBinSpeed>0 then sum(MeanBaseRateIMACAdj * opModeFraction * avgSpeedFractionClause) / avgBinSpeed else null end.
			 * @output BaseRateByAge
			 * @input RatesOpModeDistribution
			 * @input SBWeightedEmissionRateByAge
			 * @condition Not Start Exhaust
			 * @condition Retaining average speed bin (Non-Project domain; Rates; Running Exhaust, Brakewear, or Tirewear)
			**/

			/**
			 * @step 110
			 * @algorithm Calculate BaseRateByAge without operating mode, aggregating average speed bins.
			 * opModeFraction=sum(opModeFraction * avgSpeedFractionClause * sumSBDClause).
			 * opModeFractionRate=sum(opModeFraction * avgSpeedFractionClause * sumSBDClause).
			 * MeanBaseRate=sum(MeanBaseRate * opModeFraction * avgSpeedFractionClause * quantAdjustClause).
			 * MeanBaseRateIM=sum(MeanBaseRateIM * opModeFraction * avgSpeedFractionClause * quantAdjustClause).
			 * MeanBaseRateACAdj=sum(MeanBaseRateACAdj * opModeFraction * avgSpeedFractionClause * quantAdjustClause).
			 * MeanBaseRateIMACAdj=sum(MeanBaseRateIMACAdj * opModeFraction * avgSpeedFractionClause * quantAdjustClause).
			 * emissionRate=sum(MeanBaseRate * opModeFraction * avgSpeedFractionClause).
			 * emissionRateIM=sum(MeanBaseRateIM * opModeFraction * avgSpeedFractionClause).
			 * emissionRateACAdj=sum(MeanBaseRateACAdj * opModeFraction * avgSpeedFractionClause).
			 * emissionRateIMACAdj=sum(MeanBaseRateIMACAdj * opModeFraction * avgSpeedFractionClause).
			 * @output BaseRateByAge
			 * @input RatesOpModeDistribution
			 * @input SBWeightedEmissionRateByAge
			 * @condition Not Start Exhaust
			 * @condition Aggregate average speed bins (Project domain or Inventory or Rates for Non-(Running, Brakewear, or Tirewear))
			**/

			"#baseratebyage",
			"insert into baseratebyage_" + processID + "_" + yearID + " ("
			+ " 	sourcetypeid, roadtypeid, avgspeedbinid, hourdayid, polprocessid, modelyearid, fueltypeid, agegroupid, regclassid, opmodeid,"
			+ " 	opmodefraction, opmodefractionrate, meanbaserate, meanbaserateim, meanbaserateacadj, meanbaserateimacadj, emissionrate, emissionrateim, emissionrateacadj, emissionrateimacadj, processid, pollutantid)"
			+ " select"
			+ " 	romd.sourcetypeid, romd.roadtypeid,"
			+ (!useAvgSpeedBin? " 0 as avgspeedbinid," : " romd.avgspeedbinid,")
			+ "		romd.hourdayid, romd.polprocessid,"
			+ " 	er.modelyearid, er.fueltypeid, er.agegroupid, er.regclassid, 0 as opmodeid,"
			+ "		sum(opmodefraction" + avgSpeedFraction + sumSBD + "),"
			+ "		sum(opmodefraction" + avgSpeedFraction + sumSBD + "),"
			+ " 	sum(meanbaserate * opmodefraction" + avgSpeedFraction + quantAdjust + "),"
			+ " 	sum(meanbaserateim * opmodefraction" + avgSpeedFraction + quantAdjust + "),"
			+ " 	sum(meanbaserateacadj * opmodefraction" + avgSpeedFraction + quantAdjust + "),"
			+ " 	sum(meanbaserateimacadj * opmodefraction" + avgSpeedFraction + quantAdjust + "),"
			+ (useAvgSpeedBin?
				( " 	case when avgbinspeed>0 then sum(meanbaserate * opmodefraction" + avgSpeedFraction + ") / avgbinspeed else null end,"
				+ " 	case when avgbinspeed>0 then sum(meanbaserateim * opmodefraction" + avgSpeedFraction + ") / avgbinspeed else null end,")
				:
				( " 	sum(meanbaserate * opmodefraction" + avgSpeedFraction + "),"
				+ " 	sum(meanbaserateim * opmodefraction" + avgSpeedFraction + "),")
				)
			+ (useAvgSpeedBin?
				( " 	case when avgbinspeed>0 then sum(meanbaserateacadj * opmodefraction" + avgSpeedFraction + ") / avgbinspeed else null end,"
				+ " 	case when avgbinspeed>0 then sum(meanbaserateimacadj * opmodefraction" + avgSpeedFraction + ") / avgbinspeed else null end,")
				:
				( " 	sum(meanbaserateacadj * opmodefraction" + avgSpeedFraction + "),"
				+ " 	sum(meanbaserateimacadj * opmodefraction" + avgSpeedFraction + "),")
				)
			+ " 	mod(romd.polprocessid,100) as processid, floor(romd.polprocessid/100) as pollutantid"
			+ " from"
			+ " 	ratesopmodedistribution romd"
			+ " 	inner join sbweightedemissionratebyage er on ("
			+ " 		er.sourcetypeid = romd.sourcetypeid"
			+ " 		and er.polprocessid = romd.polprocessid"
			+ " 		and er.opmodeid = romd.opmodeid"
			+ " 	)"
			+ " where romd.sourcetypeid = ##sourcetypeid## and romd.polprocessid = ##polprocessid##"
			+ (isProjectDomain && roadTypeID > 0? " and romd.roadTypeID=" + roadTypeID : "")
			+ " group by"
			+ " 	romd.sourcetypeid, romd.polprocessid, romd.roadtypeid, romd.hourdayid,"
			+ (!useAvgSpeedBin? "" : " romd.avgspeedbinid,")
			+ " 	er.modelyearid, er.fueltypeid, er.agegroupid, er.regclassid",

			/**
			 * @step 110
			 * @algorithm Calculate BaseRate without operating mode, retaining average speed bin.
			 * opModeFraction=sum(opModeFraction * avgSpeedFractionClause * sumSBDClause).
			 * opModeFractionRate=sum(opModeFraction * avgSpeedFractionClause * sumSBDClause).
			 * MeanBaseRate=sum(MeanBaseRate * opModeFraction * avgSpeedFractionClause * quantAdjustClause).
			 * MeanBaseRateIM=sum(MeanBaseRateIM * opModeFraction * avgSpeedFractionClause * quantAdjustClause).
			 * MeanBaseRateACAdj=sum(MeanBaseRateACAdj * opModeFraction * avgSpeedFractionClause * quantAdjustClause).
			 * MeanBaseRateIMACAdj=sum(MeanBaseRateIMACAdj * opModeFraction * avgSpeedFractionClause * quantAdjustClause).
			 * emissionRate=case when avgBinSpeed>0 then sum(MeanBaseRate * opModeFraction * avgSpeedFractionClause) / avgBinSpeed else null end.
			 * emissionRateIM=case when avgBinSpeed>0 then sum(MeanBaseRateIM * opModeFraction * avgSpeedFractionClause) / avgBinSpeed else null end.
			 * emissionRateACAdj=case when avgBinSpeed>0 then sum(MeanBaseRateACAdj * opModeFraction * avgSpeedFractionClause) / avgBinSpeed else null end.
			 * emissionRateIMACAdj=case when avgBinSpeed>0 then sum(MeanBaseRateIMACAdj * opModeFraction * avgSpeedFractionClause) / avgBinSpeed else null end.
			 * @output BaseRate
			 * @input RatesOpModeDistribution
			 * @input SBWeightedEmissionRate
			 * @condition Not Start Exhaust
			 * @condition Retaining average speed bin (Non-Project domain; Rates; Running Exhaust, Brakewear, or Tirewear)
			**/

			/**
			 * @step 110
			 * @algorithm Calculate BaseRate without operating mode, aggregating average speed bins.
			 * opModeFraction=sum(opModeFraction * avgSpeedFractionClause * sumSBDClause).
			 * opModeFractionRate=sum(opModeFraction * avgSpeedFractionClause * sumSBDClause).
			 * MeanBaseRate=sum(MeanBaseRate * opModeFraction * avgSpeedFractionClause * quantAdjustClause).
			 * MeanBaseRateIM=sum(MeanBaseRateIM * opModeFraction * avgSpeedFractionClause * quantAdjustClause).
			 * MeanBaseRateACAdj=sum(MeanBaseRateACAdj * opModeFraction * avgSpeedFractionClause * quantAdjustClause).
			 * MeanBaseRateIMACAdj=sum(MeanBaseRateIMACAdj * opModeFraction * avgSpeedFractionClause * quantAdjustClause).
			 * emissionRate=sum(MeanBaseRate * opModeFraction * avgSpeedFractionClause).
			 * emissionRateIM=sum(MeanBaseRateIM * opModeFraction * avgSpeedFractionClause).
			 * emissionRateACAdj=sum(MeanBaseRateACAdj * opModeFraction * avgSpeedFractionClause).
			 * emissionRateIMACAdj=sum(MeanBaseRateIMACAdj * opModeFraction * avgSpeedFractionClause).
			 * @output BaseRateByAge
			 * @input RatesOpModeDistribution
			 * @input SBWeightedEmissionRate
			 * @condition Not Start Exhaust
			 * @condition Aggregate average speed bins (Project domain or Inventory or Rates for Non-(Running, Brakewear, or Tirewear))
			**/

			"#baserate",
			"insert into baserate_" + processID + "_" + yearID + " ("
			+ " 	sourcetypeid, roadtypeid, avgspeedbinid, polprocessid, hourdayid, modelyearid, fueltypeid, regclassid, opmodeid,"
			+ " 	opmodefraction, opmodefractionrate, meanbaserate, meanbaserateim, meanbaserateacadj, meanbaserateimacadj, emissionrate, emissionrateim, emissionrateacadj, emissionrateimacadj, processid, pollutantid)"
			+ " select"
			+ " 	romd.sourcetypeid, romd.roadtypeid,"
			+ (!useAvgSpeedBin? " 0 as avgspeedbinid," : " romd.avgspeedbinid,")
			+ "		romd.polprocessid, romd.hourdayid,"
			+ " 	er.modelyearid, er.fueltypeid, er.regclassid, 0 as opmodeid,"
			+ "		sum(opmodefraction" + avgSpeedFraction + sumSBD + "),"
			+ "		sum(opmodefraction" + avgSpeedFraction + sumSBD + "),"
			+ " 	sum(meanbaserate * opmodefraction" + avgSpeedFraction + quantAdjust + "),"
			+ " 	sum(meanbaserateim * opmodefraction" + avgSpeedFraction + quantAdjust + "),"
			+ " 	sum(meanbaserateacadj * opmodefraction" + avgSpeedFraction + quantAdjust + "),"
			+ " 	sum(meanbaserateimacadj * opmodefraction" + avgSpeedFraction + quantAdjust + "),"
			+ (useAvgSpeedBin?
				( " 	case when avgbinspeed>0 then sum(meanbaserate * opmodefraction" + avgSpeedFraction + ") / avgbinspeed else null end,"
				+ " 	case when avgbinspeed>0 then sum(meanbaserateim * opmodefraction" + avgSpeedFraction + ") / avgbinspeed else null end,")
				:
				( " 	sum(meanbaserate * opmodefraction" + avgSpeedFraction + "),"
				+ " 	sum(meanbaserateim * opmodefraction" + avgSpeedFraction + "),")
				)
			+ (useAvgSpeedBin?
				( " 	case when avgbinspeed>0 then sum(meanbaserateacadj * opmodefraction" + avgSpeedFraction + ") / avgbinspeed else null end,"
				+ " 	case when avgbinspeed>0 then sum(meanbaserateimacadj * opmodefraction" + avgSpeedFraction + ") / avgbinspeed else null end,")
				:
				( " 	sum(meanbaserateacadj * opmodefraction" + avgSpeedFraction + "),"
				+ " 	sum(meanbaserateimacadj * opmodefraction" + avgSpeedFraction + "),")
				)
			+ " 	mod(romd.polprocessid,100) as processid, floor(romd.polprocessid/100) as pollutantid"
			+ " from"
			+ " 	ratesopmodedistribution romd"
			+ " 	inner join sbweightedemissionrate er on ("
			+ " 		er.sourcetypeid = romd.sourcetypeid"
			+ " 		and er.polprocessid = romd.polprocessid"
			+ " 		and er.opmodeid = romd.opmodeid"
			+ " 	)"
			+ " where romd.sourcetypeid = ##sourcetypeid## and romd.polprocessid = ##polprocessid##"
			+ (isProjectDomain && roadTypeID > 0? " and romd.roadtypeid=" + roadTypeID : "")
			+ " group by"
			+ " 	romd.sourceTypeID, romd.polProcessID, romd.roadTypeID, romd.hourDayID,"
			+ (!useAvgSpeedBin? "" : " romd.avgspeedbinid,")
			+ " 	er.modelyearid, er.fueltypeid, er.regclassiD",

			// Add from distanceEmissionRate.

			/**
			 * @step 110
			 * @algorithm Calculate BaseRate without operating mode, retaining average speed bin.
			 * opModeFraction=sum(avgSpeedFractionClause * sumSBDClause).
			 * opModeFractionRate=sum(avgSpeedFractionClause * sumSBDClause).
			 * MeanBaseRate=sum(MeanBaseRate * avgSpeedFractionClause * quantAdjustClause).
			 * MeanBaseRateIM=sum(MeanBaseRateIM * avgSpeedFractionClause * quantAdjustClause).
			 * MeanBaseRateACAdj=sum(MeanBaseRateACAdj * avgSpeedFractionClause * quantAdjustClause).
			 * MeanBaseRateIMACAdj=sum(MeanBaseRateIMACAdj * avgSpeedFractionClause * quantAdjustClause).
			 * emissionRate=case when avgBinSpeed>0 then sum(MeanBaseRate * avgSpeedFractionClause) / avgBinSpeed else null end.
			 * emissionRateIM=case when avgBinSpeed>0 then sum(MeanBaseRateIM * avgSpeedFractionClause) / avgBinSpeed else null end.
			 * emissionRateACAdj=case when avgBinSpeed>0 then sum(MeanBaseRateACAdj * avgSpeedFractionClause) / avgBinSpeed else null end.
			 * emissionRateIMACAdj=case when avgBinSpeed>0 then sum(MeanBaseRateIMACAdj * avgSpeedFractionClause) / avgBinSpeed else null end.
			 * @output BaseRate
			 * @input SBWeightedDistanceRate
			 * @condition Not Start Exhaust
			 * @condition Retaining average speed bin (Non-Project domain; Rates; Running Exhaust, Brakewear, or Tirewear)
			**/

			/**
			 * @step 110
			 * @algorithm Calculate BaseRate without operating mode, aggregating average speed bins.
			 * opModeFraction=sum(avgSpeedFractionClause * sumSBDClause).
			 * opModeFractionRate=sum(avgSpeedFractionClause * sumSBDClause).
			 * MeanBaseRate=sum(MeanBaseRate * avgSpeedFractionClause * quantAdjustClause).
			 * MeanBaseRateIM=sum(MeanBaseRateIM * avgSpeedFractionClause * quantAdjustClause).
			 * MeanBaseRateACAdj=sum(MeanBaseRateACAdj * avgSpeedFractionClause * quantAdjustClause).
			 * MeanBaseRateIMACAdj=sum(MeanBaseRateIMACAdj * avgSpeedFractionClause * quantAdjustClause).
			 * emissionRate=sum(MeanBaseRate * avgSpeedFractionClause).
			 * emissionRateIM=sum(MeanBaseRateIM * avgSpeedFractionClause).
			 * emissionRateACAdj=sum(MeanBaseRateACAdj * avgSpeedFractionClause).
			 * emissionRateIMACAdj=sum(MeanBaseRateIMACAdj * avgSpeedFractionClause).
			 * @output BaseRateByAge
			 * @input SBWeightedDistanceRate
			 * @condition Not Start Exhaust
			 * @condition Aggregate average speed bins (Project domain or Inventory or Rates for Non-(Running, Brakewear, or Tirewear))
			**/
			"#baserate",
			"insert into baserate_" + processID + "_" + yearID + " ("
			+ " 	sourcetypeid, roadtypeid, avgspeedbinid, polprocessid, hourdayid, modelyearid, fueltypeid, regclassid, opmodeid,"
			+ " 	opmodefraction, opmodefractionrate, meanbaserate, meanbaserateim, meanbaserateacadj, meanbaserateimacadj, emissionrate, emissionrateim, emissionrateacadj, emissionrateimacadj, processid, pollutantid)"
			+ " select"
			+ " 	er.sourcetypeid, rsrt.roadtypeid,"
			+ (!useAvgSpeedBin? " 0 as avgspeedbinid," : " er.avgspeedbinid,")
			+ "		er.polprocessid, rshd.hourdayid,"
			+ " 	er.modelyearid, er.fueltypeid, er.regclassid, 0 as opmodeid,"
			+ "		sum(1" + avgSpeedFraction + sumSBD + "),"
			+ "		sum(1" + avgSpeedFraction + sumSBD + "),"
			+ " 	sum(meanbaserate" + avgSpeedFraction + quantAdjust + "),"
			+ " 	sum(meanbaserateim" + avgSpeedFraction + quantAdjust + "),"
			+ " 	sum(meanbaserateacadj" + avgSpeedFraction + quantAdjust + "),"
			+ " 	sum(meanbaserateimacadj" + avgSpeedFraction + quantAdjust + "),"
			+ (useAvgSpeedBin?
				( " 	case when avgbinspeed>0 then sum(meanbaserate" + avgSpeedFraction + ") / avgbinspeed else null end,"
				+ " 	case when avgbinspeed>0 then sum(meanbaserateim" + avgSpeedFraction + ") / avgbinspeed else null end,")
				:
				( " 	sum(meanbaserate" + avgSpeedFraction + "),"
				+ " 	sum(meanbaserateim" + avgSpeedFraction + "),")
				)
			+ (useAvgSpeedBin?
				( " 	case when avgbinspeed>0 then sum(meanbaserateacadj" + avgSpeedFraction + ") / avgbinspeed else null end,"
				+ " 	case when avgbinspeed>0 then sum(meanbaserateimacadj" + avgSpeedFraction + ") / avgbinspeed else null end,")
				:
				( " 	sum(meanbaserateacadj" + avgSpeedFraction + "),"
				+ " 	sum(meanbaserateimacadj" + avgSpeedFraction + "),")
				)
			+ " 	mod(er.polprocessid,100) as processid, floor(er.polprocessid/100) as pollutantid"
			+ " from"
			+ " 	sbweighteddistancerate er"
			+ " 	inner join runspecroadtype rsrt"
			+ " 	inner join runspechourday rshd"
			+ (applyAvgSpeedDistribution?
			  "		left outer join avgspeeddistribution asd on ("
			+ "			asd.sourcetypeid = er.sourcetypeid"
			+ "			and asd.roadtypeid = rsrt.roadtypeid"
			+ "			and asd.hourdayid = rshd.hourdayid"
			+ "			and asd.avgspeedbinid = er.avgspeedbinid)"
				: ""
			)
			+ (useAvgSpeedBin?
				" inner join avgspeedbin asb on (asb.avgspeedbinid = er.avgspeedbinid)"
				:
				"")
			+ " where rsrt.roadtypeid > 1 and rsrt.roadtypeid < 100"
			+ " and er.sourcetypeid = ##sourcetypeid## and er.polprocessid = ##polprocessid##"
			+ (isProjectDomain && roadTypeID > 0? " and rsrt.roadtypeid=" + roadTypeID : "")
			+ " group by"
			+ "		rsrt.roadtypeid, rshd.hourdayid,"
			+ " 	er.sourcetypeid,"
			+ "		er.polprocessid,"
			+ " 	er.modelyearid, er.fueltypeid, er.regclassid"
			+ (!useAvgSpeedBin? "" : ", er.avgspeedbinid")
		};

		String[] withOpModeStatements = {
			/**
			 * @step 110
			 * @algorithm Calculate BaseRateByAge with operating mode, retaining average speed bin.
			 * opModeFraction=sum(opModeFraction * avgSpeedFractionClause * sumSBDClause * quantAdjustClause).
			 * opModeFractionRate=sum(opModeFraction * avgSpeedFractionClause * sumSBDClause).
			 * MeanBaseRate=sum(MeanBaseRate * opModeFraction * avgSpeedFractionClause * quantAdjustClause).
			 * MeanBaseRateIM=sum(MeanBaseRateIM * opModeFraction * avgSpeedFractionClause * quantAdjustClause).
			 * MeanBaseRateACAdj=sum(MeanBaseRateACAdj * opModeFraction * avgSpeedFractionClause * quantAdjustClause).
			 * MeanBaseRateIMACAdj=sum(MeanBaseRateIMACAdj * opModeFraction * avgSpeedFractionClause * quantAdjustClause).
			 * emissionRate=case when avgBinSpeed>0 then sum(MeanBaseRate * opModeFraction * avgSpeedFractionClause) / avgBinSpeed else null end.
			 * emissionRateIM=case when avgBinSpeed>0 then sum(MeanBaseRateIM * opModeFraction * avgSpeedFractionClause) / avgBinSpeed else null end.
			 * emissionRateACAdj=case when avgBinSpeed>0 then sum(MeanBaseRateACAdj * opModeFraction * avgSpeedFractionClause) / avgBinSpeed else null end.
			 * emissionRateIMACAdj=case when avgBinSpeed>0 then sum(MeanBaseRateIMACAdj * opModeFraction * avgSpeedFractionClause) / avgBinSpeed else null end.
			 * @output BaseRateByAge
			 * @input RatesOpModeDistribution
			 * @input SBWeightedEmissionRateByAge
			 * @condition Start Exhaust
			 * @condition Retaining average speed bin (Non-Project domain; Rates; Running Exhaust, Brakewear, or Tirewear)
			**/

			/**
			 * @step 110
			 * @algorithm Calculate BaseRateByAge with operating mode, aggregating average speed bins.
			 * opModeFraction=sum(opModeFraction * avgSpeedFractionClause * sumSBDClause * quantAdjustClause).
			 * opModeFractionRate=sum(opModeFraction * avgSpeedFractionClause * sumSBDClause).
			 * MeanBaseRate=sum(MeanBaseRate * opModeFraction * avgSpeedFractionClause * quantAdjustClause).
			 * MeanBaseRateIM=sum(MeanBaseRateIM * opModeFraction * avgSpeedFractionClause * quantAdjustClause).
			 * MeanBaseRateACAdj=sum(MeanBaseRateACAdj * opModeFraction * avgSpeedFractionClause * quantAdjustClause).
			 * MeanBaseRateIMACAdj=sum(MeanBaseRateIMACAdj * opModeFraction * avgSpeedFractionClause * quantAdjustClause).
			 * emissionRate=sum(MeanBaseRate * opModeFraction * avgSpeedFractionClause).
			 * emissionRateIM=sum(MeanBaseRateIM * opModeFraction * avgSpeedFractionClause).
			 * emissionRateACAdj=sum(MeanBaseRateACAdj * opModeFraction * avgSpeedFractionClause).
			 * emissionRateIMACAdj=sum(MeanBaseRateIMACAdj * opModeFraction * avgSpeedFractionClause).
			 * @output BaseRateByAge
			 * @input RatesOpModeDistribution
			 * @input SBWeightedEmissionRateByAge
			 * @condition Start Exhaust
			 * @condition Aggregate average speed bins (Project domain or Inventory or Rates for Non-(Running, Brakewear, or Tirewear))
			**/

			"#baseratebyage",
			"insert into baseratebyage_" + processID + "_" + yearID + " ("
			+ " 	sourcetypeid, roadtypeid, avgspeedbinid, hourdayid, polprocessid, modelyearid, fueltypeid, agegroupid, regclassid, opmodeid,"
			+ " 	opmodefraction, opmodefractionrate, meanbaserate, meanbaserateim, meanbaserateacadj, meanbaserateimacadj, emissionrate, emissionrateim, emissionrateacadj, emissionrateimacadj, processid, pollutantid)"
			+ " select"
			+ " 	romd.sourcetypeid, romd.roadtypeid,"
			+ (!useAvgSpeedBin? " 0 as avgspeedbinid," : " romd.avgspeedbinid,")
			+ "		romd.hourdayid, romd.polprocessid,"
			+ " 	er.modelyearid, er.fueltypeid, er.agegroupid, er.regclassid, romd.opmodeid,"
			+ "		sum(opmodefraction" + avgSpeedFraction + sumSBD + quantAdjust + "),"
			+ "		sum(opmodefraction" + avgSpeedFraction + sumSBD + "),"

			+ " 	sum(meanbaserate * opmodefraction" + avgSpeedFraction + quantAdjust + "),"
			+ " 	sum(meanbaserateim * opmodefraction" + avgSpeedFraction + quantAdjust + "),"
			+ " 	sum(meanbaserateacadj * opmodefraction" + avgSpeedFraction + quantAdjust + "),"
			+ " 	sum(meanbaserateimacadj * opmodefraction" + avgSpeedFraction + quantAdjust + "),"
			+ (useAvgSpeedBin?
				( " 	case when avgbinspeed>0 then sum(meanbaserate * opmodefraction" + avgSpeedFraction + ") / avgbinspeed else null end,"
				+ " 	case when avgbinspeed>0 then sum(meanbaserateim * opmodefraction" + avgSpeedFraction + ") / avgbinspeed else null end,")
				:
				( " 	sum(meanbaserate * opmodefraction" + avgSpeedFraction + "),"
				+ " 	sum(meanbaserateim * opmodefraction" + avgSpeedFraction + "),")
				)
			+ (useAvgSpeedBin?
				( " 	case when avgbinspeed>0 then sum(meanbaserateacadj * opmodefraction" + avgSpeedFraction + ") / avgbinspeed else null end,"
				+ " 	case when avgbinspeed>0 then sum(meanbaserateimacadj * opmodefraction" + avgSpeedFraction + ") / avgbinspeed else null end,")
				:
				( " 	sum(meanbaserateacadj * opmodefraction" + avgSpeedFraction + "),"
				+ " 	sum(meanbaserateimacadj * opmodefraction" + avgSpeedFraction + "),")
				)
			+ " 	mod(romd.polprocessid,100) as processid, floor(romd.polprocessid/100) as pollutantid"
			+ " from"
			+ " 	ratesopmodedistribution romd"
			+ " 	inner join Sbweightedemissionratebyage er on ("
			+ " 		er.sourcetypeid = romd.sourcetypeid"
			+ " 		and er.polprocessid = romd.polprocessid"
			+ " 		and er.opmodeid = romd.opmodeid"
			+ " 	)"
			+ " where romd.sourcetypeid = ##sourcetypeid## and romd.polprocessid = ##polprocessid##"
			+ (isProjectDomain && roadTypeID > 0? " and romd.roadtypeid=" + roadTypeID : "")
			+ " group by"
			+ " 	romd.sourcetypeid, romd.polprocessid, romd.roadtypeid, romd.hourdayid, romd.opmodeid,"
			+ (!useAvgSpeedBin? "" : " romd.avgspeedbinid,")
			+ " 	er.modelyearid, er.fueltypeid, er.agegroupid, er.regclassid",

			/**
			 * @step 110
			 * @algorithm Calculate BaseRate with operating mode, retaining average speed bin.
			 * opModeFraction=sum(opModeFraction * avgSpeedFractionClause * sumSBDClause * quantAdjustClause).
			 * opModeFractionRate=sum(opModeFraction * avgSpeedFractionClause * sumSBDClause).
			 * MeanBaseRate=sum(MeanBaseRate * opModeFraction * avgSpeedFractionClause * quantAdjustClause).
			 * MeanBaseRateIM=sum(MeanBaseRateIM * opModeFraction * avgSpeedFractionClause * quantAdjustClause).
			 * MeanBaseRateACAdj=sum(MeanBaseRateACAdj * opModeFraction * avgSpeedFractionClause * quantAdjustClause).
			 * MeanBaseRateIMACAdj=sum(MeanBaseRateIMACAdj * opModeFraction * avgSpeedFractionClause * quantAdjustClause).
			 * emissionRate=case when avgBinSpeed>0 then sum(MeanBaseRate * opModeFraction * avgSpeedFractionClause) / avgBinSpeed else null end.
			 * emissionRateIM=case when avgBinSpeed>0 then sum(MeanBaseRateIM * opModeFraction * avgSpeedFractionClause) / avgBinSpeed else null end.
			 * emissionRateACAdj=case when avgBinSpeed>0 then sum(MeanBaseRateACAdj * opModeFraction * avgSpeedFractionClause) / avgBinSpeed else null end.
			 * emissionRateIMACAdj=case when avgBinSpeed>0 then sum(MeanBaseRateIMACAdj * opModeFraction * avgSpeedFractionClause) / avgBinSpeed else null end.
			 * @output BaseRateByAge
			 * @input RatesOpModeDistribution
			 * @input SBWeightedEmissionRate
			 * @condition Start Exhaust
			 * @condition Retaining average speed bin (Non-Project domain; Rates; Running Exhaust, Brakewear, or Tirewear)
			**/

			/**
			 * @step 110
			 * @algorithm Calculate BaseRate with operating mode, aggregating average speed bins.
			 * opModeFraction=sum(opModeFraction * avgSpeedFractionClause * sumSBDClause * quantAdjustClause).
			 * opModeFractionRate=sum(opModeFraction * avgSpeedFractionClause * sumSBDClause).
			 * MeanBaseRate=sum(MeanBaseRate * opModeFraction * avgSpeedFractionClause * quantAdjustClause).
			 * MeanBaseRateIM=sum(MeanBaseRateIM * opModeFraction * avgSpeedFractionClause * quantAdjustClause).
			 * MeanBaseRateACAdj=sum(MeanBaseRateACAdj * opModeFraction * avgSpeedFractionClause * quantAdjustClause).
			 * MeanBaseRateIMACAdj=sum(MeanBaseRateIMACAdj * opModeFraction * avgSpeedFractionClause * quantAdjustClause).
			 * emissionRate=sum(MeanBaseRate * opModeFraction * avgSpeedFractionClause).
			 * emissionRateIM=sum(MeanBaseRateIM * opModeFraction * avgSpeedFractionClause).
			 * emissionRateACAdj=sum(MeanBaseRateACAdj * opModeFraction * avgSpeedFractionClause).
			 * emissionRateIMACAdj=sum(MeanBaseRateIMACAdj * opModeFraction * avgSpeedFractionClause).
			 * @output BaseRateByAge
			 * @input RatesOpModeDistribution
			 * @input SBWeightedEmissionRate
			 * @condition Start Exhaust
			 * @condition Aggregate average speed bins (Project domain or Inventory or Rates for Non-(Running, Brakewear, or Tirewear))
			**/

			"#baserate",
			"insert into baserate_" + processid + "_" + yearid + " ("
			+ " 	sourcetypeid, roadtypeid, avgspeedbinid, polprocessid, hourdayid, modelyearid, fueltypeid, regclassid, opmodeid,"
			+ " 	opmodefraction, opmodefractionrate, meanbaserate, meanbaserateim, meanbaserateacadj, meanbaserateimacadj, emissionrate, emissionrateim, emissionrateacadj, emissionrateimacadj, processid, pollutantid)"
			+ " select"
			+ " 	romd.sourcetypeid, romd.roadtypeid,"
			+ (!useAvgSpeedBin? " 0 as avgspeedbinid," : " romd.avgspeedbinid,")
			+ "		romd.polprocessid, romd.hourdayid,"
			+ " 	er.modelyearid, er.fueltypeid, er.regclassid, romd.opmodeid,"
			+ "		sum(opmodefraction" + avgSpeedFraction + sumSBD + quantAdjust + "),"
			+ "		sum(opmodefraction" + avgSpeedFraction + sumSBD + "),"

			+ " 	sum(meanbaserate * opmodefraction" + avgSpeedFraction + quantAdjust + "),"
			+ " 	sum(meanbaserateim * opmodefraction" + avgSpeedFraction + quantAdjust + "),"
			+ " 	sum(meanbaserateacadj * opmodefraction" + avgSpeedFraction + quantAdjust + "),"
			+ " 	sum(meanbaserateimacadj * opmodefraction" + avgSpeedFraction + quantAdjust + "),"
			+ (useAvgSpeedBin?
				( " 	case when avgbinspeed>0 then sum(meanbaserate * opmodefraction" + avgSpeedFraction + ") / avgbinspeed else null end,"
				+ " 	case when avgbinspeed>0 then sum(meanbaserateim * opmodefraction" + avgSpeedFraction + ") / avgbinspeed else null end,")
				:
				( " 	sum(MeanBaseRate * opModeFraction" + avgSpeedFraction + "),"
				+ " 	sum(MeanBaseRateIM * opModeFraction" + avgSpeedFraction + "),")
				)
			+ (useAvgSpeedBin?
				( " 	case when avgbinspeed>0 then sum(meanbaserateacadj * opmodefraction" + avgSpeedFraction + ") / avgbinspeed else null end,"
				+ " 	case when avgbinspeed>0 then sum(meanbaserateimacadj * opmodefraction" + avgSpeedFraction + ") / avgbinspeed else null end,")
				:
				( " 	sum(meanbaserateacadj * opmodefraction" + avgSpeedFraction + "),"
				+ " 	sum(meanbaserateimacadj * opmodefraction" + avgSpeedFraction + "),")
				)
			+ " 	mod(romd.polprocessid,100) as processid, floor(romd.polprocessid/100) as pollutantid"
			+ " from"
			+ " 	ratesopmodedistribution romd"
			+ " 	inner join sbweightedemissionrate er on ("
			+ " 		er.sourcetypeid = romd.sourcetypeid"
			+ " 		and er.polprocessid = romd.polprocessid"
			+ " 		and er.opmodeid = romd.opmodeid"
			+ " 	)"
			+ " where romd.sourcetypeid = ##sourcetypeid## and romd.polprocessid = ##polprocessid##"
			+ (isProjectDomain && roadTypeID > 0? " and romd.roadtypeid=" + roadTypeID : "")
			+ " group by"
			+ " 	romd.sourcetypeid, romd.polprocessid, romd.roadtypeid, romd.hourdayid, romd.opmodeid,"
			+ (!useAvgSpeedBin? "" : " romd.avgspeedbinid,")
			+ " 	er.modelyearid, er.fueltypeid, er.regclassid",

			// Add from distanceEmissionRate. Use 300 as the opModeID.

			/**
			 * @step 110
			 * @algorithm Calculate BaseRate with operating mode 300, retaining average speed bin.
			 * opModeFraction=sum(avgSpeedFractionClause * sumSBDClause * quantAdjustClause).
			 * opModeFractionRate=sum(avgSpeedFractionClause * sumSBDClause).
			 * MeanBaseRate=sum(MeanBaseRate * avgSpeedFractionClause * quantAdjustClause).
			 * MeanBaseRateIM=sum(MeanBaseRateIM * avgSpeedFractionClause * quantAdjustClause).
			 * MeanBaseRateACAdj=sum(MeanBaseRateACAdj * avgSpeedFractionClause * quantAdjustClause).
			 * MeanBaseRateIMACAdj=sum(MeanBaseRateIMACAdj * avgSpeedFractionClause * quantAdjustClause).
			 * emissionRate=case when avgBinSpeed>0 then sum(MeanBaseRate * avgSpeedFractionClause) / avgBinSpeed else null end.
			 * emissionRateIM=case when avgBinSpeed>0 then sum(MeanBaseRateIM * avgSpeedFractionClause) / avgBinSpeed else null end.
			 * emissionRateACAdj=case when avgBinSpeed>0 then sum(MeanBaseRateACAdj * avgSpeedFractionClause) / avgBinSpeed else null end.
			 * emissionRateIMACAdj=case when avgBinSpeed>0 then sum(MeanBaseRateIMACAdj * avgSpeedFractionClause) / avgBinSpeed else null end.
			 * @output BaseRateByAge
			 * @input RatesOpModeDistribution
			 * @input SBWeightedDistanceRate
			 * @condition Start Exhaust
			 * @condition Retaining average speed bin (Non-Project domain; Rates; Running Exhaust, Brakewear, or Tirewear)
			**/

			/**
			 * @step 110
			 * @algorithm Calculate BaseRate with operating mode 300, aggregating average speed bins.
			 * opModeFraction=sum(avgSpeedFractionClause * sumSBDClause * quantAdjustClause).
			 * opModeFractionRate=sum(avgSpeedFractionClause * sumSBDClause).
			 * MeanBaseRate=sum(MeanBaseRate * avgSpeedFractionClause * quantAdjustClause).
			 * MeanBaseRateIM=sum(MeanBaseRateIM * avgSpeedFractionClause * quantAdjustClause).
			 * MeanBaseRateACAdj=sum(MeanBaseRateACAdj * avgSpeedFractionClause * quantAdjustClause).
			 * MeanBaseRateIMACAdj=sum(MeanBaseRateIMACAdj * avgSpeedFractionClause * quantAdjustClause).
			 * emissionRate=sum(MeanBaseRate * avgSpeedFractionClause).
			 * emissionRateIM=sum(MeanBaseRateIM * avgSpeedFractionClause).
			 * emissionRateACAdj=sum(MeanBaseRateACAdj * avgSpeedFractionClause).
			 * emissionRateIMACAdj=sum(MeanBaseRateIMACAdj * avgSpeedFractionClause).
			 * @output BaseRateByAge
			 * @input RatesOpModeDistribution
			 * @input SBWeightedDistanceRate
			 * @condition Start Exhaust
			 * @condition Aggregate average speed bins (Project domain or Inventory or Rates for Non-(Running, Brakewear, or Tirewear))
			**/

			"#baserate",
			"insert into baserate_" + processID + "_" + yearID + " ("
			+ " 	sourcetypeid, roadtypeid, avgspeedbinid, polprocessid, hourdayid, modelyearid, fueltypeid, regclassid, opmodeid,"
			+ " 	opmodefraction, opmodefractionrate, meanbaserate, meanbaserateim, meanbaserateacadj, meanbaserateimacadj, emissionrate, emissionrateim, emissionrateacadj, emissionrateimacadj, processid, pollutantid)"
			+ " select"
			+ " 	er.sourcetypeid, rsrt.roadtypeid,"
			+ (!useAvgSpeedBin? " 0 as avgspeedbinid," : " er.avgspeedbinid,")
			+ "		er.polprocessid, rshd.hourdayid,"
			+ " 	er.modelyearid, er.fueltypeid, er.regclassid, 300 as opmodeid,"
			+ "		sum(1" + avgSpeedFraction + sumSBD + "),"
			+ "		sum(1" + avgSpeedFraction + sumSBD + "),"
			+ " 	sum(meanbaserate" + avgSpeedFraction + quantAdjust + "),"
			+ " 	sum(meanbaserateim" + avgSpeedFraction + quantAdjust + "),"
			+ " 	sum(meanbaserateacadj" + avgSpeedFraction + quantAdjust + "),"
			+ " 	sum(meanbaserateimacadj" + avgSpeedFraction + quantAdjust + "),"
			+ (useAvgSpeedBin?
				( " 	case when avgbinspeed>0 then sum(meanbaserate" + avgSpeedFraction + ") / avgbinspeed else null end,"
				+ " 	case when avgbinspeed>0 then sum(meanbaserateim" + avgSpeedFraction + ") / avgbinspeed else null end,")
				:
				( " 	sum(meanbaserate" + avgSpeedFraction + "),"
				+ " 	sum(meanbaserateim" + avgSpeedFraction + "),")
				)
			+ (useAvgSpeedBin?
				( " 	case when avgbinspeed>0 then sum(meanbaserateacadj" + avgSpeedFraction + ") / avgbinspeed else null end,"
				+ " 	case when avgbinspeed>0 then sum(meanbaserateimacadj" + avgSpeedFraction + ") / avgbinspeed else null end,")
				:
				( " 	sum(meanbaserateacadj" + avgSpeedFraction + "),"
				+ " 	sum(meanbaserateimacadj" + avgSpeedFraction + "),")
				)
			+ " 	mod(er.polprocessid,100) as processid, floor(er.polprocessid/100) as pollutantid"
			+ " from"
			+ " 	sbweighteddistancerate er"
			+ " 	inner join runspecroadtype rsrt"
			+ " 	inner join runspechourday rshd"
			+ (applyAvgSpeedDistribution?
			  "		left outer join avgspeeddistribution asd on ("
			+ "			asd.sourcetypeid = er.sourcetypeid"
			+ "			and asd.roadtypeid = rsrt.roadtypeid"
			+ "			and asd.hourdayid = rshd.hourdayid"
			+ "			and asd.avgspeedbinid = er.avgspeedbinid)"
				: ""
			)
			+ (useAvgSpeedBin?
				" inner join avgspeedbin asb on (asb.avgspeedbinid = er.avgspeedbinid)"
				:
				"")
			+ " where rsrt.roadtypeid > 1 and rsrt.roadtypeid < 100"
			+ " and er.sourcetypeid = ##sourcetypeid## and er.polprocessid = ##polprocessid##"
			+ (isProjectDomain && roadTypeID > 0? " and rsrt.roadtypeid=" + roadTypeID : "")
			+ " group by"
			+ "		rsrt.roadtypeid, rshd.hourdayid,"
			+ " 	er.sourcetypeid,"
			+ "		er.polprocessid,"
			+ " 	er.modelyearid, er.fueltypeid, er.regclassid"
			+ (!useAvgSpeedBin? "" : ", er.avgspeedbinid")
		};

		String[] statements = keepOpModeID? withOpModeStatements : noOpModeStatements;
		String sql = "";
		boolean useExternalGenerator = USE_EXTERNAL_GENERATOR && RatesOperatingModeDistributionGenerator.USE_EXTERNAL_GENERATOR
				&& (processID == 1 || processID == 9 || processID == 10);
		SQLRunner.Query query = new SQLRunner.Query();
		TaggedSQLRunner concurrentSQL = null;
		try {
			concurrentSQL = getSQLRunner();
			String context = "#CORE";

			if(isProjectDomain && previousRoadTypeID > 0) {
				context = "#CORE";
				String[] cleanupStatements = {
					"#baseratebyage",
					"delete from baseratebyage_" + processID + "_" + yearID + " where roadtypeid=" + previousRoadTypeID,
					"#baserate",
					"delete from baserate_" + processID + "_" + yearID + " where roadtypeid=" + previousRoadTypeID
				};
				concurrentSQL.clear();
				for(int i=0;i<cleanupStatements.length;i++) {
					sql = cleanupStatements[i];
					if(sql != null && sql.length() > 0) {
						if(sql.startsWith("#")) {
							context = sql;
							continue;
						}
						//Logger.log(LogMessageCategory.DEBUG,sql);
						if(context.length() <= 0 || context.equalsIgnoreCase("#CORE")) {
							//Logger.log(LogMessageCategory.INFO,"Running #CORE: " + StringUtilities.substring(sql,0,100));
							SQLRunner.executeSQL(db,sql);
						} else {
							if(concurrentSQL == null) {
								Logger.log(LogMessageCategory.INFO,"Directly Running: " + context + ": " + StringUtilities.substring(sql,0,100));
								SQLRunner.executeSQL(db,sql);
							} else {
								concurrentSQL.add(context,sql);
							}
						}
					}
				}
				concurrentSQL.execute();
				concurrentSQL.clear();
				concurrentSQL.close();
				previousRoadTypeID = 0;
			}
			if(applyAvgSpeedDistribution && !useExternalGenerator) {
				sql = "update ratesopmodedistribution, avgspeeddistribution"
						+ " 	set ratesopmodedistribution.avgspeedfraction = avgspeeddistribution.avgspeedfraction"
						+ " where ratesopmodedistribution.sourcetypeid = avgspeeddistribution.sourcetypeid"
						+ " and ratesopmodedistribution.roadtypeid = avgspeeddistribution.roadtypeid"
						+ " and ratesopmodedistribution.hourdayid = avgspeeddistribution.hourdayid"
						+ " and ratesopmodedistribution.avgspeedbinid = avgspeeddistribution.avgspeedbinid"
						+ " and ratesopmodedistribution.avgspeedfraction <= 0";
				long startMillis = System.currentTimeMillis();
				SQLRunner.executeSQL(db,sql);
				long endMillis = System.currentTimeMillis();
				Logger.log(LogMessageCategory.INFO,"BRG update ROMD,ASD: " + (endMillis-startMillis) + " ms");
			}
			if(useExternalGenerator) {
				String externalGeneratorParameters = brbaFlags.getCSVForExternalGenerator()
						+ "," + processID
						+ "," + yearID
						+ "," + roadTypeID;
				if(runLocalExternalGenerator(loopContext,"BaseRateGenerator.generateBaseRates",externalGeneratorParameters,null,null)) {
					Logger.log(LogMessageCategory.DEBUG,"Success running the external generator in BaseRateGenerator.generateBaseRates");
				} else {
					Logger.log(LogMessageCategory.ERROR,"Unable to run external generator in BaseRateGenerator.generateBaseRates");
				}
			} else {
				// Get iteration dimensions, keeping only those that belong to the desired process.
				ArrayList<String> tuples = new ArrayList<String>();
				sql = "select distinct sourcetypeid, polprocessid from sbweightedemissionratebyage"
						+ " union"
						+ " select distinct sourcetypeid, polprocessid from sbweightedemissionrate"
						+ " union"
						+ " select distinct sourcetypeid, polprocessid from sbweighteddistancerate";
				query.open(db,sql);
				while(query.rs.next()) {
					String sourceTypeID = query.rs.getString(1);
					int polProcessID = query.rs.getInt(2);
					if(polProcessID % 100 == processID) {
						tuples.add(sourceTypeID);
						tuples.add("" + polProcessID);
					}
				}
				query.close();
				Logger.log(LogMessageCategory.DEBUG,"BaseRateGenerator processID=" + processID + ", roadTypeID=" + roadTypeID + ", yearID=" + yearID + " has " + (tuples.size()/2) + " iterations");
	
				concurrentSQL.clear();
				TreeMapIgnoreCase replacements = new TreeMapIgnoreCase();
				context = "#CORE";
				for(int ti=0;ti<tuples.size();ti+=2) {
					String sourceTypeID = tuples.get(ti+0);
					String polProcessID = tuples.get(ti+1);
					//Logger.log(LogMessageCategory.DEBUG,"BaseRateGenerator for sourceTypeID=" + sourceTypeID + ", polProcessID=" + polProcessID);
					replacements.clear();
					replacements.put("##sourcetypeid##",sourceTypeID);
					replacements.put("##polprocessid##",polProcessID);
					for(int i=0;i<statements.length;i++) {
						sql = statements[i];
						if(sql != null && sql.length() > 0) {
							sql = StringUtilities.doReplacements(sql,replacements);
							//Logger.log(LogMessageCategory.INFO,sql);
	
							if(sql.startsWith("#")) {
								if(context.length() <= 0 || context.equalsIgnoreCase("#CORE")) {
									if(concurrentSQL != null) {
										concurrentSQL.execute();
										concurrentSQL.clear();
										concurrentSQL.close();
									}
								}
								context = sql;
								continue;
							}
							if(concurrentSQL == null) {
								SQLRunner.executeSQL(db,sql);
							} else {
								BaseRateByAgeHelper.Context brbaContext = new BaseRateByAgeHelper.Context();
								brbaContext.polProcessID = Integer.parseInt(polProcessID);
								brbaContext.processID = processID;
								brbaContext.roadTypeID = isProjectDomain && roadTypeID > 0? roadTypeID : 0;
								brbaContext.sourceTypeID = Integer.parseInt(sourceTypeID);
								brbaContext.yearID = yearID;
								concurrentSQL.add(context,sql,brbaContext,brbaFlags);
							}
						}
					}
				}
				Logger.log(LogMessageCategory.DEBUG,"BaseRateGenerator executing concurrent SQL...");
				concurrentSQL.execute();
				Logger.log(LogMessageCategory.DEBUG,"BaseRateGenerator concurrent SQL done.");
			}
		} catch(Exception e) {
			Logger.logSqlError(e,"Could not generate base rates",sql);
		} finally {
			query.onFinally();
			if(concurrentSQL != null) {
				concurrentSQL.stats.print();
				concurrentSQL.onFinally();
			}
			Logger.log(LogMessageCategory.DEBUG,"Done with BaseRateGenerator processID=" + processID + ", roadTypeID=" + roadTypeID + ", yearID=" + yearID);
		}
		previousRoadTypeID = roadTypeID;
	}

	/**
	 * Combine metalEmissionRate and dioxinEmissionRate tables into a single format
	 * useful to both rate/SHO and rate/mile calculations. Do this after any GFRE adjustments
	 * to either table. The output table is distanceEmissionRate.
	**/
	void makeDistanceRates() {
		String normalize = "";

		if(ExecutionRunSpec.theExecutionRunSpec.getModelScale() == ModelScale.MESOSCALE_LOOKUP) {
			Logger.log(LogMessageCategory.DEBUG,"Normalizing sourcebin-weighted distance rates");
			normalize = "/ SUM(sbd.sourcebinactivityfraction)";
		} else {
			Logger.log(LogMessageCategory.DEBUG,"Not normalizing sourcebin-weighted distance rates");
		}

		String[] statements = {
			"truncate table distanceEmissionRate",

			/**
			 * @step 015
			 * @algorithm Make distance emission rates for metals.
			 * ratePerMile=meanBaseRate * (1.0 when units of g/mile, 1.609344 when g/km, 1.0 when TEQ/mile, 1.609344 when TEQ/km).
			 * ratePerSHO=meanBaseRate * avgBinSpeed * (1.0 when units of g/mile, 1.609344 when g/km, 1.0 when TEQ/mile, 1.609344 when TEQ/km).
			 * @output distanceEmissionRate
			 * @input metalEmissionRate
			 * @input averageSpeedBin
			**/

			// metalEmissionRate
			"insert into distanceemissionrate ("
			+ " 	polprocessid,"
			+ " 	fueltypeid, sourcetypeid,"
			+ " 	modelyearid,"
			+ " 	avgspeedbinid,"
			+ " 	ratepermile, ratepersho)"
			+ " select r.polprocessid,"
			+ " 	rssf.fueltypeid, rssf.sourcetypeid, "
			+ " 	rsmy.modelyearid,"
			+ " 	asb.avgspeedbinid,"
			+ " 	(case units when 'g/mile' then 1.0"
			+ " 		when 'g/km' then 1.609344"
			+ " 		when 'TEQ/mile' then 1.0"
			+ " 		when 'TEQ/km' then 1.609344"
			+ " 		else 1.0"
			+ " 	end)*(r.meanbaserate) as ratepermile,"
			+ " 	(case units when 'g/mile' then 1.0"
			+ " 		when 'g/km' then 1.609344"
			+ " 		when 'TEQ/mile' then 1.0"
			+ " 		when 'TEQ/km' then 1.609344"
			+ " 		else 1.0"
			+ " 	end)*(r.meanbaserate * asb.avgbinspeed) as ratepersho"
			+ " from metalemissionrate r"
			+ " inner join runspecsourcefueltype rssf on ("
			+ " 	rssf.sourcetypeid = r.sourcetypeid"
			+ " 	and rssf.fueltypeid = r.fueltypeid)"
			+ " inner join runspecmodelyear rsmy on ("
			+ " 	rsmy.modelyearid >= floor(r.modelyeargroupid/10000)"
			+ " 	and rsmy.modelyearid <= mod(r.modelyeargroupid,10000))"
			+ " inner join avgspeedbin asb",

			/**
			 * @step 015
			 * @algorithm Make distance emission rates for dioxins.
			 * ratePerMile=meanBaseRate * (1.0 when units of g/mile, 1.609344 when g/km, 1.0 when TEQ/mile, 1.609344 when TEQ/km).
			 * ratePerSHO=meanBaseRate * avgBinSpeed * (1.0 when units of g/mile, 1.609344 when g/km, 1.0 when TEQ/mile, 1.609344 when TEQ/km).
			 * @output distanceEmissionRate
			 * @input dioxinEmissionRate
			 * @input averageSpeedBin
			**/
			
			// dioxinEmissionRate
			"insert into distanceemissionrate ("
			+ " 	polprocessid,"
			+ " 	fueltypeid, sourcetypeid,"
			+ " 	modelyearid,"
			+ " 	avgspeedbinid,"
			+ " 	ratepermile, ratepersho)"
			+ " select r.polprocessid,"
			+ " 	rssf.fueltypeid, rssf.sourcetypeid,"
			+ " 	rsmy.modelyearid,"
			+ " 	asb.avgspeedbinid,"
			+ " 	(case units when 'g/mile' then 1.0"
			+ " 		when 'g/km' then 1.609344"
			+ " 		when 'TEQ/mile' then 1.0"
			+ " 		when 'TEQ/km' then 1.609344"
			+ " 		else 1.0"
			+ " 	end)*(r.meanbaserate) as ratepermile,"
			+ " 	(case units when 'g/mile' then 1.0"
			+ " 		when 'g/km' then 1.609344"
			+ " 		when 'TEQ/mile' then 1.0"
			+ " 		when 'TEQ/km' then 1.609344"
			+ " 		else 1.0"
			+ " 	end)*(r.meanbaserate * asb.avgbinspeed) as ratepersho"
			+ " from dioxinemissionrate r"
			+ " inner join runspecsourcefueltype rssf on ("
			+ " 	rssf.fueltypeid = r.fueltypeid)"
			+ " inner join runspecmodelyear rsmy on ("
			+ " 	rsmy.modelyearid >= floor(r.modelyeargroupid/10000)"
			+ " 	and rsmy.modelyearid <= mod(r.modelyeargroupid,10000))"
			+ " inner join avgspeedbin asb"
		};

		String sql = "";
		try {
			for(int i=0;i<statements.length;i++) {
				sql = statements[i];
				if(sql != null && sql.length() > 0) {
					//Logger.log(LogMessageCategory.DEBUG,sql);
					SQLRunner.executeSQL(db,sql);
				}
			}
		} catch (SQLException e) {
			Logger.logSqlError(e,"Could not generate distance rates",sql);
		}
	}

	/**
	 * Obtain a TaggedSQLRunner that is connected to the execution database.
	 * @returns a TaggedSQLRunner, never null
	**/	
	TaggedSQLRunner getSQLRunner() {
		return new TaggedSQLRunner(new TaggedSQLRunner.ConnectionProvider() {
			public Connection checkOutConnection() {
				try {
					return DatabaseConnectionManager.checkOutConnection(MOVESDatabaseType.EXECUTION);
				} catch(Exception e) {
					return null;
				}
			}
			
			public void checkInConnection(Connection c) {
				try {
					DatabaseConnectionManager.checkInConnection(MOVESDatabaseType.EXECUTION,c);
				} catch(Exception e) {
					// Nothing to do here
				}
			}
		},
		new TaggedSQLRunner.OverrideHandler() {
			public boolean onOverrideSQL(Connection db,String sql,Object data1,Object data2) throws Exception {
				if(!useBaseRateByAgeHelper
						|| !sql.startsWith("insert into baseratebyage_")
						|| !(data1 instanceof BaseRateByAgeHelper.Context)
						|| !(data2 instanceof BaseRateByAgeHelper.Flags)) {
					return false;
				}
				Logger.log(LogMessageCategory.DEBUG,"BaseRateGenerator executing BaseRateByAgeHelper...");
				BaseRateByAgeHelper helper = new BaseRateByAgeHelper(db);
				helper.process((BaseRateByAgeHelper.Context)data1,(BaseRateByAgeHelper.Flags)data2);
				Logger.log(LogMessageCategory.DEBUG,"BaseRateGenerator executing BaseRateByAgeHelper Done");
				return true;
			}
		});
	}
}
