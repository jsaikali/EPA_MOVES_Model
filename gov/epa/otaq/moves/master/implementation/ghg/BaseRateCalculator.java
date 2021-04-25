/**************************************************************************************************
 * @(#)BaseRateCalculator.java
 *
 *
 *
 *************************************************************************************************/
package gov.epa.otaq.moves.master.implementation.ghg;

import gov.epa.otaq.moves.common.*;
import gov.epa.otaq.moves.master.framework.*;

import java.util.*;
import java.sql.*;

/**
 * Calculate rates using the rates-first methodology.
 *
 * @author		EPA Ed Glover
 * @author		Wesley Faler
 * @version		2017-09-19
**/
public class BaseRateCalculator extends EmissionCalculator
		implements MasterLoopContext.IContextFilter
			, EmissionCalculatorExternal
				{
	/** @calculator **/

	/** Pollutants generated by the processes **/
	static final int[] pollutantIDs = { 1, 2, 3, 6, 30, 91, 118, 112, 116, 117 };
	/** Pollutants generated via integrated chaining **/
	static final int[] internalChainedPollutantIDs = {  };
	/** processes **/
	static final int[] processIDs = { 1, 2, 9, 10, 90, 91 };
	/** polProcessIDs that are based upon distance and handled by this calculator **/
	static final int[] distancePolProcessIDs = {
		6001,6101,6201,6301,6401,6501,6601,6701,
		13001,13101,13201,13301,13401,13501,13601,13701,
		13801,13901,14001,14101,14201,14301,14401,14501,
		14601
	};

	/** The process objects used by the run, null if not used **/
	EmissionProcess[] processes = new EmissionProcess[processIDs.length];
	/** Process-Zone-RoadType-Year-Month[-Link] combinations that have been processed **/
	TreeSet<String> processedKeys = new TreeSet<String>();
	/** Loop that owns this calculator **/
	MasterLoop owningLoop = null;
	/** true when the owningLoop is only counting bundles **/
	boolean isCountingBundles = true;
	/** true when doing the first bundle **/
	boolean isFirstBundle = true;

	/**
	 * Constructor, including registration of potential processes and pollutants
	 * handled by this calculator.  Such registration facilitates calculator
	 * chaining.
	**/
	public BaseRateCalculator() {
		// Register
		for(int i=0;i<pollutantIDs.length;i++) {
			Pollutant pollutant = Pollutant.findByID(pollutantIDs[i]);
			if(pollutant != null) {
				for(int j=0;j<processIDs.length;j++) {
					EmissionProcess process = EmissionProcess.findByID(processIDs[j]);
					if(process != null) {
						EmissionCalculatorRegistration.register(pollutant,process,this);
					}
				}
			}
		}
		// Register distance-based pollutants
		for(int i=0;i<distancePolProcessIDs.length;i++) {
			Pollutant pollutant = Pollutant.findByID((int)(distancePolProcessIDs[i]/100));
			EmissionProcess process = EmissionProcess.findByID(distancePolProcessIDs[i] % 100);
			if(pollutant != null && process != null) {
				EmissionCalculatorRegistration.register(pollutant,process,this);
			}
		}
		// Register internally chained pollutants
		for(int i=0;i<internalChainedPollutantIDs.length;i++) {
			Pollutant pollutant = Pollutant.findByID(internalChainedPollutantIDs[i]);
			if(pollutant != null) {
				for(int j=0;j<processIDs.length;j++) {
					EmissionProcess process = EmissionProcess.findByID(processIDs[j]);
					if(process != null) {
						EmissionCalculatorRegistration.register(pollutant,process,this);
					}
				}
			}
		}
	}

	/**
	 * MasterLoopable override that performs loop registration.
	 * @param targetLoop The loop to subscribe to.
	**/
	public void subscribeToMe(MasterLoop targetLoop) {
		owningLoop = targetLoop;
		isCountingBundles = true;

		boolean didRegister = false;
		for(int j=0;j<processIDs.length;j++) {
			EmissionProcess process = EmissionProcess.findByID(processIDs[j]);
			if(process != null) {
				processes[j] = process;
				if(ExecutionRunSpec.theExecutionRunSpec.doesHavePollutantAndProcess(
						null,process.processName)) {
					targetLoop.subscribe(this, process, MasterLoopGranularity.MONTH,
							MasterLoopPriority.EMISSION_CALCULATOR);
					didRegister = true;
				}
			}
		}
	}

	/**
	 * Examine a context for suitability.  Used to override the natural execution hierarchy.
	 * @param context Context to be examined
	 * @return true if the context should be used by a MasterLoopable.
	**/
	public boolean doesProcessContext(MasterLoopContext context) {
		if(owningLoop.isCountingBundles() != isCountingBundles) {
			isCountingBundles = owningLoop.isCountingBundles();
			processedKeys.clear();
		}
		if(context.iterLocation == null || context.iterLocation.zoneRecordID < 0 || context.iterLocation.roadTypeRecordID < 0
				|| context.year < 0 || context.monthID < 0) {
			return false;
		}

		if(context.iterLocation != null && context.iterLocation.roadTypeRecordID > 0) {
			boolean isOffNetwork = context.iterLocation.roadTypeRecordID == 1;
			// Running Exhaust should not occur off network for Project mode. It does occur due to ONI for other modes.
			if(isOffNetwork && context.iterProcess.databaseKey == 1
					&& ExecutionRunSpec.theExecutionRunSpec.getModelDomain() == ModelDomain.PROJECT) {
				return false;
			}
			// Breakwear should not occur off network.
			// Tirewear should not occur off network.
			if((context.iterProcess.databaseKey == 9 || context.iterProcess.databaseKey == 10)
					&& isOffNetwork) {
				return false;
			}
			// Several processes only occur off network.
			if(!isOffNetwork &&
					(context.iterProcess.databaseKey == 2
					|| context.iterProcess.databaseKey == 90
					|| context.iterProcess.databaseKey == 91)) {
				return false;
			}
		}

		String key = "" + context.iterProcess.databaseKey
				+ "|" + context.iterLocation.zoneRecordID + "|" + context.iterLocation.roadTypeRecordID
				+ "|" + context.year + "|" + context.monthID;
		if(context.iterProcess.databaseKey == 1 || context.iterProcess.databaseKey == 9 || context.iterProcess.databaseKey == 10) {
			key += "|" + context.iterLocation.linkRecordID;
		}
		if(processedKeys.contains(key)) {
			// doExecute was already called for the process-zone-road-year-month[-link] combination and
			// should not be run again.
			return false;
		}
		processedKeys.add(key);
		//System.out.println("Authorizing BaseRateCalculator for key: " + key + ", " + context.toBundleManifestContextForHumans());
		return true;
	}

	/**
	 * Builds SQL statements for a distributed worker to execute. This is called by
	 * EmissionCalculator.executeLoop. Implementations of this method
	 * should contain uncertainty logic when UncertaintyParameters specifies that
	 * this mode is enabled.
	 * @param context The MasterLoopContext that applies to this execution.
	 * @return The resulting sql lists as an SQLForWorker object.
	**/
	public SQLForWorker doExecute(MasterLoopContext context) {
		// Determine which pollutant(s) should be calculated
		String pollutantIDsText = "";
		String pollutantProcessIDs = "";
		boolean foundPollutant = false;
		TreeSetIgnoreCase enabledSectionNames = new TreeSetIgnoreCase();
		TreeMapIgnoreCase replacements = new TreeMapIgnoreCase();
		String sbdPolProcessID = "";

		Connection executionDatabase = null;
		try {
			executionDatabase = DatabaseConnectionManager.checkOutConnection(MOVESDatabaseType.EXECUTION);
		} catch(Exception e) {
			Logger.logError(e,"Unable to get the Execution Database connection needed for running Base Rate Calculations.");
			return null;
		}

		// Check top-level pollutants
		for(int i=0;i<pollutantIDs.length;i++) {
			Pollutant pollutant = Pollutant.findByID(pollutantIDs[i]);
			if(pollutant == null) {
				continue;
			}
			PollutantProcessAssociation a = PollutantProcessAssociation.findByName(
					pollutant.pollutantName,context.iterProcess.processName);
			if(a != null) {
				if(pollutantProcessIDs.length() > 0) {
					pollutantProcessIDs += ",";
				}
				int polProcessID = a.getDatabaseKey(executionDatabase);
				pollutantProcessIDs += polProcessID;
				foundPollutant = true;
				if(pollutantIDsText.length() > 0) {
					pollutantIDsText += ",";
				}
				pollutantIDsText += "" + pollutantIDs[i];
			}
		}
		// Check distance-based pollutants
		for(int i=0;i<distancePolProcessIDs.length;i++) {
			if((distancePolProcessIDs[i] % 100) != context.iterProcess.databaseKey) {
				continue;
			}
			Pollutant pollutant = Pollutant.findByID((int)(distancePolProcessIDs[i]/100));
			if(pollutant == null) {
				continue;
			}
			PollutantProcessAssociation a = PollutantProcessAssociation.findByName(
					pollutant.pollutantName,context.iterProcess.processName);
			if(a != null) {
				if(pollutantProcessIDs.length() > 0) {
					pollutantProcessIDs += ",";
				}
				int polProcessID = a.getDatabaseKey(executionDatabase);
				pollutantProcessIDs += polProcessID;
				foundPollutant = true;
				if(pollutantIDsText.length() > 0) {
					pollutantIDsText += ",";
				}
				pollutantIDsText += "" + ((int)(distancePolProcessIDs[i]/100));
			}
		}
		// Check internally chained pollutants
		for(int i=0;i<internalChainedPollutantIDs.length;i++) {
			Pollutant pollutant = Pollutant.findByID(internalChainedPollutantIDs[i]);
			if(pollutant == null) {
				continue;
			}
			if(ExecutionRunSpec.theExecutionRunSpec.doesHavePollutantAndProcess(
					pollutant.pollutantName,context.iterProcess.processName)) {
				enabledSectionNames.add("Chain" + internalChainedPollutantIDs[i]);
			}
		}

		// Get the first Pollutant/Process for the context process that is in the
		// SourceBinDistribution and runspec.
		String sql = "SELECT sbd.polprocessid from sourcebindistribution sbd"
				+ " inner join pollutantprocessassoc ppa on ppa.polprocessid = sbd.polprocessid"
				+ " inner join runspecpollutantprocess rpp on rpp.polprocessid = sbd.polprocessid"
				+ " where ppa.processid = " + context.iterProcess.databaseKey + " LIMIT 1";
		SQLRunner.Query query = new SQLRunner.Query();
		try {
			query.open(executionDatabase,sql);
			if(query.rs.next()) {
				sbdPolProcessID = query.rs.getString(1);
			}
			if(91 == context.iterProcess.databaseKey) {
				int hotellingActivityZoneID = TotalActivityGenerator.findHotellingActivityDistributionZoneIDToUse(executionDatabase,context.iterLocation.stateRecordID,context.iterLocation.zoneRecordID);
				replacements.put("##hotellingActivityZoneID##",""+hotellingActivityZoneID);
			}
		} catch(Exception e) {
			Logger.logError(e,"Unable to get a Pollutant/Process needed for SBD aggregation.");
		} finally {
			query.onFinally();
		}
		replacements.put("##sbdPolProcessID##",sbdPolProcessID);

		if(executionDatabase != null) {
			DatabaseConnectionManager.checkInConnection(MOVESDatabaseType.EXECUTION, executionDatabase);
			executionDatabase = null;
		}

		if(!foundPollutant) {
			return null;
		}

		SQLForWorker sqlForWorker = new SQLForWorker();

		replacements.put("##pollutantIDs##",pollutantIDsText);
		replacements.put("##pollutantProcessIDs##",pollutantProcessIDs);

		OutputEmissionsBreakdownSelection outputEmissionsBreakdownSelection =
			ExecutionRunSpec.theExecutionRunSpec.getOutputEmissionsBreakdownSelection();
		if(outputEmissionsBreakdownSelection.onRoadSCC) {
			enabledSectionNames.add("SCCOutput");
		} else {
			enabledSectionNames.add("NoSCCOutput");
		}

		if(ExecutionRunSpec.theExecutionRunSpec.getModelDomain() == ModelDomain.PROJECT) {
			enabledSectionNames.add("Project");
		} else {
			enabledSectionNames.add("NotProject");
		}

		// When in rates mode and not including sourceTypeID, modelYearID, fuelTypeID, and/or regClassID
		// in the output, records need to be weighted together.
		// Don't do this in Inventory mode. Inventory mode uses sourceBinActivityFraction
		// which is already weighted.
		boolean isStartsOrExtIdleOrAPU = (context.iterProcess.databaseKey == 2 || context.iterProcess.databaseKey == 90 || context.iterProcess.databaseKey == 91);
		if(ExecutionRunSpec.theExecutionRunSpec.getModelScale() == ModelScale.MESOSCALE_LOOKUP
				&& (!ExecutionRunSpec.theExecutionRunSpec.getRunSpec().outputEmissionsBreakdownSelection.sourceUseType
					|| !ExecutionRunSpec.theExecutionRunSpec.getRunSpec().outputEmissionsBreakdownSelection.modelYear
					|| !ExecutionRunSpec.theExecutionRunSpec.getRunSpec().outputEmissionsBreakdownSelection.fuelType
					|| !ExecutionRunSpec.theExecutionRunSpec.getRunSpec().outputEmissionsBreakdownSelection.regClassID)) {
			enabledSectionNames.add("GetActivity");
			enabledSectionNames.add("AggregateSMFR");

			String activityTotalSelect = "";
			String activityTotalGroup = "";
			String activityWeightJoin = "";
			// So as to match the SQL statements, the following sections must be in this order:
			// sourceTypeID, modelYearID, fuelTypeID, regClassID
			if(ExecutionRunSpec.theExecutionRunSpec.getRunSpec().outputEmissionsBreakdownSelection.sourceUseType) {
				activityTotalSelect += ",u.sourcetypeid";
				activityTotalGroup += ",u.sourcetypeid";
				activityWeightJoin += ",sourcetypeid";
			} else {
				activityTotalSelect += ",0 as sourcetypeid";
				enabledSectionNames.add("DiscardSourceTypeID");
			}
			if(ExecutionRunSpec.theExecutionRunSpec.getRunSpec().outputEmissionsBreakdownSelection.modelYear) {
				activityTotalSelect += ",u.modelyearid";
				activityTotalGroup += ",u.modelyearid";
				activityWeightJoin += ",modelyearid";
			} else {
				activityTotalSelect += ",0 as modelyearid";
				enabledSectionNames.add("DiscardModelYearID");
			}
			if(ExecutionRunSpec.theExecutionRunSpec.getRunSpec().outputEmissionsBreakdownSelection.fuelType) {
				activityTotalSelect += ",u.fueltypeid";
				activityTotalGroup += ",u.fueltypeid";
				activityWeightJoin += ",fueltypeid";
			} else {
				activityTotalSelect += ",0 as fueltypeid";
				enabledSectionNames.add("DiscardFuelTypeID");
			}
			if(ExecutionRunSpec.theExecutionRunSpec.getRunSpec().outputEmissionsBreakdownSelection.regClassID) {
				activityTotalSelect += ",u.regclassid";
				activityTotalGroup += ",u.regclassid";
				activityWeightJoin += ",regclassid";
			} else {
				activityTotalSelect += ",0 as regclassid";
				enabledSectionNames.add("DiscardRegClassID");
			}

			replacements.put("##activityTotalSelect##",activityTotalSelect);
			replacements.put("##activityTotalGroup##",activityTotalGroup);
			replacements.put("##activityWeightJoin##",activityWeightJoin);

			if(context.iterProcess.databaseKey == 2
					|| context.iterProcess.databaseKey == 90
					|| context.iterProcess.databaseKey == 91) {
				enabledSectionNames.add("AdjustEmissionRateOnly");
			} else {
				enabledSectionNames.add("AdjustMeanBaseRateAndEmissionRate");
			}
		}

		if(context.iterProcess.databaseKey != 1) {
			enabledSectionNames.add("NotProcess1");
		}
		if(context.iterProcess.databaseKey != 2) {
			enabledSectionNames.add("NotProcess2");
		}
		if(context.iterProcess.databaseKey == 1 || context.iterProcess.databaseKey == 2) {
			enabledSectionNames.add("Process1_2");
		}
		if(context.iterProcess.databaseKey == 1 || context.iterProcess.databaseKey == 9 || context.iterProcess.databaseKey == 10) {
			enabledSectionNames.add("Process1_9_10");
		} else {
			enabledSectionNames.add("NotProcess1_9_10");
		}
		if(isFirstBundle) {
			enabledSectionNames.add("FirstBundle");
		}

		if(chainedCalculators != null && chainedCalculators.size() > 0) {
			enabledSectionNames.add("HasChainedCalculators");
		} else {
			enabledSectionNames.add("HasNoChainedCalculators");
		}

		// EM- we no longer only want the operating mode adjustment to happen in inventory once we add ONI to the model.
		//if(ExecutionRunSpec.theExecutionRunSpec.getModelScale() != ModelScale.MESOSCALE_LOOKUP) {
			// For inventory, APU rates need to be multiplied by zone-specific operating mode fraction
			// spent using a diesel APU (opModeID 201).
			if(91 == context.iterProcess.databaseKey) {
				enabledSectionNames.add("AdjustAPUEmissionRate");
			}
		//}
		if(ExecutionRunSpec.theExecutionRunSpec.getModelScale() == ModelScale.MESOSCALE_LOOKUP) {
			if(2 == context.iterProcess.databaseKey
					|| 90 == context.iterProcess.databaseKey
					|| 91 == context.iterProcess.databaseKey
					// EM - we also need to apply activity for ONI and rates, which is roadTypeID 1 and processID 1
					|| (1 == context.iterProcess.databaseKey && 1 == context.iterLocation.roadTypeRecordID)) {
				enabledSectionNames.add("ApplyActivity");
				enabledSectionNames.add("GetActivity");
			}
			/* OLD as of T1702
			if(91 == context.iterProcess.databaseKey) {
				enabledSectionNames.add("AdjustAPUEmissionRate");
			}
			*/
		} else if(ExecutionRunSpec.theExecutionRunSpec.getModelScale() == ModelScale.MACROSCALE) {
			enabledSectionNames.add("ApplyActivity");
			enabledSectionNames.add("GetActivity");
		}

		if(CompilationFlags.USE_EMISSIONRATEADJUSTMENT_FACTOR) {
			enabledSectionNames.add("EmissionRateAdjustment");
		}

		// Pass section names as flags to the external calculator
		for(String s : enabledSectionNames) {
			sqlForWorker.externalModules.add("BRC_" + s);
		}
		sqlForWorker.externalModules.add("BRC_Process" + context.iterProcess.databaseKey);
		if(ExecutionRunSpec.theExecutionRunSpec != null) {
			if(ExecutionRunSpec.theExecutionRunSpec.getModelScale() == ModelScale.MESOSCALE_LOOKUP) {
				sqlForWorker.externalModules.add("BRC_Rates");
			} else if(ExecutionRunSpec.theExecutionRunSpec.getModelScale() == ModelScale.MACROSCALE) {
				sqlForWorker.externalModules.add("BRC_Inventory");
			}
		}

		//Logger.log(LogMessageCategory.INFO,"BaseRateCalculator: " + context.toBundleManifestContextForHumans());
		boolean isOK = readAndHandleScriptedCalculations(context,replacements,
				"database/BaseRateCalculator.sql",enabledSectionNames,
				sqlForWorker);

		isFirstBundle = false;

		if(isOK) {
			return sqlForWorker;
		} else {
			return null;
		}
	}
}
