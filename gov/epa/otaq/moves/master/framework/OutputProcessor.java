/**************************************************************************************************
 * @(#)OutputProcessor.java
 *
 *
 *
 *************************************************************************************************/
package gov.epa.otaq.moves.master.framework;

import gov.epa.otaq.moves.common.*;
import gov.epa.otaq.moves.master.runspec.RunSpec;
import java.io.File;
import java.sql.Connection;
import java.util.*;
import java.sql.*;
import java.io.*;

import org.apache.commons.lang.math.NumberUtils;

/**
 * This Singleton updates and appends output database with results from the distributed
 * workers. Called by the unbundler.
 *
 * @author		Wesley Faler
 * @version		2016-08-30
**/
public class OutputProcessor {
	// true to retain bundles from workers
	static final boolean keepDebugData = false;

	/** File containing the table data that was output by a worker **/
	static final String OUTPUT_TABLE_FILE_NAME = "Output.tbl";

	/** File containing the activity table data that was output by a worker **/
	static final String ACTIVITY_TABLE_FILE_NAME = "Activity.tbl";

	/** File containing text error message reported by a worker **/
	static final String ERROR_FILE_NAME = "Errors.txt";
	
	/** File containing text error message reported by a worker **/
	static final String NR_ERROR_FILE_NAME = "nrerrors.txt";
	
	/** File containing text error message reported by a worker **/
	static final String NR_WARNING_FILE_NAME = "nrwarnings.txt";

	/** File containing worker version information **/
	public static final String VERSION_FILE_NAME = "WorkerVersion.txt";

	/** Flag indicating if uncertainty is being estimated. **/
	boolean estimateUncertainty = false;

	/** The singleton instance **/
	private static OutputProcessor theOutputProcessor = new OutputProcessor();

	/** List of IntegratedPostProcessor objects that modify data added to the Output database. **/
	public LinkedList<IntegratedPostProcessor> integratedPostProcessors =
			new LinkedList<IntegratedPostProcessor>();

	/**
	 * Access method to the singleton instance.
	 * @return The singleton OutputProcessor object.
	**/
	public static OutputProcessor getTheOutputProcessor() {
		return theOutputProcessor;
	}

	/** Default constructor **/
	public OutputProcessor() {
	}

	/** Clear the list of post processors in preparation for a new simulation run **/
	public void resetPostProcessors() {
		integratedPostProcessors = new LinkedList<IntegratedPostProcessor>();
	}

	/**
	 * Process a completed work bundle received from a distributed worker. This
	 * is called by the unbundler.
	 * @param folder The folder that the completed work bundle was unpacked to.
	 * @param filePaths A list of File objects referencing the contents of a completed work
	 * bundle from the distributed processing system.
	**/
	public void processWorkerFiles(File folder, LinkedList<File> filePaths) {
		Logger.log(LogMessageCategory.INFO, "Received bundle from worker");

		Connection executionDatabase = null;
		Connection outputDatabase = null;
		BundleManifest manifest = new BundleManifest();
		try {
			if(!manifest.containsManifest(folder) || !manifest.readFromFolder(folder)) {
				manifest.tablesToRetrieve.add("movesworkeroutput");
				manifest.tablesToRetrieve.add("movesworkeractivityoutput");
				if(CompilationFlags.DO_RATES_FIRST) {
					manifest.tablesToRetrieve.add("baserateoutput");
				}
			}
			executionDatabase = DatabaseConnectionManager.checkOutConnection(
					MOVESDatabaseType.EXECUTION);
			outputDatabase = DatabaseConnectionManager.checkOutConnection(
					MOVESDatabaseType.OUTPUT);

			String workerVersion = "";
			String workerComputerID = "";
			String workerID = "";

			File versionFile = new File(folder, VERSION_FILE_NAME);
			if(versionFile.isFile()) {
				ArrayList<String> lines = null;
				try {
					lines = FileUtilities.readLines(versionFile);
				} catch (Exception exception) {
					/**
					 * @explain A bundle of completed data from a worker did not include
					 * the required internal file giving the worker's version information.
					 * The worker is likely out of date and should be upgraded.
					**/
					Logger.log(LogMessageCategory.ERROR, "Failed to read worker version file.");
				}
				if(lines != null) {
					for(Iterator<String> i=lines.iterator();i.hasNext();) {
						String line = (String)i.next();
						line = line.trim();
						int index = line.indexOf('=');
						if(index <= 0) {
							continue;
						}
						String name = line.substring(0,index).trim();
						String value = line.substring(index+1).trim();
						if(name.equalsIgnoreCase("WorkerVersion")) {
							if(value.length() > 100) {
								value = value.substring(0,100).trim();
							}
							workerVersion = DatabaseUtilities.escapeSQL(value,true);
						} else if(name.equalsIgnoreCase("WorkerComputerID")) {
							if(value.length() > 255) {
								value = value.substring(0,255).trim();
							}
							workerComputerID = DatabaseUtilities.escapeSQL(value,true);
						} else if(name.equalsIgnoreCase("WorkerID")) {
							if(value.length() > 255) {
								value = value.substring(0,255).trim();
							}
							workerID = DatabaseUtilities.escapeSQL(value,true);
						}
					}
					Logger.log(LogMessageCategory.INFO, "Bundle " + manifest.bundleNumber + " is from worker: "
							+ workerComputerID + "/" + workerVersion + "/" + workerID);
					if(workerVersion.length() > 0 && workerComputerID.length() > 0
							&& workerID.length() > 0) {
						String sql = "";
						try {
							// INSERT IGNORE into MOVESWorkersUsed to establish the record
							sql = "insert ignore into movesworkersused (movesrunid,"
									+ " workerversion, workercomputerid, workerid,"
									+ " bundlecount, failedbundlecount)"
									+ " values (" + MOVESEngine.theInstance.getActiveRunID()
									+ "," + workerVersion + "," + workerComputerID
									+ "," + workerID + ",0,0)";
							SQLRunner.executeSQL(outputDatabase, sql);

							// Update MOVESWorkersUsed.bundleCount
							sql = "update movesworkersused set bundlecount=bundlecount+1"
									+ " where movesrunid="
									+ MOVESEngine.theInstance.getActiveRunID()
									+ " and workerversion=" + workerVersion
									+ " and workercomputerid=" + workerComputerID
									+ " and workerid=" + workerID;
							SQLRunner.executeSQL(outputDatabase, sql);
						} catch(Exception e) {
							Logger.logSqlError(e,"Unable to update MOVESWorkersUsed",sql);
						}
						manifest.recordEvent(outputDatabase,false,MOVESEngine.theInstance.getActiveRunID());
					}
				}
			}

			if(workerVersion == null || workerVersion.length() <= 0) {
				workerVersion = StringUtilities.safeGetString(manifest.workerVersion).trim();
				if(workerVersion.length() > 100) {
					workerVersion = workerVersion.substring(0,100).trim();
				}
			}
			if(workerComputerID == null || workerComputerID.length() <= 0) {
				workerComputerID = StringUtilities.safeGetString(manifest.workerComputerID).trim();
				if(workerComputerID.length() > 255) {
					workerComputerID = workerComputerID.substring(0,255).trim();
				}
			}
			if(workerID == null || workerID.length() <= 0) {
				workerID = StringUtilities.safeGetString(manifest.workerID).trim();
				if(workerID.length() > 255) {
					workerID = workerID.substring(0,255).trim();
				}
			}
			
			// Process NR Errors and Warnings
			File nrErrorFile = new File(folder, NR_ERROR_FILE_NAME);
			if(nrErrorFile.isFile()) {
				String allNrErrors = FileUtilities.readEntireFile(nrErrorFile);
				for(String nrError : allNrErrors.split("\n")) {
					MOVESEngine.theInstance.logRunError(
							getValue(manifest.context, "run"), 
							0, 
							getValue(manifest.context, "proc"), 
							getValue(manifest.context, "st"), 
							getValue(manifest.context, "cty"), 
							getValue(manifest.context, "zone"), 
							getValue(manifest.context, "link"), 
							getValue(manifest.context, "y"), 
							getValue(manifest.context, "m"), 
							getValue(manifest.context, "d"), 
							getValue(manifest.context, "h"), 
							nrError);
				}
			}
			File nrWarningFile = new File(folder, NR_WARNING_FILE_NAME);
			if(nrWarningFile.isFile()) {
				String allNrWarnings = FileUtilities.readEntireFile(nrWarningFile);
				for(String nrWarning : allNrWarnings.split("\n")) {
					MOVESEngine.theInstance.logRunError(
							getValue(manifest.context, "run"), 
							0, 
							getValue(manifest.context, "proc"), 
							getValue(manifest.context, "st"), 
							getValue(manifest.context, "cty"), 
							getValue(manifest.context, "zone"), 
							getValue(manifest.context, "link"), 
							getValue(manifest.context, "y"), 
							getValue(manifest.context, "m"), 
							getValue(manifest.context, "d"), 
							getValue(manifest.context, "h"), 
							nrWarning);
				}
			}
			
			// Process other errors
			File errorFile = new File(folder, ERROR_FILE_NAME);
			if(errorFile.isFile()) {
				String errorMessage;
				try {
					errorMessage = "Bundle " + manifest.bundleNumber
						+ " from worker " + workerComputerID + "/" + workerVersion + "/" + workerID + " has errors: "
						+ FileUtilities.readEntireFile(errorFile);
				} catch (Exception exception) {
					errorMessage = "Worker reported errors. Failed to read file.";
				}
				/** @nonissue **/
				Logger.log(LogMessageCategory.ERROR, errorMessage);

				// Update MOVESWorkersUsed.failedBundleCount
				if(workerVersion.length() > 0 && workerComputerID.length() > 0
						&& workerID.length() > 0) {
					String sql = "";
					try {
						sql = "update movesworkersused set failedbundlecount=failedbundlecount+1"
								+ " where movesrunid="
								+ MOVESEngine.theInstance.getActiveRunID()
								+ " and workerversion=" + workerVersion
								+ " and workercomputerid=" + workerComputerID
								+ " and workerid=" + workerID;
						SQLRunner.executeSQL(outputDatabase, sql);
					} catch(Exception e) {
						Logger.logSqlError(e,"Unable to update MOVESWorkersUsed",sql);
					}
				}

				return;
			}

			String sql;
			boolean hasCalculatorOutputTables = false;

			sql = "update movesrun set retrieveddonefiles=retrieveddonefiles+1 where movesrunid=" + MOVESEngine.theInstance.getActiveRunID();
			SQLRunner.executeSQL(outputDatabase, sql);

			// SELECT * can't be used since it would export MOVESOutputRowID. This
			// AUTO_INCREMENT field can't be exported across databases.
			String outputTableFields = "movesrunid,"
					+"yearid,"
					+"monthid,"
					+"dayid,"
					+"hourid,"
					+"stateid,"
					+"countyid,"
					+"zoneid,"
					+"linkid,"
					+"pollutantid,"
					+"processid,"
					+"sourcetypeid,"
					+"regclassid,"
					+"fueltypeid,"
					+"fuelsubtypeid,"
					+"modelyearid,"
					+"roadtypeid,"
					+"scc,"
					+"engtechid,"
					+"sectorid,"
					+"hpid,"
					+"emissionquant";

			String outputTableFieldsWithFuelSubType = "movesrunid,"
					+"yearid,"
					+"monthid,"
					+"dayid,"
					+"hourid,"
					+"stateid,"
					+"countyid,"
					+"zoneid,"
					+"linkid,"
					+"pollutantid,"
					+"processid,"
					+"sourcetypeid,"
					+"regclassid,"
					+"fueltypeid,"
					+"fuelsubtypeid,"
					+"modelyearid,"
					+"roadtypeid,"
					+"scc,"
					+"engtechid,"
					+"sectorid,"
					+"hpid,"
					+"emissionquant";

			String outputTableFieldsNoFuelSubType = "movesrunid,"
					+"yearid,"
					+"monthid,"
					+"dayid,"
					+"hourid,"
					+"stateid,"
					+"countyid,"
					+"zoneid,"
					+"linkid,"
					+"pollutantid,"
					+"processid,"
					+"sourcetypeid,"
					+"regclassid,"
					+"fueltypeid,"
					+"modelyearid,"
					+"roadtypeid,"
					+"scc,"
					+"engtechid,"
					+"sectorid,"
					+"hpid,"
					+"emissionquant";

			// SELECT * can't be used since it would export MOVESActivityOutputRowID. This
			// AUTO_INCREMENT field can't be exported across databases.
			String outputActivityTableFields = "movesrunid,"
					+"yearid,"
					+"monthid,"
					+"dayid,"
					+"hourid,"
					+"stateid,"
					+"countyid,"
					+"zoneid,"
					+"linkid,"
					+"sourcetypeid,"
					+"regclassid,"
					+"fueltypeid,"
					+"fuelsubtypeid,"
					+"modelyearid,"
					+"roadtypeid,"
					+"scc,"
					+"engtechid,"
					+"sectorid,"
					+"hpid,"
					+"activitytypeid,"
					+"activity";

			String outputActivityTableFieldsWithFuelSubType = "movesrunid,"
					+"yearid,"
					+"monthid,"
					+"dayid,"
					+"hourid,"
					+"stateid,"
					+"countyid,"
					+"zoneid,"
					+"linkid,"
					+"sourcetypeid,"
					+"regclassid,"
					+"fueltypeid,"
					+"fuelsubtypeid,"
					+"modelyearid,"
					+"roadtypeid,"
					+"scc,"
					+"engtechid,"
					+"sectorid,"
					+"hpid,"
					+"activitytypeid,"
					+"activity";

			String outputActivityTableFieldsNoFuelSubType = "movesrunid,"
					+"yearid,"
					+"monthid,"
					+"dayid,"
					+"hourid,"
					+"stateid,"
					+"countyid,"
					+"zoneid,"
					+"linkid,"
					+"sourcetypeid,"
					+"regclassid,"
					+"fueltypeid,"
					+"modelyearid,"
					+"roadtypeid,"
					+"scc,"
					+"engtechid,"
					+"sectorid,"
					+"hpid,"
					+"activitytypeid,"
					+"activity";

			for(Iterator ti=manifest.tablesToRetrieve.iterator();ti.hasNext();) {
				String tableName = (String)ti.next();

				if(tableName.equalsIgnoreCase("movesworkeroutput")) {
					hasCalculatorOutputTables = true;
					File outputTableFile = new File(folder, OUTPUT_TABLE_FILE_NAME);
					if(!outputTableFile.isFile()) {
						/**
						 * @explain A bundle of completed data from a worker did not include
						 * a required data file.  The simulation results should be discarded.
						**/
						Logger.log(LogMessageCategory.ERROR,
								"Didn't get output data from distributed bundle");
						return;
					}

					// Create a temporary table to hold the worker's results
					// (only if the table doesn't already exist)
					sql = "CREATE TABLE IF NOT EXISTS temporaryoutputimport "
							+ "SELECT * FROM " + ExecutionRunSpec.getEmissionOutputTable() + " LIMIT 0";
					SQLRunner.executeSQL(outputDatabase, sql);
					if(!CompilationFlags.ALLOW_FUELSUBTYPE_OUTPUT) {
						try {
							SQLRunner.executeSQL(outputDatabase, "alter table temporaryoutputimport add fuelsubtypeid SMALLINT UNSIGNED NULL DEFAULT NULL");
						} catch(Exception e) {
							// Nothing to do here. This may happen if the fuelSubTypeID column already exists.
						}
					}
					// Clear temporary output table.
					sql = "TRUNCATE temporaryoutputimport";
					SQLRunner.executeSQL(outputDatabase, sql);

					// Import data file
					sql = "LOAD DATA INFILE "
							+ DatabaseUtilities.escapeSQL(outputTableFile.getCanonicalPath())
							+ " INTO TABLE temporaryoutputimport " + "(" + outputTableFields + ")";
					SQLRunner.executeSQL(outputDatabase, sql);

					// Use the Run ID of the current run.
					sql = "UPDATE temporaryoutputimport set movesrunid = "
							+ MOVESEngine.theInstance.getActiveRunID();
					SQLRunner.executeSQL(outputDatabase, sql);

					// Use the current iterationID.
					if(estimateUncertainty) {
						sql = "UPDATE temporaryoutputimport set iterationid = "
								+ MOVESEngine.theInstance.getActiveIterationID();
						SQLRunner.executeSQL(outputDatabase, sql);
					}
				} else if(tableName.equalsIgnoreCase("MOVESWorkerActivityOutput")) {
					hasCalculatorOutputTables = true;
					File outputActivityTableFile = new File(folder, ACTIVITY_TABLE_FILE_NAME);
					if(!outputActivityTableFile.isFile()) {
						/**
						 * @explain A bundle of completed data from a worker did not include
						 * a required data file.  The simulation results should be discarded.
						**/
						Logger.log(LogMessageCategory.ERROR,
								"Didn't get output data from distributed bundle");
						return;
					}

					// Drop the temporary activity table so we can be certain its indexing is correct
					sql = "DROP TABLE IF EXISTS temporaryactivityoutputimport";
					SQLRunner.executeSQL(outputDatabase, sql);

					// Create a temporary table to hold the activity results
					// (only if the table doesn't already exist)
					sql = "CREATE TABLE IF NOT EXISTS temporaryactivityoutputimport "
							+ "SELECT * FROM " + ExecutionRunSpec.getActivityOutputTable() + " LIMIT 0";
					SQLRunner.executeSQL(outputDatabase, sql);
					if(!CompilationFlags.ALLOW_FUELSUBTYPE_OUTPUT) {
						try {
							SQLRunner.executeSQL(outputDatabase, "alter table temporaryactivityoutputimport add fuelsubtypeid SMALLINT UNSIGNED NULL DEFAULT NULL");
						} catch(Exception e) {
							// Nothing to do here. This may happen if the fuelSubTypeID column already exists.
						}
					}
					// Clear temporary output table.
					sql = "TRUNCATE temporaryactivityoutputimport";
					SQLRunner.executeSQL(outputDatabase, sql);

					// Import data file
					sql = "LOAD DATA INFILE "
							+ DatabaseUtilities.escapeSQL(outputActivityTableFile.getCanonicalPath())
							+ " INTO TABLE temporaryactivityoutputimport " + "("
							+ outputActivityTableFields + ")";
					SQLRunner.executeSQL(outputDatabase, sql);

					// Use the Run ID of the current run.
					sql = "UPDATE temporaryactivityoutputimport SET movesrunid = "
							+ MOVESEngine.theInstance.getActiveRunID();
					SQLRunner.executeSQL(outputDatabase, sql);

					// Use the current iterationID.
					if(estimateUncertainty) {
						sql = "UPDATE temporaryactivityoutputimport SET iterationid = "
								+ MOVESEngine.theInstance.getActiveIterationID();
						SQLRunner.executeSQL(outputDatabase, sql);
					}
				} else {
					Connection cmitDB = executionDatabase;
					boolean isOutputDatabase = false;
					if(tableName.equalsIgnoreCase("baserateoutput")) {
						cmitDB = outputDatabase;
						isOutputDatabase = true;
					}
					// The table is a CMIT table and should be read into a temporary table
					// then INSERT IGNORE'd into the primary CMIT table
					String tempTableName = "temp" + tableName;
					File dataFile = new File(folder,tableName + ".tbl");
					if(dataFile.exists()) {
						// Create a temporary table to handle the loaded data
						sql = "create table if not exists " + tempTableName + " like " + tableName;
						SQLRunner.executeSQL(cmitDB,sql);
						// Ensure the temporary table is empty
						sql = "truncate table " + tempTableName;
						SQLRunner.executeSQL(cmitDB,sql);
						// Import data into the temporary table
						sql = "LOAD DATA INFILE "
								+ DatabaseUtilities.escapeSQL(dataFile.getCanonicalPath())
								+ " INTO TABLE " + tempTableName;
						SQLRunner.executeSQL(cmitDB, sql);

						if(CompilationFlags.DO_RATES_FIRST) {
							if(isOutputDatabase) {
								try {
									// Use the Run ID of the current run.
									sql = "UPDATE " + tempTableName + " SET movesrunid = " + MOVESEngine.theInstance.getActiveRunID();
									SQLRunner.executeSQL(cmitDB, sql);

									// Use the current iterationID.
									if(estimateUncertainty) {
										sql = "UPDATE " + tempTableName + " SET iterationid = " + MOVESEngine.theInstance.getActiveIterationID();
										SQLRunner.executeSQL(cmitDB, sql);
									}
								} catch(Exception e) {
									// Nothing to do here because the table simply may not contain MOVESRunID or iterationID columns.
								}
							}
						}

						// INSERT IGNORE the data into the primary table
						sql = "insert ignore into " + tableName
								+ " select * from " + tempTableName;
						SQLRunner.executeSQL(cmitDB, sql);
						// Get rid of the temporary table
						sql = "drop table if exists " + tempTableName;
						SQLRunner.executeSQL(cmitDB,sql);
					} else {
						Logger.log(LogMessageCategory.ERROR,"TBL file not sent from the worker for table " + tableName);
					}
				}
			}

			if(hasCalculatorOutputTables) {
				try {
					// Invoke IntegratedPostProcessor objects
					for (Iterator i = integratedPostProcessors.iterator(); i.hasNext(); ) {
						IntegratedPostProcessor iterProcessor = (IntegratedPostProcessor) i.next();

						iterProcessor.execute(outputDatabase);
					}

					// Move data into the final output emission table
					String outputTableFieldsToUse = CompilationFlags.ALLOW_FUELSUBTYPE_OUTPUT? outputTableFieldsWithFuelSubType : outputTableFieldsNoFuelSubType;
					sql = "INSERT INTO " + ExecutionRunSpec.getEmissionOutputTable()
							+ "(iterationid," + outputTableFieldsToUse + ") "
							+ "SELECT iterationid," + outputTableFieldsToUse
							+ " FROM temporaryoutputimport";
					SQLRunner.executeSQL(outputDatabase, sql);

					// Move data into the final output activity table
					String activityTableFieldsToUse = CompilationFlags.ALLOW_FUELSUBTYPE_OUTPUT? outputActivityTableFieldsWithFuelSubType : outputActivityTableFieldsNoFuelSubType;
					sql = "INSERT INTO " + ExecutionRunSpec.getActivityOutputTable()
							+ "(iterationid," + activityTableFieldsToUse + ") "
							+ "SELECT iterationid," + activityTableFieldsToUse
							+ " FROM temporaryactivityoutputimport";
					SQLRunner.executeSQL(outputDatabase, sql);

					/* The following incremental aggregation was removed for Task 812.
					 * MOVES now only aggregates its outputs (emissions and activity)
					 * at the end of each iteration, if required and if desired by
					 * the user.
						// Aggregate the moves output for some of the processed bundles
						aggregateOutput(outputDatabase);
					*/
				} finally {
					if(!keepDebugData) {
						// Clear temporary output table.
						sql = "TRUNCATE temporaryoutputimport";
						SQLRunner.executeSQL(outputDatabase, sql);
	
						// Clear temporary output table.
						sql = "TRUNCATE temporaryactivityoutputimport";
						SQLRunner.executeSQL(outputDatabase, sql);
					}
				}
			}
		} catch (Exception exception) {
			/**
			 * @explain An error occurred while reading results returned from a worker.
			**/
			Logger.logError(exception, "Failed to process Worker Files in OutputProcessor.");
		} finally {
			if(executionDatabase != null) {
				DatabaseConnectionManager.checkInConnection(
						MOVESDatabaseType.EXECUTION, executionDatabase);
				executionDatabase = null;
			}
			if(outputDatabase != null) {
				DatabaseConnectionManager.checkInConnection(
						MOVESDatabaseType.OUTPUT, outputDatabase);
				outputDatabase = null;
			}
			updateMOVESRun();
		}
	}

	/**
	 * This method will aggregate the output every 10% of the total bundles.
	 * In this way, we minimize data in the output table and minimize the amount
	 * of time spent aggregating output data at the final pass.
	 * @param outputDatabase A connection to a MOVES output database containing the
	 * MOVESOutput table.
	**/
	private void aggregateOutput(Connection outputDatabase) {
		Integer t = MOVESEngine.theInstance.getHowManyOutboundBundlesWillBeCreated();
		int totalBundleCount = 0;
		if(t != null) {
			totalBundleCount = t.intValue();
		}
		if(totalBundleCount == 0) {
			return;
		}
		try {
			// Check the progress so far.  If not at a 10% point, then return, otherwise allow
			// the partial aggregation of results accumulated so far.
			if(0 != (MOVESEngine.theInstance.getHowManyBundlesProcessedSoFar()
					% ((totalBundleCount+9)/10))) {
				return;
			}
			Vector outputProcessorSQLs = ExecutionRunSpec.theExecutionRunSpec.outputProcessorSQLs;
			for(int i=0; i<outputProcessorSQLs.size(); i++) {
				SQLRunner.executeSQL(outputDatabase, (String) outputProcessorSQLs.elementAt(i));
			}
		} catch(Exception e) {
			/**
			 * @explain A database error occurred while aggregating data in the master-side
			 * output database.
			**/
			Logger.logError(e, "Failed to aggregate output, OutputProcessor.aggregateOutput()");
		}
	}

	/**
	 * Give IntegratedPostProcessor objects one last chance to examine the output database,
	 * aggregate the final output, then convert the units in the output database to the units
	 * the user has selected.  This is called by the unbundler.
	**/
	public void doFinalPostProcessingOnOutputDatabase() {
		Connection executionDatabase = null;
		Connection outputDatabase = null;
		try {
			outputDatabase = DatabaseConnectionManager.checkOutConnection(
					MOVESDatabaseType.OUTPUT);

			if(!keepDebugData) {
				// Drop our intermediate results tables
				SQLRunner.executeSQL(outputDatabase,
						"DROP TABLE IF EXISTS temporaryoutputimport");
				SQLRunner.executeSQL(outputDatabase,
						"DROP TABLE IF EXISTS temporaryactivityoutputimport");
			}

			if(MOVESEngine.theInstance.allowFinalPostProcessing()) {
				executionDatabase = DatabaseConnectionManager.checkOutConnection(
						MOVESDatabaseType.EXECUTION);

				// Invoke IntegratedPostProcessor objects before units get converted
				for (Iterator i = integratedPostProcessors.iterator(); i.hasNext(); ) {
					IntegratedPostProcessor iterProcessor = (IntegratedPostProcessor) i.next();

					iterProcessor.doFinalPostProcessingOnOutputDatabase(outputDatabase,
							executionDatabase,true);
				}

				// Final aggregation done to the output database
				if(ExecutionRunSpec.theExecutionRunSpec.shouldDoFinalAggregation()) {
					Logger.log(LogMessageCategory.INFO,"Final Aggregation starting...");
					if(keepDebugData) {
						SQLRunner.executeSQL(outputDatabase,"drop table if exists finalaggbefore");
						SQLRunner.executeSQL(outputDatabase,"create table finalaggbefore select * from movesoutput");
					}
					Vector finalProcessSQLs = ExecutionRunSpec.theExecutionRunSpec.finalProcessSQLs;
					for(int i=0; i<finalProcessSQLs.size(); i++) {
						SQLRunner.executeSQL(outputDatabase, (String) finalProcessSQLs.elementAt(i));
					}
					if(keepDebugData) {
						SQLRunner.executeSQL(outputDatabase,"drop table if exists finalaggafter");
						SQLRunner.executeSQL(outputDatabase,"create table finalaggafter select * from movesoutput");
					}
					Logger.log(LogMessageCategory.INFO,"Final Aggregation complete.");
				}

				// Convert the units
				Logger.log(LogMessageCategory.INFO,"Unit conversions starting...");
				convertOutputUnits(executionDatabase, outputDatabase);
				if(keepDebugData) {
					SQLRunner.executeSQL(outputDatabase,"drop table if exists unitconvertafter");
					SQLRunner.executeSQL(outputDatabase,"create table unitconvertafter select * from movesoutput");
				}
				Logger.log(LogMessageCategory.INFO,"Unit conversions done.");

				// Invoke IntegratedPostProcessor objects after units get converted
				if(integratedPostProcessors.size() > 0) {
					Logger.log(LogMessageCategory.INFO,"Final Post processing starting...");
					for (Iterator i = integratedPostProcessors.iterator(); i.hasNext(); ) {
						IntegratedPostProcessor iterProcessor = (IntegratedPostProcessor) i.next();

						iterProcessor.doFinalPostProcessingOnOutputDatabase(outputDatabase,
								executionDatabase,false);
					}
					/** @nonissue **/
					Logger.log(LogMessageCategory.INFO,"Final Post processing done.");
				} else {
					/** @nonissue **/
					Logger.log(LogMessageCategory.INFO,"No final post processing required.");
				}
			} else {
				/** @nonissue **/
				Logger.log(LogMessageCategory.INFO,"No unit conversion, aggregation, or post processing is performed in this mode.");
			}
		} catch (Exception exception) {
			//Logger.logException(LogMessageCategory.ERROR, exception);
			/**
			 * @explain A database error occurred while performing post processing on the
			 * simulator results, before converting units to user-selected units.
			**/
			Logger.logError(exception, "Failed to do final post processing in OutputProcessor.");
		} finally {
			if(executionDatabase != null) {
				DatabaseConnectionManager.checkInConnection(
					MOVESDatabaseType.EXECUTION, executionDatabase);
				executionDatabase = null;
			}
			if(outputDatabase != null) {
				DatabaseConnectionManager.checkInConnection(
					MOVESDatabaseType.OUTPUT, outputDatabase);
				outputDatabase = null;
			}
			updateMOVESRun();
		}
	}

	/**
	 * Converts the units of the Emission Quantities reported in MOVESOutput to the units
	 * selected in the RunSpec. The unit conversion involves,
	 * 1. Mass & Energy unit conversion
	 * 2. Time unit conversion
	 * This method is called by doFinalPostProcessingOnOutputDatabase after all
	 * other processing on the output database is complete.<br>
	 * The base output units are grams for mass and kilojoules for energy.
	 * @param executionDatabase A connection to an MOVES execution database containing the
	 * Pollutant table.
	 * @param outputDatabase A connection to an MOVES output database containing the
	 * MOVESOutput table.
	**/
	void convertOutputUnits(Connection executionDatabase, Connection outputDatabase) {
		PreparedStatement selectStatement = null;
		ResultSet results = null;
		String sql = "";
		try {
			String selectSQL = "SELECT DISTINCT pollutantid, energyormass from pollutant";
			String massPollutants = "";
			String energyPollutants = "";
			selectStatement = executionDatabase.prepareStatement(selectSQL);
			results = SQLRunner.executeQuery(selectStatement, selectSQL);
			boolean isFirstEnergyPollutant = true;
			boolean isFirstMassPollutant = true;
			while(results.next()) {
				String pollutantID = results.getString(1);
				String units = results.getString(2);
				if(units == null || pollutantID == null) {
					continue;
				} else if(units.trim().equalsIgnoreCase("energy") ||
						units.indexOf("energy") >= 0){
					if(isFirstEnergyPollutant) {
						energyPollutants = pollutantID;
						isFirstEnergyPollutant = false;
					} else {
						energyPollutants = energyPollutants + "," + pollutantID;
					}
				} else {
					if(isFirstMassPollutant) {
						massPollutants = pollutantID;
						isFirstMassPollutant = false;
					} else {
						massPollutants = massPollutants + "," + pollutantID;
					}
				}
			}
			results.close();
			selectStatement.close();
			if(massPollutants.length() > 0) {
				double massConversionToKilogramFactor =
						ExecutionRunSpec.theExecutionRunSpec.getOutputFactors().
						massMeasurementSystem.getConversionToKilogramsFactor();
				// Native database units are grams (g) and measurement systems provide
				// factor in kilogram/user-unit.  Remember that dividing by a factor is the
				// same as multiplying by its inverse:
				//
				// n g      kg     user-unit      n
				// ---- * ------ * --------- = -------- user-units
				//  1     1000 g     c kg      c * 1000
				String updateMassUnitSQL = "UPDATE " + ExecutionRunSpec.getEmissionOutputTable()
						+ " SET emissionquant ="
						+ " emissionquant / 1000 / " + massConversionToKilogramFactor
						+ " WHERE pollutantid IN (" + massPollutants + ") AND movesrunid ="
						+ MOVESEngine.theInstance.getActiveRunID();
				if(estimateUncertainty) {
					updateMassUnitSQL += " AND iterationid ="
							+ MOVESEngine.theInstance.getActiveIterationID();
				}
				//SQLRunner.executeSQL(outputDatabase, "FLUSH TABLES");
				SQLRunner.executeSQL(outputDatabase, updateMassUnitSQL);

				if(CompilationFlags.DO_RATES_FIRST) {
					updateMassUnitSQL = "update baserateunits set"
							+ " meanbaserateunitsnumerator = " + DatabaseUtilities.escapeSQL(MOVESEngine.theInstance.masterFragment.massUnits,true)
							+ " ,emissionbaserateunitsnumerator = " + DatabaseUtilities.escapeSQL(MOVESEngine.theInstance.masterFragment.massUnits,true)
							+ " where movesrunid=" + MOVESEngine.theInstance.getActiveRunID()
							+ " and pollutantid in (" + massPollutants + ")";
					SQLRunner.executeSQL(outputDatabase, updateMassUnitSQL);

					updateMassUnitSQL = "update baserateoutput set"
						+ " meanbaserate = meanbaserate / 1000 / " + massConversionToKilogramFactor
						+ ",emissionrate = emissionrate / 1000 / " + massConversionToKilogramFactor
						+ " where movesrunid=" + MOVESEngine.theInstance.getActiveRunID()
						+ " and pollutantid in (" + massPollutants + ")";
					if(estimateUncertainty) {
						updateMassUnitSQL += " AND iterationid ="
								+ MOVESEngine.theInstance.getActiveIterationID();
					}
					SQLRunner.executeSQL(outputDatabase, updateMassUnitSQL);
				}
			}
			sql = "update movesrun set massunits=" + DatabaseUtilities.escapeSQL(MOVESEngine.theInstance.masterFragment.massUnits,true)
					+ " where movesrunid=" + MOVESEngine.theInstance.getActiveRunID();
			SQLRunner.executeSQL(outputDatabase, sql);

			if(energyPollutants.length() > 0) {
				double energyConversionToJoulesFactor =
						ExecutionRunSpec.theExecutionRunSpec.getOutputFactors().
						energyMeasurementSystem.getConversionToJoulesFactor();
				// Native database units are kilojoules (KJ) and measurement systems provide
				// factor in joules/user-unit.  Remember that dividing by a factor is the
				// same as multiplying by its inverse:
				//
				// n KJ   1000 J   user-unit   n*1000
				// ---- * ------ * --------- = ------ user-units
				//  1       KJ        c J         c
				String updateEnergyUnitSQL = "UPDATE " + ExecutionRunSpec.getEmissionOutputTable()
						+ " SET emissionquant ="
						+ " emissionquant * 1000 / " + energyConversionToJoulesFactor
						+ " WHERE pollutantid IN (" + energyPollutants + ") AND movesrunid ="
						+ MOVESEngine.theInstance.getActiveRunID();
				if(estimateUncertainty) {
						updateEnergyUnitSQL += " AND iterationid="
								+ MOVESEngine.theInstance.getActiveIterationID();
				}
				//SQLRunner.executeSQL(outputDatabase, "FLUSH TABLES");
				SQLRunner.executeSQL(outputDatabase, updateEnergyUnitSQL);

				if(CompilationFlags.DO_RATES_FIRST) {
					updateEnergyUnitSQL = "update baserateunits set"
							+ " meanbaserateunitsnumerator = " + DatabaseUtilities.escapeSQL(MOVESEngine.theInstance.masterFragment.energyUnits,true)
							+ " ,emissionbaserateunitsnumerator = " + DatabaseUtilities.escapeSQL(MOVESEngine.theInstance.masterFragment.energyUnits,true)
							+ " where movesrunid=" + MOVESEngine.theInstance.getActiveRunID()
							+ " and pollutantid in (" + energyPollutants + ")";
					SQLRunner.executeSQL(outputDatabase, updateEnergyUnitSQL);

					updateEnergyUnitSQL = "update baserateoutput set"
						+ " meanbaserate = meanbaserate * 1000 / " + energyConversionToJoulesFactor
						+ ",emissionrate = emissionrate * 1000 / " + energyConversionToJoulesFactor
						+ " where movesrunid=" + MOVESEngine.theInstance.getActiveRunID()
						+ " and pollutantid in (" + energyPollutants + ")";
					if(estimateUncertainty) {
						updateEnergyUnitSQL += " AND iterationid ="
								+ MOVESEngine.theInstance.getActiveIterationID();
					}
					SQLRunner.executeSQL(outputDatabase, updateEnergyUnitSQL);
				}
			}
			sql = "update movesrun set energyunits=" + DatabaseUtilities.escapeSQL(MOVESEngine.theInstance.masterFragment.energyUnits,true)
					+ " where movesrunid=" + MOVESEngine.theInstance.getActiveRunID();
			SQLRunner.executeSQL(outputDatabase, sql);

			// Time unit conversion
			double timeConversionToSecondsFactor =
					ExecutionRunSpec.theExecutionRunSpec.getOutputFactors().timeMeasurementSystem.
					getConversionToSecondsFactor();
			double averageHours = ExecutionRunSpec.theExecutionRunSpec.getOutputTimeStep().
					getAverageHours();
			Logger.log(LogMessageCategory.INFO,"Time Measurement System = " +
					ExecutionRunSpec.theExecutionRunSpec.getOutputFactors().timeMeasurementSystem);
			Logger.log(LogMessageCategory.INFO,"Output Time Step = " +
					ExecutionRunSpec.theExecutionRunSpec.getOutputTimeStep());
			Logger.log(LogMessageCategory.INFO,"Output Time Step average hours = " + averageHours);

			// Native database units are user defined and measurement systems,
			// provide factor in sec/(TMS)user-unit.  Output time step uses hour/(OTS)user-unit.
			// Remember that dividing by a factor is the same as multiplying by its inverse:
			// OTS : Output Time Step
			// TMS : Time Measurement System
			//
			//  n      OTS     hour   sec
			// ---- * ------ * ---- * --- =  (n / averageHours / 3600 * TMS) user-units
			// OTS     hour    sec    TMS
			String updateTimeUnitSQL = "UPDATE " + ExecutionRunSpec.getEmissionOutputTable()
					+ " SET emissionquant =" + " emissionquant / " + averageHours + " / 3600 * "
					+ timeConversionToSecondsFactor
					+ " WHERE movesrunid =" + MOVESEngine.theInstance.getActiveRunID();
			if(estimateUncertainty) {
				updateTimeUnitSQL += " AND iterationid ="
						+ MOVESEngine.theInstance.getActiveIterationID();
			}
			//SQLRunner.executeSQL(outputDatabase, "FLUSH TABLES");
//			SQLRunner.executeSQL(outputDatabase, updateTimeUnitSQL);
			// Distance unit conversion, including the time unit conversion too
			if(ExecutionRunSpec.theExecutionRunSpec.getOutputFactors().distanceMeasurementSystem
					!= null) {
				double distanceConversionToMetersFactor =
						ExecutionRunSpec.theExecutionRunSpec.getOutputFactors().
						distanceMeasurementSystem.getConversionToMetersFactor();
				// Native database units are miles and measurement systems provide
				// factor in meters/user-unit.
				//
				//  n miles    1609.344 meters       user-units
				// --------- * ----------------- * ------------- = (n * 1609.344 / c) user-units
				//     1           miles             c meters
				/*
				String updateDistanceUnitSQL = "UPDATE " + ExecutionRunSpec.getActivityOutputTable()
						+ " SET activity ="
						+ " activity * 1609.344 / " + distanceConversionToMetersFactor
						+ " / " + averageHours + " / 3600 * "
						+ timeConversionToSecondsFactor
						+ " WHERE MOVESRunID =" + MOVESEngine.theInstance.getActiveRunID()
						+ " AND activityTypeID=1";
				*/
				String updateDistanceUnitSQL = "UPDATE " + ExecutionRunSpec.getActivityOutputTable()
						+ " SET activity ="
						+ " activity * 1609.344 / " + distanceConversionToMetersFactor
						+ " WHERE movesrunid =" + MOVESEngine.theInstance.getActiveRunID()
						+ " AND activitytypeid=1";
				if(estimateUncertainty) {
					updateDistanceUnitSQL += " AND iterationid = "
							+ MOVESEngine.theInstance.getActiveIterationID();
				}

				//SQLRunner.executeSQL(outputDatabase, "FLUSH TABLES");
				SQLRunner.executeSQL(outputDatabase, updateDistanceUnitSQL);

				if(CompilationFlags.DO_RATES_FIRST) {
					updateDistanceUnitSQL = "update baserateunits set"
							+ " emissionbaserateunitsdenominator = " + DatabaseUtilities.escapeSQL(MOVESEngine.theInstance.masterFragment.distanceUnits,true)
							+ " where movesrunid=" + MOVESEngine.theInstance.getActiveRunID()
							+ " and emissionbaserateunitsdenominator='mi'";
					SQLRunner.executeSQL(outputDatabase, updateDistanceUnitSQL);

					// g        mi       c meters         g
					// -- * ---------- * ---------- = ----------
					// mi   1609.344 m   user-units   user-units
					updateDistanceUnitSQL = "update baserateoutput set"
						+ " emissionrate = emissionrate * " + distanceConversionToMetersFactor + " / 1609.344"
						+ " where movesrunid=" + MOVESEngine.theInstance.getActiveRunID()
						+ " and processid in (1,9,10,15)";
					if(estimateUncertainty) {
						updateDistanceUnitSQL += " AND iterationid ="
								+ MOVESEngine.theInstance.getActiveIterationID();
					}
					SQLRunner.executeSQL(outputDatabase, updateDistanceUnitSQL);
				}
			}
			sql = "update movesrun set distanceunits=" + DatabaseUtilities.escapeSQL(MOVESEngine.theInstance.masterFragment.distanceUnits,true)
					+ " where movesrunid=" + MOVESEngine.theInstance.getActiveRunID();
			SQLRunner.executeSQL(outputDatabase, sql);
		} catch (Exception exception) {
			/**
			 * @explain A database error occurred while converting standard units in the output
			 * database into user-selected units.  The output data should be considered suspect.
			**/
			Logger.logError(exception, "Failed to convert emission quantity units.");
		} finally {
			if(results != null) {
				try {
					results.close();
				} catch(Exception e) {
					// Nothing to do here
				}
			}
			if(selectStatement != null) {
				try {
					selectStatement.close();
				} catch(Exception e) {
					// Nothing to do here
				}
			}
		}
	}

	/**
	 * Prepare output database for uncertainty estimations
	**/
	public void prepareForEstimateUncertainty() {
		estimateUncertainty = ExecutionRunSpec.theExecutionRunSpec.estimateUncertainty();
		if(estimateUncertainty) {
			Connection outputDatabase = null;
			String sql = "";
			try {
				outputDatabase = DatabaseConnectionManager.checkOutConnection(
							MOVESDatabaseType.OUTPUT);
				try {
					sql = "ALTER TABLE " + ExecutionRunSpec.getEmissionOutputTable()
						+ " ADD COLUMN (emissionquantsum DOUBLE, emissionquantsum2 DOUBLE)";
					SQLRunner.executeSQL(outputDatabase, sql);
				} catch (Exception exception) {
					// Nothing to do here.
				}
				try {
					sql = "ALTER TABLE " + ExecutionRunSpec.getActivityOutputTable()
						+ " ADD COLUMN (activitysum DOUBLE, activitysum2 DOUBLE)";
					SQLRunner.executeSQL(outputDatabase, sql);
				} catch (Exception exception) {
					// Nothing to do here.
				}
			} catch (Exception exception) {
				/**
				 * @explain A database error occurred while processing uncertainty estimation
				 * data.
				**/
				Logger.logError(exception,
						"Failed to add columns used in estimating uncertainty.");
			} finally {
				if(outputDatabase != null) {
					DatabaseConnectionManager.checkInConnection(
							MOVESDatabaseType.OUTPUT, outputDatabase);
					outputDatabase = null;
				}
			}
		}
	}

	/**
	 * Clean up after uncertainty estimations
	**/
	public void cleanUpAfterEstimateUncertainty() {
		Connection outputDatabase = null;
		String sql = "";
		try {
			outputDatabase = DatabaseConnectionManager.checkOutConnection(
						MOVESDatabaseType.OUTPUT);
			if(estimateUncertainty) {
				try {
					sql = "ALTER TABLE " + ExecutionRunSpec.getEmissionOutputTable()
						+ " DROP COLUMN emissionquantsum, DROP COLUMN emissionquantsum2";
					SQLRunner.executeSQL(outputDatabase, sql);
					sql = "ALTER TABLE " + ExecutionRunSpec.getActivityOutputTable()
						+ " DROP COLUMN activitysum, DROP COLUMN activitysum2";
					SQLRunner.executeSQL(outputDatabase, sql);
				} catch (Exception exception) {
					/**
					 * @explain A database error occurred while processing uncertainty estimation
					 * data.
					**/
					Logger.logError(exception,
							"Failed to drop columns used in estimating uncertainty.");
				}
			}
			if(ExecutionRunSpec.getRunSpec().scale == ModelScale.MESOSCALE_LOOKUP) {
				if(ExecutionRunSpec.getRunSpec().shouldTruncateMOVESOutput) {
					Logger.log(LogMessageCategory.INFO,"Removing data from MOVESOutput per Advanced Performance Features setting...");
					sql = "delete from movesoutput where movesrunid=" + MOVESEngine.theInstance.getActiveRunID();
					SQLRunner.executeSQL(outputDatabase, sql);
					// Truncate if there is no more data in order to save time when transfering output databases.
					sql = "select count(*) from movesoutput";
					if(SQLRunner.executeScalar(outputDatabase, sql) <= 0) {
						Logger.log(LogMessageCategory.INFO,"Truncating empty MOVESOutput per Advanced Performance Features setting.");
						sql = "truncate movesoutput";
						SQLRunner.executeSQL(outputDatabase, sql);
					}
					Logger.log(LogMessageCategory.INFO,"Removed data from MOVESOutput per Advanced Performance Features setting.");
				}
				if(ExecutionRunSpec.getRunSpec().shouldTruncateMOVESActivityOutput) {
					Logger.log(LogMessageCategory.INFO,"Removing data from MOVESActivityOutput per Advanced Performance Features setting...");
					sql = "delete from movesactivityoutput where movesrunid=" + MOVESEngine.theInstance.getActiveRunID();
					SQLRunner.executeSQL(outputDatabase, sql);
					// Truncate if there is no more data in order to save time when transfering output databases.
					sql = "select count(*) from movesactivityoutput";
					if(SQLRunner.executeScalar(outputDatabase, sql) <= 0) {
						Logger.log(LogMessageCategory.INFO,"Truncating empty MOVESActivityOutput per Advanced Performance Features setting.");
						sql = "truncate movesactivityoutput";
						SQLRunner.executeSQL(outputDatabase, sql);
					}
					Logger.log(LogMessageCategory.INFO,"Removed data from MOVESActivityOutput per Advanced Performance Features setting.");
				}
				if(ExecutionRunSpec.getRunSpec().shouldTruncateBaseRateOutput) {
					Logger.log(LogMessageCategory.INFO,"Removing data from BaseRateOutput per Advanced Performance Features setting...");
					sql = "delete from baserateoutput where movesrunid=" + MOVESEngine.theInstance.getActiveRunID();
					SQLRunner.executeSQL(outputDatabase, sql);
					// Truncate if there is no more data in order to save time when transfering output databases.
					sql = "select count(*) from baserateoutput";
					if(SQLRunner.executeScalar(outputDatabase, sql) <= 0) {
						Logger.log(LogMessageCategory.INFO,"Truncating empty BaseRateOutput per Advanced Performance Features setting.");
						sql = "truncate baserateoutput";
						SQLRunner.executeSQL(outputDatabase, sql);
					}
					Logger.log(LogMessageCategory.INFO,"Removed data from BaseRateOutput per Advanced Performance Features setting.");

					Logger.log(LogMessageCategory.INFO,"Removing data from BaseRateUnits per Advanced Performance Features setting...");
					sql = "delete from baserateunits where movesrunid=" + MOVESEngine.theInstance.getActiveRunID();
					SQLRunner.executeSQL(outputDatabase, sql);
					// Truncate if there is no more data in order to save time when transfering output databases.
					sql = "select count(*) from baserateunits";
					if(SQLRunner.executeScalar(outputDatabase, sql) <= 0) {
						Logger.log(LogMessageCategory.INFO,"Truncating empty BaseRateUnits per Advanced Performance Features setting.");
						sql = "truncate baserateunits";
						SQLRunner.executeSQL(outputDatabase, sql);
					}
					Logger.log(LogMessageCategory.INFO,"Removed data from BaseRateUnits per Advanced Performance Features setting.");
				}
			}
		} catch(Exception exception) {
			/**
			 * @explain A database error occurred while removing data from MOVESOutput, MOVESActivityOutput, BaseRateOutput, or BaseRateUnits.
			**/
			Logger.logError(exception,
					"A database error occurred while removing data from MOVESOutput, MOVESActivityOutput, BaseRateOutput, or BaseRateUnits.");
		} finally {
			if(outputDatabase != null) {
				DatabaseConnectionManager.checkInConnection(
						MOVESDatabaseType.OUTPUT, outputDatabase);
				outputDatabase = null;
			}
		}
	}

	/**
	 * Perform uncertainty estimations for the iteration.
	**/
	public void performEstimateUncertainty() {
		if(estimateUncertainty) {
			Connection outputDatabase = null;
			String sql = "";
			try {
				int activeRunID = MOVESEngine.theInstance.getActiveRunID();
				int activeIterationID = MOVESEngine.theInstance.getActiveIterationID();
				Logger.log(LogMessageCategory.INFO,"Estimating uncertainty for run "
						+ activeRunID + " iteration "+activeIterationID);
				outputDatabase = DatabaseConnectionManager.checkOutConnection(
							MOVESDatabaseType.OUTPUT);
				if(activeIterationID == 1) {
					sql = "UPDATE " + ExecutionRunSpec.getEmissionOutputTable()
							+ " SET emissionquantmean = emissionquant,"
							+ " emissionquantsum = emissionquant, emissionquantsum2 ="
							+ " emissionquant * emissionquant where movesrunid = " + activeRunID
							+ " AND iterationid = 1";
					SQLRunner.executeSQL(outputDatabase,sql);
					/*=======================================================
					 * For calculating activity uncertainty when available.
					 *=======================================================
					sql = "UPDATE " + ExecutionRunSpec.getActivityOutputTable()
							+ " SET activityMean = activity, "
							+ " activitySigma = 0, activitySum = activity,"
							+ " activitySum2 = activity * activity"
							+ " WHERE MOVESRunID = " + activeRunID
							+ " AND iterationID = 1";
					SQLRunner.executeSQL(outputDatabase,sql);
					 *=====================================================*/
				} else {
					sql = "drop table if exists movesoutputsumtemp";
					SQLRunner.executeSQL(outputDatabase,sql);
					sql = "create table movesoutputsumtemp like "
							+ ExecutionRunSpec.getEmissionOutputTable();
					SQLRunner.executeSQLCore(outputDatabase,sql);

					String[] emissionStatements = {
						"create index ixoutputsumtemp on movesoutputsumtemp (iterationid,yearid,monthid,dayid,hourid,stateid,countyid,zoneid,linkid,pollutantid,processid,"
								+ " sourcetypeid,regclassid,fueltypeid,fuelsubtypeid,modelyearid,roadtypeid,scc)",
						"insert into movesoutputsumtemp (movesrunid,iterationid,yearid,monthid,dayid,hourid,stateid,countyid,zoneid,linkid,pollutantid,processid,"
								+ " sourcetypeid,regclassid,fueltypeid,fuelsubtypeid,modelyearid,roadtypeid,scc,"
//								+ " engtechid,sectorid,hpid,"
								+ " emissionquant, emissionquantmean, emissionquantsum, emissionquantsum2)"
								+ " select movesrunid,iterationid,"
								+ " coalesce(yearid,0),coalesce(monthid,0),coalesce(dayid,0),coalesce(hourid,0),coalesce(stateid,0),coalesce(countyid,0),"
								+ " coalesce(zoneid,0),coalesce(linkid,0),coalesce(pollutantid,0),coalesce(processid,0),"
								+ " coalesce(sourcetypeid,0),coalesce(regclassid,0),coalesce(fueltypeid,0),coalesce(fuelsubtypeid,0),coalesce(modelyearid,0),coalesce(roadtypeid,0),coalesce(scc,0),"
//								+ " coalesce(engtechid,0),coalesce(sectorid,0),coalesce(hpid,0),"
								+ " emissionquant, emissionquantmean, emissionquantsum, emissionquantsum2"
								+ " from " + ExecutionRunSpec.getEmissionOutputTable()
								+ " where movesrunid=" + activeRunID
								+ " and iterationid in (" + activeIterationID + "," + (activeIterationID-1) + ")",
						"CREATE TABLE IF NOT EXISTS movesoutputsum"
								+ " select mo2.movesrunid, mo2.iterationid, mo2.yearid, mo2.monthid, mo2.dayid, mo2.hourid, mo2.stateid, "
								+ " mo2.countyid, mo2.zoneid, mo2.linkid, mo2.pollutantid, mo2.processid, mo2.sourcetypeid, mo2.regclassid, mo2.fueltypeid, mo2.fuelsubtypeid,"
								+ " mo2.modelyearid, mo2.roadtypeid, mo2.scc, "
//								+ " mo2.engtechid, mo2.sectorid, mo2.hpid,"
								+ " mo2.emissionquant,"
								+ " (mo2.emissionquant + mo1.emissionquantsum)/ mo2.iterationid as emissionquantmean, "
								+ " mo2.emissionquant + mo1.emissionquantsum as emissionquantsum, "
								+ " mo2.emissionquant * mo2.emissionquant + mo1.emissionquantsum2 as emissionquantsum2 "
								+ " from movesoutputsumtemp as mo1, movesoutputsumtemp as mo2 "
								+ " where mo2.iterationid = " + activeIterationID
								+ " AND mo1.iterationid = " + (activeIterationID-1)
								+ " AND mo1.yearid=mo2.yearid"
								+ " and mo1.monthid=mo2.monthid"
								+ " and mo1.dayid=mo2.dayid"
								+ " and mo1.hourid=mo2.hourid"
								+ " and mo1.stateid=mo2.stateid"
								+ " and mo1.countyid=mo2.countyid"
								+ " and mo1.zoneid=mo2.zoneid"
								+ " and mo1.linkid=mo2.linkid"
								+ " and mo1.pollutantid=mo2.pollutantid"
								+ " and mo1.processid=mo2.processid"
								+ " and mo1.sourcetypeid=mo2.sourcetypeid"
								+ " and mo1.regclassid=mo2.regclassid"
								+ " and mo1.fueltypeid=mo2.fueltypeid"
								+ " and mo1.fuelsubtypeid=mo2.fuelsubtypeid"
								+ " and mo1.modelyearid=mo2.modelyearid"
								+ " and mo1.roadtypeid=mo2.roadtypeid"
								+ " and mo1.scc=mo2.scc",
//								+ " AND mo1.engTechID=mo2.engTechID"
//								+ " AND mo1.sectorID=mo2.sectorID"
//								+ " AND mo1.hpID=mo2.hpID",
						"drop table movesoutputsumtemp",
						"DELETE FROM " + ExecutionRunSpec.getEmissionOutputTable()
								+ " WHERE movesrunid = " + activeRunID
								+ " AND iterationid = " + activeIterationID,
						"INSERT INTO " + ExecutionRunSpec.getEmissionOutputTable()
								+ " (movesrunid, iterationid, yearid, monthid,"
								+ " dayid, hourid, stateid, countyid, zoneid, linkid, pollutantid,"
								+ " processid, sourcetypeid, regclassid, fueltypeid, fuelsubtypeid, modelyearid, roadtypeid,"
								+ " scc,"
//								+ " engTechID,sectorID,hpID,"
								+ " emissionquant, emissionquantmean, emissionquantsigma, "
								+ " emissionquantsum, emissionquantsum2)"
								+ " select movesrunid,"
								+ " iterationid, yearid, monthid, dayid, hourid, stateid,"
								+ " countyid,zoneid, linkid, pollutantid, processid, sourcetypeid, regclassid,"
								+ " fueltypeid, fuelsubtypeid, modelyearid, roadtypeid, scc,"
//								+ " engtechid, sectorid, hpid,"
								+ " emissionquant,"
								+ " emissionquantmean,sqrt((emissionquantsum2-"
								+ "(emissionquantsum*emissionquantsum/iterationid))"
								+ " /(iterationid-1)) as emissionquantsigma,"
								+ " emissionquantsum,emissionquantsum2 from movesoutputsum",
						"DROP TABLE movesoutputsum"
					};
					for(int i=0;i<emissionStatements.length;i++) {
						sql = emissionStatements[i];
						SQLRunner.executeSQL(outputDatabase,sql);
					}

					/*=======================================================
					 * For calculating distance uncertainty when available.
					 *=======================================================
					sql = "CREATE TABLE IF NOT EXISTS MOVESActivityOutputSum SELECT"
							+ " mo2.MOVESRunID, mo2.iterationID, mo2.yearID, mo2.monthID,"
							+ " mo2.dayID, mo2.hourID, mo2.stateID, mo2.countyID, mo2.zoneID,"
							+ " mo2.linkID, mo2.sourceTypeID, mo2.regClassID,"
							+ " mo2.fuelTypeID, mo2.fuelSubTypeID, mo2.modelYearID, mo2.roadTypeID, mo2.SCC,"
							+ " mo2.activityTypeID,"
							+ " mo2.activity,(mo2.activity + mo1.activitySum)/"
							+ " mo2.iterationID AS activityMean,"
							+ " mo2.activity + mo1.activitySum AS activitySum,"
							+ " mo2.activity * mo2.activity + mo1.activitySum2"
							+ " AS activitySum2"
							+ " FROM " + ExecutionRunSpec.getActivityOutputTable() + " AS mo1,"
							+ " " + ExecutionRunSpec.getActivityOutputTable() + " AS mo2"
							+ " WHERE mo2.MOVESRunID = " + activeRunID
							+ " AND mo1.MOVESRunID = mo2.MOVESRunID"
							+ " AND mo2.iterationID = " + activeIterationID
							+ " AND mo1.iterationID= " + (activeIterationID-1) // mo2.iterationID - 1"
							+ " AND mo1.activityTypeID=mo2.activityTypeID"
							+ " AND COALESCE(mo1.yearID,0)=COALESCE(mo2.yearID,0)"
							+ " AND COALESCE(mo1.monthID,0)=COALESCE(mo2.monthID,0)"
							+ " AND COALESCE(mo1.dayID,0)=COALESCE(mo2.dayID,0)"
							+ " AND COALESCE(mo1.hourID,0)=COALESCE(mo2.hourID,0)"
							+ " AND COALESCE(mo1.stateID,0)=COALESCE(mo2.stateID,0)"
							+ " AND COALESCE(mo1.countyID,0)=COALESCE(mo2.countyID,0)"
							+ " AND COALESCE(mo1.zoneID,0)=COALESCE(mo2.zoneID,0)"
							+ " AND COALESCE(mo1.linkID,0)=COALESCE(mo2.linkID,0)"
							+ " AND COALESCE(mo1.sourceTypeID,0)=COALESCE(mo2.sourceTypeID,0)"
							+ " AND COALESCE(mo1.regClassID,0)=COALESCE(mo2.regClassID,0)"
							+ " AND COALESCE(mo1.fuelTypeID,0)=COALESCE(mo2.fuelTypeID,0)"
							+ " AND COALESCE(mo1.fuelSubTypeID,0)=COALESCE(mo2.fuelSubTypeID,0)"
							+ " AND COALESCE(mo1.modelYearID,0)=COALESCE(mo2.modelYearID,0)"
							+ " AND COALESCE(mo1.roadTypeID,0)=COALESCE(mo2.roadTypeID,0)"
							+ " AND COALESCE(mo1.SCC,0)=COALESCE(mo2.SCC,0)";
					SQLRunner.executeSQL(outputDatabase,sql);

					sql = "DELETE FROM " + ExecutionRunSpec.getActivityOutputTable()
							+ " WHERE MOVESRunID = " + activeRunID
							+ " AND iterationID = " + activeIterationID;
					SQLRunner.executeSQL(outputDatabase,sql);

					sql = "INSERT INTO " + ExecutionRunSpec.getActivityOutputTable()
							+ " (MOVESRunID, iterationID, yearID,"
							+ " monthID,"
							+ " dayID, hourID, stateID, countyID, zoneID, linkID,"
							+ " sourceTypeID, regClassID, fuelTypeID, modelYearID, roadTypeID,"
							+ " SCC, activityTypeID, activity, activityMean, activitySigma, "
							+ " activitySum, activitySum2) SELECT MOVESRunID,"
							+ " iterationID, yearID, monthID, dayID, hourID, stateID,"
							+ " countyID, zoneID, linkID, sourceTypeID, regClassID, fuelTypeID,"
							+ " modelYearID, roadTypeID, SCC, activityTypeID,"
							+ " activity, activityMean,"
							+ " SQRT(activitySum2-iterationID*activityMean/"
							+ " (iterationID-1)) AS activitySigma, activitySum,"
							+ " activitySum2 FROM MOVESActivityOutputSum";
					SQLRunner.executeSQL(outputDatabase,sql);

					sql = "DROP TABLE MOVESActivityOutputSum";
					SQLRunner.executeSQL(outputDatabase,sql);
				     *=====================================================*/
				}

				OutputEmissionsBreakdownSelection outputEmissionsBreakdownSelection =
						ExecutionRunSpec.theExecutionRunSpec
						.getOutputEmissionsBreakdownSelection();
				if(!outputEmissionsBreakdownSelection.keepIterations) {
					sql = "DELETE FROM " + ExecutionRunSpec.getEmissionOutputTable()
							+ " WHERE movesrunid = " + activeRunID
							+ " AND iterationid < " + activeIterationID;
					SQLRunner.executeSQL(outputDatabase,sql);

					sql = "DELETE FROM " + ExecutionRunSpec.getActivityOutputTable()
							+ " WHERE movesrunid = " + activeRunID
							+ " AND iterationid < " + activeIterationID;
					SQLRunner.executeSQL(outputDatabase,sql);
				}
			} catch (Exception exception) {
				/**
				 * @explain A database error occurred while processing uncertainty estimation
				 * data.
				**/
				Logger.logError(exception, "Failed to prepare OutputProcessor for estimating"
						+ " uncertainty.");
			} finally {
				if(outputDatabase != null) {
					DatabaseConnectionManager.checkInConnection(
							MOVESDatabaseType.OUTPUT, outputDatabase);
					outputDatabase = null;
				}
			}
		}
	}

	/**
	 * Update MOVESRun.minutesDuration
	**/
	void updateMOVESRun() {
		Connection outputDatabase = null;
		String sql = "";
		try {
			outputDatabase = DatabaseConnectionManager.checkOutConnection(
						MOVESDatabaseType.OUTPUT);
			long elapsedMillis = System.currentTimeMillis()
					- MOVESEngine.theInstance.startTimeMillis;
			double elapsedMinutes = elapsedMillis/1000.0/60.0;
			sql = "update movesrun set minutesduration=" + elapsedMinutes
					+ " where movesrunid=" + MOVESEngine.theInstance.getActiveRunID();
			SQLRunner.executeSQL(outputDatabase, sql);
		} catch (Exception exception) {
			Logger.logSqlError(exception,
					"Failed to update MOVESRun.minutesDuration",sql);
		} finally {
			if(outputDatabase != null) {
				DatabaseConnectionManager.checkInConnection(
						MOVESDatabaseType.OUTPUT, outputDatabase);
				outputDatabase = null;
			}
		}
	}

	/**
	 * Preserve CMITs if the run spec indicates
	**/
	public void saveCMITs() {
		if(!ExecutionRunSpec.shouldCopySavedGeneratorData()) {
			return;
		}
		TreeSetIgnoreCase classesToSaveData = ExecutionRunSpec.getClassesToSaveData();
		if(classesToSaveData.size() <= 0) {
			return;
		}
		Logger.log(LogMessageCategory.INFO,"Saving CMITs...");
		String[] sqlStatements = {
			"gov.epa.otaq.moves.master.implementation.ghg.TotalActivityGenerator",
				"SHO", "SourceHours", "ExtendedIdleHours", "Starts", "StartsPerVehicle", "",
			"gov.epa.otaq.moves.master.implementation.ghg.ProjectTAG",
				"SHO", "SourceHours", "ExtendedIdleHours", "Starts", "StartsPerVehicle", "",
			"gov.epa.otaq.moves.master.implementation.ghg.OperatingModeDistributionGenerator",
				"OpModeDistribution", "",
			"gov.epa.otaq.moves.master.implementation.ghg.LinkOperatingModeDistributionGenerator",
				"OpModeDistribution", "",
			"gov.epa.otaq.moves.master.implementation.ghg.StartOperatingModeDistributionGenerator",
				"OpModeDistribution", "",
			"gov.epa.otaq.moves.master.implementation.ghg.EvaporativeEmissionsOperatingModeDistributionGenerator",
				"OpModeDistribution", "",
			"gov.epa.otaq.moves.master.implementation.ghg.AverageSpeedOperatingModeDistributionGenerator",
				"OpModeDistribution", "",
			"gov.epa.otaq.moves.master.implementation.ghg.SourceBinDistributionGenerator",
				"SourceBinDistribution", "SourceBin", "",
		    "gov.epa.otaq.moves.master.implementation.general.MeteorologyGenerator",
				"ZoneMonthHour", "",
			"gov.epa.otaq.moves.master.implementation.ghg.TankTemperatureGenerator",
				"AverageTankTemperature", "SoakActivityFraction",
				"ColdSoakInitialHourFraction", "ColdSoakTankTemperature",
				"SampleVehicleTripByHour", "HotSoakEventByHour",
				"",
			"gov.epa.otaq.moves.master.implementation.ghg.TankFuelGenerator",
				"AverageTankGasoline", "",
			"gov.epa.otaq.moves.master.implementation.ghg.FuelEffectsGenerator",
				"ATRatio", "criteriaRatio", "GeneralFuelRatio", "",
			"gov.epa.otaq.moves.master.implementation.ghg.MesoscaleLookupTotalActivityGenerator",
				"SHO", "SourceHours", "",
			"gov.epa.otaq.moves.master.implementation.ghg.MesoscaleLookupOperatingModeDistributionGenerator",
				"OpModeDistribution", ""
		};
		DatabaseSelection dbSelection = ExecutionRunSpec.getSavedGeneratorDatabase();
		String sql = "";
		Connection userDB = null;
		Connection executionDB = null;
		String calculatorName = EmissionCalculator.class.getName();
		SQLRunner.Query query = new SQLRunner.Query();
		try {
			userDB = dbSelection.openConnectionOrNull();
			if(userDB == null) {
				if(dbSelection.safeCreateDatabase("database/CreateDefault.sql") == DatabaseSelection.NOT_CREATED) {
					/**
					 * @explain A database error occurred while creating a database to hold
					 * saved generator data.
					**/
					Logger.log(LogMessageCategory.ERROR,
							"Could not create the generator database.");
					return;
				}
				userDB = dbSelection.openConnectionOrNull();
				if(userDB == null) {
					return;
				}
			}
			executionDB = DatabaseConnectionManager.getGUIConnection(MOVESDatabaseType.EXECUTION);

			for(Iterator i=classesToSaveData.iterator();i.hasNext();) {
				String c = (String)i.next();
				if(c.equalsIgnoreCase(calculatorName)) {
					continue;
				}
				for(int j=0;j<sqlStatements.length;j++) {
					if(c.equalsIgnoreCase(sqlStatements[j])) {
						for(j++;j<sqlStatements.length && sqlStatements[j].length() > 0;j++) {
							// Create the table if it doesn't exist.  Doing so allows for
							// tables that aren't in CreateDefault.sql
							try {
								sql = "SHOW CREATE TABLE " + sqlStatements[j];
								query.open(executionDB,sql);
								if(query.rs.next()) {
									String createStatement = StringUtilities.replace(
											query.rs.getString(2),
											"CREATE TABLE `","CREATE TABLE IF NOT EXISTS `") + ";";
									SQLRunner.executeSQL(userDB,createStatement);
								}
							} catch(SQLException e) {
								// Skip the table because it doesn't exist in the execution database.
								// This happens when a required generator was not part of the runspec.
								// As such, the table can be skipped because the generator isn't needed.
								continue;
							} finally {
								query.onFinally();
							}
							// Limit the data with an optional WHERE clause
							String whereClause = getCMITWhereClause(executionDB,c,sqlStatements[j]);
							if(whereClause != null && whereClause.length() > 0) {
								// Try to remove all old data before applying new data.  This
								// is the safest thing to do with distribution data.
								sql = "delete from " + sqlStatements[j]
										+ " where " + whereClause;
								SQLRunner.executeSQL(userDB,sql);
							}
							// Move data
							DatabaseUtilities.replaceIntoTable(executionDB,userDB,sqlStatements[j],
									whereClause,true);
						}
						break;
					}
				}
			}
			/** @nonissue **/
			Logger.log(LogMessageCategory.INFO,"Done saving CMITs.");
		} catch(SQLException e) {
			/** @explain An error occurred while capturing generator data **/
			Logger.logSqlError(e,"Unable to save CMITs",sql);
		} catch(IOException e) {
			/** @explain An error occurred while capturing generator data **/
			Logger.logError(e,"Unable to save CMITs");
		} finally {
			if(userDB != null) {
				DatabaseUtilities.closeConnection(userDB);
			}
			executionDB = null; // does not need to be closed
			query.onFinally();
		}
	}

	/**
	 * Generate a SQL fragment to filter a CMIT during its copy to
	 * another database as part of the Advanced Performance Features.
	 * @param executionDB a connection to the execution database
	 * @param className class that generated the CMIT table
	 * @param tableName CMIT table name
	 * @return SQL fragment suitable to use as with the WHERE word.
	**/
	String getCMITWhereClause(Connection executionDB,String className,String tableName) {
		String whereClause = "";
		if(tableName.equalsIgnoreCase("OpModeDistribution")) {
			String processIDs = "";
			String polProcessIDs = "";
			// The OMDG-family needs OpModeDistribution filtered by processes
			/*
			+-----------+-------------------------+
			| processID | processName             |
			+-----------+-------------------------+
			|         1 | Running Exhaust         |
			|         2 | Start Exhaust           |
			|        90 | Extended Idle Exhaust   |
			|        99 | Well-to-Pump            |
			|         7 | Crankcase               |
			|         9 | Brakewear               |
			|        10 | Tirewear                |
			|        11 | Evap Permeation         |
			|        12 | Evap Fuel Vapor Venting |
			|        13 | Evap Fuel Leaks         |
			|        14 | Evap Non-Fuel Vapors    |
			+-----------+-------------------------+
			*/
			if(className.equalsIgnoreCase("gov.epa.otaq.moves.master.implementation.ghg.OperatingModeDistributionGenerator")) {
				processIDs = "1,9";
			} else if(className.equalsIgnoreCase("gov.epa.otaq.moves.master.implementation.ghg.StartOperatingModeDistributionGenerator")) {
				processIDs = "2";
			} else if(className.equalsIgnoreCase("gov.epa.otaq.moves.master.implementation.ghg.EvaporativeEmissionsOperatingModeDistributionGenerator")) {
				processIDs = "11,12,13,14";
			} else if(className.equalsIgnoreCase("gov.epa.otaq.moves.master.implementation.ghg.MesoscaleLookupOperatingModeDistributionGenerator")) {
				processIDs = "1,9";
			} else if(className.equalsIgnoreCase("gov.epa.otaq.moves.master.implementation.ghg.AverageSpeedOperatingModeDistributionGenerator")) {
				processIDs = "10";
				polProcessIDs = "11710";
			} else if(className.equalsIgnoreCase("gov.epa.otaq.moves.master.implementation.ghg.LinkOperatingModeDistributionGenerator")) {
				processIDs = "1,9";
			}

			String sql = "select polprocessid"
					+ " from pollutantprocessassoc"
					+ " where processid in (" + processIDs + ")";
			if(polProcessIDs.length() > 0) {
				sql += " and polprocessid in (" + polProcessIDs + ")";
			}
			polProcessIDs = "";
			PreparedStatement statement = null;
			ResultSet rs = null;
			try {
				statement = executionDB.prepareStatement(sql);
				rs = SQLRunner.executeQuery(statement, sql);
				while(rs.next()) {
					if(polProcessIDs.length() > 0) {
						polProcessIDs += ",";
					}
					polProcessIDs += rs.getString(1);
				}
				rs.close();
				rs = null;
				statement.close();
				statement = null;
			} catch(SQLException e) {
				Logger.logSqlError(e,"Unable to query PollutantProcessAssoc",sql);
			} finally {
				if(rs != null) {
					try {
						rs.close();
					} catch(Exception e) {
						// Nothing to do here
					}
				}
				if(statement != null) {
					try {
						statement.close();
					} catch(Exception e) {
						// Nothing to do here
					}
				}
			}
			if(polProcessIDs.length() <= 0) {
				polProcessIDs = "0"; // safeguard in case nothing was found
			}
			whereClause = "polprocessid in (" + polProcessIDs + ")";
		}
		return whereClause;
	}
	
	// Parses Manifest context strings
	private int getValue(String str, String id) {
		int index = str.indexOf("|" + id + ":");
		int pipeIndex = str.indexOf("|", index + 1);
		
		String s = str.substring(index + id.length() + 2, pipeIndex);

		if(NumberUtils.isNumber(s)) {
			return Integer.valueOf(s);
		}

		return 0;
	}
}
