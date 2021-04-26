/**************************************************************************************************
 * @(#)InputDataManager.java
 *
 *
 *
 *************************************************************************************************/
package gov.epa.otaq.moves.master.framework;

import gov.epa.otaq.moves.common.*;
import gov.epa.otaq.moves.master.runspec.*;
import java.io.*;
import java.util.*;
import java.sql.*;
import java.lang.*;

/**
 * Moves data from the pristine databases (National, County, and Emission Rate default) to
 * temporary databases (Execution Location Database, Execution Emission Rate Database) used for a
 * single simulation run . The InputDataManager uses the Execution RunSpec criteria to filter
 * the data moved, there by reducing the size of the temporary databases and decreasing the run
 * time.
 *
 * @author		Wesley Faler
 * @author		Sarah Luo, ERG
 * @author		Don Smith
 * @author		Mitch C.
 * @author		Ed Glover William Aikman Mods for NO NO2 SO2
 * @author		Tim Hull
 * @author 		John Covey - Task 1806 changes
 * @version 	2018-03-20
**/
public class InputDataManager {
	/** When copying tables, indicates whether missing tables not in the source
	 * database are allowed and should not cause an error that causes the merge to fail.
	**/
	static boolean allowMissingTables = true;

	/** Table-by-table filter settings **/
	public static String NONROAD_TABLE_FILTER_FILE_NAME = "NonroadTableFilter.csv";

	/** Maximum length of SQL that any portion of a WHERE clause is allowed to be **/
	static final int MAX_SINGLE_CLAUSE_SQL_LENGTH = 7500;

	/** Random number generator for generating uncertainty values **/
	static Random uncertaintyGenerator = new Random();

	/** Folder used for temporary uncertainty files **/
	static File temporaryFolderPath = null;

	/** Active merge session, if startMergeSession() has been used **/
	static MergeSession mergeSession = null;

	/**
	 * Validate that required tables are present. Does not check column names and types.
	 * This uses SchemaInspector.
	 * @param db The database connection to test.
	 * @return Boolean true if the specified database contains Default schema.
	**/
	static public boolean isDefaultSchemaPresent(Connection db) {
		String defaultDatabaseName , dbName ;
		SystemConfiguration scfg = SystemConfiguration.theSystemConfiguration ;

		defaultDatabaseName =
				scfg.databaseSelections[MOVESDatabaseType.DEFAULT.getIndex()].databaseName ;

		try {
			dbName = db.getCatalog() ;
			allowMissingTables = dbName.equalsIgnoreCase( defaultDatabaseName ) ? false : true ;

			return SchemaInspector.isMOVESSchemaPresent(db,false,allowMissingTables );
		} catch( Exception ex ) {
			/*
			System. out.println("Unable to check schema: " + ex.toString());
			ex.printStackTrace();
			*/
			return false ;
		}
	}

	/**
	 * Validate that required tables are present. Does not check column names and types.
	 * This uses SchemaInspector.
	 * @param db The database connection to test.
	 * @return Boolean true if the specified database contains Default schema.
	**/
	/*
	static public boolean isNonRoadDefaultSchemaPresent(Connection db) {
		if(!CompilationFlags.USE_NONROAD) {
			return false;
		}
		String defaultDatabaseName , dbName ;
		SystemConfiguration scfg = SystemConfiguration.theSystemConfiguration ;

		defaultDatabaseName =
				scfg.databaseSelections[MOVESDatabaseType.NRDEFAULT.getIndex()].databaseName ;

		try {
			dbName = db.getCatalog() ;
			allowMissingTables = dbName.equalsIgnoreCase( defaultDatabaseName ) ? false : true ;

			return SchemaInspector.isNonRoadSchemaPresent(db,false,allowMissingTables );
		} catch( Exception ex ) {
			//System. out.println("Unable to check schema: " + ex.toString());
			//ex.printStackTrace();
			return false ;
		}
	}
	*/

	/**
	 * Builds a WHERE clause based on the ExecutionRunSpec's TimeSpan year selections.
	 * @param columnName The name of the column to use in the WHERE clause.  As appropriate,
	 * this could also include a table name qualifier.
	 * @return Vector of String objects each with the SQL WHERE clause as String.
	**/
	static Vector<String> buildSQLWhereClauseForYears(String columnName) {
		Vector<String> sqls = new Vector<String>();
		String sql = "";
		boolean isFirst = true;
		for(Iterator<Integer> iter =
				ExecutionRunSpec.theExecutionRunSpec.years.iterator();
				iter.hasNext(); ) {
			Integer year = (Integer)iter.next();
			if(isFirst) {
				sql += columnName + " IN (";
			} else {
				sql += ",";
			}
			sql += year.intValue();
			isFirst = false;
			// Force each year to be in a separate clause so as to restrict data quantities
			// moved in each query.  This speeds up MySQL
			sql += ")";
			sqls.add(sql);
			sql = new String();
			isFirst = true;
		}
		if(!isFirst) {
			sql += ")";
			sqls.add(sql);
		}
		return sqls;
	}

	/**
	 * Builds a WHERE clause for fuelYearIDs within the years of the current runspec.
	 * @param columnName The name of the column to use in the WHERE clause.  As appropriate,
	 * this could also include a table name qualifier.
	 * @return Vector of String objects each with the SQL WHERE clause as String.
	**/
	static Vector<String> buildSQLWhereClauseForFuelYears(String columnName) {
		Vector<String> sqls = new Vector<String>();
		String sql = "";
		boolean isFirst = true;
		for(Iterator<Integer> iter =
				ExecutionRunSpec.theExecutionRunSpec.fuelYears.iterator();
				iter.hasNext(); ) {
			Integer fuelYear = (Integer)iter.next();
			if(isFirst) {
				sql += columnName + " IN (";
			} else {
				sql += ",";
			}
			sql += fuelYear.intValue();
			isFirst = false;
			// Force each fuel year to be in a separate clause so as to restrict data quantities
			// moved in each query.  This speeds up MySQL
			sql += ")";
			sqls.add(sql);
			sql = new String();
			isFirst = true;
		}
		if(!isFirst) {
			sql += ")";
			sqls.add(sql);
		}
		return sqls;
	}

	/**
	 * Builds a modelYearID WHERE clause based on the ExecutionRunSpec's TimeSpan year selections.
	 * @param columnName The name of the column to use in the WHERE clause.  As appropriate,
	 * this could also include a table name qualifier.
	 * @return Vector of String objects each with the SQL WHERE clause as String.
	**/
	static Vector<String> buildSQLWhereClauseForModelYears(String columnName) {
		// Because model years will overlap if done for each calendar year, they must
		// be OR'd together in one big query. For calendar years 2010, 2012, and 2000:
		// ( (modelYearID <= 2010 and modelYearID >= 1980)
		//   or (modelYearID <= 2012 and modelYearID >= 1982)
		//   or (modelYearID <= 2000 and modelYearID >= 1970) )
		// It is believed the above SQL will be smaller than explicitly listing all
		// model years.
		Vector<String> sqls = new Vector<String>();
		String sql = "(";
		boolean hasData = false;
		for(Iterator<Integer> iter =
				ExecutionRunSpec.theExecutionRunSpec.years.iterator();
				iter.hasNext(); ) {
			Integer year = (Integer)iter.next();
			if(hasData) {
				sql += " or ";
			}
			sql += "(" + columnName + "<=" + year + " and " + columnName + ">=" + (year.intValue()-30) + ")";
			hasData = true;
		}
		if(hasData) {
			sql += ")";
			sqls.add(sql);
		}
		return sqls;
	}

	/**
	 * Check a table for presence of data.
	 * @param destination database connection
	 * @param tableName table to check
	 * @return true if there is any data present in the table
	**/
	static boolean hasExistingData(Connection destination, String tableName) {
		String sql = "select * from " + tableName + " limit 1";
		SQLRunner.Query query = new SQLRunner.Query();
		try {
			query.open(destination,sql);
			return query.rs.next();
		} catch(Exception e) {
			Logger.logError(e,"Unable to check for data in " + tableName + " with " + sql);
			return false;
		} finally {
			query.onFinally();
		}
	}

	/**
	 * Builds a WHERE clause based on (fuelRegionID, fuelYearID, monthGroupID) combinations
	 * that exist in the destination database's fuelSupply table.  A clause is created
	 * for each countyID.
	 * @return Vector of String objects each with the SQL WHERE clause as String.
	**/
	static Vector<String> buildSQLWhereClauseForFuelSupply(Connection destination) {
		//System.out.println("buildSQLWhereClauseForFuelSupply...");
		/*
		Example clause, one clause per fuelRegionID:
			not (fuelRegionID=12345 and (
					(fuelYearID=2010 and monthGroupID in (1,2,3))
				or	(fuelYearID=2011 and monthGroupID in (2,4))
				))
		*/
		Vector<String> sqls = new Vector<String>();
		String searchSQL = "select distinct fuelregionid, fuelyearid, monthgroupid"
				+ " from fuelsupply"
				+ " order by fuelregionid, fuelyearid, monthgroupid";
		SQLRunner.Query query = new SQLRunner.Query();
		String sql = "";
		try {
			// If all counties have the same fuel years and month groups, a fast
			// SQL statement can be used.
			boolean hasData = false;
			boolean allCountiesMatch = true;

			query.open(destination,searchSQL);
			String priorFuelRegionID = "";
			String countyParameters = "";
			String referenceParameters = "";
			while(query.rs.next()) {
				hasData = true;
				String fuelRegionID = query.rs.getString("fuelregionid");
				String fuelYearID = query.rs.getString("fuelyearid");
				String monthGroupID = query.rs.getString("monthgroupid");
				if(priorFuelRegionID.length() > 0 && !priorFuelRegionID.equals(fuelRegionID)) {
					if(referenceParameters.length() <= 0) {
						referenceParameters = countyParameters;
					} else if(!referenceParameters.equals(countyParameters)) {
						allCountiesMatch = false;
						break;
					}
					countyParameters = "";
				}
				priorFuelRegionID = fuelRegionID;
				countyParameters += fuelYearID + "|" + monthGroupID + "-";
			}
			query.close();
			if(!hasData) {
				return sqls;
			}

			priorFuelRegionID = "";
			query.open(destination,searchSQL);
			String wholeClause = "";
			if(allCountiesMatch) {
				TreeSet<String> fuelRegionIDs = new TreeSet<String>();
				TreeSet<String> fuelYearIDs = new TreeSet<String>();
				TreeSet<String> monthGroupIDs = new TreeSet<String>();
				while(query.rs.next()) {
					fuelRegionIDs.add(query.rs.getString("fuelregionid"));
					fuelYearIDs.add(query.rs.getString("fuelyearid"));
					monthGroupIDs.add(query.rs.getString("monthgroupid"));
				}
				wholeClause = "(not (fuelregionid in (" + StringUtilities.getCSV(fuelRegionIDs) + ")"
						+ " and fuelyearid in (" + StringUtilities.getCSV(fuelYearIDs) + ")"
						+ " and monthgroupid in (" + StringUtilities.getCSV(monthGroupIDs) + ")))";
			} else {
				String priorFuelYearID = "";
				String monthGroupIDs = "";
				String clause = "";
				while(query.rs.next()) {
					String fuelRegionID = query.rs.getString("fuelregionid");
					String fuelYearID = query.rs.getString("fuelyearid");
					String monthGroupID = query.rs.getString("monthgroupid");
					if(!fuelRegionID.equals(priorFuelRegionID)) {
						if(clause.length() > 0) {
							wholeClause = addToWhereClause(wholeClause,clause + monthGroupIDs + ")))");
							// sqls.add(clause + monthGroupIDs + ")))");
							// System.out.println(clause + monthGroupIDs + ")))");
						}
						priorFuelRegionID = fuelRegionID;
						priorFuelYearID = fuelYearID;
						monthGroupIDs = monthGroupID;
						clause = "not (fuelregionid=" + fuelRegionID + " and (fuelyearid=" + fuelYearID + " and monthgroupid in (";
					} else if(!fuelYearID.equals(priorFuelYearID)) {
						clause += monthGroupIDs + ")) or (fuelyearid=" + fuelYearID + " and monthgroupid in (";
						monthGroupIDs = monthGroupID;
						priorFuelYearID = fuelYearID;
					} else {
						monthGroupIDs += "," + monthGroupID;
					}
				}
				if(clause.length() > 0) {
					wholeClause = addToWhereClause(wholeClause,clause + monthGroupIDs + ")))");
					// sqls.add(clause + monthGroupIDs + ")))");
					// System.out.println(clause + monthGroupIDs + ")))");
				}
			}
			sqls.add(wholeClause);
		} catch(Exception e) {
			Logger.logError(e,"Unable to build fuel supply clauses using: " + searchSQL);
		} finally {
			query.onFinally();
		}
		return sqls;
	}

	/**
	 * Builds a WHERE clause based on the ExecutionRunSpec's TimeSpan month selections.
	 * @param columnName The name of the column to use in the WHERE clause.  As appropriate,
	 * this could also include a table name qualifier.
	 * @return Vector of String objects each with the SQL WHERE clause as String.
	**/
	static Vector<String> buildSQLWhereClauseForMonths(String columnName) {
		Vector<String> sqls = new Vector<String>();
		String sql = "";
		boolean isFirst = true;
		for(Iterator<Integer> iter =
				ExecutionRunSpec.theExecutionRunSpec.months.iterator();
				iter.hasNext(); ) {
			Integer month = (Integer)iter.next();
			if(isFirst) {
				sql += columnName + " IN (";
			} else {
				sql += ",";
			}
			sql += month.intValue();
			isFirst = false;
		}
		if(!isFirst) {
			sql += ")";
			sqls.add(sql);
		}
		return sqls;
	}

	/**
	 * Builds a WHERE clause based on the ExecutionRunSpec's TimeSpan day selections.
	 * @param columnName The name of the column to use in the WHERE clause.  As appropriate,
	 * this could also include a table name qualifier.
	 * @return Vector of String objects each with the SQL WHERE clause as String.
	**/
	static Vector<String> buildSQLWhereClauseForDays(String columnName) {
		Vector<String> sqls = new Vector<String>();
		String sql = "";
		boolean isFirst = true;
		for(Iterator<Integer> iter =
				ExecutionRunSpec.theExecutionRunSpec.days.iterator();
				iter.hasNext(); ) {
			Integer day = (Integer)iter.next();
			if(isFirst) {
				sql += columnName + " IN (";
			} else {
				sql += ",";
			}
			sql += day.intValue();
			isFirst = false;
		}
		if(!isFirst) {
			sql += ")";
			sqls.add(sql);
		}
		return sqls;
	}

	/**
	 * Builds a WHERE clause based on the ExecutionRunSpec's TimeSpan hour selections.
	 * @param columnName The name of the column to use in the WHERE clause.  As appropriate,
	 * this could also include a table name qualifier.
	 * @return Vector of String objects each with the SQL WHERE clause as String.
	**/
	static Vector<String> buildSQLWhereClauseForHours(String columnName) {
		Vector<String> sqls = new Vector<String>();
		//TreeSet hours = new TreeSet();
		String sql = "";
		boolean isFirst = true;
		for(Iterator<Integer> iter =
				ExecutionRunSpec.theExecutionRunSpec.hours.iterator();
				iter.hasNext(); ) {
			Integer hour = (Integer)iter.next();
			if(isFirst) {
				sql += columnName + " IN (";
			} else {
				sql += ",";
			}
			sql += hour.intValue();
			isFirst = false;
		}
		if(!isFirst) {
			sql += ")";
			sqls.add(sql);
		}
		return sqls;
	}

	/**
	 * Builds a WHERE clause based on the ExecutionRunSpec's GeographicSelection link selections.
	 * @param columnName The name of the column to use in the WHERE clause.  As appropriate,
	 * this could also include a table name qualifier.
	 * @return Vector of String objects each with the SQL WHERE clause as String.
	**/
	static Vector<String> buildSQLWhereClauseForLinks(String columnName) {
		Vector<String> sqls = new Vector<String>();
		TreeSet<Integer> linkIDs = new TreeSet<Integer>();
		for(Iterator<ExecutionLocation> iter =
				ExecutionRunSpec.theExecutionRunSpec.executionLocations.iterator();
				iter.hasNext(); ) {
			ExecutionLocation location = (ExecutionLocation)iter.next();
			linkIDs.add(Integer.valueOf(location.linkRecordID));
		}
		String sql = "";
		boolean isFirst = true;
		for(Iterator<Integer> iter=linkIDs.iterator();iter.hasNext();) {
			Integer id = (Integer)iter.next();
			if(isFirst) {
				sql += columnName + " IN (";
			} else {
				sql += ",";
			}
			sql += id;
			isFirst = false;
			if(sql.length() >= MAX_SINGLE_CLAUSE_SQL_LENGTH) {
				sql += ")";
				sqls.add(sql);
				sql = new String();
				isFirst = true;
			}
		}
		if(sql.length() > 0) {
			sql += ")";
			sqls.add(sql);
		}
		return sqls;
	}

	/**
	 * Builds a WHERE clause based on the ExecutionRunSpec's GeographicSelection zone selections.
	 * @param columnName The name of the column to use in the WHERE clause.  As appropriate,
	 * this could also include a table name qualifier.
	 * @return Vector of String objects each with the SQL WHERE clause as String.
	**/
	static Vector<String> buildSQLWhereClauseForZones(String columnName) {
		Vector<String> sqls = new Vector<String>();
		TreeSet<Integer> zoneIDs = new TreeSet<Integer>();
		String sql = "";
		for(Iterator<ExecutionLocation> iter =
				ExecutionRunSpec.theExecutionRunSpec.executionLocations.iterator();
				iter.hasNext(); ) {
			ExecutionLocation location = (ExecutionLocation)iter.next();
			zoneIDs.add(Integer.valueOf(location.zoneRecordID));
		}
		boolean isFirst = true;
		for(Iterator<Integer> iter=zoneIDs.iterator();iter.hasNext();) {
			Integer id = (Integer)iter.next();
			if(isFirst) {
				sql += columnName + " IN (";
			} else {
				sql += ",";
			}
			sql += id;
			isFirst = false;
			if(sql.length() >= MAX_SINGLE_CLAUSE_SQL_LENGTH) {
				sql += ")";
				sqls.add(sql);
				sql = new String();
				isFirst = true;
			}
		}
		if(sql.length() > 0) {
			sql += ")";
			sqls.add(sql);
		}
		return sqls;
	}

	/**
	 * Builds a WHERE clause based on the ExecutionRunSpec's PollutantProcessAssociations
	 * pollutant selections.
	 * @param columnName The name of the column to use in the WHERE clause.  As appropriate,
	 * this could also include a table name qualifier.
	 * @return Vector of String objects each with the SQL WHERE clause as String.
	**/
	static Vector<String> buildSQLWhereClauseForPollutants(String columnName) {
		Vector<String> sqls = new Vector<String>();
		String sql = "";
		boolean isFirst = true;

		if(ExecutionRunSpec.theExecutionRunSpec.getOutputVMTData()
				&& !ExecutionRunSpec.theExecutionRunSpec.doesHaveDistancePollutantAndProcess()) {
			// Add THC to the pollutants that we bring over
			Pollutant p = Pollutant.findByID(1);
			if(p != null) {
				isFirst = false;
				sql += columnName + " IN (" + p.databaseKey;
			}
		}

		for(Iterator<PollutantProcessAssociation> iter =
				ExecutionRunSpec.theExecutionRunSpec.pollutantProcessAssociations.iterator();
				iter.hasNext(); ) {
			PollutantProcessAssociation selection = (PollutantProcessAssociation)iter.next();
			if(isFirst) {
				sql += columnName + " IN (";
			} else {
				sql += ",";
			}
			sql += selection.pollutant.databaseKey;
			isFirst = false;
		}
		if(!isFirst) {
			sql += ")";
			sqls.add(sql);
		}
		return sqls;
	}

	/**
	 * Builds a WHERE clause based on the ExecutionRunSpec's PollutantProcessAssociations
	 * emission process selections.
	 * @param columnName The name of the column to use in the WHERE clause.  As appropriate,
	 * this could also include a table name qualifier.
	 * @return Vector of String objects each with the SQL WHERE clause as String.
	**/
	static Vector<String> buildSQLWhereClauseForProcesses(String columnName) {
		Vector<String> sqls = new Vector<String>();
		String sql = "";
		TreeSet<EmissionProcess> processes = new TreeSet<EmissionProcess>();
		boolean isFirst = true;

		if(ExecutionRunSpec.theExecutionRunSpec.getOutputVMTData()
				&& !ExecutionRunSpec.theExecutionRunSpec.doesHaveDistancePollutantAndProcess()) {
			// Add Running Exhaust to the processes that we bring over
			EmissionProcess p = EmissionProcess.findByID(1);
			if(p != null) {
				isFirst = false;
				sql += columnName + " IN (" + p.databaseKey;
			}
		}

		for(Iterator<PollutantProcessAssociation> iter =
				ExecutionRunSpec.theExecutionRunSpec.pollutantProcessAssociations.iterator();
				iter.hasNext(); ) {
			PollutantProcessAssociation selection = (PollutantProcessAssociation)iter.next();
			if(!processes.contains(selection.emissionProcess)) {
				if(isFirst) {
					sql += columnName + " IN (";
				} else {
					sql += ",";
				}
				sql += selection.emissionProcess.databaseKey;
				isFirst = false;
				processes.add(selection.emissionProcess);
			}
		}
		if(!isFirst) {
			sql += ")";
			sqls.add(sql);
		}
		return sqls;
	}

	/**
	 * Builds a WHERE clause based on the ExecutionRunSpec's GeographicSelection
	 * county selections.
	 * @param columnName The name of the column to use in the WHERE clause.  As appropriate,
	 * this could also include a table name qualifier.
	 * @return Vector of String objects each with the SQL WHERE clause as String.
	**/
	static Vector<String> buildSQLWhereClauseForCounties(String columnName) {
		Vector<String> sqls = new Vector<String>();
		TreeSet<Integer> countyIDs = new TreeSet<Integer>();
		for(Iterator<ExecutionLocation> iter =
				ExecutionRunSpec.theExecutionRunSpec.executionLocations.iterator();
				iter.hasNext(); ) {
			ExecutionLocation location = (ExecutionLocation)iter.next();
			countyIDs.add(Integer.valueOf(location.countyRecordID));
		}
		boolean isFirst = true;;
		String sql = "";
		for(Iterator<Integer> iter=countyIDs.iterator();iter.hasNext();) {
			Integer id = (Integer)iter.next();
			if(isFirst) {
				sql += columnName + " IN (";
			} else {
				sql += ",";
			}
			sql += id;
			isFirst = false;
			if(sql.length() >= MAX_SINGLE_CLAUSE_SQL_LENGTH) {
				sql += ")";
				sqls.add(sql);
				sql = new String();
				isFirst = true;
			}
		}
		if(countyIDs.size() != 0) {
			sql += ")";
			sqls.add(sql);
		}
		return sqls;
	}

	/**
	 * Builds a WHERE clause based on the regions of ExecutionRunSpec's GeographicSelection
	 * county selections.
	 * @param columnName The name of the column to use in the WHERE clause.  As appropriate,
	 * this could also include a table name qualifier.
	 * @return Vector of String objects each with the SQL WHERE clause as String.
	**/
	static Vector<String> buildSQLWhereClauseForRegions(String columnName) {
		Vector<String> sqls = new Vector<String>();
		boolean isFirst = true;;
		String sql = "";
		for(Iterator<Integer> iter=ExecutionRunSpec.theExecutionRunSpec.regions.iterator();iter.hasNext();) {
			Integer id = (Integer)iter.next();
			if(isFirst) {
				sql += columnName + " IN (";
			} else {
				sql += ",";
			}
			sql += id;
			isFirst = false;
			if(sql.length() >= MAX_SINGLE_CLAUSE_SQL_LENGTH) {
				sql += ")";
				sqls.add(sql);
				sql = new String();
				isFirst = true;
			}
		}
		if(ExecutionRunSpec.theExecutionRunSpec.regions.size() != 0) {
			sql += ")";
			sqls.add(sql);
		}
		return sqls;
	}

	/**
	 * Builds a WHERE clause based on the ExecutionRunSpec's GeographicSelection
	 * state selections.
	 * @param columnName The name of the column to use in the WHERE clause.  As appropriate,
	 * this could also include a table name qualifier.
	 * @return Vector of String objects each with the SQL WHERE clause as String.
	**/
	static Vector<String> buildSQLWhereClauseForStates(String columnName) {
		Vector<String> sqls = new Vector<String>();
		TreeSet<Integer> stateIDs = new TreeSet<Integer>();
		for(Iterator<ExecutionLocation> iter =
				ExecutionRunSpec.theExecutionRunSpec.executionLocations.iterator();
				iter.hasNext(); ) {
			ExecutionLocation location = (ExecutionLocation)iter.next();
			stateIDs.add(Integer.valueOf(location.stateRecordID));
		}
		String sql = "";
		boolean isFirst = true;
		for(Iterator<Integer> iter=stateIDs.iterator();iter.hasNext();) {
			Integer id = (Integer)iter.next();
			if(isFirst) {
				sql = columnName + " IN (";
			} else {
				sql += ",";
			}
			sql += id;
			isFirst = false;
			if(sql.length() >= MAX_SINGLE_CLAUSE_SQL_LENGTH) {
				sql += ")";
				sqls.add(sql);
				sql = new String();
				isFirst = true;
			}
		}
		if(stateIDs.size() != 0) {
			sql += ")";
			sqls.add(sql);
		}
		return sqls;
	}

	/**
	 * Builds a WHERE clause based on the RunSpec's HourDayIDs
	 * selections.
	 * @param columnName The name of the column to use in the WHERE clause.  As appropriate,
	 * this could also include a table name qualifier.
	 * @return Vector of String objects each with the SQL WHERE clause as String.
	**/
	static Vector<String> buildSQLWhereClauseForHourDayIDs(String columnName) {
		Vector<String> sqls = new Vector<String>();
		String sql = "";
		ResultSet results = null;
		Connection defaultDB = null;
		TreeSet<Integer> hourDayIDs = new TreeSet<Integer>();
		boolean isFirst = true;
		try {
			defaultDB = DatabaseConnectionManager.checkOutConnection(
					MOVESDatabaseType.DEFAULT);
			for(Iterator<Integer> d =
					ExecutionRunSpec.theExecutionRunSpec.days.iterator();
					d.hasNext(); ) {
				Integer day = (Integer)d.next();
				for(Iterator<Integer> h =
						ExecutionRunSpec.theExecutionRunSpec.hours.iterator();
						h.hasNext(); ) {
					Integer hour = (Integer)h.next();
					if(sql.length()==0) {
						sql = "SELECT hourdayid from hourday WHERE ";
					} else {
						sql += " OR ";
					}
					sql += "(dayid=" + day + " AND hourid=" + hour + ")";
				}
			}
			if(sql !="") {
				results = SQLRunner.executeQuery(defaultDB,sql);
				hourDayIDs.clear();
				while(results.next()) {
					hourDayIDs.add(Integer.valueOf(results.getInt(1)));
				}
				sql = "";
				for(Iterator<Integer> iter = hourDayIDs.iterator();iter.hasNext();) {
					Integer hourDayIDInteger = (Integer) iter.next();
					if(isFirst) {
						sql += columnName + " IN (";
					} else {
						sql += ",";
					}
					sql +=  hourDayIDInteger.intValue();
					isFirst = false;
				}
			}
		} catch(Exception e) {
			/** @explain A connection to the default database could not be established. **/
			Logger.logError(e,"Unable to get the Default Database connection in"
					+ " InputDataManager for HourDayIDs.");
			return null;
		} finally {
			if(results != null) {
				try {
					results.close();
				} catch(Exception e) {
					// Nothing can be done here
				}
				results = null;
			}
			if(defaultDB != null) {
				DatabaseConnectionManager.checkInConnection(MOVESDatabaseType.DEFAULT,
						defaultDB);
				defaultDB = null;
			}
		}
		if(!isFirst) {
			sql += ")";
			sqls.add(sql);
		}
		return sqls;
	}

	/**
	 * Builds a WHERE clause based on the ExecutionRunSpec's Road Types
	 * selections.
	 * @param columnName The name of the column to use in the WHERE clause.  As appropriate,
	 * this could also include a table name qualifier.
	 * @return Vector of String objects each with the SQL WHERE clause as String.
	**/
	static Vector<String> buildSQLWhereClauseForRoadTypes(String columnName) {
		Vector<String> sqls = new Vector<String>();
		String sql = "";
		boolean isFirst = true;
		for(Iterator<RoadType> iter =
				ExecutionRunSpec.theExecutionRunSpec.getRoadTypes().iterator();
				iter.hasNext(); ) {
			RoadType roadTypeSelection = (RoadType)iter.next();
			Integer roadTypeInteger = Integer.valueOf(roadTypeSelection.roadTypeID);
			if(isFirst) {
				sql += columnName + " IN (";
			} else {
				sql += ",";
			}
			sql += roadTypeInteger.intValue();
			isFirst = false;
		}
		if(!isFirst) {
			sql += ")";
			sqls.add(sql);
		}
		return sqls;
	}

	/**
	 * Builds a WHERE clause based on the ExecutionRunSpec's Pollutant - Process
	 * selections.
	 * @param columnName The name of the column to use in the WHERE clause.  As appropriate,
	 * this could also include a table name qualifier.
	 * @return Vector of String objects each with the SQL WHERE clause as String.
	**/
	static Vector<String> buildSQLWhereClauseForPollutantProcessIDs(String columnName) {
		Vector<String> sqls = new Vector<String>();
		String sql = columnName + " < 0"; // always bring in representing pollutant/processes
		boolean isFirst = true;

		if(ExecutionRunSpec.theExecutionRunSpec.getOutputVMTData()
				&& !ExecutionRunSpec.theExecutionRunSpec.doesHaveDistancePollutantAndProcess()) {
			// Add THC/RunningExhaust to the pollutants that we bring over
			isFirst = false;
			sql += " OR " + columnName + " IN (101";
		}

		Connection defaultDB = null;
		try {
			defaultDB = DatabaseConnectionManager.checkOutConnection(
					MOVESDatabaseType.DEFAULT);
			for(Iterator<PollutantProcessAssociation> iter =
					ExecutionRunSpec.theExecutionRunSpec.pollutantProcessAssociations.iterator();
					iter.hasNext(); ) {
				PollutantProcessAssociation selection = (PollutantProcessAssociation)iter.next();
				int polProcessID = selection.getDatabaseKey(defaultDB);
				if(isFirst) {
					sql += " OR " + columnName + " IN (";
				} else {
					sql += ",";
				}
				sql += polProcessID;
				
				// NonECNonSO4PM (120) is silently made from NonECPM (118), so include 120 whenever 118 is needed.
				if((int)(polProcessID / 100) == 118) {
					int newValue = 120*100 + (polProcessID % 100);
					sql += "," + newValue;
				}
				isFirst = false;
			}
		} catch(Exception e) {
			/** @explain A connection to the default database could not be established. **/
			Logger.logError(e,"Unable to get the Default Database connection in"
					+ " InputDataManager for PollutantProcessIDs.");
			return null;
		} finally {
			if(defaultDB != null) {
				DatabaseConnectionManager.checkInConnection(MOVESDatabaseType.DEFAULT,
						defaultDB);
				defaultDB = null;
			}
		}
		if(!isFirst) {
			sql += ")";
			sqls.add(sql);
		}
		return sqls;
	}

	/**
	 * Builds a WHERE clause based on the ExecutionRunSpec's Source
	 * Use Type selections.
	 * @param columnName The name of the column to use in the WHERE clause.  As appropriate,
	 * this could also include a table name qualifier.
	 * @return Vector of String objects each with the SQL WHERE clause as String.
	**/
	static Vector<String> buildSQLWhereClauseForSourceUseTypes(String columnName) {
		Vector<String> sqls = new Vector<String>();
		String sql = "";
		TreeSet<Integer> sourceTypes = new TreeSet<Integer>();
		for(Iterator<OnRoadVehicleSelection> iter =
				ExecutionRunSpec.theExecutionRunSpec.getOnRoadVehicleSelections().iterator();
				iter.hasNext(); ) {
			OnRoadVehicleSelection selection = (OnRoadVehicleSelection)iter.next();
			Integer sourceTypeInteger = Integer.valueOf(selection.sourceTypeID);
			if(!sourceTypes.contains(sourceTypeInteger)) {
				if(sourceTypes.size() == 0) {
					sql += columnName + " IN (";
				} else {
					sql += ",";
				}
				sql += sourceTypeInteger.intValue();
				sourceTypes.add(sourceTypeInteger);
			}
		}
		if(sourceTypes.size() != 0) {
			sql += ")";
			sqls.add(sql);
		}
		return sqls;
	}

	/**
	 * Builds a WHERE clause based on the ExecutionRunSpec's NonRoad Source Use Type selections.
	 * @param columnName The name of the column to use in the WHERE clause.  As appropriate,
	 * this could also include a table name qualifier.
	 * @return Vector of String objects each with the SQL WHERE clause as String.
	**/
	static Vector<String> buildSQLWhereClauseForNonRoadSourceUseTypes(String columnName) {
		Vector<String> sqls = new Vector<String>();
		// Find the source types that relate through NRSCC and NREquipmentType
		// based upon fuel/sector combinations in the RunSpec.
		String clauses = "";
		for(Iterator<OffRoadVehicleSelection> iter =
				ExecutionRunSpec.theExecutionRunSpec.getOffRoadVehicleSelections().iterator();
				iter.hasNext(); ) {
			OffRoadVehicleSelection selection = iter.next();
			String t = "(et.sectorID=" + selection.sectorID + " and s.fuelTypeID=" + selection.fuelTypeID + ")";
			if(clauses.length() > 0) {
				clauses += " or ";
			}
			clauses += t;
		}
		String sql = "select distinct sourcetypeid"
			+ " from nrsourceusetype sut"
			+ " inner join nrscc s on (s.scc=sut.scc)"
			+ " inner join nrequipmenttype et on (et.nrequiptypeid=s.nrequiptypeid)"
			+ " where (" + clauses + ")";
		Connection nrDefaultDatabase = null;
		String resultSQL = "";
		boolean isFirst = true;
		SQLRunner.Query query = new SQLRunner.Query();
		try {
			nrDefaultDatabase = DatabaseConnectionManager.checkOutConnection(
					MOVESDatabaseType.DEFAULT);
			isFirst = true;
			query.open(nrDefaultDatabase,sql);
			while(query.rs.next()) {
				if(isFirst) {
					resultSQL += columnName + " IN (";
				} else {
					resultSQL += ",";
				}
				resultSQL += query.rs.getString("sourcetypeid");
				isFirst = false;
			}
			query.close();
		} catch(Exception e) {
			//do nothing
		} finally {
			query.onFinally();
			if(nrDefaultDatabase != null) {
				DatabaseConnectionManager.checkInConnection(MOVESDatabaseType.DEFAULT,
						nrDefaultDatabase);
				nrDefaultDatabase = null;
			}
		}
		if(!isFirst) {
			resultSQL += ")";
			sqls.add(resultSQL);
		}
		return sqls;
	}

	/**
	 * Builds a WHERE clause based on the ExecutionRunSpec's Source Fuel
	 * Type selections.
	 * @param columnName The name of the column to use in the WHERE clause.  As appropriate,
	 * this could also include a table name qualifier.
	 * @return Vector of String objects each with the SQL WHERE clause as String.
	**/
	static Vector<String> buildSQLWhereClauseForFuelTypes(String columnName) {
		Vector<String> sqls = new Vector<String>();
		String sql = "";
		TreeSet<Integer> fuelTypes = new TreeSet<Integer>();
		for(Iterator<OnRoadVehicleSelection> iter =
				ExecutionRunSpec.theExecutionRunSpec.getOnRoadVehicleSelections().iterator();
				iter.hasNext(); ) {
			OnRoadVehicleSelection selection = (OnRoadVehicleSelection)iter.next();
			Integer fuelTypeInteger = Integer.valueOf(selection.fuelTypeID);
			if(!fuelTypes.contains(fuelTypeInteger)) {
				if(fuelTypes.size() == 0) {
					sql += columnName + " IN (";
				} else {
					sql += ",";
				}
				sql +=  fuelTypeInteger.intValue();
				fuelTypes.add(fuelTypeInteger);
			}
		}
		for(Iterator<OffRoadVehicleSelection> iter =
				ExecutionRunSpec.theExecutionRunSpec.getOffRoadVehicleSelections().iterator();
				iter.hasNext(); ) {
			OffRoadVehicleSelection selection = iter.next();
			Integer fuelTypeInteger = Integer.valueOf(selection.fuelTypeID);
			if(!fuelTypes.contains(fuelTypeInteger)) {
				if(fuelTypes.size() == 0) {
					sql += columnName + " IN (";
				} else {
					sql += ",";
				}
				sql +=  fuelTypeInteger.intValue();
				fuelTypes.add(fuelTypeInteger);
			}
		}
		if(fuelTypes.size() != 0) {
			sql += ")";
			sqls.add(sql);
		}
		return sqls;
	}

	/**
	 * Builds a WHERE clause based on the ExecutionRunSpec's Source Fuel
	 * Type selections.
	 * @param columnName The name of the column to use in the WHERE clause.  As appropriate,
	 * this could also include a table name qualifier.
	 * @return Vector of String objects each with the SQL WHERE clause as String.
	**/
	static Vector<String> buildSQLWhereClauseForNonroadFuelTypes(String columnName) {
		Vector<String> sqls = new Vector<String>();
		String sql = "";
		TreeSet<Integer> fuelTypes = new TreeSet<Integer>();
		for(Iterator<OffRoadVehicleSelection> iter =
				ExecutionRunSpec.theExecutionRunSpec.getOffRoadVehicleSelections().iterator();
				iter.hasNext(); ) {
			OffRoadVehicleSelection selection = iter.next();
			Integer fuelTypeInteger = Integer.valueOf(selection.fuelTypeID);
			if(!fuelTypes.contains(fuelTypeInteger)) {
				if(fuelTypes.size() == 0) {
					sql += columnName + " IN (";
				} else {
					sql += ",";
				}
				sql +=  fuelTypeInteger.intValue();
				fuelTypes.add(fuelTypeInteger);
			}
		}
		if(fuelTypes.size() != 0) {
			sql += ")";
			sqls.add(sql);
		}
		return sqls;
	}

	/**
	 * Builds a WHERE clause based on the ExecutionRunSpec's
	 * Month Group ID selections.
	 * @param columnName The name of the column to use in the WHERE clause.  As appropriate,
	 * this could also include a table name qualifier.
	 * @return Vector of String objects each with the SQL WHERE clause as String.
	**/
	static Vector<String> buildSQLWhereClauseForMonthGroupIDs(String columnName) {
		Vector<String> sqls = new Vector<String>();
		String sql = "";
		TreeSet<Integer> monthGroupIDs = new TreeSet<Integer>();
		Connection defaultDB = null;
		ResultSet results = null;
		boolean isFirst = true;
		String monthIDs[] = new String[12];
		ExecutionRunSpec executionRunSpec = ExecutionRunSpec.theExecutionRunSpec;
		if(executionRunSpec.monthGroups.size() > 0) {
			monthGroupIDs.addAll(executionRunSpec.monthGroups);
		} else {
			try {
				defaultDB = DatabaseConnectionManager.checkOutConnection(
						MOVESDatabaseType.DEFAULT);
				monthGroupIDs.clear();
				sql = "";
				for(Iterator<Integer> iter =
						ExecutionRunSpec.theExecutionRunSpec.months.iterator();
						iter.hasNext(); ) {
					Integer month = (Integer)iter.next();
					sql = "SELECT DISTINCT monthgroupid from monthofanyyear where "
							+ " monthid =" + month.intValue();
					results = SQLRunner.executeQuery(defaultDB, sql);
					while (results.next()) {
						monthGroupIDs.add(Integer.valueOf(results.getInt("monthgroupid")));
					}
				}
			} catch(Exception e) {
				//do nothing
			} finally {
				if(results != null) {
					try {
						results.close();
					} catch(Exception e) {
						// Nothing can be done here
					}
					results = null;
				}
				if(defaultDB != null) {
					DatabaseConnectionManager.checkInConnection(MOVESDatabaseType.DEFAULT,
							defaultDB);
					defaultDB = null;
				}
			}
		}
		sql = "";
		isFirst = true;
		for(Iterator<Integer> iter = monthGroupIDs.iterator();iter.hasNext();) {
			Integer monthGroupIDInteger = (Integer) iter.next();
			if(isFirst) {
				sql += columnName + " IN (";
			} else {
				sql += ",";
			}
			sql +=  monthGroupIDInteger.intValue();
			isFirst = false;
		}

		if(monthGroupIDs.size() != 0) {
			sql += ")";
			sqls.add(sql);
		}
		return sqls;
	}

	/**
	 * Builds a WHERE clause based on the ExecutionRunSpec's
	 * Fuel Sub Type selections.
	 * @param columnName The name of the column to use in the WHERE clause.  As appropriate,
	 * this could also include a table name qualifier.
	 * @return Vector of String objects each with the SQL WHERE clause as String.
	**/
	static Vector<String> buildSQLWhereClauseForFuelSubTypes(String columnName) {
		Vector<String> sqls = new Vector<String>();
		Connection defaultDatabase = null;
		String sql = "";
		boolean isFirst = true;

		// E85 air toxics requires E10 information (fuelsubtypeID=12)
		boolean forceE10 = false;
		for(Iterator<OnRoadVehicleSelection> iter =
				ExecutionRunSpec.theExecutionRunSpec.getOnRoadVehicleSelections().iterator();
				iter.hasNext(); ) {
			OnRoadVehicleSelection selection = (OnRoadVehicleSelection)iter.next();
			if(selection.fuelTypeID == 5) {
				forceE10 = true;
				break;
			}
		}
		if(!forceE10) {
			for(Iterator<OffRoadVehicleSelection> iter =
					ExecutionRunSpec.theExecutionRunSpec.getOffRoadVehicleSelections().iterator();
					iter.hasNext(); ) {
				OffRoadVehicleSelection selection = iter.next();
				if(selection.fuelTypeID == 5) {
					forceE10 = true;
					break;
				}
			}
		}

		try {
			defaultDatabase = DatabaseConnectionManager.checkOutConnection(
					MOVESDatabaseType.DEFAULT);
			sql = "SELECT DISTINCT fuelsubtypeid from fuelsubtype";
			Vector<String> fuelTypeClauses = buildSQLWhereClauseForFuelTypes("fueltypeid");
			if(fuelTypeClauses.size() > 0) {
				sql += " WHERE " + fuelTypeClauses.get(0);
			}
			isFirst = true;
			ResultSet results = SQLRunner.executeQuery(defaultDatabase, sql);
			sql = "";
			if(forceE10) {
				isFirst = false;
				sql += columnName + " IN (12";
			}
			while(results.next()) {
				if(isFirst) {
					sql += columnName + " IN (";
				} else {
					sql += ",";
				}
				sql +=  results.getString("fuelsubtypeid");
				isFirst = false;
			}
			results.close();
		} catch(Exception e) {
			//do nothing
		} finally {
			if(defaultDatabase != null) {
				DatabaseConnectionManager.checkInConnection(MOVESDatabaseType.DEFAULT,
						defaultDatabase);
				defaultDatabase = null;
			}
		}
		if(!isFirst) {
			sql += ")";
			sqls.add(sql);
		}
		return sqls;
	}

	/**
	 * Builds a WHERE clause based on the ExecutionRunSpec's
	 * Nonroad Fuel Sub Type selections.
	 * @param columnName The name of the column to use in the WHERE clause.  As appropriate,
	 * this could also include a table name qualifier.
	 * @return Vector of String objects each with the SQL WHERE clause as String.
	**/
	static Vector<String> buildSQLWhereClauseForNonroadFuelSubTypes(String columnName) {
		Vector<String> sqls = new Vector<String>();
		Connection defaultDatabase = null;
		String sql = "";
		boolean isFirst = true;

		try {
			defaultDatabase = DatabaseConnectionManager.checkOutConnection(
					MOVESDatabaseType.DEFAULT);
			sql = "SELECT DISTINCT fuelsubtypeid from nrfuelsubtype";
			Vector<String> fuelTypeClauses = buildSQLWhereClauseForNonroadFuelTypes("fueltypeid");
			if(fuelTypeClauses.size() > 0) {
				sql += " WHERE " + fuelTypeClauses.get(0);
			}
			isFirst = true;
			ResultSet results = SQLRunner.executeQuery(defaultDatabase, sql);
			sql = "";
			while(results.next()) {
				if(isFirst) {
					sql += columnName + " IN (";
				} else {
					sql += ",";
				}
				sql +=  results.getString("fuelsubtypeid");
				isFirst = false;
			}
			results.close();
		} catch(Exception e) {
			//do nothing
		} finally {
			if(defaultDatabase != null) {
				DatabaseConnectionManager.checkInConnection(MOVESDatabaseType.DEFAULT,
						defaultDatabase);
				defaultDatabase = null;
			}
		}
		if(!isFirst) {
			sql += ")";
			//Logger.log(LogMessageCategory.INFO,"buildSQLWhereClauseForNonroadFuelSubTypes=" + sql);
			sqls.add(sql);
		}
		return sqls;
	}

	/**
	 * Builds a WHERE clause based on the ExecutionRunSpec's NonRoad sector selections.
	 * @param columnName The name of the column to use in the WHERE clause.  As appropriate,
	 * this could also include a table name qualifier.
	 * @return Vector of String objects each with the SQL WHERE clause as String.
	**/
	static Vector<String> buildSQLWhereClauseForSectors(String columnName) {
		Vector<String> sqls = new Vector<String>();
		String sql = "";
		TreeSet<Integer> sectors = new TreeSet<Integer>();
		for(Iterator<OffRoadVehicleSelection> iter =
				ExecutionRunSpec.theExecutionRunSpec.getOffRoadVehicleSelections().iterator();
				iter.hasNext(); ) {
			OffRoadVehicleSelection selection = iter.next();
			Integer sectorInteger = Integer.valueOf(selection.sectorID);
			if(!sectors.contains(sectorInteger)) {
				if(sectors.size() == 0) {
					sql += columnName + " IN (";
				} else {
					sql += ",";
				}
				sql +=  sectorInteger.intValue();
				sectors.add(sectorInteger);
			}
		}
		if(sectors.size() != 0) {
			sql += ")";
			sqls.add(sql);
		}
		return sqls;
	}

	/**
	 * Builds a WHERE clause based on the ExecutionRunSpec's NonRoad sector selections.
	 * @param columnName The name of the column to use in the WHERE clause.  As appropriate,
	 * this could also include a table name qualifier.
	 * @return Vector of String objects each with the SQL WHERE clause as String.
	**/
	static Vector<String> buildSQLWhereClauseForNonRoadEquipmentTypes(String columnName) {
		Vector<String> sqls = new Vector<String>();
		Connection nrDefaultDatabase = null;
		String resultSQL = "";
		String sql = "";
		boolean isFirst = true;
		SQLRunner.Query query = new SQLRunner.Query();
		try {
			nrDefaultDatabase = DatabaseConnectionManager.checkOutConnection(
					MOVESDatabaseType.DEFAULT);
			sql = "SELECT DISTINCT nrequiptypeid, surrogateid from nrequipmenttype";
			Vector<String> sectorClauses = buildSQLWhereClauseForSectors("sectorid");
			if(sectorClauses.size() > 0) {
				sql += " WHERE " + sectorClauses.get(0);
			}
			isFirst = true;
			query.open(nrDefaultDatabase,sql);
			sql = "";
			TreeSet<Integer> equipmentTypes = new TreeSet<Integer>();
			TreeSet<Integer> surrogateTypes = new TreeSet<Integer>();
			String surrogateSQL = "";
			while(query.rs.next()) {
				if(isFirst) {
					resultSQL += columnName + " IN (";
				} else {
					resultSQL += ",";
				}
				int equipmentTypeID = query.rs.getInt("nrequiptypeid");
				equipmentTypes.add(Integer.valueOf(equipmentTypeID));
				resultSQL +=  equipmentTypeID;
				isFirst = false;

				int surrogateID = query.rs.getInt("surrogateid");
				if(surrogateID > 0) {
					Integer ts = Integer.valueOf(surrogateID);
					if(!equipmentTypes.contains(ts) && !surrogateTypes.contains(ts)) {
						surrogateTypes.add(ts);
						if(surrogateSQL.length() > 0) {
							surrogateSQL += ",";
						}
						surrogateSQL += surrogateID;
					}
				}
			}
			query.close();

			if(surrogateSQL.length() > 0) {
				sql = "select distinct nrequiptypeid"
						+ " from nrequipmenttype"
						+ " where nrequiptypeid in (" + surrogateSQL + ")";
				query.open(nrDefaultDatabase,sql);
				while(query.rs.next()) {
					int equipmentTypeID = query.rs.getInt("nrequiptypeid");
					Integer t = Integer.valueOf(equipmentTypeID);
					if(!equipmentTypes.contains(t)) {
						equipmentTypes.add(t);
						resultSQL += "," + equipmentTypeID;
					}
				}
				query.close();
			}
		} catch(Exception e) {
			//do nothing
		} finally {
			query.onFinally();
			if(nrDefaultDatabase != null) {
				DatabaseConnectionManager.checkInConnection(MOVESDatabaseType.DEFAULT,
						nrDefaultDatabase);
				nrDefaultDatabase = null;
			}
		}
		if(!isFirst) {
			resultSQL += ")";
			sqls.add(resultSQL);
		}
		return sqls;
	}

	/**
	 * Default constructor
	**/
	public InputDataManager() {
	}

	/**
	 * Handles connection checkout/checkin and calls the next merge implementation which actually
	 * performs the merge. This moves a subset of data from the default database to the execution
	 * database based on the ExecutionRunSpec.
	 * @throws InterruptedException If the active thread is interrupted
	 * @throws SQLException If any SQL errors occur.
	 * @throws IOException If there is an error during any temporary file operations
	**/
	public static void merge() throws InterruptedException, SQLException, IOException, Exception {
		merge(ModelDomain.NATIONAL_ALLOCATION);
	}

	/**
	 * Handles connection checkout/checkin and calls the next merge implementation which actually
	 * performs the merge. This moves a subset of data from the NonRoad default database to the execution
	 * database based on the ExecutionRunSpec.
	 * @throws InterruptedException If the active thread is interrupted
	 * @throws SQLException If any SQL errors occur.
	 * @throws IOException If there is an error during any temporary file operations
	**/
	public static void mergeNonRoad() throws InterruptedException, SQLException, IOException, Exception {
		if(CompilationFlags.USE_NONROAD) {
			mergeNonRoad(ModelDomain.NATIONAL_ALLOCATION);
		}
	}

	/**
	 * Handles connection checkout/checkin and calls the next merge implementation which actually
	 * performs the merge. This moves a subset of data from the default database to the execution
	 * database based on the ExecutionRunSpec.
	 * @param domain model domain, required since not all tables are needed for each domain.
	 * domain may be null, in which case it is treated as a national-level domain.
	 * @throws InterruptedException If the active thread is interrupted
	 * @throws SQLException If any SQL errors occur.
	 * @throws IOException If there is an error during any temporary file operations
	 * @return true upon successful merge
	**/
	public static boolean merge(ModelDomain domain)
			throws InterruptedException, SQLException, IOException, Exception {
		Connection inputConnection = null;
		Connection executionConnection = null;
		
		try {
			InputDataManager inputDataManager = new InputDataManager();
			inputConnection = DatabaseConnectionManager.checkOutConnection
					(MOVESDatabaseType.DEFAULT);
			executionConnection = DatabaseConnectionManager.checkOutConnection
					(MOVESDatabaseType.EXECUTION);
			// Do not copy the default database's link table's contents when using
			// the project domain.  Doing so would cause unwanted links to be calculated.
			boolean includeLinkTable = domain != ModelDomain.PROJECT;
			// Single county and Project domains require the user to provide a complete fuel supply table
			// in a domain-level database.  As such, do not copy the fuel supply table for those domains.
			boolean includeFuelSupply = domain != ModelDomain.SINGLE_COUNTY && domain != ModelDomain.PROJECT;
			//System.out.println("merge(domain)");
			inputDataManager.merge(inputConnection, executionConnection, includeLinkTable, includeFuelSupply, true);
			return true;
		} catch (Exception e) {
			/** @nonissue **/
			Logger.log(LogMessageCategory.ERROR,
					"Error while merging default database:" + e.toString());
			e.printStackTrace();
			return false;
		} finally {
			if (inputConnection != null) {
				DatabaseConnectionManager.checkInConnection
						(MOVESDatabaseType.DEFAULT, inputConnection);
				inputConnection = null;
			}
			if (executionConnection != null) {
				DatabaseConnectionManager.checkInConnection
						(MOVESDatabaseType.EXECUTION, executionConnection);
				executionConnection = null;
			}
		}
	}

	/**
	 * Handles connection checkout/checkin and calls the next merge implementation which actually
	 * performs the merge. This moves a subset of data from the NonRoad default database to the execution
	 * database based on the ExecutionRunSpec.
	 * @param domain model domain, required since not all tables are needed for each domain.
	 * domain may be null, in which case it is treated as a national-level domain.
	 * @throws InterruptedException If the active thread is interrupted
	 * @throws SQLException If any SQL errors occur.
	 * @throws IOException If there is an error during any temporary file operations
	 * @return true upon successful merge
	**/
	public static boolean mergeNonRoad(ModelDomain domain)
			throws InterruptedException, SQLException, IOException, Exception {
		if(!CompilationFlags.USE_NONROAD) {
			return true;
		}
		Connection inputConnection = null;
		Connection executionConnection = null;
		
		try {
			InputDataManager inputDataManager = new InputDataManager();
			inputConnection = DatabaseConnectionManager.checkOutConnection(MOVESDatabaseType.DEFAULT);
			executionConnection = DatabaseConnectionManager.checkOutConnection(MOVESDatabaseType.EXECUTION);
			inputDataManager.mergeNonRoad(inputConnection, executionConnection, true);
			return true;
		} catch (Exception e) {
			/** @nonissue **/
			Logger.log(LogMessageCategory.ERROR,
					"Error while merging default database:" + e.toString());
			e.printStackTrace();
			return false;
		} finally {
			if (inputConnection != null) {
				DatabaseConnectionManager.checkInConnection(MOVESDatabaseType.DEFAULT, inputConnection);
				inputConnection = null;
			}
			if (executionConnection != null) {
				DatabaseConnectionManager.checkInConnection(MOVESDatabaseType.EXECUTION, executionConnection);
				executionConnection = null;
			}			
		}
	}

	/**
	 * Handles connection checkout/checkin and calls the next merge implementation which actually
	 * performs the merge. This moves a subset of data from an input database to the execution
	 * database based on the ExecutionRunSpec.
	 * @param userDatabase an input database selected by a user
	 * @throws InterruptedException If the active thread is interrupted
	 * @throws SQLException If any SQL errors occur.
	 * @throws IOException If there is an error during any temporary file operations
	 * @return true upon a successful merge
	**/
	public static boolean merge(DatabaseSelection userDatabase) throws InterruptedException,
			SQLException, IOException, Exception {
		//System.out.println("Merging from user supplied database");
		Connection inputConnection = null;
		Connection executionConnection = null;
		
		try {
			InputDataManager inputDataManager = new InputDataManager();
			inputConnection = userDatabase.openConnection();
			executionConnection = DatabaseConnectionManager.checkOutConnection
					(MOVESDatabaseType.EXECUTION);
			//System.out.println("merge(userDatabase)");
			inputDataManager.merge(inputConnection, executionConnection, true, true, false);
			return true;
		} catch (Exception e) {
			/** @nonissue **/
			Logger.log(LogMessageCategory.ERROR,
					"Error while merging a user database:" + e.toString());
			/**
			 * @issue Unable to import database [*]
			 * @explain A database could not be merged into the MOVESExecution database.
			 * Check the database availability.
			**/
			Logger.log(LogMessageCategory.ERROR,
					"Unable to import database " + userDatabase.databaseName);
			e.printStackTrace();
			return false;
		} finally {
			if (inputConnection != null) {
				inputConnection.close();
			}
			if (executionConnection != null) {
				DatabaseConnectionManager.checkInConnection
						(MOVESDatabaseType.EXECUTION, executionConnection);
				executionConnection = null;
			}
		}
	}

	/**
	 * Handles connection checkout/checkin and calls the next merge implementation which actually
	 * performs the merge. This moves a subset of data from an input database to the execution
	 * database based on the ExecutionRunSpec.
	 * @param userDatabase an input database selected by a user
	 * @throws InterruptedException If the active thread is interrupted
	 * @throws SQLException If any SQL errors occur.
	 * @throws IOException If there is an error during any temporary file operations
	 * @return true upon successful merge
	**/
	public static boolean mergeNonRoad(DatabaseSelection userDatabase) throws InterruptedException,
			SQLException, IOException, Exception {
		//System.out.println("Merging from user supplied NonRoad database");
		Connection inputConnection = null;
		Connection executionConnection = null;
		
		try {
			InputDataManager inputDataManager = new InputDataManager();
			inputConnection = userDatabase.openConnection();
			executionConnection = DatabaseConnectionManager.checkOutConnection
					(MOVESDatabaseType.EXECUTION);
			//System.out.println("merge(userDatabase)");
			inputDataManager.mergeNonRoad(inputConnection, executionConnection, false);
			return true;
		} catch (Exception e) {
			/** @nonissue **/
			Logger.log(LogMessageCategory.ERROR,
					"Error while merging a user database:" + e.toString());
			/**
			 * @issue Unable to import database [*]
			 * @explain A database could not be merged into the MOVESExecution database.
			 * Check the database availability.
			**/
			Logger.log(LogMessageCategory.ERROR,
					"Unable to import database " + userDatabase.databaseName);
			e.printStackTrace();
			return false;
		} finally {
			if (inputConnection != null) {
				inputConnection.close();
			}
			if (executionConnection != null) {
				DatabaseConnectionManager.checkInConnection
						(MOVESDatabaseType.EXECUTION, executionConnection);
				executionConnection = null;
			}
		}
	}

	/**
	 *  Make base tables used for generating input uncertainty in successive iterations.
	**/
	public static void makeBaseUncertaintyInput() {
		Connection executionConnection = null;
		String[] commands = {
			// CrankcaseEmissionRatio
			"DROP TABLE IF EXISTS crankcaseemissionratiosample",
			"CREATE TABLE crankcaseemissionratiosample select polprocessid, minmodelyearid, maxmodelyearid,"
					+ " sourcetypeid,fueltypeid,coalesce(crankcaseratio,0) as crankcaseratio,"
					+ " coalesce(crankcaseratiocv,0) as crankcaseratiocv"
					+ " from crankcaseemissionratio",

			// PM10EmissionRatio
			"DROP TABLE IF EXISTS pm10emissionratiosample",
			"CREATE TABLE pm10emissionratiosample select polprocessid, "
					+ " sourcetypeid,fueltypeid,coalesce(pm10pm25ratio,0) as pm10pm25ratio,"
					+ " coalesce(pm10pm25ratiocv,0) as pm10pm25ratiocv"
					+ " from pm10emissionratio",

			// EmissionRate
			"DROP TABLE IF EXISTS emissionratesample",
			"CREATE TABLE emissionratesample select sourcebinid, polprocessid, opmodeid,"
					+ " coalesce(meanbaserate,0) as meanbaserate, coalesce(meanbaseratecv,0)"
					+ " as meanbaseratecv, coalesce(meanbaserateim,0) as meanbaserateim,"
					+ " coalesce(meanbaserateimcv,0) as meanbaserateimcv, datasourceid "
					+ " from emissionrate",

			// EmissionRateByAge
			"DROP TABLE IF EXISTS emissionratebyagesample",
			"CREATE TABLE emissionratebyagesample select sourcebinid, polprocessid, opmodeid,"
					+ " agegroupid, coalesce(meanbaserate,0) as meanbaserate,"
					+ " coalesce(meanbaseratecv,0) as meanbaseratecv, coalesce(meanbaserateim,0)"
					+ " as meanbaserateim, coalesce(meanbaserateimcv,0) as meanbaserateimcv,"
					+ " datasourceid"
					+ " from emissionratebyage",

			// SulfateEmissionRate
			"DROP TABLE IF EXISTS sulfateemissionratesample",
			"CREATE TABLE sulfateemissionratesample select polprocessid, fueltypeid, "
					+ "modelyeargroupid, coalesce(meanbaserate,0) as meanbaserate,"
					+ " coalesce(meanbaseratecv,0) as meanbaseratecv, "
					+ " datasourceid"
					+ " from sulfateemissionrate",

			// NONO2Ratio
			"DROP TABLE IF EXISTS nono2ratiosample",
			"CREATE TABLE nono2ratiosample select polprocessid, sourcetypeid, "
					+ " fueltypeid, modelyeargroupid, coalesce(noxratio,0) as noxratio,"
					+ " coalesce(noxratiocv,0) as noxratiocv, "
					+ " datasourceid"
					+ " from nono2ratio",

			// methaneTHCRatio
			"drop table if exists methanethcratio",
			"create table methanethcratiosample"
					+ " select processid, fueltypeid, sourcetypeid, modelyeargroupid, agegroupid,"
					+ " coalesce(ch4thcratio,0) as ch4thcratio, coalesce(ch4thcratiocv,0) as ch4thcratiocv"
					+ " from methanethcratio",

			// CumTVVCoeffs
			"drop table if exists cumtvvcoeffssample",
			"create table cumtvvcoeffssample"
					+ " select regclassid, modelyeargroupid, agegroupid, polprocessid,"
					+ " coalesce(tvvterma,0.0) as tvvterma,"
					+ " coalesce(tvvtermb,0.0) as tvvtermb,"
					+ " coalesce(tvvtermc,0.0) as tvvtermc,"
					+ " coalesce(tvvtermacv,0.0) as tvvtermacv,"
					+ " coalesce(tvvtermbcv,0.0) as tvvtermbcv,"
					+ " coalesce(tvvtermccv,0.0) as tvvtermccv,"
					+ " coalesce(tvvtermaim,0.0) as tvvtermaim,"
					+ " coalesce(tvvtermbim,0.0) as tvvtermbim,"
					+ " coalesce(tvvtermcim,0.0) as tvvtermcim,"
					+ " coalesce(tvvtermaimcv,0.0) as tvvtermaimcv,"
					+ " coalesce(tvvtermbimcv,0.0) as tvvtermbimcv,"
					+ " coalesce(tvvtermcimcv,0.0) as tvvtermcimcv"
					+ " from cumtvvcoeffs",

			// TemperatureAdjustment
			"drop table if exists temperatureadjustmentsample",
			"create table temperatureadjustmentsample"
					+ " select polprocessid, fueltypeid,"
					+ " coalesce(tempadjustterma,0.0) as tempadjustterma,"
					+ " coalesce(tempadjusttermb,0.0) as tempadjusttermb,"
					+ " coalesce(tempadjusttermc,0.0) as tempadjusttermc,"
					+ " coalesce(tempadjusttermacv,0.0) as tempadjusttermacv,"
					+ " coalesce(tempadjusttermbcv,0.0) as tempadjusttermbcv,"
					+ " coalesce(tempadjusttermccv,0.0) as tempadjusttermccv"
					+ " from temperatureadjustment",

			// StartTempAdjustment
			"drop table if exists starttempadjustmentsample",
			"create table starttempadjustmentsample"
					+ " select polprocessid, fueltypeid, opmodeid, modelyeargroupid,"
					+ " coalesce(tempadjustterma,0.0) as tempadjustterma,"
					+ " coalesce(tempadjusttermb,0.0) as tempadjusttermb,"
					+ " coalesce(tempadjusttermc,0.0) as tempadjusttermc,"
					+ " coalesce(tempadjusttermacv,0.0) as tempadjusttermacv,"
					+ " coalesce(tempadjusttermbcv,0.0) as tempadjusttermbcv,"
					+ " coalesce(tempadjusttermccv,0.0) as tempadjusttermccv"
					+ " from starttempadjustment",

			// FullACAdjustment
			"drop table if exists fullacadjustmentsample",
			"create table fullacadjustmentsample"
					+ " select sourcetypeid, polprocessid, opmodeid,"
					+ " coalesce(fullacadjustment,1.0) as fullacadjustment,"
					+ " coalesce(fullacadjustmentcv,0.0) as fullacadjustmentcv"
					+ " from fullacadjustment",
/*
			// FuelAdjustment
			"drop table if exists FuelAdjustmentSample",
			"create table FuelAdjustmentSample"
					+ " select  polProcessID, sourceTypeID, fuelMYGroupID, fuelFormulationID,"
					+ " coalesce(fuelAdjustment,1.0) as fuelAdjustment,"
					+ " coalesce(fuelAdjustmentCV,0.0) as fuelAdjustmentCV,"
					+ " coalesce(fuelAdjustmentGPA,1.0) as fuelAdjustmentGPA,"
					+ " coalesce(fuelAdjustmentGPACV,0.0) as fuelAdjustmentGPACV"
					+ " from FuelAdjustment",
*/
			// RefuelingFactors
			"drop table if exists refuelingfactorssample",
			"create table refuelingfactorssample"
					+ " select fueltypeid, defaultformulationid,"
					+ " vaporterma, vaportermb, vaportermc, vaportermd, vaporterme, vaportermf,"
					+ " vaporlowtlimit, vaporhightlimit, tanktdifflimit,"
					+ " minimumrefuelingvaporloss, refuelingspillrate, refuelingspillratecv,"
					+ " displacedvaporratecv"
					+ " from refuelingfactors"
		};
		String sql = "";
		try {
			executionConnection = DatabaseConnectionManager.checkOutConnection
				(MOVESDatabaseType.EXECUTION);

			for(int i=0;i<commands.length;i++) {
				sql = commands[i];
				SQLRunner.executeSQL(executionConnection, sql);
			}

			// Folder for uncertainty files.
			temporaryFolderPath = FileUtilities.createTemporaryFolder(
					null, "InputDataManagerTemp");
			if(temporaryFolderPath == null) {
				/**
				 * @explain A directory needed for uncertainty calculations could not be created.
				**/
				Logger.log(LogMessageCategory.ERROR,
					"Create temporary folder failed, unable to perform uncertainty calculations.");
			}

			// Use the same seed for each run.
			uncertaintyGenerator.setSeed(8675309);
		} catch (Exception e) {
			/**
			 * @issue Exception occurred on 'makeBaseUncertaintyInput' [*]
			 * @explain An error occurred while generating randomized data for uncertainty
			 * analysis.
			**/
			Logger.log(LogMessageCategory.ERROR,
					"Exception occurred on 'makeBaseUncertaintyInput' " + e);
		} finally {
			if (executionConnection != null) {
				DatabaseConnectionManager.checkInConnection
					(MOVESDatabaseType.EXECUTION, executionConnection);
				executionConnection = null;
			}
		}
	}

	/**
	 *  Simulate uncertainty in input by randomizing the input values.
	 * @param nextIterationID The next iteration to be performed.
	**/
	public static void simulateUncertaintyInInput(int nextIterationID) {
		if(temporaryFolderPath == null) {
			return;
		}

		//CrankcaseEmissionRatio
		randomizeTableData("polprocessid, minmodelyearid, maxmodelyearid, sourcetypeid, fueltypeid,"
				+ " crankcaseratio, crankcaseratiocv",
				"crankcaseemissionratiosample", 5,
				"truncate crankcaseemissionratio", "crankcaseemissionratio (polprocessid, "
				+ "minmodelyearid, maxmodelyearid, sourcetypeid, fueltypeid, crankcaseratio, crankcaseratiocv)",1);
		//PM10EmissionRatio
		randomizeTableData("polprocessid, sourcetypeid, fueltypeid,"
				+ " pm10pm25ratio, pm10pm25ratiocv",
				"pm10emissionratiosample", 3,
				"truncate pm10emissionratio", "pm10emissionratio (polprocessid, "
				+ "sourcetypeid, fueltypeid, pm10pm25ratio, pm10pm25ratiocv)",1);
		//EmissionRate
		randomizeTableData(" * ", "emissionratesample", 3, "truncate emissionrate",
		        " emissionrate (sourcebinid, polprocessid, opmodeid,"
				+ " meanbaserate, meanbaseratecv, "
				+ " meanbaserateim, meanbaserateimcv, datasourceid)",2);
		//EmissionRateByAge
		randomizeTableData(" * ", "emissionratebyagesample ", 4,
				"truncate emissionratebyage", "emissionratebyage (sourcebinid, polprocessid,"
				+ " opmodeid, agegroupid, meanbaserate, meanbaseratecv, meanbaserateim,"
				+ " meanbaserateimcv, datasourceid)",2);
		//SulfateEmissionRate
		randomizeTableData("polprocessid, fueltypeid, modelyeargroupid, meanbaserate, meanbaseratecv, datasourceid",
				"sulfateemissionratesample", 3,
				"truncate sulfateemissionrate", "sulfateemissionrate (polprocessid, fueltypeid,"
				+ " modelyeargroupid, meanbaserate, meanbaseratecv, datasourceid)",1);
		//NONO2Ratio
		randomizeTableData("polprocessid, sourcetypeid, fueltypeid, modelyeargroupid, noxratio,"
				+ " noxratiocv, datasourceid",
				"nono2ratiosample", 4,
				"truncate nono2ratiosample", "nono2ratio (polprocessid, "
				+ "sourcetypeid, fueltypeid, modelyeargroupid, noxratio, noxratiocv, datasourceid)",1);
		// methaneTHCRatio
		randomizeTableData("processid, fueltypeid, sourcetypeid, modelyeargroupid, agegroupid, ch4thcratio, ch4thcratiocv",
				"methanethcratiosample",
				5,
				"truncate methanethcratio",
				"methanethcratio (processid, fueltypeid, sourcetypeid, modelyeargroupid, agegroupid, ch4thcratio, ch4thcratiocv",
				1);
		//CumTVVCoeffs
		randomizeTableData("regclassid, modelyeargroupid, agegroupid, polprocessid,"
					+ "tvvterma, tvvtermacv, tvvtermaim, tvvtermaimcv,"
					+ "tvvtermb, tvvtermbcv, tvvtermbim, tvvtermbimcv,"
					+ "tvvtermc, tvvtermccv, tvvtermcim, tvvtermcimcv",
				"cumtvvcoeffssample",
				4,
				"truncate cumtvvcoeffs",
				"cumtvvcoeffs (regclassid, modelyeargroupid, agegroupid, polprocessid,"
					+ "tvvterma, tvvtermacv, tvvtermaim, tvvtermaimcv,"
					+ "tvvtermb, tvvtermbcv, tvvtermbim, tvvtermbimcv,"
					+ "tvvtermc, tvvtermccv, tvvtermcim, tvvtermcimcv)",
				6);
		//TemperatureAdjustment
		randomizeTableData("polprocessid, fueltypeid, "
					+ " tempadjustterma, tempadjusttermacv,"
					+ " tempadjusttermb, tempadjusttermbcv,"
					+ " tempadjusttermc, tempadjusttermccv",
				"temperatureadjustmentsample",
				2,
				"truncate temperatureadjustment",
				"temperatureadjustment (polprocessid, fueltypeid, "
					+ " tempadjustterma, tempadjusttermacv,"
					+ " tempadjusttermb, tempadjusttermbcv,"
					+ " tempadjusttermc, tempadjusttermccv)",
				3);
		//StartTempAdjustment
		randomizeTableData("polprocessid, fueltypeid, "
					+ " modelyeargroupid, opmodeid, "
					+ " tempadjustterma, tempadjusttermacv,"
					+ " tempadjusttermb, tempadjusttermbcv,"
					+ " tempadjusttermc, tempadjusttermccv",
				"starttempadjustmentsample",
				4,
				"truncate starttempadjustment",
				"starttempadjustment (polprocessid, fueltypeid, "
					+ " modelyeargroupid, opmodeid, "
					+ " tempadjustterma, tempadjusttermacv,"
					+ " tempadjusttermb, tempadjusttermbcv,"
					+ " tempadjusttermc, tempadjusttermccv)",
				3);
		//FullACAdjustment
		randomizeTableData("sourcetypeid, polprocessid, opmodeid,"
				+ " fullacadjustment, fullacadjustmentcv",
				"fullacadjustmentsample", 3,
				"truncate fullacadjustment", "fullacadjustment (sourcetypeid, polprocessid,"
				+ " opmodeid, fullacadjustment, fullacadjustmentcv)",1);
/*
		//FuelAdjustment
		randomizeTableData("polProcessID, sourceTypeID, fuelMYGroupID, fuelFormulationID,"
				+ " fuelAdjustment, fuelAdjustmentCV, fuelAdjustmentGPA, fuelAdjustmentGPACV",
				"FuelAdjustmentSample", 4,
				"TRUNCATE FuelAdjustment", "FuelAdjustment (polProcessID, sourceTypeID,"
				+ " fuelMYGroupID, fuelFormulationID, "
				+ " fuelAdjustment, fuelAdjustmentCV, "
				+ " fuelAdjustmentGPA, fuelAdjustmentGPACV)",2);
*/
		// RefuelingFactors
		randomizeTableData("fueltypeid, defaultformulationid,"
					+ " vaporterma, vaportermb, vaportermc, vaportermd, vaporterme, vaportermf,"
					+ " vaporlowtlimit, vaporhightlimit, tanktdifflimit,"
					+ " minimumrefuelingvaporloss, refuelingspillrate, refuelingspillratecv,"
					+ " displacedvaporratecv",
					"refuelingfactorssample", 12,
					"truncate refuelingfactors",
					"refuelingfactors (fueltypeid, defaultformulationid,"
					+ " vaporterma, vaportermb, vaportermc, vaportermd, vaporterme, vaportermf,"
					+ " vaporlowtlimit, vaporhightlimit, tanktdifflimit,"
					+ " minimumrefuelingvaporloss, refuelingspillrate, refuelingspillratecv,"
					+ " displacedvaporratecv)",
					1);
		/*
		// ATRatioGas1
		randomizeTableData("polProcessID,fuelMYGroupID,fuelTypeID,sourceTypeID,FuelFormulationID,"
				+ " ATRatio,ATRatioCV",
				"ATRatioGas1", 5,
				"TRUNCATE ATRatioGas1",
				"ATRatioGas1 (polProcessID,fuelMYGroupID,fuelTypeID,sourceTypeID,FuelFormulationID,"
				+ " ATRatio,ATRatioCV)",1);
		*/
		// ATRatioGas2
		randomizeTableData("polprocessid,sourcetypeid,fuelsubtypeid,"
				+ " atratio,atratiocv",
				"atratiogas2", 3,
				"truncate atratiogas2",
				"atratiogas2 (polprocessid,sourcetypeid,fuelsubtypeid,"
				+ " atratio,atratiocv)",1);
		// ATRatioNonGas
		randomizeTableData("polprocessid,sourcetypeid,fuelsubtypeid,"
				+ " atratio,atratiocv",
				"atrationongas", 3,
				"truncate atrationongas",
				"atrationongas (polprocessid,sourcetypeid,fuelsubtypeid,"
				+ " atratio,atratiocv)",1);

		OutputEmissionsBreakdownSelection outputEmissionsBreakdownSelection =
				ExecutionRunSpec.theExecutionRunSpec.getOutputEmissionsBreakdownSelection();
		if(outputEmissionsBreakdownSelection.keepSampledData) {
			int	activeRunID = MOVESEngine.theInstance.getActiveRunID();

			Connection executionConnection = null;
			Connection outputConnection = null;
			File exportTableFilePath = null;
			try {
				exportTableFilePath = new File(temporaryFolderPath, "copyTable");
				if(exportTableFilePath.exists()) {
					exportTableFilePath.delete();
				}

				String fileName = exportTableFilePath.getCanonicalPath().replace('\\', '/');

				executionConnection = DatabaseConnectionManager.checkOutConnection(
							MOVESDatabaseType.EXECUTION);
				outputConnection = DatabaseConnectionManager.checkOutConnection(
						MOVESDatabaseType.OUTPUT);

				String sql = null;

				// Do CrankcaseEmissionRatio Table
				if(exportTableFilePath.exists()) {
					exportTableFilePath.delete();
				}
				sql = "SELECT " + activeRunID + ", " + nextIterationID + ","
						+ " polprocessid, minmodelyearid, maxmodelyearid, sourcetypeid, fueltypeid, crankcaseratio"
						+ " INTO OUTFILE '" + fileName + "' FROM crankcaseemissionratio";
				SQLRunner.executeSQL(executionConnection,sql);

				sql = "CREATE TABLE IF NOT EXISTS crankcaseemissionratiosample ("
						+ " movesrunid smallint unsigned not null,"
						+ " iterationid smallint unsigned default 1,"
					    + " polprocessid int not null,"
					    + " minmodelyearid smallint not null,"
					    + " maxmodelyearid smallint not null,"
						+ " sourcetypeid smallint not null,"
					    + " fueltypeid smallint not null,"
					    + " crankcaseratio float null)";
				SQLRunner.executeSQL(outputConnection,sql);

				sql = "LOAD DATA INFILE '" + fileName + "' INTO TABLE crankcaseemissionratiosample "
						+ "(movesrunid, iterationid, polprocessid, minmodelyearid, maxmodelyearid, sourcetypeid, "
						+ " fueltypeid, crankcaseratio)";
				SQLRunner.executeSQL(outputConnection, sql);

				// Do PM10EmissionRatio Table
				if(exportTableFilePath.exists()) {
					exportTableFilePath.delete();
				}
				sql = "SELECT " + activeRunID + ", " + nextIterationID + ","
						+ " polprocessid, sourcetypeid, fueltypeid, pm10pm25ratio"
						+ " INTO OUTFILE '" + fileName + "' FROM pm10emissionratio";
				SQLRunner.executeSQL(executionConnection,sql);

				sql = "CREATE TABLE IF NOT EXISTS pm10emissionratiosample ("
						+ " movesrunid smallint unsigned not null,"
						+ " iterationid smallint unsigned default 1,"
					    + " polprocessid int not null,"
						+ " sourcetypeid smallint not null,"
					    + " fueltypeid smallint not null,"
					    + " pm10pm25ratio float null)";
				SQLRunner.executeSQL(outputConnection,sql);

				sql = "LOAD DATA INFILE '" + fileName + "' INTO TABLE pm10emissionratiosample "
						+ "(movesrunid, iterationid, polprocessid, sourcetypeid, "
						+ " fueltypeid, pm10pm25ratio)";
				SQLRunner.executeSQL(outputConnection, sql);

				// Do EmissionRate table
				if(exportTableFilePath.exists()) {
					exportTableFilePath.delete();
				}
				sql = "SELECT " + activeRunID + ", " + nextIterationID
						+ ", sourcebinid, polprocessid, opmodeid, meanbaserate, meanbaserateim"
						+ " INTO OUTFILE '" + fileName + "' FROM emissionrate";
				SQLRunner.executeSQL(executionConnection,sql);

				sql = "CREATE TABLE IF NOT EXISTS emissionratesample ("
						+ " movesrunid smallint unsigned not null,"
						+ " iterationid smallint unsigned default 1,"
						+ " sourcebinid bigint not null,"
					    + " polprocessid int not null,"
					    + " opmodeid smallint not null,"
					    + " meanbaserate float null,"
					    + " meanbaserateim float null)";
				SQLRunner.executeSQL(outputConnection,sql);

				sql = "LOAD DATA INFILE '" + fileName + "' INTO TABLE emissionratesample "
						+ "(movesrunid, iterationid, sourcebinid, polprocessid, opmodeid,"
						+ " meanbaserate, meanbaserateim)";
				SQLRunner.executeSQL(outputConnection, sql);

				// Do EmissionRateByAge table
				if(exportTableFilePath.exists()) {
					exportTableFilePath.delete();
				}

				sql = "SELECT " + activeRunID + ", " + nextIterationID
						+ ", sourcebinid, polprocessid, opmodeid, agegroupid, meanbaserate,"
						+ " meanbaserateim INTO OUTFILE '" + fileName + "' FROM emissionratebyage";
				SQLRunner.executeSQL(executionConnection,sql);

				sql = "CREATE TABLE IF NOT EXISTS emissionratebyagesample ("
						+ " movesrunid smallint unsigned not null,"
						+ " iterationid smallint unsigned default 1,"
						+ " sourcebinid bigint not null,"
					    + " polprocessid int not null,"
					    + " opmodeid smallint not null,"
						+ " agegroupid smallint not null,"
					    + " meanbaserate float null,"
					    + " meanbaserateim float null)";
				SQLRunner.executeSQL(outputConnection,sql);

				sql = "LOAD DATA INFILE '" + fileName + "' INTO TABLE emissionratebyagesample ("
						+ " movesrunid, iterationid, sourcebinid, polprocessid, opmodeid,"
						+ " agegroupid, meanbaserate, meanbaserateim)";
				SQLRunner.executeSQL(outputConnection, sql);

				// Do SulfateEmissionRate table
				if(exportTableFilePath.exists()) {
					exportTableFilePath.delete();
				}

				sql = "SELECT " + activeRunID + ", " + nextIterationID
						+ ", polprocessid, fueltypeid, modelyeargroupid, meanbaserate"
						+ " INTO OUTFILE '" + fileName + "' FROM sulfateemissionrate";
				SQLRunner.executeSQL(executionConnection,sql);

				sql = "CREATE TABLE IF NOT EXISTS sulfateemissionratesample ("
						+ " movesrunid smallint unsigned not null,"
						+ " iterationid smallint unsigned default 1,"
					    + " polprocessid int not null,"
					    + " fueltypeid smallint not null,"
						+ " modelyeargroupid int not null,"
					    + " meanbaserate float null,"
					    + " datasourceid smallint null)";
				SQLRunner.executeSQL(outputConnection,sql);

				sql = "LOAD DATA INFILE '" + fileName + "' INTO TABLE sulfateemissionratesample "
						+ "(movesrunid, iterationid, polprocessid, fueltypeid, modelyeargroupid,"
						+ " meanbaserate, datasourceid)";
				SQLRunner.executeSQL(outputConnection, sql);

				// Do NONO2Ratio table
				if(exportTableFilePath.exists()) {
					exportTableFilePath.delete();
				}

				sql = "SELECT " + activeRunID + ", " + nextIterationID
						+ ", polprocessid, sourcetypeid, fueltypeid, modelyeargroupid, noxratio, datasourceid "
						+ " INTO OUTFILE '" + fileName + "' FROM nono2ratio";
				SQLRunner.executeSQL(executionConnection,sql);

				sql = "CREATE TABLE IF NOT EXISTS NONO2Sample ("
						+ " MOVESRunID SMALLINT UNSIGNED NOT NULL,"
						+ " iterationID SMALLINT UNSIGNED DEFAULT 1,"
					    + " polProcessID INT NOT NULL,"
					    + " sourceTypeID SMALLINT NOT NULL,"
					    + " fuelTypeID SMALLINT NOT NULL,"
						+ " modelYearGroupID INT NOT NULL,"
					    + " NOxRatio FLOAT NULL,"
					    + " dataSourceId SMALLINT NULL)";
				SQLRunner.executeSQL(outputConnection,sql);

				sql = "LOAD DATA INFILE '" + fileName + "' INTO TABLE nono2ratiosample "
						+ "(movesrunid, iterationid, polprocessid, sourcetypeid, fueltypeid, "
						+ " modelyeargroupid, noxratio, datasourceid)";
				SQLRunner.executeSQL(outputConnection, sql);

				// Do methaneTHCRatio table
				if(exportTableFilePath.exists()) {
					exportTableFilePath.delete();
				}

				sql = "SELECT " + activeRunID + ", " + nextIterationID
						+ ", processid, fueltypeid, sourcetypeid, modelyeargroupid, agegroupid, ch4thcratio"
						+ " INTO OUTFILE '" + fileName + "' FROM methanethcratio";
				SQLRunner.executeSQL(executionConnection,sql);

				sql = "CREATE TABLE IF NOT EXISTS methanethcratiosample ("
						+ " movesrunid smallint unsigned not null,"
						+ " iterationid smallint unsigned default 1,"
						+ " processid smallint(6) not null default '0',"
						+ " fueltypeid smallint(6) not null default '0',"
						+ " sourcetypeid smallint(6) not null default '0',"
						+ " modelyeargroupid int(11) not null default '0',"
						+ " agegroupid smallint(6) not null default '0',"
						+ " ch4thcratio double default null)";
				SQLRunner.executeSQL(outputConnection,sql);

				sql = "LOAD DATA INFILE '" + fileName + "' INTO TABLE methanethcratiosample ("
						+ " movesrunid, iterationid"
						+ ", processid, fueltypeid, sourcetypeid, modelyeargroupid, agegroupid, ch4thcratio"
						+ ")";
				SQLRunner.executeSQL(outputConnection, sql);

				// Do CumTVVCoeffs table
				if(exportTableFilePath.exists()) {
					exportTableFilePath.delete();
				}

				sql = "SELECT " + activeRunID + ", " + nextIterationID
						+ ", regclassid, modelyeargroupid, agegroupid, polprocessid"
						+ ", tvvterma, tvvtermb, tvvtermc"
						+ ", tvvtermaim, tvvtermbim, tvvtermcim"
						+ " INTO OUTFILE '" + fileName + "' FROM cumtvvcoeffs";
				SQLRunner.executeSQL(executionConnection,sql);

				sql = "CREATE TABLE IF NOT EXISTS cumtvvcoeffssample ("
						+ " movesrunid smallint unsigned not null,"
						+ " iterationid smallint unsigned default 1,"
						+ " regclassid smallint(6) not null,"
						+ " modelyeargroupid int(11) not null,"
						+ " agegroupid smallint(6) not null,"
						+ " polprocessid int not null,"
						+ " tvvterma float null,"
						+ " tvvtermb float null,"
						+ " tvvtermc float null,"
						+ " tvvtermaim float null,"
						+ " tvvtermbim float null,"
						+ " tvvtermcim float null)";
				SQLRunner.executeSQL(outputConnection,sql);

				sql = "LOAD DATA INFILE '" + fileName + "' INTO TABLE cumtvvcoeffssample ("
						+ " movesrunid, iterationid"
						+ ", regclassid, modelyeargroupid, agegroupid, polprocessid"
						+ ", tvvterma, tvvtermb, tvvtermc"
						+ ", tvvtermaim, tvvtermbim, tvvtermcim"
						+ ")";
				SQLRunner.executeSQL(outputConnection, sql);

				// Do TemperatureAdjustment Table
				if(exportTableFilePath.exists()) {
					exportTableFilePath.delete();
				}

				sql = "SELECT " + activeRunID + ", " + nextIterationID + ","
						+ " polprocessid, fueltypeid, "
						+ " tempadjustterma, tempadjusttermb, tempadjusttermc"
						+ " INTO OUTFILE '" + fileName + "' FROM temperatureadjustment";
				SQLRunner.executeSQL(executionConnection,sql);

				sql = "CREATE TABLE IF NOT EXISTS temperatureadjustmentsample ("
						+ " movesrunid smallint unsigned not null,"
						+ " iterationid smallint unsigned default 1,"
					    + " polprocessid int not null,"
					    + " fueltypeid smallint not null,"
					    + " tempadjustterma float null,"
					    + " tempadjusttermb float null,"
					    + " tempadjusttermc float null)";
				SQLRunner.executeSQL(outputConnection,sql);

				sql = "LOAD DATA INFILE '" + fileName + "' INTO TABLE temperatureadjustmentsample "
						+ "(movesrunid, iterationid, polprocessid, fueltypeid,"
						+ " tempadjustterma, tempadjusttermb, tempadjusttermc)";
				SQLRunner.executeSQL(outputConnection, sql);

				// Do StartTempAdjustment Table
				if(exportTableFilePath.exists()) {
					exportTableFilePath.delete();
				}
				sql = "SELECT " + activeRunID + ", " + nextIterationID + ","
						+ " polprocessid, fueltypeid, modelyeargroupid, opmodeid,"
						+ " tempadjustterma, tempadjusttermb, tempadjusttermc"
						+ " INTO OUTFILE '" + fileName + "' FROM starttempadjustment";
				SQLRunner.executeSQL(executionConnection,sql);

				sql = "CREATE TABLE IF NOT EXISTS starttempadjustmentsample ("
						+ " movesrunid smallint unsigned not null,"
						+ " iterationid smallint unsigned default 1,"
					    + " polprocessid int not null,"
					    + " fueltypeid smallint not null,"
					    + " modelyeargroupid integer not null,"
					    + " opmodeid smallint not null,"
					    + " tempadjustterma float null,"
					    + " tempadjusttermb float null,"
					    + " tempadjusttermc float null)";
				SQLRunner.executeSQL(outputConnection,sql);

				sql = "LOAD DATA INFILE '" + fileName + "' INTO TABLE starttempadjustmentsample "
						+ "(movesrunid, iterationid, polprocessid, fueltypeid,"
						+ " modelyeargroupid, opmodeid,"
						+ " tempadjustterma, tempadjusttermb, tempadjusttermc)";
				SQLRunner.executeSQL(outputConnection, sql);

				// Do FullACAdjustment Table
				if(exportTableFilePath.exists()) {
					exportTableFilePath.delete();
				}
				sql = "SELECT " + activeRunID + ", " + nextIterationID + ","
						+ " sourcetypeid, polprocessid, opmodeid, fullacadjustment"
						+ " INTO OUTFILE '" + fileName + "' FROM fullacadjustment";
				SQLRunner.executeSQL(executionConnection,sql);

				sql = "CREATE TABLE IF NOT EXISTS fullacadjustmentsample ("
						+ " movesrunid smallint unsigned not null,"
						+ " iterationid smallint unsigned default 1,"
						+ " sourcetypeid smallint not null,"
					    + " polprocessid int not null,"
					    + " opmodeid smallint not null,"
					    + " fullacadjustment float null)";
				SQLRunner.executeSQL(outputConnection,sql);

				sql = "LOAD DATA INFILE '" + fileName + "' INTO TABLE fullacadjustmentsample "
						+ "(movesrunid, iterationid, sourcetypeid, polprocessid, opmodeid,"
						+ " fullacadjustment)";
				SQLRunner.executeSQL(outputConnection, sql);
/*
				// Do FuelAdjustment Table
				if(exportTableFilePath.exists()) {
					exportTableFilePath.delete();
				}
				sql = "SELECT " + activeRunID + ", " + nextIterationID + ","
						+ " polProcessID, sourceTypeID, fuelMYGroupID, fuelFormulationID,"
						+ " fuelAdjustment, fuelAdjustmentGPA"
						+ " INTO OUTFILE '" + fileName + "' FROM fuelAdjustment";
				SQLRunner.executeSQL(executionConnection,sql);

				sql = "CREATE TABLE IF NOT EXISTS FuelAdjustmentSample ("
						+ " MOVESRunID SMALLINT UNSIGNED NOT NULL,"
						+ " iterationID SMALLINT UNSIGNED DEFAULT 1,"
					    + " polProcessID INT NOT NULL,"
					    + " sourceTypeID SMALLINT NOT NULL,"
					    + " fuelMYGroupID INTEGER NOT NULL,"
					    + " fuelFormulationID SMALLINT NOT NULL,"
					    + " fuelAdjustment FLOAT NULL,"
					    + " fuelAdjustmentGPA FLOAT NULL)";
				SQLRunner.executeSQL(outputConnection,sql);

				sql = "LOAD DATA INFILE '" + fileName + "' INTO TABLE FuelAdjustmentSample "
						+ " (MOVESRunID, iterationID,  polProcessID, sourceTypeID,"
						+ " fuelMYGroupID, fuelFormulationID, "
						+ " fuelAdjustment, fuelAdjustmentGPA)";
				SQLRunner.executeSQL(outputConnection, sql);
*/
				// Do RefuelingFactors table
				if(exportTableFilePath.exists()) {
					exportTableFilePath.delete();
				}

				sql = "SELECT " + activeRunID + ", " + nextIterationID
						+ ", fueltypeid, defaultformulationid,"
						+ " vaporterma, vaportermb, vaportermc, vaportermd, vaporterme, vaportermf,"
						+ " vaporlowtlimit, vaporhightlimit, tanktdifflimit,"
						+ " minimumrefuelingvaporloss, refuelingspillrate, refuelingspillratecv,"
						+ " displacedvaporratecv"
						+ " INTO OUTFILE '" + fileName + "' FROM refuelingfactors";
				SQLRunner.executeSQL(executionConnection,sql);

				sql = "CREATE TABLE IF NOT EXISTS refuelingfactorssample ("
						+ " movesrunid smallint unsigned not null,"
						+ " iterationid smallint unsigned default 1,"
						+ " fueltypeid           smallint not null,"
						+ " defaultformulationid smallint null,"
						+ " vaporterma           float not null default 0,"
						+ " vaportermb           float not null default 0,"
						+ " vaportermc           float not null default 0,"
						+ " vaportermd           float not null default 0,"
						+ " vaporterme           float not null default 0,"
						+ " vaportermf           float not null default 0,"
						+ " vaporlowtlimit       float not null default 0,"
						+ " vaporhightlimit      float not null default 0,"
						+ " tanktdifflimit       float not null default 0,"
						+ " minimumrefuelingvaporloss float not null default 0,"
						+ " refuelingspillrate   float not null default 0,"
						+ " refuelingspillratecv float null,"
						+ " displacedvaporratecv float null"
						+ ")";
				SQLRunner.executeSQL(outputConnection,sql);

				sql = "LOAD DATA INFILE '" + fileName + "' INTO TABLE refuelingfactorssample ("
						+ " movesrunid, iterationid"
						+ ", fueltypeid, defaultformulationid,"
						+ " vaporterma, vaportermb, vaportermc, vaportermd, vaporterme, vaportermf,"
						+ " vaporlowtlimit, vaporhightlimit, tanktdifflimit,"
						+ " minimumrefuelingvaporloss, refuelingspillrate, refuelingspillratecv,"
						+ " displacedvaporratecv"
						+ ")";
				SQLRunner.executeSQL(outputConnection, sql);

/*
				// Do ATRatioGas1 Table
				if(exportTableFilePath.exists()) {
					exportTableFilePath.delete();
				}

				sql = "SELECT " + activeRunID + ", " + nextIterationID + ","
						+ " polProcessID, fuelMYGroupID, fuelTypeID, sourceTypeID, FuelFormulationID,"
						+ " ATRatio, ATRatioCV"
						+ " INTO OUTFILE '" + fileName + "' FROM ATRatioGas1";
				SQLRunner.executeSQL(executionConnection,sql);

				sql = "CREATE TABLE IF NOT EXISTS ATRatioGas1Sample ("
						+ " MOVESRunID SMALLINT UNSIGNED NOT NULL,"
						+ " iterationID SMALLINT UNSIGNED DEFAULT 1,"
						+ " polProcessID int NOT NULL default '0',"
						+ " fuelMYGroupID int(11) default NULL,"
						+ " fuelTypeID smallint(6) default NULL,"
						+ " sourceTypeID smallint(6) NOT NULL default '0',"
						+ " FuelFormulationID smallint(6) default NULL,"
						+ " ATRatio float default NULL,"
						+ " ATRatioCV float default NULL)";
				SQLRunner.executeSQL(outputConnection,sql);

				sql = "LOAD DATA INFILE '" + fileName + "' INTO TABLE ATRatioGas1Sample "
						+ " (MOVESRunID, iterationID,  polProcessID, fuelMYGroupID,"
						+ " fuelTypeID, sourceTypeID, FuelFormulationID,"
						+ " ATRatio, ATRatioCV)";
				SQLRunner.executeSQL(outputConnection, sql);
*/
				// Do ATRatioGas2 Table
				if(exportTableFilePath.exists()) {
					exportTableFilePath.delete();
				}

				sql = "SELECT " + activeRunID + ", " + nextIterationID + ","
						+ " polprocessid, sourcetypeid, fuelsubtypeid,"
						+ " atratio, atratiocv"
						+ " INTO OUTFILE '" + fileName + "' FROM atratiogas2";
				SQLRunner.executeSQL(executionConnection,sql);

				sql = "CREATE TABLE IF NOT EXISTS atratiogas2sample ("
						+ " movesrunid smallint unsigned not null,"
						+ " iterationid smallint unsigned default 1,"
						+ " polprocessid int not null default '0',"
						+ " sourcetypeid smallint(6) not null default '0',"
						+ " fuelsubtypeid smallint(6) default null,"
						+ " atratio float default null,"
						+ " atratiocv float default null)";
				SQLRunner.executeSQL(outputConnection,sql);

				sql = "LOAD DATA INFILE '" + fileName + "' INTO TABLE atratiogas2sample "
						+ " (movesrunid, iterationid,  polprocessid, "
						+ " sourcetypeid, fuelsubtypeid,"
						+ " atratio, atratiocv)";
				SQLRunner.executeSQL(outputConnection, sql);

				// Do ATRatioNonGas Table
				if(exportTableFilePath.exists()) {
					exportTableFilePath.delete();
				}

				sql = "SELECT " + activeRunID + ", " + nextIterationID + ","
						+ " polprocessid, sourcetypeid, fuelsubtypeid,"
						+ " atratio, atratiocv"
						+ " INTO OUTFILE '" + fileName + "' FROM atrationongas";
				SQLRunner.executeSQL(executionConnection,sql);

				sql = "CREATE TABLE IF NOT EXISTS atrationongassample ("
						+ " movesrunid smallint unsigned not null,"
						+ " iterationid smallint unsigned default 1,"
						+ " polprocessid int not null default '0',"
						+ " sourcetypeid smallint(6) not null default '0',"
						+ " fuelsubtypeid smallint(6) default null,"
						+ " atratio float default null,"
						+ " atratiocv float default null)";
				SQLRunner.executeSQL(outputConnection,sql);

				sql = "LOAD DATA INFILE '" + fileName + "' INTO TABLE atrationongassample "
						+ " (movesrunid, iterationid,  polprocessid, "
						+ " sourcetypeid, fuelsubtypeid,"
						+ " atratio, atratiocv)";
				SQLRunner.executeSQL(outputConnection, sql);
			} catch (Exception e) {
				/**
				 * @issue Exception occurred on 'simulateUncertaintyInInput' [*]
				 * @explain An error occurred while generating randomized data for uncertainty
				 * analysis.
				**/
				Logger.log(LogMessageCategory.ERROR,
						"Exception occurred on 'simulateUncertaintyInInput'" + e);
			} finally {
				if (executionConnection != null) {
					DatabaseConnectionManager.checkInConnection
							(MOVESDatabaseType.EXECUTION, executionConnection);
					executionConnection = null;
				}
				if (outputConnection != null) {
					DatabaseConnectionManager.checkInConnection
							(MOVESDatabaseType.OUTPUT, outputConnection);
					outputConnection = null;
				}
				if (exportTableFilePath != null) {
					exportTableFilePath.delete();
					exportTableFilePath = null;
				}
			}
		}
	}

	/**
	 * Randomize a column value in a table.
	 * @param sqlSelectColumns Columns to select from source table(s)
	 * @param sqlSelectFrom Source(s) to select columns from.
	 * @param dataColumnIndex Index to mean and covariance columns in select, can't be zero
	 * @param sqlPrepareDestination SQL statement to prepare destination table to receive data.
	 * @param sqlLoadDestination Destination table name and columns.
	 * @param howManyColumnSets number of dataItem/dataItemCV sets in the SQL
	**/
	static void randomizeTableData(String sqlSelectColumns, String sqlSelectFrom,
			int dataColumnIndex, String sqlPrepareDestination, String sqlLoadDestination,
			int howManyColumnSets) {
		String sql = null;
		String fileName = null;
		Connection executionConnection = null;
		BufferedReader tableReader = null;
		BufferedWriter tableWriter = null;
		File originalTableFilePath = null;
		File randomizedTableFilePath = null;

		try {
			originalTableFilePath = new File(temporaryFolderPath, "original" + System.currentTimeMillis());
			originalTableFilePath.delete();
			randomizedTableFilePath = new File(temporaryFolderPath, "randomized" + System.currentTimeMillis());
			randomizedTableFilePath.delete();

			fileName = originalTableFilePath.getCanonicalPath().replace('\\', '/');

			executionConnection = DatabaseConnectionManager.checkOutConnection(
						MOVESDatabaseType.EXECUTION);

			sql = "SELECT " + sqlSelectColumns + " INTO OUTFILE '" + fileName + "' FROM "
					+ sqlSelectFrom;
			SQLRunner.executeSQL(executionConnection,sql);

			tableReader = new BufferedReader(new FileReader(originalTableFilePath));
			tableWriter = new BufferedWriter(new OutputStreamWriter(
						new FileOutputStream(randomizedTableFilePath)));

			String columns = null;
			int linenum = 0;
			while((columns=tableReader.readLine())!=null) {
				linenum++;
				StringTokenizer tokenizer = new StringTokenizer(columns, "\t");
				int columnCount = tokenizer.countTokens();
				double dataItem = 0.0;
				int dataColumnIndexCopy = dataColumnIndex;
				int howManyColumnSetsCopy = howManyColumnSets;
				for (int column=0; column < columnCount; column++) {
					String columnValue = tokenizer.nextToken();
					if(column==0) {
						tableWriter.write(columnValue);
					} else if (column==dataColumnIndexCopy) {
						try {
							if(columnValue.equalsIgnoreCase("\\N")) {
								dataItem = 0.0;
							} else {
								dataItem = Double.valueOf(columnValue).doubleValue();
							}
						} catch(Exception e) {
							dataItem = 0.0;
						}
					} else if (column==dataColumnIndexCopy+1) {
						double dataItemCV = 0.0;
						try {
							if(columnValue.equalsIgnoreCase("\\N")) {
								dataItemCV = 0.0;
							} else {
								dataItemCV = Double.valueOf(columnValue).doubleValue();
								dataItem *=
										(1.0 + dataItemCV * uncertaintyGenerator.nextGaussian());
							}
						} catch(Exception e) {
							dataItemCV = 0.0;
						}
						tableWriter.write("\t"+Double.toString(dataItem));
						tableWriter.write("\t"+columnValue);
						howManyColumnSetsCopy--;
						if(howManyColumnSetsCopy > 0) {
							dataColumnIndexCopy = column+1;
						}
					} else {
						tableWriter.write("\t"+columnValue);
					}
				}
				//tableWriter.newLine();
				tableWriter.write("\n");
			}

			tableReader.close();
			tableReader = null;
			tableWriter.close();
			tableWriter = null;

			SQLRunner.executeSQL(executionConnection, sqlPrepareDestination);
			SQLRunner.executeSQL(executionConnection, "FLUSH TABLE " + sqlSelectFrom);

			fileName = randomizedTableFilePath.getCanonicalPath().replace('\\', '/');
			sql = "LOAD DATA INFILE '" + fileName + "' INTO TABLE " + sqlLoadDestination;
			SQLRunner.executeSQL(executionConnection, sql);
		} catch (Exception e) {
			/**
			 * @issue Exception occurred on 'randomizeTableData' [*] using [*]
			 * @explain An error occurred while generating randomized data for uncertainty
			 * analysis.
			**/
			Logger.log(LogMessageCategory.ERROR,
					"Exception occurred on 'randomizeTableData' " + e
					+ " using " + sql);
			e.printStackTrace();
			if (originalTableFilePath != null) {
				String t = "";
				try {
					t = originalTableFilePath.getCanonicalPath();
				} catch(Exception e2) {
					// Nothing can be done here
				}
				originalTableFilePath = null; // don't delete, leave for debugging
				/** @nonissue **/
				Logger.log(LogMessageCategory.ERROR,t);
			}
			if (randomizedTableFilePath != null) {
				String t = "";
				try {
					t = randomizedTableFilePath.getCanonicalPath();
				} catch(Exception e2) {
					// Nothing can be done here
				}
				randomizedTableFilePath = null; // don't delete, leave for debugging
				/** @nonissue **/
				Logger.log(LogMessageCategory.ERROR,t);
			}
		} finally {
			if (executionConnection != null) {
				DatabaseConnectionManager.checkInConnection
					(MOVESDatabaseType.EXECUTION, executionConnection);
				executionConnection = null;
			}
			if (tableReader != null) {
				try {
					tableReader.close();
				} catch (IOException exception) {
					Logger.logSqlError(exception,"Unable to process exported file contents", sql);
				}
			}
			if (tableWriter != null) {
				try {
					tableWriter.close();
				} catch (IOException exception) {
					Logger.logSqlError(exception,"Unable to process exported file contents", sql);
				}
			}
			if (originalTableFilePath != null) {
				originalTableFilePath.delete();
			}
			if (randomizedTableFilePath != null) {
				randomizedTableFilePath.delete();
			}
		}
	}

	/**
	 *  Conditionally "preaggregates" Execution database depending on RunSpec
	 *  perhaps combining all counties within states
	 *  or perhaps combining all counties within nation
	**/
	public static void preAggregateExecutionDB() {
		File nationFile = null;
		File stateFile = null;
		File yearFile = null;
		File monthFile = null;
		File dayFile = null;
		Connection executionConnection = null;

		ModelScale scale = ExecutionRunSpec.theExecutionRunSpec.getModelScale();
		if(ModelScale.MACROSCALE == scale || ModelScale.MESOSCALE_LOOKUP == scale) {
			// Aggregate based on Geographic Selection Types
			GeographicSelectionType geoType = null;
			Iterator<GeographicSelection>
					i = ExecutionRunSpec.theExecutionRunSpec.getGeographicSelections().iterator();
			if (i.hasNext()) {
				geoType = ((GeographicSelection) i.next()).type;
			}
			if (geoType == GeographicSelectionType.NATION){
				// run nation aggregation script;
				/** @nonissue **/
				Logger.log(LogMessageCategory.DEBUG,
						"Running script to preaggregate to NATION level");
				if (nationFile==null) {
					nationFile = new File ("database/PreAggNATION.sql");
				}
				try {
					executionConnection = DatabaseConnectionManager.checkOutConnection
						(MOVESDatabaseType.EXECUTION);
					DatabaseUtilities.executeScript(executionConnection,nationFile);
				} catch (Exception e) {
					/**
					 * @issue Exception occurred on 'PreAggNATION.sql'
					 * @explain An error occurred while aggregating data to the nation level
					 * before doing the simulation.
					**/
					Logger.log(LogMessageCategory.ERROR,
							"Exception occurred on 'PreAggNATION.sql'" + e);
				} finally {
					if (executionConnection != null) {
						DatabaseConnectionManager.checkInConnection
							(MOVESDatabaseType.EXECUTION, executionConnection);
						executionConnection = null;
					}
				}
			}else if (geoType == GeographicSelectionType.STATE){
				// run state aggregation script;
				/** @nonissue **/
				Logger.log(LogMessageCategory.DEBUG,
						"Running script to preaggregate to STATE level");
				if (stateFile==null) {
					stateFile = new File ("database/PreAggSTATE.sql");
				}
				try {
					executionConnection = DatabaseConnectionManager.checkOutConnection
						(MOVESDatabaseType.EXECUTION);
					DatabaseUtilities.executeScript(executionConnection,stateFile);
				} catch (Exception e) {
					/**
					 * @issue Exception occurred on 'PreAggSTATE.sql'
					 * @explain An error occurred while aggregating data to the state level
					 * before doing the simulation.
					**/
					Logger.log(LogMessageCategory.ERROR,
							"Exception occurred on 'PreAggSTATE.sql'" + e);
				} finally {
					if (executionConnection != null) {
						DatabaseConnectionManager.checkInConnection
							(MOVESDatabaseType.EXECUTION, executionConnection);
						executionConnection = null;
					}
				}
			}

			// Aggregate based on TimeSpan aggregation selection.
			TimeSpan timeSpan = ExecutionRunSpec.theExecutionRunSpec.getTimeSpan();
			if(timeSpan.aggregateBy != OutputTimeStep.HOUR) {
				/** @nonissue **/
				Logger.log(LogMessageCategory.DEBUG,
						"Running script to preaggregate to DAY level");

				if (dayFile==null) {
					dayFile = new File ("database/PreAggDAY.sql");
				}
				try {
					executionConnection = DatabaseConnectionManager.checkOutConnection
						(MOVESDatabaseType.EXECUTION);
					DatabaseUtilities.executeScript(executionConnection,dayFile);
				} catch (Exception e) {
					/**
					 * @issue Exception occurred on 'PreAggDAY.sql'
					 * @explain An error occurred while aggregating data to the day level
					 * before doing the simulation.
					**/
					Logger.log(LogMessageCategory.ERROR,
								"Exception occurred on 'PreAggDAY.sql'" + e);
				} finally {
					if (executionConnection != null) {
						DatabaseConnectionManager.checkInConnection
							(MOVESDatabaseType.EXECUTION, executionConnection);
						executionConnection = null;
					}
				}

				if(timeSpan.aggregateBy.compareTo(OutputTimeStep.MONTH)>=0) {
					/** @nonissue **/
					Logger.log(LogMessageCategory.DEBUG,
							"Running script to preaggregate to MONTH level");
					if (monthFile==null) {
						monthFile = new File ("database/PreAggMONTH.sql");
					}
					try {
						executionConnection = DatabaseConnectionManager.checkOutConnection
							(MOVESDatabaseType.EXECUTION);
						DatabaseUtilities.executeScript(executionConnection,monthFile);
					} catch (Exception e) {
						/**
						 * @issue Exception occurred on 'PreAggMONTH.sql'
						 * @explain An error occurred while aggregating data to the month level
						 * before doing the simulation.
						**/
						Logger.log(LogMessageCategory.ERROR,
								"Exception occurred on 'PreAggMONTH.sql'" + e);
					} finally {
						if (executionConnection != null) {
							DatabaseConnectionManager.checkInConnection
								(MOVESDatabaseType.EXECUTION, executionConnection);
							executionConnection = null;
						}
					}
				}

				if(timeSpan.aggregateBy.compareTo(OutputTimeStep.YEAR)>=0) {
					/** @nonissue **/
					Logger.log(LogMessageCategory.DEBUG,
							"Running script to preaggregate to YEAR level");
					if (yearFile==null) {
						yearFile = new File ("database/PreAggYEAR.sql");
					}
					try {
						executionConnection = DatabaseConnectionManager.checkOutConnection
							(MOVESDatabaseType.EXECUTION);
						DatabaseUtilities.executeScript(executionConnection,yearFile);
					} catch (Exception e) {
						/**
						 * @issue Exception occurred on 'PreAggYEAR.sql'
						 * @explain An error occurred while aggregating data to the year level
						 * before doing the simulation.
						**/
						Logger.log(LogMessageCategory.ERROR,
								"Exception occurred on 'PreAggYEAR.sql'" + e);
					} finally {
						if (executionConnection != null) {
							DatabaseConnectionManager.checkInConnection
								(MOVESDatabaseType.EXECUTION, executionConnection);
							executionConnection = null;
						}
					}
				}
			}
		}
	}
	/**
	 * Append a new clause onto a partially completed SQL WHERE clause.
	 * @param wholeWhereClause SQL WHERE clause (without the "WHERE" word) that has
	 * been previously built.
	 * @param textToAppend additional clause to be AND'd onto wholeWhereClause.  Parenthesis
	 * are used to ensure embedded OR statements do not affect the logic.
	 * @return updated wholeWhereClause
	**/
	static String addToWhereClause(String wholeWhereClause,String textToAppend) {
		if(textToAppend != null && textToAppend.length() > 0) {
			if(wholeWhereClause.length() > 0) {
				wholeWhereClause = wholeWhereClause + " AND ";
			}
			wholeWhereClause = wholeWhereClause + "(" + textToAppend + ")";
		}

		return wholeWhereClause;
	}

	/**
	 * Append a set of clauses to the master set of clauses but only if the passed set
	 * is not null and not empty.
	 * @param clauseSets Vector of Vector objects where each Vector holds String
	 * objects that are fragments of the total SQL WHERE clause
	 * @param clauses Vector of String objects with new fragments for one variable
	**/
	void addToClauseSets(Vector< Vector<String> > clauseSets,Vector<String> clauses) {
		if(clauses != null && clauses.size() > 0) {
			clauseSets.add(clauses);
		}
	}

	/**
	 * Merge from a "default" database to an "execution" database.
	 * @param source The "default" database to get data from.
	 * @param destination The "execution" database to write data to.
	 * @param includeLinkTable true if the Link table should be included
	 * @param includeFuelSupply true if the fuelSupply table should be included
	 * @param isDefaultDatabase true if the source is a default database, false for user databases
	 * @throws SQLException If there is an error during any java.sql operations.
	 * @throws IOException If there is an error during any temporary file operations
	**/
	public void merge(Connection source, Connection destination, boolean includeLinkTable, 
			boolean includeFuelSupply, boolean isDefaultDatabase)
			throws SQLException, IOException, Exception {
		//System.out.println("merge(includeFuelSupply=" + includeFuelSupply + ")");

		// Validate source

		if(!isDefaultSchemaPresent(source)) {
			throw new IllegalArgumentException("Source does not have default schema.");
		}

		boolean includeEmissionRates = true;
		if(isDefaultDatabase && CompilationFlags.USE_ONLY_USER_SUPPLIED_EMISSION_RATES) {
			includeEmissionRates = false;
		}

		boolean includeHotellingActivityDistribution = true;
		if(isDefaultDatabase && ModelDomain.PROJECT == ExecutionRunSpec.getRunSpec().domain) {
			includeHotellingActivityDistribution = false;
		}

		Models models = ExecutionRunSpec.theExecutionRunSpec.getModels();
		Models.ModelCombination mc = Models.evaluateModels(models);

		DatabaseMetaData dmd ;
		String tableTypes[] = new String[ 1 ] ;
		ResultSet rs ;
		String mdTableName = "" ;

		dmd = source.getMetaData() ;
		tableTypes[ 0 ] = "TABLE" ;

		boolean includeIMCoverage = includeFuelSupply;
		if(includeIMCoverage && isDefaultDatabase) {
			// If there is already data in the destination IMCoverage table, do not import
			// the default database's IMCoverage table.
			if(DatabaseUtilities.getRowCount(destination,"imcoverage") > 0) {
				includeIMCoverage = false;
			}
		}

		boolean includeFuelUsageFraction = CompilationFlags.USE_FUELUSAGEFRACTION;
		if(includeFuelUsageFraction && isDefaultDatabase) {
			// If there is already data in the destination FuelUsageFraction table, do not import
			// the default database's FuelUsageFraction table.
			if(DatabaseUtilities.getRowCount(destination,"fuelusagefraction") > 0) {
				includeFuelUsageFraction = false;
			}
		}
		
		boolean includeSourceTypeAgeDistribution = !isDefaultDatabase || ModelDomain.SINGLE_COUNTY != ExecutionRunSpec.getRunSpec().domain;
		boolean includeSourceTypeYear = !isDefaultDatabase || ModelDomain.SINGLE_COUNTY != ExecutionRunSpec.getRunSpec().domain;

		boolean includeHPMSVtypeYear = !isDefaultDatabase || ModelDomain.SINGLE_COUNTY != ExecutionRunSpec.getRunSpec().domain;
		if(includeHPMSVtypeYear && isDefaultDatabase) {
			// If there is already data in a destination table that supplies VMT, do not
			// import the default database's HPMSVtypeYear table.
			if(DatabaseUtilities.getRowCount(destination,"hpmsvtypeday") > 0
					|| DatabaseUtilities.getRowCount(destination,"sourcetypedayvmt") > 0
					|| DatabaseUtilities.getRowCount(destination,"sourcetypeyearvmt") > 0) {
				includeHPMSVtypeYear = false;
			}
		}

		/**
		 * Inner class used to identify the tables and table rows to be copied from the "default"
		 * database to the "execution" database. This class allows table rows to be filtered by
		 * criteria specified in the Execution Runspec. Rows can be filtered by <ul><li>year</li>
		 * <li>month</li><li>link</li><li>zone</li><li>county</li><li>state</li><li>pollutant,
		 * </li><li>emission process.</li><li>day</li><li>hour</li><li>hourDayID</li>
		 * <li>roadType</li><li>pollutantProcessID</li><li>sourceUseType</li><li>fuelType</li>
		 * <li>monthGroupID</li>, and <li>fuelSubType</li>.
		 * </ul>To filter rows by runspec criteria, place the
		 * table's column names for the criteria in the corresponding entries below.
		 * <p>Note that some tables contain data that must <em>not</em> be filtered. Tables with
		 * population data for a base year must not be filtered by the analysis year as the base
		 * year data is needed to grow the table data to the analysis year. Tables that contain
		 * distribution data for an entire population must not be filtered by selected members of
		 * the population as the full population data will be needed in performing calculations.
		 * (The exception to this is "Fraction" tables, where a member's fractional value of the
		 * entire population has already been calculated).</p>
		**/

		class TableToCopy {
			/** name of the table **/
			public String tableName = null;
			/** optional name of a column that refers to a year **/
			public String yearColumnName = null;
			/** optional name of a column that refers to a monthID **/
			public String monthColumnName = null;
			/** optional name of a column that refers to a linkID **/
			public String linkColumnName = null;
			/** optional name of a column that refers to a zoneID **/
			public String zoneColumnName = null;
			/** optional name of a column that refers to a countyID **/
			public String countyColumnName = null;
			/** optional name of a column that refers to a stateID **/
			public String stateColumnName = null;
			/** optional name of a column that refers to a pollutantID **/
			public String pollutantColumnName = null;
			/** optional name of a column that refers to a processID **/
			public String processColumnName = null;
			/** optional name of a column that refers to an hourID **/
			public String hourColumnName = null;
			/** optional name of a column that refers to a dayID **/
			public String dayColumnName = null;
			/** optional name of a column that refers to a hourDayID **/
			public String hourDayIDColumnName = null;
			/** optional name of a column that refers to a road type **/
			public String roadTypeColumnName = null;
			/** optional name of a column that refers to a pollutant process **/
			public String pollutantProcessIDColumnName = null;
			/** optional name of a column that refers to a source type use  **/
			public String sourceUseTypeColumnName = null;
			/** optional name of a column that refers to a fuel type**/
			public String fuelTypeColumnName = null;
			/** optional name of a column that refers to a fuel sub type**/
			public String fuelSubTypeIDColumnName = null;
			/** optional name of a column that refers to a monthGroupID **/
			public String monthGroupIDColumnName = null;
			/**
			 * optional name that identifies a column whose values indicate
			 * whether records were user inputs.
			**/
			public String isUserInputColumnName = null;

			/** optional name of a column that refers to a fuel year **/
			public String fuelYearColumnName = null;
			/** optional name of a column that refers to a region **/
			public String regionColumnName = null;
			/** optional name of a column that refers to a model year **/
			public String modelYearColumnName = null;

			/** Constructor for filling all parameters **/
			public TableToCopy(
					String tableNameToUse,
					String yearColumnNameToUse,
					String monthColumnNameToUse,
					String linkColumnNameToUse,
					String zoneColumnNameToUse,
					String countyColumnNameToUse,
					String stateColumnNameToUse,
					String pollutantColumnNameToUse,
					String processColumnNameToUse,
					String dayColumnNameToUse,
					String hourColumnNameToUse,
					String hourDayIDColumnNameToUse,
					String roadTypeColumnNameToUse,
					String pollutantProcessIDColumnNameToUse,
					String sourceUseTypeColumnNameToUse,
					String fuelTypeColumnNameToUse,
					String fuelSubTypeIDColumnNameToUse,
					String monthGroupIDColumnNameToUse,
					String isUserInputColumnNameToUse,
					String fuelYearColumnNameToUse,
					String regionColumnNameToUse,
					String modelYearColumnNameToUse) {
				tableName = tableNameToUse;
				yearColumnName = yearColumnNameToUse;
				monthColumnName = monthColumnNameToUse;
				linkColumnName = linkColumnNameToUse;
				zoneColumnName = zoneColumnNameToUse;
				countyColumnName = countyColumnNameToUse;
				stateColumnName = stateColumnNameToUse;
				pollutantColumnName = pollutantColumnNameToUse;
				processColumnName = processColumnNameToUse;
				dayColumnName = dayColumnNameToUse;
				hourColumnName = hourColumnNameToUse;
				hourDayIDColumnName = hourDayIDColumnNameToUse;
				roadTypeColumnName = roadTypeColumnNameToUse;
				pollutantProcessIDColumnName = pollutantProcessIDColumnNameToUse;
				sourceUseTypeColumnName = sourceUseTypeColumnNameToUse;
				fuelTypeColumnName =fuelTypeColumnNameToUse;
				fuelSubTypeIDColumnName = fuelSubTypeIDColumnNameToUse;
				monthGroupIDColumnName = monthGroupIDColumnNameToUse;
				isUserInputColumnName = isUserInputColumnNameToUse;
				fuelYearColumnName = fuelYearColumnNameToUse;
				regionColumnName = regionColumnNameToUse;
				modelYearColumnName = modelYearColumnNameToUse;
			}

			/** Constructor for filling most parameters **/
			public TableToCopy(
					String tableNameToUse,
					String yearColumnNameToUse,
					String monthColumnNameToUse,
					String linkColumnNameToUse,
					String zoneColumnNameToUse,
					String countyColumnNameToUse,
					String stateColumnNameToUse,
					String pollutantColumnNameToUse,
					String processColumnNameToUse,
					String dayColumnNameToUse,
					String hourColumnNameToUse,
					String hourDayIDColumnNameToUse,
					String roadTypeColumnNameToUse,
					String pollutantProcessIDColumnNameToUse,
					String sourceUseTypeColumnNameToUse,
					String fuelTypeColumnNameToUse,
					String fuelSubTypeIDColumnNameToUse,
					String monthGroupIDColumnNameToUse,
					String isUserInputColumnNameToUse) {
				tableName = tableNameToUse;
				yearColumnName = yearColumnNameToUse;
				monthColumnName = monthColumnNameToUse;
				linkColumnName = linkColumnNameToUse;
				zoneColumnName = zoneColumnNameToUse;
				countyColumnName = countyColumnNameToUse;
				stateColumnName = stateColumnNameToUse;
				pollutantColumnName = pollutantColumnNameToUse;
				processColumnName = processColumnNameToUse;
				dayColumnName = dayColumnNameToUse;
				hourColumnName = hourColumnNameToUse;
				hourDayIDColumnName = hourDayIDColumnNameToUse;
				roadTypeColumnName = roadTypeColumnNameToUse;
				pollutantProcessIDColumnName = pollutantProcessIDColumnNameToUse;
				sourceUseTypeColumnName = sourceUseTypeColumnNameToUse;
				fuelTypeColumnName =fuelTypeColumnNameToUse;
				fuelSubTypeIDColumnName = fuelSubTypeIDColumnNameToUse;
				monthGroupIDColumnName = monthGroupIDColumnNameToUse;
				isUserInputColumnName = isUserInputColumnNameToUse;
			}
		}
		TableToCopy[] tablesAndFilterColumns = {
			new TableToCopy("agecategory",null,null,null,null,null,null,null,null,
					null,null,null,null,null,null,null,null,null,null),
			new TableToCopy("agegroup",null,null,null,null,null,null,null,null,
					null,null,null,null,null,null,null,null,null,null),
			new TableToCopy("atbaseemissions",null,null,null,null,null,null,null,null,
					null,null,null,null,"polprocessid",null,null,null,"monthgroupid",null),
			new TableToCopy("atratio",null,null,null,null,null,null,null,null,null,
					null,null,null,"polprocessid",null,"fueltypeid",null,"monthgroupid",null),
			new TableToCopy("atratiogas2",null,null,null,null,null,null,null,null,null,
					null,null,null,"polprocessid","sourcetypeid",null,"fuelsubtypeid",null,null),
			new TableToCopy("atrationongas",null,null,null,null,null,null,null,null,null,
					null,null,null,"polprocessid","sourcetypeid",null,"fuelsubtypeid",null,null),
			new TableToCopy("averagetankgasoline",null,null,null,"zoneid",null,null,null,null,
					null,null,null,null,null,null,"fueltypeid",null,"monthgroupid","isuserinput","fuelyearid",null,null),
			new TableToCopy("averagetanktemperature",null,"monthid",null,"zoneid",null,null,null,
					null,null,null,"hourdayid",null,null,null,null,null,null,"isuserinput"),
			new TableToCopy("avgspeedbin",null,null,null,null,null,null,null,null,
					null,null,null,null,null,null,null,null,null,null),
			// avgspeeddistribution cannot filter by roadtypeid or hourdayid because totalactivitygenerator will
			// not be able to calculate sourcehours properly.
			new TableToCopy("avgspeeddistribution",null,null,null,null,null,null,null,null,
					null,null,null/*"hourdayid"*/,null/*"roadtypeid"*/,null,"sourcetypeid",null,null,null,null),
			new TableToCopy("avft",null,null,null,null,null,null,null,null,
					null,null,null,null,null,"sourcetypeid",null/*"fueltypeid"*/,null,null,null),
			new TableToCopy("basefuel",null,null,null,null,null,null,null,null,
					null,null,null,null,null,null,"fueltypeid",null,null,null),
			new TableToCopy("coldsoakinitialhourfraction",null,"monthid",null,"zoneid",null,null,null,null,
					null,null,"hourdayid",null,null,"sourcetypeid",null,null,null,"isuserinput"),
			new TableToCopy("coldsoaktanktemperature",null,"monthid",null,"zoneid",null,null,null,null,
					null,"hourid",null,null,null,null,null,null,null,null),
			new TableToCopy("complexmodelparametername",null,null,null,null,null,null,null,null,
					null,null,null,null,null,null,null,null,null,null),
			new TableToCopy("complexmodelparameters",null,null,null,null,null,null,null,null,
					null,null,null,null,"polprocessid",null,null,null,null,null),

			((mc == Models.ModelCombination.M2 || mc == Models.ModelCombination.M12) ? 
					new TableToCopy("county", null, null, null, null, null, null, null, null, 
					null, null, null, null, null, null, null, null, null, null)
						: 
					new TableToCopy("county",null,null,null,null,"countyid","stateid",null,null,
					null,null,null,null,null,null,null,null,null,null)),
			new TableToCopy("countytype",null,null,null,null,null,null,null,null,
					null,null,null,null,null,null,null,null,null,null),

			new TableToCopy("countytype",null,null,null,null,null,null,null,null,
					null,null,null,null,null,null,null,null,null,null),


			((mc == Models.ModelCombination.M2 || mc == Models.ModelCombination.M12) ? 
					new TableToCopy("countyyear", null, null, null, null, null, null, null,	null, 
					null, null, null, null, null, null, null, null, null, null) 
						: 
					new TableToCopy("countyyear","yearid",null,null,null,"countyid",null,null,null,
					null,null,null,null,null,null,null,null,null,null)),

			// CrankcaseEmissionRatio can't be filtered by polProcessID because PM needs
			// NonECNonSO4PM which isn't shown on the GUI.
			new TableToCopy("crankcaseemissionratio", null, null, null, null, null, null, null, null,
					null, null, null, null, null/*"polprocessid"*/, "sourcetypeid", "fueltypeid",
					null, null, null),
			new TableToCopy("criteriaratio",null,null,null,null,null,null,null,null,
					null, null, null, null, "polprocessid", "sourcetypeid", "fueltypeid",
					null, null, null),
			new TableToCopy("cumtvvcoeffs",null,null,null,null,null,null,null,null,
					null,null,null,null,"polprocessid",null,null,null,null,null),
			new TableToCopy("datasource",null,null,null,null,null,null,null,null,
					null,null,null,null,null,null,null,null,null,null),
			new TableToCopy("dayofanyweek",null,null,null,null,null,null,null,null,
					"dayid",null,null,null,null,null,null,null,null,null),
			// dayvmtfraction cannot filter by roadtypeid or totalactivitygenerator will
			// not be able to calculate sourcehours properly.
			new TableToCopy("dayvmtfraction",null,"monthid",null,null,null,null,null,null,
					"dayid",null,null,null/*"roadtypeid"*/,null,"sourcetypeid",null,null,null,null),
			new TableToCopy("dioxinemissionrate", null, null, null, null, null, null, null, null,
					null, null, null, null, "polprocessid", null, "fueltypeid",
					null, null, null),
			new TableToCopy("driveschedule",null,null,null,null,null,null,null,null,
					null,null,null,null,null,null,null,null,null,null),
			new TableToCopy("drivescheduleassoc",null,null,null,null,null,null,null,null,
					null,null,null,"roadtypeid",null,"sourcetypeid",null,null,null,null),
			new TableToCopy("driveschedulesecond",null,null,null,null,null,null,null,null,
					null,null,null,null,null,null,null,null,null,null),
			new TableToCopy("driveschedulesecondlink",null,null, null /*"linkid"*/,null,null,null,null,
					null,null,null,null,null,null,null,null,null,null,null),
			new TableToCopy("e10fuelproperties",null,null,null,null,null,null,null,null,
					null,null,null,null,null,null,null,null,"monthgroupid",null,"fuelyearid","fuelregionid",null),
			new TableToCopy("emissionprocess",null,null,null,null,null,null,null,"processid",
					null,null,null,null,null,null,null,null,null,null),

			includeEmissionRates?
					new TableToCopy("emissionrate",null,null,null,null,null,null,null,null,
							null,null,null,null,"polprocessid",null,null,null,null,null)
					: null,
			includeEmissionRates?
					new TableToCopy("emissionratebyage",null,null,null,null,null,null,null,null,
							null,null,null,null,"polprocessid",null,null,null,null,null)
					: null,

			new TableToCopy("emissionrateadjustment",null,null,null,null,null,null,null,null,
					null,null,null,null,"polprocessid","sourcetypeid","fueltypeid",null,null,null),
			new TableToCopy("enginesize",null,null,null,null,null,null,null,null,
					null,null,null,null,null,null,null,null,null,null),
			new TableToCopy("enginetech",null,null,null,null,null,null,null,null,
					null,null,null,null,null,null,null,null,null,null),
			new TableToCopy("etohbin",null,null,null,null,null,null,null,null,
					null,null,null,null,null,null,null,null,null,null),
			new TableToCopy("evaptemperatureadjustment",null,null,null,null,null,null,null,"processid",
					null,null,null,null,null,null,null,null,null,null),
			new TableToCopy("evaprvptemperatureadjustment",null,null,null,null,null,null,null,"processid",
					null,null,null,null,null,null,"fueltypeid",null,null,null),
			new TableToCopy("extendedidlehours","yearid","monthid",null,"zoneid",null,null,null,
					null,null,null,"hourdayid",null,null,"sourcetypeid",null,null,null,
					"isuserinput"),
//			new TableToCopy("fueladjustment",null,null,null,null,null,null,null,null,null,
//					null,null,null,"polprocessid","sourcetypeid",null,/*"fuelsubtypeid"*/ null,null,null),
			// fuelengtechassoc not filtered because avft control strategy
			// intends to make control strategy objects which can be used with all runspecs
			new TableToCopy("fuelengtechassoc",null,null,null,null,null,null,null,null,
					null,null,null,null,null,null,null,null,null,null),
			new TableToCopy("fuelformulation",null,null,null,null,null,null,null,null,
					null,null,null,null,null,null,null,null,null,null),
			new TableToCopy("fuelmodelname",null,null,null,null,null,null,null,null,
					null,null,null,null,null,null,null,null,null,null),
			new TableToCopy("fuelmodelwtfactor",null,null,null,null,null,null,null,null,
					null,null,null,null,null,null,null,null,null,null),
			new TableToCopy("complexmodelparameters",null,null,null,null,null,null,null,null,
					null,null,null,null,null,null,null,null,null,null),
			new TableToCopy("fuelmodelyeargroup",null,null,null,null,null,null,null,null,
					null,null,null,null,null,null,null,null,null,null),
			new TableToCopy("fuelparametername",null,null,null,null,null,null,null,null,
					null,null,null,null,null,null,null,null,null,null),

			((mc == Models.ModelCombination.M2 || mc == Models.ModelCombination.M12) ? 
					new TableToCopy("fuelsubtype", null, null, null, null, null, null, null, null, 
					null, null, null, null, null, null, null, null, null, null)
						: 
					new TableToCopy("fuelsubtype",null,null,null,null,null,null,null,null,
					null,null,null,null,null,null,"fueltypeid",null,null,null)),

			includeFuelSupply?
				new TableToCopy("fuelsupply",null,null,null,null,null,null,null,null,
						null,null,null,null,null,null,null,null,"monthgroupid",null,"fuelyearid","fuelregionid",null)
				: null,

			new TableToCopy("fuelsupplyyear",null,null,null,null,null,null,null,null,
					null,null,null,null,null,null,null,null,null,null,"fuelyearid",null,null),
			new TableToCopy("fueltype",null,null,null,null,null,null,null,null,
					null,null,null,null,null,null,"fueltypeid",null,null,null),

			includeFuelUsageFraction?
					new TableToCopy("fuelusagefraction",null,null,null,null,"countyid",null,null,null,
					null,null,null,null,null,null,"sourcebinfueltypeid",null,null,null,"fuelyearid",null,null)
					: null,

			new TableToCopy("fuelwizardfactors",null,null,null,null,null,null,null,null,
					null,null,null,null,null,null,null,null,null,null),
			new TableToCopy("fullacadjustment",null,null,null,null,null,null,null,null,
					null,null,null,null,"polprocessid","sourcetypeid",null,null,null,null),
			new TableToCopy("generalfuelratio",null,null,null,null,null,null,"pollutantid","processid",
					null, null, null, null, "polprocessid", null, "fueltypeid", null, null, null),
			new TableToCopy("generalfuelratioexpression",null,null,null,null,null,null,null,null,
					null, null, null, null, "polprocessid", null, "fueltypeid", null, null, null),
			new TableToCopy("greetmanfanddisposal",null,null,null,null,null,null,"pollutantid",
					null,null,null,null,null,null,null,null ,null,null,null),
			/* contains "base year," or bounding, values for an analysis year so this table cannot
			 * be filtered by the year, it is filtered by pollutantID and fuelSubType */
			new TableToCopy("greetwelltopump",null,null,null,null,null,null,"pollutantid",null,
					null,null,null,null,null,null,null,"fuelsubtypeid",null,null),
			new TableToCopy("grid",null,null,null,null,null,null,null,null,
					null,null,null,null,null,null,null,null,null,null),
			new TableToCopy("gridzoneassoc",null,null,null,"zoneid",null,null,null,null,
					null,null,null,null,null,null,null,null,null,null),
			new TableToCopy("hcpermeationcoeff",null,null,null,null,null,null,null,null,
					null,null,null,null,"polprocessid",null,null,null,null,null),
			new TableToCopy("hcspeciation",null,null,null,null,null,null,null,null,
					null,null,null,null,"polprocessid",null,null,"fuelsubtypeid",null,null),

			(CompilationFlags.ENABLE_AUXILIARY_POWER_EXHAUST && includeHotellingActivityDistribution)?
					// hotellingActivityDistribution uses wildcards for zoneID, so it cannot be filtered by zone.
					new TableToCopy("hotellingactivitydistribution",null,null,null,null/*"zoneid"*/,null,null,null,null,
					null,null,null,null,null,null,null,null,null,null)
					: null,
			new TableToCopy("hotellingagefraction",null,null,null,"zoneid",null,null,null,null,
					null,null,null,null,null,null,null,null,null,null),
			CompilationFlags.ENABLE_AUXILIARY_POWER_EXHAUST?
					new TableToCopy("hotellinghours","yearid","monthid",null,"zoneid",null,null,null,
					null,null,null,"hourdayid",null,null,"sourcetypeid",null,null,null,
					"isuserinput")
					: null,

			new TableToCopy("hotellingcalendaryear","yearid",null,null,null,null,null,null,null,
					null,null,null,null,null,null,null,null,null,null),

			new TableToCopy("hotellinghourfraction",null,null,null,"zoneid",null,null,null,null,
					"dayid","hourid",null,null,null,null,null,null,null,null),
			new TableToCopy("hotellingmonthadjust",null,"monthid",null,"zoneid",null,null,null,
					null,null,null,null,null,null,null,null,null,null,null),
			new TableToCopy("hotellinghoursperday","yearid",null,null,"zoneid",null,null,null,null,
					"dayid",null,null,null,null,null,null,null,null,null),

			// hourday cannot be filtered by hourid because totalactivitygenerator requires all hours
			new TableToCopy("hourday",null,null,null,null,null,null,null,null,
					"dayid",null/*"hourid"*/,null,null,null,null,null,null,null,null),
			// hourofanyday cannot be filtered by hourid because totalactivitygenerator requires all hours
			new TableToCopy("hourofanyday",null,null,null,null,null,null,null,null,
					null,null/*"hourid"*/,null,null,null,null,null,null,null,null),
			/* hourvmtfraction cannot be filtered by sourcetypeid because info for
			 * type 21 is needed by preaggday.sql */
			// hourvmtfraction cannot filter by roadtypeid or hourid because totalactivitygenerator will
			// not be able to calculate sourcehours properly.
			new TableToCopy("hourvmtfraction",null,null,null,null,null,null,null,null,
					"dayid",null/*"hourid"*/,null,null/*"roadtypeid"*/,null,null,null,null,null,null),
			new TableToCopy("hpmsvtype",null,null,null,null,null,null,null,null,
					null,null,null,null,null,null,null,null,null,null),
			new TableToCopy("hpmsvtypeday","yearid","monthid",null,null,null,null,null,null,
					"dayid",null,null,null,null,null,null,null,null,null),

			/* Contains "base year", or bounding, values for an analysis year so this table cannot
			 * be filtered by the selected runspec criteria. */
			includeHPMSVtypeYear? new TableToCopy("hpmsvtypeyear",null,null,null,null,null,null,null,null,
					null,null,null,null,null,null,null,null,null,null)
					: null,

			includeIMCoverage?
					new TableToCopy("imcoverage","yearid",null,null,null,"countyid","stateid",null,null,
					null,null,null,null,"polprocessid","sourcetypeid","fueltypeid",null,null,null)
					: null,

			new TableToCopy("idledayadjust",null,null,null,null,null,null,null,null,
					"dayid",null,null,null,null,"sourcetypeid",null,null,null,null),
			new TableToCopy("idlemodelyeargrouping",null,null,null,null,null,null,null,null,
					null,null,null,null,null,"sourcetypeid",null,null,null,null),
			new TableToCopy("idlemonthadjust",null,"monthid",null,null,null,null,null,null,
					null,null,null,null,null,"sourcetypeid",null,null,null,null),
			new TableToCopy("idleregion",null,null,null,null,null,null,null,null,
					null,null,null,null,null,null,null,null,null,null),
			new TableToCopy("imfactor",null,null,null,null,null,null,null,null,
					null,null,null,null,"polprocessid","sourcetypeid","fueltypeid",null,null,null),
			new TableToCopy("iminspectfreq",null,null,null,null,null,null,null,null,
					null,null,null,null,null,null,null,null,null,null),
			new TableToCopy("immodelyeargroup",null,null,null,null,null,null,null,null,
					null,null,null,null,null,null,null,null,null,null),
			new TableToCopy("imteststandards",null,null,null,null,null,null,null,null,
					null,null,null,null,null,null,null,null,null,null),
			new TableToCopy("startsopmodedistribution",null,null,null /*"linkid"*/,null,null,null,null,null,
					"dayid","hourid",null,null,null,"sourcetypeid",null,null,null,"isuserinput"),
			new TableToCopy("integratedspeciesset",null,null,null,null,null,null,null,null,
					null,null,null,null,null,null,null,null,null,null),
			new TableToCopy("integratedspeciessetname",null,null,null,null,null,null,null,null,
					null,null,null,null,null,null,null,null,null,null),

			includeLinkTable?
					new TableToCopy("link",null,null,null,"zoneid","countyid",null,null,null,
					null,null,null,"roadtypeid",null,null,null,null,null,null)
					: null,

			new TableToCopy("linkaveragespeed",null,null,null /*"linkid"*/,null,null,null,null,null,
					null,null,null,null,null,null,null,null,null,null),
			new TableToCopy("linkhourvmtfraction",null,"monthid", null /*"linkid"*/,null,null,null,null,
					"dayid","hourid",null,null,null,null,"sourcetypeid",null,null,null,null),
			new TableToCopy("linksourcetypehour",null,null, null /*"linkid"*/,null,null,null,null,
					null,null,null,null,null,null,"sourcetypeid",null,null,null,null),
			new TableToCopy("lumpedspeciesname",null,null,null,null,null,null,null,null,
					null,null,null,null,null,null,null,null,null,null),
			new TableToCopy("m6sulfurcoeff",null,null,null,null,null,null,"pollutantid",
					null,null,null,null,null,null,null,null ,null,null,null),
			new TableToCopy("meanfuelparameters",null,null,null,null,null,null,null,null,
					null,null,null,null,"polprocessid",null,"fueltypeid",null,null,null),
			new TableToCopy("mechanismname",null,null,null,null,null,null,null,null,
					null,null,null,null,null,null,null,null,null,null),
			new TableToCopy("metalemissionrate", null, null, null, null, null, null, null, null,
					null, null, null, null, "polprocessid", "sourcetypeid", "fueltypeid",
					null, null, null),
			new TableToCopy("methanethcratio",null,null,null,null,null,null,null,"processid",
					null,null,null,null,null,null,null,null/*"fuelsubtypeid"*/,null,null),
			new TableToCopy("minorhapratio", null, null, null, null, null, null, null, null,
					null, null, null, null, "polprocessid", null, "fueltypeid",
					null, null, null),
			new TableToCopy("modelyear",null,null,null,null,null,null,null,null,
					null,null,null,null,null,null,null,null,null,null),
			new TableToCopy("modelyearcutpoints",null,null,null,null,null,null,null,null,
					null,null,null,null,null,null,null,null,null,null),
			new TableToCopy("modelyeargroup",null,null,null,null,null,null,null,null,
					null,null,null,null,null,null,null,null,null,null),
			new TableToCopy("modelyearmapping",null,null,null,null,null,null,null,null,
					null,null,null,null,null,null,null,null,null,null),
			new TableToCopy("monthgrouphour",null,null,null,null,null,null,null,null,
					null,"hourid",null,null,null,null,null,null,"monthgroupid",null),
			new TableToCopy("monthgroupofanyyear",null,null,null,null,null,null,null,null,
					null,null,null,null,null,null,null,null,"monthgroupid",null),
			/* aggregationsqlgenerator needs monthofanyyear to have all 12 months */
			new TableToCopy("monthofanyyear",null,null /*"monthid"*/,null,null,null,null,null,null,
					null,null,null,null,null,null,null,null,null,null),
			/* monthvmtfraction cannot be filtered by sourcetypeid because info for
			 * type 21 is needed by preaggyear.sql */
			new TableToCopy("monthvmtfraction",null,"monthid",null,null,null,null,null,null,
					null,null,null,null,null,null,null,null,null,null),
			new TableToCopy("nono2ratio",null,null,null,null,null,null,null,null,
					null,null,null,null,"polprocessid","sourcetypeid","fueltypeid",null,null,null),
			new TableToCopy("offnetworklink",null,null,null,null,null,null,null,
					null,null,null,null,null,null,"sourcetypeid",null,null,null,null),
			new TableToCopy("omdgpolprocessrepresented",null,null,null,null,null,null,null,null,
					null, null, null, null, "polprocessid", null, null, null, null, null),
			new TableToCopy("onroadretrofit",null,null,null,null,null,null,"pollutantid",
					"processid",null,null,null,null,null,"sourcetypeid","fueltypeid",null,null,null),
			new TableToCopy("operatingmode",null,null,null,null,null,null,null,null,
					null,null,null,null,null,null,null,null,null,null),
			new TableToCopy("opmodedistribution",null,null,null /*"linkid"*/,null,null,null,null,null,
					null,null,"hourdayid",null,"polprocessid","sourcetypeid",null,null,null,
					"isuserinput"),
			new TableToCopy("opmodepolprocassoc",null,null,null,null,null,null,null,null,
					null,null,null,null,"polprocessid",null,null,null,null,null),
			new TableToCopy("oxythreshname",null,null,null,null,null,null,null,null,
					null,null,null,null,null,null,null,null,null,null),
			new TableToCopy("pahgasratio", null, null, null, null, null, null, null, null,
					null, null, null, null, "polprocessid", null, "fueltypeid",
					null, null, null),
			new TableToCopy("pahparticleratio", null, null, null, null, null, null, null, null,
					null, null, null, null, "polprocessid", null, "fueltypeid",
					null, null, null),
			new TableToCopy("pm10emissionratio", null, null, null, null, null, null, null, null,
					null, null, null, null, "polprocessid", "sourcetypeid", "fueltypeid",
					null, null, null),
			new TableToCopy("pmspeciation", null, null, null, null, null, null, "outputpollutantid", "processid",
					null, null, null, null, null, "sourcetypeid", "fueltypeid", null, null, null),
			new TableToCopy("pollutantdisplaygroup",null,null,null,null,null,null,null,null,
					null,null,null,null,null,null,null,null,null,null),

			((mc == Models.ModelCombination.M2 || mc == Models.ModelCombination.M12) ? 
					new TableToCopy("pollutant", null, null, null, null, null, null, null,
					null, null, null, null, null, null, null, null, null, null, null)
						: 
					new TableToCopy("pollutant",null,null,null,null,null,null,"pollutantid",null,
					null,null,null,null,null,null,null,null,null,null)),
			((mc == Models.ModelCombination.M2 || mc == Models.ModelCombination.M12) ? 
					new TableToCopy("pollutantprocessassoc", null, null, null, null, null, null, null, 
					null, null, null, null, null, null, null, null, null, null, null) 
						: 
					new TableToCopy("pollutantprocessassoc",null,null,null,null,null,null,"pollutantid",
					"processid",null,null,null,null,"polprocessid",null,null,null,null,null)),

			new TableToCopy("pollutantprocessmodelyear",null,null,null,null,null,null,null,null,
					null,null,null,null,"polprocessid",null,null,null,null,null),
			new TableToCopy("refuelingfactors", null, null, null, null, null, null, null, null,
					null, null, null, null, null, null, "fueltypeid", null, null, null),
			new TableToCopy("regulatoryclass",null,null,null,null,null,null,null,null,
					null,null,null,null,null,null,null,null,null,null),
			new TableToCopy("region",null,null,null,null,null,null,null,null,
					null,null,null,null,null,null,null,null,null,null,null,"regionid",null),
			new TableToCopy("regioncode",null,null,null,null,null,null,null,null,
					null,null,null,null,null,null,null,null,null,null),
			new TableToCopy("regioncounty","fuelyearid",null,null,null,"countyid",null,null,null, //modifying filter to include fuel year in regioncounty filtering
					null,null,null,null,null,null,null,null,null,null,null,null,null),
			new TableToCopy("retrofitinputassociations",null,null,null,null,null,null,null,null,
					null,null,null,null,null,null,null,null,null,null),
			// roadtype cannot filter by roadtypeid or totalactivitygenerator will
			// not be able to calculate sourcehours properly.
			new TableToCopy("roadtype",null,null,null,null,null,null,null,null,
					null,null,null,null/*"roadtypeid"*/,null,null,null,null,null,null),
			/* roadtypedistribution cannot be filtered by sourcetypeid because info for
			 * type 21 is needed by preaggday.sql */
			// roadtypedistribution cannot filter by roadtypeid or totalactivitygenerator will
			// not be able to calculate sourcehours properly.
			// roadtypedistribution cannot filter by roadtypeid or activitycalculator will
			// not be able to calculate population properly.
			new TableToCopy("roadtypedistribution",null,null,null,null,null,null,null,null,
					null,null,null,null/*"roadtypeid"*/,null,null,null,null,null,null),
			new TableToCopy("samplevehicleday",null,null,null,null,null,null,null,null,
					"dayid",null,null,null,null,"sourcetypeid",null,null,null,null),
			new TableToCopy("samplevehiclesoaking",null,null,null,null,null,null,null,null,
					"dayid",null,null,null,null,"sourcetypeid",null,null,null,null),
			new TableToCopy("samplevehiclesoakingday",null,null,null,null,null,null,null,null,
					"dayid",null,null,null,null,"sourcetypeid",null,null,null,null),
			new TableToCopy("samplevehiclesoakingdayused",null,null,null,null,null,null,null,null,
					"dayid",null,null,null,null,"sourcetypeid",null,null,null,null),
			new TableToCopy("samplevehiclesoakingdaybasis",null,null,null,null,null,null,null,null,
					"dayid",null,null,null,null,null,null,null,null,null),
			new TableToCopy("samplevehiclesoakingdaybasisused",null,null,null,null,null,null,null,null,
					"dayid",null,null,null,null,null,null,null,null,null),
			new TableToCopy("samplevehicletrip",null,null,null,null,null,null,null,null,
					"dayid",null,null,null,null,null,null,null,null,null),
					
			// do not filter samplevehiclepopulation by fuel type. doing so breaks features that
			// require this distribution even when not selected in the runspec.
			new TableToCopy("samplevehiclepopulation",null,null,null,null,null,null,null,null,
					null,null,null,null,null,"sourcetypeid",null/*"fueltypeid"*/,null,null,null,null,null,"modelyearid"),

			new TableToCopy("scc",null,null,null,null,null,null,null,null,
					null,null,null,null,null,null,null,null,null,null),
			new TableToCopy("sector",null,null,null,null,null,null,null,null,
					null,null,null,null,null,null,null,null,null,null),
			new TableToCopy("sho","yearid","monthid",null /*"linkid"*/,null,null,null,null,null,
					null,null,"hourdayid",null,null,"sourcetypeid",null,null,null,"isuserinput"),
			// avft needs all of these fractions so it can move vehicles from one type
			// of fuel to another.
			new TableToCopy("sizeweightfraction",null,null,null,null,null,null,null,null,
					null,null,null,null,null,null,null/*"fueltypeid"*/,null,null,null),
			new TableToCopy("soakactivityfraction",null,"monthid",null,"zoneid",null,null,null,
					null,null,null,"hourdayid",null,null,null,null,null,null,
					"isuserinput"),
			new TableToCopy("sourcebin",null,null,null,null,null,null,null,null,
					null,null,null,null,null,null,"fueltypeid",null,null,null),
			new TableToCopy("sourcebindistribution",null,null,null,null,null,null,null,null,
					null,null,null,null,"polprocessid",null,null,null,null,"isuserinput"),
			new TableToCopy("sourcehours","yearid","monthid",null,null,null,null,null,
					null,null,null,"hourdayid",null,null,"sourcetypeid",null,null,null,
					"isuserinput"),
			new TableToCopy("sourcetypeage",null,null,null,null,null,null,null,null,
					null,null,null,null,null,null,null,null,null,null),

			/* Used in base year calculations so this table cannot be filtered */
			includeSourceTypeAgeDistribution? new TableToCopy("sourcetypeagedistribution",null,null,null,null,null,null,null,null,
					null,null,null,null,null,null,null,null,null,null)
					: null,

			new TableToCopy("sourcetypedayvmt","yearid","monthid",null,null,null,null,null,null,
					"dayid",null,null,null,null,"sourcetypeid",null,null,null,null),
					
			// sourcetypehour cannot be filtered by hour because hotelling shaping requires
			// all hours of a day. the tag filters it by day.
			new TableToCopy("sourcetypehour",null,null,null,null,null,null,null,null,
					null,null,null/*"hourdayid"*/,null,null,"sourcetypeid",null,null,null,null),

			// used in avftcontrolstrategy calculations, so do not use filter by sourcetypeid
			new TableToCopy("sourcetypemodelyear",null,null,null,null,null,null,null,null,
					null,null,null,null,null,null/*"sourcetypeid"*/,null,null,null,null,null,null,"modelyearid"),
			new TableToCopy("sourcetypemodelyeargroup",null,null,null,null,null,null,null,null,
					null,null,null,null,null,"sourcetypeid",null,null,null,null),
			new TableToCopy("sourcetypepolprocess",null,null,null,null,null,null,null,null,
					null,null,null,null,"polprocessid","sourcetypeid",null,null,null,null),
			new TableToCopy("sourcetypetechadjustment", null, null, null, null, null, null, null,
					"processid", null, null, null, null, null, "sourcetypeid", null,
					null, null, null),

			// "SourceTypeYear" Used in base year calculations so this table cannot be filtered
			// Also, sourceTypeID cannot be filtered in SourceTypeYear due to the need
			// to calculate relativeMAR in TotalActivityGenerator, a calculation that needs
			// data from all source use types.
			includeSourceTypeYear? new TableToCopy("sourcetypeyear",null,null,null,null,null,null,null,null,
					null,null,null,null,null,null,null,null,null,null)
					: null,

			new TableToCopy("sourcetypeyearvmt","yearid",null,null,null,null,null,null,null,
					null,null,null,null,null,"sourcetypeid",null,null,null,null),
			new TableToCopy("sourceusetype",null,null,null,null,null,null,null,null,
					null,null,null,null,null,null,null,null,null,null),
			new TableToCopy("sourceusetypephysics",null,null,null,null,null,null,null,null,
					null,null,null,null,null,"sourcetypeid",null,null,null,null),
			new TableToCopy("starts","yearid","monthid",null,"zoneid",null,null,null,null,
					null,null,"hourdayid",null,null,null,null,null,null,"isuserinput"),
			new TableToCopy("startsageadjustment",null,null,null,null,null,null,null,null,
					null,null,null,null,null,"sourcetypeid",null,null,null,null),
			new TableToCopy("startsperday",null,null,null,null,null,null,null,null,
					"dayid",null,null,null,null,"sourcetypeid",null,null,null,null),
			new TableToCopy("startsperdaypervehicle",null,null,null,null,null,null,null,null,
					"dayid",null,null,null,null,"sourcetypeid",null,null,null,null),
			new TableToCopy("startshourfraction",null,null,null,null,null,null,null,null,
					"dayid","hourid",null,null,null,"sourcetypeid",null,null,null,null),
			new TableToCopy("startsmonthadjust",null,"monthid",null,null,null,null,null,null,
					null,null,null,null,null,"sourcetypeid",null,null,null,null),
			new TableToCopy("startspervehicle",null,null,null,null,null,null,null,null,
					null,null,"hourdayid",null,null,"sourcetypeid",null,null,null,null),

			((mc == Models.ModelCombination.M2 || mc == Models.ModelCombination.M12) ? 
					new TableToCopy("state",null,null,null,null,null,null,null,null,
					null,null,null,null,null,null,null,null,null,null)
						:
					new TableToCopy("state",null,null,null,null,null,"stateid",null,null,
					null,null,null,null,null,null,null,null,null,null)),

			new TableToCopy("sulfateemissionrate",null,null,null,null,null,null,null,null,
					null,null,null,null,"polprocessid",null,"fueltypeid",null,null,null),
			new TableToCopy("sulfatefractions", null, null, null, null, null, null, null, "processid",
					null, null, null, null, null, "sourcetypeid", "fueltypeid", null, null, null),
			new TableToCopy("sulfurbase",null,null,null,null,null,null,null,null,
					null,null,null,null,null,null,null,null,null,null),
			new TableToCopy("sulfurcapamount",null,null,null,null,null,null,null,null,
					null,null,null,null,null,null,"fueltypeid",null,null,null),
			new TableToCopy("sulfurmodelcoeff",null,null,null,null,null,null,null,"processid",
					null,null,null,null,null,null,null,null,null,null),
			new TableToCopy("sulfurmodelname",null,null,null,null,null,null,null,null,
					null,null,null,null,null,null,null,null,null,null),
			new TableToCopy("tanktemperaturegroup",null,null,null,null,null,null,null,null,
					null,null,null,null,null,null,null,null,null,null),
			new TableToCopy("tanktemperaturerise",null,null,null,null,null,null,null,null,
					null,null,null,null,null,null,null,null,null,null),
			new TableToCopy("tankvaporgencoeffs",null,null,null,null,null,null,null,null,
					null,null,null,null,null,null,null,null,null,null),
			new TableToCopy("temperatureadjustment",null,null,null,null,null,null,null,null,
					null,null,null,null,"polprocessid",null,"fueltypeid",null,null,null),
			new TableToCopy("temperaturefactorexpression", null, null, null, null, null, null, "pollutantid", "processid",
					null, null, null, null, null, "sourcetypeid", "fueltypeid", null, null, null),
			new TableToCopy("temperatureprofileid",null,"monthid",null,"zoneid",null,null,null,null,
					null,null,null,null,null,null,null,null,null,null),
			new TableToCopy("togspeciation",null,null,null,null,null,null,null,"processid",
					null,null,null,null,null,null,null,"fuelsubtypeid",null,null),
			new TableToCopy("togspeciationprofile",null,null,null,null,null,null,null,null,
					null,null,null,null,null,null,null,null,null,null),
			new TableToCopy("togspeciationprofilename",null,null,null,null,null,null,null,null,
					null,null,null,null,null,null,null,null,null,null),
			new TableToCopy("totalidlefraction",null,"monthid",null,null,null,null,null,null,
					"dayid",null,null,null,null,"sourcetypeid",null,null,null,null),
			new TableToCopy("starttempadjustment",null,null,null,null,null,null,null,null,
					null,null,null,null,"polprocessid",null,"fueltypeid",null,null,null),
			new TableToCopy("weightclass",null,null,null,null,null,null,null,null,
					null,null,null,null,null,null,null,null,null,null),
			/* "year" contains "base year," or bounding, values for an analysis year so this table
			 * cannot be filtered by the selected runspec criteria. */
			new TableToCopy("year",null,null,null,null,null,null,null,null,
					null,null,null,null,null,null,null,null,null,null),

			((mc == Models.ModelCombination.M2 || mc == Models.ModelCombination.M12) ? 
					new TableToCopy("zone", null, null, null, null, null, null, null, null,
					null, null, null, null, null, null, null, null, null, null)
						: 
					new TableToCopy("zone",null,null,null,"zoneid","countyid",null,null,null,
					null,null,null,null,null,null,null,null,null,null)),
			((mc == Models.ModelCombination.M2 || mc == Models.ModelCombination.M12) ? 
					new TableToCopy("zonemonthhour", null, null, null, null, null, null,
					null, null, null, null, null, null, null, null, null, null, null, null) 
						: 
					new TableToCopy("zonemonthhour",null,"monthid",null,"zoneid",null,null,null,null,
					null,null/*"hourid"*/,null,null,null,null,null,null,null,null)),

			// zoneroadtype cannot filter by roadtypeid or activitycalculator will
			// not be able to calculate population properly.
			// also, the hotelling/extended idle algorithm requires data from the
			// rural restricted road type even if it is not in the runspec.
			new TableToCopy("zoneroadtype",null,null,null,"zoneid",null,null,null,null,
					null,null,null,null/*"roadTypeID"*/,null,null,null,null,null,null)
		};
		TreeSetIgnoreCase tablesToFilter = new TreeSetIgnoreCase();
		boolean allowTablesToFilter = false;
		if(mergeSession != null) {
			String[] shallowTables = {
				"year", "regioncounty", 
				"monthgroupofanyyear", "monthofanyyear"
			};
			for(int i=0;i<shallowTables.length;i++) {
				tablesToFilter.add(shallowTables[i]);
			}
			allowTablesToFilter = mergeSession.doShallowOnly;
		}
		for(int i=0;i<tablesAndFilterColumns.length;i++) {
			TableToCopy t = tablesAndFilterColumns[i];
			if(t == null) { // skip entries that are conditionally created
				continue;
			}
			if(tablesToFilter.size() > 0) {
				if(allowTablesToFilter && !tablesToFilter.contains(t.tableName)) {
					continue;
				}
				if(!allowTablesToFilter && tablesToFilter.contains(t.tableName)) {
					continue;
				}
			}
			boolean shouldLog = false; // t.tableName.equalsIgnoreCase("IMCoverage");
			//shouldLog = t.tableName.equalsIgnoreCase("FuelSupply");
			if(shouldLog) {
				Logger.log(LogMessageCategory.INFO,"InputDataManager transferring table " + t.tableName);
			}
			rs = dmd.getTables( null , "" , t.tableName , tableTypes );
			mdTableName = "" ;
			if( rs != null) {
				if (rs.next()) {
					mdTableName = rs.getString(3) ;
				}
				rs.close() ;
			}

			if ( mdTableName.length() == 0 && allowMissingTables == true ) {
				continue ;
			}

			if ( mdTableName.length() == 0 && allowMissingTables == false ) {
				Exception ex = new Exception(
						"The Table " + t.tableName + " does not exist in the "
						+ " source database " + source.getCatalog() + ". The merge is canceled." );
				throw ex ;
			}

			Vector< Vector<String> > clauseSets = new Vector< Vector<String> >();

			if(includeFuelSupply && t.tableName.equalsIgnoreCase("fuelSupply")) {
				Vector<String> fuelSupplyClauses = buildSQLWhereClauseForFuelSupply(destination);
				if(fuelSupplyClauses.size() > 0) {
					addToClauseSets(clauseSets,fuelSupplyClauses);
				}
			}
			if(t.tableName.equalsIgnoreCase("hotellingActivityDistribution")) {
				if(hasExistingData(destination,t.tableName)) {
					continue;
				}
			}
			if(t.yearColumnName != null) {
				addToClauseSets(clauseSets,
						buildSQLWhereClauseForYears(t.yearColumnName));
			}
			if(t.monthColumnName != null) {
				addToClauseSets(clauseSets,
						buildSQLWhereClauseForMonths(t.monthColumnName));
			}
			if(t.dayColumnName != null) {
				addToClauseSets(clauseSets,
						buildSQLWhereClauseForDays(t.dayColumnName));
			}
			if(t.hourColumnName != null) {
				addToClauseSets(clauseSets,
						buildSQLWhereClauseForHours(t.hourColumnName));
			}
			if(t.linkColumnName != null) {
				addToClauseSets(clauseSets,
						buildSQLWhereClauseForLinks(t.linkColumnName));
			}
			if(t.zoneColumnName != null) {
				addToClauseSets(clauseSets,
						buildSQLWhereClauseForZones(t.zoneColumnName));
			}
			if(t.countyColumnName != null) {
				addToClauseSets(clauseSets,
						buildSQLWhereClauseForCounties(t.countyColumnName));
			}
			if(t.stateColumnName != null) {
				addToClauseSets(clauseSets,
						buildSQLWhereClauseForStates(t.stateColumnName));
			}
			if(t.pollutantColumnName != null) {
				addToClauseSets(clauseSets,
						buildSQLWhereClauseForPollutants(t.pollutantColumnName));
			}
			if(t.processColumnName != null) {
				addToClauseSets(clauseSets,
						buildSQLWhereClauseForProcesses(t.processColumnName));
			}
			if(t.hourDayIDColumnName != null) {
				addToClauseSets(clauseSets,
						buildSQLWhereClauseForHourDayIDs(t.hourDayIDColumnName));
			}
			if(t.roadTypeColumnName != null) {
				addToClauseSets(clauseSets,
						buildSQLWhereClauseForRoadTypes(t.roadTypeColumnName));
			}
			if(t.pollutantProcessIDColumnName != null) {
				addToClauseSets(clauseSets,
						buildSQLWhereClauseForPollutantProcessIDs(t.pollutantProcessIDColumnName));
			}
			if(t.sourceUseTypeColumnName != null) {
				addToClauseSets(clauseSets,
						buildSQLWhereClauseForSourceUseTypes(t.sourceUseTypeColumnName));
			}
			if(t.fuelTypeColumnName != null) {
				addToClauseSets(clauseSets,
						buildSQLWhereClauseForFuelTypes(t.fuelTypeColumnName));
			}
			if(t.fuelSubTypeIDColumnName != null) {
				addToClauseSets(clauseSets,
						buildSQLWhereClauseForFuelSubTypes(t.fuelSubTypeIDColumnName));
			}
			if(t.monthGroupIDColumnName != null) {
				addToClauseSets(clauseSets,
						buildSQLWhereClauseForMonthGroupIDs(t.monthGroupIDColumnName));
			}
			if(t.fuelYearColumnName != null) {
				addToClauseSets(clauseSets,
						buildSQLWhereClauseForFuelYears(t.fuelYearColumnName));
			}
			if(t.regionColumnName != null) {
				addToClauseSets(clauseSets,
						buildSQLWhereClauseForRegions(t.regionColumnName));
			}
			if(t.modelYearColumnName != null) {
				addToClauseSets(clauseSets,
						buildSQLWhereClauseForModelYears(t.modelYearColumnName));
			}

			if(clauseSets.size() <= 0) {
if(shouldLog) System.out.println("IDM No clause sets for " + t.tableName);
				try {
					copyTable(source,destination,t.tableName,"",t.isUserInputColumnName,isDefaultDatabase);
				} catch( SQLException ex ) {
					/**
					 * @explain A database error occurred while copying a table from one
					 * database to another.
					**/
					Logger.logError(ex,"copyTable threw an SQLException" ) ;
					throw ex ;
				} catch( IOException ex ) {
					/**
					 * @explain A file system error occurred while copying a table from one
					 * database to another.
					**/
					Logger.logError(ex,"copyTable threw an IOException" ) ;
					throw ex ;
				}
			} else {
				// Issue a copyTable for each combination of entries within clauseSets
				String log = "IDM " + clauseSets.size() + " clause sets for " +
						t.tableName + " (";
				int [] counters = new int[clauseSets.size()];
				for(int j=0;j<clauseSets.size();j++) {
					counters[j] = 0;
					Vector<String> clauses = (Vector<String>)clauseSets.get(j);
					if(j > 0) {
						log += ",";
					}
					log += clauses.size();
				}
				log += ")";
				if(shouldLog) {
					System.out.println(log);
				}

				boolean done = false;
				do {
					String wholeWhereClause = "";
					for(int j=0;j<clauseSets.size();j++) {
						Vector<String> clauses = (Vector<String>)clauseSets.get(j);
						String clause = (String)clauses.get(counters[j]);
						wholeWhereClause = addToWhereClause(wholeWhereClause,clause);
					}

					log = "\t\tdoing clause (";
					for(int j=0;j<clauseSets.size();j++) {
						if(j > 0) {
							log += ",";
						}
						log += counters[j];
					}
					log += ") wholeWhereClause is " + wholeWhereClause.length() + " long";
					if(shouldLog) {
						System.out.println(log);
					}

					try {
						if(shouldLog) {
							System.out.println(wholeWhereClause);
						}
						copyTable(source,destination,t.tableName,
								wholeWhereClause,t.isUserInputColumnName,isDefaultDatabase);
					} catch( SQLException ex ) {
						/**
						 * @explain A database error occurred while copying a table from one
						 * database to another.
						**/
						Logger.logError(ex,"copyTable threw an SQLException" ) ;
						throw ex ;
					} catch( IOException ex ) {
						/**
						 * @explain A file system error occurred while copying a table from one
						 * database to another.
						**/
						Logger.logError(ex,"copyTable threw an IOException" ) ;
						throw ex ;
					}

					// Move to the next combination of clauses
					int index = 0;
					boolean doneIncrementing = false;
					while(!doneIncrementing) {
						doneIncrementing = true;
						counters[index]++;
						Vector<String> clauses = (Vector<String>)clauseSets.get(index);
						if(counters[index] >= clauses.size()) {
							doneIncrementing = false;
							counters[index] = 0;
							index++;
							if(index >= clauseSets.size()) {
								done = true;
								doneIncrementing = true;
							}
						}
					}
				} while(!done);
			}
			// Update MySQL's statistics on the table
			SQLRunner.executeSQL(destination,"ANALYZE TABLE " + t.tableName);
		}
		// Move data from staging tables to production tables.
		if(CompilationFlags.DO_RATES_FIRST) {
			String sql = "insert ignore into ratesopmodedistribution (avgspeedbinid, roadtypeid, "
					+ " sourcetypeid, hourdayid, polprocessid, opmodeid, opmodefraction, opmodefractioncv)"
					+ " select 0 as avgspeedbinid, 1 as roadtypeid, "
					+ " sourcetypeid, hourdayid, polprocessid, opmodeid, opmodefraction, opmodefractioncv"
					+ " from importstartsopmodedistribution";

			SQLRunner.executeSQL(destination,sql);
		} else {
			String sql = "insert ignore into opmodedistribution (sourcetypeid, hourdayid, linkid, polprocessid, opmodeid, opmodefraction, opmodefractioncv)"
					+ " select sourcetypeid, hourdayid, linkid, polprocessid, opmodeid, opmodefraction, opmodefractioncv"
					+ " from importstartsopmodedistribution";
			SQLRunner.executeSQL(destination,sql);
		}

		// Done
		/** @nonissue **/
		Logger.log(LogMessageCategory.INFO,"InputDataManager transferred all default tables.");
	}

	/**
	 * Merge from a NonRoad "default" database to an "execution" database.
	 * @param source The NonRoad "default" database to get data from.
	 * @param destination The "execution" database to write data to.
	 * @param isDefaultDatabase true when the default database is the source.
	 * @throws SQLException If there is an error during any java.sql operations.
	 * @throws IOException If there is an error during any temporary file operations
	**/
	public void mergeNonRoad(Connection source, Connection destination,
			boolean isDefaultDatabase)
			throws SQLException, IOException, Exception {
		if (!CompilationFlags.USE_NONROAD) {
			return;
		}

		DatabaseMetaData dmd;
		String tableTypes[] = new String[1];
		ResultSet rs;
		String mdTableName = "";

		dmd = source.getMetaData();
		tableTypes[0] = "TABLE";

		boolean includeNRFuelSupply = true;
		if(isDefaultDatabase) {
			// If there is already data in the destination nrFuelSupply table, do not import
			// the default database's nrFuelSupply table.
			if(DatabaseUtilities.getRowCount(destination,"nrfuelsupply") > 0) {
				includeNRFuelSupply = false;
			}
		}

		/**
		 * Inner class used to identify the tables and table rows to be copied
		 * from the "default" database to the "execution" database. This class
		 * allows table rows to be filtered by criteria specified in the
		 * Execution Runspec. Rows can be filtered by
		 * <ul>
		 * <li>year</li>
		 * <li>month</li>
		 * <li>link</li>
		 * <li>zone</li>
		 * <li>county</li>
		 * <li>state</li>
		 * <li>pollutant,</li>
		 * <li>emission process.</li>
		 * <li>day</li>
		 * <li>hour</li>
		 * <li>hourDayID</li>
		 * <li>roadType</li>
		 * <li>pollutantProcessID</li>
		 * <li>sourceUseType</li>
		 * <li>fuelType</li>
		 * <li>monthGroupID</li>,
		 * <li>fuelSubType</li>, and
		 * <li>sector</li>.
		 * </ul>
		 * To filter rows by runspec criteria, place the table's column names
		 * for the criteria in the corresponding entries below.
		 * <p>
		 * Note that some tables contain data that must <em>not</em> be
		 * filtered. Tables with population data for a base year must not be
		 * filtered by the analysis year as the base year data is needed to grow
		 * the table data to the analysis year. Tables that contain distribution
		 * data for an entire population must not be filtered by selected
		 * members of the population as the full population data will be needed
		 * in performing calculations. (The exception to this is "Fraction"
		 * tables, where a member's fractional value of the entire population
		 * has already been calculated).
		 * </p>
		 **/

		class NRTableToCopy {
			/** name of the table **/
			public String tableName = null;
			/** optional name of a column that refers to a year **/
			public String yearColumnName = null;
			/** optional name of a column that refers to a monthID **/
			public String monthColumnName = null;
			/** optional name of a column that refers to a zoneID **/
			public String zoneColumnName = null;
			/** optional name of a column that refers to a countyID **/
			public String countyColumnName = null;
			/** optional name of a column that refers to a stateID **/
			public String stateColumnName = null;
			/** optional name of a column that refers to a pollutantID **/
			public String pollutantColumnName = null;
			/** optional name of a column that refers to a processID **/
			public String processColumnName = null;
			/** optional name of a column that refers to an hourID **/
			public String hourColumnName = null;
			/** optional name of a column that refers to a dayID **/
			public String dayColumnName = null;
			/** optional name of a column that refers to a hourDayID **/
			public String hourDayIDColumnName = null;
			/** optional name of a column that refers to a pollutant process **/
			public String pollutantProcessIDColumnName = null;
			/** optional name of a column that refers to a source type use **/
			public String sourceUseTypeColumnName = null;
			/** optional name of a column that refers to a fuel type **/
			public String fuelTypeColumnName = null;
			/** optional name of a column that refers to a fuel sub type **/
			public String fuelSubTypeIDColumnName = null;
			/** optional name of a column that refers to a monthGroupID **/
			public String monthGroupIDColumnName = null;
			/** optional name of a column that refers to a sectorID **/
			public String sectorIDColumnName = null;
			/** optional name of a column that refers to an equipment type ID **/
			public String equipmentTypeIDColumnName = null;
			/**
			 * optional name that identifies a column whose values indicate
			 * whether records were user inputs.
			 **/
			public String isUserInputColumnName = null;

			/** Constructor for filling all parameters **/
			public NRTableToCopy(String tableNameToUse,
					String yearColumnNameToUse, String monthColumnNameToUse,
					String zoneColumnNameToUse, String countyColumnNameToUse,
					String stateColumnNameToUse,
					String pollutantColumnNameToUse,
					String processColumnNameToUse, String dayColumnNameToUse,
					String hourColumnNameToUse,
					String hourDayIDColumnNameToUse,
					String pollutantProcessIDColumnNameToUse,
					String sourceUseTypeColumnNameToUse,
					String fuelTypeColumnNameToUse,
					String fuelSubTypeIDColumnNameToUse,
					String monthGroupIDColumnNameToUse,
					String sectorIDColumnNameToUse,
					String equipmentTypeIDColumnNameToUse,
					String isUserInputColumnNameToUse) {
				tableName = tableNameToUse;
				yearColumnName = yearColumnNameToUse;
				monthColumnName = monthColumnNameToUse;
				zoneColumnName = zoneColumnNameToUse;
				countyColumnName = countyColumnNameToUse;
				stateColumnName = stateColumnNameToUse;
				pollutantColumnName = pollutantColumnNameToUse;
				processColumnName = processColumnNameToUse;
				dayColumnName = dayColumnNameToUse;
				hourColumnName = hourColumnNameToUse;
				hourDayIDColumnName = hourDayIDColumnNameToUse;
				pollutantProcessIDColumnName = pollutantProcessIDColumnNameToUse;
				sourceUseTypeColumnName = sourceUseTypeColumnNameToUse;
				fuelTypeColumnName = fuelTypeColumnNameToUse;
				fuelSubTypeIDColumnName = fuelSubTypeIDColumnNameToUse;
				monthGroupIDColumnName = monthGroupIDColumnNameToUse;
				sectorIDColumnName = sectorIDColumnNameToUse;
				equipmentTypeIDColumnName = equipmentTypeIDColumnNameToUse;
				isUserInputColumnName = isUserInputColumnNameToUse;
			}
		}

		NRTableToCopy[] tablesAndFilterColumns = {
				new NRTableToCopy("nragecategory", null, null, null, null,
						null, null, null, null, null, null, null, null, null,
						null, null, null, null, null),
				new NRTableToCopy("nratratio", null, null, null,
						null, null, "pollutantid", "processid", null, null, null,
						null, null, null, "fuelsubtypeid", null, null,
						null, null),
				new NRTableToCopy("nrbaseyearequippopulation", null, null,
						null, null, "stateid", null, null, null, null, null,
						null, "sourcetypeid", null, null, null, null, null,
						null),
				new NRTableToCopy("nrcrankcaseemissionrate", null, null, null,
						null, null, null, null, null, null, null,
						"polprocessid", "sourcetypeid", null, null, null, null,
						null, null),
				new NRTableToCopy("nrdayallocation", null, null, null, null,
						null, null, null, "dayid", null, null, null, null,
						null, null, null, null, "nrequiptypeid", null),
				new NRTableToCopy("nrdeterioration", null, null, null, null,
						null, null, null, null, null, null, "polprocessid",
						null, null, null, null, null, null, null),
				new NRTableToCopy("nrengtechfraction", null, null, null, null,
						null, null, "processid", null, null, null, null,
						"sourcetypeid", null, null, null, null, null, null),
				new NRTableToCopy("nrequipmenttype", null, null, null, null,
						null, null, null, null, null, null, null, null, null,
						null, null, "sectorid", "nrequiptypeid", null),
				new NRTableToCopy("nrevapemissionrate", null, null, null, null,
						null, null, null, null, null, null, "polprocessid",
						"sourcetypeid", null, null, null, null, null, null),
				new NRTableToCopy("nrexhaustemissionrate", null, null, null,
						null, null, null, null, null, null, null,
						"polprocessid", "sourcetypeid", null, null, null, null,
						null, null),
				new NRTableToCopy("nrfueloxyadjustment", null, null, null,
						null, null, null, null, null, null, null,
						"polprocessid", null, "fueltypeid", null, null, null,
						null, null),
				new NRTableToCopy("nrgrowthindex", "yearid", null, null, null,
						null, null, null, null, null, null, null, null, null,
						null, null, null, null, null),
				new NRTableToCopy("nrgrowthpattern", null, null, null, null,
						null, null, null, null, null, null, null, null, null,
						null, null, null, null, null),
				new NRTableToCopy("nrgrowthpatternfinder", null, null, null,
						null, "stateid", null, null, null, null, null, null,
						null, null, null, null, null, null, null),
				new NRTableToCopy("nrhcspeciation", null, null, null,
						null, null, "pollutantid", "processid", null, null, null,
						null, null, null, "fuelsubtypeid", null, null,
						null, null),
				new NRTableToCopy("nrhourallocation", null, null, null, null,
						null, null, null, null, "hourid", null, null, null,
						null, null, null, null, null, null),
				new NRTableToCopy("nrhourallocpattern", null, null, null, null,
						null, null, null, null, null, null, null, null, null,
						null, null, null, null, null),
				new NRTableToCopy("nrhourpatternfinder", null, null, null,
						null, null, null, null, null, null, null, null, null,
						null, null, null, null, "nrequiptypeid", null),
				new NRTableToCopy("nrhprangebin", null, null, null, null, null,
						null, null, null, null, null, null, null, null, null,
						null, null, null, null),
				new NRTableToCopy("nrmethanethcratio", null, null, null,
						null, null, null, "processid", null, null, null,
						null, null, null, "fuelsubtypeid", null, null,
						null, null),
				new NRTableToCopy("nrmonthallocation", null, "monthid", null,
						null, "stateid", null, null, null, null, null, null,
						null, null, null, null, null, "nrequiptypeid", null),
				new NRTableToCopy("nrusmonthallocation", null, "monthid", null,
						null, null, null, null, null, null, null, null,
						null, null, null, null, null, "nrequiptypeid", null),
				new NRTableToCopy("nrpollutantprocessmodelyear", null, null,
						null, null, null, null, null, null, null, null,
						"polprocessid", null, null, null, null, null, null,
						null),
				new NRTableToCopy("nrprocessemissionrate", null, null, null,
						null, null, null, null, null, null, null,
						"polprocessid", "sourcetypeid", null, null, null, null,
						null, null),
				new NRTableToCopy("nrscc", null, null, null, null, null, null,
						null, null, null, null, null, null, "fueltypeid", null,
						null, null, "nrequiptypeid", null),
				new NRTableToCopy("nrscrappagecurve", null, null, null, null,
						null, null, null, null, null, null, null, null, null,
						null, null, null, "nrequiptypeid", null),
				new NRTableToCopy("nrsourcebin", null, null, null, null, null,
						null, null, null, null, null, null, null, "fueltypeid",
						null, null, null, null, null),
				new NRTableToCopy("nrsourceusetype", null, null, null, null,
						null, null, null, null, null, null, null,
						"sourcetypeid", null, null, null, null, null, null),
				new NRTableToCopy("nrstatesurrogatetotal", null, null, null,
						null, "stateid", null, null, null, null, null, null,
						null, null, null, null, null, null, null),
				new NRTableToCopy("nrsulfuradjustment", null, null, null, null,
						null, null, null, null, null, null, null, null,
						"fueltypeid", null, null, null, null, null),
				new NRTableToCopy("nrsurrogate", null, null, null, null, null,
						null, null, null, null, null, null, null, null, null,
						null, null, null, null),
				new NRTableToCopy("nrtemperatureadjustment", null, null, null,
						null, null, null, null, null, null, null,
						"polprocessid", null, "fueltypeid", null, null, null,
						null, null),
				new NRTableToCopy("nrtransientadjustfactor", null, null, null,
						null, null, null, null, null, null, null,
						"polprocessid", null, "fueltypeid", null, null, null,
						"nrequiptypeid", null),
				new NRTableToCopy("nrzoneallocation", null, null, "zoneid",
						null, "stateid", null, null, null, null, null, null,
						null, null, null, null, null, null, null),

		};

		// MUST be changed later
		List<NRTableToCopy> listTableToCopy = new ArrayList<NRTableToCopy>();
		File tableFile = new File(NONROAD_TABLE_FILTER_FILE_NAME);
		BufferedReader reader = new BufferedReader(new FileReader(tableFile));
		String line = null;
		String[] cols = new String[19];
		while ((line = reader.readLine()) != null) {
			if (line.trim().isEmpty() || line.trim().startsWith("#"))
				continue;
			line = line.trim();
			int start = 0;
			int pos = line.indexOf(",", start);
			int col = 0;
			while (pos >= 0) {
				cols[col++] = line.substring(start, pos).trim();
				start = pos + 1;
				pos = line.indexOf(",", start);
			}
			cols[col++] = line.substring(start, line.length()).trim();
			if (col != 19) {
				Logger.log(LogMessageCategory.ERROR,
						"The row in NoroadTableFilter.csv is invalid: " + line);
				continue;
			}
			if (cols[0] == null || cols[0].trim().isEmpty()) {
				Logger.log(LogMessageCategory.ERROR,
						"Table name must be non-empty: " + line);
				continue;
			}
			listTableToCopy.add(new NRTableToCopy(cols[0].trim(),
					(cols[1] == null || cols[1].trim().isEmpty()) ? null
							: cols[1], (cols[2] == null || cols[2].trim()
							.isEmpty()) ? null : cols[2],
					(cols[3] == null || cols[3].trim().isEmpty()) ? null
							: cols[3], (cols[4] == null || cols[4].trim()
							.isEmpty()) ? null : cols[4],
					(cols[5] == null || cols[5].trim().isEmpty()) ? null
							: cols[5], (cols[6] == null || cols[6].trim()
							.isEmpty()) ? null : cols[6],
					(cols[7] == null || cols[7].trim().isEmpty()) ? null
							: cols[7], (cols[8] == null || cols[8].trim()
							.isEmpty()) ? null : cols[8],
					(cols[9] == null || cols[9].trim().isEmpty()) ? null
							: cols[9], (cols[10] == null || cols[10].trim()
							.isEmpty()) ? null : cols[10],
					(cols[11] == null || cols[11].trim().isEmpty()) ? null
							: cols[11], (cols[12] == null || cols[12].trim()
							.isEmpty()) ? null : cols[12],
					(cols[13] == null || cols[13].trim().isEmpty()) ? null
							: cols[13], (cols[14] == null || cols[14].trim()
							.isEmpty()) ? null : cols[14],
					(cols[15] == null || cols[15].trim().isEmpty()) ? null
							: cols[15], (cols[16] == null || cols[16].trim()
							.isEmpty()) ? null : cols[16],
					(cols[17] == null || cols[17].trim().isEmpty()) ? null
							: cols[17], (cols[18] == null || cols[18].trim()
							.isEmpty()) ? null : cols[18]));
		}
		tablesAndFilterColumns = listTableToCopy.toArray(new NRTableToCopy[0]);
		// end of MUST be changed later
		reader.close();

		for (int i = 0; i < tablesAndFilterColumns.length; i++) {
			NRTableToCopy t = tablesAndFilterColumns[i];
			if (t == null) { // skip entries that are conditionally created
				continue;
			}
			if(!includeNRFuelSupply && t.tableName.equalsIgnoreCase("nrFuelSupply")) {
				continue;
			}
			boolean shouldLog = false; // t.tableName.equalsIgnoreCase("IMCoverage");
			if (shouldLog)
				Logger.log(LogMessageCategory.INFO,
						"InputDataManager transferring NonRoad table "
								+ t.tableName);
			rs = dmd.getTables(null, "", t.tableName, tableTypes);
			mdTableName = "";
			if (rs != null) {
				if (rs.next()) {
					mdTableName = rs.getString(3);
				}
				rs.close();
			}

			if (mdTableName.length() == 0 && allowMissingTables == true) {
				continue;
			}

			if (mdTableName.length() == 0 && allowMissingTables == false) {
				Exception ex = new Exception("The Table " + t.tableName
						+ " does not exist in the " + " source database "
						+ source.getCatalog() + ". The merge is canceled.");
				throw ex;
			}

			Vector<Vector<String>> clauseSets = new Vector<Vector<String>>();

			if (t.yearColumnName != null) {
				addToClauseSets(clauseSets,
						buildSQLWhereClauseForYears(t.yearColumnName));
			}
			if (t.monthColumnName != null) {
				addToClauseSets(clauseSets,
						buildSQLWhereClauseForMonths(t.monthColumnName));
			}
			if (t.dayColumnName != null) {
				addToClauseSets(clauseSets,
						buildSQLWhereClauseForDays(t.dayColumnName));
			}
			if (t.hourColumnName != null) {
				addToClauseSets(clauseSets,
						buildSQLWhereClauseForHours(t.hourColumnName));
			}
			if (t.zoneColumnName != null) {
				addToClauseSets(clauseSets,
						buildSQLWhereClauseForZones(t.zoneColumnName));
			}
			if (t.countyColumnName != null) {
				addToClauseSets(clauseSets,
						buildSQLWhereClauseForCounties(t.countyColumnName));
			}
			if (t.stateColumnName != null) {
				addToClauseSets(clauseSets,
						buildSQLWhereClauseForStates(t.stateColumnName));
			}
			if (t.pollutantColumnName != null) {
				addToClauseSets(clauseSets,
						buildSQLWhereClauseForPollutants(t.pollutantColumnName));
			}
			if (t.processColumnName != null) {
				addToClauseSets(clauseSets,
						buildSQLWhereClauseForProcesses(t.processColumnName));
			}
			if (t.hourDayIDColumnName != null) {
				addToClauseSets(clauseSets,
						buildSQLWhereClauseForHourDayIDs(t.hourDayIDColumnName));
			}
			if (t.pollutantProcessIDColumnName != null) {
				addToClauseSets(
						clauseSets,
						buildSQLWhereClauseForPollutantProcessIDs(t.pollutantProcessIDColumnName));
			}
			if (t.sourceUseTypeColumnName != null) {
				addToClauseSets(
						clauseSets,
						buildSQLWhereClauseForNonRoadSourceUseTypes(t.sourceUseTypeColumnName));
			}
			if (t.fuelTypeColumnName != null) {
				addToClauseSets(clauseSets,
						buildSQLWhereClauseForNonroadFuelTypes(t.fuelTypeColumnName));
			}
			if (t.fuelSubTypeIDColumnName != null) {
				addToClauseSets(
						clauseSets,
						buildSQLWhereClauseForNonroadFuelSubTypes(t.fuelSubTypeIDColumnName));
			}
			if (t.monthGroupIDColumnName != null) {
				addToClauseSets(
						clauseSets,
						buildSQLWhereClauseForMonthGroupIDs(t.monthGroupIDColumnName));
			}
			if (t.sectorIDColumnName != null) {
				addToClauseSets(clauseSets,
						buildSQLWhereClauseForSectors(t.sectorIDColumnName));
			}
			if (t.equipmentTypeIDColumnName != null) {
				addToClauseSets(
						clauseSets,
						buildSQLWhereClauseForNonRoadEquipmentTypes(t.equipmentTypeIDColumnName));
			}

			if (clauseSets.size() <= 0) {
				if (shouldLog)
					System.out.println("IDM No clause sets for " + t.tableName);
				try {
					copyTable(source, destination, t.tableName, "",
							t.isUserInputColumnName,isDefaultDatabase);
				} catch (SQLException ex) {
					/**
					 * @explain A database error occurred while copying a table
					 *          from one database to another.
					 **/
					Logger.logError(ex, "copyTable threw an SQLException");
					throw ex;
				} catch (IOException ex) {
					/**
					 * @explain A file system error occurred while copying a
					 *          table from one database to another.
					 **/
					Logger.logError(ex, "copyTable threw an IOException");
					throw ex;
				}
			} else {
				// Issue a copyTable for each combination of entries within
				// clauseSets
				String log = "IDM " + clauseSets.size() + " clause sets for "
						+ t.tableName + " (";
				int[] counters = new int[clauseSets.size()];
				for (int j = 0; j < clauseSets.size(); j++) {
					counters[j] = 0;
					Vector<String> clauses = clauseSets.get(j);
					if (j > 0) {
						log += ",";
					}
					log += clauses.size();
				}
				log += ")";
				if (shouldLog)
					System.out.println(log);

				boolean done = false;
				do {
					String wholeWhereClause = "";
					for (int j = 0; j < clauseSets.size(); j++) {
						Vector<String> clauses = clauseSets.get(j);
						String clause = clauses.get(counters[j]);
						wholeWhereClause = addToWhereClause(wholeWhereClause,
								clause);
					}

					log = "\t\tdoing clause (";
					for (int j = 0; j < clauseSets.size(); j++) {
						if (j > 0) {
							log += ",";
						}
						log += counters[j];
					}
					log += ") wholeWhereClause is " + wholeWhereClause.length()
							+ " long";
					if (shouldLog)
						System.out.println(log);

					try {
						if (shouldLog)
							System.out.println(wholeWhereClause);
						copyTable(source, destination, t.tableName,
								wholeWhereClause, t.isUserInputColumnName,isDefaultDatabase);
					} catch (SQLException ex) {
						/**
						 * @explain A database error occurred while copying a
						 *          table from one database to another.
						 **/
						Logger.logError(ex, "copyTable threw an SQLException");
						throw ex;
					} catch (IOException ex) {
						/**
						 * @explain A file system error occurred while copying a
						 *          table from one database to another.
						 **/
						Logger.logError(ex, "copyTable threw an IOException");
						throw ex;
					}

					// Move to the next combination of clauses
					int index = 0;
					boolean doneIncrementing = false;
					while (!doneIncrementing) {
						doneIncrementing = true;
						counters[index]++;
						Vector<String> clauses = clauseSets.get(index);
						if (counters[index] >= clauses.size()) {
							doneIncrementing = false;
							counters[index] = 0;
							index++;
							if (index >= clauseSets.size()) {
								done = true;
								doneIncrementing = true;
							}
						}
					}
				} while (!done);
			}
			// Update MySQL's statistics on the table
			SQLRunner.executeSQL(destination, "ANALYZE TABLE " + t.tableName);
		}
		/** @nonissue **/
		Logger.log(LogMessageCategory.INFO,
				"InputDataManager transferred all NonRoad tables.");
	}

	/**
	 * Copies a table from one database to another. Used internally by merge.
	 * It is <b>not</b> an error if the table is not present in the source database.
	 * It <i>is</i> an error if the table is not present in the destination database.
	 * This allows partial databases to be imported easier.
	 * @param source The database to get data from.
	 * @param destination The database to write data to
	 * @param tableName The table name to copy over.
	 * @param whereClause The where clause to filter the rows of the source table with.
	 * @param isUserInputColumnName Column name which is been populated as 'Y' if user inputs data.
	 * @param isDefaultDatabase true when the source database is the default database.
	 * This flag affects conversion of kilometers to miles.
	 * @throws SQLException If an SQL error occurs.
	 * @throws IOException If an IO error occurs while working with a temporary data file.
	**/
	void copyTable(Connection source, Connection destination, String tableName, String whereClause,
			String isUserInputColumnName, boolean isDefaultDatabase) throws SQLException, IOException {
		String sqlConvertToMiles = "";
		String sqlConvertBackToKM = "";
		boolean needToConvertBackToKM = false;
		try {
			String updateSQL = "UPDATE " + tableName + " SET isUserInput=?";
			String alterTable = "ALTER TABLE " + tableName + " ADD ( "
					+ "isUserInput CHAR(1) NOT NULL DEFAULT 'N')";
			PreparedStatement updateStatement = source.prepareStatement(updateSQL);
			String selectSQL = "SELECT * FROM " + tableName + " LIMIT 0";
			PreparedStatement selectSourceStatement = source.prepareStatement(selectSQL);
			PreparedStatement selectDestinationStatement = destination.prepareStatement(selectSQL);
			if(!isDefaultDatabase && CompilationFlags.USE_KILOMETERS_IN_USER_DATA) {
				// Look for tables that contain distance or speed data
				String[] columns = null;
				if(tableName.equalsIgnoreCase("HPMSVtypeYear")) {
					columns = new String[] {
						"HPMSBaseYearVMT"
					};
				} else if(tableName.equalsIgnoreCase("HPMSVtypeDay")) {
					columns = new String[] {
						"VMT"
					};
				} else if(tableName.equalsIgnoreCase("SourceTypeYearVMT")) {
					columns = new String[] {
						"VMT"
					};
				} else if(tableName.equalsIgnoreCase("SourceTypeDayVMT")) {
					columns = new String[] {
						"VMT"
					};
				} else if(tableName.equalsIgnoreCase("Link")) {
					columns = new String[] {
						"linkLength", "linkAvgSpeed"
					};
				} else if(tableName.equalsIgnoreCase("LinkAverageSpeed")) {
					columns = new String[] {
						"averageSpeed"
					};
				} else if(tableName.equalsIgnoreCase("SHO")) {
					columns = new String[] {
						"distance"
					};
				} else if(tableName.equalsIgnoreCase("DriveSchedule")) {
					columns = new String[] {
						"averageSpeed"
					};
				} else if(tableName.equalsIgnoreCase("DriveScheduleSecond")) {
					columns = new String[] {
						"speed"
					};
				} else if(tableName.equalsIgnoreCase("driveScheduleSecondLink")) {
					columns = new String[] {
						"speed"
					};
				}
				if(columns != null && columns.length > 0) {
					String kmPerMile = "1.609344";
					sqlConvertToMiles = "update " + tableName + " set ";
					sqlConvertBackToKM = "update " + tableName + " set ";
					for(int i=0;i<columns.length;i++) {
						if(i > 0) {
							sqlConvertToMiles += ", ";
							sqlConvertBackToKM += ", ";
						}
						sqlConvertToMiles += columns[i] + "=" + columns[i] + "/" + kmPerMile;
						sqlConvertBackToKM += columns[i] + "=" + columns[i] + "*" + kmPerMile;
					}
				}
			}
			if(sqlConvertToMiles.length() > 0) {
				SQLRunner.executeSQL(source, sqlConvertToMiles);
				needToConvertBackToKM = true;
				// Now the user's database contains MILE data and needs to be changed back
				// to KM basis ASAP.
			}
			if(isUserInputColumnName != null) {
				ResultSet result = SQLRunner.executeQuery(selectSourceStatement, selectSQL);

				ResultSetMetaData metaData = result.getMetaData();
				boolean isUserInputColumn = false;
				for(int i=1; i<=metaData.getColumnCount(); i++) {
					if(metaData.getColumnName(i).equalsIgnoreCase("isUserInput")) {
						isUserInputColumn = true;
					}
				}
				if(!isUserInputColumn) {
					SQLRunner.executeSQL(source, alterTable);
				}
				result.close();
				selectSourceStatement.close();
				result = SQLRunner.executeQuery(selectDestinationStatement, selectSQL);
				metaData = result.getMetaData();
				isUserInputColumn = false;
				for(int i=1; i<=metaData.getColumnCount(); i++) {
					if(metaData.getColumnName(i).equalsIgnoreCase("isUserInput")) {
						isUserInputColumn = true;
					}
				}
				if(!isUserInputColumn) {
					SQLRunner.executeSQL(destination, alterTable);
				}
				result.close();
				selectSourceStatement.close();
				updateStatement.setString(1, "Y");
				SQLRunner.execute(updateStatement, updateSQL);
			}
			updateStatement.close();
			boolean sourceHadCopiedData =
					DatabaseUtilities.copyTable(source,destination,tableName,whereClause,false);
			if(mergeSession != null && sourceHadCopiedData) {
				mergeSession.add(source,tableName);
			}
		} catch( SQLException e ) {
			/**
			 * @issue copyTable threw an SQLException while Copying table [*] from [*] to [*]
			 * @explain An error occurred while moving data withi a table into another database.
			**/
			String s = "copyTable threw an SQLException while Copying table " + tableName +
					" from " + source.getCatalog() + " to " + destination.getCatalog() ;
			Logger.logError( e , s  ) ;
			throw e;
		} finally {
			if(needToConvertBackToKM && sqlConvertBackToKM.length() > 0) {
				try {
					SQLRunner.executeSQL(source, sqlConvertBackToKM);
				} catch(SQLException e) {
					String s = "copyTable threw an SQLException while converting miles to KM in table " + tableName +
							" from " + source.getCatalog();
					Logger.logError( e , s  ) ;
				}
				needToConvertBackToKM = false;
			}
		}
	} // end of copyTable method

	/** Ensure each vehID and dayID combination gets a unique vehID. **/
	public static void createUniqueVehicleIDs() {
		String[] statements = {
			"alter table samplevehicleday add column originalvehid int null",
			"alter table samplevehicletrip add column originalvehid int null",
			"alter table samplevehicletrip add column isconverted int default 0",
			"alter table samplevehicleday drop primary key",
			"alter table samplevehicleday add index (vehid, dayid)",
			"alter table samplevehicleday add column uniquevehid int not null auto_increment"
					+ ", add primary key (uniquevehid)",
			"update samplevehicleday set originalvehid=vehid",
			"update samplevehicletrip set originalvehid=vehid",
			"update samplevehicletrip, samplevehicleday"
					+ " set samplevehicletrip.vehid=samplevehicleday.uniquevehid,"
					+ " samplevehicletrip.isconverted=1"
					+ " where samplevehicletrip.dayid=samplevehicleday.dayid"
					+ " and samplevehicletrip.vehid=samplevehicleday.vehid"
					+ " and samplevehicletrip.isconverted=0",
			"update samplevehicleday set vehid=uniquevehid",
			"alter table samplevehicleday drop column uniquevehid",
			"alter table samplevehicleday drop column originalvehid",
			"alter table samplevehicletrip drop column originalvehid",
			"alter table samplevehicletrip drop column isconverted"
		};
		String sql = "";

		Connection executionConnection = null;
		try {
			executionConnection = DatabaseConnectionManager.checkOutConnection
					(MOVESDatabaseType.EXECUTION);
			for(int i=0;i<statements.length;i++) {
				sql = statements[i];
				SQLRunner.executeSQL(executionConnection, sql);
			}
		} catch(Exception e) {
			Logger.logSqlError(e,"Could not create unique vehicle IDs.", sql);
		} finally {
			if (executionConnection != null) {
				DatabaseConnectionManager.checkInConnection
						(MOVESDatabaseType.EXECUTION, executionConnection);
				executionConnection = null;
			}
		}
	}

	private static class MergedTable implements Comparable {
		public String serverName = "";
		public String databaseName = "";
		public String tableName = "";
		public long dataFileSize = 0;
		public long dataFileModified = 0;
		// Use FileTimeUtility.convertFileTimeToString(filePath.lastModified())

		public int compareTo(Object other) {
			if(!(other instanceof MergedTable)) {
				return 1;
			}
			MergedTable o = (MergedTable)other;
			int t = serverName.compareTo(o.serverName);
			if(t == 0) {
				t = databaseName.compareTo(o.databaseName);
				if(t == 0) {
					t = tableName.compareTo(o.tableName);
				}
			}
			return t;
		}
	}

	private static class MergeConnectionInformation {
		public Connection db;
		public String serverName = "";
		public String databaseName = "";
		public File dataFolder = null;
	}

	private static class MergeSession {
		TreeSet<MergedTable> merges = new TreeSet<MergedTable>();
		ArrayList<MergedTable> orderedMerges = new ArrayList<MergedTable>();
		ArrayList<MergeConnectionInformation>
				connections = new ArrayList<MergeConnectionInformation>();
		TreeMap<String,String> folderNameByServerName = new TreeMap<String,String>();

		boolean doShallowOnly = true;
		boolean didShallowTables = false;

		public void add(Connection db, String tableName) {
			MergeConnectionInformation info = getConnectionInformation(db);
			if(info == null) {
				return;
			}
			MergedTable mt = new MergedTable();
			mt.serverName = info.serverName;
			mt.databaseName = info.databaseName;
			mt.tableName = tableName;
			if(merges.contains(mt)) {
				return;
			}
			merges.add(mt);
			orderedMerges.add(mt);
			boolean gotToImportantStep = false;
			try {
				// Obtain the file's size and modification date
				if(info.dataFolder != null && info.dataFolder.exists()) {
					File sourceFolder = new File(info.dataFolder,info.databaseName);
					File sourceMYD = new File(sourceFolder,tableName.toLowerCase() + ".MYD");
					if(sourceMYD.exists()) {
						gotToImportantStep = true;
						mt.dataFileSize = sourceMYD.length();
						mt.dataFileModified = sourceMYD.lastModified();
					}
				}
			} catch(Exception e) {
				if(gotToImportantStep) {
					/**
					 * @explain While details for the files used by a database table, an
					 * error occurred.
					**/
					Logger.logError(e,"Unable to get file information for " + tableName
							+ " while tracking table usage.");
				}
			}
		}

		private MergeConnectionInformation getConnectionInformation(Connection db) {
			MergeConnectionInformation info = null;
			for(Iterator<MergeConnectionInformation> i=connections.iterator();i.hasNext();) {
				info = (MergeConnectionInformation)i.next();
				if(info.db == db) {
					return info;
				}
			}
			info = new MergeConnectionInformation();
			info.db = db;
			String sql = "";
			SQLRunner.Query query = new SQLRunner.Query();
			try {
				// Get database name and server name
				info.databaseName = db.getCatalog();
				info.serverName = db.getMetaData().getURL();
				// Example URL: jdbc:mysql://localhost/proj20090125input
				int startIndex = info.serverName.indexOf("//");
				if(startIndex >= 0) {
					int endIndex = info.serverName.indexOf("/",startIndex+2);
					if(endIndex >= 0) {
						info.serverName = info.serverName.substring(startIndex+2,endIndex);
					}
				}

				// Get and validate the data folder for the database's server
				String dataFolderName = (String)folderNameByServerName.get(info.serverName);
				if(dataFolderName == null) {
					sql = "SHOW VARIABLES";
					query.open(db,sql);
					while(query.rs.next()) {
						String name = query.rs.getString("Variable_name");
						if(name != null && name.equalsIgnoreCase("datadir")) {
							dataFolderName = query.rs.getString("Value");
							break;
						}
					}
					query.close();

					if(dataFolderName == null || dataFolderName.length() <= 0) {
						throw new SQLException("Unable to find datadir variable using SHOW VARIABLES");
					}
					folderNameByServerName.put(info.serverName,dataFolderName);
				}
				File dataFolder = new File(dataFolderName);
				if(!dataFolder.exists()) {
					dataFolder = null; // this happens when working with remote servers
					// throw new IOException("datadir (" + dataFolderName + ") does not exist");
				}
				info.dataFolder = dataFolder;

				connections.add(info);
				return info;
			} catch(Exception e) {
				/**
				 * @explain A database error occurred while gathering table details for auditing.
				**/
				Logger.logError(e,"Unable to get database details for table tracking");
				return null;
			} finally {
				query.onFinally();
			}
		}
	}

	/** Begin tracking tables used within merges. **/
	public static void startMergeSession() {
		mergeSession = new MergeSession();
		mergeSession.doShallowOnly = true;
		mergeSession.didShallowTables = false;
	}
	
	/** Authorize merging of deep tables after all key tables have been merged **/
	public static void advanceMergeSession() {
		mergeSession.doShallowOnly = false;
		mergeSession.didShallowTables = true;
	}

	/**
	 * Store the information tracked during merges into the output database.
	 * @param runID MOVESRun.MOVESRunID to be used within MOVESTablesUsed.MOVESRunID.
	**/
	public static void endMergeSession(int runID) {
		if(mergeSession == null) {
			return;
		}
		String sql = "";
		Connection outputDB = null;
		try {
			outputDB = DatabaseConnectionManager.checkOutConnection(MOVESDatabaseType.OUTPUT);
			if(outputDB == null) {
				return;
			}
			for(Iterator<MergedTable> i=mergeSession.orderedMerges.iterator();i.hasNext();) {
				MergedTable mt = (MergedTable)i.next();
				sql = "insert into movestablesused (movesrunid, databaseserver, databasename,"
						+ "tablename, datafilesize, datafilemodificationdate) "
						+ " values (" + runID
						+ "," + DatabaseUtilities.escapeSQL(mt.serverName,true)
						+ "," + DatabaseUtilities.escapeSQL(mt.databaseName,true)
						+ "," + DatabaseUtilities.escapeSQL(mt.tableName,true)
						+ "," + (mt.dataFileSize > 0? (""+mt.dataFileSize) : "null")
						+ "," + (mt.dataFileModified != 0? DatabaseUtilities.escapeSQL(
							FileTimeUtility.convertFileTimeToString(mt.dataFileModified),
							true) : "null")
						+ ")";
				SQLRunner.executeSQL(outputDB,sql);
			}
		} catch(Exception e) {
			Logger.logSqlError(e,"Unable to store table tracking details",sql);
		} finally {
			if(outputDB != null) {
				DatabaseConnectionManager.checkInConnection(MOVESDatabaseType.OUTPUT,outputDB);
				outputDB = null;
			}
		}
		mergeSession = null;
	}
} // end of InputDataManager class
