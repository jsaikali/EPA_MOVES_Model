/************************************************************************************************
 * @(#)DefaultDataMaker.java
 *
 ***********************************************************************************************/
package gov.epa.otaq.moves.master.framework;

import java.util.*;
import java.sql.*;
import java.io.*;
import gov.epa.otaq.moves.common.*;

/**
 * An instance of this class is created by a calculator. It is used to insert
 * default values into the tables for the worker.
 * It identifies the tables extracted by an SQL script, and the additional
 * tables to be extracted.
 *
 * @author		Wesley Faler
 * @author      EPA - Mitch C
 * @version		2016-11-03
**/
public class DefaultDataMaker {
	/** a TreeSetIgnoreCase of the tables extracted by the script of a calculator **/
	TreeSetIgnoreCase tablesExtractedByScript = new TreeSetIgnoreCase();
	/** a TreeSetIgnoreCase of the tables to be extracted by the script of a calculator &*/
	TreeSetIgnoreCase tablesToBeExtracted = new TreeSetIgnoreCase();

	/**
	 * Add default data to the execution database.  This is useful for things such
	 * as the FuelSupply table which is used by WTP calculators on the master-side
	 * yet must have default data present.
	**/
	public static void addDefaultDataToExecutionDatabase() {
		boolean needsNonRoad = ExecutionRunSpec.getRunSpec().models.contains(Model.NONROAD);

		Connection executionDatabase = null;
		try {
			executionDatabase = DatabaseConnectionManager.checkOutConnection(MOVESDatabaseType.EXECUTION);
		} catch(Exception e) {
			/**
			 * @explain A connection to the MOVESExecution database could not be established but
			 * was needed in order to create default fuel supply information.
			**/
			Logger.logError(e,"Unable to get the Execution Database connection needed for running"
					+ " DefaultDataMaker.");
			return;
		}

		SQLRunner.Query query = new SQLRunner.Query();
		String sql = "";
		try {
			// Fix any misaligned fuelSubtypeIDs in gasoline and ethanol fuels
			String[] formulationFixes = {
				"update fuelformulation set fuelsubtypeid = 10 where fuelsubtypeid <> 10 and etohvolume < 0.10  and fuelsubtypeid <> 11 and fuelsubtypeid in (10,11,12,13,14,15,51,52,18)",
				"update fuelformulation set fuelsubtypeid = 12 where fuelsubtypeid <> 12 and etohvolume >= 9    and etohvolume < 12.5 and fuelsubtypeid in (10,11,12,13,14,15,51,52,18)",
				"update fuelformulation set fuelsubtypeid = 13 where fuelsubtypeid <> 13 and etohvolume >= 6    and etohvolume < 9 and fuelsubtypeid in (10,11,12,13,14,15,51,52,18)",
				"update fuelformulation set fuelsubtypeid = 14 where fuelsubtypeid <> 14 and etohvolume >= 0.10 and etohvolume < 6 and fuelsubtypeid in (10,11,12,13,14,15,51,52,18)",
				"update fuelformulation set fuelsubtypeid = 15 where fuelsubtypeid <> 15 and etohvolume >= 12.5 and etohvolume < 17.5 and fuelsubtypeid in (10,11,12,13,14,15,51,52,18)",
				"update fuelformulation set fuelsubtypeid = 51 where fuelsubtypeid <> 51 and etohvolume >= 70.5 and etohvolume <= 100 and fuelsubtypeid in (10,11,12,13,14,15,51,52,18)",
				"update fuelformulation set fuelsubtypeid = 52 where fuelsubtypeid <> 52 and etohvolume >= 50.5 and etohvolume < 70.5 and fuelsubtypeid in (10,11,12,13,14,15,51,52,18)",
				"update fuelformulation set fuelsubtypeid = 18 where fuelsubtypeid <> 18 and etohvolume >= 17.5 and etohvolume < 50.5 and fuelsubtypeid in (10,11,12,13,14,15,51,52,18)"
			};
			for(int i=0;i<formulationFixes.length;i++) {
				sql = formulationFixes[i];
				SQLRunner.executeSQL(executionDatabase,sql);
			}

			//Set fuelFormulation values to 0 instead of null
			sql = getFuelFormulationNullsSQL();
			SQLRunner.executeSQL(executionDatabase,sql);

			//Calculate volToWtPercentOxy
			sql = "calculatevoltowtpercentoxy()";
			calculateVolToWtPercentOxy(executionDatabase);

			// Update T50, T90, e200, and e300
			String[] t50t90Calculations = {
				"update fuelformulation set t50 = 2.0408163 * (147.91 - e200) where e200 is not null and e200 > 0 and (t50 is null or t50 <= 0)",
				"update fuelformulation set t90 = 4.5454545 * (155.47 - e300) where e300 is not null and e300 > 0 and (t90 is null or t90 <= 0)",
				"update fuelformulation set e200 = 147.91-(t50/2.0408163) where t50 is not null and t50 > 0 and (e200 is null or e200 <= 0)",
				"update fuelformulation set e300 = 155.47-(t90/4.5454545) where t90 is not null and t90 > 0 and (e300 is null or e300 <= 0)"
				/*
				"update nrFuelFormulation set T50 = 2.0408163 * (147.91 - e200) where e200 is not null and e200 > 0 and (T50 is null or T50 <= 0)",
				"update nrFuelFormulation set T90 = 4.5454545 * (155.47 - e300) where e300 is not null and e300 > 0 and (T90 is null or T90 <= 0)",
				"update nrFuelFormulation set e200 = 147.91-(T50/2.0408163) where T50 is not null and T50 > 0 and (e200 is null or e200 <= 0)",
				"update nrFuelFormulation set e300 = 155.47-(T90/4.5454545) where T90 is not null and T90 > 0 and (e300 is null or e300 <= 0)"
				*/
			};
			for(int i=0;i<t50t90Calculations.length;i++) {
				sql = t50t90Calculations[i];
				SQLRunner.executeSQL(executionDatabase,sql);
			}

			//Remove fuelSupply records with zero market shares.  Their presence causes unneeded
			//overhead when joining to the fuelSupply table.
			sql = "delete from fuelsupply where marketshare < 0.0001";
			SQLRunner.executeSQL(executionDatabase,sql);

			//use default values for the Marketshare field  if no record is found in the
			//FuelSupply table for a given county, year, monthgroup, and fueltype.
			// As first step, create a lists of fuelYears and fuelTypes relevant to the run specification
			sql = "DROP TABLE IF EXISTS runspecfuelyear";
			SQLRunner.executeSQL(executionDatabase,sql);

			sql = "CREATE TABLE runspecfuelyear " +
				  "SELECT DISTINCT fuelyearid from year inner join runspecyear using(yearid)";
			SQLRunner.executeSQL(executionDatabase,sql);

			sql = "DROP TABLE IF EXISTS runspecfueltype";
			SQLRunner.executeSQL(executionDatabase,sql);

			sql = "CREATE TABLE runspecfueltype select distinct fueltypeid from runspecsourcefueltype union select distinct fueltypeid from runspecsectorfueltype";
			SQLRunner.executeSQL(executionDatabase,sql);

			TreeSet<String> defaultFuelMessages = new TreeSet<String>();
			boolean[] isNonroadTables = { false, true };
			String[] fuelSupplyTables = { "fuelsupply", "nrfuelsupply" };
			String[] fuelTypeTables = { "fueltype", "nrfueltype" };
			String[] fuelSubTypeTables = { "fuelsubtype", "nrfuelsubtype" };
			for(int i=0;i<fuelSupplyTables.length;i++) {
				if(needsNonRoad != isNonroadTables[i]) {
					continue;
				}
				// as second step, create a list of fuelTypes by county, fuelYear, and MonthGroup
				// which have a non-default fuel supply, making list relevant to the run specification
				sql = "DROP TABLE IF EXISTS givenfuelsupply";
				SQLRunner.executeSQL(executionDatabase,sql);
	
				sql = "CREATE TABLE givenfuelsupply " +
					"select distinct fs.fuelregionid, fs.fuelyearid, fs.monthgroupid, fst.fueltypeid " +
					"FROM " + fuelSupplyTables[i] + " fs " +
					"INNER JOIN runspecfuelregion rsc on fs.fuelregionid=rsc.fuelregionid " +
					"inner join runspecfuelyear rsfy on fs.fuelyearid=rsfy.fuelyearid " +
					"inner join runspecmonthgroup rsmg on fs.monthgroupid=rsmg.monthgroupid " +
					"inner join fuelformulation ff on fs.fuelformulationid=ff.fuelformulationid " +
					"INNER JOIN " + fuelSubTypeTables[i] + " fst on ff.fuelsubtypeid=fst.fuelsubtypeid ";
				SQLRunner.executeSQL(executionDatabase,sql);
	
				// as third step create a list of fuelTypes, also by county, fuelYear, and monthGroup
				//   which need a fuel supply
				sql = "DROP TABLE IF EXISTS neededfuelsupply";
				SQLRunner.executeSQL(executionDatabase,sql);
	
				sql = "CREATE TABLE neededfuelsupply " +
					"select fuelregionid, fuelyearid, monthgroupid, " +
						"ft.fueltypeid, defaultformulationid " +
					" from runspecfuelregion " +
					" cross join runspecfuelyear " +
					" cross join runspecmonthgroup " +
					" cross join runspecfueltype " +
					" INNER JOIN " + fuelTypeTables[i] + " ft ON runspecfueltype.fueltypeid = ft.fueltypeid ";
				SQLRunner.executeSQL(executionDatabase,sql);
	
				sql = "create unique index xpkgivenfuelsupply on givenfuelsupply ("
						+ " fuelregionid, fuelyearid, monthgroupid, fueltypeid)";
				SQLRunner.executeSQL(executionDatabase,sql);
	
				sql = "create unique index xpkneededfuelsupply on neededfuelsupply ("
						+ " fuelregionid, fuelyearid, monthgroupid, fueltypeid)";
				SQLRunner.executeSQL(executionDatabase,sql);
	
				// as fourth and final step, insert needed-but-missing records into FuelSupply
				sql = "INSERT INTO " + fuelSupplyTables[i] + " (fuelregionid, fuelyearid, monthgroupid, " +
					"fuelformulationid, marketshare, marketsharecv) " +
					"select nfs.fuelregionid, nfs.fuelyearid, nfs.monthgroupid, " +
					"nfs.defaultformulationid, 1.0, 0.0 " +
					"from neededfuelsupply nfs  left join givenfuelsupply gfs " +
					"using(fuelregionid, fuelyearid, monthgroupid, fueltypeid) " +
					"where gfs.fueltypeid is null ";
				SQLRunner.executeSQL(executionDatabase,sql);
	
				sql = "ANALYZE TABLE " + fuelSupplyTables[i];
				SQLRunner.executeSQL(executionDatabase,sql);
	
				// Issue warnings for each default fuel that had to be added
				sql = "SELECT nfs.fuelregionid, nfs.fuelyearid, nfs.monthgroupid, " +
					"nfs.defaultformulationid, fueltypedesc " +
					"from neededfuelsupply nfs " +
					"inner join " + fuelTypeTables[i] + " ft using (fueltypeid) " +
					"left join givenfuelsupply gfs " +
					"using(fuelregionid, fuelyearid, monthgroupid, fueltypeid) " +
					"where gfs.fueltypeid is null and ft.fueltypeid <> 9 " + // give no warning about default formulation for electricity
					"order by nfs.fuelregionid, nfs.fuelyearid, nfs.monthgroupid, fueltypedesc";
				query.open(executionDatabase,sql);
				while(query.rs.next()) {
					int regionID = query.rs.getInt(1);
					int fuelYearID = query.rs.getInt(2);
					int monthGroupID = query.rs.getInt(3);
					int defaultFormulationID = query.rs.getInt(4);
					String fuelTypeDescription = query.rs.getString(5);
					String message = "WARNING: Using default formulation " + defaultFormulationID
							+ " for " + fuelTypeDescription
							+ " in region " + regionID
							+ ", fuel year " + fuelYearID
							+ ", month group " + monthGroupID
							+ " in the " + fuelSupplyTables[i] + " table.";
					if(!defaultFuelMessages.contains(message)) {
						defaultFuelMessages.add(message);
						Logger.log(LogMessageCategory.WARNING,message);
					}
				}
				query.close();
			}

			/*
			//use default values for the fuel adjustment field  if no record is found in the
			//FuelAdjustment table for a given pollutant-process-modelYearGroup, sourceTypeID, and
			//fuelFormulationID.
			sql = "INSERT INTO FuelAdjustment ("
					+ " polProcessID,"
					+ " fuelMYGroupID,"
					+ " sourceTypeID,"
					+ " fuelFormulationID,"
					+ " fuelAdjustment,"
					+ " fuelAdjustmentCV,"
					+ " fuelAdjustmentGPA,"
					+ " fuelAdjustmentGPACV)"
					+ " SELECT"
					+ " rspp.polProcessID,"
					+ " fmyg.fuelMYGroupID,"
					+ " rssft.sourceTypeID,"
					+ " ff.fuelFormulationID,"
					+ " 1, 0, 1, 0"
					+ " FROM RunSpecPollutantProcess rspp"
					+ " INNER JOIN FuelModelYearGroup fmyg"
					+ " INNER JOIN RunSpecSourceFuelType rssft"
					+ " INNER JOIN FuelSubType fst ON fst.fuelTypeID = rssft.fuelTypeID"
					+ " INNER JOIN FuelFormulation ff ON ff.fuelSubTypeID = fst.fuelSubTypeID"
					+ " LEFT JOIN FuelAdjustment fa ON (fa.polProcessID = rspp.polProcessID"
					+ " AND fa.fuelMYGroupID = fmyg.fuelMYGroupID"
					+ " AND fa.sourceTypeID = rssft.sourceTypeID"
					+ " AND fa.fuelFormulationID = ff.fuelFormulationID)"
					+ " WHERE fa.polProcessID IS NULL";

			SQLRunner.executeSQL(executionDatabase,sql);

			sql = "ANALYZE TABLE FuelAdjustment";
			SQLRunner.executeSQL(executionDatabase,sql);
			*/

			sql = "insert ignore into countyyear ("
					+ " countyid, yearid, refuelingvaporprogramadjust, refuelingspillprogramadjust)"
					+ " select countyid, yearid, 0.0, 0.0"
					+ " from runspeccounty, runspecyear";
			SQLRunner.executeSQL(executionDatabase,sql);

			sql = "insert into temperatureprofileid (temperatureprofileid, zoneid, monthid)"
					+ " select distinct (zoneid*10000)+(monthid*100) as temperatureprofileid, zoneid, monthid"
					+ " from zonemonthhour"
					+ " where not exists ("
					+ " select *"
					+ " from temperatureprofileid"
					+ " where temperatureprofileid.zoneid=zonemonthhour.zoneid"
					+ " and temperatureprofileid.monthid=zonemonthhour.monthid)";
			SQLRunner.executeSQL(executionDatabase,sql);

			sql = "create table if not exists nrmodelyear like modelyear";
			SQLRunner.executeSQL(executionDatabase,sql);
			for(int i=1940;i<2060;i++) {
				sql = "insert ignore into nrmodelyear (modelyearid) values (" + i + ")";
				SQLRunner.executeSQL(executionDatabase,sql);
			}
		} catch(SQLException e) {
			/**
			 * @explain An error occurred while creating default records.
			**/
			Logger.logError(e,"Unable to create default data");
		} finally {
			query.onFinally();
		}

		DatabaseConnectionManager.checkInConnection(
			MOVESDatabaseType.EXECUTION, executionDatabase);
		executionDatabase = null;
	}

	/**
	 * Obtain SQL to change any NULL-valued columns into 0-valued columns in the fuelFormulation table.
	**/
	public static String getFuelFormulationNullsSQL() {
		String[] columnNames = { "rvp","sulfurlevel","etohvolume","mtbevolume","etbevolume",
				"tamevolume","aromaticcontent","olefincontent","benzenecontent","e200","e300",
				"voltowtpercentoxy","biodieselestervolume","cetaneindex","pahcontent","t50","t90"
		};
		String sql = "update fuelformulation set ";
		for(int i=0;i<columnNames.length;i++) {
			if(i > 0) {
				sql += ",";
			}
			sql += columnNames[i] + "=ifnull(" + columnNames[i] + ",0)";
		}
		return sql;
	}

	/**
	 * Adds the tables extracted by the calculator script to tablesExtractedByScript
	 * @param tableName The name of the table to be added to the TreeSet
	**/
	public void addTableExtractedByScript(String tableName) {
		String t = tableName.toLowerCase();
		tablesExtractedByScript.add(t);
	}

	/**
	 * Adds the tables to be extracted by the calculator script to the
	 * tablesToBeExtracted TreeSet. These tables are related and are identified
	 * by tablesExtractedByScript.
	**/
	public void determineAllTablesToExtractAndCreate() {
		if(tablesExtractedByScript.contains("temperatureadjustment")) {
			tablesToBeExtracted.add("runspecsourcefueltype");
			tablesToBeExtracted.add("pollutantprocessassoc");
		}

		// Disregard any tables already being handled by the script
		for(Iterator i=tablesExtractedByScript.iterator();i.hasNext();) {
			String tableName = (String)i.next();
			tablesToBeExtracted.remove(tableName);
		}
	}

	/**
	 * Creates a linked list of the SQLs which stores default data into the
	 * tables identified by a calculator
	 * @return the Linked List of the SQLs to store default values into
	 * the tables used in a calculator
	**/
	public LinkedList<String> getTableCreationSQL() {
		LinkedList<String> result = new LinkedList<String>();
		String sql;

		for(Iterator<String> i=tablesToBeExtracted.iterator();i.hasNext();) {
			String tableName = (String)i.next();
			sql = (String)DatabaseConnectionManager.executionDatabaseCreateTableStatements.get(tableName);
			result.add(sql);
			sql = "TRUNCATE TABLE " + tableName + ";";
			result.add(sql);
		}

		/*
		// Give the total set of pollutant/process selections to the external calculator. Just
		// the file is needed. It doesn't need to be loaded into a real table.
		sql = (String)DatabaseConnectionManager.executionDatabaseCreateTableStatements.get("RunSpecPollutantProcess");
		sql = StringUtilities.replace(sql,"RunSpecPollutantProcess","extpollutantprocess");
		result.add(sql);
		sql = "TRUNCATE TABLE extpollutantprocess;";
		result.add(sql);
		*/

		return result;
	}

	/**
	 * Creates a linked list of the SQLs to extract data from the
	 * tablesToBeExtracted TreeSet into output flat files
	 * @return the Linked List of the SQLs to extract data from the
	 * tablesToBeExtracted TreeSet into output flat files
	**/
	public LinkedList<String> getDataExtractionSQL() {
		LinkedList<String> result = new LinkedList<String>();
		String sql;

		for(Iterator<String> i=tablesToBeExtracted.iterator();i.hasNext();) {
			String tableName = (String)i.next();
			sql = "SELECT * INTO OUTFILE '##" + tableName.toLowerCase() + "##' FROM " + tableName + ";";
			result.add(sql);
		}

		String[] externalCalculatorStatements = {
			"cache select "
			+ " ##context.iterlocation.staterecordid## as stateid,"
			+ " ##context.iterlocation.countyrecordid## as countyid,"
			+ " ##context.iterlocation.zonerecordid## as zoneid,"
			+ " ##context.iterlocation.linkrecordid## as linkid,"
			+ " ##context.year## as yearid,"
			+ " ##context.monthid## as monthid"
			+ " into outfile '##extconstants##';",

			// give the total set of pollutant/process selections to the external calculator. just
			// the file is needed. it doesn't need to be loaded into a real table.
			"cache select * into outfile '##extpollutantprocess##' from runspecpollutantprocess;",

			"cache select ageid, agegroupid"
			+ " into outfile '##extagecategory##'"
			+ " from agecategory;",

			"cache select ##context.iterlocation.countyrecordid##, ##context.year##, ##context.monthid##, "
			+ " 		fst.fueltypeid, fst.fuelsubtypeid, ff.fuelformulationid, fs.marketshare"
			+ " into outfile '##extfuelsupply##'"
			+ " from year"
			+ " inner join fuelsupply fs on (fs.fuelyearid=year.fuelyearid)"
			+ " inner join monthofanyyear moay on (moay.monthgroupid=fs.monthgroupid)"
			+ " inner join fuelformulation ff on (ff.fuelformulationid=fs.fuelformulationid)"
			+ " inner join fuelsubtype fst on (fst.fuelsubtypeid=ff.fuelsubtypeid)"
			+ " where yearid = ##context.year##"
			+ " and fs.fuelregionid = ##context.fuelregionid##"
			+ " and moay.monthid = ##context.monthid##"
			+ " and fst.fueltypeid in (##macro.csv.all.fueltypeid##);",

			"cache select ##context.iterlocation.countyrecordid##, ##context.year##, ##context.monthid##,"
			+ " 	fst.fueltypeid, fst.fuelsubtypeid, ff.fuelformulationid, fs.marketshare"
			+ " into outfile '##extnrfuelsupply##'"
			+ " from year"
			+ " inner join nrfuelsupply fs on (fs.fuelyearid=year.fuelyearid)"
			+ " inner join monthofanyyear moay on (moay.monthgroupid=fs.monthgroupid)"
			+ " inner join fuelformulation ff on (ff.fuelformulationid=fs.fuelformulationid)"
			+ " inner join nrfuelsubtype fst on (fst.fuelsubtypeid=ff.fuelsubtypeid)"
			+ " where yearid = ##context.year##"
			+ " and fs.fuelregionid = ##context.fuelregionid##"
			+ " and moay.monthid = ##context.monthid##"
			+ " and fst.fueltypeid in (##macro.csv.all.nrfueltypeid##);",

			"cache select fueltypeid, humiditycorrectioncoeff, fueldensity, subjecttoevapcalculations"
			+ " into outfile '##extfueltype##'"
			+ " from fueltype;",

			"cache select fueltypeid, humiditycorrectioncoeff, fueldensity, subjecttoevapcalculations"
			+ " into outfile '##extnrfueltype##'"
			+ " from nrfueltype;",
			
			"cache select fuelsubtypeid, fueltypeid, fuelsubtypepetroleumfraction, fuelsubtypefossilfraction,"
			+ " 	carboncontent, oxidationfraction, energycontent"
			+ " into outfile '##extfuelsubtype##'"
			+ " from fuelsubtype;",

			"cache select fuelsubtypeid, fueltypeid, fuelsubtypepetroleumfraction, fuelsubtypefossilfraction,"
			+ " 	carboncontent, oxidationfraction, energycontent"
			+ " into outfile '##extnrfuelsubtype##'"
			+ " from nrfuelsubtype;",
			
			"cache select distinct"
			+ " 	fuelformulation.fuelformulationid,"
			+ " 	fuelformulation.fuelsubtypeid,"
			+ " 	ifnull(fuelformulation.rvp,0),"
			+ " 	ifnull(fuelformulation.sulfurlevel,0),"
			+ " 	ifnull(fuelformulation.etohvolume,0),"
			+ " 	ifnull(fuelformulation.mtbevolume,0),"
			+ " 	ifnull(fuelformulation.etbevolume,0),"
			+ " 	ifnull(fuelformulation.tamevolume,0),"
			+ " 	ifnull(fuelformulation.aromaticcontent,0),"
			+ " 	ifnull(fuelformulation.olefincontent,0),"
			+ " 	ifnull(fuelformulation.benzenecontent,0),"
			+ " 	ifnull(fuelformulation.e200,0),"
			+ " 	ifnull(fuelformulation.e300,0),"
			+ " 	ifnull(fuelformulation.voltowtpercentoxy,0),"
			+ " 	ifnull(fuelformulation.biodieselestervolume,0),"
			+ " 	ifnull(fuelformulation.cetaneindex,0),"
			+ " 	ifnull(fuelformulation.pahcontent,0),"
			+ " 	ifnull(fuelformulation.t50,0),"
			+ " 	ifnull(fuelformulation.t90,0)"
			+ " into outfile '##extfuelformulation##'"
			+ " from fuelformulation"
			+ " where fuelformulation.fuelformulationid in ("
			+ " 	select distinct ff.fuelformulationid"
			+ " 	from fuelsupply"
			+ " 	inner join runspecmonthgroup on (fuelsupply.monthgroupid = runspecmonthgroup.monthgroupid)"
			+ " 	inner join year on (fuelsupply.fuelyearid = year.fuelyearid)"
			+ " 	inner join fuelformulation ff on (ff.fuelformulationid = fuelsupply.fuelformulationid)"
			+ " 	inner join fuelsubtype on (fuelsubtype.fuelsubtypeid = ff.fuelsubtypeid)"
			+ " 	inner join monthofanyyear on (monthofanyyear.monthgroupid = fuelsupply.monthgroupid)"
			+ " 	inner join runspecmonth on (runspecmonth.monthid = monthofanyyear.monthid)"
			+ " 	where fuelregionid = ##context.fuelregionid##"
			+ " 	and yearid = ##context.year##"
			+ " 	and monthofanyyear.monthid = ##context.monthid##"
			+ " 	and fuelsubtype.fueltypeid in (##macro.csv.all.fueltypeid##)"
			+ " );",

			"cache select distinct"
			+ " 	fuelformulation.fuelformulationid,"
			+ " 	fuelformulation.fuelsubtypeid,"
			+ " 	ifnull(fuelformulation.rvp,0),"
			+ " 	ifnull(fuelformulation.sulfurlevel,0),"
			+ " 	ifnull(fuelformulation.etohvolume,0),"
			+ " 	ifnull(fuelformulation.mtbevolume,0),"
			+ " 	ifnull(fuelformulation.etbevolume,0),"
			+ " 	ifnull(fuelformulation.tamevolume,0),"
			+ " 	ifnull(fuelformulation.aromaticcontent,0),"
			+ " 	ifnull(fuelformulation.olefincontent,0),"
			+ " 	ifnull(fuelformulation.benzenecontent,0),"
			+ " 	ifnull(fuelformulation.e200,0),"
			+ " 	ifnull(fuelformulation.e300,0),"
			+ " 	ifnull(fuelformulation.voltowtpercentoxy,0),"
			+ " 	ifnull(fuelformulation.biodieselestervolume,0),"
			+ " 	ifnull(fuelformulation.cetaneindex,0),"
			+ " 	ifnull(fuelformulation.pahcontent,0),"
			+ " 	ifnull(fuelformulation.t50,0),"
			+ " 	ifnull(fuelformulation.t90,0)"
			+ " into outfile '##extnrfuelformulation##'"
			+ " from fuelformulation as fuelformulation"
			+ " where fuelformulation.fuelformulationid in ("
			+ " 	select distinct ff.fuelformulationid"
			+ " 	from nrfuelsupply as fuelsupply"
			+ " 	inner join runspecmonthgroup on (fuelsupply.monthgroupid = runspecmonthgroup.monthgroupid)"
			+ " 	inner join year on (fuelsupply.fuelyearid = year.fuelyearid)"
			+ " 	inner join fuelformulation ff on (ff.fuelformulationid = fuelsupply.fuelformulationid)"
			+ " 	inner join nrfuelsubtype as fuelsubtype on (fuelsubtype.fuelsubtypeid = ff.fuelsubtypeid)"
			+ " 	inner join monthofanyyear on (monthofanyyear.monthgroupid = fuelsupply.monthgroupid)"
			+ " 	inner join runspecmonth on (runspecmonth.monthid = monthofanyyear.monthid)"
			+ " 	where fuelregionid = ##context.fuelregionid##"
			+ " 	and yearid = ##context.year##"
			+ " 	and monthofanyyear.monthid = ##context.monthid##"
			+ " 	and fuelsubtype.fueltypeid in (##macro.csv.all.nrfueltypeid##)"
			+ " );",

			"cache select scc, nrequiptypeid, fueltypeid"
			+ " into outfile '##extnrscc##'"
			+ " from nrscc;",

			"cache select nrhprangebinid, engtechid, nrhpcategory"
			+ " into outfile '##extnrhpcategory##'"
			+ " from nrhpcategory;"
		};
		for(int i=0;i<externalCalculatorStatements.length;i++) {
			result.add(externalCalculatorStatements[i]);
		}

		return result;
	}

	/**
	 * Creates a linked list of the SQLs to extract data from the
	 * tablesToBeExtracted TreeSet into input flat files
	 * @return the Linked List of the SQLs to extract data from the
	 * tablesToBeExtracted TreeSet into input flat files
	**/
	public LinkedList<String> getRemoteProcessingSQL() {
		LinkedList<String> result = new LinkedList<String>();
		String sql;

		// Do LOAD DATA INFILE statements first
		for(Iterator<String> i=tablesToBeExtracted.iterator(); i.hasNext();) {
			String tableName = (String)i.next();
			tableName = tableName.toLowerCase();
			sql = "LOAD DATA INFILE '##" + tableName + "##' INTO TABLE "
					+ tableName + ";";
			result.add(sql);

			sql = "ANALYZE TABLE " + tableName + ";";
			result.add(sql);
		}

		// use TempAdjustTermA, TempAdjustTermB and TempAdjustTermC values of 0.0 for a given
		// pollutant/process and fueltype if no record is found in the TemperatureAdjustment
		// table for a particular combination of these key values in the Temperature Adjustment
		// table.
		if(tablesExtractedByScript.contains("temperatureadjustment")) {

// !!! Gwo Shyu - Start of Change, 04/09/2014

			sql = "drop table if exists tmpsfppa;";
			result.add(sql);
			sql = "create table tmpsfppa select * from runspecsourcefueltype, pollutantprocessassoc;";
			result.add(sql);
			sql = "alter table tmpsfppa add index ndxstftpolprocess (sourcetypeid,fueltypeid, polprocessid);";
			result.add(sql);
			sql = "alter table tmpsfppa add index ndxfueltypepolprocess (fueltypeid, polprocessid);";
			result.add(sql);
			sql = "INSERT IGNORE INTO temperatureadjustment ( "
					+	"polprocessid, fueltypeid, "
					+	"tempadjustterma, tempadjusttermacv, "
					+	"tempadjusttermb, tempadjusttermbcv, "
					+	"tempadjusttermc, tempadjusttermccv, "
					+	"minmodelyearid, maxmodelyearid ) "
					+"select distinct "
					+	"polprocessid, fueltypeid, "
					+	"0, 0, 0, 0, 0, 0, 1960, 2060 "
					+"from "
					+	"tmpsfppa "
					+	"left outer join temperatureadjustment using (fueltypeid, polprocessid) "
					+	"where temperatureadjustment.polprocessid is null;";

/*
			sql = "INSERT IGNORE INTO TemperatureAdjustment ( "
					+	"polProcessID, fuelTypeID, "
					+	"tempAdjustTermA, tempAdjustTermACV, "
					+	"tempAdjustTermB, tempAdjustTermBCV, "
					+	"tempAdjustTermC, tempAdjustTermCCV, "
					+	"minModelYearID, maxModelYearID ) "
					+"SELECT DISTINCT "
					+	"polProcessID, fuelTypeID, "
					+	"0, 0, 0, 0, 0, 0, 1960, 2060 "
					+"FROM "
					+	"RunSpecSourceFuelType "
					+	"inner join PollutantProcessAssoc "
					+	"left outer join TemperatureAdjustment using (fuelTypeID, polProcessID) "
					+	"where TemperatureAdjustment.polProcessID is null;";
*/
// !!! End of change

			result.add(sql);

			sql = "ANALYZE TABLE temperatureadjustment;";
			result.add(sql);
		}

		return result;
	}

	/**
	 * Creates a linked list of the SQLs to remove tables extracted
	 * @return the Linked List of the SQLs to delete the tables extracted
	**/
	public LinkedList<String> getRemoteCleanupSQL() {
		LinkedList<String> result = new LinkedList<String>();
		String sql;
		for(Iterator<String> i=tablesToBeExtracted.iterator(); i.hasNext();) {
			String tableName = (String)i.next();
			sql = "DROP TABLE IF EXISTS " + tableName;
			result.add(sql);
		}
		
		sql = "DROP TABLE IF EXISTS extpollutantprocess;";
		result.add(sql);

		return result;
	}

	/**
	 * Update the FuelFormulation table's volToWtPercentOxy field.
	 * @param db database to be used
	 * @throws SQLException if anything goes wrong
	**/
	public static void calculateVolToWtPercentOxy(Connection db) throws SQLException {
		String[] tableNames = { "fuelformulation" }; // , "nrFuelFormulation" };
		for(int i=0;i<tableNames.length;i++) {
			String sql = "update " + tableNames[i] + " set voltowtpercentoxy="
					+ " case when (etohvolume+mtbevolume+etbevolume+tamevolume) > 0 then"
					+ " 	(etohvolume*0.3653"
					+ " 	+ mtbevolume*0.1792"
					+ " 	+ etbevolume*0.1537"
					+ " 	+ tamevolume*0.1651) / (etohvolume+mtbevolume+etbevolume+tamevolume)"
					+ " else 0"
					+ " end";
			SQLRunner.executeSQL(db,sql);
		}
	}
}
