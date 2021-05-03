/**************************************************************************************************
 * @(#)SourceTypePhysics.java
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
 * Utilities to create modelyear-specific VSP and operating modes.
 *
 * @author		Wesley Faler
 * @version		2017-04-08
**/
public class SourceTypePhysics {
	/** Flags for tasks already done, used to prevent duplicate execution **/
	TreeSet<String> alreadyDoneFlags = new TreeSet<String>();
	/** Flags for error messages already reported, used to prevent duplicate messages **/
	TreeSet<String> alreadyDoneMessages = new TreeSet<String>();

	/**
	 * Populate the sourceUseTypePhysicsMapping table, making it appropriate for use with VSP creation.
	 * @param db database connection to use
	 * @throws SQLException if anything goes wrong
	**/
	public void setup(Connection db) throws SQLException {
		String alreadyKey = "setup";
		if(alreadyDoneFlags.contains(alreadyKey)) {
			return;
		}
		alreadyDoneFlags.add(alreadyKey);

		SQLRunner.Query query = new SQLRunner.Query();
		String sql = "";
		try {
			// Fill the mapping table with basic data
			String[] statements = {
				"drop table if exists sourceusetypephysicsmapping",

				"create table sourceusetypephysicsmapping ("
						+ " 	realsourcetypeid smallint not null,"
						+ " 	tempsourcetypeid smallint not null,"
						+ " 	regclassid smallint not null,"
						+ " 	beginmodelyearid smallint not null,"
						+ " 	endmodelyearid smallint not null,"
						+ " 	opmodeidoffset smallint not null,"

						+ " 	rollingterma float default null,"
						+ " 	rotatingtermb float default null,"
						+ " 	dragtermc float default null,"
						+ " 	sourcemass float default null,"
						+ " 	fixedmassfactor float default null,"

						+ " 	primary key (realsourcetypeid, regclassid, beginmodelyearid, endmodelyearid),"
						+ " 	key (beginmodelyearid, endmodelyearid, realsourcetypeid, regclassid),"
						+ " 	key (tempsourcetypeid, realsourcetypeid, regclassid, beginmodelyearid, endmodelyearid)"
						+ ")",

				"insert ignore into sourceusetypephysicsmapping (realsourcetypeid, tempsourcetypeid, regclassid, "
						+ " 	beginmodelyearid, endmodelyearid, opmodeidoffset,"
						+ " 	rollingterma, rotatingtermb, dragtermc, sourcemass, fixedmassfactor)"
						+ " select sourcetypeid as realsourcetypeid, sourcetypeid as tempsourcetypeid, regclassid,"
						+ " 	beginmodelyearid, endmodelyearid, 0 as opmodeidoffset,"
						+ " 	rollingterma, rotatingtermb, dragtermc, sourcemass, fixedmassfactor"
						+ " from sourceusetypephysics"
						+ " join runspecmodelyear rsmy"
						+ " where rsmy.modelyearid between beginmodelyearid and endmodelyearid"
			};
			for(int i=0;i<statements.length;i++) {
				sql = statements[i];
				SQLRunner.executeSQL(db,sql);
			}
			// Create temporary source types and assign op mode mapping
			sql = "select * from sourceusetypephysicsmapping order by realsourcetypeid, regclassid, beginmodelyearid";
			ArrayList<String> updateStatements = new ArrayList<String>();
			int nextTempSourceTypeID = 100;
			int nextOpModeIDOffset = 1000;
			int priorRealSourceTypeID = 0;
			int priorRegClassID = 0;
			int priorBeginModelYearID = 0;
			boolean isFirst = true;
			query.open(db,sql);
			while(query.rs.next()) {
				int realSourceTypeID = query.rs.getInt("realsourcetypeid");
				int regClassID = query.rs.getInt("regclassid");
				int beginModelYearID = query.rs.getInt("beginmodelyearid");
				
				/*
				A new sourceTypeID should be assigned for each combination of [realSourceTypeID, regClassID, beginModelYearID].
				Each realSourceTypeID gets its own offset sequence, starting at 1000 and adding 100 for each [regClassID, beginModelYearID] combination.
				*/
				if(!isFirst && realSourceTypeID == priorRealSourceTypeID && (regClassID != priorRegClassID || beginModelYearID != priorBeginModelYearID)) {
					sql = "update sourceusetypephysicsmapping set tempsourcetypeid=" + nextTempSourceTypeID
							+ ", opmodeidoffset=" + nextOpModeIDOffset
							+ " where realsourcetypeid=" + realSourceTypeID
							+ " and regclassid=" + regClassID
							+ " and beginmodelyearid=" + beginModelYearID;
					updateStatements.add(sql);
					nextTempSourceTypeID++;
					nextOpModeIDOffset += 100; // Opmodes used are 0-99, so an offset of 100 won't overlap
				} else {
					// Leave the current row using the original sourceTypeID tied to the original operating modes.
					// The next modelyear break for this [sourceTypeID, regClassID] will get new operating modes.

					sql = "update sourceusetypephysicsmapping set tempsourcetypeid=" + nextTempSourceTypeID
							+ ", opmodeidoffset=" + nextOpModeIDOffset
							+ " where realsourcetypeid=" + realSourceTypeID
							+ " and regclassid=" + regClassID
							+ " and beginmodelyearid=" + beginModelYearID;
					updateStatements.add(sql);
					nextTempSourceTypeID++;
					nextOpModeIDOffset += 100; // Opmodes used are 0-99, so an offset of 100 won't overlap
				}
				isFirst = false;
				priorRealSourceTypeID = realSourceTypeID;
				priorRegClassID = regClassID;
				priorBeginModelYearID = beginModelYearID;
			}
			query.close();
			for(Iterator<String> i=updateStatements.iterator();i.hasNext();) {
				sql = i.next();
				SQLRunner.executeSQL(db,sql);
			}
		} catch(SQLException e) {
			Logger.logError(e,"unable to setup sourceusetypephysicsmapping");
			throw e;
		} finally {
			query.onFinally();
		}
	}

	/**
	 * Update an operating mode distribution table, replacing temporary source types with
	 * real source types and changing real operating modes to modelyear-specific modes.
	 * @param db database connection to use
	 * @param tableName table containing an operating mode distribution and using standard
	 * column names of "sourcetypeid" and "opmodeid".
	 * @throws SQLException if anything goes wrong
	**/
	public void updateOperatingModeDistribution(Connection db, String tableName) throws SQLException {
		updateOperatingModeDistribution(db,tableName,null);
	}

	/**
	 * Update an operating mode distribution table, replacing temporary source types with
	 * real source types and changing real operating modes to modelyear-specific modes.
	 * @param db database connection to use
	 * @param tableName table containing an operating mode distribution and using standard
	 * column names of "sourcetypeid" and "opmodeid".
	 * @param whereClause optional string containing an expression to add to the WHERE clause
	 * used to update the table.  Use this to improve performance when incrementally constructing
	 * a table.
	 * @throws SQLException if anything goes wrong
	**/
	public void updateOperatingModeDistribution(Connection db, String tableName, String whereClause) throws SQLException {
		String alreadyKey = "updateomd|" + tablename + "|" + StringUtilities.safeGetString(whereClause);
		if(alreadyDoneFlags.contains(alreadyKey)) {
			//return;
		}
		alreadyDoneFlags.add(alreadyKey);

		SQLRunner.Query query = new SQLRunner.Query();
		String sql = "";
		try {
			sql = "select distinct realsourcetypeid, tempsourcetypeid, opmodeidoffset"
					+ " from sourceusetypephysicsmapping"
					+ " join (select distinct sourcetypeid,linkid from opmodedistribution) omd"
					+ " on (omd.sourcetypeid = realsourcetypeid or omd.sourcetypeid = tempsourcetypeid)"
					+ " where realsourcetypeid <> tempsourcetypeid";
					if(whereClause != null && whereClause.length() > 0) {
						sql += " and (" + whereclause + ")";
					}
					sql += " order by realsourcetypeid, beginmodelyearid";
			ArrayList<String> updateStatements = new ArrayList<String>();
			query.open(db,sql);
			
			while(query.rs.next()) {	
				int realSourceTypeID = query.rs.getInt("realsourcetypeid");
				int tempSourceTypeID = query.rs.getInt("tempsourcetypeid");
				int opModeIDOffset = query.rs.getInt("opmodeidoffset");

				// Change source types for any new operating modes
				sql = "update " + tablename + " set sourcetypeid=" + realSourceTypeID
						+ " where sourcetypeid=" + tempSourceTypeID
						+ " and opmodeid >= 0+" + opmodeidoffset + " and opmodeid < 100+" + opModeIDOffset
						+ " and (polprocessid < 0 or mod(polprocessid,100) = 1)";
				if(whereClause != null && whereClause.length() > 0) {
					sql += " and (" + whereclause + ")";
				}
				updateStatements.add(sql);
				
				sql = "create table if not exists physics_" + tableName
						+ " like " + tableName;
				updateStatements.add(sql);

				// Change source types for brakewear
				sql = "truncate physics_" + tableName;
				updateStatements.add(sql);
				
				
				sql = "insert into physics_" + tableName
						+ " select * from " + tableName
						+ " where sourcetypeid=" + tempSourceTypeID
						+ " and polprocessid = 11609";
				if(whereClause != null && whereClause.length() > 0) {
					sql += " and (" + whereclause + ")";
				}
				updateStatements.add(sql);
				
				sql = "delete from " + tableName
						+ " where sourcetypeid=" + tempSourceTypeID
						+ " and polprocessid = 11609";
				if(whereClause != null && whereClause.length() > 0) {
					sql += " and (" + whereclause + ")";
				}
				updateStatements.add(sql);

				sql = "update physics_" + tablename + " set sourcetypeid=" + realSourceTypeID
					+ ", opmodeid=opmodeid+ " + opModeIDOffset
					+ " where opmodeid <> 501";
				if(whereClause != null && whereClause.length() > 0) {
					sql += " and (" + whereclause + ")";
				}
				updateStatements.add(sql);

				sql = "insert ignore into " + tablename + " select * from physics_" + tableName;
				updateStatements.add(sql);

				// Promote old operating modes and change source types
				// This statement fail (which is ok and can be ignored) if
				// entries exist with extended operating modes already.
				sql = "update " + tablename + " set sourcetypeid=" + realSourceTypeID
						+ ", opmodeid=opmodeid + " + opModeIDOffset
						+ " where sourcetypeid=" + tempSourceTypeID
						+ " and opmodeid >= 0 and opmodeid < 100"
						+ " and (polprocessid < 0 or mod(polprocessid,100) = 1)";
				if(whereClause != null && whereClause.length() > 0) {
					sql += " and (" + whereclause + ")";
				}
				updateStatements.add(sql);
				
				if(opModeIDOffset > 0) {
					sql = "delete from " + tableName
							+ " where sourcetypeid=" + tempSourceTypeID
							+ " and opmodeid >= 0 and opmodeid < 100"
							+ " and (polprocessid < 0 or mod(polprocessid,100) = 1)"
							+ " and isuserinput = 'N'";
					if(whereClause != null && whereClause.length() > 0) {
						sql += " and (" + whereclause + ")";
					}
					updateStatements.add(sql);

					// tempSourceTypeID never equals realSourceTypeID any more, so get rid of real source type operating modes
					sql = "delete from " + tableName
							+ " where sourcetypeid=" + realSourceTypeID
							+ " and opmodeid >= 0 and opmodeid < 100"
							+ " and (polprocessid < 0 or mod(polprocessid,100) = 1)"
							+ " and isuserinput = 'N'";
					if(whereClause != null && whereClause.length() > 0) {
						sql += " and (" + whereclause + ")";
					}
					updateStatements.add(sql);
				}
			}
			query.close();
			for(Iterator<String> i=updateStatements.iterator();i.hasNext();) {
				sql = i.next();
				try {
					SQLRunner.executeSQL(db,sql);
				} catch(SQLException e) {
					// Nothing to do here
				}
			}

		} catch(SQLException e) {
			Logger.logError(e,"unable to updateoperatingmodedistribution");
			throw e;
		} finally {
			query.onFinally();
		}
	}

	/**
	 * Update an operating mode distribution table, replacing temporary source types with
	 * real source types and changing real operating modes to modelyear-specific modes.
	 * @param db database connection to use
	 * @param tableName table containing an operating mode distribution and using standard
	 * column names of "sourcetypeid" and "opmodeid".
	 * @param whereClause optional string containing an expression to add to the WHERE clause
	 * used to update the table.  Use this to improve performance when incrementally constructing
	 * a table.
	 * @throws SQLException if anything goes wrong
	**/
	public void updateOpModes(Connection db, String tableName) throws SQLException {
		String alreadyKey = "updateopmodes|" + tableName;
		if(alreadyDoneFlags.contains(alreadyKey)) {
			//return;
		}
		alreadyDoneFlags.add(alreadyKey);

		SQLRunner.Query query = new SQLRunner.Query();
		String sql = "";
		TreeSet<Integer> seenRealSourceTypeIDs = new TreeSet<Integer>();
		try {
			sql = "select distinct realsourcetypeid, tempsourcetypeid, opmodeidoffset"
					+ " from sourceusetypephysicsmapping"
					+ " where realsourcetypeid <> tempsourcetypeid"
					+ " order by realsourcetypeid, beginmodelyearid";
			ArrayList<String> updateStatements = new ArrayList<String>();
			query.open(db,sql);
			while(query.rs.next()) {
				int realSourceTypeID = query.rs.getInt("realsourcetypeid");
				int tempSourceTypeID = query.rs.getInt("tempsourcetypeid");
				int opModeIDOffset = query.rs.getInt("opmodeidoffset");

				// Translate the old operating mode to a new operating mode
				sql = "drop table if exists physics_" + tableName;
				updateStatements.add(sql);

				sql = "create table physics_" + tablename + " like " + tableName;
				updateStatements.add(sql);
				
				sql = "insert into physics_" + tableName
						+ " select * from " + tableName
						+ " where opmodeid >= 0 and opmodeid < 100"
						+ " and sourcetypeid=" + tempSourceTypeID
						+ " and (polprocessid < 0 or mod(polprocessid,100) = 1)";
				updateStatements.add(sql);
		
				sql = "update physics_" + tablename + " set opmodeid=opmodeid+" + opModeIDOffset;
				updateStatements.add(sql);
				
				sql = "insert ignore into " + tablename + " select * from physics_" + tableName;
				updateStatements.add(sql);
				
				Integer t = Integer.valueOf(realSourceTypeID);
				if(!seenRealSourceTypeIDs.contains(t)) {
					seenRealSourceTypeIDs.add(t);

					sql = "truncate table physics_" + tableName;
					updateStatements.add(sql);

					// Handle brakewear special case for pollutant 11609
					sql = "insert into physics_" + tableName
							+ " select * from " + tableName
							+ " where (opmodeid=501 or (opmodeid >= 0 and opmodeid < 100))"
							+ " and sourcetypeid=" + tempSourceTypeID
							+ " and polprocessid=11609 and roadtypeid in (2,4)";
					updateStatements.add(sql);
	
					// Finish the updates by changing to the real sourceTypeID
					sql = "update physics_" + tableName
							+ " set sourcetypeid=" + realSourceTypeID;
					updateStatements.add(sql);
	
					sql = "insert ignore into " + tableName
							+ " select * from physics_" + tableName;
					updateStatements.add(sql);
				}
			}
			query.close();
			for(Iterator<String> i=updateStatements.iterator();i.hasNext();) {
				sql = i.next();
				try {
					SQLRunner.executeSQL(db,sql);
				} catch(SQLException e) {
					Logger.logError(e,"unable to updateopmodes using: " + sql);
					// Nothing to do here
				}
			}
		} catch(SQLException e) {
			Logger.logError(e,"unable to updateopmodes using: " + sql);
			throw e;
		} finally {
			query.onFinally();
		}
	}

	static class SourceTypeOpMode {
		public int sourceTypeID;
		public int opModeID;
		public int newOpModeID;
		public int beginModelYearID;
		public int endModelYearID;
	}

	/**
	 * Get the new operating mode IDs for each opModeID.
	 * @param db database connection to use
	 * @throws SQLException if anything goes wrong
	**/
	ArrayList<SourceTypeOpMode> getOpModeUpdates(Connection db) throws SQLException {
		ArrayList<SourceTypeOpMode> opModeMap = new ArrayList<SourceTypeOpMode>();
		SQLRunner.Query query = new SQLRunner.Query();
		String sql = "";
		try {
			ArrayList<Integer> opModes = new ArrayList<Integer>();
			sql = "select opmodeid from operatingmode where opmodeid >= 0 and opmodeid < 100";
			query.open(db,sql);
			while(query.rs.next()) {
				opModes.add(Integer.valueOf(query.rs.getInt(1)));
			}
			query.close();

			ArrayList<Integer> bases = new ArrayList<Integer>();
			sql = "select distinct realsourcetypeid, opmodeidoffset, beginmodelyearid, endmodelyearid"
					+ " from sourceusetypephysicsmapping"
					+ " where opmodeidoffset > 0"
					+ " order by opmodeidoffset";
			query.open(db,sql);
			while(query.rs.next()) {
				int sourceTypeID = query.rs.getInt(1);
				int offset = query.rs.getInt(2);
				int beginModelYearID = query.rs.getInt(3);
				int endModelYearID = query.rs.getInt(4);

				for(Iterator<Integer> i=opModes.iterator();i.hasNext();) {
					SourceTypeOpMode t = new SourceTypeOpMode();
					t.sourceTypeID = sourceTypeID;
					t.opModeID = i.next().intValue();
					t.newOpModeID = t.opModeID + offset;
					t.beginModelYearID = beginModelYearID;
					t.endModelYearID = endModelYearID;
					opModeMap.add(t);
				}
			}
			query.close();
		} catch(SQLException e) {
			Logger.logError(e,"unable to setup getopmodeupdates");
			throw e;
		} finally {
			query.onFinally();
		}
		return opModeMap;
	}

	/**
	 * Change operating mode assignments for sourcebins in affected model years
	 * in the EmissionRate and EmissionRateByAge tables.  Also ensure the new
	 * operating modes are associated with the pollutant/processes.
	 * @param db database connection to use
	 * @param processID identifier of the affected emission process
	 * @throws SQLException if anything goes wrong
	**/
	public void updateEmissionRateTables(Connection db, int processID) throws SQLException {
		if(!(processID == 9 || processID == 1)) {
			//Logger.log(LogMessageCategory.DEBUG,"sourcetypephysics.updateemissionratetables skipped for process " + processID);
			return;
		}
		System.out.println("sourcetypephysics.updateemissionratetables starting for process 1...");
		String alreadyKey = "updateemissionrates|" + processID;
		if(alreadyDoneFlags.contains(alreadyKey)) {
			Logger.log(LogMessageCategory.DEBUG,"sourcetypephysics.updateemissionratetables already done");
			return;
		}
		alreadyDoneFlags.add(alreadyKey);

		SQLRunner.Query query = new SQLRunner.Query();
		ArrayList<String> updateStatements = new ArrayList<String>();
		String[] tableNames = {
			"emissionrate",
			"emissionratebyage"
		};

		TreeMap<Integer,TreeSet<Integer>> offsetsByPolProcess = new TreeMap<Integer,TreeSet<Integer>>();
		char MYGroupYesOrNo;
		if (processID == 9) {
			MYGroupYesOrNo = 'N';
		} else {
			MYGroupYesOrNo = 'Y';
		}

		String sql = "select distinct stpm.opmodeidoffset, sbd.polprocessid, sbd.sourcebinid"
				+ " from sourcetypepolprocess stpp"
				+ " inner join sourceusetypephysicsmapping stpm on ("
				+ " 	stpm.realsourcetypeid=stpp.sourcetypeid"
				+ " 	and stpm.opmodeidoffset>0)"
				+ " inner join pollutantprocessassoc ppa on ("
				+ " 	ppa.polprocessid=stpp.polprocessid"
				+ " 	and ppa.processid=" + processID
				+ " 	and stpp.ismygroupreqd='" + mygroupyesorno + "')"
				+ " inner join sourcebindistribution sbd on ("
				+ " 	sbd.polprocessid=ppa.polprocessid)"
				+ " inner join sourcetypemodelyear stmy on ("
				+ " 	stmy.sourcetypemodelyearid=sbd.sourcetypemodelyearid"
				+ " 	and stmy.sourcetypeid=stpm.realsourcetypeid"
				+ " 	and stmy.modelyearid >= stpm.beginmodelyearid"
				+ " 	and stmy.modelyearid <= stpm.endmodelyearid)"
				+ " inner join sourcebin sb on ("
				+ " 	sb.sourcebinid = sbd.sourcebinid"
				+ " 	and (sb.regclassid = 0 or sb.regclassid = stpm.regclassid or stpm.regclassid = 0) )"
				+ " order by stpm.opmodeidoffset, sbd.polprocessid";

		try {
			checkSourceBins(db,processID);

			// Build update statements
			query.open(db,sql);
			int opModeIDOffset = 0, polProcessID = 0;
			String sourceBinIDs = "";
			TreeMap<String,TreeSet<String> > polProcessIDsByOpModeIDOffset = new TreeMap<String,TreeSet<String> >();
			while(query.rs.next()) {
				int tempOpModeIDOffset = query.rs.getInt(1);
				int tempPolProcessID = query.rs.getInt(2);
				String tempSourceBinID = query.rs.getString(3);

				//Logger.log(LogMessageCategory.DEBUG,"sourcetypephysics.updateemissionratetables opmodeidoffset="+tempopmodeidoffset + " polprocessid=" + temppolprocessid + " sourcebinid=" + tempSourceBinID);
				if(tempOpModeIDOffset != opModeIDOffset || tempPolProcessID != polProcessID) {
					if(sourceBinIDs.length() > 0) {
						for(int ti=0;ti<tableNames.length;ti++) {
							String tableName = tableNames[ti];
							copyOperatingModes(updateStatements,tableName,opModeIDOffset,polProcessID,sourceBinIDs);
						}

						/*
						sql = "insert ignore into fullacadjustment (sourcetypeid, polprocessid, fullacadjustment, fullacadjustmentcv, opmodeid)"
								+ " select sourcetypeid, polprocessid, fullacadjustment, fullacadjustmentcv, opmodeid + " + opModeIDOffset
								+ " from fullacadjustment"
								+ " where opmodeid >= 0 and opmodeid < 100"
								+ " and polprocessid = " + polProcessID;
						updateStatements.add(sql);
						*/
						String textOpModeIDOffset = "" + opModeIDOffset;
						String textPolProcessID = "" + polProcessID;
						TreeSet<String> opModeOffsetList = polProcessIDsByOpModeIDOffset.get(textOpModeIDOffset);
						if(opModeOffsetList == null) {
							opModeOffsetList = new TreeSet<String>();
							polProcessIDsByOpModeIDOffset.put(textOpModeIDOffset,opModeOffsetList);
						}
						opModeOffsetList.add(textPolProcessID);
					}
					opModeIDOffset = tempOpModeIDOffset;
					polProcessID = tempPolProcessID;
					sourceBinIDs = "";

					Integer polProcessInt = Integer.valueOf(polProcessID);					
					TreeSet<Integer> offsets = offsetsByPolProcess.get(polProcessInt);
					if(offsets == null) {
						offsets = new TreeSet<Integer>();
						offsetsByPolProcess.put(polProcessInt,offsets);
					}
					offsets.add(Integer.valueOf(opModeIDOffset));
				}
				if(sourceBinIDs.length() > 0) {
					sourceBinIDs += ",";
				}
				sourceBinIDs += tempSourceBinID;
			}
			if(sourceBinIDs.length() > 0) {
				for(int ti=0;ti<tableNames.length;ti++) {
					String tableName = tableNames[ti];
					copyOperatingModes(updateStatements,tableName,opModeIDOffset,polProcessID,sourceBinIDs);
				}

				/*
				sql = "insert ignore into fullacadjustment (sourcetypeid, polprocessid, fullacadjustment, fullacadjustmentcv, opmodeid)"
						+ " select sourcetypeid, polprocessid, fullacadjustment, fullacadjustmentcv, opmodeid + " + opModeIDOffset
						+ " from fullacadjustment"
						+ " where opmodeid >= 0 and opmodeid < 100"
						+ " and polprocessid = " + polProcessID;
				updateStatements.add(sql);
				*/
				String textOpModeIDOffset = "" + opModeIDOffset;
				String textPolProcessID = "" + polProcessID;
				TreeSet<String> opModeOffsetList = polProcessIDsByOpModeIDOffset.get(textOpModeIDOffset);
				if(opModeOffsetList == null) {
					opModeOffsetList = new TreeSet<String>();
					polProcessIDsByOpModeIDOffset.put(textOpModeIDOffset,opModeOffsetList);
				}
				opModeOffsetList.add(textPolProcessID);
			}
			query.close();
			for(String textOpModeIDOffset : polProcessIDsByOpModeIDOffset.keySet()) {
				TreeSet<String> polProcessIDs = polProcessIDsByOpModeIDOffset.get(textOpModeIDOffset);
				if(polProcessIDs == null || polProcessIDs.size() <= 0) {
					continue;
				}
				sql = "insert ignore into fullacadjustment (sourcetypeid, polprocessid, fullacadjustment, fullacadjustmentcv, opmodeid)"
						+ " select sourcetypeid, polprocessid, fullacadjustment, fullacadjustmentcv, opmodeid + " + textOpModeIDOffset
						+ " from fullacadjustment"
						+ " where opmodeid >= 0 and opmodeid < 100"
						+ " and polprocessid in (";
				boolean isFirst = true;
				for(String textPolProcessID : polProcessIDs) {
					if(!isFirst) {
						sql += ",";
					}
					sql += textPolProcessID;
					isFirst = false;
				}
				sql += ")";
				updateStatements.add(sql);
			}
			// Execute the updates
			for(Iterator<String> i=updateStatements.iterator();i.hasNext();) {
				sql = i.next();
				SQLRunner.executeSQL(db,sql);
				//Logger.log(LogMessageCategory.DEBUG,"sourcetypephysics.updateemissionratetables "+sql);
			}
			updateStatements.clear();
			// Remove unwanted unadjusted entries
			sql = "select distinct p1.regclassid"
					+ " from sourceusetypephysicsmapping p1"
					+ " left outer join sourceusetypephysicsmapping p2 on ("
					+ " 	p1.regclassid = p2.regclassid"
					+ " 	and p2.opmodeidoffset = 0"
					+ " )"
					+ " where p2.regclassid is null and p1.regclassid > 0";
			query.open(db,sql);
			/*
			while(query.rs.next()) {
				int regClassID = query.rs.getInt(1);
				for(int ti=0;ti<tableNames.length;ti++) {
					String tableName = tableNames[ti];
					sql = "delete from " + tableName
							+ " where opmodeid >= 0 and opmodeid < 100"
							+ " and polprocessid = " + polProcessID
							+ " and sourcebinid in ("
							+ " 	select sourcebinid"
							+ " 	from sourcebin"
							+ " 	where regclassid = " + regClassID
							+ " )";
					updateStatements.add(sql);
				}
			}
			*/
			String regClassIDsCSV = "";
			while(query.rs.next()) {
				int regClassID = query.rs.getInt(1);
				if(regClassIDsCSV.length() > 0) {
					regClassIDsCSV += ",";
				}
				regClassIDsCSV += regClassID;
			}
			query.close();
			if(regClassIDsCSV.length() > 0) {
				for(int ti=0;ti<tableNames.length;ti++) {
					String tableName = tableNames[ti];
					sql = "delete from " + tableName
							+ " where opmodeid >= 0 and opmodeid < 100"
							+ " and polprocessid = " + polProcessID
							+ " and sourcebinid in ("
							+ " 	select sourcebinid"
							+ " 	from sourcebin"
							+ " 	where regclassid in (" + regclassidscsv + ")"
							+ " )";
					updateStatements.add(sql);
				}
			}
			for(Iterator<String> i=updateStatements.iterator();i.hasNext();) {
				sql = i.next();
				SQLRunner.executeSQL(db,sql);
				//Logger.log(LogMessageCategory.DEBUG,"sourcetypephysics.updateemissionratetables#2 "+sql);
			}
			updateStatements.clear();
			// Ensure opModePolProcAssoc has the required values
			TreeSet<Integer> allOffsets = new TreeSet<Integer>();
			Set<Integer> keys = offsetsByPolProcess.keySet();
			for(Iterator<Integer> i=offsetsByPolProcess.keySet().iterator();i.hasNext();) {
				Integer polProcessInt = i.next();
				TreeSet<Integer> offsets = offsetsByPolProcess.get(polProcessInt);
				if(offsets != null) {
					for(Iterator<Integer> oi=offsets.iterator();oi.hasNext();) {
						Integer offset = oi.next();
						allOffsets.add(offset);
						for(int ti=0;ti<tableNames.length;ti++) {
							String tableName = tableNames[ti];
							sql = "insert ignore into opmodepolprocassoc (polprocessid, opmodeid)"
									+ " select distinct " + polprocessint + ", opmodeid"
									+ " from " + tableName
									+ " where polprocessid=" + polProcessInt
									+ " and opmodeid >= 0 + " + offset + " and opmodeid < 100 + " + offset;
							SQLRunner.executeSQL(db,sql);
							//Logger.log(LogMessageCategory.DEBUG,"sourcetypephysics.updateemissionratetables#4 "+sql);
						}
					}
				}
			}
			for(int i=0;i<tableNames.length;i++) {
				sql = "analyze table " + tableNames[i];
				SQLRunner.executeSQL(db,sql);
				//Logger.log(LogMessageCategory.DEBUG,"sourcetypephysics.updateemissionratetables#3 "+sql);
			}
			sql = "analyze table opmodepolprocassoc";
			SQLRunner.executeSQL(db,sql);
		} catch(SQLException e) {
			Logger.logError(e,"unable to update emission rate tables for source type physics");
			throw e;
		} finally {
			query.onFinally();
		}
		Logger.log(LogMessageCategory.DEBUG,"sourcetypephysics.updateemissionratetables done");
	}

	/**
	 * Make SQL statements to copy operating modes within a table.
	 * @param updateStatements list of statements to be appended
	 * @param tableName affected table
	 * @param opModeIDOffset offset used to create new operating modes
	 * @param polProcessID affected pollutant/process identifier
	 * @param sourceBinIDs comma-separated list of affected sourcebin identifiers
	**/
	private void copyOperatingModes(ArrayList<String> updateStatements,
			String tableName, int opModeIDOffset, int polProcessID, String sourceBinIDs) {
		if(opModeIDOffset <= 0) {
			return;
		}
		String sql = "";
		sql = "drop table if exists physics_" + tableName;
		updateStatements.add(sql);
		
		sql = "create table physics_" + tablename + " like " + tableName;
		updateStatements.add(sql);
		
		sql = "insert into physics_" + tableName
				+ " select * from " + tableName
				+ " where opmodeid >= 0 and opmodeid < 100"
				+ " and polprocessid=" + polProcessID
				+ " and sourcebinid in (" + sourcebinids + ")";
		updateStatements.add(sql);

		sql = "update physics_" + tablename + " set opmodeid=opmodeid+" + opModeIDOffset;
		updateStatements.add(sql);
		
		sql = "insert ignore into " + tablename + " select * from physics_" + tableName;
		updateStatements.add(sql);
	}

	/**
	 * Create and populate the physicsOperatingMode table containing all operating modes.
	 * @param db database connection to use
	 * @throws SQLException if anything goes wrong
	**/	
	public void createExpandedOperatingModesTable(Connection db) throws SQLException {
		String alreadyKey = "createexpandedoperatingmodestable";
		if(alreadyDoneFlags.contains(alreadyKey)) {
			return;
		}
		alreadyDoneFlags.add(alreadyKey);

		String sql = "";
		try {
			String[] statements = {
				"drop table if exists physicsoperatingmode",
	
				"create table physicsoperatingmode like operatingmode",
	
				"insert into physicsoperatingmode select * from operatingmode",
	
				"insert ignore into physicsoperatingmode (opmodeid, opmodename, vsplower, vspupper, speedlower, speedupper, brakerate1sec, brakerate3sec, minsoaktime, maxsoaktime)"
						+ " select distinct opmodeid+opmodeidoffset, "
						+ " 	opmodename, vsplower, vspupper, speedlower, speedupper, brakerate1sec, brakerate3sec, minsoaktime, maxsoaktime"
						+ " from operatingmode, sourceusetypephysicsmapping"
						+ " where opmodeid >= 0 and opmodeid < 100"
						+ " and opmodeidoffset>0"
			};
			for(int i=0;i<statements.length;i++) {
				sql = statements[i];
				SQLRunner.executeSQL(db,sql);
			}
		} catch(SQLException e) {
			Logger.logError(e,"unable to update emission rate tables for source type physics");
			throw e;
		}
	}

	/**
	 * Check sourceBins for model year overlap issues.
	 * @param db database connection to use
	 * @param processID identifier of the affected emission process
	 * @throws SQLException if anything goes wrong
	**/
	void checkSourceBins(Connection db, int processID) throws SQLException {
		String alreadyKey = "checksourcebins|" + processID;
		if(alreadyDoneFlags.contains(alreadyKey)) {
			return;
		}
		alreadyDoneFlags.add(alreadyKey);

		String[] setupStatements = {
			"create table if not exists sourcebinmodelyearrange ("
					+ " 	sourcebinid bigint not null,"
					+ " 	polprocessid int not null,"
					+ " 	minmodelyearid int not null,"
					+ " 	maxmodelyearid int not null,"
					+ " 	howmanymodelyears int not null,"
					+ " 	primary key (sourcebinid, polprocessid)"
					+ " )",

			"insert ignore into sourcebinmodelyearrange(sourcebinid,polprocessid,minmodelyearid,maxmodelyearid,howmanymodelyears)"
					+ " select sbd.sourcebinid, sbd.polprocessid, min(stmy.modelyearid) as minmodelyearid, max(stmy.modelyearid) as maxmodelyearid, count(distinct stmy.modelyearid) as howmany"
					+ " from sourcetypepolprocess stpp"
					+ " inner join pollutantprocessassoc ppa on ("
					+ " 	ppa.polprocessid=stpp.polprocessid"
					+ " 	and ppa.processid=" + processID
					+ " 	and stpp.ismygroupreqd='Y')"
					+ " inner join sourcebindistribution sbd on ("
					+ " 	sbd.polprocessid=ppa.polprocessid)"
					+ " inner join sourcetypemodelyear stmy on ("
					+ " 	stmy.sourcetypemodelyearid=sbd.sourcetypemodelyearid"
					+ " 	and stmy.sourcetypeid=stpp.sourcetypeid)"
					+ " group by sbd.sourcebinid, sbd.polprocessid",

			// Get sbid, my, st, rc that are allowed.
			"drop table if exists allowedsourcebinmystrc",
			"create table allowedsourcebinmystrc ("
					+ " 	sourcebinid bigint not null,"
					+ " 	modelyearid smallint not null,"
					+ " 	sourcetypeid smallint not null,"
					+ " 	regclassid smallint not null,"
					+ " 	primary key (modelyearid, sourcetypeid, regclassid, sourcebinid)"
					+ " )",
			"insert into allowedsourcebinmystrc (sourcebinid, modelyearid, sourcetypeid, regclassid)"
					+ " select distinct sbd.sourcebinid, stmy.modelyearid, stpp.sourcetypeid, sb.regclassid"
					+ " from sourcetypepolprocess stpp"
					+ " inner join pollutantprocessassoc ppa on ("
					+ " 	ppa.polprocessid=stpp.polprocessid"
					+ " 	and ppa.processid=" + processID
					+ " 	and stpp.ismygroupreqd='Y')"
					+ " inner join sourcebindistribution sbd on ("
					+ " 	sbd.polprocessid=ppa.polprocessid)"
					+ " inner join sourcetypemodelyear stmy on ("
					+ " 	stmy.sourcetypemodelyearid=sbd.sourcetypemodelyearid"
					+ " 	and stmy.sourcetypeid=stpp.sourcetypeid)"
					+ " inner join sourcebin sb on ("
					+ " 	sb.sourcebinid = sbd.sourcebinid)"
					+ " inner join samplevehiclepopulation svp on ("
					+ " 	svp.sourcetypemodelyearid = stmy.sourcetypemodelyearid"
					+ " 	and (sb.regclassid = 0 or svp.regclassid = sb.regclassid)"
					+ " 	and svp.fueltypeid = sb.fueltypeid)",
			// Get my, st, rc used by physics.
			"drop table if exists usedmystrc",
			"create table usedmystrc ("
					+ " 	modelyearid smallint not null,"
					+ " 	sourcetypeid smallint not null,"
					+ " 	regclassid smallint not null,"
					+ " 	primary key (modelyearid, sourcetypeid, regclassid)"
					+ " )",
			"insert into usedmystrc (modelyearid, sourcetypeid, regclassid)"
					+ " select distinct stmy.modelyearid, stpm.realsourcetypeid as sourcetypeid, stpm.regclassid"
					+ " from sourceusetypephysicsmapping stpm"
					+ " inner join sourcetypemodelyear stmy on ("
					+ " 	stmy.sourcetypeid=stpm.realsourcetypeid"
					+ " 	and stmy.modelyearid >= stpm.beginmodelyearid"
					+ " 	and stmy.modelyearid <= stpm.endmodelyearid)"
					+ " inner join samplevehiclepopulation svp on ("
					+ " 	svp.sourcetypemodelyearid = stmy.sourcetypemodelyearid"
					+ " 	and svp.regclassid = stpm.regclassid)",
			// Find missing entries
			"drop table if exists missingmystrc",
			"create table missingmystrc ("
					+ " 	modelyearid smallint not null,"
					+ " 	sourcetypeid smallint not null,"
					+ " 	regclassid smallint not null,"
					+ " 	primary key (modelyearid, sourcetypeid, regclassid)"
					+ " )",
			"insert into missingmystrc (modelyearid, sourcetypeid, regclassid)"
					+ " select distinct a.modelyearid, a.sourcetypeid, a.regclassid"
					+ " from allowedsourcebinmystrc a"
					+ " left outer join usedmystrc u using (modelyearid, sourcetypeid, regclassid)"
					+ " where u.modelyearid is null",
			// Report using a range
			"drop table if exists missingmystrc_ranges",
			"create table missingmystrc_ranges ("
					+ " 	modelyearid smallint not null,"
					+ " 	sourcetypeid smallint not null,"
					+ " 	regclassid smallint not null,"
					+ " 	isupperlimit smallint not null,"
					+ " 	minmodelyearid smallint null,"
					+ " 	maxmodelyearid smallint null,"
					+ " 	primary key (modelyearid, sourcetypeid, regclassid, isupperlimit)"
					+ " )",
			"insert into missingmystrc_ranges (sourcetypeid, regclassid, modelyearid, isupperlimit)"
					+ " select b.sourcetypeid, b.regclassid, b.modelyearid,"
					+ " 	case when bp1.modelyearid is null then 1 else 0 end as isupperlimit"
					+ " from missingmystrc b"
					+ " left outer join missingmystrc bp1 on ("
					+ " 	b.modelyearid + 1 = bp1.modelyearid"
					+ " 	and b.sourcetypeid = bp1.sourcetypeid"
					+ " 	and b.regclassid = bp1.regclassid"
					+ " )"
					+ " left outer join missingmystrc bm1 on ("
					+ " 	b.modelyearid - 1 = bm1.modelyearid"
					+ " 	and b.sourcetypeid = bm1.sourcetypeid"
					+ " 	and b.regclassid = bm1.regclassid"
					+ " )"
					+ " where bp1.modelyearid is null or bm1.modelyearid is null",
			"drop table if exists missingmystrc_ranges2",
			"create table missingmystrc_ranges2 like missingmystrc_ranges",
			"insert into missingmystrc_ranges2 select * from missingmystrc_ranges",
			"update missingmystrc_ranges set minmodelyearid=modelyearid where isupperlimit = 0",
			"update missingmystrc_ranges set maxmodelyearid=("
					+ " 	select min(modelyearid)"
					+ " 	from missingmystrc_ranges2 r"
					+ " 	where r.isupperlimit=1"
					+ " 	and r.modelyearid >= missingmystrc_ranges.modelyearid"
					+ " )"
					+ " where isupperlimit=0",
			"drop table if exists missingmystrc_ranges2"
		};
		
		String[] queryStatements = {
				/*
				"select distinct case when howmany > 1 then"
				+ " 		concat('sourcebin ',sourcebinid,' is affected by ',howmany,' sourceusetypephysics entries.')"
				+ " 	else ''"
				+ " 	end as errormessage"
				+ " from ("
					+ " select sourcetypeid, sourcebinid, "
					+ " 	count(distinct opmodeidoffset) as howmany, "
					+ " 	min(minmodelyearid) as minmodelyearid, "
					+ " 	max(maxmodelyearid) as maxmodelyearid,"
					+ " 	min(binminmodelyearid) as binminmodelyearid,"
					+ " 	max(binmaxmodelyearid) as binmaxmodelyearid,"
					+ " 	max(howmanymodelyears) as howmanymodelyears,"
					+ " 	max(binhowmanymodelyears) as binhowmanymodelyears"
					+ " from ("
						+ " select stpp.sourcetypeid, sbd.sourcebinid, sbd.polprocessid, stpm.opmodeidoffset, "
						+ " 	min(stmy.modelyearid) as minmodelyearid, max(stmy.modelyearid) as maxmodelyearid, "
						+ " 	count(distinct stmy.modelyearid) as howmanymodelyears,"
						+ " 	sbmyr.minmodelyearid as binminmodelyearid,"
						+ " 	sbmyr.maxmodelyearid as binmaxmodelyearid,"
						+ " 	sbmyr.howmanymodelyears as binhowmanymodelyears"
						+ " from sourcetypepolprocess stpp"
						+ " inner join sourceusetypephysicsmapping stpm on ("
						+ " 	stpm.realsourcetypeid=stpp.sourcetypeid"
						+ " 	and stpm.opmodeidoffset>=0)"
						+ " inner join pollutantprocessassoc ppa on ("
						+ " 	ppa.polprocessid=stpp.polprocessid"
						+ " 	and ppa.processid=" + processID
						+ " 	and stpp.ismygroupreqd='Y')"
						+ " inner join sourcebindistribution sbd on ("
						+ " 	sbd.polprocessid=ppa.polprocessid)"
						+ " inner join sourcetypemodelyear stmy on ("
						+ " 	stmy.sourcetypemodelyearid=sbd.sourcetypemodelyearid"
						+ " 	and stmy.sourcetypeid=stpm.realsourcetypeid"
						+ " 	and stmy.modelyearid >= stpm.beginmodelyearid"
						+ " 	and stmy.modelyearid <= stpm.endmodelyearid)"
						+ " inner join sourcebin sb on ("
						+ " 	sb.sourcebinid = sbd.sourcebinid"
						+ " 	and (sb.regclassid = 0 or sb.regclassid = stpm.regclassid or stpm.regclassid = 0) )"
						+ " inner join sourcebinmodelyearrange sbmyr on ("
						+ " 	sbmyr.sourcebinid = sbd.sourcebinid"
						+ " 	and sbmyr.polprocessid = sbd.polprocessid)"
						+ " group by stpp.sourcetypeid, sbd.sourcebinid, sbd.polprocessid, stpm.opmodeidoffset"
					+ " ) t"
					+ " group by sourcetypeid, sourcebinid"
				+ " ) t2"
				+ " where howmany > 1"
				+ " order by sourcebinid",
				*/

				"select concat('sourcetype ',sourcetypeid,' regclass ',regclassid,' has no sourceusetypephysics coverage for model years ',minmodelyearid,'-',maxmodelyearid) as errormessage"
						+ " from missingmystrc_ranges"
						+ " where isupperlimit=0 and minmodelyearid is not null and maxmodelyearid is not null"
						+ " order by sourcetypeid, regclassid, minmodelyearid"
		};

		String sql = "";
		SQLRunner.Query query = new SQLRunner.Query();
		try {
			for(int i=0;i<setupStatements.length;i++) {
				sql = setupStatements[i];
				if(sql != null && sql.length() > 0) {
					SQLRunner.executeSQL(db,sql);
				}
			}

			for(int i=0;i<queryStatements.length;i++) {
				sql = queryStatements[i];
				query.open(db,sql);
				while(query.rs.next()) {
					String message = StringUtilities.safeGetString(query.rs.getString(1));
					if(message.length() <= 0) {
						continue;
					}
					if(alreadyDoneMessages.contains(message)) {
						continue;
					}
					alreadyDoneMessages.add(message);
					Logger.log(LogMessageCategory.ERROR,"error: " + message);
				}
				query.close();
			}
		} catch(SQLException e) {
			Logger.logError(e,"unable to check sourcebins using: " + sql);
			throw e;
		} finally {
			query.onFinally();
		}
	}
	
	// This code is called becuase user-provided operating mode distributions (at project scale) do not have the same
	// offsets as other input options, which causes no output. Therefore we need to offset them using sourceusetypephysicsmapping
	// as the reference for the opModeOffsets. This only applies for roadTypes 2-5
	public void offsetUserInputOpModeIDs(Connection db) throws SQLException {
		ArrayList<SourceTypeOpMode> gottenOpModeUpdates = new ArrayList<SourceTypeOpMode>();
		gottenOpModeUpdates = getOpModeUpdates(db);
		
		
		
		// Step 1: Create temporary table to hold the offset opModeIDs
		String[] setupStatements = {
			"drop table if exists tempoffsetopmodedistribution",
			"create table if not exists tempoffsetopmodedistribution like opmodedistribution", 
			"drop table if exists tempopmodeupdates",
			"create table if not exists tempopmodeupdates (" +
				"sourcetypeid int, " +
				"opmodeid int, " +
				"newopmodeid int, " +
				"beginmodelyearid int, " +
				"endmodelyearid int, " +
				"unique key(sourcetypeid,opmodeid,newopmodeid,beginmodelyearid,endmodelyearid), key(sourcetypeid,opmodeid,newopmodeid,beginmodelyearid,endmodelyearid))"
		};
		for(String sql : setupStatements) {
			SQLRunner.executeSQL(db,sql);
		}
		
		// Step 2: Put the corect offset OpModeIDs in the temporary table using the non-offset table
		for (SourceTypeOpMode s : gottenOpModeUpdates) {
			String sql = "insert into tempopmodeupdates values (" + 
				s.sourceTypeID + ", " + s.opModeID + ", " + s.newOpModeID + ", " + s.beginModelYearID + ", " + s.endModelYearID + ")";
			SQLRunner.executeSQL(db,sql);
		}
		// join the update table with the existing op mode distribution table
		String sql = "insert into tempoffsetopmodedistribution " +
			"select tomu.sourcetypeid, omd.hourdayid, omd.linkid, omd.polprocessid, tomu.newopmodeid, omd.opmodefraction, omd.opmodefractioncv, omd.isuserinput " +
			"from tempopmodeupdates tomu " +
			"left join opmodedistribution omd " +
			"using (sourcetypeid,opmodeid) " +
			"where opmodefraction is not null and omd.opmodeid < 100 and linkid > 0";
		SQLRunner.executeSQL(db,sql);
		
		// Step 3: delete relevant rows from old table without the offset IDs
		sql = "delete from opmodedistribution where opmodeid < 100 and linkid > 0";
		SQLRunner.executeSQL(db,sql);
		
		// Step 4: put all the data from the temporary table into the existing table
		sql = "insert into opmodedistribution select * from tempoffsetopmodedistribution";
		SQLRunner.executeSQL(db,sql);
		
		// Step 5: drop the temporary tables
		sql = "drop table if exists tempoffsetopmodedistribution;";
		SQLRunner.executeSQL(db,sql);
		sql = "drop table if exists tempopmodeupdates;";
		SQLRunner.executeSQL(db,sql);
	}
}
