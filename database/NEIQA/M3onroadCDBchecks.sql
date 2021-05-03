-- MOVES3 NEI QA Script for Onroad CDBs
-- Last Updated: MOVES3.0.1
-- ##############################################################################
-- Usage Notes:  This script is intended to be run via the ANT command 
--               "onroadNEIQA". See database\NEIQA\NEIQAHelp.pdf for more info.
--               If this script needs to be run directly against MariaDB (i.e.,
--               using mysql.exe or through Workbench/Heidi:
--               1. Find & Replace "BeginBlock" with "Delimiter $$"
--               2. Find & Replace "EndBlock" with "$$\nDelimiter ;" (converting
--                     the \n to a new line)
--               3. Find & Replace "##defaultdb##" with the name of the default 
--                     database, as found in MOVESConfiguration.txt
-- ##############################################################################

set @version = MID("##defaultdb##", 8, 8);

-- Coordinate this message with the Done statement at the end of this file.
select '  .. M3onroadCDBchecks.sql',curTime(), database();

-- ##############################################################################
-- define stored procedures
-- ##############################################################################


-- ##############################################################################
-- Check the IMCoverage table for overlaps and gaps
-- ##############################################################################
DROP   PROCEDURE IF EXISTS checkimcoverage;
BeginBlock
CREATE PROCEDURE checkimcoverage()
BEGIN
  DECLARE done INT DEFAULT FALSE;
  declare cpol, ccou, cyea, csou, cfue, cbegmy, cendmy int;  -- c is for current (row of data)
  declare lpol, lcou, lyea, lsou, lfue, lbegmy, lendmy int;  -- l is for last     "   "  "
  declare rows_processed,
          rows_skipped   int default 0;
  declare cuseimyn char(1);
  declare reason char(40);
  declare sameset int;
  declare curimcov cursor for select polprocessid,
                                     countyid,
                                     yearid,
                                     sourcetypeid,
                                     fueltypeid,
                                     begmodelyearid,
                                     endmodelyearid,
                                     useimyn
                                from imcoverage
                            order by useimyn,
                                     polprocessid,
                                     countyid,
                                     yearid,
                                     sourcetypeid,
                                     fueltypeid,
                                     begmodelyearid,
                                     endmodelyearid;

  declare continue handler for not found set done = true;
  open curimcov;
  set lpol   = -1;
  set lcou   = -1;
  set lyea   = -1;
  set lsou   = -1;
  set lfue   = -1;
  set lendmy = -1;

  read_loop: loop
    fetch curimcov into cpol, ccou, cyea, csou, cfue, cbegmy, cendmy, cuseimyn;
    if done then leave read_loop; end if;
    if cuseimyn = 'Y' then
        set rows_processed = rows_processed + 1;
        
        -- Order Error:  (regardless of sameSet or not)
        if CBEGMY > CENDMY
        then
          set reason = 'cbegmodelyearid>cendmodelyearid,';
          insert into qa_checks_im values ( cpol, ccou, cyea, csou, cfue, lendmy, cbegmy, cendmy, cuseimyn, reason );
    else
          if cpol=lpol and ccou=lcou and cyea=lyea and csou=lsou and cfue=lfue
      then  -- in set:

            -- overlap error from consecutive rows
            if cbegmy<=lendmy      then  -- overlap error:
            set reason = 'cbegmy<=lendmy (overlap)';
            insert into qa_checks_im values ( cpol, ccou, cyea, csou, cfue, lendmy, cbegmy, cendmy, cuseimyn, reason );

            -- gap from begmy to lendmy > 1
            elseif cbegmy>lendmy+1
            then  -- gap error:
               set reason = 'cbegmy>lendmy+1 (gap)';
               insert into qa_checks_im values ( cpol, ccou, cyea, csou, cfue, lendmy, cbegmy, cendmy, cuseimyn, reason );
      end if;  -- end of overlap & gap checking

      else  -- not in set:
      set lpol   =   cpol;
            set lcou   =   ccou;
            set lyea   =   cyea;
            set lsou   =   csou;
            set lfue   =   cfue;
      end if;  -- end of set

      set lendmy = cendmy;
          
    end if;  -- end of order check
        
   else     -- cuseimyn check
    set rows_skipped = rows_skipped + 1;
     end if;  -- cuseimyn check

  end loop;
  close curimcov;

end
endblock
-- ##############################################################################


-- ##############################################################################
-- check the hotellingactivitydistribution table for overlaps and gaps in mys
-- ##############################################################################
drop   procedure if exists checkhotellingactivitydistribution;
beginblock
create procedure checkhotellingactivitydistribution()
begin
  declare done int default false;
  declare czone, cbegmy, cendmy int;  -- c is for current (row of data)
  declare lzone, lbegmy, lendmy int;  -- l is for last     "   "  "
  declare rows_processed,
          rows_skipped   int default 0;
  declare reason char(40);
  declare sameset int;
  declare curhad cursor for select distinct zoneid,
                                     beginmodelyearid,
                                     endmodelyearid
                                from hotellingactivitydistribution
                            order by zoneid,
                                     beginmodelyearid,
                                     endmodelyearid;

  declare continue handler for not found set done = true;
  open curhad;
  set lzone   = -1;
  set lendmy = -1;

  read_loop: loop
    fetch curhad into czone, cbegmy, cendmy;
    if done then leave read_loop; end if;

  set rows_processed = rows_processed + 1;
  
  -- order error:  (regardless of sameset or not)
  if cbegmy > cendmy
  then
    set reason = 'cbegmodelyearid>cendmodelyearid,';
    insert into qa_checks_had values ( czone, lendmy, cbegmy, cendmy, reason );
  else
    if czone=lzone
    then  -- in set:

    -- overlap error from consecutive rows
    if cbegmy<=lendmy      then  -- overlap error:
    set reason = 'cbegmy<=lendmy (overlap)';
    insert into qa_checks_had values ( czone, lendmy, cbegmy, cendmy, reason );

    -- gap from begmy to lendmy > 1
    elseif cbegmy>lendmy+1
    then  -- gap error:
       set reason = 'cbegmy>lendmy+1 (gap)';
       insert into qa_checks_had values ( czone, lendmy, cbegmy, cendmy, reason );
    end if;  -- end of overlap & gap checking

    else  -- not in set:
    set lzone   =   czone;
    end if;  -- end of set

    set lendmy = cendmy;
    
  end if;  -- end of order check
  
  end loop;
  close curhad;

end
endblock
-- ##############################################################################


-- ##############################################################################
-- check the idlemodelyeargrouping table for overlaps and gaps in mys
-- ##############################################################################
drop procedure if exists checkidlemodelyeargrouping;
beginblock
create procedure checkidlemodelyeargrouping()
begin
  declare done int default false;
  declare cst, cbegmy, cendmy int;  -- c is for current (row of data)
  declare lst, lbegmy, lendmy int;  -- l is for last     "   "  "
  declare rows_processed,
          rows_skipped   int default 0;
  declare reason char(40);
  declare sameset int;
  declare curimyg cursor for select distinct sourcetypeid,
                       minmodelyearid,
                       maxmodelyearid
                    from idlemodelyeargrouping
                  order by sourcetypeid,
                       minmodelyearid,
                       maxmodelyearid;

  declare continue handler for not found set done = true;
  open curimyg;
  set lst    = -1;
  set lendmy = -1;

  read_loop: loop
    fetch curimyg into cst, cbegmy, cendmy;
    if done then leave read_loop; end if;

  set rows_processed = rows_processed + 1;
  
  -- order error:  (regardless of sameset or not)
  if cbegmy > cendmy
  then
    set reason = 'cminmodelyearid>cmaxmodelyearid,';
    insert into qa_checks_imyg values ( cst, lendmy, cbegmy, cendmy, reason );
  else
    if cst=lst
    then  -- in set:

    -- overlap error from consecutive rows
    if cbegmy<=lendmy      then  -- overlap error:
    set reason = 'cminmy<=lmaxmy (overlap)';
    insert into qa_checks_imyg values ( cst, lendmy, cbegmy, cendmy, reason );

    -- gap from begmy to lendmy > 1
    elseif cbegmy>lendmy+1
    then  -- gap error:
       set reason = 'cminmy>lmaxmy+1 (gap)';
       insert into qa_checks_imyg values ( cst, lendmy, cbegmy, cendmy, reason );
    end if;  -- end of overlap & gap checking

    else  -- not in set:
    set lst   =   cst;
    end if;  -- end of set

    set lendmy = cendmy;
    
  end if;  -- end of order check
  
  end loop;
  close curimyg;

end
endblock
-- ##############################################################################


-- ##############################################################################
-- check the totalidlefraction table for overlaps and gaps
-- ##############################################################################
drop   procedure if exists checktotalidlefraction;
beginblock
create procedure checktotalidlefraction()
begin
  declare done int default false;
  declare cst, cmonth, cday, cbegmy, cendmy int;  -- c is for current (row of data)
  declare lst, lmonth, lday, lbegmy, lendmy int;  -- l is for last     "   "  "
  declare rows_processed,
          rows_skipped   int default 0;
  declare reason char(40);
  declare sameset int;
  declare curtif cursor for select sourcetypeid,
                                     monthid,
                                     dayid,
                                     minmodelyearid,
                                     maxmodelyearid
                                from totalidlefraction
                            order by sourcetypeid,
                                     monthid,
                                     dayid,
                                     minmodelyearid,
                                     maxmodelyearid;

  declare continue handler for not found set done = true;
  open curtif;
  set lst    = -1;
  set lmonth = -1;
  set lday   = -1;
  set lendmy = -1;

  read_loop: loop
    fetch curtif into cst, cmonth, cday, cbegmy, cendmy;
    if done then leave read_loop; end if;

  set rows_processed = rows_processed + 1;
  
  -- order error:  (regardless of sameset or not)
  if cbegmy > cendmy
  then
    set reason = 'cminmodelyearid>cmaxmodelyearid,';
    insert into qa_checks_tif values ( cst, cmonth, cday, lendmy, cbegmy, cendmy, reason );
  else
    if cst=lst and cmonth=lmonth and cday=lday
    then  -- in set:

    -- overlap error from consecutive rows
    if cbegmy<=lendmy      then  -- overlap error:
    set reason = 'cmaxmy<=lminmy (overlap)';
    insert into qa_checks_tif values ( cst, cmonth, cday, lendmy, cbegmy, cendmy, reason );

    -- gap from begmy to lendmy > 1
    elseif cbegmy>lendmy+1
    then  -- gap error:
       set reason = 'cminmy>lmaxmy+1 (gap)';
       insert into qa_checks_tif values ( cst, cmonth, cday, lendmy, cbegmy, cendmy, reason );
    end if;  -- end of overlap & gap checking

    else  -- not in set:
    set lst    =   cst;
    set lmonth =   cmonth;
    set lday   =   cday;
    end if;  -- end of set

    set lendmy = cendmy;
    
  end if;  -- end of order check

  end loop;
  close curtif;

end
endblock
-- ##############################################################################


-- ##############################################################################
-- the overlaps and gaps checking store procedures create temporary tables to
-- store the detailed results of their checks; drop these tables if they are
-- empty, but keep them if they have useful data showing where the errors are
--
-- perform other cleanup
-- ##############################################################################
drop   procedure if exists emptytablecleanup;
beginblock
create procedure emptytablecleanup()
begin
  -- holds rows of the qa_checks_x tables that exist and are empty
  drop table if exists tempc;
  create table tempc
  select distinct
       table_name as tablename,
       table_rows
  from   information_schema.tables
  where  table_schema = (select database())
    and  table_rows = 0
    and  table_name in ( 'qa_checks_im', 'qa_checks_had', 'qa_checks_imyg', 'qa_checks_tif');

  -- drop qa_checks_x tables if they appear in tempc; otherwise, save them
  if (select count(*) from tempc where tablename = 'qa_checks_im') = 1 then
  drop table qa_checks_im;
  end if;
  if (select count(*) from tempc where tablename = 'qa_checks_had') = 1 then
  drop table qa_checks_had;
  end if;
  if (select count(*) from tempc where tablename = 'qa_checks_imyg') = 1 then
  drop table qa_checks_imyg;
  end if;
  if (select count(*) from tempc where tablename = 'qa_checks_tif') = 1 then
  drop table qa_checks_tif;
  end if;
  
  -- drop tables that get added to the input database if they are missing (so the script doesn't exit early)
  -- but we don't actually want them after the script runs if they are not necessary
  if (select count(*) from cdb_checks where status = 'todo' and testdescription='table added by qa script and should be removed' and tablename = 'countyyear') = 1 then
  delete from cdb_checks where status = 'todo' and testdescription='table added by qa script and should be removed' and tablename = 'countyyear';
    drop table if exists countyyear;
  end if;
  if (select count(*) from cdb_checks where status = 'todo' and testdescription='table added by qa script and should be removed' and tablename = 'emissionratebyage') = 1 then
  delete from cdb_checks where status = 'todo' and testdescription='table added by qa script and should be removed' and tablename = 'emissionratebyage';
    drop table if exists emissionratebyage;
  end if;
  if (select count(*) from cdb_checks where status = 'todo' and testdescription='table added by qa script and should be removed' and tablename = 'hotellinghours') = 1 then
  delete from cdb_checks where status = 'todo' and testdescription='table added by qa script and should be removed' and tablename = 'hotellinghours';
    drop table if exists hotellinghours;
  end if;
  
  -- drop temporary table
  drop table if exists tempc;

end
endblock
-- ##############################################################################


-- ##############################################################################
-- check for missing tables or warn if certain tables are present but unexpected
-- ##############################################################################
-- create a table to contain the results of the table checks.
drop table if exists cdb_checks;
create table cdb_checks (
   countyid          int(11),
   `status`          char(20),
   tablename         char(100),
   checknumber       smallint(6),
   testdescription   char(250),
   testvalue         text,
   `count`           int(11),
   databasename      char(100),
   dayid             smallint(6),
   fuelformulationid smallint(6),
   fueltypeid        smallint(6),
   fuelsubtypeid     smallint(6),
   fuelyearid        smallint(6),
   hourdayid         smallint(6),
   hourid            smallint(6),
   hpmsvtypeid       smallint(6),
   monthgroupid      smallint(6),
   monthid           smallint(6),
   roadtypeid        smallint(6),
   sourcetypeid      smallint(6),
   stateid           smallint(6),
   yearid            smallint(6),
   zoneid            int(11),
   msgtype           char(50),
   msgdate           date,
   msgtime           time,
   version           char(8),
   sumkeyid          int(11),
   sumkeydescription char(50)
 ) engine=myisam default charset=latin1;

-- this table contains entries for every qa check, so if this script fails, looking at the last row
-- should help determine where the error occurred.
drop table if exists qa_checks_log;
create table qa_checks_log (
    checkno    int(11),
    status     char(20),
    version    char(8),
    msgdate    date,
    msgtime    time
  ) engine=myisam default charset=latin1;

-- the first set of rows in the the cdb_checks simply list the tables we are checking along with 
-- how many rows are in each table. this chunk creates the entries for each table; the number of
-- rows are added to these rows below. the reason why this is done in a two-step process is because
-- we don't want the script to crash if a table is missing (so get the information about them from
-- table schema instead of directly)
insert into cdb_checks set tablename = 'auditlog';
insert into cdb_checks set tablename = 'county';
insert into cdb_checks set tablename = 'countyyear';
insert into cdb_checks set tablename = 'state';
insert into cdb_checks set tablename = 'zone';
insert into cdb_checks set tablename = 'zonemonthhour';
insert into cdb_checks set tablename = 'zoneroadtype';
insert into cdb_checks set tablename = 'year';
insert into cdb_checks set tablename = 'avft';
insert into cdb_checks set tablename = 'fuelformulation';
insert into cdb_checks set tablename = 'fuelsupply';
insert into cdb_checks set tablename = 'fuelusagefraction';
insert into cdb_checks set tablename = 'hourvmtfraction';
insert into cdb_checks set tablename = 'dayvmtfraction';
insert into cdb_checks set tablename = 'monthvmtfraction';
insert into cdb_checks set tablename = 'hpmsvtypeyear';
insert into cdb_checks set tablename = 'hotellingactivitydistribution';
insert into cdb_checks set tablename = 'hotellingagefraction';
insert into cdb_checks set tablename = 'hotellinghoursperday';
insert into cdb_checks set tablename = 'hotellinghourfraction';
insert into cdb_checks set tablename = 'hotellingmonthadjust';
insert into cdb_checks set tablename = 'starts';
insert into cdb_checks set tablename = 'startsageadjustment';
insert into cdb_checks set tablename = 'startshourfraction';
insert into cdb_checks set tablename = 'startsmonthadjust';
insert into cdb_checks set tablename = 'startsperday';
insert into cdb_checks set tablename = 'startsperdaypervehicle';
insert into cdb_checks set tablename = 'startsopmodedistribution';
insert into cdb_checks set tablename = 'idledayadjust';
insert into cdb_checks set tablename = 'idlemodelyeargrouping';
insert into cdb_checks set tablename = 'idlemonthadjust';
insert into cdb_checks set tablename = 'totalidlefraction';
insert into cdb_checks set tablename = 'avgspeeddistribution';
insert into cdb_checks set tablename = 'imcoverage';
insert into cdb_checks set tablename = 'onroadretrofit';
insert into cdb_checks set tablename = 'roadtypedistribution';
insert into cdb_checks set tablename = 'sourcetypeagedistribution';
insert into cdb_checks set tablename = 'sourcetypeyear';
insert into cdb_checks set tablename = 'emissionratebyage';
insert into cdb_checks set tablename = 'hotellinghours';

update      cdb_checks set msgtype   = 'table check';
update      cdb_checks set msgdate   = curdate();
update      cdb_checks set msgtime   = curtime();

-- tempb holds the number of rows for each table that we are checking
drop   table if exists tempb;
create table           tempb
select distinct
       table_name as tablename,
       table_rows
from   information_schema.tables
where  table_schema = (select database())
  and  table_rows >= 0
  and  table_name in ( 'auditlog',
             'county',
             'countyyear',
             'state',
             'zone',
             'zonemonthhour',
             'zoneroadtype',
             'year',
             'avft',
             'fuelformulation',
             'fuelsupply',
             'fuelusagefraction',
             'hourvmtfraction',
             'dayvmtfraction',
             'monthvmtfraction',
             'hpmsvtypeyear',
             'hotellingactivitydistribution',
             'hotellingagefraction',
             'hotellinghoursperday',
             'hotellinghourfraction',
             'hotellingmonthadjust',
             'starts',
             'startsageadjustment',
             'startshourfraction',
             'startsmonthadjust',
             'startsperday',
             'startsperdaypervehicle',
             'startsopmodedistribution',
             'idledayadjust',
             'idlemodelyeargrouping',
             'idlemonthadjust',
             'totalidlefraction',
             'avgspeeddistribution',
             'imcoverage',
             'onroadretrofit',
             'roadtypedistribution',
             'sourcetypeagedistribution',
             'sourcetypeyear',
             'emissionratebyage',
             'hotellinghours'
                     );

-- this chunk updates the first set of entries in cdb_checks to contain the number of rows in each table
update cdb_checks as a, tempb as b set a.count = b.table_rows where a.tablename = 'auditlog' and  b.tablename = 'auditlog'; 
update cdb_checks as a, tempb as b set a.count = b.table_rows where a.tablename = 'county' and  b.tablename = 'county';
update cdb_checks as a, tempb as b set a.count = b.table_rows where a.tablename = 'countyyear' and  b.tablename = 'countyyear';
update cdb_checks as a, tempb as b set a.count = b.table_rows where a.tablename = 'state' and  b.tablename = 'state';
update cdb_checks as a, tempb as b set a.count = b.table_rows where a.tablename = 'zone' and  b.tablename = 'zone';
update cdb_checks as a, tempb as b set a.count = b.table_rows where a.tablename = 'zonemonthhour' and  b.tablename = 'zonemonthhour';
update cdb_checks as a, tempb as b set a.count = b.table_rows where a.tablename = 'zoneroadtype' and  b.tablename = 'zoneroadtype';
update cdb_checks as a, tempb as b set a.count = b.table_rows where a.tablename = 'year' and  b.tablename = 'year';
update cdb_checks as a, tempb as b set a.count = b.table_rows where a.tablename = 'avft' and  b.tablename = 'avft';
update cdb_checks as a, tempb as b set a.count = b.table_rows where a.tablename = 'fuelformulation' and  b.tablename = 'fuelformulation';
update cdb_checks as a, tempb as b set a.count = b.table_rows where a.tablename = 'fuelsupply' and  b.tablename = 'fuelsupply';
update cdb_checks as a, tempb as b set a.count = b.table_rows where a.tablename = 'fuelusagefraction' and  b.tablename = 'fuelusagefraction';
update cdb_checks as a, tempb as b set a.count = b.table_rows where a.tablename = 'hourvmtfraction' and  b.tablename = 'hourvmtfraction';
update cdb_checks as a, tempb as b set a.count = b.table_rows where a.tablename = 'dayvmtfraction' and  b.tablename = 'dayvmtfraction';
update cdb_checks as a, tempb as b set a.count = b.table_rows where a.tablename = 'monthvmtfraction' and  b.tablename = 'monthvmtfraction';
update cdb_checks as a, tempb as b set a.count = b.table_rows where a.tablename = 'hpmsvtypeyear' and  b.tablename = 'hpmsvtypeyear';
update cdb_checks as a, tempb as b set a.count = b.table_rows where a.tablename = 'hotellingactivitydistribution' and  b.tablename = 'hotellingactivitydistribution';
update cdb_checks as a, tempb as b set a.count = b.table_rows where a.tablename = 'hotellingagefraction' and  b.tablename = 'hotellingagefraction';
update cdb_checks as a, tempb as b set a.count = b.table_rows where a.tablename = 'hotellinghoursperday' and  b.tablename = 'hotellinghoursperday';
update cdb_checks as a, tempb as b set a.count = b.table_rows where a.tablename = 'hotellinghourfraction' and  b.tablename = 'hotellinghourfraction';
update cdb_checks as a, tempb as b set a.count = b.table_rows where a.tablename = 'hotellingmonthadjust' and  b.tablename = 'hotellingmonthadjust';
update cdb_checks as a, tempb as b set a.count = b.table_rows where a.tablename = 'starts' and  b.tablename = 'starts';
update cdb_checks as a, tempb as b set a.count = b.table_rows where a.tablename = 'startsageadjustment' and  b.tablename = 'startsageadjustment';
update cdb_checks as a, tempb as b set a.count = b.table_rows where a.tablename = 'startshourfraction' and  b.tablename = 'startshourfraction';
update cdb_checks as a, tempb as b set a.count = b.table_rows where a.tablename = 'startsmonthadjust' and  b.tablename = 'startsmonthadjust';
update cdb_checks as a, tempb as b set a.count = b.table_rows where a.tablename = 'startsperday' and  b.tablename = 'startsperday';
update cdb_checks as a, tempb as b set a.count = b.table_rows where a.tablename = 'startsperdaypervehicle' and  b.tablename = 'startsperdaypervehicle';
update cdb_checks as a, tempb as b set a.count = b.table_rows where a.tablename = 'startsopmodedistribution' and  b.tablename = 'startsopmodedistribution';
update cdb_checks as a, tempb as b set a.count = b.table_rows where a.tablename = 'idledayadjust' and  b.tablename = 'idledayadjust';
update cdb_checks as a, tempb as b set a.count = b.table_rows where a.tablename = 'idlemodelyeargrouping' and  b.tablename = 'idlemodelyeargrouping';
update cdb_checks as a, tempb as b set a.count = b.table_rows where a.tablename = 'idlemonthadjust' and  b.tablename = 'idlemonthadjust';
update cdb_checks as a, tempb as b set a.count = b.table_rows where a.tablename = 'totalidlefraction' and  b.tablename = 'totalidlefraction';
update cdb_checks as a, tempb as b set a.count = b.table_rows where a.tablename = 'avgspeeddistribution' and  b.tablename = 'avgspeeddistribution';
update cdb_checks as a, tempb as b set a.count = b.table_rows where a.tablename = 'imcoverage' and  b.tablename = 'imcoverage';
update cdb_checks as a, tempb as b set a.count = b.table_rows where a.tablename = 'onroadretrofit' and  b.tablename = 'onroadretrofit';
update cdb_checks as a, tempb as b set a.count = b.table_rows where a.tablename = 'roadtypedistribution' and  b.tablename = 'roadtypedistribution';
update cdb_checks as a, tempb as b set a.count = b.table_rows where a.tablename = 'sourcetypeagedistribution' and  b.tablename = 'sourcetypeagedistribution';
update cdb_checks as a, tempb as b set a.count = b.table_rows where a.tablename = 'sourcetypeyear' and  b.tablename = 'sourcetypeyear';
update cdb_checks as a, tempb as b set a.count = b.table_rows where a.tablename = 'emissionratebyage' and  b.tablename = 'emissionratebyage';
update cdb_checks as a, tempb as b set a.count = b.table_rows where a.tablename = 'hotellinghours' and  b.tablename = 'hotellinghours';

-- present tables
update cdb_checks set testdescription = 'Present'       where `count` is not null;
                                  
update cdb_checks set testdescription = 'Table likely to be overwritten',
					  `status` = 'Warning'
														where `count` > 0
														and tablename in ('emissionratebyage', 'fuelsupply', 'fuelformulation', 'zonemonthhour');
                                                        
Update cdb_checks set testdescription = 'Table no longer used as user input',
					  `status` = 'Error'
														where `count` > 0
														and tablename in ('hotellinghours');
                                                        
-- Missing tables
update cdb_checks set testdescription = 'Table Missing' where count is null;
update cdb_checks set status          = 'Error'         where count is null and tablename not in ('countyyear');
update cdb_checks set status          = 'Warning'       where count is null and tablename in ('countyyear');

-- These tables are okay if they are missing
Update cdb_checks set status = 'todo', testdescription='table added by QA script and should be removed' WHERE count is null and tableName in ('countyyear', 'emissionratebyage', 'hotellinghours');

Delete from cdb_checks where testdescription = 'Present';


-- ----------------------------------------------------------------------------------------
-- Create missing tables to keep error-checking code from crashing-------------------------
-- Get table schemas from the default database to make sure the schemas are up to date ----
-- ----------------------------------------------------------------------------------------

-- AuditLog is the only input database table that is not in the default database
-- Ideally, we'd like to get this via SOURCE "database/CreateAuditLogTables.sql" but that isn't working at the moment
CREATE TABLE IF NOT EXISTS `auditlog` (
  `whenhappened`     datetime          not null,
  `importername`     varchar(100)      not null,
  `briefdescription` varchar(100)  default null,
  `fulldescription`  varchar(4096) default null,
  key `logbydate`     (`whenhappened`),
  key `logbyimporter` (`importername`)
) engine=myisam default charset=latin1 delay_key_write=1;

-- geography
create table if not exists `county` like ##defaultdb##.county;
create table if not exists `countyyear` like ##defaultdb##.countyyear;
create table if not exists `state` like ##defaultdb##.state;
create table if not exists `zone` like ##defaultdb##.zone;
create table if not exists `zonemonthhour` like ##defaultdb##.zonemonthhour;
create table if not exists `zoneroadtype` like ##defaultdb##.zoneroadtype;
create table if not exists `year` like ##defaultdb##.year;

-- fuels
create table if not exists `avft` like ##defaultdb##.avft;
create table if not exists `fuelformulation` like ##defaultdb##.fuelformulation;
create table if not exists `fuelsupply` like ##defaultdb##.fuelsupply;
create table if not exists `fuelusagefraction` like ##defaultdb##.fuelusagefraction;

-- vmt
create table if not exists `hourvmtfraction` like ##defaultdb##.hourvmtfraction;
create table if not exists `dayvmtfraction` like ##defaultdb##.dayvmtfraction;
create table if not exists `monthvmtfraction` like ##defaultdb##.monthvmtfraction;
create table if not exists `hpmsvtypeyear` like ##defaultdb##.hpmsvtypeyear;

-- hotelling
create table if not exists `hotellingactivitydistribution` like ##defaultdb##.hotellingactivitydistribution;
create table if not exists `hotellingagefraction` like ##defaultdb##.hotellingagefraction;
create table if not exists `hotellinghoursperday` like ##defaultdb##.hotellinghoursperday;
create table if not exists `hotellinghourfraction` like ##defaultdb##.hotellinghourfraction;
create table if not exists `hotellingmonthadjust` like ##defaultdb##.hotellingmonthadjust;

-- starts
create table if not exists `starts` like ##defaultdb##.`starts`;
create table if not exists `startsageadjustment` like ##defaultdb##.startsageadjustment;
create table if not exists `startshourfraction` like ##defaultdb##.startshourfraction;
create table if not exists `startsmonthadjust` like ##defaultdb##.startsmonthadjust;
create table if not exists `startsperday` like ##defaultdb##.startsperday;
create table if not exists `startsperdaypervehicle` like ##defaultdb##.startsperdaypervehicle;
create table if not exists `startsopmodedistribution` like ##defaultdb##.startsopmodedistribution;

-- idle
create table if not exists `idledayadjust` like ##defaultdb##.idledayadjust;
create table if not exists `idlemodelyeargrouping` like ##defaultdb##.idlemodelyeargrouping;
create table if not exists `idlemonthadjust` like ##defaultdb##.idlemonthadjust;
create table if not exists `totalidlefraction` like ##defaultdb##.totalidlefraction;

-- other input tables
create table if not exists `avgspeeddistribution` like ##defaultdb##.avgspeeddistribution;
create table if not exists `imcoverage` like ##defaultdb##.imcoverage;
create table if not exists `onroadretrofit` like ##defaultdb##.onroadretrofit;
create table if not exists `roadtypedistribution` like ##defaultdb##.roadtypedistribution;
create table if not exists `sourcetypeagedistribution` like ##defaultdb##.sourcetypeagedistribution;
create table if not exists `sourcetypeyear` like ##defaultdb##.sourcetypeyear;
create table if not exists `emissionratebyage` like ##defaultdb##.emissionratebyage;


-- ##############################################################################
-- start data qa checks
-- ##############################################################################

--       check no. 1001 -- check that one and only one of hpmsvtypeyear, hpmsvtypeday, sourcetypeyearvmt, sourcetypedayvmt has at least 1 row
insert into qa_checks_log values ( 1001, 'OK', @hVersion, curDate(), curTime() );   
Insert into cdb_checks
 ( tablename,
   checknumber,
   testdescription,
   testvalue,
   `count`   )
 select
  "vmt tables" as tablename,
   1001                       as checknumber,
  "# vmt tables with data <> 1"                       as testdescription,
  group_concat(distinct tablename separator ', ') as testvalue,
  sum(tableisused)                                as `count`
from (
  select distinct 'hpmsvtypeday' as tablename, 1 as tableisused from `hpmsvtypeday`
  union
  select distinct 'hpmsvtypeyear' as tablename, 1 as tableisused from `hpmsvtypeyear`
  union
  select distinct 'sourcetypedayvmt' as tablename, 1 as tableisused from `sourcetypedayvmt`
  union
  select distinct 'sourcetypeyearvmt' as tablename, 1 as tableisused from `sourcetypeyearvmt`
) as t1
having `count` <> 1;

--       check no. 1002 -- Record number of rows in HPMSVTypeDay
INSERT INTO qa_checks_log values ( 1002, 'OK', @hVersion, curDate(), curTime() );
Insert into cdb_checks
 ( tablename,
   checknumber,
   testdescription,
   testvalue,
   msgtype   )
 values
 ("hpmsvtypeday",
   1002,
  "number of rows",
  (select count(*) from hpmsvtypeday),
  "info" );

--       check no. 1003 -- Record number of rows in HPMSVtypeYear
INSERT INTO qa_checks_log values ( 1003, 'OK', @hVersion, curDate(), curTime() );
Insert into cdb_checks
 ( tablename,
   checknumber,
   testdescription,
   testvalue,
   msgtype   )
 values
 ("hpmsvtypeyear",
   1003,
  "number of rows",
  (select count(*) from hpmsvtypeyear),
  "info" );

--       check no. 1004 -- Record number of rows in SourceTypeDayVMT
INSERT INTO qa_checks_log values ( 1004, 'OK', @hVersion, curDate(), curTime() );
Insert into cdb_checks
 ( tablename,
   checknumber,
   testdescription,
   testvalue,
   msgtype   )
 values
 ("sourcetypedayvmt",
   1004,
  "number of rows",
  (select count(*) from sourcetypedayvmt), 
  "info" );

--       check no. 1005 -- Record number of rows in SourceTypeYearVMT
INSERT INTO qa_checks_log values ( 1005, 'OK', @hVersion, curDate(), curTime() );
Insert into cdb_checks
 ( tablename,
   checknumber,
   testdescription,
   testvalue,
   msgtype   )
 values
 ("sourcetypeyearvmt",
   1005,
  "number of rows",
  (select count(*) from sourcetypeyearvmt),
  "info" );

--       check no. 1006 -- Check that 0 or 1 total of startsPerDay or startsPerDayPerVehicle or starts are included
INSERT INTO qa_checks_log values ( 1006, 'OK', @hVersion, curDate(), curTime() );
Insert into cdb_checks
 ( tablename,
   checknumber,
   testdescription,
   testvalue,
   `count`   )
 select
  "starts or startsperday or startsperdaypervehicle" as tablename,
   1006                        as checknumber,
  "number of starts tables used"                     as testdescription,
  group_concat(distinct tablename separator ', ')    as testvalue,
  sum(tableisused)                                   as `count`
from (
  select distinct 'starts' as tablename, 1 as tableisused from `starts`
  union
  select distinct 'startsperday' as tablename, 1 as tableisused from `startsperday`
  union
  select distinct 'startsperdaypervehicle' as tablename, 1 as tableisused from `startsperdaypervehicle`
) as t1
having `count` > 1;

--       check no. 1007 -- Record number of rows in starts
INSERT INTO qa_checks_log values ( 1007, 'OK', @hVersion, curDate(), curTime() );
Insert into CDB_Checks
 ( tablename,
   checknumber,
   testdescription,
   testvalue,
   msgtype   )
 values
 ("starts",
   1007,
  "number of rows",
  (select count(*) from `starts`),
  "info" );

--       check no. 1008 -- record number of rows in startsperday
insert into qa_checks_log values ( 1008, 'OK', @hVersion, curDate(), curTime() );
Insert into cdb_checks
 ( tablename,
   checknumber,
   testdescription,
   testvalue,
   msgtype   )
 values
 ("startsperday",
   1008,
  "number of rows",
  (select count(*) from `startsperday`),
  "info" );

--       check no. 1009 -- record number of rows in startsperdaypervehicle
insert into qa_checks_log values ( 1009, 'OK', @hVersion, curDate(), curTime() );
Insert into cdb_checks
 ( tablename,
   checknumber,
   testdescription,
   testvalue,
   msgtype   )
 values
 ("startsperdaypervehicle",
   1009,
  "number of rows",
  (select count(*) from `startsperdaypervehicle`),
  "info" );


-- year
insert into cdb_checks (checknumber, tablename, testdescription) values (1100, "year", "Table Check:");

--       check no. 1101 -- check that isBaseYear is either Y, N, y, n
INSERT INTO qa_checks_log values ( 1101, 'OK', @hVersion, curDate(), curTime() );
Drop table if exists tempa;
create table tempa
select   isbaseyear   as isbaseyear2,
         'no '        as amatch,
         count(*)     as n
from     year
group by isbaseyear2;

update tempa as a set amatch='yes' where isbaseyear2 in ('Y', 'N', 'y', 'n');

Insert into cdb_checks
       ( tablename,
         checknumber,
         testdescription,
         testvalue,
         count  )
select   "year"                  as tablename,
         1101,
         "isbaseyear not Y or N" as testdescription,
         null                    as testvalue,
         n                       as count
from     tempa
where    amatch <> 'yes';

--       check no. 1102 -- check that fuelYearID is the same as yearID in this table
INSERT INTO qa_checks_log values ( 1102, 'OK', @hVersion, curDate(), curTime() );
Drop table if exists tempa;
create table tempa
select   fuelyearid   as fuelyearid2,
         yearid       as yearid2,
         'no '        as amatch,
         count(*)     as n
from     year
group by fuelyearid2,
         yearid2;

Update tempa set amatch='yes' where fuelyearid2=yearid2;

insert into cdb_checks
       ( tablename,
         checknumber,
         testdescription,
         testvalue,
         count     )
Select   "year"                          as tablename,
         1102                             as checknumber,
        "fuelyearid not equal to yearid" as testdescription,
         fuelyearid2                     as testvalue,
         n                               as count
from     tempa
where    amatch <> 'yes';

--       check no. 1103 -- check for unknown yearIDs
INSERT INTO qa_checks_log values ( 1103, 'OK', @hVersion, curDate(), curTime() );
Drop table if exists tempa;
create table tempa
select   yearid       as yearid2,
         'no '        as amatch,
         count(*)     as n
from     year
group by yearid2;
                
update tempa as a 
inner join ##defaultdb##.year as m on a.yearId2 = m.yearId
set amatch='yes';

Insert into cdb_checks
       ( tablename,
         checknumber,
         testdescription,
         testvalue,
         count  )
select   "year"   as tablename,
         1103,
        "yearid"  as testdescription,
         yearid2  as testvalue,
         n        as count
from     tempa
where    amatch <> 'yes';

--       check no. 1104 -- check that yearID matches the name of the database (cXXXXXyYYYY)
INSERT INTO qa_checks_log values ( 1104, 'OK', @hVersion, curDate(), curTime() );
Drop table if exists tempa;
create table tempa
select   yearid       as yearid2,
         'no '        as amatch,
         count(*)     as n
from     year
group by yearid2;

update tempa as a set amatch='yes' where mid((select database() from dual), 7, 5) = concat('y', yearid2);

Insert into cdb_checks
       ( tablename,
         checknumber,
         testdescription,
         testvalue,
         count  )
select   "year"   as tablename,
         1104,
        "yearid doesn't match database name"  as testdescription,
         yearid2  as testvalue,
         n        as count
from     tempa
where    amatch <> 'yes';

-- state
Insert into cdb_checks (checknumber, tablename, testdescription) values (1200, "state", "Table Check:");

--       check no. 1201 -- check for unknown stateID
INSERT INTO qa_checks_log values ( 1201, 'OK', @hVersion, curDate(), curTime() );
Drop table if exists tempa;
create table tempa
select   stateid      as stateid2,
         'no '        as amatch,
         count(*)     as n
from     state
group by stateid2;
                    
Update tempa as a 
inner join ##defaultdb##.state as m on a.stateid2 = m.stateid
set amatch='yes';

Insert into cdb_checks
       ( tablename,
         checknumber,
         testdescription,
         testvalue,
         count  )
select   "state"             as tablename,
         1201,
         "stateid not valid" as testdescription,
         stateid2            as testvalue,
         n                   as count
from     tempa
where    amatch <> 'yes';

--       check no. 1202 -- check for unknown idleRegionID
INSERT INTO qa_checks_log values ( 1202, 'OK', @hVersion, curDate(), curTime() );
Drop table if exists tempa;
create table tempa
select   idleregionid as idleregionid2,
         'no '        as amatch,
         count(*)     as n
from     state
group by idleregionid2;

update tempa as a 
inner join ##defaultdb##.idleregion as m on a.idleregionid2 = m.idleregionid
set amatch='yes';

Insert into Cdb_checks
       ( tablename,
         checknumber,
         testdescription,
         testvalue,
         count  )
select   "state"             as tablename,
         1202,
         "idleregionid not valid" as testdescription,
         idleregionid2       as testvalue,
         n                   as count
from     tempa
where    amatch <> 'yes';

--       check no. 1203 -- check that state has at least 1 row
insert into qa_checks_log values ( 1203, 'OK', @hVersion, curDate(), curTime() );
Insert into cdb_checks
 ( tablename,
   checknumber,
   testdescription,
   testvalue )
 values
 ("state",
   1203,
  "number of rows",
  (select count(*) from state) );
delete from cdb_checks where checknumber=1203 and testvalue>0;


-- check for the county table
insert into cdb_checks (checknumber, tablename, testdescription) values (1300, "county", "Table Check:");

--       check no. 1301: check for unknown countyIDs
INSERT INTO qa_checks_log values ( 1301, 'OK', @hVersion, curDate(), curTime() );
Drop table if exists tempa;
create table tempa
select   countyid    as countyid2,
         'no '       as amatch,
         count(*)    as n
from     county
group by countyid2;

update tempa as a 
inner join ##defaultdb##.county as m on a.countyid2 = m.countyid
set amatch='yes';

insert into cdb_checks
       ( tablename,
         checknumber,
         testdescription,
         testvalue,
         count  )
select   "county"   as tablename,
         1301,
         "countyid" as testdescription,
         countyid2  as testvalue,
         n          as count
from     tempa
where    amatch <> 'yes';

--       check no. 1302 -- check that countyID matches the name of the database (cXXXXXyYYYY)
INSERT INTO qa_checks_log values ( 1302, 'OK', @hVersion, curDate(), curTime() );
Drop table if exists tempa;
create table tempa
select   countyid     as countyid2,
         'no '        as amatch,
         count(*)     as n
from     county
group by countyid2;

Update tempa as a set amatch='yes' where convert(mid((select DATABASE() from dual), 2, 5), UNSIGNED) = countyid2;

insert into cdb_checks
       ( tablename,
         checknumber,
         testdescription,
         testvalue,
         count  )
select   "county"   as tablename,
         1302,
        "countyid doesn't match database name"  as testdescription,
         countyid2  as testvalue,
         n        as count
from     tempa
where    amatch <> 'yes';

--       check no. 1303: check to make sure the altitude field is l or h
insert into qa_checks_log values ( 1303, 'OK', @hVersion, curDate(), curTime() );
Drop table if exists tempa;
create table tempa
select   altitude     as altitude2,
         'no '        as amatch,
         count(*)     as n
from     county
group by altitude2;

Update tempa as a set amatch='yes' where altitude2 in ('L','H');

insert into cdb_checks
       ( tablename,
         checknumber,
         testdescription,
         -- testvalue,
         count  )
select   "county"              as tablename,
         1303,
         "altitude not L or H" as testdescription,
         -- altitude2          as testvalue,
         n                     as count
from     tempa
where    amatch <> 'yes';

--       check no. 1304: make sure gpafract is between 0 and 1
insert into qa_checks_log values ( 1304, 'OK', @hVersion, curDate(), curTime() );
Drop table if exists tempa;
create table tempa
select   gpafract     as gpafract2,
         'no '        as amatch,
         count(*)     as n
from     county
group by gpafract2;

update tempa set amatch='yes' where gpafract2>=0.0 and gpafract2<=1.0;

Insert into Cdb_checks
       ( tablename,
         checknumber,
         testdescription,
         testvalue,
         count  )
select   "county"       as tablename,
         1304,
         "gpacfract"    as testdescription,
         gpafract2      as testvalue,
         n              as count
from     tempa
where    amatch <> 'yes';

--       check no. 1305: make sure the barometricPressure field is between 20 and 33
INSERT INTO qa_checks_log values ( 1305, 'OK', @hVersion, curDate(), curTime() );
Drop table if exists tempa;
create table tempa
select   barometricpressure as barpre,
         'no '              as amatch,
         count(*)           as n
from     county
group by barpre;

Update tempa as a set amatch='yes' where barpre>=20.0 and barpre<=33.0;

insert into cdb_checks
       ( tablename,
         checknumber,
         testdescription,
         testvalue,
         count  )
select   "county"             as tablename,
         1305,
         "barometricpressure" as testdescription,
         barpre               as testvalue,
         n                    as count
from     tempa
where    amatch <> 'yes';

--       check no. 1306: check for unknown stateIDs
INSERT INTO qa_checks_log values ( 1306, 'OK', @hVersion, curDate(), curTime() );
Drop table if exists tempa;
Create table tempa
select   stateid      as stateid2,
         'no '        as amatch,
         count(*)     as n
from     county
group by stateid2;

Update tempa as a 
inner join state as c on a.stateid2 = c.stateid
set amatch='yes';

insert into cdb_checks
       ( tablename,
         checknumber,
         testdescription,
         testvalue,
         count  )
select   "county"            as tablename,
         1306,
         "stateid not valid" as testdescription,
         stateid2            as testvalue,
         n                   as count
from     tempa
where    amatch <> 'yes';

--       check no. 1307 -- check that county has at least 1 row
INSERT INTO qa_checks_log values ( 1307, 'OK', @hVersion, curDate(), curTime() );
Insert into cdb_checks
 ( tablename,
   checknumber,
   testdescription,
   testvalue )
 values
 ("county",
   1307,
  "Number of Rows",
  (Select count(*) from county) );
Delete from cdb_checks where checknumber=1307 and testvalue>0;

--       check no. 1308 -- check for unknown countytypeid
insert into qa_checks_log values ( 1308, 'OK', @hVersion, curDate(), curTime() );
Drop table if exists tempa;
create table tempa
select   countytypeid as countytypeid2,
         'no '        as amatch,
         count(*)     as n
from     county
group by countytypeid2;

Update tempa as a 
inner join ##defaultdb##.countytype as m on a.countytypeid2 = m.countytypeid
set amatch='yes';

Insert into cdb_checks
       ( tablename,
         checknumber,
         testdescription,
         testvalue,
         count  )
select   "county"             as tablename,
         1308,
         "countytypeid not valid" as testdescription,
         countytypeid2       as testvalue,
         n                   as count
from     tempa
where    amatch <> 'yes';


-- Zone
Insert into cdb_checks (checknumber, tablename, testdescription) values (1400, "zone", "Table Check:");

--       check no. 1401 -- check for unknown countyIDs
INSERT INTO qa_checks_log values ( 1401, 'OK', @hVersion, curDate(), curTime() );
Drop table if exists tempa;
Create table tempa
select   countyid        as countyid2,
         'no '           as amatch,
         count(*)        as n
from     zone
group by countyid2;

Update tempa as a 
inner join county as c on a.countyid2 = c.countyid
set amatch='yes';

Insert into Cdb_checks
       ( tablename,
         checknumber,
         testdescription,
         testvalue,
         count  )
select   "zone"     as tablename,
         1401,
        "countyid"  as testdescription,
         countyid2  as testvalue,
         n          as count               --
from     tempa
where    amatch <> 'yes';

--       check no. 1402 -- check that the startAllocFactor sums to 1
INSERT INTO qa_checks_log values ( 1402, 'OK', @hVersion, curDate(), curTime() );
Insert into cdb_checks
       ( tablename,
         checknumber,
         testdescription,
         testvalue    )
select  "zone"                           as tablename,
         1402                            as checknumber,
        "sum of startallocfactor <> 1.0" as testdescription,
         sum(startallocfactor)           as testvalue
from     zone
having   testvalue <0.99999 or testvalue >1.00001;

--       check no. 1403 -- checks that the idleAllocFactor sums to 1
INSERT INTO qa_checks_log values ( 1403, 'OK', @hVersion, curDate(), curTime() );
Insert into CDB_Checks
       ( tableName,
         checkNumber,
         testDescription,
         testValue    )
Select  "zone"                          as tableName,
         1403                            as checkNumber,
        "sum of idleAllocFactor <> 1.0" as testDescription,
         sum(idleAllocFactor)           as testValue
From     zone
Having   testValue <0.99999 or testValue >1.00001;

--       check no. 1404 -- checks that the SHPAllocFactor sums to 1
INSERT INTO QA_Checks_Log values ( 1404, 'OK', @hVersion, curDate(), curTime() );
Insert into CDB_Checks
       ( tableName,
         checkNumber,
         testDescription,
         testValue    )
Select  "zone"                         as tableName,
         1404                           as checkNumber,
        "sum of SHPAllocFactor <> 1.0" as testDescription,
         sum(SHPAllocFactor)           as testValue
From     zone
Having   testValue <0.99999 or testValue >1.00001;

--       check no. 1405 -- check that zoneID is consistent with countyID
INSERT INTO QA_Checks_Log values ( 1405, 'OK', @hVersion, curDate(), curTime() );
Insert into CDB_Checks
       ( TableName,
         CheckNumber,
         TestDescription,
         testValue,
         count  )
Select   "zone"   as tableName,
         1405	  as CheckNumber,
        "zoneId does not match countyID*10"  as testDescription,
         zoneId   as testValue,
         count(*) as count
From     zone
Group By zoneID, countyID
Having   zoneID <> countyID * 10;

--       check no. 1406 -- check that zone has at least 1 row
INSERT INTO QA_Checks_Log values ( 1406, 'OK', @hVersion, curDate(), curTime() );
Insert into CDB_Checks
 ( TableName,
   checkNumber,
   TestDescription,
   testValue )
 values
 ("zone",
   1406,
  "Number of Rows",
  (Select count(*) from zone) );
Delete from CDB_Checks where checkNumber=1406 and testValue>0;


--       Table  Check: avft
Insert into CDB_Checks (CheckNumber, TableName, TestDescription) values (1500, "avft", "Table Check:");

--       check no. 1501: check for unknown sourceTypeIDs (e.g., 22)
INSERT INTO QA_Checks_Log values ( 1501, 'OK', @hVersion, curDate(), curTime() );
Drop table if exists tempA;
Create table tempA
Select   sourceTypeId as sourceTypeId2,
         'no '        as aMatch,
         count(*)     as n
From     avft
Group by sourceTypeId2;
                        
Update tempA as a 
inner join ##defaultdb##.sourceUseType as m on a.sourceTypeId2 = m.sourceTypeId
set aMatch='yes';										  
										  
Insert into CDB_Checks
       ( TableName,
         CheckNumber,
         TestDescription,
         testValue,
         count  )
Select   "avft"        as tableName,
         1501,
        "sourceTypeId" as testDescription,
         sourceTypeId2 as testValue,
         n             as count               --
From     tempA
Where    aMatch <> 'yes';

--       check no. 1502: Check for unknown modelYearIDs (e.g., 2061)
INSERT INTO QA_Checks_Log values ( 1502, 'OK', @hversion, curDate(), curTime() );
Drop table if exists tempA;
Create table tempA
Select   modelYearId  as modelYearId2,
         'no '        as aMatch,
         count(*)     as n
From     avft
Group by modelYearId2;
                    
Update tempA as a 
inner join ##defaultdb##.modelYear as m on a.modelYearId2 = m.modelYearId
set aMatch='yes';										  

Insert into CDB_Checks
       ( TableName,
         CheckNumber,
         TestDescription,
         testValue,
         count  )
Select   "avft"       as tableName,
         1502,
        "modelYearId" as testDescription,
         modelYearId2 as testValue,
         n            as count               --
From     tempA
Where    aMatch <> 'yes';

--       check no. 1503: check for unknown fuelTypeIDs (e.g., 10)
INSERT INTO QA_Checks_Log values ( 1503, 'OK', @hVersion, curDate(), curTime() );
Drop table if exists tempA;
Create table tempA
Select   fuelTypeId  as fuelTypeId2,
         'no '       as aMatch,
         count(*)    as n
From     avft
Group by fuelTypeId2;
                
Update tempA as a 
inner join ##defaultdb##.fuelType as m on a.fuelTypeId2 = m.fuelTypeId
set aMatch='yes';										  

Insert into CDB_Checks
       ( TableName,
         CheckNumber,
         TestDescription,
         testValue,
         count  )
Select   "avft"      as tableName,
         1503,
        "fuelTypeId" as testDescription,
         fuelTypeId2 as testValue,
         n           as count               --
From     tempA
Where    aMatch <> 'yes';

--       check no. 1504: check for unknown engTechIDs
INSERT INTO QA_Checks_Log values ( 1504, 'OK', @hVersion, curDate(), curTime() );
Drop table if exists tempA;
Create table tempA
Select   engTechId  as engTechId2,
         'no '      as aMatch,
         count(*)   as n
From     avft
Group by engTechId2;

Update tempA as a 
inner join ##defaultdb##.engineTech as m on a.engTechId2 = m.engTechId
set aMatch='yes';		

Insert into CDB_Checks
       ( TableName,
         CheckNumber,
         TestDescription,
         testValue,
         count  )
Select   "avft"     as tableName,
         1504,
        "engTechId" as testDescription,
         engTechId2 as testValue,
         n          as count
From     tempA
Where    aMatch <> 'yes';

--       check no. 1505: check for bad fuelEngFractions in avft
--                       allow fractions up to 1.00001 because if you export defaults, MOVES is doing joins
--                       in the background, and it is plausible that it would end up with floating point noise
--                       above 1 for an individual row.
INSERT INTO QA_Checks_Log values ( 1505, 'OK', @hVersion, curDate(), curTime() );
Drop table if exists tempA;
Create table tempA
Select   fuelEngFraction as fuelEngFraction2,
         'no '           as aMatch,
         count(*)        as n
From     avft
Group by fuelEngFraction2;

Update tempA as a set aMatch='yes' where fuelEngFraction2 between 0.0 and 1.00001;

Insert into CDB_Checks
       ( TableName,
         CheckNumber,
         TestDescription,
         testValue,
         count  )
Select   "avft"           as tableName,
         1505,
        "fuelEngFraction" as testDescription,
         fuelEngFraction2 as testValue,
         n                as count
From     tempA
Where    aMatch <> 'yes';

--       check no. 1506 -- check that the fuelEngFracion distributions of AVFT table sums to 1
Drop table if exists tempA;
Create table tempA
select   sourceTypeId,
         modelYearId,
         sum(fuelEngFraction) as distribution
from     avft
group by sourceTypeId,
         modelYearId;

update tempA set distribution = 1.0 where distribution > 0.99999 and distribution < 1.00001;

INSERT INTO QA_Checks_Log values ( 1506, 'OK', @hVersion, curDate(), curTime() );
  Insert into CDB_Checks
       ( tableName,
         checkNumber,
         testDescription,
         testValue
       )
  Select 'avft'                 as tableName,
          1506                   as checkNumber,
         'Sum <> 1.0'           as testDescription,
         ( select count(*)
           from   tempA
           where  distribution <> 1.0 ) as testValue;
delete from CDB_Checks where checkNumber = 1506 and testValue = 0;
Drop table if exists tempA;

--       check no. 1507: check for missing source type, fuel type, and model year combinations
-- 		 Note: this completeness check is different from others in that not all combinations of 
-- 		 source type, fuel type, and model year are valid (i.e., no diesel motorcycles).
-- 		 So this table checks for completeness vs. samplevehiclepopulation, which contains this definition
--       Also, only check for the existence of modelyearids that will appear in the run (according to the year table)
INSERT INTO QA_Checks_Log values ( 1507, 'OK', @hVersion, curDate(), curTime() );
  Insert into CDB_Checks
       ( tableName,
         checkNumber,
         testDescription,
         testValue
       )
  Select 'avft'                 as tableName,
          1507                  as checkNumber,
         'Missing combination of valid sourceTypeID, fuelTypeID, and modelYearID'           as testDescription,
		 concat('ST: ', sourceTypeID, ', FT: ', fuelTypeID, ', MY: ', modelyearID) as testValue
  from (select distinct sourceTypeID, fuelTypeID, modelYearID
		from ##defaultdb##.samplevehiclepopulation
        join `year`
        where modelYearID between yearID-30 and yearID) as t1
  left join avft using (sourceTypeID, fuelTypeID, modelYearID)
	where fuelEngFraction is NULL 
	ORDER BY sourceTypeID, fuelTypeID, modelYearID LIMIT 1;


--       Table  Check: avgspeeddistribution
Insert into CDB_Checks (CheckNumber, TableName, TestDescription) values (1600, "avgspeeddistribution", "Table Check:");

--       check no. 1601: checks for unknown avgSpeedBinIDs (e.g., 17)
INSERT INTO QA_Checks_Log values ( 1601, 'OK', @hVersion, curDate(), curTime() );
Drop table if exists tempA;
Create table tempA
Select   avgSpeedBinId   as avgSpeedBinId2,
         'no '           as aMatch,
         count(*)        as n
From     avgSpeedDistribution
Group by avgSpeedBinId2;

Update   tempA as a set aMatch='yes' where (Select m.avgSpeedBinId
                                            From   ##defaultdb##.avgSpeedBin as m
                                            Where  a.avgSpeedBinId2 = m.avgSpeedBinId);

Insert into CDB_Checks
       ( TableName,
         CheckNumber,
         TestDescription,
         testValue,
         count  )
Select   "avgSpeedDistribution"    as tableName,
         1601,
         "avgSpeedBinId Not Valid" as testDescription,
         avgSpeedBinId2            as testValue,
         n                         as count
From     tempA
Where    aMatch <> 'yes';

--       check no. 1602: make sure the avgSpeedFraction sums to 1 for each sourceTypeID, roadTypeID, and hourDayID
INSERT INTO QA_Checks_Log values ( 1602, 'OK', @hVersion, curDate(), curTime() );
Insert into CDB_Checks
       ( tableName,
         checkNumber,
         testDescription,
         testValue,
           sourceTypeId,
           roadTypeId,
           hourDayId )
Select  "avgspeeddistribution"           as tableName,
         1602                              as checkNumber,
        "sum of avgSpeedFraction <> 1.0" as testDescription,
         sum(avgSpeedFraction)           as testValue,
           sourceTypeId,
           roadTypeId,
           hourDayId
From     avgspeeddistribution
Group by sourceTypeId,
         roadTypeId,
         hourDayId
Having   testValue <0.99999 or testValue >1.00001;

--       check no. 1603: checks for unknown hourDayIDs
INSERT INTO QA_Checks_Log values ( 1603, 'OK', @hVersion, curDate(), curTime() );
Drop table if exists tempA;
Create table tempA
Select   hourDayId       as hourDayId2,
         'no '           as aMatch,
         count(*)        as n
From     avgSpeedDistribution
Group by hourDayId2;

Update tempA as a 
inner join ##defaultdb##.hourDay as m on a.hourDayId2 = m.hourDayId
set aMatch='yes';	

Insert into CDB_Checks
       ( TableName,
         CheckNumber,
         TestDescription,
         testValue,
         count  )
Select   "avgSpeedDistribution" as tableName,
         1603,
         "hourDayId"            as testDescription,
         hourDayId2             as testValue,
         n                      as count
From     tempA
Where    aMatch <> 'yes';

--       check no. 1604: check for unkown roadTypeIDs
INSERT INTO QA_Checks_Log values ( 1604, 'OK', @hVersion, curDate(), curTime() );
Drop table if exists tempA;
Create table tempA
Select   roadTypeId      as roadTypeId2,
         'no '           as aMatch,
         count(*)        as n
From     avgSpeedDistribution
Group by roadTypeId2;
									  
Update tempA as a 
inner join ##defaultdb##.roadType as m on a.roadTypeId2 = m.roadTypeId
set aMatch='yes';	

Insert into CDB_Checks
       ( TableName,
         CheckNumber,
         TestDescription,
         testValue,
         count  )
Select   "avgSpeedDistribution" as tableName,
         1604,
         "roadTypeId"           as testDescription,
         roadTypeId2            as testValue,
         n                      as count
From     tempA
Where    aMatch <> 'yes';

--       check no. 1605: check for unknown sourceTypeIDs
INSERT INTO QA_Checks_Log values ( 1605, 'OK', @hVersion, curDate(), curTime() );
Drop table if exists tempA;
Create table tempA
Select   sourceTypeId    as sourceTypeId2,
         'no '           as aMatch,
         count(*)        as n
From     avgSpeedDistribution
Group by sourceTypeId2;

Update tempA as a 
inner join ##defaultdb##.sourceUseType as m on a.sourceTypeId2 = m.sourceTypeId
set aMatch='yes';	

Insert into CDB_Checks
       ( TableName,
         CheckNumber,
         TestDescription,
         testValue,
         count  )
Select   "avgSpeedDistribution" as tableName,
         1605,
         "sourceTypeId"         as testDescription,
         sourceTypeId2          as testValue,
         n                      as count
From     tempA
Where    aMatch <> 'yes';

--       check no. 1606: check for missing sourceTypeID, roadTypeID, hourDayID, avgSpeedBinID combinations
INSERT INTO QA_Checks_Log values ( 1606, 'OK', @hVersion, curDate(), curTime() );
Insert into CDB_Checks
       ( TableName,
         CheckNumber,
         TestDescription,
         testValue)
Select   "avgSpeedDistribution" as tableName,
         1606,
         'Missing combination of valid sourceTypeID, roadTypeID, hourDayID, avgSpeedBinID' as testDescription,
		 concat('ST: ', sourceTypeID, ', RT: ', roadTypeID, ', HD: ', hourDayID, ', SB: ', avgSpeedBinID) as testValue
from (
	SELECT sourceTypeID, roadTypeID, hourDayID, avgSpeedBinID
	FROM  ##defaultdb##.sourceusetype
	CROSS JOIN ##defaultdb##.roadtype on roadTypeID in (2, 3, 4, 5)
	CROSS JOIN ##defaultdb##.hourday
	CROSS JOIN ##defaultdb##.avgspeedbin
) as t1 left join avgspeeddistribution using (sourceTypeID, roadTypeID, hourDayID, avgSpeedBinID)
where avgSpeedFraction is NULL 
ORDER BY sourceTypeID, roadTypeID, hourDayID, avgSpeedBinID LIMIT 1;

--       check no. 1607: make sure no fractions are 1
INSERT INTO QA_Checks_Log values ( 1607, 'OK', @hVersion, curDate(), curTime() );
Insert into CDB_Checks
       ( TableName,
         CheckNumber,
         TestDescription,
         testValue)
Select   "avgSpeedDistribution" as tableName,
         1607,
         'avgSpeedFraction >= 1' as testDescription,
		 concat('ST: ', sourceTypeID, ', RT: ', roadTypeID, ', HD: ', hourDayID,
                ', SB: ', avgSpeedBinID, ', avgSpeedFraction = ', avgSpeedFraction) as testValue
FROM  avgspeeddistribution
where avgSpeedFraction >= 1.0
ORDER BY sourceTypeID, roadTypeID, hourDayID, avgSpeedBinID LIMIT 1;

--       check no. 1608: make sure no profiles are flat
INSERT INTO QA_Checks_Log values ( 1608, 'OK', @hVersion, curDate(), curTime() );
Insert into CDB_Checks
       ( TableName,
         CheckNumber,
         TestDescription,
         testValue,
         sourceTypeID,
         roadTypeID,
         hourDayID)
Select   "avgSpeedDistribution" as tableName,
         1608 as checkNumber,
         'avgSpeedFraction is a flat profile' as testDescription,
         concat('all are ', avgSpeedFraction) as testValue,
		 sourceTypeID,
         roadTypeID,
         hourDayID
from avgspeeddistribution
group by sourceTypeID, roadTypeID, hourDayID, avgSpeedFraction
having count(*) = (select count(*) from ##defaultdb##.avgspeedbin)
order by sourceTypeID, roadTypeID, hourDayID, avgSpeedBinID LIMIT 1;

--       check no. 1609: make sure weekend and weekday profiles are different
INSERT INTO QA_Checks_Log values ( 1609, 'OK', @hVersion, curDate(), curTime() );
Insert into CDB_Checks
       ( TableName,
         CheckNumber,
         TestDescription,
         sourceTypeID,
         roadTypeID,
         hourID)
Select   "avgSpeedDistribution" as tableName,
         1609 as checkNumber,
         'avgSpeedFraction is the same between weekend and weekday' as testDescription,
		 sourceTypeID,
         roadTypeID,
         hourID
from (select sourceTypeID, roadTypeID, hourID, avgSpeedBinID, avgSpeedFraction as weekendFraction
	  from avgspeeddistribution
	  join ##defaultdb##.hourday using (hourDayID)
	  where dayID = 2) as we
join (select sourceTypeID, roadTypeID, hourID, avgSpeedBinID, avgSpeedFraction as weekdayFraction
	  from avgspeeddistribution
	  join ##defaultdb##.hourday using (hourDayID)
	  where dayID = 5) as wd using (sourceTypeID, roadTypeID, hourID, avgSpeedBinID)
group by sourceTypeID, roadTypeID, hourID
having sum(abs(weekendFraction - weekdayFraction)) < 0.00001
order by sourceTypeID, roadTypeID, hourID LIMIT 1;

--       check no. 1610: make sure weekend and weekday profiles are different
--  	 compare distributions by sourceTypeID and hourDayID for each road type pair 
--  	 by summing the absolute differences by avgSpeedBinID, and anywhere the sum
--       is 0, the distributions are identical
INSERT INTO QA_Checks_Log values ( 1610, 'OK', @hVersion, curDate(), curTime() );
Insert into CDB_Checks
       ( TableName,
         CheckNumber,
         TestDescription,
         sourceTypeID,
         hourDayID)
Select   "avgSpeedDistribution" as tableName,
         1610 as checkNumber,
         'avgSpeedFraction is the same between at least two road types' as testDescription,
		 sourceTypeID,
         hourDayID
from (select sourceTypeID, hourDayID, avgSpeedBinID, avgSpeedFraction as rt2Fraction
	  from avgspeeddistribution
	  where roadTypeID = 2) as rt2
join (select sourceTypeID, hourDayID, avgSpeedBinID, avgSpeedFraction as rt3Fraction
	  from avgspeeddistribution
	  where roadTypeID = 3) as rt3 using (sourceTypeID, hourDayID, avgSpeedBinID)
join (select sourceTypeID, hourDayID, avgSpeedBinID, avgSpeedFraction as rt4Fraction
	  from avgspeeddistribution
	  where roadTypeID = 4) as rt4 using (sourceTypeID, hourDayID, avgSpeedBinID)
join (select sourceTypeID, hourDayID, avgSpeedBinID, avgSpeedFraction as rt5Fraction
	  from avgspeeddistribution
	  where roadTypeID = 5) as rt5 using (sourceTypeID, hourDayID, avgSpeedBinID)
group by sourceTypeID, hourDayID
HAVING sum(abs(rt2Fraction - rt3Fraction)) < 0.00001 or
	   sum(abs(rt2Fraction - rt4Fraction)) < 0.00001 or 
	   sum(abs(rt2Fraction - rt5Fraction)) < 0.00001 or 
	   sum(abs(rt3Fraction - rt4Fraction)) < 0.00001 or 
	   sum(abs(rt3Fraction - rt5Fraction)) < 0.00001 or 
	   sum(abs(rt4Fraction - rt5Fraction)) < 0.00001
LIMIT 1;

--       check no. 1611: check for 0% speed distributions in speed bin 1
INSERT INTO QA_Checks_Log values ( 1611, 'OK', @hVersion, curDate(), curTime() );
Insert into CDB_Checks
       ( TableName,
         CheckNumber,
         TestDescription,
         sourceTypeID,
         roadTypeID,
         hourDayID)
Select   "avgSpeedDistribution" as tableName,
         1611 as checkNumber,
         'avgSpeedFraction is 0 in avgSpeedBinID 1' as testDescription,
		 sourceTypeID,
         roadTypeID,
         hourDayID
from avgspeeddistribution
where avgSpeedFraction = 0 and avgSpeedBinID = 1
LIMIT 1;


-- countyYear
Insert into CDB_Checks (CheckNumber, TableName, TestDescription) values (1700, "countyyear", "Table Check:");

--       check no. 1701 -- check for unknown countyIDs
INSERT INTO QA_Checks_Log values ( 1701, 'OK', @hVersion, curDate(), curTime() );
Drop table if exists tempA;
Create table tempA
Select   countyId        as countyId2,
         'no '           as aMatch,
         count(*)        as n
From     countyYear
Group by countyId2;

Update tempA as a 
inner join county as c on a.countyId2 = c.countyId
set aMatch='yes';	

Insert into CDB_Checks
       ( TableName,
         CheckNumber,
         TestDescription,
         testValue,
         count  )
Select   "countyYear" as tableName,
         1701,
        "countyId" as testDescription,
         countyId2 as testValue,
         n         as count               --
From     tempA
Where    aMatch <> 'yes';

--       check no. 1702 -- check for unknown yearIDs
INSERT INTO QA_Checks_Log values ( 1702, 'OK', @hVersion, curDate(), curTime() );
Drop table if exists tempA;
Create table tempA
Select   yearId          as yearId2,
         'no '           as aMatch,
         count(*)        as n
From     countyYear
Group by yearId2;

Update tempA as a 
inner join year as c on a.yearId2 = c.yearId
set aMatch='yes';	

Insert into CDB_Checks
       ( TableName,
         CheckNumber,
         TestDescription,
         testValue,
         count  )
Select   "countyYear" as tableName,
         1702,
        "yearId" as testDescription,
         yearId2 as testValue,
         n       as count               --
From     tempA
Where    aMatch <> 'yes';

--       check no. 1703 -- check that the refuelingVaporProgramAdjust value is between 0 and 1
INSERT INTO QA_Checks_Log values ( 1703, 'OK', @hVersion, curDate(), curTime() );
Drop table if exists tempA;
Create table tempA
Select   refuelingVaporProgramAdjust as refuelingVaporProgramAdjust2,
         'no '                       as aMatch,
         count(*)                    as n
From     countyYear
Group by refuelingVaporProgramAdjust2;

Update tempA as a set aMatch='yes' where refuelingVaporProgramAdjust2 >= 0.0
                                     and refuelingVaporProgramAdjust2 <= 1.0;

Insert into CDB_Checks
       ( TableName,
         CheckNumber,
         TestDescription,
         testValue,
         count  )
Select   "countyYear"                 as tableName,
         1703,
        "refuelingVaporProgramAdjust" as testDescription,
         refuelingVaporProgramAdjust2 as testValue,
         n       as count               --
From     tempA
Where    aMatch <> 'yes';

--       check no. 1704 -- check that the refuelingSpillProgramAdjust value is between 0 and 1
INSERT INTO QA_Checks_Log values ( 1704, 'OK', @hVersion, curDate(), curTime() );
Drop table if exists tempA;
Create table tempA
Select   refuelingSpillProgramAdjust as refuelingSpillProgramAdjust2,
         'no '                       as aMatch,
         count(*)                    as n
From     countyYear
Group by refuelingSpillProgramAdjust2;

Update tempA as a set aMatch='yes' where refuelingSpillProgramAdjust2 >= 0.0
                                     and refuelingSpillProgramAdjust2 <= 1.0;

Insert into CDB_Checks
       ( TableName,
         CheckNumber,
         TestDescription,
         testValue,
         count  )
Select   "countyYear"                 as tableName,
         1704,
        "refuelingSpillProgramAdjust" as testDescription,
         refuelingSpillProgramAdjust2 as testValue,
         n       as count               --
From     tempA
Where    aMatch <> 'yes';


-- dayVMTFraction checks
Insert into CDB_Checks (CheckNumber, TableName, TestDescription) values (1800, "dayVmtFraction", "Table Check:");

--       check no. 1801: check for unknown dayIDs
INSERT INTO QA_Checks_Log values ( 1801, 'OK', @hVersion, curDate(), curTime() );
Drop table if exists tempA;
Create table tempA
Select   dayId        as dayId2,
         'no '        as aMatch,
         count(*)     as n
From     dayVmtFraction
Group by dayId2;

Update tempA as a 
inner join ##defaultdb##.dayOfAnyWeek as m on a.dayId2 = m.dayId
set aMatch='yes';

Insert into CDB_Checks
       ( TableName,
         CheckNumber,
         TestDescription,
         testValue,
         count  )
Select   "dayVmtFraction" as tableName,
         1801,
         "dayId"          as testDescription,
         dayId2           as testValue,
         n                as count
From     tempA
Where    aMatch <> 'yes';

--       check no. 1802: check that dayVMTFraction sums to 1 for each source type, road type, and month for the onroad road types
INSERT INTO QA_Checks_Log values ( 1802, 'OK', @hVersion, curDate(), curTime() );
Insert into CDB_Checks
       ( tableName,
         checkNumber,
         testDescription,
         testValue,
           sourceTypeId,
           monthId,
           roadTypeId )
Select  "dayVmtFraction"               as tableName,
         1802                          as checkNumber,
        "sum of dayVMTFraction <> 1.0" as testDescription,
         sum(dayVMTFraction)           as testValue,
           sourceTypeId,
           monthId,
           roadTypeId
From     dayVmtFraction
Where    roadTypeId in (2,3,4,5)
Group by sourceTypeId,
         monthId,
         roadTypeId
Having   testValue <0.99999 or testValue >1.00001;

--       check no. 1803: check for unknown monthIDs
INSERT INTO QA_Checks_Log values ( 1803, 'OK', @hVersion, curDate(), curTime() );
Drop table if exists tempA;
Create table tempA
Select   monthId      as monthId2,
         'no '        as aMatch,
         count(*)     as n
From     dayVmtFraction
Group by monthId2;
										  
Update tempA as a 
inner join ##defaultdb##.monthOfAnyYear as m on a.monthId2 = m.monthId
set aMatch='yes';

Insert into CDB_Checks
       ( TableName,
         CheckNumber,
         TestDescription,
         testValue,
         count  )
Select   "dayVmtFraction" as tableName,
         1803,
         "monthId"        as testDescription,
         monthId2         as testValue,
         n                as count
From     tempA
Where    aMatch <> 'yes';

--       check no. 1804: check for unknown roadTypeIDs
INSERT INTO QA_Checks_Log values ( 1804, 'OK', @hVersion, curDate(), curTime() );
Drop table if exists tempA;
Create table tempA
Select   roadTypeId   as roadTypeId2,
         'no '        as aMatch,
         count(*)     as n
From     dayVmtFraction
Group by roadTypeId2;

Update tempA as a 
inner join ##defaultdb##.roadType as m on a.roadTypeId2 = m.roadTypeId
set aMatch='yes';

Insert into CDB_Checks
       ( TableName,
         CheckNumber,
         TestDescription,
         testValue,
         count  )
Select   "dayVmtFraction" as tableName,
         1804,
         "roadTypeId"     as testDescription,
         roadTypeId2      as testValue,
         n                as count
From     tempA
Where    aMatch <> 'yes';

--       check no. 1805: check for unknown sourceTypeIDs
INSERT INTO QA_Checks_Log values ( 1805, 'OK', @hVersion, curDate(), curTime() );
Drop table if exists tempA;
Create table tempA
Select   sourceTypeId as sourceTypeId2,
         'no '        as aMatch,
         count(*)     as n
From     dayVmtFraction
Group by sourceTypeId2;

Update tempA as a 
inner join ##defaultdb##.sourceUseType as m on a.sourceTypeId2 = m.sourceTypeId
set aMatch='yes';	

Insert into CDB_Checks
       ( TableName,
         CheckNumber,
         TestDescription,
         testValue,
         count  )
Select   "dayVmtFraction" as tableName,
         1805,
         "sourceTypeId"   as testDescription,
         sourceTypeId2    as testValue,
         n                as count
From     tempA
Where    aMatch <> 'yes';

--       check no. 1806: make sure no fractions are 1
INSERT INTO QA_Checks_Log values ( 1806, 'OK', @hVersion, curDate(), curTime() );
Insert into CDB_Checks
       ( TableName,
         CheckNumber,
         TestDescription,
         testValue)
Select   "dayVMTFraction" as tableName,
         1806,
         'dayVMTFraction >= 1' as testDescription,
		 concat('ST: ', sourceTypeID, ', Month: ', monthID, ', RT: ', roadTypeID,
                ', Day: ', dayID, ', dayVMTFraction = ', dayVMTFraction) as testValue
FROM  dayvmtfraction
where dayVMTFraction >= 1.0
ORDER BY sourceTypeID, monthID, roadTypeID, dayID LIMIT 1;

--       check no. 1807: make sure no profiles are flat
INSERT INTO QA_Checks_Log values ( 1807, 'OK', @hVersion, curDate(), curTime() );
Insert into CDB_Checks
       ( TableName,
         CheckNumber,
         TestDescription,
         testValue,
          sourceTypeID, monthID, roadTypeID)
Select   "dayVMTFraction" as tableName,
         1807 as checkNumber,
         'dayVMTFraction is a flat profile' as testDescription,
         'dayID 2 is 2/7 and dayID 5 is 5/7' as testValue,
         sourceTypeID, monthID, roadTypeID
from dayvmtfraction
where abs(dayVMTFraction - dayID/7) < 0.00001
order by sourceTypeID, monthID, roadTypeID LIMIT 1;

--       check no. 1808: check for missing sourceTypeID, monthID, roadTypeID, dayID combinations
--                       only if dayVMTFraction is used
INSERT INTO QA_Checks_Log values ( 1808, 'OK', @hVersion, curDate(), curTime() );
Insert into CDB_Checks
       ( TableName,
         CheckNumber,
         TestDescription,
         sourceTypeID, monthID, roadTypeID, dayID)
Select   "dayvmtfraction" as tableName,
         1808,
         'Missing combination of valid sourceTypeID, monthID, roadTypeID, dayID' as testDescription,
		 sourceTypeID, monthID, roadTypeID, dayID
from (
	SELECT sourceTypeID, monthID, roadTypeID, dayID
	FROM  ##defaultdb##.sourceusetype
	CROSS JOIN ##defaultdb##.monthOfAnyYear
	CROSS JOIN ##defaultdb##.roadtype
	CROSS JOIN ##defaultdb##.dayOfAnyWeek
    where roadTypeID in (2, 3, 4, 5)
) as t1 
left join dayvmtfraction using (sourceTypeID, monthID, roadTypeID, dayID)
join (select count(*) as n from dayvmtfraction) as t2
where dayVMTFraction is NULL and n > 0
ORDER BY sourceTypeID, monthID, roadTypeID, dayID LIMIT 1;


-- emissionRateByAge
Insert into CDB_Checks (CheckNumber, TableName, TestDescription) values (1900, "emissionRateByAge", "Table Check:");

--       check no. 1901 -- check for unknown polProcessIDs
INSERT INTO QA_Checks_Log values ( 1901, 'OK', @hVersion, curDate(), curTime() );
Drop table if exists tempA;
Create table tempA
Select   polProcessId    as polProcessId2,
         'no '           as aMatch,
         count(*)        as n
From     emissionRateByAge
Group by polProcessId2;
										  
Update tempA as a 
inner join ##defaultdb##.emissionRateByAge as m on a.polProcessId2 = m.polProcessId
set aMatch='yes';	

Insert into CDB_Checks
       ( TableName,
         CheckNumber,
         TestDescription,
         testValue,
         count  )
Select   "emissionRateByAge" as tableName,
         1901,
        "polProcessId"       as testDescription,
         polProcessId2       as testValue,
         n                   as count               --
From     tempA
Where    aMatch <> 'yes';

--       check no. 1902 -- check for unknown opModeIDs
INSERT INTO QA_Checks_Log values ( 1902, 'OK', @hVersion, curDate(), curTime() );
Drop table if exists tempA;
Create table tempA
Select   opModeId        as opModeId2,
         'no '           as aMatch,
         count(*)        as n
From     emissionRateByAge
Group by opModeId2;

Update tempA as a 
inner join ##defaultdb##.operatingmode as m on a.opModeId2 = m.opModeId
set aMatch='yes';	

Insert into CDB_Checks
       ( TableName,
         CheckNumber,
         TestDescription,
         testValue,
         count  )
Select   "emissionRateByAge" as tableName,
         1902,
        "opModeId"       as testDescription,
         opModeId2       as testValue,
         n               as count               --
From     tempA
Where    aMatch <> 'yes';

--       check no. 1903 -- check for unknown ageGroupIDs
INSERT INTO QA_Checks_Log values ( 1903, 'OK', @hVersion, curDate(), curTime() );
Drop table if exists tempA;
Create table tempA
Select   ageGroupId      as ageGroupId2,
         'no '           as aMatch,
         count(*)        as n
From     emissionRateByAge
Group by ageGroupId2;

Update tempA as a 
inner join ##defaultdb##.ageGroup as m on a.ageGroupId2 = m.ageGroupId
set aMatch='yes';	

Insert into CDB_Checks
       ( TableName,
         CheckNumber,
         TestDescription,
         testValue,
         count  )
Select   "emissionRateByAge" as tableName,
         1903,
        "ageGroupId"         as testDescription,
         ageGroupId2         as testValue,
         n                   as count               --
From     tempA
Where    aMatch <> 'yes';

-- Checks for fuelformulation
Insert into CDB_Checks (CheckNumber, TableName, TestDescription) values (2000, "fuelFormulation", "Table Check:");

--       check no. 2001: check for unknown fuelSubTypeIDs
INSERT INTO QA_Checks_Log values ( 2001, 'OK', @hVersion, curDate(), curTime() );
Drop table if exists tempA;
Create table tempA
Select   fuelSubTypeId as fuelSubTypeId2,
         'no '         as aMatch,
         count(*)      as n
From     fuelFormulation
Group by fuelSubTypeId2;

Update tempA as a 
inner join ##defaultdb##.fuelSubType as m on a.fuelSubTypeId2 = m.fuelSubTypeId
set aMatch='yes';

Insert into CDB_Checks
       ( TableName,
         CheckNumber,
         TestDescription,
         testValue,
         count  )
Select   "fuelFormulation" as tableName,
         2001,
         "fuelSubTypeId"   as testDescription,
         fuelSubTypeId2    as testValue,
         n                 as count
From     tempA
Where    aMatch <> 'yes';

--       check no. 2002: checks for RVP between 5 and 20 for gasoline subtypes (not including E85)
INSERT INTO QA_Checks_Log values ( 2002, 'OK', @hVersion, curDate(), curTime() );
Drop table if exists tempA;
Create table tempA
Select   RVP           as RVP2,
         fuelSubTypeId as fuelSubType2,
         'no '         as aMatch,
         count(*)      as n
From     fuelFormulation
Group by RVP2;

Update tempA as a set aMatch='yes' where RVP2>=5.0 and RVP2<=20.0 and fuelSubType2     in (10,11,12,13,14,15);
Update tempA as a set aMatch='yes' where                              fuelSubType2 not in (10,11,12,13,14,15);

Insert into CDB_Checks
       ( TableName,
         CheckNumber,
         TestDescription,
         testValue,
         count  )
Select   "fuelFormulation" as tableName,
         2002,
         "RVP"             as testDescription,
         RVP2              as testValue,
         n                 as count
From     tempA
Where    aMatch <> 'yes';

--       check no. 2003: check for valid sulfur levels between 0 and 5000
INSERT INTO QA_Checks_Log values ( 2003, 'OK', @hVersion, curDate(), curTime() );
Drop table if exists tempA;
Create table tempA
Select   sulfurLevel   as sulfurLevel2,
         'no '         as aMatch,
         count(*)      as n
From     fuelFormulation
Group by sulfurLevel2;

Update tempA as a set aMatch='yes' where sulfurLevel2>=0.0 and sulfurLevel2<=5000.0;

Insert into CDB_Checks
       ( TableName,
         CheckNumber,
         TestDescription,
         testValue,
         count  )
Select   "fuelFormulation" as tableName,
         2003,
         "sulfurLevel"     as testDescription,
         sulfurLevel2      as testValue,
         n                 as count
From     tempA
Where    aMatch <> 'yes';

--       check no. 2004: check for an ETOHVolume between 0 and 100 for all fuels
INSERT INTO QA_Checks_Log values ( 2004, 'OK', @hVersion, curDate(), curTime() );
Drop table if exists tempA;
Create table tempA
Select   ETOHVolume    as ETOHVolume2,
         'no '         as aMatch,
         count(*)      as n,
         fuelsubtypeid as fuelsubtype2
From     fuelFormulation
Where    fuelsubtypeid in (10,11,12,13,14,15)
  and    fuelformulationid >=100
Group by ETOHVolume2;

Update tempA as a set aMatch='yes' where ETOHVolume2>=0.0 and ETOHVolume2<=100.0;

Insert into CDB_Checks
       ( TableName,
         CheckNumber,
         TestDescription,
         testValue,
         count  )
Select   "fuelFormulation" as tableName,
         2004,
         "ETOHVolume"      as testDescription,
         ETOHVolume2       as testValue,
         n                 as count
From     tempA
Where    aMatch <> 'yes';

--       check no. 2005: checks that MTBEVolume is 0 or NULL
INSERT INTO QA_Checks_Log values ( 2005, 'OK', @hVersion, curDate(), curTime() );
Drop table if exists tempA;
Create table tempA
Select   MTBEVolume    as MTBEVolume2,
         'no '         as aMatch,
         count(*)      as n,
         fuelsubtypeid as fuelsubtype2
From     fuelFormulation
Where    fuelsubtypeid in (10,11,12,13,14,15)
  and    fuelformulationid >=100
Group by MTBEVolume2;

Update tempA as a set aMatch='yes' where MTBEVolume2 is NULL OR MTBEVolume2=0;

Insert into CDB_Checks
       ( TableName,
         CheckNumber,
         TestDescription,
         testValue,
         count  )
Select   "fuelFormulation" as tableName,
         2005,
         "MTBEVolume"      as testDescription,
         MTBEVolume2       as testValue,
         n                 as count
From     tempA
Where    aMatch <> 'yes';

--       check no. 2006: Check that ETBEVolume is 0 or NULL
INSERT INTO QA_Checks_Log values ( 2006, 'OK', @hVersion, curDate(), curTime() );
Drop table if exists tempA;
Create table tempA
Select   fuelFormulationID,
         ETBEVolume    as ETBEVolume2,
         'no '         as aMatch,
         count(*)      as n,
         fuelsubtypeid as fuelsubtype2
From     fuelFormulation
Where    fuelsubtypeid in (10,11,12,13,14,15)
  and    fuelformulationid >=100
Group by ETBEVolume2;

Update tempA as a set aMatch='yes' where ETBEVolume2 is NULL or ETBEVolume2=0;

Insert into CDB_Checks
       ( TableName,
         fuelFormulationID,
         CheckNumber,
         TestDescription,
         testValue,
         count  )
Select   "fuelFormulation" as tableName,
         fuelFormulationID,
         2006,
         "ETBEVolume"      as testDescription,
         ETBEVolume2       as testValue,
         n                 as count
From     tempA
Where    aMatch <> 'yes';

--       check no. 2007: check that TAMEVolume is 0 or NULL
INSERT INTO QA_Checks_Log values ( 2007, 'OK', @hVersion, curDate(), curTime() );
Drop table if exists tempA;
Create table tempA
Select   TAMEVolume    as TAMEVolume2,
         'no '         as aMatch,
         count(*)      as n,
         fuelsubtypeid as fuelsubtype2
From     fuelFormulation
Where    fuelsubtypeid in (10,11,12,13,14,15)
  and    fuelformulationid >=100
Group by TAMEVolume2;

Update tempA as a set aMatch='yes' where TAMEVolume2 is NULL or TAMEVolume2=0;

Insert into CDB_Checks
       ( TableName,
         CheckNumber,
         TestDescription,
         testValue,
         count  )
Select   "fuelFormulation" as tableName,
         2007,
         "TAMEVolume"      as testDescription,
         TAMEVolume2       as testValue,
         n                 as count
From     tempA
Where    aMatch <> 'yes';

--       check no. 2008: check for aromaticContent between 0 and 55 for gasoline subtypes
INSERT INTO QA_Checks_Log values ( 2008, 'OK', @hVersion, curDate(), curTime() );
Drop table if exists tempA;
Create table tempA
Select   aromaticContent as aromaticContent2,
         'no '           as aMatch,
         count(*)        as n,
         fuelsubtypeid   as fuelsubtype2
From     fuelFormulation
Where    fuelsubtypeid in (10,11,12,13,14,15)
  and    fuelformulationid >=100
Group by aromaticContent2;

Update tempA as a set aMatch='yes' where aromaticContent2>=0.0 and aromaticContent2<=55.0;

Insert into CDB_Checks
       ( TableName,
         CheckNumber,
         TestDescription,
         testValue,
         count  )
Select   "fuelFormulation" as tableName,
         2008,
         "aromaticContent" as testDescription,
         aromaticContent2  as testValue,
         n                 as count
From     tempA
Where    aMatch <> 'yes';

--       check no. 2009: check for olefinContent between 0 and 25 for gasoline subtypes
INSERT INTO QA_Checks_Log values ( 2009, 'OK', @hVersion, curDate(), curTime() );
Drop table if exists tempA;
Create table tempA
Select   olefinContent as olefinContent2,
         'no '           as aMatch,
         count(*)        as n,
         fuelsubtypeid   as fuelsubtype2
From     fuelFormulation
Where    fuelsubtypeid in (10,11,12,13,14,15)
  and    fuelformulationid >=100
Group by olefinContent2;

Update tempA as a set aMatch='yes' where olefinContent2>=0.0 and olefinContent2<=25.0;

Insert into CDB_Checks
       ( TableName,
         CheckNumber,
         TestDescription,
         testValue,
         count  )
Select   "fuelFormulation" as tableName,
         2009,
         "olefinContent"   as testDescription,
         olefinContent2    as testValue,
         n                 as count
From     tempA
Where    aMatch <> 'yes';

--       check no. 2010: check for benzeneContent between 0 and 5 for gasoline subtypes
INSERT INTO QA_Checks_Log values ( 2010, 'OK', @hVersion, curDate(), curTime() );
Drop table if exists tempA;
Create table tempA
Select   benzeneContent  as benzeneContent2,
         'no '           as aMatch,
         count(*)        as n,
         fuelsubtypeid   as fuelsubtype2
From     fuelFormulation
Where    fuelsubtypeid in (10,11,12,13,14,15)
  and    fuelformulationid >=100
Group by benzeneContent2;

Update tempA as a set aMatch='yes' where benzeneContent2>=0.0 and benzeneContent2<=5.0;

Insert into CDB_Checks
       ( TableName,
         CheckNumber,
         TestDescription,
         testValue,
         count  )
Select   "fuelFormulation" as tableName,
         2010,
         "benzeneContent"  as testDescription,
         benzeneContent2   as testValue,
         n                 as count
From     tempA
Where    aMatch <> 'yes';

--       check no. 2011: check for e200 between 0 and 70 for gasoline subtypes
INSERT INTO QA_Checks_Log values ( 2011, 'OK', @hVersion, curDate(), curTime() );
Drop table if exists tempA;
Create table tempA
Select   e200  as e2002,
         'no '           as aMatch,
         count(*)        as n,
         fuelsubtypeid   as fuelsubtype2
From     fuelFormulation
Where    fuelsubtypeid in (10,11,12,13,14,15)
  and    fuelformulationid >=100
Group by e2002;

Update tempA as a set aMatch='yes' where e2002>=0.0 and e2002<=70.0;

Insert into CDB_Checks
       ( TableName,
         CheckNumber,
         TestDescription,
         testValue,
         count  )
Select   "fuelFormulation" as tableName,
         2011,
         "e200"            as testDescription,
         e2002             as testValue,
         n                 as count
From     tempA
Where    aMatch <> 'yes';

--       check no. 2012: check for e300 between 0 and 100 for gasoline subtypes
INSERT INTO QA_Checks_Log values ( 2012, 'OK', @hVersion, curDate(), curTime() );
Drop table if exists tempA;
Create table tempA
Select   e300            as e3002,
         'no '           as aMatch,
         count(*)        as n,
         fuelsubtypeid   as fuelsubtype2
From     fuelFormulation
Where    fuelsubtypeid in (10,11,12,13,14,15)
  and    fuelformulationid >=100
Group by e3002;

Update tempA as a set aMatch='yes' where e3002>=0.0 and e3002<=100.0;

Insert into CDB_Checks
       ( TableName,
         CheckNumber,
         TestDescription,
         testValue,
         count  )
Select   "fuelFormulation" as tableName,
         2012,
         "e300"            as testDescription,
         e3002             as testValue,
         n                 as count
From     tempA
Where    aMatch <> 'yes';

--       check no. 2013 -- check that the T50/T90 columns exist
INSERT INTO QA_Checks_Log values ( 2013, 'OK', @hVersion, curDate(), curTime() );
Insert into CDB_Checks
 ( TableName,
   checkNumber,
   TestDescription,
   testValue )
 values
 ("fuelFormulation",
   2013,
  "T50 and/or T90 Missing",
  (Select count(*)
   from   information_schema.columns
   where  table_name   = 'fuelformulation'
     and  column_name in ('t50', 't90')
     and  table_schema = database()) );
Delete from CDB_Checks where checkNumber=2013 and testValue=2;

-- fuelsupply checks
Insert into CDB_Checks (CheckNumber, TableName, TestDescription) values (2100, "fuelSupply", "Table Check:");

--       check no. 2101: check for unknown fuelFormulationIDs
INSERT INTO QA_Checks_Log values ( 2101, 'OK', @hVersion, curDate(), curTime() );
Drop table if exists tempA;
Create table tempA
Select   fuelFormulationID as fuelFormulationID2,
         'no '             as aMatch,
         count(*)          as n
From     fuelSupply
Group by fuelFormulationID2;

Update tempA as a 
inner join fuelformulation as c on a.fuelFormulationID2 = c.fuelFormulationID
set aMatch='yes';

Insert into CDB_Checks
       ( TableName,
         CheckNumber,
         TestDescription,
         testValue,
         count  )
Select   "fuelSupply" as tableName,
         2101,
        "fuelFormulationID" as testDescription,
         fuelFormulationID2  as testValue,
         n                   as count
From     tempA
Where    aMatch <> 'yes';

--       check no. 2102: check for unknown fuelYearIDs
INSERT INTO QA_Checks_Log values ( 2102, 'OK', @hVersion, curDate(), curTime() );
Drop table if exists tempA;
Create table tempA
Select   fuelYearId as fuelYearId2,
         'no '      as aMatch,
         count(*)   as n
From     fuelSupply
Group by fuelYearId2;

Update tempA as a 
inner join year as c on a.fuelYearId2 = c.fuelYearId
set aMatch='yes';	

Insert into CDB_Checks
       ( TableName,
         CheckNumber,
         TestDescription,
         testValue,
         count  )
Select   "fuelSupply" as tableName,
         2102,
        "fuelYearId"  as testDescription,
         fuelYearId2  as testValue,
         n            as count
From     tempA
Where    aMatch <> 'yes';

--       check no. 2103 -- check for unknown monthGroupIDs
INSERT INTO QA_Checks_Log values ( 2103, 'OK', @hVersion, curDate(), curTime() );
Drop table if exists tempA;
Create table tempA
Select   monthGroupId as monthGroupId2,
         'no '        as aMatch,
         count(*)     as n
From     fuelSupply
Group by monthGroupId2;

Update tempA as a 
inner join ##defaultdb##.monthGroupOfAnyYear as m on a.monthGroupId2 = m.monthGroupId
set aMatch='yes';

Insert into CDB_Checks
       ( TableName,
         CheckNumber,
         TestDescription,
         testValue,
         count  )
Select   "fuelSupply"   as tableName,
         2103,
        "monthGroupId"  as testDescription,
         monthGroupId2  as testValue,
         n              as count
From     tempA
Where    aMatch <> 'yes';

--       check no. 2104 -- check for multiple fuelRegionIDs
INSERT INTO QA_Checks_Log values ( 2104, 'OK', @hVersion, curDate(), curTime() );
Insert into CDB_Checks
       ( TableName,
         CheckNumber,
         TestDescription,
         `count`  )
Select   "fuelSupply"   as tableName,
         2104,
        "Multiple fuelRegionIDs"  as testDescription,
         count(distinct fuelRegionID)              as `count`
from fuelsupply
having `count` > 1;


-- fuelusagefraction
Insert into CDB_Checks (CheckNumber, TableName, TestDescription) values (2200, "fuelUsageFraction", "Table Check:");

--       check no. 2201 -- check for unknown fuelYearIDs
INSERT INTO QA_Checks_Log values ( 2201, 'OK', @hVersion, curDate(), curTime() );
Drop table if exists tempA ;
Create table tempA
select  fuelYearId,
       'no '    as aMatch,
       count(*) as n
from   fuelUsageFraction
group by fuelYearId;

Update tempA as a 
inner join year as c on a.fuelYearId = c.fuelYearId
set aMatch='yes';	


Insert into CDB_Checks
       ( TableName,
         CheckNumber,
         TestDescription,
         testValue,
         count  )
Select   "fuelUsageFraction" as tableName,
         2201                 as checkNumber,
        "fuelYearId"         as testDescription,
         fuelYearId          as testValue,
         count(*)            as cou
from     tempA
where    aMatch <> 'yes ';
Delete from CDB_Checks where checkNumber=2201 and count=0;

--       check no. 2202 -- checks for unknown countyIDs
INSERT INTO QA_Checks_Log values ( 2202, 'OK', @hVersion, curDate(), curTime() );
Drop table if exists tempA;
Create table tempA
Select   countyId,
         'no '           as aMatch,
         count(*)        as n
From     fuelUsageFraction
group by countyId;

Update tempA as a 
inner join county as c on a.countyId = c.countyId
set aMatch='yes';	

Insert into CDB_Checks
       ( TableName,
         CheckNumber,
         TestDescription,
         testValue,
         count )
Select   "fuelUsageFraction"  as tableName,
         2202                  as checkNumber,
        'countyId'            as testDescription,
         countyId             as testValue,
         count(*)             as count
from     tempA
where    aMatch <> 'yes';
Delete from CDB_Checks where checkNumber=2202 and count=0;

--       check no. 2203 -- check for unknown modelYearGroupIDs
INSERT INTO QA_Checks_Log values ( 2203, 'OK', @hVersion, curDate(), curTime() );
Drop table if exists tempA;
Create table tempA
Select   modelYearGroupId,
         'no '           as aMatch,
         count(*)        as n
From     fuelUsageFraction
group by modelYearGroupId;

Update tempA as a 
inner join ##defaultdb##.modelYearGroup as m on a.modelYearGroupId = m.modelYearGroupId
set aMatch='yes';	

Insert into CDB_Checks
       ( TableName,
         CheckNumber,
         TestDescription,
         testValue,
         count )
Select   "fuelUsageFraction"  as tableName,
         2203                  as checkNumber,
        'modelYearGroupId'    as testDescription,
         modelYearGroupId     as testValue,
         count(*)             as count
from     tempA
where    aMatch <> 'yes';
Delete from CDB_Checks where checkNumber=2203 and count=0;

--       check no. 2204 -- check for unknown SourceBinFuelTypeIDs
INSERT INTO QA_Checks_Log values ( 2204, 'OK', @hVersion, curDate(), curTime() );
Drop table if exists tempA;
Create table tempA
Select   sourceBinFuelTypeId,
         'no '           as aMatch,
         count(*)        as n
From     fuelUsageFraction
group by sourceBinFuelTypeId;

Update tempA as a 
inner join ##defaultdb##.fuelType as m on a.sourceBinFuelTypeId = m.fuelTypeId
set aMatch='yes';

Insert into CDB_Checks
       ( TableName,
         CheckNumber,
         TestDescription,
         testValue,
         count )
Select   "fuelUsageFraction"  as tableName,
         2204                  as checkNumber,
        'sourceBinFuelTypeId' as testDescription,
         sourceBinFuelTypeId  as testValue,
         count(*)             as count
from     tempA
where    aMatch <> 'yes';
Delete from CDB_Checks where checkNumber=2204 and count=0;

--       check no. 2205 -- checks for unknown fuelSupplyFuelTypeIDs
INSERT INTO QA_Checks_Log values ( 2205, 'OK', @hVersion, curDate(), curTime() );
Drop table if exists tempA;
Create table tempA
Select   fuelSupplyFuelTypeId,
         'no '           as aMatch,
         count(*)        as n
From     fuelUsageFraction
group by fuelSupplyFuelTypeId;

Update tempA as a 
inner join ##defaultdb##.fuelType as m on a.fuelSupplyFuelTypeId = m.fuelTypeId
set aMatch='yes';

Insert into CDB_Checks
       ( TableName,
         CheckNumber,
         TestDescription,
         testValue,
         count )
Select   "fuelUsageFraction"   as tableName,
         2205                   as checkNumber,
        'fuelSupplyFuelTypeId' as testDescription,
         fuelSupplyFuelTypeId  as testValue,
         count(*)              as count
from     tempA
where    aMatch <> 'yes';
Delete from CDB_Checks where checkNumber=2205 and count=0;

--       check no. 2206 -- check that fuelSupplyFuelTypeId must match sourceBinFuelTypeId for non-FFV
INSERT INTO QA_Checks_Log values ( 2206, 'OK', @hVersion, curDate(), curTime() );
Drop table if exists tempA;
Create table tempA
select   countyId,
         fuelYearId,
         sourceBinFuelTypeId,
         fuelSupplyFuelTypeId,
         count(*) as cou
From     fuelUsageFraction
where    sourceBinFuelTypeId<>5
  and    sourceBinFuelTypeId<>fuelSupplyFuelTypeId
group by countyId,
         fuelYearId,
         sourceBinFuelTypeId;

Insert into CDB_Checks
       ( TableName,
         CheckNumber,
         countyId,
         TestDescription,
         testValue,
         fuelYearId,
         count  )
Select  'fuelUsageFraction'  as tableName,
         2206                 as checkNumber,
         countyId            as countyId,
        'fuelTypes Mismatch for non-FFV' as testDescription,
         sourceBinFuelTypeId as testValue,
         fuelYearId          as fuelYearId,
         cou                 as count
from     tempA;
Delete from CDB_Checks where checkNumber=2206 and count=0;

--       check no. 2207 -- check that sourceBinFuelTypeId must = 1 or 5 when fuelSupplyFuelTypeId =5 
INSERT INTO QA_Checks_Log values (2207, 'OK', @hVersion, curDate(), curTime() );
Drop table if exists tempA;
Create table tempA
select *,
       count(*) as cou
from (select countyId,
             fuelYearId,
             sourceBinFuelTypeId,
             fuelSupplyFuelTypeId
      from   fuelUsageFraction
      where  sourceBinFuelTypeId=5
        and  fuelSupplyFuelTypeId<>1) as a
where        a.fuelSupplyFuelTypeId<>5
group by     countyId,
             fuelYearId,
             fuelSupplyFuelTypeId;

Insert into CDB_Checks
           ( TableName,
             CheckNumber,
             countyId,
             fuelYearId,
             TestDescription,
             testValue,
             count  )
Select      'fuelUsageFraction'               as tableName,
             2207                              as checkNumber,
             countyId                         as countyId,
             fuelYearId                       as fuelYearId,
            'FFV assigned a fuel other than FT 1 or 5' as testDescription,
             sourceBinFuelTypeId              as testValue,
             cou                              as count
from         tempA;
Delete from CDB_Checks where checkNumber=2207 and count=0;

--       check no. 2208 -- make sure there is a row for sourceBinFuelTypeID = 5 and each of fuelSupplyFuelTypeID = 1 and 5
Insert into CDB_Checks
           ( TableName,
             CheckNumber,
             countyId,
             fuelYearId,
             TestDescription,
             testValue,
             count  )
Select      'fuelUsageFraction'               as tableName,
             2208                              as checkNumber,
             countyId                         as countyId,
             fuelYearId                       as fuelYearId,
            'FFV are missing values for FT 1 or 5' as testDescription,
             CONCAT('modelYearGroupID: ', modelYearGroupID) as testValue,
             count(distinct fuelSupplyFuelTypeID) as count
from fuelusagefraction
where sourceBinFuelTypeID = 5 and fuelSupplyFuelTypeID in (1, 5)
group by countyID, fuelYearID, modelYearGroupID, sourceBinFuelTypeID
having `count` <> 2;

--       check no. 2209 -- checks that usageFraction sums to 1 for all sourceBinFuelTypeIds
INSERT INTO QA_Checks_Log values ( 2209, 'OK', @hVersion, curDate(), curTime() );
Drop table if exists tempA;
Create table tempA
select   countyId,
	   fuelYearId,
	   modelYearGroupID,
	   sourceBinFuelTypeId,
	   sum(usageFraction) as s
from     fuelUsageFraction
group by countyId,
	   fuelYearId,
	   modelYearGroupID,
	   sourceBinFuelTypeId;
       
Insert into CDB_Checks
           ( TableName,
             CheckNumber,
             TestDescription,
             testValue,
             countyId,
             fuelYearId)
Select      'fuelUsageFraction'   as tableName,
             2209                 as checkNumber,
            'distribution <> 1.0' as testDescription,
             CONCAT('MYG: ', modelYearGroupID, ', sourceBinFuelTypeID: ', sourceBinFuelTypeID, ' sums to ', s) as testValue,
             countyId             as countyId,
             fuelYearId           as fuelYearId
from         tempA
where    s < 0.99999
   or    s > 1.00001;
 

-- hotellingActivityDistribution
Insert into CDB_Checks (CheckNumber, TableName, TestDescription) values (2300, "hotellingActivityDistribution", "Table Check:");

--       check no. 2301 -- check for unknown opModeID
INSERT INTO QA_Checks_Log values ( 2301, 'OK', @hVersion, curDate(), curTime() );
Drop table if exists tempA;
Create table tempA
Select   opModeId,
         'no '           as aMatch,
         count(*)        as n
From     hotellingActivityDistribution
group by opModeId;

Update tempA as a 
inner join ##defaultdb##.operatingMode as m on a.opModeId = m.opModeId
set aMatch='yes';

Insert into CDB_Checks
       ( TableName,
         CheckNumber,
         TestDescription,
         testValue,
         count )
Select   "hotellingActivityDistribution" as tableName,
         2301                             as checkNumber,
        'opModeId'                       as testDescription,
         opModeId                        as testValue,
         count(*)                        as count
from     tempA
where    aMatch <> 'yes';
Delete from CDB_Checks where checkNumber=2301 and count=0;

--       check no. 2302 -- check that the opModeFraction sums to 1 by model year range
INSERT INTO QA_Checks_Log values ( 2302, 'OK', @hVersion, curDate(), curTime() );
Drop table if exists tempA;
Create table tempA
select   beginmodelyearId,endmodelyearid,
         sum(opModeFraction) as s,
         count(*)            as cou
From     hotellingActivityDistribution
group by beginmodelyearId,endmodelyearid;

Insert into CDB_Checks
       ( TableName,
         CheckNumber,
         TestDescription,
         testValue,
         count    )
Select   "hotellingActivityDistribution" as tableName,
         2302                             as checkNumber,
        'distribution<>1.0'              as testDescription,
         CONCAT(beginmodelyearId, '-', endmodelyearid, ' sums to ', s)               as testValue,
         cou                             as count
from     tempA
where    s<0.99999
   or    s>1.00001;

--       check no. 2303 -- check for unknown zoneID
INSERT INTO QA_Checks_Log values ( 2303, 'OK', @hVersion, curDate(), curTime() );
Drop table if exists tempA;
Create table tempA
Select   zoneID,
         'no '           as aMatch,
         count(*)        as n
From     hotellingActivityDistribution
group by zoneID;

Update tempA as a 
inner join zone as c on a.zoneID = c.zoneID
set aMatch='yes';								

Insert into CDB_Checks
       ( TableName,
         CheckNumber,
         TestDescription,
         testValue,
         count )
Select   "hotellingActivityDistribution" as tableName,
         2303                             as checkNumber,
        'zoneID'                       as testDescription,
         zoneID                        as testValue,
         count(*)                        as count
from     tempA
where    aMatch <> 'yes';
Delete from CDB_Checks where checkNumber=2303 and count=0;

--       check no. 2304 -- Check for gaps and overlaps in the model years columns in the hotellingActivityDistribution  table.
INSERT INTO QA_Checks_Log values ( 2304, 'OK', @hVersion, curDate(), curTime() );

-- Add a table to contain the results of the gaps/overlaps check.
Drop   table if exists     qa_checks_had;
Create Table if Not Exists qa_checks_had (
  Czone   int,          -- zoneID
  LENDMY int,          -- last    row's end model year
  CBEGMY int,          -- Current row's beg model year
  CENDMY int,          -- Current row's end model year
  Reason varchar(40) );

-- Call the procedure to check for gaps and overlaps.
call checkHotellingActivityDistribution();

Insert into CDB_Checks
       ( TableName,
         CheckNumber,
         TestDescription,
         count  )
Select  "hotellingActivityDistribution" as tableName,
         2304,
        "gaps and overlaps" as testDescription,
         (Select count(*) from qa_checks_had) as count
From     qa_checks_had
Where    (Select count(*) from qa_checks_had) > 0;


-- hotellingagefraction
Insert into CDB_Checks (CheckNumber, TableName, TestDescription) values (2400, "hotellingagefraction", "Table Check:");

--       check no. 2401 -- check for unknown zoneID
INSERT INTO QA_Checks_Log values ( 2401, 'OK', @hVersion, curDate(), curTime() );
Drop table if exists tempA;
Create table tempA
Select   zoneID,
         'no '           as aMatch,
         count(*)        as n
From     hotellingagefraction
group by zoneID;

Update tempA as a 
inner join zone as c on a.zoneID = c.zoneID
set aMatch='yes';	

Insert into CDB_Checks
       ( TableName,
         CheckNumber,
         TestDescription,
         testValue,
         count )
Select   "hotellingagefraction" as tableName,
         2401                   as checkNumber,
        'zoneID'                as testDescription,
         zoneID                 as testValue,
         count(*)               as count
from     tempA
where    aMatch <> 'yes';
Delete from CDB_Checks where checkNumber=2401 and count=0;

--       check no. 2402 -- check for unknown ageID
INSERT INTO QA_Checks_Log values ( 2402, 'OK', @hVersion, curDate(), curTime() );
Drop table if exists tempA;
Create table tempA
Select   ageId         as ageId2,
         'no '         as aMatch,
         count(*)      as n
From     hotellingagefraction
Group by ageId2;

Update tempA as a 
inner join ##defaultdb##.ageCategory as m on a.ageId2 = m.ageId
set aMatch='yes';

Insert into CDB_Checks
       ( TableName,
         CheckNumber,
         TestDescription,
         testValue,
         count  )
Select   "hotellingagefraction" as tableName,
         2402,
        "ageId"                      as testDescription,
         ageId2                      as testValue,
         n                           as count
From     tempA
Where    aMatch <> 'yes';

--       check no. 2403 -- check for missing zoneID and ageID combinations (as long as this table has contents)
INSERT INTO QA_Checks_Log values ( 2403, 'OK', @hVersion, curDate(), curTime() );
Insert into CDB_Checks
       ( TableName,
         CheckNumber,
         TestDescription,
         testValue)
Select   "hotellingagefraction" as tableName,
         2403,
         'Missing combination of valid zoneID and ageID' as testDescription,
		 concat('Z: ', zoneID, ', age: ', ageID) as testValue
from (
	SELECT zoneID, ageID
	FROM  zone
	CROSS JOIN ##defaultdb##.ageCategory
) as t1 
left join hotellingagefraction using (zoneID, ageID)
join (select count(*) as c from hotellingagefraction) as t2
where ageFraction is NULL and c > 0
ORDER BY zoneID, ageID LIMIT 1;

--       check no. 2404 -- check for ageFractions >= 1
INSERT INTO QA_Checks_Log values ( 2404, 'OK', @hVersion, curDate(), curTime() );
Insert into CDB_Checks
       ( TableName,
         CheckNumber,
         TestDescription,
         testValue)
Select   "hotellingagefraction" as tableName,
         2404,
         'ageFraction >= 1' as testDescription,
		 ageFraction as testValue
from hotellingagefraction
where ageFraction >= 1
ORDER BY zoneID, ageID LIMIT 1;

--       check no. 2405 -- check that the distribution sums to 1
INSERT INTO QA_Checks_Log values ( 2405, 'OK', @hVersion, curDate(), curTime() );
Insert into CDB_Checks
       ( TableName,
         CheckNumber,
         TestDescription,
         testValue,
         zoneID)
Select   "hotellingagefraction" as tableName,
         2405,
         'distribution <> 1' as testDescription,
		 sum(ageFraction) as testValue,
         zoneID
from hotellingagefraction
GROUP BY zoneID
having testValue < .99999 or testvalue > 1.00001;

--       check no. 2406: make sure age distributions aren't flat
INSERT INTO QA_Checks_Log values ( 2406, 'OK', @hVersion, curDate(), curTime() );
Insert into CDB_Checks
       ( TableName,
         CheckNumber,
         TestDescription,
         testValue,
         zoneID)
Select   "hotellingagefraction" as tableName,
         2406 as checkNumber,
         'ageFraction is a flat profile' as testDescription,
         concat('all are ', ageFraction) as testValue,
		 zoneID
from hotellingagefraction
group by zoneID, ageFraction
having count(*) = (select count(*) from ##defaultdb##.agecategory)
order by zoneID LIMIT 1;


-- hotellinghourfraction
Insert into CDB_Checks (CheckNumber, TableName, TestDescription) values (2500, "hotellinghourfraction", "Table Check:");

--       check no. 2501 -- check for unknown zoneID
INSERT INTO QA_Checks_Log values ( 2501, 'OK', @hVersion, curDate(), curTime() );
Drop table if exists tempA;
Create table tempA
Select   zoneID,
         'no '           as aMatch,
         count(*)        as n
From     hotellinghourfraction
group by zoneID;

Update tempA as a 
inner join zone as c on a.zoneID = c.zoneID
set aMatch='yes';

Insert into CDB_Checks
       ( TableName,
         CheckNumber,
         TestDescription,
         testValue,
         count )
Select   "hotellinghourfraction" as tableName,
         2501                   as checkNumber,
        'zoneID'                as testDescription,
         zoneID                 as testValue,
         count(*)               as count
from     tempA
where    aMatch <> 'yes';
Delete from CDB_Checks where checkNumber=2501 and count=0;

--       check no. 2502 -- check for unknown dayID
INSERT INTO QA_Checks_Log values ( 2502, 'OK', @hVersion, curDate(), curTime() );
Drop table if exists tempA;
Create table tempA
Select   dayID         as dayId2,
         'no '         as aMatch,
         count(*)      as n
From     hotellinghourfraction
Group by dayId2;

Update tempA as a 
inner join ##defaultdb##.dayOfAnyWeek as m on a.dayId2 = m.dayId
set aMatch='yes';

Insert into CDB_Checks
       ( TableName,
         CheckNumber,
         TestDescription,
         testValue,
         count  )
Select   "hotellinghourfraction" as tableName,
         2502,
        "dayId"                      as testDescription,
         dayId2                      as testValue,
         n                           as count
From     tempA
Where    aMatch <> 'yes';

--       check no. 2503 -- check for unknown hourID
INSERT INTO QA_Checks_Log values ( 2503, 'OK', @hVersion, curDate(), curTime() );
Drop table if exists tempA;
Create table tempA
Select   hourID         as hourId2,
         'no '         as aMatch,
         count(*)      as n
From     hotellinghourfraction
Group by hourID2;

Update tempA as a 
inner join ##defaultdb##.hourOfAnyDay as m on a.hourId2 = m.hourId
set aMatch='yes';

Insert into CDB_Checks
       ( TableName,
         CheckNumber,
         TestDescription,
         testValue,
         count  )
Select   "hotellinghourfraction" as tableName,
         2503,
        "hourId"                      as testDescription,
         hourId2                      as testValue,
         n                           as count
From     tempA
Where    aMatch <> 'yes';

--       check no. 2504 -- check for missing zoneID, dayID, and hourID combinations (as long as this table has contents)
INSERT INTO QA_Checks_Log values ( 2504, 'OK', @hVersion, curDate(), curTime() );
Insert into CDB_Checks
       ( TableName,
         CheckNumber,
         TestDescription,
         testValue)
Select   "hotellinghourfraction" as tableName,
         2504,
         'Missing combination of valid zoneID, dayID, hourID' as testDescription,
		 concat('Z: ', zoneID, ', dayID: ', dayID, ', hourID: ', hourID) as testValue
from (
	SELECT zoneID, dayID, hourID
	FROM  zone
	CROSS JOIN ##defaultdb##.dayOfAnyWeek
	CROSS JOIN ##defaultdb##.hourOfAnyDay
) as t1 
left join hotellinghourfraction using (zoneID, dayID, hourID)
join (select count(*) as c from hotellinghourfraction) as t2
where hourFraction is NULL and c > 0
ORDER BY zoneID, dayID, hourID LIMIT 1;

--       check no. 2505 -- check for hourFractions >= 1
INSERT INTO QA_Checks_Log values ( 2505, 'OK', @hVersion, curDate(), curTime() );
Insert into CDB_Checks
       ( TableName,
         CheckNumber,
         TestDescription,
         testValue,
         zoneID, dayID, hourID)
Select   "hotellinghourfraction" as tableName,
         2505,
         'hourFraction >= 1' as testDescription,
		 hourFraction as testValue,
         zoneID, dayID, hourID
from hotellinghourfraction
where hourFraction >= 1
ORDER BY zoneID, dayID, hourID LIMIT 1;

--       check no. 2506 -- check that the distribution sums to 1
INSERT INTO QA_Checks_Log values ( 2506, 'OK', @hVersion, curDate(), curTime() );
Insert into CDB_Checks
       ( TableName,
         CheckNumber,
         TestDescription,
         testValue,
         zoneID, dayID)
Select   "hotellinghourfraction" as tableName,
         2506,
         'distribution <> 1' as testDescription,
		 sum(hourFraction) as testValue,
         zoneID, dayID
from hotellinghourfraction
GROUP BY zoneID, dayID
having testValue < .99999 or testvalue > 1.00001;

--       check no. 2507: make sure hour distributions aren't flat
INSERT INTO QA_Checks_Log values ( 2507, 'OK', @hVersion, curDate(), curTime() );
Insert into CDB_Checks
       ( TableName,
         CheckNumber,
         TestDescription,
         testValue,
         zoneID,
         dayID)
Select   "hotellinghourfraction" as tableName,
         2507 as checkNumber,
         'hourFraction is a flat profile' as testDescription,
         concat('all are ', hourFraction) as testValue,
		 zoneID, dayID
from hotellinghourfraction
group by zoneID, dayID, hourFraction
having count(*) = (select count(*) from ##defaultdb##.hourOfAnyDay)
order by zoneID, dayID LIMIT 1;

--       check no. 2508: make sure weekend and weekday profiles are different
INSERT INTO QA_Checks_Log values ( 2508, 'OK', @hVersion, curDate(), curTime() );
Insert into CDB_Checks
       ( TableName,
         CheckNumber,
         TestDescription,
         zoneID)
Select   "hotellinghourfraction" as tableName,
         2508 as checkNumber,
         'hourFraction is the same between weekend and weekday' as testDescription,
		 zoneID
from (select zoneID, hourID, hourFraction as weekendFraction
	  from hotellinghourfraction
	  where dayID = 2) as we
join (select zoneID, hourID, hourFraction as weekdayFraction
	  from hotellinghourfraction
	  where dayID = 5) as wd using (zoneID, hourID)
group by zoneID
having sum(abs(weekendFraction - weekdayFraction)) < 0.00001
order by zoneID LIMIT 1;


-- hotellinghoursperday
Insert into CDB_Checks (CheckNumber, TableName, TestDescription) values (2600, "hotellinghoursperday", "Table Check:");

--       check no. 2601 -- check for unknown yearID
INSERT INTO QA_Checks_Log values ( 2601, 'OK', @hVersion, curDate(), curTime() );
Drop table if exists tempA;
Create table tempA
Select   yearID         as yearId2,
         'no '         as aMatch,
         count(*)      as n
From     hotellinghoursperday
Group by yearID2;

Update tempA as a 
inner join year as c on a.yearId2 = c.yearId
set aMatch='yes';	

Insert into CDB_Checks
       ( TableName,
         CheckNumber,
         TestDescription,
         testValue,
         count  )
Select   "hotellinghoursperday" as tableName,
         2601,
        "yearID"                      as testDescription,
         yearID2                      as testValue,
         n                           as count
From     tempA
Where    aMatch <> 'yes';

--       check no. 2602 -- check for unknown zoneID
INSERT INTO QA_Checks_Log values ( 2602, 'OK', @hVersion, curDate(), curTime() );
Drop table if exists tempA;
Create table tempA
Select   zoneID,
         'no '           as aMatch,
         count(*)        as n
From     hotellinghoursperday
group by zoneID;

Update tempA as a 
inner join zone as c on a.zoneID = c.zoneID
set aMatch='yes';	

Insert into CDB_Checks
       ( TableName,
         CheckNumber,
         TestDescription,
         testValue,
         count )
Select   "hotellinghoursperday" as tableName,
         2602                   as checkNumber,
        'zoneID'                as testDescription,
         zoneID                 as testValue,
         count(*)               as count
from     tempA
where    aMatch <> 'yes';
Delete from CDB_Checks where checkNumber=2602 and count=0;

--       check no. 2603 -- check for unknown dayID
INSERT INTO QA_Checks_Log values ( 2603, 'OK', @hVersion, curDate(), curTime() );
Drop table if exists tempA;
Create table tempA
Select   dayID         as dayId2,
         'no '         as aMatch,
         count(*)      as n
From     hotellinghoursperday
Group by dayId2;

Update tempA as a 
inner join ##defaultdb##.dayOfAnyWeek as m on a.dayId2 = m.dayId
set aMatch='yes';

Insert into CDB_Checks
       ( TableName,
         CheckNumber,
         TestDescription,
         testValue,
         count  )
Select   "hotellinghoursperday" as tableName,
         2603,
        "dayId"                      as testDescription,
         dayId2                      as testValue,
         n                           as count
From     tempA
Where    aMatch <> 'yes';

--       check no. 2604 -- check for missing yearID, zoneID, and dayID combinations (as long as this table has contents)
INSERT INTO QA_Checks_Log values ( 2604, 'OK', @hVersion, curDate(), curTime() );
Insert into CDB_Checks
       ( TableName,
         CheckNumber,
         TestDescription,
         testValue)
Select   "hotellinghoursperday" as tableName,
         2604,
         'Missing combination of valid yearID, zoneID, dayID' as testDescription,
		 concat('Year: ', yearID, ', Z: ', zoneID, ', dayID: ', dayID) as testValue
from (
	SELECT yearID, zoneID, dayID
	FROM  year
	CROSS JOIN zone
	CROSS JOIN ##defaultdb##.dayOfAnyWeek
) as t1 
left join hotellinghoursperday using (yearID, zoneID, dayID)
join (select count(*) as c from hotellinghoursperday) as t2
where hotellingHoursPerDay is NULL and c > 0
ORDER BY yearID, zoneID, dayID LIMIT 1;


-- hotellingmonthadjust
Insert into CDB_Checks (CheckNumber, TableName, TestDescription) values (2700, "hotellingmonthadjust", "Table Check:");

--       check no. 2701 -- check for unknown zoneID
INSERT INTO QA_Checks_Log values ( 2701, 'OK', @hVersion, curDate(), curTime() );
Drop table if exists tempA;
Create table tempA
Select   zoneID,
         'no '           as aMatch,
         count(*)        as n
From     hotellingmonthadjust
group by zoneID;

Update tempA as a 
inner join zone as c on a.zoneID = c.zoneID
set aMatch='yes';	

Insert into CDB_Checks
       ( TableName,
         CheckNumber,
         TestDescription,
         testValue,
         count )
Select   "hotellingmonthadjust" as tableName,
         2701                   as checkNumber,
        'zoneID'                as testDescription,
         zoneID                 as testValue,
         count(*)               as count
from     tempA
where    aMatch <> 'yes';
Delete from CDB_Checks where checkNumber=2701 and count=0;

--       check no. 2702 -- check for unknown monthID
INSERT INTO QA_Checks_Log values ( 2702, 'OK', @hVersion, curDate(), curTime() );
Drop table if exists tempA;
Create table tempA
Select   monthID,
         'no '           as aMatch,
         count(*)        as n
From     hotellingmonthadjust
group by monthID;

Update tempA as a 
inner join ##defaultdb##.monthOfAnyYear as m on a.monthId = m.monthId
set aMatch='yes';

Insert into CDB_Checks
       ( TableName,
         CheckNumber,
         TestDescription,
         testValue,
         count )
Select   "hotellingmonthadjust" as tableName,
         2702                   as checkNumber,
        'monthID'                as testDescription,
         monthID                 as testValue,
         count(*)               as count
from     tempA
where    aMatch <> 'yes';
Delete from CDB_Checks where checkNumber=2702 and count=0;

--       check no. 2703 -- check for missing zoneID and monthID combinations (as long as this table has contents)
INSERT INTO QA_Checks_Log values ( 2703, 'OK', @hVersion, curDate(), curTime() );
Insert into CDB_Checks
       ( TableName,
         CheckNumber,
         TestDescription,
         testValue)
Select   "hotellingmonthadjust" as tableName,
         2703,
         'Missing combination of valid zoneID and monthID' as testDescription,
		 concat('Z: ', zoneID, ', monthID: ', monthID) as testValue
from (
	SELECT zoneID, monthID
	FROM  zone
	CROSS JOIN ##defaultdb##.monthOfAnyYear
) as t1 
left join hotellingmonthadjust using (zoneID, monthID)
join (select count(*) as c from hotellingmonthadjust) as t2
where monthAdjustment is NULL and c > 0
ORDER BY zoneID, monthID LIMIT 1;


-- hourVMTFraction checks
Insert into CDB_Checks (CheckNumber, TableName, TestDescription) values (2800, "hourVmtFraction", "Table Check:");

--       check no. 2801 -- checks unknown dayIDs
INSERT INTO QA_Checks_Log values ( 2801, 'OK', @hVersion, curDate(), curTime() );
Drop table if exists tempA;
Create table tempA
Select   dayId    as dayId2,
         'no '    as aMatch,
         count(*) as n
From     hourVmtFraction
Group by dayId2;

Update tempA as a 
inner join ##defaultdb##.dayOfAnyWeek as m on a.dayId2 = m.dayId
set aMatch='yes';

Insert into CDB_Checks
       ( TableName,
         CheckNumber,
         TestDescription,
         testValue,
         count  )
Select   "hourVmtFraction" as tableName,
         2801,
         "dayId"           as testDescription,
         dayId2            as testValue,
         n                 as count               --
From     tempA
Where    aMatch <> 'yes';

--       check no. 2802 -- check for unknown hourIDs
INSERT INTO QA_Checks_Log values ( 2802, 'OK', @hVersion, curDate(), curTime() );
Drop table if exists tempA;
Create table tempA
Select   hourId    as hourId2,
         'no '     as aMatch,
         count(*)  as n
From     hourVmtFraction
Group by hourId2;

Update tempA as a 
inner join ##defaultdb##.hourOfAnyDay as m on a.hourId2 = m.hourId
set aMatch='yes';

Insert into CDB_Checks
       ( TableName,
         CheckNumber,
         TestDescription,
         testValue,
         count  )
Select  "hourVmtFraction" as tableName,
         2802,
        "hourId"          as testDescription,
         hourId2          as testValue,
         n                as count               --
From     tempA
Where    aMatch <> 'yes';

--       check no. 2803 -- check for unknown roadTypeIDs
INSERT INTO QA_Checks_Log values (2803, 'OK', @hVersion, curDate(), curTime() );
Drop table if exists tempA;
Create table tempA
Select   roadTypeId as roadTypeId2,
         'no '      as aMatch,
         count(*)   as n
From     hourVmtFraction
Group by roadTypeId2;

Update tempA as a 
inner join ##defaultdb##.roadType as m on a.roadTypeId2 = m.roadTypeId
set aMatch='yes';

Insert into CDB_Checks
       ( TableName,
         CheckNumber,
         TestDescription,
         testValue,
         count  )
Select  "hourVmtFraction" as tableName,
         2803,
        "roadTypeId"      as testDescription,
         roadTypeId2      as testValue,
         n                as count               --
From     tempA
Where    aMatch <> 'yes';

--       check no. 2804 -- check for unknown sourceTypeIDs
INSERT INTO QA_Checks_Log values ( 2804, 'OK', @hVersion, curDate(), curTime() );
Drop table if exists tempA;
Create table tempA
Select   sourceTypeId as sourceTypeId2,
         'no '        as aMatch,
         count(*)     as n
From     hourVmtFraction
Group by sourceTypeId2;

Update tempA as a 
inner join ##defaultdb##.sourceUseType as m on a.sourceTypeId2 = m.sourceTypeId
set aMatch='yes';	

Insert into CDB_Checks
       ( TableName,
         CheckNumber,
         TestDescription,
         testValue,
         count  )
Select  "hourVmtFraction" as tableName,
         2804,
        "sourceTypeId"    as testDescription,
         sourceTypeId2    as testValue,
         n                as count               --
From     tempA
Where    aMatch <> 'yes';

--       check no. 2805 -- check that hourVMTFraction sums to 1 over sourceTypeID, roadTypeID, and dayID
INSERT INTO QA_Checks_Log values ( 2805, 'OK', @hVersion, curDate(), curTime() );
Drop table if exists tempA;

Insert into CDB_Checks
       (  tableName,
          checkNumber,
          testDescription,
          testValue,
            sourceTypeId,
            roadTypeId,
            dayId  )
Select   "hourVmtFraction"               as tableName,
          2805                             as checkNumber,
         "sum of hourVmtFraction <> 1.0" as tesDescription,
          sum(hourVmtFraction)           as testValue,
            sourceTypeId,
            roadTypeId,
            dayId
From      hourVmtFraction
Where     roadTypeId in (2,3,4,5)
Group by  sourceTypeId,
          roadTypeId,
          dayId
Having    testValue <0.99999 or testValue >1.00001;

--       check no. 2806: check for missing sourceTypeID, roadTypeID, dayID, hourID combinations
Insert into CDB_Checks
       ( TableName,
         CheckNumber,
         TestDescription,
         sourceTypeID, roadTypeID, dayID, hourID)
Select   "hourVMTFraction" as tableName,
         2806,
         'Missing combination of valid sourceTypeID, roadTypeID, dayID, hourID' as testDescription,
         sourceTypeID, roadTypeID, dayID, hourID
from (
	SELECT sourceTypeID, roadTypeID, dayID, hourID
	FROM  ##defaultdb##.sourceusetype
	CROSS JOIN ##defaultdb##.roadtype
	CROSS JOIN ##defaultdb##.dayofanyweek
	CROSS JOIN ##defaultdb##.hourofanyday
) as t1 left join hourvmtfraction using (sourceTypeID, roadTypeID, dayID, hourID)
where hourVMTFraction is NULL 
ORDER BY sourceTypeID, roadTypeID, dayID, hourID LIMIT 1;

--       check no. 2807 -- check for hourVMTFractions >= 1
INSERT INTO QA_Checks_Log values ( 2807, 'OK', @hVersion, curDate(), curTime() );
Insert into CDB_Checks
       ( TableName,
         CheckNumber,
         TestDescription,
         testValue,
         sourceTypeID, roadTypeID, dayID, hourID)
Select   "hourVMTFraction" as tableName,
         2807,
         'hourVMTFraction >= 1' as testDescription,
		 hourVMTFraction as testValue,
         sourceTypeID, roadTypeID, dayID, hourID
from hourvmtfraction
where hourVMTFraction >= 1
ORDER BY sourceTypeID, roadTypeID, dayID, hourID LIMIT 1;

--       check no. 2808: make sure hour VMT distributions aren't flat
INSERT INTO QA_Checks_Log values ( 2808, 'OK', @hVersion, curDate(), curTime() );
Insert into CDB_Checks
       ( TableName,
         CheckNumber,
         TestDescription,
         testValue,
         sourceTypeID, roadTypeID, dayID)
Select   "hourVMTFraction" as tableName,
         2808 as checkNumber,
         'hourVMTFraction is a flat profile' as testDescription,
         concat('all are ', hourVMTFraction) as testValue,
		 sourceTypeID, roadTypeID, dayID
from hourvmtfraction
group by sourceTypeID, roadTypeID, dayID, hourVMTFraction
having count(*) = (select count(*) from ##defaultdb##.hourOfAnyDay)
order by sourceTypeID, roadTypeID, dayID LIMIT 1;

--       check no. 2809: make sure weekend and weekday profiles are different
INSERT INTO QA_Checks_Log values ( 2809, 'OK', @hVersion, curDate(), curTime() );
Insert into CDB_Checks
       ( TableName,
         CheckNumber,
         TestDescription,
         sourceTypeID,
         roadTypeID)
Select   "hourVMTFraction" as tableName,
         2809 as checkNumber,
         'hourVMTFraction is the same between weekend and weekday' as testDescription,
		 sourceTypeID,
         roadTypeID
from (select sourceTypeID, roadTypeID, hourID, hourVMTFraction as weekendFraction
	  from hourVMTFraction
	  where dayID = 2 and roadTypeID <> 1) as we
join (select sourceTypeID, roadTypeID, hourID, hourVMTFraction as weekdayFraction
	  from hourVMTFraction
	  where dayID = 5 and roadTypeID <> 1) as wd using (sourceTypeID, roadTypeID, hourID)
group by sourceTypeID, roadTypeID
having sum(abs(weekendFraction - weekdayFraction)) < 0.00001
order by sourceTypeID, roadTypeID LIMIT 1;


-- HPMSVTypeDay
Insert into CDB_Checks (CheckNumber, TableName, TestDescription) values (2900, "HPMSVTypeDay", "Table Check:");

--       check no. 2901 -- check for unknown yearIDs
INSERT INTO QA_Checks_Log values ( 2901, 'OK', @hVersion, curDate(), curTime() );
Drop table if exists tempA;
Create table tempA
Select   yearId       as yearId2,
         'no '        as aMatch,
         count(*)     as n
From     hpmsVTypeDay
Group by yearId2;

Update tempA as a 
inner join year as c on a.yearId2 = c.yearId
set aMatch='yes';	

Insert into CDB_Checks
       ( TableName,
         CheckNumber,
         TestDescription,
         testValue,
         count,
         yearId  )
Select   "hpmsVTypeDay"   as tableName,
         2901,
        "yearId"          as testDescription,
         yearId2          as testValue,
         n                as count,
         yearId2
From     tempA
Where    aMatch <> 'yes';

--       check no. 2902 -- check for unknown monthIDs
INSERT INTO QA_Checks_Log values ( 2902, 'OK', @hVersion, curDate(), curTime() );
Drop table if exists tempA;
Create table tempA
Select   monthId      as monthId2,
         'no '        as aMatch,
         count(*)     as n
From     hpmsVTypeDay
Group by monthId2;

Update tempA as a 
inner join ##defaultdb##.monthOfAnyYear as m on a.monthId2 = m.monthId
set aMatch='yes';

Insert into CDB_Checks
       ( TableName,
         CheckNumber,
         TestDescription,
         testValue,
         count,
         monthId  )
Select   "hpmsVTypeDay" as tableName,
         2902,
         "monthId"      as testDescription,
         monthId2       as testValue,
         n              as count,
         monthId2
From     tempA
Where    aMatch <> 'yes';

--       check no. 2903 -- check for unknown dayIDs
INSERT INTO QA_Checks_Log values ( 2903, 'OK', @hVersion, curDate(), curTime() );
Drop table if exists tempA;
Create table tempA
Select   dayId        as dayId2,
         'no '        as aMatch,
         count(*)     as n
From     hpmsVTypeDay
Group by dayId2;

Update tempA as a 
inner join ##defaultdb##.dayOfAnyWeek as m on a.dayId2 = m.dayId
set aMatch='yes';

Insert into CDB_Checks
       ( TableName,
         CheckNumber,
         TestDescription,
         testValue,
         count,
         dayId   )
Select   "hpmsVTypeDay" as tableName,
         2903,
         "dayId"        as testDescription,
         dayId2         as testValue,
         n              as count,
         dayId2
From     tempA
Where    aMatch <> 'yes';

--       check no. 2904 -- check for unknown hpmsVTypeIDs
INSERT INTO QA_Checks_Log values ( 2904, 'OK', @hVersion, curDate(), curTime() );
Drop table if exists tempA;
Create table tempA
Select   HPMSVtypeID  as HPMSVtypeID2,
         'no '        as aMatch,
         count(*)     as n
From     hpmsVTypeDay
Group by hpmsVTypeID2;

Update tempA as a 
inner join ##defaultdb##.hpmsvtype as m on a.HPMSVtypeID2 = m.HPMSVtypeID
set aMatch='yes';

Insert into CDB_Checks
       ( TableName,
         CheckNumber,
         TestDescription,
         testValue,
         count)
Select   "hpmsVTypeDay" as tableName,
         2904,
         "HPMSVtypeID"        as testDescription,
         HPMSVtypeID2         as testValue,
         n              as count
From     tempA
Where    aMatch <> 'yes';

--       check no. 2905: check for missing yearID, monthID, dayID, HPMSVtypeID combinations
--                       only when hpmsvtypeday is used
INSERT INTO QA_Checks_Log values ( 2905, 'OK', @hVersion, curDate(), curTime() );
Insert into CDB_Checks
       ( TableName,
         CheckNumber,
         TestDescription,
         yearID, monthID, dayID, HPMSVtypeID)
Select   "hpmsvtypeday" as tableName,
         2905,
         'Missing combination of valid yearID, monthID, dayID, HPMSVtypeID' as testDescription,
		 yearID, monthID, dayID, HPMSVtypeID
from (
	SELECT yearID, monthID, dayID, HPMSVtypeID
	FROM  `year`
	CROSS JOIN ##defaultdb##.monthOfAnyYear
	CROSS JOIN ##defaultdb##.dayOfAnyWeek
	CROSS JOIN ##defaultdb##.hpmsvtype
) as t1
left join hpmsvtypeday using (yearID, monthID, dayID, HPMSVtypeID)
join (select count(*) as n from hpmsvtypeday) as t2
where VMT is NULL and n > 0
ORDER BY yearID, monthID, dayID, HPMSVtypeID LIMIT 1;


-- checks for HPMSVTypeYear
Insert into CDB_Checks (CheckNumber, TableName, TestDescription) values (3000, "hpmsVTypeYear", "Table Check:");

--       check no. 3001 -- check for unknown HPMSVTypeIDs
INSERT INTO QA_Checks_Log values ( 3001, 'OK', @hVersion, curDate(), curTime() );
Drop table if exists tempA;
Create table tempA
Select   HPMSVtypeID  as HPMSVtypeID2,
         'no '        as aMatch,
         count(*)     as n
From     hpmsVTypeYear
Group by HPMSVtypeID2;

Update tempA as a 
inner join ##defaultdb##.hpmsvtype as m on a.HPMSVtypeID2 = m.HPMSVtypeID
set aMatch='yes';

Insert into CDB_Checks
       ( TableName,
         CheckNumber,
         TestDescription,
         testValue,
         count  )
Select  "hpmsVTypeYear"   as tableName,
         3001,
        "hpmSvTypeId"      as testDescription,
         HPMSVtypeId2      as testValue,
         n                 as count               --
From     tempA
Where    aMatch <> 'yes';

--       check no. 3002 -- check for unknown yearIDs
INSERT INTO QA_Checks_Log values ( 3002, 'OK', @hVersion, curDate(), curTime() );
Drop table if exists tempA;
Create table tempA
Select   yearId          as yearId2,
         'no '           as aMatch,
         count(*)        as n
From     hpmsVTypeYear
Group by yearId2;

Update tempA as a 
inner join year as c on a.yearId2 = c.yearId
set aMatch='yes';

Insert into CDB_Checks
       ( TableName,
         CheckNumber,
         TestDescription,
         testValue,
         count,
         yearId  )
Select  "hpmsVTypeYear"   as tableName,
         3002,
        "yearId"      as testDescription,
         yearId2      as testValue,
         n            as count,
         yearId2
From     tempA
Where    aMatch <> 'yes';

--       check no. 3003: check for missing yearID, HPMSVtypeID combinations
INSERT INTO QA_Checks_Log values ( 3003, 'OK', @hVersion, curDate(), curTime() );
Insert into CDB_Checks
       ( TableName,
         CheckNumber,
         TestDescription,
         yearID, HPMSVtypeID)
Select   "hpmsvtypeyear" as tableName,
         3003,
         'Missing combination of valid yearID, HPMSVtypeID' as testDescription,
		 yearID, HPMSVtypeID
from (
	SELECT yearID, HPMSVtypeID
	FROM  `year`
	CROSS JOIN ##defaultdb##.hpmsvtype
) as t1
left join hpmsvtypeyear using (yearID, HPMSVtypeID)
join (select count(*) as n from hpmsvtypeyear) as t2
where HPMSBaseYearVMT is NULL and n > 0
ORDER BY yearID, HPMSVtypeID LIMIT 1;


-- idleDayAdjust
Insert into CDB_Checks (CheckNumber, TableName, TestDescription) values (3100, "idleDayAdjust", "Table Check:");

--       check no. 3101 -- check for unknown sourceTypeIDs
INSERT INTO QA_Checks_Log values ( 3101, 'OK', @hVersion, curDate(), curTime() );
Drop table if exists tempA;
Create table tempA
Select   sourceTypeId as sourceTypeId2,
         'no '        as aMatch,
         count(*)     as n
From     idledayadjust
Group by sourceTypeId2;

Update tempA as a 
inner join ##defaultdb##.sourceUseType as m on a.sourceTypeId2 = m.sourceTypeId
set aMatch='yes';

Insert into CDB_Checks
       ( TableName,
         CheckNumber,
         TestDescription,
         testValue,
         count  )
Select  "idledayadjust" as tableName,
         3101,
        "sourceTypeId"    as testDescription,
         sourceTypeId2    as testValue,
         n                as count               --
From     tempA
Where    aMatch <> 'yes';

--       check no. 3102 -- checks unknown dayIDs
INSERT INTO QA_Checks_Log values ( 3102, 'OK', @hVersion, curDate(), curTime() );
Drop table if exists tempA;
Create table tempA
Select   dayId    as dayId2,
         'no '    as aMatch,
         count(*) as n
From     idledayadjust
Group by dayId2;

Update tempA as a 
inner join ##defaultdb##.dayOfAnyWeek as m on a.dayId2 = m.dayId
set aMatch='yes';

Insert into CDB_Checks
       ( TableName,
         CheckNumber,
         TestDescription,
         testValue,
         count  )
Select   "idledayadjust" as tableName,
         3102,
         "dayId"           as testDescription,
         dayId2            as testValue,
         n                 as count               --
From     tempA
Where    aMatch <> 'yes';

--       check no. 3103: check for missing sourceTypeID and dayID combinations
INSERT INTO QA_Checks_Log values ( 3103, 'OK', @hVersion, curDate(), curTime() );
Insert into CDB_Checks
       ( TableName,
         CheckNumber,
         TestDescription,
         sourceTypeID, dayID)
Select   "idledayadjust" as tableName,
         3103,
         'Missing combination of valid sourceTypeID, dayID' as testDescription,
		 sourceTypeID, dayID
from (
	SELECT sourceTypeID, dayID
	FROM  ##defaultdb##.sourceusetype
	CROSS JOIN ##defaultdb##.dayofanyweek
) as t1 left join idledayadjust using (sourceTypeID, dayID)
join (select count(*) as n from idledayadjust) as t2
where idleDayAdjust is NULL and n > 0
ORDER BY sourceTypeID, dayID LIMIT 1;


-- idlemodelyeargrouping
Insert into CDB_Checks (CheckNumber, TableName, TestDescription) values (3200, "idlemodelyeargrouping", "Table Check:");

--       check no. 3201 -- check for unknown sourceTypeIDs
INSERT INTO QA_Checks_Log values ( 3201, 'OK', @hVersion, curDate(), curTime() );
Drop table if exists tempA;
Create table tempA
Select   sourceTypeId as sourceTypeId2,
         'no '        as aMatch,
         count(*)     as n
From     idlemodelyeargrouping
Group by sourceTypeId2;

Update tempA as a 
inner join ##defaultdb##.sourceUseType as m on a.sourceTypeId2 = m.sourceTypeId
set aMatch='yes';

Insert into CDB_Checks
       ( TableName,
         CheckNumber,
         TestDescription,
         testValue,
         count  )
Select  "idlemodelyeargrouping" as tableName,
         3201,
        "sourceTypeId"    as testDescription,
         sourceTypeId2    as testValue,
         n                as count               --
From     tempA
Where    aMatch <> 'yes';

--       check no. 3202: check for missing source types if this table is populated
INSERT INTO QA_Checks_Log values ( 3202, 'OK', @hVersion, curDate(), curTime() );
Insert into CDB_Checks
       ( TableName,
         CheckNumber,
         TestDescription,
         sourceTypeID)
Select   "idlemodelyeargrouping" as tableName,
         3202,
         'Missing sourceTypeID' as testDescription,
		 sourceTypeID
from ##defaultdb##.sourceusetype
left join idlemodelyeargrouping using (sourceTypeID)
join (select count(*) as n from idlemodelyeargrouping) as t1
where totalIdleFraction is NULL and n > 0
ORDER BY sourceTypeID LIMIT 1;

--       check no. 3203 -- Check for gaps and overlaps in the model years columns in the idlemodelyeargrouping table.
INSERT INTO QA_Checks_Log values ( 3203, 'OK', @hVersion, curDate(), curTime() );

-- Add a table to contain the results of the gaps/overlaps check.
Drop   table if exists     qa_checks_imyg;
Create Table if Not Exists qa_checks_imyg (
  CsourceType   int,   -- sourceTypeID
  LMaxMY int,          -- last    row's max model year
  CMinMY int,          -- Current row's min model year
  CMaxMY int,          -- Current row's max model year
  Reason varchar(40) );

-- Call the procedure to check for gaps and overlaps.
call checkIdleModelYearGrouping();

Insert into CDB_Checks
       ( TableName,
         CheckNumber,
         TestDescription,
         count  )
Select  "idlemodelyeargrouping" as tableName,
         3203,
        "gaps and overlaps" as testDescription,
         (Select count(*) from qa_checks_imyg) as count
From     qa_checks_imyg
Where    (Select count(*) from qa_checks_imyg) > 0;


-- idlemonthadjust
Insert into CDB_Checks (CheckNumber, TableName, TestDescription) values (3300, "idlemonthadjust", "Table Check:");

--       check no. 3301 -- check for unknown sourceTypeIDs
INSERT INTO QA_Checks_Log values ( 3301, 'OK', @hVersion, curDate(), curTime() );
Drop table if exists tempA;
Create table tempA
Select   sourceTypeId as sourceTypeId2,
         'no '        as aMatch,
         count(*)     as n
From     idlemonthadjust
Group by sourceTypeId2;

Update tempA as a 
inner join ##defaultdb##.sourceUseType as m on a.sourceTypeId2 = m.sourceTypeId
set aMatch='yes';

Insert into CDB_Checks
       ( TableName,
         CheckNumber,
         TestDescription,
         testValue,
         count  )
Select  "idlemonthadjust" as tableName,
         3301,
        "sourceTypeId"    as testDescription,
         sourceTypeId2    as testValue,
         n                as count               --
From     tempA
Where    aMatch <> 'yes';

--       check no. 3302 -- checks unknown monthIDs
INSERT INTO QA_Checks_Log values ( 3302, 'OK', @hVersion, curDate(), curTime() );
Drop table if exists tempA;
Create table tempA
Select   monthID    as monthID2,
         'no '    as aMatch,
         count(*) as n
From     idlemonthadjust
Group by monthID2;

Update tempA as a 
inner join ##defaultdb##.monthOfAnyYear as m on a.monthId2 = m.monthId
set aMatch='yes';

Insert into CDB_Checks
       ( TableName,
         CheckNumber,
         TestDescription,
         testValue,
         count  )
Select   "idlemonthadjust" as tableName,
         3302,
         "monthID"           as testDescription,
         monthID2            as testValue,
         n                 as count               --
From     tempA
Where    aMatch <> 'yes';

--       check no. 3303: check for missing sourceTypeID and monthID combinations
INSERT INTO QA_Checks_Log values ( 3303, 'OK', @hVersion, curDate(), curTime() );
Insert into CDB_Checks
       ( TableName,
         CheckNumber,
         TestDescription,
         sourceTypeID, monthID)
Select   "idlemonthadjust" as tableName,
         3303,
         'Missing combination of valid sourceTypeID, monthID' as testDescription,
		 sourceTypeID, monthID
from (
	SELECT sourceTypeID, monthID
	FROM  ##defaultdb##.sourceusetype
	CROSS JOIN ##defaultdb##.monthOfAnyYear
) as t1 left join idlemonthadjust using (sourceTypeID, monthID)
join (select count(*) as n from idlemonthadjust) as t2
where idlemonthadjust is NULL and n > 0
ORDER BY sourceTypeID, monthID LIMIT 1;


-- totalidlefraction
Insert into CDB_Checks (CheckNumber, TableName, TestDescription) values (3400, "totalidlefraction", "Table Check:");

--       check no. 3401 -- check for unknown sourceTypeIDs
INSERT INTO QA_Checks_Log values ( 3401, 'OK', @hVersion, curDate(), curTime() );
Drop table if exists tempA;
Create table tempA
Select   sourceTypeId as sourceTypeId2,
         'no '        as aMatch,
         count(*)     as n
From     totalidlefraction
Group by sourceTypeId2;

Update tempA as a 
inner join ##defaultdb##.sourceUseType as m on a.sourceTypeId2 = m.sourceTypeId
set aMatch='yes';	

Insert into CDB_Checks
       ( TableName,
         CheckNumber,
         TestDescription,
         testValue,
         count  )
Select  "totalidlefraction" as tableName,
         3401,
        "sourceTypeId"    as testDescription,
         sourceTypeId2    as testValue,
         n                as count               --
From     tempA
Where    aMatch <> 'yes';

--       check no. 3402 -- checks unknown monthIDs
INSERT INTO QA_Checks_Log values ( 3402, 'OK', @hVersion, curDate(), curTime() );
Drop table if exists tempA;
Create table tempA
Select   monthID    as monthID2,
         'no '    as aMatch,
         count(*) as n
From     totalidlefraction
Group by monthID2;

Update tempA as a 
inner join ##defaultdb##.monthOfAnyYear as m on a.monthId2 = m.monthId
set aMatch='yes';
	
Insert into CDB_Checks
       ( TableName,
         CheckNumber,
         TestDescription,
         testValue,
         count  )
Select   "totalidlefraction" as tableName,
         3402,
         "monthID"           as testDescription,
         monthID2            as testValue,
         n                 as count               --
From     tempA
Where    aMatch <> 'yes';

--       check no. 3403 -- checks unknown dayIDs
INSERT INTO QA_Checks_Log values ( 3403, 'OK', @hVersion, curDate(), curTime() );
Drop table if exists tempA;
Create table tempA
Select   dayId    as dayId2,
         'no '    as aMatch,
         count(*) as n
From     totalidlefraction
Group by dayId2;

Update tempA as a 
inner join ##defaultdb##.dayOfAnyWeek as m on a.dayId2 = m.dayId
set aMatch='yes';

Insert into CDB_Checks
       ( TableName,
         CheckNumber,
         TestDescription,
         testValue,
         count  )
Select   "totalidlefraction" as tableName,
         3403,
         "dayId"           as testDescription,
         dayId2            as testValue,
         n                 as count               --
From     tempA
Where    aMatch <> 'yes';

--       check no. 3404 -- checks unknown idleRegionID
INSERT INTO QA_Checks_Log values ( 3404, 'OK', @hVersion, curDate(), curTime() );
Drop table if exists tempA;
Create table tempA
Select   idleRegionID    as idleRegionID2,
         'no '    as aMatch,
         count(*) as n
From     totalidlefraction
Group by idleRegionID2;

Update tempA as a 
inner join ##defaultdb##.idleregion as m on a.idleRegionID2 = m.idleRegionID
set aMatch='yes';

Insert into CDB_Checks
       ( TableName,
         CheckNumber,
         TestDescription,
         testValue,
         count  )
Select   "totalidlefraction" as tableName,
         3404,
         "idleRegionID"      as testDescription,
         idleRegionID2       as testValue,
         n                   as count               --
From     tempA
Where    aMatch <> 'yes';

--       check no. 3405 -- check for multiple idleRegionIDs
INSERT INTO QA_Checks_Log values ( 3405, 'OK', @hVersion, curDate(), curTime() );
Insert into CDB_Checks
       ( TableName,
         CheckNumber,
         TestDescription,
         `count`  )
Select   "totalidlefraction"           as tableName,
         3405,
        "Multiple idleRegionIDs"       as testDescription,
         count(distinct idleRegionID) as `count`
from totalidlefraction
having `count` > 1;

--       check no. 3406 -- checks unknown countyTypeID
INSERT INTO QA_Checks_Log values ( 3406, 'OK', @hVersion, curDate(), curTime() );
Drop table if exists tempA;
Create table tempA
Select   countyTypeID    as countyTypeID2,
         'no '    as aMatch,
         count(*) as n
From     totalidlefraction
Group by countyTypeID2;

Update tempA as a 
inner join ##defaultdb##.countytype as m on a.countyTypeID2 = m.countyTypeID
set aMatch='yes';

Insert into CDB_Checks
       ( TableName,
         CheckNumber,
         TestDescription,
         testValue,
         count  )
Select   "totalidlefraction" as tableName,
         3406,
         "countyTypeID"      as testDescription,
         countyTypeID2       as testValue,
         n                   as count               --
From     tempA
Where    aMatch <> 'yes';

--       check no. 3407 -- check for multiple countyTypeIDs
INSERT INTO QA_Checks_Log values ( 3407, 'OK', @hVersion, curDate(), curTime() );
Insert into CDB_Checks
       ( TableName,
         CheckNumber,
         TestDescription,
         `count`  )
Select   "totalidlefraction"          as tableName,
         3407,
        "Multiple countyTypeIDs"      as testDescription,
         count(distinct countyTypeID) as `count`
from totalidlefraction
having `count` > 1;

--       check no. 3408 -- Check for gaps and overlaps in the model years columns in the TIF table.
INSERT INTO QA_Checks_Log values ( 3408, 'OK', @hVersion, curDate(), curTime() );

-- Add a table to contain the results of the gaps/overlaps check.
Drop   table if exists     qa_checks_tif;
Create Table if Not Exists qa_checks_tif (
  CsourceType   int,   -- sourceTypeID
  Cmonth		int,   -- monthID
  Cday          int,   -- dayID
  LMaxMY int,          -- last    row's max model year
  CMinMY int,          -- Current row's min model year
  CMaxMY int,          -- Current row's max model year
  Reason varchar(40) );

-- Call the procedure to check for gaps and overlaps.
call checkTotalIdleFraction();

Insert into CDB_Checks
       ( TableName,
         CheckNumber,
         TestDescription,
         count  )
Select  "totalidlefraction" as tableName,
         3408,
        "gaps and overlaps" as testDescription,
         (Select count(*) from qa_checks_tif) as count
From     qa_checks_tif
Where    (Select count(*) from qa_checks_tif) > 0;

--       check no. 3409: check for missing sourceTypeID, monthID, dayID combinations
INSERT INTO QA_Checks_Log values ( 3409, 'OK', @hVersion, curDate(), curTime() );
Insert into CDB_Checks
       ( TableName,
         CheckNumber,
         TestDescription,
         sourceTypeID, monthID, dayID)
Select   "totalidlefraction" as tableName,
         3409,
         'Missing combination of valid sourceTypeID, monthID, dayID' as testDescription,
		 sourceTypeID, monthID, dayID
from (
	SELECT sourceTypeID, monthID, dayID
	FROM  ##defaultdb##.sourceusetype
	CROSS JOIN ##defaultdb##.monthOfAnyYear
	CROSS JOIN ##defaultdb##.dayOfAnyWeek
) as t1 left join totalidlefraction using (sourceTypeID, monthID, dayID)
join (select count(*) as n from totalidlefraction) as t2
where totalIdleFraction is NULL and n > 0
ORDER BY sourceTypeID, monthID, dayID LIMIT 1;


-- IMCoverage checks
Insert into CDB_Checks (CheckNumber, TableName, TestDescription) values (3500, "imCoverage", "Table Check:");

--       check no. 3501 -- check for unknown countyIDs
INSERT INTO QA_Checks_Log values ( 3501, 'OK', @hVersion, curDate(), curTime() );
Drop table if exists tempA;
Create table tempA
Select   countyId        as countyId2,
         'no '           as aMatch,
         count(*)        as n
From     imCoverage
Group by countyId2;

Update tempA as a 
inner join county as c on a.countyId2 = c.countyId
set aMatch='yes';

Insert into CDB_Checks
       ( TableName,
         CheckNumber,
         TestDescription,
         testValue,
         count  )
Select  "imCoverage" as tableName,
         3501,
        "countyId"      as testDescription,
         countyId2      as testValue,
         n              as count               --
From     tempA
Where    aMatch <> 'yes';

--       check no. 3502 -- check for unknown fuelTypeIDs
INSERT INTO QA_Checks_Log values ( 3502, 'OK', @hVersion, curDate(), curTime() );
Drop table if exists tempA;
Create table tempA
Select   fuelTypeId      as fuelTypeId2,
         'no '           as aMatch,
         count(*)        as n
From     imCoverage
Group by fuelTypeId2;

Update tempA as a 
inner join ##defaultdb##.fuelType as m on a.FuelTypeId2 = m.fuelTypeId
set aMatch='yes';

Insert into CDB_Checks
       ( TableName,
         CheckNumber,
         TestDescription,
         testValue,
         count  )
Select  "imCoverage" as tableName,
         3502,
        "fuelTypeId" as testDescription,
         fuelTypeId2 as testValue,
         n           as count
From     tempA
Where    aMatch <> 'yes';

--       check no. 3503 -- check for unknown polProcessIDs
INSERT INTO QA_Checks_Log values ( 3503, 'OK', @hVersion, curDate(), curTime() );
Drop table if exists tempA;
Create table tempA
Select   polProcessId    as polProcessId2,
         'no '           as aMatch,
         count(*)        as n
From     imCoverage
Group by polProcessId2;
							  
Update tempA as a 
inner join ##defaultdb##.pollutantProcessAssoc as m on a.polProcessId2 = m.polProcessId
set aMatch='yes';										 

Insert into CDB_Checks
       ( TableName,
         CheckNumber,
         TestDescription,
         testValue,
         count  )
Select  "imCoverage" as tableName,
         3503,
        "polProcessId"      as testDescription,
         polProcessId2      as testValue,
         n                  as count               --
From     tempA
Where    aMatch <> 'yes';

--       check no. 3504 -- check that inspectFreq is either 1 or 2
INSERT INTO QA_Checks_Log values ( 3504, 'OK', @hVersion, curDate(), curTime() );
Drop table if exists tempA;
Create table tempA
Select   inspectFreq     as inspectFreq2,
         'no '           as aMatch,
         count(*)        as n
From     imCoverage
Group by inspectFreq2;

Update tempA as a set aMatch='yes' where inspectFreq2 in (1,2);

Insert into CDB_Checks
       ( TableName,
         CheckNumber,
         TestDescription,
         testValue,
         count  )
Select  "imCoverage"  as tableName,
         3504,
        "inspectFreq" as testDescription,
         inspectFreq2 as testValue,
         n            as count               --
From     tempA
Where    aMatch <> 'yes';

--       check no. 3505 -- check that useIMyn is either "y" or "n"
INSERT INTO QA_Checks_Log values ( 3505, 'OK', @hVersion, curDate(), curTime() );
Drop table if exists tempA;
Create table tempA
Select   useIMyn         as useIMyn2,
         'no '           as aMatch,
         count(*)        as n
From     imCoverage
Group by useIMyn2;

Update tempA as a set aMatch='yes' where useIMyn2 in ('y', 'n', 'Y', 'N');

Insert into CDB_Checks
       ( TableName,
         CheckNumber,
         TestDescription,
         testValue,
         count  )
Select   "imCoverage" as tableName,
         3505,
        "useIMyn not Y or N" as testDescription,
         useIMyn2      as testValue,
         n         as count               --
From     tempA
Where    aMatch <> 'yes';

--       check no. 3506 -- check that the complianceFactor is between 0 and 100
INSERT INTO QA_Checks_Log values ( 3506, 'OK', @hVersion, curDate(), curTime() );
Drop table if exists tempA;
Create table tempA
Select   complianceFactor as complianceFactor2,
         'no '            as aMatch,
         count(*)         as n
From     imCoverage
Group by complianceFactor;

Update tempA as a set aMatch='yes' where complianceFactor2>=0.0 and complianceFactor2<=100.0;

Insert into CDB_Checks
       ( TableName,
         CheckNumber,
         TestDescription,
         testValue,
         count  )
Select   "imCoverage" as tableName,
         3506,
        "complianceFactor range" as testDescription,
         complianceFactor2       as testValue,
         n                       as count
From     tempA
Where    aMatch <> 'yes';

--       check no. 3507 -- check for unknown sourceTypeIDs
INSERT INTO QA_Checks_Log values ( 3507, 'OK', @hVersion, curDate(), curTime() );
Drop table if exists tempA;
Create table tempA
Select   sourceTypeId as sourceTypeId2,
         'no '        as aMatch,
         count(*)     as n
From     imCoverage
Group by sourceTypeId2;

Update tempA as a 
inner join ##defaultdb##.sourceUseType as m on a.sourceTypeId2 = m.sourceTypeId
set aMatch='yes';	

Insert into CDB_Checks
       ( TableName,
         CheckNumber,
         TestDescription,
         testValue,
         count  )
Select   "imCoverage"  as tableName,
         3507,
        "sourceTypeId" as testDescription,
         sourceTypeId2 as testValue,
         n             as count
From     tempA
Where    aMatch <> 'yes';

--       check no. 3508 -- check for unknown stateIDs
INSERT INTO QA_Checks_Log values ( 3508, 'OK', @hVersion, curDate(), curTime() );
Drop table if exists tempA;
Create table tempA
Select   stateId      as stateId2,
         'no '        as aMatch,
         count(*)     as n
From     imcoverage
Group by stateId2;

Update tempA as a 
inner join state as c on a.stateId2 = c.stateId
set aMatch='yes';	

Insert into CDB_Checks
       ( TableName,
         CheckNumber,
         TestDescription,
         testValue,
         count  )
Select   "imCoverage"        as tableName,
         3508,
         "stateId not valid" as testDescription,
         stateId2            as testValue,
         n                   as count
From     tempA
Where    aMatch <> 'yes';

--       check no. 3509 -- check for unknown yearIDs
INSERT INTO QA_Checks_Log values ( 3509, 'OK', @hVersion, curDate(), curTime() );
Drop table if exists tempA;
Create table tempA
Select   yearId          as yearId2,
         'no '           as aMatch,
         count(*)        as n
From     imcoverage
Group by yearId2;

Update tempA as a 
inner join year as c on a.yearId2 = c.yearId
set aMatch='yes';	

Insert into CDB_Checks
       ( TableName,
         CheckNumber,
         TestDescription,
         testValue,
         count  )
Select   "imCoverage" as tableName,
         3509,
        "yearId"      as testDescription,
         yearId2      as testValue,
         n            as count               --
From     tempA
Where    aMatch <> 'yes';

--       check no. 3510 -- check to make sure there are the same number of entries (by state, county, year, and source type) for gasoline and FFV.
INSERT INTO QA_Checks_Log values ( 3510, 'OK', @hVersion, curDate(), curTime() );

Drop table if exists tempA;
Create table tempA
select stateId,
       countyId,
       yearId,
       sourceTypeId,
       fuelTypeId
from   imCoverage;

alter table tempA add numFtype1 int default 0;
alter table tempA add numFtype5 int default 0;

drop   table if exists tempB;
create table           tempB
select * from tempA;

create INDEX idxA on tempA (stateId, countyId, yearId, sourceTypeId);
create INDEX idxB on tempB (stateId, countyId, yearId, sourceTypeId);

update tempA as a set a.numFtype1 = (select count(*)
                                     from tempB as b
                                     where b.fuelTypeId=1
                                       and a.stateId      = b.stateId
                                       and a.countyId     = b.countyId
                                       and a.yearId       = b.yearId
                                       and a.sourceTypeId = b.sourceTypeId
                                     group by               b.stateId,
                                                            b.countyId,
                                                            b.yearId,
                                                            b.sourceTypeId);

update tempA as a set a.numFtype5 = (select count(*)
                                     from tempB as b
                                     where b.fuelTypeId=5
                                       and a.stateId      = b.stateId
                                       and a.countyId     = b.countyId
                                       and a.yearId       = b.yearId
                                       and a.sourceTypeId = b.sourceTypeId
                                     group by               b.stateId,
                                                            b.countyId,
                                                            b.yearId,
                                                            b.sourceTypeId);

update tempa set numFtype1=0 where isnull(numFtype1);
update tempa set numFtype5=0 where isnull(numFtype5);

Insert into CDB_Checks
       ( TableName,
         CheckNumber,
         TestDescription,
         count,
         stateId,
         countyId,
         yearId,
         sourceTypeId )
Select  'imCoverage'               as tableName,
         3510                       as checkNumber,
        'counts fuelTypesIds 1<>5' as testDescription,
         count(*)                  as count,
         stateId,
         countyId,
         yearId,
         sourceTypeId
from     tempA
where numFtype1<>numFtype5
group by stateId,
         countyId,
         yearId,
         sourceTypeId;

--       check no. 3511 -- Check for gaps and overlaps in the IMCoverage table.
-- Add a table to contain the results of the IMCoverage gaps/overlaps check.
Drop   table if exists     qa_checks_im;
Create Table if Not Exists qa_checks_im (
  Cpol   int,          -- polProcessId
  Ccou   int,          -- CountyId
  Cyea   int,          -- yearId
  Csou   int,          -- sourceTypeId
  Cfue   int,          -- fuelTypeId
  LENDMY int,          -- last    row's end model year
  CBEGMY int,          -- Current row's beg model year
  CENDMY int,          -- Current row's end model year
  useIMyn char(1),
  Reason varchar(40) );

-- Call the procedure to check for gaps and overlaps.
call checkImCoverage();

Insert into CDB_Checks
       ( TableName,
         CheckNumber,
         TestDescription,
         count  )
Select  "imCoverage" as tableName,
         3511,
        "gaps and overlaps" as testDescription,
         (Select count(*) from qa_checks_im) as count
From     qa_checks_im
Where    (Select count(*) from qa_checks_im) > 0;


-- monthVMTFraction checks
Insert into CDB_Checks (CheckNumber, TableName, TestDescription) values (3600, "monthVmtFraction", "Table Check:");

--       check no. 3601 -- check for unknown monthIDs
INSERT INTO QA_Checks_Log values ( 3601, 'OK', @hVersion, curDate(), curTime() );
Drop table if exists tempA;
Create table tempA
Select   monthId      as monthId2,
         'no '        as aMatch,
         count(*)     as n
From     monthVmtFraction
Group by monthId2;

Update tempA as a 
inner join ##defaultdb##.monthOfAnyYear as m on a.monthId2 = m.monthId
set aMatch='yes';

Insert into CDB_Checks
       ( TableName,
         CheckNumber,
         TestDescription,
         testValue,
         count  )
Select   "monthVmtFraction" as tableName,
         3601,
        "monthId"           as testDescription,
         monthId2           as testValue,
         n                  as count               --
From     tempA
Where    aMatch <> 'yes';

--       check no. 3602 -- check for unknown sourceTypeIDs
INSERT INTO QA_Checks_Log values ( 3602, 'OK', @hVersion, curDate(), curTime() );
Drop table if exists tempA;
Create table tempA
Select   sourceTypeId as sourceTypeId2,
         'no '        as aMatch,
         count(*)     as n
From     monthVmtFraction
Group by sourceTypeId2;

Update tempA as a 
inner join ##defaultdb##.sourceUseType as m on a.sourceTypeId2 = m.sourceTypeId
set aMatch='yes';

Insert into CDB_Checks
       ( TableName,
         CheckNumber,
         TestDescription,
         testValue,
         count  )
Select   "monthVmtFraction" as tableName,
         3602,
         "sourceTypeId"     as testDescription,
         sourceTypeId2      as testValue,
         n                  as count
From     tempA
Where    aMatch <> 'yes';

--       check no. 3603 -- check that monthVMTFraction sums to 1 for each sourceTypeID
INSERT INTO QA_Checks_Log values ( 3603, 'OK', @hVersion, curDate(), curTime() );
Drop table if exists tempA;
Create table tempA
select   sourceTypeId,
         sum(monthVmtfraction) as s,
         'n' as Ok
from     monthVMTfraction
group by sourceTypeId;

update tempA set Ok = 'y' where s > 0.99999 and s < 1.00001 and s is not null;

Insert into CDB_Checks
       ( tableName,
         checkNumber,
         testDescription,
         testValue,
         sourceTypeId
       )
Select  'monthVmtFraction'    as tableName,
         3603                  as checkNumber,
         'sum of monthVMTFraction <> 1' as testDescription,
         s                    as testValue,
         sourceTypeId         as sourceType
From     tempA
Where    Ok = 'n';

--       check no. 3604 -- check for monthVMTFractions >= 1
INSERT INTO QA_Checks_Log values ( 3604, 'OK', @hVersion, curDate(), curTime() );
Insert into CDB_Checks
       ( TableName,
         CheckNumber,
         TestDescription,
         testValue,
         sourceTypeID)
Select   "monthvmtfraction" as tableName,
         3604,
         'monthVMTFraction >= 1' as testDescription,
		 monthVMTFraction as testValue,
         sourceTypeID
from monthvmtfraction
where monthVMTFraction >= 1
ORDER BY sourceTypeID LIMIT 1;

--       check no. 3605: make sure month distributions aren't flat
INSERT INTO QA_Checks_Log values ( 3605, 'OK', @hVersion, curDate(), curTime() );
Insert into CDB_Checks
       ( TableName,
         CheckNumber,
         TestDescription,
         testValue,
         sourceTypeID)
Select   "monthvmtfraction" as tableName,
         3605 as checkNumber,
         'monthVMTFraction is a flat profile' as testDescription,
         concat('all are ', monthVMTFraction) as testValue,
		 sourceTypeID
from monthvmtfraction
group by sourceTypeID, monthVMTFraction
having count(*) = (select count(*) from ##defaultdb##.monthOfAnyYear)
order by sourceTypeID LIMIT 1;

--       check no. 3606: check for missing sourceTypeID, monthID combinations
--                       only if monthVMTFraction is used
INSERT INTO QA_Checks_Log values ( 3606, 'OK', @hVersion, curDate(), curTime() );
Insert into CDB_Checks
       ( TableName,
         CheckNumber,
         TestDescription,
         sourceTypeID, monthID)
Select   "monthVMTFraction" as tableName,
         3606,
         'Missing combination of valid sourceTypeID, monthID' as testDescription,
		 sourceTypeID, monthID
from (
	SELECT sourceTypeID, monthID
	FROM  ##defaultdb##.sourceusetype
	CROSS JOIN ##defaultdb##.monthOfAnyYear
) as t1 
left join monthVMTFraction using (sourceTypeID, monthID)
join (select count(*) as n from monthVMTFraction) as t2
where monthVMTFraction.monthVMTFraction is NULL and n > 0
ORDER BY sourceTypeID, monthID LIMIT 1;


-- onroadretrofit
Insert into CDB_Checks (CheckNumber, TableName, TestDescription) values (3700, "onRoadRetroFit", "Table Check:");

--       check no. 3701 -- check for unknown pollutantIDs
INSERT INTO QA_Checks_Log values ( 3701, 'OK', @hVersion, curDate(), curTime() );
Drop table if exists tempA;
Create table tempA
select  pollutantId,
       'no '    as aMatch,
       count(*) as n
from   onRoadRetroFit
group by pollutantId;

Update tempA as a 
inner join ##defaultdb##.pollutant as m on a.pollutantId = m.pollutantId
set aMatch='yes';

Insert into CDB_Checks
       ( TableName,
         CheckNumber,
         TestDescription,
         testValue,
         count  )
Select   "onRoadRetroFit" as tableName,
         3701              as checkNumber,
        "pollutantId"     as testDescription,
         pollutantId      as testValue,
         count(*)         as cou
from     tempA
where    aMatch <> 'yes ';
Delete from CDB_Checks where checkNumber=3701 and count=0;

--       check no. 3702 -- check for unknown processIDs
INSERT INTO QA_Checks_Log values ( 3702, 'OK', @hVersion, curDate(), curTime() );
Drop table if exists tempA;
Create table tempA
select  processId,
       'no '    as aMatch,
       count(*) as n
from   onRoadRetroFit
group by processId;
									  
Update tempA as a 
inner join ##defaultdb##.pollutantProcessAssoc as m on a.processId = m.processId
set aMatch='yes';	
									  
Insert into CDB_Checks
       ( TableName,
         CheckNumber,
         TestDescription,
         testValue,
         count  )
Select   "onRoadRetroFit" as tableName,
         3702              as checkNumber,
        "processId"       as testDescription,
         processId        as testValue,
         count(*)         as cou
from     tempA
where    aMatch <> 'yes ';
Delete from CDB_Checks where checkNumber=3702 and count=0;

--       check no. 3703 -- check for unknown fuelTypeIDs
INSERT INTO QA_Checks_Log values ( 3703, 'OK', @hVersion, curDate(), curTime() );
Drop table if exists tempA;
Create table tempA
select  fuelTypeId ,
       'no '    as aMatch,
       count(*) as n
from   onRoadRetroFit
group by fuelTypeId;

Update tempA as a 
inner join ##defaultdb##.fuelType as m on a.FuelTypeId = m.fuelTypeId
set aMatch='yes';

Insert into CDB_Checks
       ( TableName,
         CheckNumber,
         TestDescription,
         testValue,
         count  )
Select   "onRoadRetroFit" as tableName,
         3703              as checkNumber,
        "fuelTypeId "     as testDescription,
         fuelTypeId       as testValue,
         count(*)         as cou
from     tempA
where    aMatch <> 'yes ';
Delete from CDB_Checks where checkNumber=3703 and count=0;

--       check no. 3704 -- check for unknown sourceTypeIDs
INSERT INTO QA_Checks_Log values ( 3704, 'OK', @hVersion, curDate(), curTime() );
Drop table if exists tempA;
Create table tempA
select  sourceTypeId,
       'no '    as aMatch,
       count(*) as n
from   onRoadRetroFit
group by sourceTypeId;

Update tempA as a 
inner join ##defaultdb##.sourceUseType as m on a.sourceTypeId = m.sourceTypeId
set aMatch='yes';

Insert into CDB_Checks
       ( TableName,
         CheckNumber,
         TestDescription,
         testValue,
         count  )
Select   "onRoadRetroFit" as tableName,
         3704              as checkNumber,
        "sourceTypeId"    as testDescription,
         sourceTypeId     as testValue,
         count(*)         as cou
from     tempA
where    aMatch <> 'yes ';
Delete from CDB_Checks where checkNumber=3704 and count=0;

--       check no. 3705 -- check that the retrofitYearID <= the analysis year
INSERT INTO QA_Checks_Log values ( 3705, 'OK', @hVersion, curDate(), curTime() );
Insert into CDB_Checks
       ( TableName,
         CheckNumber,
         TestDescription,
         testValue,
         count  )
Select   "onRoadRetroFit" as tableName,
         3705              as checkNumber,
        "retrofitYearID > yearID"  as testDescription,
         retrofitYearId   as testValue,
         count(*)         as cou
from     onroadretrofit
join     `year` on yearID
where    retrofitYearID > yearID
group by retrofitYearID;

--       check no. 3706 -- check that endModelYearID <= retrofitYearID
INSERT INTO QA_Checks_Log values ( 3706, 'OK', @hVersion, curDate(), curTime() );
Insert into CDB_Checks
       ( TableName,
         CheckNumber,
         TestDescription,
         testValue,
         count  )
Select   "onRoadRetroFit" as tableName,
         3706              as checkNumber,
        "endModelYearId > retrofitYearID"  as testDescription,
         CONCAT(endModelYearId, ' > ', retrofitYearID) as testValue,
         count(*)         as cou
from     onroadretrofit
where    endModelYearID > retrofitYearID
group by endModelYearID, retrofitYearID;

--       check no. 3707 -- check that beginModelYearID <= endModelYearID
INSERT INTO QA_Checks_Log values ( 3707, 'OK', @hVersion, curDate(), curTime() );
Insert into CDB_Checks
       ( TableName,
         CheckNumber,
         TestDescription,
         testValue,
         count  )
Select   "onRoadRetroFit"  as tableName,
         3707               as checkNumber,
        "beginModelYearId > endModelYearID" as testDescription,
         CONCAT(beginModelYearId, ' > ', endModelYearID)  as testValue,
         count(*)          as cou
from     onroadretrofit
where    beginModelYearId > endModelYearID
group by beginModelYearId, endModelYearID;

--       check no. 3708 -- check that cumFractionRetrofit between 0 and 1
INSERT INTO QA_Checks_Log values ( 3708, 'OK', @hVersion, curDate(), curTime() );
Insert into CDB_Checks
       ( TableName,
         CheckNumber,
         TestDescription,
         testValue,
         count  )
Select   "onRoadRetroFit"  as tableName,
         3708               as checkNumber,
        "cumFractionRetrofit range" as testDescription,
         cumFractionRetrofit as testValue,
         count(*)          as cou
from     onroadretrofit
where    cumFractionRetrofit NOT BETWEEN 0 and 1
group by cumFractionRetrofit;

--       check no. 3709 -- check that retrofitEffectiveFraction <= 1
INSERT INTO QA_Checks_Log values ( 3709, 'OK', @hVersion, curDate(), curTime() );
Insert into CDB_Checks
       ( TableName,
         CheckNumber,
         TestDescription,
         testValue,
         count  )
Select   "onRoadRetroFit"  as tableName,
         3709               as checkNumber,
        "retrofitEffectiveFraction range" as testDescription,
         retrofitEffectiveFraction as testValue,
         count(*)          as cou
from     onroadretrofit
where    retrofitEffectiveFraction > 1
group by retrofitEffectiveFraction;


-- roadTypeDistribution checks
Insert into CDB_Checks (CheckNumber, TableName, TestDescription) values (3800, "roadTypeDistribution", "Table Check:");

--       check no. 3801 -- check for unknown roadTypeID
INSERT INTO QA_Checks_Log values ( 3801, 'OK', @hVersion, curDate(), curTime() );
Drop table if exists tempA;
Create table tempA
Select   roadTypeId as roadTypeId2,
         'no '      as aMatch,
         count(*)   as n
From     roadTypeDistribution
Group by roadTypeId2;

Update tempA as a 
inner join ##defaultdb##.roadType as m on a.roadTypeId2 = m.roadTypeId
set aMatch='yes';

Insert into CDB_Checks
       ( TableName,
         CheckNumber,
         TestDescription,
         testValue,
         count  )
Select   "roadTypeDistribution" as tableName,
         3801,
        "roadTypeId"            as testDescription,
         roadTypeId2            as testValue,
         n                      as count
From     tempA
Where    aMatch <> 'yes';

--       check no. 3802 -- check for unknown sourceTypeIDs
INSERT INTO QA_Checks_Log values ( 3802, 'OK', @hVersion, curDate(), curTime() );
Drop table if exists tempA;
Create table tempA
Select   sourceTypeId as sourceTypeId2,
         'no '        as aMatch,
         count(*)     as n
From     roadTypeDistribution
Group by sourceTypeId2;

Update tempA as a 
inner join ##defaultdb##.sourceUseType as m on a.sourceTypeId2 = m.sourceTypeId
set aMatch='yes';	

Insert into CDB_Checks
       ( TableName,
         CheckNumber,
         TestDescription,
         testValue,
         count  )
Select   "roadTypeDistribution" as tableName,
         3802,
        "sourceTypeId"          as testDescription,
         sourceTypeId2          as testValue,
         n                      as count
From     tempA
Where    aMatch <> 'yes';

--       check no. 3803 -- check that the roadTypeVMTFraction sums to 1
INSERT INTO QA_Checks_Log values ( 3803, 'OK', @hVersion, curDate(), curTime() );
Insert into CDB_Checks
       ( tableName,
         checkNumber,
         testDescription,
         testValue,
           sourceTypeId  )
Select  "roadTypeDistribution"              as tableName,
         3803                                 as checkNumber,
        "sum of roadTypeVmtFraction <> 1.0" as testDescription,
         sum(roadTypeVmtFraction)           as testValue,
           sourceTypeId
From     roadTypeDistribution
Group by sourceTypeId
Having   testValue <0.99999 or testValue >1.00001;

--       check no. 3804 -- check for roadTypeVMTFraction >= 1
INSERT INTO QA_Checks_Log values ( 3804, 'OK', @hVersion, curDate(), curTime() );
Insert into CDB_Checks
       ( TableName,
         CheckNumber,
         TestDescription,
         testValue,
         sourceTypeID)
Select   "roadtypedistribution" as tableName,
         3804,
         'roadTypeVMTFraction >= 1' as testDescription,
		 roadTypeVMTFraction as testValue,
         sourceTypeID
from roadtypedistribution
where roadTypeVMTFraction >= 1
ORDER BY sourceTypeID LIMIT 1;

--       check no. 3805: make sure road type distributions aren't flat
INSERT INTO QA_Checks_Log values ( 3805, 'OK', @hVersion, curDate(), curTime() );
Insert into CDB_Checks
       ( TableName,
         CheckNumber,
         TestDescription,
         testValue,
         sourceTypeID)
Select   "roadtypedistribution" as tableName,
         3805 as checkNumber,
         'roadTypeVMTFraction is a flat profile' as testDescription,
         concat('all are ', roadTypeVMTFraction) as testValue,
		 sourceTypeID
from roadtypedistribution
where roadTypeID <> 1
group by sourceTypeID, roadTypeVMTFraction
having count(*) = (select count(*) from ##defaultdb##.roadtype where roadTypeID not in (1, 100))
order by sourceTypeID LIMIT 1;

--       check no. 3806: check for missing sourceTypeID, roadTypeID combinations
INSERT INTO QA_Checks_Log values ( 3806, 'OK', @hVersion, curDate(), curTime() );
Insert into CDB_Checks
       ( TableName,
         CheckNumber,
         TestDescription,
         sourceTypeID, roadTypeID)
Select   "roadtypedistribution" as tableName,
         3806,
         'Missing combination of valid sourceTypeID, roadTypeID' as testDescription,
		 sourceTypeID, roadTypeID
from (
	SELECT sourceTypeID, roadTypeID
	FROM  ##defaultdb##.sourceusetype
	CROSS JOIN ##defaultdb##.roadtype where roadTypeID in (2, 3, 4, 5)
) as t1 left join roadtypedistribution using (sourceTypeID, roadTypeID)
where roadTypeVMTFraction is NULL 
ORDER BY sourceTypeID, roadTypeID LIMIT 1;


-- SourceTypeAgeDistribution checks
Insert into CDB_Checks (CheckNumber, TableName, TestDescription) values (3900, "sourceTypeAgeDistribution", "Table Check:");

--       check no. 3901  -- check for unknown ageIDs
INSERT INTO QA_Checks_Log values ( 3901, 'OK', @hVersion, curDate(), curTime() );
Drop table if exists tempA;
Create table tempA
Select   ageId         as ageId2,
         'no '         as aMatch,
         count(*)      as n
From     sourceTypeAgeDistribution
Group by ageId2;

Update tempA as a 
inner join ##defaultdb##.ageCategory as m on a.ageId2 = m.ageId
set aMatch='yes';

Insert into CDB_Checks
       ( TableName,
         CheckNumber,
         TestDescription,
         testValue,
         count  )
Select   "sourceTypeAgeDistribution" as tableName,
         3901,
        "ageId"                      as testDescription,
         ageId2                      as testValue,
         n                           as count
From     tempA
Where    aMatch <> 'yes';

--       check no. 3902 -- check for unknown sourceTypeIDs
INSERT INTO QA_Checks_Log values ( 3902, 'OK', @hVersion, curDate(), curTime() );
Drop table if exists tempA;
Create table tempA
Select   sourceTypeId as sourceTypeId2,
         'no '        as aMatch,
         count(*)     as n
From     sourceTypeAgeDistribution
Group by sourceTypeId2;

Update tempA as a 
inner join ##defaultdb##.sourceUseType as m on a.sourceTypeId2 = m.sourceTypeId
set aMatch='yes';

Insert into CDB_Checks
       ( TableName,
         CheckNumber,
         TestDescription,
         testValue,
         count  )
Select   "sourceTypeAgeDistribution" as tableName,
         3902,
        "sourceTypeId"               as testDescription,
         sourceTypeId2               as testValue,
         n                           as count
From     tempA
Where    aMatch <> 'yes';

--       check no. 3903 -- check for unknown yearIDs
INSERT INTO QA_Checks_Log values ( 3903, 'OK', @hVersion, curDate(), curTime() );
Drop table if exists tempA;
Create table tempA
Select   yearId       as yearId2,
         'no '        as aMatch,
         count(*)     as n
From     sourceTypeAgeDistribution
Group by yearId2;

Update tempA as a 
inner join year as c on a.yearId2 = c.yearId
set aMatch='yes';	

Insert into CDB_Checks
       ( TableName,
         CheckNumber,
         TestDescription,
         testValue,
         count  )
Select   "sourceTypeAgeDistribution" as tableName,
         3903,
        "yearId"                     as testDescription,
         yearId2                     as testValue,
         n                           as count
From     tempA
Where    aMatch <> 'yes';

--       check no. 3904 -- check that ageFraction sums to 1
INSERT INTO QA_Checks_Log values ( 3904, 'OK', @hVersion, curDate(), curTime() );
Insert into CDB_Checks
       ( tableName,
         checkNumber,
         testDescription,
         testValue,
           sourceTypeId,
           yearId)
Select  "sourceTypeAgeDistribution"   as tableName,
         3904                           as checkNumber,
        "sum of AgeFraction <> 1.0"   as testDescription,
         sum(ageFraction)             as testValue,
           sourceTypeId,
           yearId
From     sourceTypeAgeDistribution
Group by sourceTypeId,
         yearId
Having   testValue <0.99999 or testValue >1.00001;

--       check no. 3905: check for missing sourceTypeID, yearID, ageID combinations
INSERT INTO QA_Checks_Log values ( 3905, 'OK', @hVersion, curDate(), curTime() );
Insert into CDB_Checks
       ( TableName,
         CheckNumber,
         TestDescription,
         testValue)
Select   "sourcetypeagedistribution" as tableName,
         3905,
         'Missing combination of valid sourceTypeID, yearID, ageID' as testDescription,
		 concat('ST: ', sourceTypeID, ', year: ', yearID, ', age: ', ageID) as testValue
from (
	SELECT sourceTypeID, yearID, ageID
	FROM  ##defaultdb##.sourceusetype
	CROSS JOIN year
	CROSS JOIN ##defaultdb##.agecategory
) as t1 left join sourcetypeagedistribution using (sourceTypeID, yearID, ageID)
where ageFraction is NULL 
ORDER BY sourceTypeID, yearID, ageID LIMIT 1;

--       check no. 3906: make sure age distributions aren't flat
INSERT INTO QA_Checks_Log values ( 3906, 'OK', @hVersion, curDate(), curTime() );
Insert into CDB_Checks
       ( TableName,
         CheckNumber,
         TestDescription,
         testValue,
         sourceTypeID, yearID)
Select   "sourcetypeagedistribution" as tableName,
         3906 as checkNumber,
         'ageFraction is a flat profile' as testDescription,
         concat('all are ', ageFraction) as testValue,
		 sourceTypeID, yearID
from sourcetypeagedistribution
group by sourceTypeID, yearID, ageFraction
having count(*) = (select count(*) from ##defaultdb##.ageCategory)
order by sourceTypeID, yearID LIMIT 1;


-- sourceTypeDayVMT checks
Insert into CDB_Checks (CheckNumber, TableName, TestDescription) values (4000, "sourceTypeDayVMT", "Table Check:");

--       check no. 4001 -- check for unknown yearIDs
INSERT INTO QA_Checks_Log values ( 4001, 'OK', @hVersion, curDate(), curTime() );
Drop table if exists tempA;
Create table tempA
Select   yearId       as yearId2,
         'no '        as aMatch,
         count(*)     as n
From     sourceTypeDayVmt
Group by yearId2;

Update tempA as a 
inner join year as c on a.yearId2 = c.yearId
set aMatch='yes';	

Insert into CDB_Checks
       ( TableName,
         CheckNumber,
         TestDescription,
         testValue,
         count,
         yearId  )
Select   "sourceTypeDayVmt" as tableName,
         4001,
        "yearId"            as testDescription,
         yearId2            as testValue,
         n                  as count,
         yearId2
From     tempA
Where    aMatch <> 'yes';

--       check no. 4002 -- check for unknown monthIDs
INSERT INTO QA_Checks_Log values ( 4002, 'OK', @hVersion, curDate(), curTime() );
Drop table if exists tempA;
Create table tempA
Select   monthId      as monthId2,
         'no '        as aMatch,
         count(*)     as n
From     sourceTypeDayVmt
Group by monthId2;

Update tempA as a 
inner join ##defaultdb##.monthOfAnyYear as m on a.monthId2 = m.monthId
set aMatch='yes';

Insert into CDB_Checks
       ( TableName,
         CheckNumber,
         TestDescription,
         testValue,
         count,
         monthId  )
Select   "sourceTypeDayVmt" as tableName,
         4002,
         "monthId"          as testDescription,
         monthId2           as testValue,
         n                  as count,
         monthId2
From     tempA
Where    aMatch <> 'yes';

--       check no. 4003 -- check for unknown dayIDs
INSERT INTO QA_Checks_Log values ( 4003, 'OK', @hVersion, curDate(), curTime() );
Drop table if exists tempA;
Create table tempA
Select   dayId        as dayId2,
         'no '        as aMatch,
         count(*)     as n
From     sourceTypeDayVmt
Group by dayId2;

Update tempA as a 
inner join ##defaultdb##.dayOfAnyWeek as m on a.dayId2 = m.dayId
set aMatch='yes';

Insert into CDB_Checks
       ( TableName,
         CheckNumber,
         TestDescription,
         testValue,
         count,
         dayId  )
Select   "sourceTypeDayVmt" as tableName,
         4003,
         "dayId"            as testDescription,
         dayId2             as testValue,
         n                  as count,
         dayId2
From     tempA
Where    aMatch <> 'yes';

--       check no. 4004 -- check for unknown sourceTypeIDs
INSERT INTO QA_Checks_Log values ( 4004, 'OK', @hVersion, curDate(), curTime() );
Drop table if exists tempA;
Create table tempA
Select   sourceTypeId   as sourceTypeId2,
         'no '          as aMatch,
         count(*)       as n
From     sourceTypeDayVmt
Group by sourceTypeId2;

Update tempA as a 
inner join ##defaultdb##.sourceUseType as m on a.sourceTypeId2 = m.sourceTypeId
set aMatch='yes';	

Insert into CDB_Checks
       ( TableName,
         CheckNumber,
         TestDescription,
         testValue,
         count,
         sourceTypeId  )
Select   "sourceTypeDayVmt" as tableName,
         4004,
        "sourceTypeId"      as testDescription,
         sourceTypeId2      as testValue,
         n                  as count,
         sourceTypeId2
From     tempA
Where    aMatch <> 'yes';

--       check no. 4005: check for missing yearID, monthID, dayID, sourceTypeID combinations
--                       only when sourcetypedayvmt is used
INSERT INTO QA_Checks_Log values ( 4005, 'OK', @hVersion, curDate(), curTime() );
Insert into CDB_Checks
       ( TableName,
         CheckNumber,
         TestDescription,
         yearID, monthID, dayID, sourceTypeID)
Select   "sourcetypedayvmt" as tableName,
         4005,
         'Missing combination of valid yearID, monthID, dayID, sourceTypeID' as testDescription,
		 yearID, monthID, dayID, sourceTypeID
from (
	SELECT yearID, monthID, dayID, sourceTypeID
	FROM  `year`
	CROSS JOIN ##defaultdb##.monthOfAnyYear
	CROSS JOIN ##defaultdb##.dayOfAnyWeek
	CROSS JOIN ##defaultdb##.sourceusetype
) as t1
left join sourcetypedayvmt using (yearID, monthID, dayID, sourceTypeID)
join (select count(*) as n from sourcetypedayvmt) as t2
where VMT is NULL and n > 0
ORDER BY yearID, monthID, dayID, sourceTypeID LIMIT 1;


-- sourceTypeYearVMT
Insert into CDB_Checks (CheckNumber, TableName, TestDescription) values (4100, "sourceTypeYearVMT", "Table Check:");

--       check no. 4101 -- check for unknown yearIDs
INSERT INTO QA_Checks_Log values ( 4101, 'OK', @hVersion, curDate(), curTime() );
Drop table if exists tempA;
Create table tempA
Select   yearId       as yearId2,
         'no '        as aMatch,
         count(*)     as n
From     sourceTypeYearVmt
Group by yearId2;

Update tempA as a 
inner join year as c on a.yearId2 = c.yearId
set aMatch='yes';

Insert into CDB_Checks
       ( TableName,
         CheckNumber,
         TestDescription,
         testValue,
         count,
         yearId  )
Select   "sourceTypeYearVmt" as tableName,
         4101,
        "yearId"             as testDescription,
         yearId2             as testValue,
         n                   as count,
         yearId2
From     tempA
Where    aMatch <> 'yes';

--       check no. 4102 -- check for unknown sourceTypeIDs
INSERT INTO QA_Checks_Log values ( 4102, 'OK', @hVersion, curDate(), curTime() );
Drop table if exists tempA;
Create table tempA
Select   sourceTypeId   as sourceTypeId2,
         'no '          as aMatch,
         count(*)       as n
From     sourceTypeYearVMT
Group by sourceTypeId2;

Update tempA as a 
inner join ##defaultdb##.sourceUseType as m on a.sourceTypeId2 = m.sourceTypeId
set aMatch='yes';	

Insert into CDB_Checks
       ( TableName,
         CheckNumber,
         TestDescription,
         testValue,
         count,
         sourceTypeId  )
Select   "sourceTypeYearVmt" as tableName,
         4102,
        "sourceTypeId"       as testDescription,
         sourceTypeId2       as testValue,
         n                   as count,
         sourceTypeId2
From     tempA
Where    aMatch <> 'yes';

--       check no. 4103: check for missing yearID, sourceTypeID combinations
--                       only when sourcetypeyearvmt is used
INSERT INTO QA_Checks_Log values ( 4103, 'OK', @hVersion, curDate(), curTime() );
Insert into CDB_Checks
       ( TableName,
         CheckNumber,
         TestDescription,
         yearID, sourceTypeID)
Select   "sourcetypeyearvmt" as tableName,
         4103,
         'Missing combination of valid yearID, sourceTypeID' as testDescription,
		 yearID, sourceTypeID
from (
	SELECT yearID, sourceTypeID
	FROM  `year`
	CROSS JOIN ##defaultdb##.sourceusetype
) as t1
left join sourcetypeyearvmt using (yearID, sourceTypeID)
join (select count(*) as n from sourcetypeyearvmt) as t2
where VMT is NULL and n > 0
ORDER BY yearID, sourceTypeID LIMIT 1;


-- SourceTypeYear
Insert into CDB_Checks (CheckNumber, TableName, TestDescription) values (4200, "sourceTypeYear", "Table Check:");

--       check no. 4201 -- check for unknown yearIDs
INSERT INTO QA_Checks_Log values ( 4201, 'OK', @hVersion, curDate(), curTime() );
Drop table if exists tempA;
Create table tempA
Select   yearId       as yearId2,
         'no '        as aMatch,
         count(*)     as n
From     sourceTypeYear
Group by yearId2;

Update tempA as a 
inner join year as c on a.yearId2 = c.yearId
set aMatch='yes';

Insert into CDB_Checks
       ( TableName,
         CheckNumber,
         TestDescription,
         testValue,
         count  )
Select   "sourceTypeYear" as tableName,
         4201,
        "yearId"          as testDescription,
         yearId2          as testValue,
         n                as count
From     tempA
Where    aMatch <> 'yes';

--       check no. 4202 -- check for unknown sourceTypeIDs
INSERT INTO QA_Checks_Log values ( 4202, 'OK', @hVersion, curDate(), curTime() );
Drop table if exists tempA;
Create table tempA
Select   sourceTypeId   as sourceTypeId2,
         'no '          as aMatch,
         count(*)       as n
From     sourceTypeYear
Group by sourceTypeId2;

Update tempA as a 
inner join ##defaultdb##.sourceUseType as m on a.sourceTypeId2 = m.sourceTypeId
set aMatch='yes';

Insert into CDB_Checks
       ( TableName,
         CheckNumber,
         TestDescription,
         testValue,
         count  )
Select   "sourceTypeYear" as tableName,
         4202,
        "sourceTypeId"    as testDescription,
         sourceTypeId2    as testValue,
         n                as count
From     tempA
Where    aMatch <> 'yes';

--       check no. 4203: check for missing yearID and sourceTypeID combinations
INSERT INTO QA_Checks_Log values ( 4203, 'OK', @hVersion, curDate(), curTime() );
Insert into CDB_Checks
       ( TableName,
         CheckNumber,
         TestDescription,
         yearID, sourceTypeID)
Select   "sourcetypeyear" as tableName,
         4203,
         'Missing combination of valid yearID and sourceTypeID' as testDescription,
		 yearID, sourceTypeID
from (
	SELECT yearID, sourceTypeID
	FROM  `year`
	CROSS JOIN ##defaultdb##.sourceusetype
) as t1 left join sourcetypeyear using (yearID, sourceTypeID)
where sourceTypePopulation is NULL 
ORDER BY yearID, sourceTypeID LIMIT 1;

--       check no. 4204 -- if using the HPMSVtypeDay input, check that population summed by HPMS type is <>0 if the VMT is also >0
INSERT INTO QA_Checks_Log values ( 4204, 'OK', @hVersion, curDate(), curTime() );
Drop table if exists tempA;
Create table tempA
Select   sum(sourceTypePopulation) as sourceTypePopulation2,
         a.yearId,
         b.HPMSVtypeId,
         b.VMT,
         'no '                as aMatch,
         count(*)             as n
From     sourceTypeYear as a
Join     ##defaultdb##.sourceUseType  as c on c.sourceTypeId = a.sourceTypeId
Join     HPMSVtypeDay   as b on b.HPMSVtypeId  = c.HPMSVtypeId
where    a.yearId = b.yearId
Group by a.yearId,
         b.HPMSVtypeId;

Update tempA as a set aMatch='yes' where a.VMT > 0 and sourceTypePopulation2 > 0;
Update tempA as a set aMatch='yes' where a.VMT = 0 and sourceTypePopulation2 = 0;

Insert into CDB_Checks
       ( TableName,
         CheckNumber,
         TestDescription,
         testValue,
         count,
         HPMSVtypeId,
         yearId)
Select   "sourceTypeYear or HPMSVtypeDay"     as tableName,
         4204,
        "sourceTypePopulation or VMT is zero" as testDescription,
         VMT                                  as testValue,
         n                                    as count,
         HPMSVTypeId,
         yearId
From     tempA
Where    aMatch <> 'yes';

--       check no. 4205 -- if using the HPMSVtypeYear input, check that population summed by HPMS type is <>0 if the VMT is also >0
INSERT INTO QA_Checks_Log values ( 4205, 'OK', @hVersion, curDate(), curTime() );
Drop table if exists tempA;
Create table tempA
Select   sum(sourceTypePopulation) as sourceTypePopulation2,
         a.yearId,
         b.HPMSVtypeId,
         b.HPMSBaseYearVMT,
         'no '                as aMatch,
         count(*)             as n

From                     sourceTypeYear as a
Join     ##defaultdb##.sourceUseType  as c on c.sourceTypeId = a.sourceTypeId
Join                     hpmsVTypeYear  as b on b.HPMSVtypeId  = c.HPMSVtypeId
where    a.yearId = b.yearId
Group by a.yearId,
         b.HPMSVtypeId;

Update tempA as a set aMatch='yes' where a.HPMSBaseYearVMT > 0 and sourceTypePopulation2 > 0;
Update tempA as a set aMatch='yes' where a.HPMSBaseYearVMT = 0 and sourceTypePopulation2 = 0;

Insert into CDB_Checks
       ( TableName,
         CheckNumber,
         TestDescription,
         testValue,
         count,
         HPMSVtypeId,
         yearId)
Select   "sourceTypeYear or hpmsVTypeYear"                as tableName,
         4205,
        "sourceTypePopulation or HPMSBaseYearVMT is zero" as testDescription,
         HPMSBaseYearVMT                                  as testValue,
         n                                                as count,
         HPMSVTypeId,
         yearId
From     tempA
Where    aMatch <> 'yes';

--       check no. 4206 -- if using the SourceTypeDayVMT input, check that population summed by HPMS type is <>0 if the VMT is also >0
INSERT INTO QA_Checks_Log values ( 4206, 'OK', @hVersion, curDate(), curTime() );
Insert into CDB_Checks
       ( TableName,
         CheckNumber,
         TestDescription,
         testValue,
         count,
         sourceTypeID,
         yearId)
Select   "sourceTypeYear or sourceTypeDayVMT" as tableName,
         4206,
        "sourceTypePopulation or VMT is zero" as testDescription,
         VMT                                  as testValue,
         count(*)                             as count,
         sourceTypeID,
         yearId
From     sourcetypedayvmt
JOIN	 sourcetypeyear using (sourceTypeID, yearID)
Where    (VMT > 0 and sourceTypePopulation = 0) OR
         (VMT = 0 and sourceTypePopulation > 0)
group by sourceTypeID, yearID;
         
--       check no. 4207 -- if using the SourceTypeYearVMT input, check that population summed by HPMS type is <>0 if the VMT is also >0
INSERT INTO QA_Checks_Log values ( 4207, 'OK', @hVersion, curDate(), curTime() );
Insert into CDB_Checks
       ( TableName,
         CheckNumber,
         TestDescription,
         testValue,
         count,
         sourceTypeID,
         yearId)
Select   "sourceTypeYear or sourceTypeYearVMT" as tableName,
         4207,
        "sourceTypePopulation or VMT is zero"  as testDescription,
         VMT                                   as testValue,
         count(*)                              as count,
         sourceTypeID,
         yearId
From     SourceTypeYearVMT
JOIN	 sourcetypeyear using (sourceTypeID, yearID)
Where    (VMT > 0 and sourceTypePopulation = 0) OR
         (VMT = 0 and sourceTypePopulation > 0)
group by sourceTypeID, yearID;


-- starts checks
Insert into CDB_Checks (CheckNumber, TableName, TestDescription) values (4300, "starts", "Table Check:");

--       check no. 4301 -- check for unknown hourDayIDs
INSERT INTO QA_Checks_Log values ( 4301, 'OK', @hVersion, curDate(), curTime() );
Drop table if exists tempA;
Create table tempA
select   hourDayId,
        'no '    as aMatch,
         count(*) as n
from     starts
group by hourDayId;

Update tempA as a 
inner join ##defaultdb##.hourDay as m on a.hourDayId = m.hourDayId
set aMatch='yes';									

Insert into CDB_Checks
       ( TableName,
         CheckNumber,
         TestDescription,
         testValue,
         count  )
Select   "starts"    as tableName,
         4301         as checkNumber,
        "hourDayId"  as testDescription,
         hourDayId   as testValue,
         count(*)    as cou
from     tempA
where    aMatch <> 'yes ';
Delete from CDB_Checks where checkNumber=4301 and count=0;

--       check no. 4302 -- check for unknown monthIDs
INSERT INTO QA_Checks_Log values ( 4302, 'OK', @hVersion, curDate(), curTime() );
Drop table if exists tempA;
Create table tempA
select   monthId,
        'no '    as aMatch,
         count(*) as n
from     starts
group by monthId;

Update tempA as a 
inner join ##defaultdb##.monthOfAnyYear as m on a.monthId = m.monthId
set aMatch='yes';

Insert into CDB_Checks
       ( TableName,
         CheckNumber,
         TestDescription,
         testValue,
         count  )
Select   "starts"       as tableName,
         4302            as checkNumber,
        "monthId"       as testDescription,
         monthId        as testValue,
         count(*)       as cou
from     tempA
where    aMatch <> 'yes ';
Delete from CDB_Checks where checkNumber=4302 and count=0;

--       check no. 4303 -- check for unknown yearIDs
INSERT INTO QA_Checks_Log values ( 4303, 'OK', @hVersion, curDate(), curTime() );
Drop table if exists tempA;
Create table tempA
select   yearId,
        'no '     as aMatch,
         count(*) as n
from     starts
group by yearId;

Update tempA as a 
inner join year as c on a.yearId = c.yearId
set aMatch='yes';	

Insert into CDB_Checks
       ( TableName,
         CheckNumber,
         TestDescription,
         testValue,
         count  )
Select   "starts" as tableName,
         4303      as checkNumber,
        "yearId"  as testDescription,
         yearId   as testValue,
         count(*) as cou
from     tempA
where    aMatch <> 'yes ';
Delete from CDB_Checks where checkNumber=4303 and count=0;

--       check no. 4304 -- check for unknown ageIDs
INSERT INTO QA_Checks_Log values ( 4304, 'OK', @hVersion, curDate(), curTime() );
Drop table if exists tempA;
Create table tempA
select   ageId,
        'no '    as aMatch,
         count(*) as n
from     starts
group by ageId;

Update tempA as a 
inner join ##defaultdb##.ageCategory as m on a.ageId = m.ageId
set aMatch='yes';

Insert into CDB_Checks
       ( TableName,
         CheckNumber,
         TestDescription,
         testValue,
         count  )
Select   "starts"       as tableName,
         4304            as checkNumber,
        "ageId"         as testDescription,
         ageId          as testValue,
         count(*)       as cou
from     tempA
where    aMatch <> 'yes ';
Delete from CDB_Checks where checkNumber=4304 and count=0;

--       check no. 4305 -- check for unknown zoneIDs
INSERT INTO QA_Checks_Log values ( 4305, 'OK', @hVersion, curDate(), curTime() );
Drop table if exists tempA;
Create table tempA
select   zoneId,
        'no '    as aMatch,
         count(*) as n
from     starts
group by zoneId;

Update tempA as a 
inner join zone as c on a.zoneID = c.zoneID
set aMatch='yes';	

Insert into CDB_Checks
       ( TableName,
         CheckNumber,
         TestDescription,
         testValue,
         count  )
Select   "starts" as tableName,
         4305      as checkNumber,
        "zoneId"  as testDescription,
         zoneId   as testValue,
         count(*) as cou
from     tempA
where    aMatch <> 'yes ';
Delete from CDB_Checks where checkNumber=4305 and count=0;

--       check no. 4306 -- check for unknown sourceTypeIDs
INSERT INTO QA_Checks_Log values ( 4306, 'OK', @hVersion, curDate(), curTime() );
Drop table if exists tempA;
Create table tempA
select   sourceTypeId,
        'no '    as aMatch,
         count(*) as n
from     starts
group by sourceTypeId;

Update tempA as a 
inner join ##defaultdb##.sourceUseType as m on a.sourceTypeId = m.sourceTypeId
set aMatch='yes';	

Insert into CDB_Checks
       ( TableName,
         CheckNumber,
         TestDescription,
         testValue,
         count  )
Select   "starts"       as tableName,
         4306            as checkNumber,
        "sourceTypeId"  as testDescription,
         sourceTypeId   as testValue,
         count(*)       as cou
from     tempA
where    aMatch <> 'yes ';
Delete from CDB_Checks where checkNumber=4306 and count=0;

--       check no. 4307 -- check that isUserInput is either "y" or "n"
INSERT INTO QA_Checks_Log values ( 4307, 'OK', @hVersion, curDate(), curTime() );
Drop table if exists tempA;
Create table tempA
Select   isUserInput     as isUserInput2,
         'no '           as aMatch,
         count(*)        as n
From     starts
Group by isUserInput2;

Update tempA as a set aMatch='yes' where isUserInput2 in ('y', 'n', 'Y', 'N');

Insert into CDB_Checks
       ( TableName,
         CheckNumber,
         TestDescription,
         testValue,
         count  )
Select   "starts" as tableName,
         4307,
        "isUserInput not Y or N" as testDescription,
         isUserInput2      as testValue,
         n         as count               --
From     tempA
Where    aMatch <> 'yes';

--       check no. 4308: check for missing hourDayID, monthID, yearID, ageID, zoneID, sourceTypeID combinations
--                       only when starts table is provided
INSERT INTO QA_Checks_Log values ( 4308, 'OK', @hVersion, curDate(), curTime() );
Insert into CDB_Checks
       ( TableName,
         CheckNumber,
         TestDescription,
         testValue,
         hourDayID, monthID, yearID, zoneID, sourceTypeID)
Select   "starts"   as tableName,
         4308                     as checkNumber,
         'Missing combination of valid hourDayID, monthID, yearID, ageID, zoneID, sourceTypeID' as testDescription,
		 concat('ageID: ', ageID) as testValue,
         hourDayID, monthID, yearID, zoneID, sourceTypeID
from (
	SELECT hourDayID, monthID, yearID, ageID, zoneID, sourceTypeID
	FROM  ##defaultdb##.hourday
	CROSS JOIN ##defaultdb##.monthOfAnyYear
	CROSS JOIN `year`
	CROSS JOIN ##defaultdb##.ageCategory
	CROSS JOIN zone
	CROSS JOIN ##defaultdb##.sourceusetype
) as t1 
left join `starts` using (hourDayID, monthID, yearID, ageID, zoneID, sourceTypeID)
join (select count(*) as n from `starts`) as t2
where `starts`.`starts` is NULL and n > 0
ORDER BY hourDayID, monthID, yearID, ageID, zoneID, sourceTypeID LIMIT 1;


-- startsAgeAdjustment checks
Insert into CDB_Checks (CheckNumber, TableName, TestDescription) values (4400, "startsAgeAdjustment", "Table Check:");

--       check no. 4401 -- check for unknown sourceTypeIDs
INSERT INTO QA_Checks_Log values ( 4401, 'OK', @hVersion, curDate(), curTime() );
Drop table if exists tempA;
Create table tempA
select   sourceTypeId,
        'no '    as aMatch,
         count(*) as n
from     startsageadjustment
group by sourceTypeId;

Update tempA as a 
inner join ##defaultdb##.sourceUseType as m on a.sourceTypeId = m.sourceTypeId
set aMatch='yes';	

Insert into CDB_Checks
       ( TableName,
         CheckNumber,
         TestDescription,
         testValue,
         count  )
Select   "startsageadjustment"       as tableName,
         4401            as checkNumber,
        "sourceTypeId"  as testDescription,
         sourceTypeId   as testValue,
         count(*)       as cou
from     tempA
where    aMatch <> 'yes ';
Delete from CDB_Checks where checkNumber=4401 and count=0;

--       check no. 4402 -- check for unknown ageIDs
INSERT INTO QA_Checks_Log values ( 4402, 'OK', @hVersion, curDate(), curTime() );
Drop table if exists tempA;
Create table tempA
select   ageId,
        'no '    as aMatch,
         count(*) as n
from     startsageadjustment
group by ageId;

Update tempA as a 
inner join ##defaultdb##.ageCategory as m on a.ageId = m.ageId
set aMatch='yes';

Insert into CDB_Checks
       ( TableName,
         CheckNumber,
         TestDescription,
         testValue,
         count  )
Select   "startsageadjustment"       as tableName,
         4402            as checkNumber,
        "ageId"         as testDescription,
         ageId          as testValue,
         count(*)       as cou
from     tempA
where    aMatch <> 'yes ';
Delete from CDB_Checks where checkNumber=4402 and count=0;

--       check no. 4403: check for missing sourceTypeID, ageID combinations
--                       only when startsageadjustment table is provided
INSERT INTO QA_Checks_Log values ( 4403, 'OK', @hVersion, curDate(), curTime() );
Insert into CDB_Checks
       ( TableName,
         CheckNumber,
         TestDescription,
         testValue,
         sourceTypeID)
Select   "startsageadjustment"   as tableName,
         4403                     as checkNumber,
         'Missing combination of valid sourceTypeID, ageID' as testDescription,
		 concat('ageID: ', ageID) as testValue,
         sourceTypeID
from (
	SELECT sourceTypeID, ageID
	FROM  ##defaultdb##.sourceusetype
	CROSS JOIN ##defaultdb##.ageCategory
) as t1 
left join startsageadjustment using (sourceTypeID, ageID)
join (select count(*) as n from startsageadjustment) as t2
where ageAdjustment is NULL and n > 0
ORDER BY sourceTypeID, ageID LIMIT 1;


-- startsHourFraction
Insert into CDB_Checks (CheckNumber, TableName, TestDescription) values (4500, "startsHourFraction", "Table Check:");

--       check no. 4501 -- check for unknown dayIDs
INSERT INTO QA_Checks_Log values ( 4501, 'OK', @hVersion, curDate(), curTime() );
Drop table if exists tempA;
Create table tempA
select   dayId,
        'no '     as aMatch,
         count(*) as n
from     startsHourFraction
group by dayId;

Update tempA as a 
inner join ##defaultdb##.dayOfAnyWeek as m on a.dayId = m.dayId
set aMatch='yes';

Insert into CDB_Checks
       ( TableName,
         CheckNumber,
         TestDescription,
         testValue,
         count  )
Select   "startsHourFraction" as tableName,
         4501                  as checkNumber,
        "dayId"               as testDescription,
         dayId                as testValue,
         count(*)             as cou
from     tempA
where    aMatch <> 'yes ';
Delete from CDB_Checks where checkNumber=4501 and count=0;

--       check no. 4502 -- check for unknown hourIDs
INSERT INTO QA_Checks_Log values ( 4502, 'OK', @hVersion, curDate(), curTime() );
Drop table if exists tempA;
Create table tempA
select   hourId,
        'no '     as aMatch,
         count(*) as n
from     startsHourFraction
group by hourId;

Update tempA as a 
inner join ##defaultdb##.hourOfAnyDay as m on a.hourId = m.hourId
set aMatch='yes';

Insert into CDB_Checks
       ( TableName,
         CheckNumber,
         TestDescription,
         testValue,
         count  )
Select   "startsHourFraction" as tableName,
         4502                  as checkNumber,
        "hourId"              as testDescription,
         hourId               as testValue,
         count(*)             as cou
from     tempA
where    aMatch <> 'yes ';
Delete from CDB_Checks where checkNumber=4502 and count=0;

--       check no. 4503 -- check for unknown sourceTypeIDs
INSERT INTO QA_Checks_Log values ( 4503, 'OK', @hVersion, curDate(), curTime() );
Drop table if exists tempA;
Create table tempA
select   sourceTypeId,
        'no '    as aMatch,
         count(*) as n
from     startshourfraction
group by sourceTypeId;

Update tempA as a 
inner join ##defaultdb##.sourceUseType as m on a.sourceTypeId = m.sourceTypeId
set aMatch='yes';	

Insert into CDB_Checks
       ( TableName,
         CheckNumber,
         TestDescription,
         testValue,
         count  )
Select   "startshourfraction"       as tableName,
         4503            as checkNumber,
        "sourceTypeId"  as testDescription,
         sourceTypeId   as testValue,
         count(*)       as cou
from     tempA
where    aMatch <> 'yes ';
Delete from CDB_Checks where checkNumber=4503 and count=0;

--       check no. 4504 -- check that allocationFraction sums to 1 for each sourceTypeID and dayID
INSERT INTO QA_Checks_Log values ( 4504, 'OK', @hVersion, curDate(), curTime() );
Insert into CDB_Checks
       ( tableName,
         checkNumber,
         testDescription,
         testValue,
           sourceTypeId,
           dayID )
Select  "startsHourFraction"               as tableName,
         4504                              as checkNumber,
        "sum of allocationFraction <> 1.0" as testDescription,
         sum(allocationFraction)           as testValue,
           sourceTypeId,
           dayID
From     startshourfraction
Group by sourceTypeId,
         dayID
Having   testValue <0.99999 or testValue >1.00001;

--       check no. 4505 -- check for allocationFraction >= 1
INSERT INTO QA_Checks_Log values ( 4505, 'OK', @hVersion, curDate(), curTime() );
Insert into CDB_Checks
       ( TableName,
         CheckNumber,
         TestDescription,
         testValue,
         dayID, hourID, sourceTypeID)
Select   "startshourfraction" as tableName,
         4505,
         'allocationFraction >= 1' as testDescription,
		 allocationFraction as testValue,
         dayID, hourID, sourceTypeID
from startshourfraction
where allocationFraction >= 1
ORDER BY dayID, hourID, sourceTypeID LIMIT 1;

--       check no. 4506: make sure allocationFraction distributions aren't flat
INSERT INTO QA_Checks_Log values ( 4506, 'OK', @hVersion, curDate(), curTime() );
Insert into CDB_Checks
       ( TableName,
         CheckNumber,
         TestDescription,
         testValue,
         dayID, sourceTypeID)
Select   "startshourfraction" as tableName,
         4506 as checkNumber,
         'allocationFraction is a flat profile' as testDescription,
         concat('all are ', allocationFraction) as testValue,
		 dayID, sourceTypeID
from startshourfraction
group by dayID, sourceTypeID, allocationFraction
having count(*) = (select count(*) from ##defaultdb##.hourOfAnyDay)
order by dayID, sourceTypeID LIMIT 1;

--       check no. 4507: check for missing dayID, hourID, sourceTypeID combinations
--                       only when startshourfraction table is provided
INSERT INTO QA_Checks_Log values ( 4507, 'OK', @hVersion, curDate(), curTime() );
Insert into CDB_Checks
       ( TableName,
         CheckNumber,
         TestDescription,
         dayID, hourID, sourceTypeID)
Select   "startshourfraction"   as tableName,
         4507                     as checkNumber,
         'Missing combination of valid dayID, hourID, sourceTypeID' as testDescription,
         dayID, hourID, sourceTypeID
from (
	SELECT dayID, hourID, sourceTypeID
	FROM  ##defaultdb##.dayOfAnyWeek
	CROSS JOIN ##defaultdb##.hourOfAnyDay
	CROSS JOIN ##defaultdb##.sourceusetype
) as t1 
left join startshourfraction using (dayID, hourID, sourceTypeID)
join (select count(*) as n from startshourfraction) as t2
where allocationFraction is NULL and n > 0
ORDER BY dayID, hourID, sourceTypeID LIMIT 1;


-- startsmonthadjust
Insert into CDB_Checks (CheckNumber, TableName, TestDescription) values (4600, "startsMonthAdjust", "Table Check:");

--       check no. 4601 -- check for unknown monthIDs
INSERT INTO QA_Checks_Log values ( 4601, 'OK', @hVersion, curDate(), curTime() );
Drop table if exists tempA;
Create table tempA
select   monthId,
        'no '     as aMatch,
         count(*) as n
from     startsMonthAdjust
group by monthId;

Update tempA as a 
inner join ##defaultdb##.monthOfAnyYear as m on a.monthId = m.monthId
set aMatch='yes';
	
Insert into CDB_Checks
       ( TableName,
         CheckNumber,
         TestDescription,
         testValue,
         count  )
Select   "startsMonthAdjust" as tableName,
         4601                 as checkNumber,
        "monthId"            as testDescription,
         monthId             as testValue,
         count(*)            as cou
from     tempA
where    aMatch <> 'yes ';
Delete from CDB_Checks where checkNumber=4601 and count=0;

--       check no. 4602 -- check for unknown sourceTypeIDs
INSERT INTO QA_Checks_Log values ( 4602, 'OK', @hVersion, curDate(), curTime() );
Drop table if exists tempA;
Create table tempA
select   sourceTypeId,
        'no '    as aMatch,
         count(*) as n
from     startsmonthadjust
group by sourceTypeId;

Update tempA as a 
inner join ##defaultdb##.sourceUseType as m on a.sourceTypeId = m.sourceTypeId
set aMatch='yes';	

Insert into CDB_Checks
       ( TableName,
         CheckNumber,
         TestDescription,
         testValue,
         count  )
Select   "startsmonthadjust"       as tableName,
         4602            as checkNumber,
        "sourceTypeId"  as testDescription,
         sourceTypeId   as testValue,
         count(*)       as cou
from     tempA
where    aMatch <> 'yes ';
Delete from CDB_Checks where checkNumber=4602 and count=0;

--       check no. 4603: check for missing monthID, sourceTypeID combinations
--                       only when startsmonthadjust table is provided
INSERT INTO QA_Checks_Log values ( 4603, 'OK', @hVersion, curDate(), curTime() );
Insert into CDB_Checks
       ( TableName,
         CheckNumber,
         TestDescription,
         monthID, sourceTypeID)
Select   "startsmonthadjust"   as tableName,
         4603                  as checkNumber,
         'Missing combination of valid monthID, sourceTypeID' as testDescription,
         monthID, sourceTypeID
from (
	SELECT monthID, sourceTypeID
	FROM  ##defaultdb##.monthOfAnyYear
	CROSS JOIN ##defaultdb##.sourceusetype
) as t1 
left join startsmonthadjust using (monthID, sourceTypeID)
join (select count(*) as n from startsmonthadjust) as t2
where monthAdjustment is NULL and n > 0
ORDER BY monthID, sourceTypeID LIMIT 1;


-- startsopmodedistribution
Insert into CDB_Checks (CheckNumber, TableName, TestDescription) values (4700, "startsopmodedistribution", "Table Check:");

--       check no. 4701 -- check for unknown dayIDs
INSERT INTO QA_Checks_Log values ( 4701, 'OK', @hVersion, curDate(), curTime() );
Drop table if exists tempA;
Create table tempA
select   dayID as dayID,
        'no '    as aMatch,
         count(*) as n
from     startsopmodedistribution
group by dayID;

Update tempA as a 
inner join ##defaultdb##.dayOfAnyWeek as m on a.dayId = m.dayId
set aMatch='yes';

Insert into CDB_Checks
       ( TableName,
         CheckNumber,
         TestDescription,
         testValue,
         count  )
Select   "startsopmodedistribution"    as tableName,
         4701         as checkNumber,
        "dayID"  as testDescription,
         dayID   as testValue,
         count(*)    as cou
from     tempA
where    aMatch <> 'yes ';
Delete from CDB_Checks where checkNumber=4701 and count=0;

--       check no. 4702 -- check for unknown hourIDs
INSERT INTO QA_Checks_Log values ( 4302, 'OK', @hVersion, curDate(), curTime() );
Drop table if exists tempA;
Create table tempA
select   hourID,
        'no '    as aMatch,
         count(*) as n
from     startsopmodedistribution
group by hourID;

Update tempA as a 
inner join ##defaultdb##.hourOfAnyDay as m on a.hourId = m.hourId
set aMatch='yes';

Insert into CDB_Checks
       ( TableName,
         CheckNumber,
         TestDescription,
         testValue,
         count  )
Select   "startsopmodedistribution"       as tableName,
         4702            as checkNumber,
        "hourID"       as testDescription,
         hourID        as testValue,
         count(*)       as cou
from     tempA
where    aMatch <> 'yes ';
Delete from CDB_Checks where checkNumber=4702 and count=0;

--       check no. 4703 -- check for unknown sourceTypeIDs
INSERT INTO QA_Checks_Log values ( 4703, 'OK', @hVersion, curDate(), curTime() );
Drop table if exists tempA;
Create table tempA
select   sourceTypeId,
        'no '    as aMatch,
         count(*) as n
from     startsopmodedistribution
group by sourceTypeId;

Update tempA as a 
inner join ##defaultdb##.sourceUseType as m on a.sourceTypeId = m.sourceTypeId
set aMatch='yes';	

Insert into CDB_Checks
       ( TableName,
         CheckNumber,
         TestDescription,
         testValue,
         count  )
Select   "startsopmodedistribution"       as tableName,
         4703            as checkNumber,
        "sourceTypeId"  as testDescription,
         sourceTypeId   as testValue,
         count(*)       as cou
from     tempA
where    aMatch <> 'yes ';
Delete from CDB_Checks where checkNumber=4703 and count=0;

--       check no. 4704 -- check for unknown ageIDs
INSERT INTO QA_Checks_Log values ( 4704, 'OK', @hVersion, curDate(), curTime() );
Drop table if exists tempA;
Create table tempA
select   ageId,
        'no '    as aMatch,
         count(*) as n
from     startsopmodedistribution
group by ageId;

Update tempA as a 
inner join ##defaultdb##.ageCategory as m on a.ageId = m.ageId
set aMatch='yes';

Insert into CDB_Checks
       ( TableName,
         CheckNumber,
         TestDescription,
         testValue,
         count  )
Select   "startsopmodedistribution"       as tableName,
         4704            as checkNumber,
        "ageId"         as testDescription,
         ageId          as testValue,
         count(*)       as cou
from     tempA
where    aMatch <> 'yes ';
Delete from CDB_Checks where checkNumber=4704 and count=0;

--       check no. 4705 -- check for unknown opModeIDs
INSERT INTO QA_Checks_Log values ( 4705, 'OK', @hVersion, curDate(), curTime() );
Drop table if exists tempA;
Create table tempA
select   opModeID,
        'no '    as aMatch,
         count(*) as n
from     startsopmodedistribution
group by opModeID;

Update tempA as a 
inner join ##defaultdb##.operatingmode as m on a.opModeID = m.opModeID
set aMatch='yes';

Insert into CDB_Checks
       ( TableName,
         CheckNumber,
         TestDescription,
         testValue,
         count  )
Select   "startsopmodedistribution"       as tableName,
         4705            as checkNumber,
        "opModeID"  as testDescription,
         opModeID   as testValue,
         count(*)       as cou
from     tempA
where    aMatch <> 'yes ';
Delete from CDB_Checks where checkNumber=4705 and count=0;

--       check no. 4706 -- check that isUserInput is either "y" or "n"
INSERT INTO QA_Checks_Log values ( 4706, 'OK', @hVersion, curDate(), curTime() );
Drop table if exists tempA;
Create table tempA
Select   isUserInput     as isUserInput2,
         'no '           as aMatch,
         count(*)        as n
From     startsopmodedistribution
Group by isUserInput2;

Update tempA as a set aMatch='yes' where isUserInput2 in ('y', 'n', 'Y', 'N');

Insert into CDB_Checks
       ( TableName,
         CheckNumber,
         TestDescription,
         testValue,
         count  )
Select   "startsopmodedistribution" as tableName,
         4706,
        "isUserInput not Y or N" as testDescription,
         isUserInput2      as testValue,
         n         as count               --
From     tempA
Where    aMatch <> 'yes';

--       check no. 4707 -- check that opModeFraction sums to 1 for each dayID, hourID, sourceTypeID, ageID
INSERT INTO QA_Checks_Log values ( 4707, 'OK', @hVersion, curDate(), curTime() );
Insert into CDB_Checks
       ( tableName,
         checkNumber,
         testDescription,
         testValue,
		 dayID, hourID, sourceTypeID )
Select  "startsopmodedistribution"               as tableName,
         4707                              as checkNumber,
        "sum of opModeFraction <> 1.0" as testDescription,
         CONCAT('age ', ageID, ' sums to ', sum(opModeFraction))           as testValue,
		 dayID, hourID, sourceTypeID
From     startsopmodedistribution
Group by dayID, hourID, sourceTypeID, ageID
Having   sum(opModeFraction) <0.99999 or sum(opModeFraction) >1.00001;

--       check no. 4708: check for missing dayID, hourID, sourceTypeID, ageID, opModeID combinations
--                       only when startsopmodedistribution table is provided
INSERT INTO QA_Checks_Log values ( 4708, 'OK', @hVersion, curDate(), curTime() );
Insert into CDB_Checks
       ( TableName,
         CheckNumber,
         TestDescription,
         testValue,
         dayID, hourID, sourceTypeID)
Select   "startsopmodedistribution"   as tableName,
         4708                     as checkNumber,
         'Missing combination of valid dayID, hourID, sourceTypeID, ageID, opModeID' as testDescription,
		 concat('ageID: ', ageID, ', opModeID: ', opModeID) as testValue,
         dayID, hourID, sourceTypeID
from (
	SELECT dayID, hourID, sourceTypeID, ageID, opModeID
	FROM  ##defaultdb##.dayOfAnyWeek
	CROSS JOIN ##defaultdb##.hourOfAnyDay
	CROSS JOIN ##defaultdb##.sourceusetype
	CROSS JOIN ##defaultdb##.agecategory
	CROSS JOIN ##defaultdb##.operatingmode
    where opModeID between 101 and 108
) as t1 
left join startsopmodedistribution using (dayID, hourID, sourceTypeID, ageID, opModeID)
join (select count(*) as n from startsopmodedistribution) as t2
where opModeFraction is NULL and n > 0
ORDER BY dayID, hourID, sourceTypeID, ageID, opModeID LIMIT 1;


-- startsPerDay
Insert into CDB_Checks (CheckNumber, TableName, TestDescription) values (4800, "startsPerDay", "Table Check:");

--       check no. 4801 -- check for unknown dayIDs
INSERT INTO QA_Checks_Log values ( 4801, 'OK', @hVersion, curDate(), curTime() );
Drop table if exists tempA;
Create table tempA
select   dayId,
        'no '     as aMatch,
         count(*) as n
from     startsPerDay
group by dayId;

Update tempA as a 
inner join ##defaultdb##.dayOfAnyWeek as m on a.dayId = m.dayId
set aMatch='yes';

Insert into CDB_Checks
       ( TableName,
         CheckNumber,
         TestDescription,
         testValue,
         count  )
Select   "startsPerDay" as tableName,
         4801            as checkNumber,
        "dayId"         as testDescription,
         dayId          as testValue,
         count(*)       as cou
from     tempA
where    aMatch <> 'yes ';
Delete from CDB_Checks where checkNumber=4801 and count=0;

--       check no. 4802 -- check for unknown sourceTypeIDs
INSERT INTO QA_Checks_Log values ( 4802, 'OK', @hVersion, curDate(), curTime() );
Drop table if exists tempA;
Create table tempA
select   sourceTypeId,
        'no '    as aMatch,
         count(*) as n
from     startsperday
group by sourceTypeId;

Update tempA as a 
inner join ##defaultdb##.sourceUseType as m on a.sourceTypeId = m.sourceTypeId
set aMatch='yes';

Insert into CDB_Checks
       ( TableName,
         CheckNumber,
         TestDescription,
         testValue,
         count  )
Select   "startsperday"       as tableName,
         4802            as checkNumber,
        "sourceTypeId"  as testDescription,
         sourceTypeId   as testValue,
         count(*)       as cou
from     tempA
where    aMatch <> 'yes ';
Delete from CDB_Checks where checkNumber=4802 and count=0;

--       check no. 4803: check for missing dayID, sourceTypeID combinations
--                       only when startsperday table is provided
INSERT INTO QA_Checks_Log values ( 4803, 'OK', @hVersion, curDate(), curTime() );
Insert into CDB_Checks
       ( TableName,
         CheckNumber,
         TestDescription,
         dayID, sourceTypeID)
Select   "startsperday"   as tableName,
         4803                  as checkNumber,
         'Missing combination of valid dayID, sourceTypeID' as testDescription,
         dayID, sourceTypeID
from (
	SELECT dayID, sourceTypeID
	FROM  ##defaultdb##.dayOfAnyWeek
	CROSS JOIN ##defaultdb##.sourceusetype
) as t1 
left join startsperday using (dayID, sourceTypeID)
join (select count(*) as n from startsperday) as t2
where startsPerDay.startsPerDay is NULL and n > 0
ORDER BY dayID, sourceTypeID LIMIT 1;


-- startsPerDayPerVehicle
Insert into CDB_Checks (CheckNumber, TableName, TestDescription) values (4900, "startsPerDayPerVehicle", "Table Check:");

--       check no. 4901 -- check for unknown dayIDs
INSERT INTO QA_Checks_Log values ( 4901, 'OK', @hVersion, curDate(), curTime() );
Drop table if exists tempA;
Create table tempA
select   dayId,
        'no '     as aMatch,
         count(*) as n
from     startsPerDayPerVehicle
group by dayId;

Update tempA as a 
inner join ##defaultdb##.dayOfAnyWeek as m on a.dayId = m.dayId
set aMatch='yes';


Insert into CDB_Checks
       ( TableName,
         CheckNumber,
         TestDescription,
         testValue,
         count  )
Select   "startsPerDayPerVehicle" as tableName,
         4901            as checkNumber,
        "dayId"         as testDescription,
         dayId          as testValue,
         count(*)       as cou
from     tempA
where    aMatch <> 'yes ';
Delete from CDB_Checks where checkNumber=4901 and count=0;

--       check no. 4902 -- check for unknown sourceTypeIDs
INSERT INTO QA_Checks_Log values ( 4902, 'OK', @hVersion, curDate(), curTime() );
Drop table if exists tempA;
Create table tempA
select   sourceTypeId,
        'no '    as aMatch,
         count(*) as n
from     startsPerDayPerVehicle
group by sourceTypeId;

Update tempA as a 
inner join ##defaultdb##.sourceUseType as m on a.sourceTypeId = m.sourceTypeId
set aMatch='yes';

Insert into CDB_Checks
       ( TableName,
         CheckNumber,
         TestDescription,
         testValue,
         count  )
Select   "startsPerDayPerVehicle"       as tableName,
         4902            as checkNumber,
        "sourceTypeId"  as testDescription,
         sourceTypeId   as testValue,
         count(*)       as cou
from     tempA
where    aMatch <> 'yes ';
Delete from CDB_Checks where checkNumber=4902 and count=0;

--       check no. 4903: check for missing dayID, sourceTypeID combinations
--                       only when startsPerDayPerVehicle table is provided
INSERT INTO QA_Checks_Log values ( 4903, 'OK', @hVersion, curDate(), curTime() );
Insert into CDB_Checks
       ( TableName,
         CheckNumber,
         TestDescription,
         dayID, sourceTypeID)
Select   "startsPerDayPerVehicle"   as tableName,
         4903                  as checkNumber,
         'Missing combination of valid dayID, sourceTypeID' as testDescription,
         dayID, sourceTypeID
from (
	SELECT dayID, sourceTypeID
	FROM  ##defaultdb##.dayOfAnyWeek
	CROSS JOIN ##defaultdb##.sourceusetype
) as t1 
left join startsPerDayPerVehicle using (dayID, sourceTypeID)
join (select count(*) as n from startsPerDayPerVehicle) as t2
where startsPerDayPerVehicle.startsPerDayPerVehicle is NULL and n > 0
ORDER BY dayID, sourceTypeID LIMIT 1;


-- zoneMonthHour
Insert into CDB_Checks (CheckNumber, TableName, TestDescription) values (5000, "zoneMonthHour", "Table Check:");

--       check no. 5001 -- check for unknown hourIDs
INSERT INTO QA_Checks_Log values ( 5001, 'OK', @hVersion, curDate(), curTime() );
Drop table if exists tempA;
Create table tempA
Select   hourId    as hourId2,
         'no '     as aMatch,
         count(*)  as n
From     zoneMonthHour
Group by hourId2;

Update tempA as a 
inner join ##defaultdb##.hourOfAnyDay as m on a.hourId2 = m.hourId
set aMatch='yes';

Insert into CDB_Checks
       ( TableName,
         CheckNumber,
         TestDescription,
         testValue,
         count  )
Select   "zoneMonthHour" as tableName,
         5001,
         "hourId"        as testDescription,
         hourId2         as testValue,
         n               as count               --
From     tempA
Where    aMatch <> 'yes';

--       check no. 5002 -- check for unknown monthIDs
INSERT INTO QA_Checks_Log values ( 5002, 'OK', @hVersion, curDate(), curTime() );
Drop table if exists tempA;
Create table tempA
Select   monthId      as monthId2,
         'no '        as aMatch,
         count(*)     as n
From     zoneMonthHour
Group by monthId2;

Update tempA as a 
inner join ##defaultdb##.monthOfAnyYear as m on a.monthId2 = m.monthId
set aMatch='yes';
	
Insert into CDB_Checks
       ( TableName,
         CheckNumber,
         TestDescription,
         testValue,
         count  )
Select   "zoneMonthHour"    as tableName,
         5002,
        "monthId"           as testDescription,
         monthId2           as testValue,
         n                  as count               --
From     tempA
Where    aMatch <> 'yes';

--       check no. 5003 -- check for unknown zoneIDs
INSERT INTO QA_Checks_Log values ( 5003, 'OK', @hVersion, curDate(), curTime() );
Drop table if exists tempA;
Create table tempA
Select   zoneId          as zoneId2,
         'no '           as aMatch,
         count(*)        as n
From     zonemonthhour
Group by zoneId2;

Update tempA as a 
inner join zone as c on a.zoneID2 = c.zoneID
set aMatch='yes';	

Insert into CDB_Checks
       ( TableName,
         CheckNumber,
         TestDescription,
         testValue,
         count  )
Select   "zoneMonthHour" as tableName,
         5003,
        "zoneId"         as testDescription,
         zoneId2         as testValue,
         n               as count               --
From     tempA
Where    aMatch <> 'yes';

--       check no. 5004 -- check that the temperature is between -80 and 150
INSERT INTO QA_Checks_Log values ( 5004, 'OK', @hVersion, curDate(), curTime() );
Drop table if exists tempA;
Create table tempA
Select   temperature  as temperature2,
         'no '        as aMatch,
         count(*)     as n
From     zoneMonthHour
Group by temperature2;

Update tempA as a set aMatch='yes' where temperature2>=-80.0 and temperature2<=150.0;

Insert into CDB_Checks
       ( TableName,
         CheckNumber,
         TestDescription,
         testValue,
         count  )
Select   "zoneMonthHour"    as tableName,
         5004,
        "temperature"       as testDescription,
         temperature2       as testValue,
         n                  as count               --
From     tempA
Where    aMatch <> 'yes';

--       check no. 5005 -- check that the humidity is between 0 and 100
INSERT INTO QA_Checks_Log values ( 5005, 'OK', @hVersion, curDate(), curTime() );
Drop table if exists tempA;
Create table tempA
Select   relHumidity  as relHumidity2,
         'no '        as aMatch,
         count(*)     as n
From     zoneMonthHour
Group by relHumidity2;

Update tempA as a set aMatch='yes' where relHumidity2>=0.0 and relHumidity2<=100.0;

Insert into CDB_Checks
       ( TableName,
         CheckNumber,
         TestDescription,
         testValue,
         count  )
Select   "zoneMonthHour"    as tableName,
         5005,
        "relHumidity"       as testDescription,
         relHumidity2       as testValue,
         n                  as count               --
From     tempA
Where    aMatch <> 'yes';

--       check no. 5006: check for missing monthID, zoneID, hourID combinations
INSERT INTO QA_Checks_Log values ( 5006, 'OK', @hVersion, curDate(), curTime() );
Insert into CDB_Checks
       ( TableName,
         CheckNumber,
         TestDescription,
         monthID, zoneID, hourID)
Select   "zonemonthhour" as tableName,
         5006,
         'Missing combination of valid monthID, zoneID, hourID' as testDescription,
		 monthID, zoneID, hourID
from (
	SELECT monthID, zoneID, hourID
	FROM  ##defaultdb##.monthOfAnyYear
	CROSS JOIN zone
	CROSS JOIN ##defaultdb##.hourOfAnyDay
) as t1
left join zonemonthhour using (monthID, zoneID, hourID)
join (select count(*) as n from zonemonthhour) as t2
where (temperature is NULL or relHumidity is NULL) and n > 0
ORDER BY monthID, zoneID, hourID LIMIT 1;


-- zoneRoadType
Insert into CDB_Checks (CheckNumber, TableName, TestDescription) values (5100, "zoneRoadType", "Table Check:");

--       check no. 5101 -- check for unknown roadTypeIDs
INSERT INTO QA_Checks_Log values ( 5101, 'OK', @hVersion, curDate(), curTime() );
Drop table if exists tempA;
Create table tempA
Select   roadTypeId as roadTypeId2,
         'no '      as aMatch,
         count(*)   as n
From     zoneRoadtype
Group by roadTypeId2;

Update tempA as a 
inner join ##defaultdb##.roadType as m on a.roadTypeId2 = m.roadTypeId
set aMatch='yes';

Insert into CDB_Checks
       ( TableName,
         CheckNumber,
         TestDescription,
         testValue,
         count  )
Select   "zoneRoadType" as tableName,
         5101,
        "roadTypeId"            as testDescription,
         roadTypeId2            as testValue,
         n                      as count
From     tempA
Where    aMatch <> 'yes';

--       check no. 5102 -- check for unknown zoneIDs
INSERT INTO QA_Checks_Log values ( 5102, 'OK', @hVersion, curDate(), curTime() );
Drop table if exists tempA;
Create table tempA
Select   zoneId          as zoneId2,
         'no '           as aMatch,
         count(*)        as n
From     zoneroadtype
Group by zoneId2;

Update tempA as a 
inner join zone as c on a.zoneID2 = c.zoneID
set aMatch='yes';	

Insert into CDB_Checks
       ( TableName,
         CheckNumber,
         TestDescription,
         testValue,
         count  )
Select   "zoneRoadType" as tableName,
         5102,
        "zoneId"        as testDescription,
         zoneId2        as testValue,
         n              as count               --
From     tempA
Where    aMatch <> 'yes';

--       check no. 5103 -- check that the SHOAllocFactor sums to 1 for each road type
--                         No tolerance on this one because there should only be one 
--                         row per road type, so no floating point issues here
INSERT INTO QA_Checks_Log values ( 5103, 'OK', @hVersion, curDate(), curTime() );
Insert into CDB_Checks
       ( TableName,
         CheckNumber,
         TestDescription,
         testValue,
         count,
         roadTypeID)
Select  "zoneRoadType"       as tableName,
         5103,
        "SHOAllocFactor should be 1"         as testDescription,
         sum(SHOAllocFactor) as SHOAllocFactor2,
         count(*) as `count`,
         roadTypeID
From     zoneroadtype
Group by roadTypeId
Having   SHOAllocFactor2 <> 1 or `count` > 1;
 
--       check no. 5104: check for missing zoneID, roadTypeID combinations
INSERT INTO QA_Checks_Log values ( 5104, 'OK', @hVersion, curDate(), curTime() );
Insert into CDB_Checks
       ( TableName,
         CheckNumber,
         TestDescription,
         zoneID, roadTypeID)
Select   "zoneroadtype" as tableName,
         5104,
         'Missing combination of valid zoneID, roadTypeID' as testDescription,
		 zoneID, roadTypeID
from (
	SELECT zoneID, roadTypeID
	FROM  zone
	CROSS JOIN ##defaultdb##.roadtype where roadTypeID in (2, 3, 4, 5)
) as t1 left join zoneroadtype using (zoneID, roadTypeID)
where SHOAllocFactor is NULL 
ORDER BY zoneID, roadTypeID LIMIT 1;
-- ##############################################################################
-- End data QA checks
-- ##############################################################################


-- ##############################################################################
-- Final steps
-- ##############################################################################
--       Clean up the results table:
Delete from   CDB_Checks where TableName = "ALL";
Insert into   CDB_Checks ( TableName, TestDescription ) values   ("All Tables",     "Tables Checked.");
Update        CDB_Checks set msgType = 'Info'   where testDescription = 'Table Check:';
Update        CDB_Checks set msgType = 'Data Problem' where checkNumber is not null and msgType is null;
Update        CDB_Checks set msgType = 'DB Checked' where tableName = 'ALL';
Update        CDB_Checks set msgDate = curDate();
Update        CDB_Checks set msgTime = curTime();
Update        CDB_Checks set dataBaseName = database();
Update        CDB_Checks set countyId = (select min(countyId) from county);

-- set Status to 'Completed' by default (this gets overwritten in the cases outlined below)
Update        CDB_Checks set status = 'Completed' where `status` is NULL or `status` = '';

-- set Status to 'Error' for most checks
Update        CDB_Checks set status = 'Error'   where checkNumber is not null and testDescription <> 'Table Check:';

-- set Status to 'Warning' for select checks (these are typically distribution checks, as well as tables that give warnings
--                                            if you supply them: fuelformulation, fuelsupply, and zonemonthhour)
Update        CDB_Checks set status = 'Warning' where checkNumber in 
			  (1608,1609,1610,1611,1807,2001,2002,2003,2004,2005,2006,2007,2008,2009,2010,2011,2012,2101,
			   2102,2103,2104,2406,2507,2508,2808,2809,3605,3805,3906,4506,5001,5002,5003,5004,5005,5006);

-- special cases for VMT/starts checks --
-- mark all VMT row count checks as 'complete' regardless of results
Update        CDB_Checks set status = 'Completed' where checkNumber in (1002, 1003, 1004, 1005);
-- mark all starts row count checks as 'complete' regardless of results
Update        CDB_Checks set status = 'Completed' where checkNumber in (1007, 1008, 1009);

-- order the results
alter table CDB_Checks order by checkNumber, tableName;

-- Eliminate temporary tables from the CDB.
Drop table if exists tempA;
Drop table if exists tempB;
call emptyTableCleanUp();

-- Version is set at beginning of the file.
Update CDB_Checks set version = @version;

-- Create a database to hold all results from a batch file run.
create Database if not exists All_CDB_Checks;

-- Create a table in the database to hold all results from a batch file run.
create Table if not exists All_CDB_Checks.All_CDB_Checks like CDB_Checks;

-- Copy the results from this run into the table holding all results from a batch run.
insert into All_CDB_Checks.All_CDB_Checks select * from CDB_Checks;

-- Clear all connections to the database tables.
flush tables;

-- Eliminate the stored procedures generated by this script.
drop procedure if exists checkImCoverage;
drop procedure if exists checkHotellingActivityDistribution;
drop procedure if exists checkIdleModelYearGrouping;
drop procedure if exists checkTotalIdleFraction;
drop procedure if exists emptyTableCleanUp;

-- drop final tables
drop table CDB_Checks;
drop table QA_Checks_Log;

select '  .. M3onroadCDBchecks.sql Done',curTime(),database();

-- done.
