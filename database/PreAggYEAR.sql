/* ***********************************************************************************************************************
-- mysql script file to preaggregate the movesexecution database
--    from the month level database to the year level
-- an attempt is made to weight some aggregations by activity.
--
-- version 2017-09-29
--
-- written by mitch cumberworth, april, 2004
-- change history:
--   corrected by mitch cumberworth, june 24, 2004
--   fuel supply calculation fixed by mitch cumberworth, october 28, 2004
--   monthgrouphour calculation fixed by mitch cumberworth, december 2, 2004
--   adapted by mitch cumberworth, june, 2005 for task 207
--   adapted by mitch cumberworth, july, 2005 for task 208
--   adapted by mitch cumberworth, november, 2005 for task 210
--   modified by cimulus, january, 2005 for task 210
--   modified by wfaler, dec, 2007 for task 804, changed "ENTIRE YEAR" TO "WHOLE YEAR" in order to fit within 10 characters
--   modified by gwo shyu, march 26, 2008 to fix the errors of duplicate entry and table existing
--   modified by wes faler, june 15, 2009 for task 912 fuel adjustments
-- *********************************************************************************************************************** */

drop table if exists monthweighting;
drop table if exists monthgroupweighting;
drop table if exists aggdayvmtfraction;
drop table if exists aggmonthvmtfraction;
drop table if exists aggsourcetypedayvmt;
drop table if exists agghpmsvtypeday;
drop table if exists oldsho;
drop table if exists oldsourcehours;
drop table if exists oldstarts;
drop table if exists oldstartsmonthadjust;
drop table if exists oldextendedidlehours;
drop table if exists oldhotellingmonthadjust;
drop table if exists aggzonemonthhour;
drop table if exists aggmonthgrouphour;
drop table if exists aggfuelsupply;
drop table if exists oldaveragetanktemperature;
drop table if exists oldsoakactivityfraction;
drop table if exists aggatbaseemissions;
drop table if exists aggatratio;
drop table if exists oldtotalidlefraction;

--todo: monthweighting only uses sourcetype 21; there should probably be a monthweightingbysourcetype for 
--      pre-agg tables that need it by source type (like sourcetypevmt and totalidlefraction)

--
-- create monthweightings to be used to weight monthly activity 
-- 
-- note:  these weightings will not sum to unity if all months are not included
-- select "MAKING MONTHWEIGHTING" as marker_point;
-- create table explicitly to control column types and avoid significance problems
create table monthweighting (
  monthid smallint,
  actfract float);
insert into monthweighting
  select monthid, monthvmtfraction as actfract
    from monthvmtfraction 
  where sourcetypeid=21;
create unique index index1 on monthweighting (monthid);

-- select "MAKING MONTHGROUPWEIGHTING" as marker_point;
-- create table explicitly to control column types and avoid significance problems
create table monthgroupweighting (
  monthgroupid smallint,
  actfract float);
insert into monthgroupweighting
  select monthgroupid, sum(monthvmtfraction) as actfract
    from monthvmtfraction inner join monthofanyyear using (monthid)
  where sourcetypeid=21
  group by monthgroupid;
create unique index index1 on monthgroupweighting (monthgroupid);

--
-- monthofanyyear table
--
-- select "MAKING MONTHOFANYYEAR" as marker_point;
truncate monthofanyyear;
insert into monthofanyyear (monthid, monthname, noofdays, monthgroupid)
  values (0, "Whole Year", 365, 0);
flush table monthofanyyear;
  
--
-- monthgroupofanyyear table
--
-- select "MAKING MONTHGROUPOFANYYEAR" as marker_point;
truncate monthgroupofanyyear;
insert into monthgroupofanyyear (monthgroupid, monthgroupname)
  values (0, "Whole Year");  
flush table monthgroupofanyyear;
  
--
-- dayvmtfraction table
--
-- select "MAKING DAYVMTFRACTION" as marker_point;
create table aggdayvmtfraction 
  select sourcetypeid, roadtypeid
    from dayvmtfraction 
    group by sourcetypeid, roadtypeid;
truncate dayvmtfraction;
replace into dayvmtfraction (sourcetypeid, monthid, roadtypeid, dayid, dayvmtfraction)
  select sourcetypeid, 0 as monthid, roadtypeid, 0 as dayid, 1.0 as dayvmtfraction
  from aggdayvmtfraction;
flush table dayvmtfraction;
  
--
-- monthvmtfraction table
--
-- select "MAKING MONTHVMTFRACTION" as marker_point;
create table aggmonthvmtfraction 
  select sourcetypeid 
    from monthvmtfraction 
    group by sourcetypeid;
truncate monthvmtfraction;
replace into monthvmtfraction (sourcetypeid, monthid, monthvmtfraction)
  select sourcetypeid, 0 as monthid, 1.0 as monthvmtfraction
  from aggmonthvmtfraction;  
flush table monthvmtfraction;

--
-- sourcetypedayvmt table
--
-- select "MAKING SOURCETYPEDAYVMT" as marker_point;
create table aggsourcetypedayvmt 
  select yearid, 0 as monthid, 0 as dayid, sourcetypeid, sum(vmt*actfract) as vmt
    from sourcetypedayvmt
    inner join monthweighting using (monthid)
    group by yearid, sourcetypeid;
truncate sourcetypedayvmt;
replace into sourcetypedayvmt (yearid, monthid, dayid, sourcetypeid, vmt)
  select yearid, monthid, dayid, sourcetypeid, vmt
  from aggsourcetypedayvmt;
flush table sourcetypedayvmt;

--
-- hpmsvtypeday table
--
-- select "MAKING HPMSVTYPEDAY" as marker_point;
create table agghpmsvtypeday 
  select yearid, 0 as monthid, 0 as dayid, hpmsvtypeid, sum(vmt*actfract) as vmt
    from hpmsvtypeday
    inner join monthweighting using (monthid)
    group by yearid, hpmsvtypeid;
truncate hpmsvtypeday;
replace into hpmsvtypeday (yearid, monthid, dayid, hpmsvtypeid, vmt)
  select yearid, monthid, dayid, hpmsvtypeid, vmt
  from agghpmsvtypeday;
flush table hpmsvtypeday;

--
--  sho    
--
-- select "MAKING SHO" as marker_point;
create table oldsho
  select monthid, yearid, ageid, linkid, sourcetypeid, sho, distance 
    from sho ;
create index index1 on oldsho (yearid, ageid, linkid, sourcetypeid);
truncate sho;
replace into sho (hourdayid, monthid, yearid, ageid, linkid, 
    sourcetypeid, sho, shocv, distance, isuserinput)
  select 0 as hourdayid, 0 as monthid, yearid, ageid, linkid, sourcetypeid, 
    sum(sho) as sho, null as shocv, sum(distance) as distance, "Y" as isuserinput
  from oldsho 
  group by yearid, ageid, linkid, sourcetypeid;
flush table sho;

--
--  sourcehours   
--
-- select "MAKING SOURCEHOURS" as marker_point;
create table oldsourcehours
  select monthid, yearid, ageid, linkid, sourcetypeid, sourcehours
    from sourcehours ;
create index index1 on oldsourcehours (yearid, ageid, linkid, sourcetypeid);
truncate sourcehours;
replace into sourcehours (hourdayid, monthid, yearid, ageid, linkid, 
    sourcetypeid, sourcehours, sourcehourscv, isuserinput)
  select 0 as hourdayid, 0 as monthid, yearid, ageid, linkid, sourcetypeid, 
    sum(sourcehours) as sourcehours, null as sourcehourscv, "Y" as isuserinput
  from oldsourcehours 
  group by yearid, ageid, linkid, sourcetypeid;
flush table sourcehours;

--
--  starts
--
-- select "MAKING STARTS" as marker_point;
create table oldstarts
  select monthid, yearid, ageid, zoneid, sourcetypeid, starts
  from starts ;
create index index1 on oldstarts (yearid, ageid, zoneid, sourcetypeid);
truncate starts;
replace into starts (hourdayid, monthid, yearid, ageid, zoneid, 
    sourcetypeid, starts, startscv, isuserinput)
  select 0 as hourdayid, 0 as monthid, yearid, ageid, zoneid, sourcetypeid, 
    sum(starts) as starts, null as startscv, "Y" as isuserinput
  from oldstarts
  group by yearid, ageid, zoneid, sourcetypeid;
flush table starts;

-- startsmonthadjust
-- 
-- select "MAKING STARTSMONTHADJUST" as marker_point;
create table oldstartsmonthadjust
  select * from startsmonthadjust;
truncate startsmonthadjust;
insert into startsmonthadjust (monthid, sourcetypeid, monthadjustment)
  select 0 as monthid, sourcetypeid, 1 as monthadjustment
  from oldstartsmonthadjust
  group by sourcetypeid;
flush table startsmonthadjust;


/* please read comment:

year pre-agg doesn't work like normal pre-agg for hotelling, becuase none of the hotelling 
input tables (hotellinghourfraction, hotellingagefraction, hotellinghoursperday) vary
by month. so there's nothing to do. however, moves automatically multiplies activity by 
12 for year input/output which must be the case when this year pre-agg script runs. the 
only mechanism we have to re-scale hotelling activty to look like a typical month is
to use the hotellingmonthadjust table, even though the value should, in theory, always be 1.
*/

-- hotellingmonthadjust
-- 
-- select "MAKING HOTELLINGMONTHADJUST" as marker_point;
create table oldhotellingmonthadjust
  select * from hotellingmonthadjust;
truncate hotellingmonthadjust;
insert into hotellingmonthadjust (zoneid, monthid, monthadjustment)
  select zoneid, 0 as monthid, 1/12 as monthadjustment
  from oldhotellingmonthadjust
  group by zoneid;
flush table hotellingmonthadjust;
  
-- 
-- zonemonthhour
--
-- select "MAKING ZONEMONTHHOUR" as marker_point;
-- explicit creation of intermediate file found necessary to avoid significance problems
create table aggzonemonthhour (
  zoneid integer,
  temperature float,
  relhumidity float);
insert into aggzonemonthhour (zoneid,temperature,relhumidity)
  select zoneid, 
    (sum(temperature*actfract)/sum(actfract)) as temperature,
    (sum(relhumidity*actfract)/sum(actfract)) as relhumidity
  from zonemonthhour inner join monthweighting using (monthid)
  group by zoneid;
truncate zonemonthhour;
replace into zonemonthhour (monthid, zoneid, hourid, temperature, temperaturecv,
    relhumidity, relativehumiditycv, heatindex, specifichumidity)
  select 0 as monthid, zoneid, 0 as hourid, temperature,
    null as temperaturecv, relhumidity, null as relativehumiditycv,
    0.0 as heatindex, 0.0 as specifichumidity 
  from aggzonemonthhour;

flush table zonemonthhour;

-- 
-- monthgrouphour
--
-- select "MAKING MONTHGROUPHOUR" as marker_point;
-- explicit creation of intermediate file found necessary to avoid significance problems
create table aggmonthgrouphour (
  acactivityterma float,
  acactivitytermb float,
  acactivitytermc float);
insert into aggmonthgrouphour (acactivityterma,acactivitytermb,acactivitytermc)
  select 
    (sum(acactivityterma*actfract)/sum(actfract)) as acactivityterma,
    (sum(acactivitytermb*actfract)/sum(actfract)) as acactivitytermb,
    (sum(acactivitytermc*actfract)/sum(actfract)) as acactivitytermc
  from monthgrouphour as mgh inner join monthgroupweighting using (monthgroupid);
truncate monthgrouphour;
replace into monthgrouphour (monthgroupid, hourid,
  acactivityterma, acactivitytermacv, 
  acactivitytermb, acactivitytermbcv, 
  acactivitytermc, acactivitytermccv 
  )
  select 0 as monthgroupid, 0 as hourid,
    acactivityterma, null as acactivitytermacv,
    acactivitytermb, null as acactivitytermbcv,
    acactivitytermc, null as acactivitytermccv
  from aggmonthgrouphour; 

flush table monthgrouphour;

create table aggatratio (
  fueltypeid int not null,
  fuelformulationid int not null,
  polprocessid int not null,
  minmodelyearid int not null,
  maxmodelyearid int not null,
  ageid int not null,
  atratio double null
);
insert into aggatratio (fueltypeid, fuelformulationid, polprocessid, minmodelyearid, maxmodelyearid, ageid, 
    atratio)
select fueltypeid, fuelformulationid, polprocessid, minmodelyearid, maxmodelyearid, ageid, 
    (sum(atratio*actfract)/sum(actfract)) as atratio
from atratio
inner join monthgroupweighting using (monthgroupid)
group by fueltypeid, fuelformulationid, polprocessid, minmodelyearid, maxmodelyearid, ageid;
truncate atratio;
insert into atratio (fueltypeid, fuelformulationid, polprocessid, minmodelyearid, maxmodelyearid,
    ageid, monthgroupid, atratio)
select fueltypeid, fuelformulationid, polprocessid, minmodelyearid, maxmodelyearid,
  ageid, 0 as monthgroupid, atratio
from aggatratio;

create table aggatbaseemissions 
(
  polprocessid      int   not null  default '0',
  atbaseemissions     float not null  default '0',
  primary key (polprocessid)
);
insert into aggatbaseemissions (polprocessid, atbaseemissions)
select polprocessid, (sum(atbaseemissions*actfract)/sum(actfract)) as atbaseemissions
from atbaseemissions
inner join monthgroupweighting using (monthgroupid)
group by polprocessid;
truncate atbaseemissions;
insert into atbaseemissions (polprocessid, monthgroupid, atbaseemissions, datasourceid)
select polprocessid, 0 as monthgroupid, atbaseemissions, 0 as datasourceid
from aggatbaseemissions;

--
-- fuel supply
--
-- note: algorithm is specific to particular default values used.
-- select "MAKING AGGREGATE FUELSUPPLY TABLE" as marker_point;
-- creating table explicitly to control column type and avoid significance problem
create table aggfuelsupply (
  fuelregionid integer,
  fuelyearid smallint,
  fuelformulationid smallint,
  havefract float,
  fractdonthave float);
insert into aggfuelsupply
  select fuelregionid, fuelyearid, fuelformulationid, 
    (sum(marketshare*actfract)/sum(actfract)) as havefract,
    (1.0 - sum(actfract)) as fractdonthave
  from fuelsupply inner join monthgroupweighting using(monthgroupid)
  group by fuelregionid, fuelyearid, fuelformulationid;
truncate fuelsupply;  
replace into fuelsupply (fuelregionid, fuelyearid, monthgroupid, fuelformulationid, 
   marketshare, marketsharecv)
  select  fuelregionid, fuelyearid, 0 as monthgroupid, fuelformulationid, 
    ((1.0-fractdonthave)*havefract) as marketshare,
    null as marketsharecv
  from aggfuelsupply;

flush table fuelsupply;


--
-- e10 fuel properties
--
-- this table already includes fuelregionid 0 and is by month, so the only aggregation that could be needed is
-- month to year. weight using the vmt activity like the other tables.
insert ignore into e10fuelproperties (fuelregionid,fuelyearid,monthgroupid,rvp,sulfurlevel,etohvolume,mtbevolume,
                                      etbevolume,tamevolume,aromaticcontent,olefincontent,benzenecontent,e200,e300,
                    biodieselestervolume,cetaneindex,pahcontent,t50,t90)
  select fuelregionid, fuelyearid,
       0 as monthgroupid, 
       sum(rvp*actfract)/sum(actfract) as rvp,
       sum(sulfurlevel*actfract)/sum(actfract) as sulfurlevel,
       sum(etohvolume*actfract)/sum(actfract) as etohvolume,
       sum(mtbevolume*actfract)/sum(actfract) as mtbevolume,
       sum(etbevolume*actfract)/sum(actfract) as etbevolume,
       sum(tamevolume*actfract)/sum(actfract) as tamevolume,
       sum(aromaticcontent*actfract)/sum(actfract) as aromaticcontent,
       sum(olefincontent*actfract)/sum(actfract) as olefincontent,
       sum(benzenecontent*actfract)/sum(actfract) as benzenecontent,
       sum(e200*actfract)/sum(actfract) as e200,
       sum(e300*actfract)/sum(actfract) as e300,
       sum(biodieselestervolume*actfract)/sum(actfract) as biodieselestervolume,
       sum(cetaneindex*actfract)/sum(actfract) as cetaneindex,
       sum(pahcontent*actfract)/sum(actfract) as pahcontent,
       sum(t50*actfract)/sum(actfract) as t50,
       sum(t90*actfract)/sum(actfract) as t90
  from e10fuelproperties
  inner join monthgroupweighting using(monthgroupid)
  group by fuelregionid, fuelyearid;

--
-- averagetanktemperature
--
-- select "MAKING AVERAGETANKTEMPERATURE" as marker_point;
create table oldaveragetanktemperature
  select tanktemperaturegroupid, zoneid, monthid, hourdayid, opmodeid, averagetanktemperature
  from averagetanktemperature;
truncate averagetanktemperature;
replace into averagetanktemperature (tanktemperaturegroupid, zoneid, monthid,
    hourdayid, opmodeid, averagetanktemperature, averagetanktemperaturecv, isuserinput) 
  select tanktemperaturegroupid, zoneid, 0 as monthid, 0 as hourdayid, opmodeid,
    sum(averagetanktemperature*actfract) as averagetanktemperature, 
    null as averagetanktemperaturecv, 'Y' as isuserinput
  from oldaveragetanktemperature as oatt inner join monthweighting using(monthid)
  group by tanktemperaturegroupid, zoneid, opmodeid ;

flush table averagetanktemperature;

--
-- soakactivityfraction
--
-- select "MAKING SOAKACTIVITYFRACTION" as marker_point;
create table oldsoakactivityfraction
  select sourcetypeid, zoneid, monthid, hourdayid, opmodeid, soakactivityfraction
  from soakactivityfraction;
truncate soakactivityfraction;
replace into soakactivityfraction (sourcetypeid, zoneid, monthid,
    hourdayid, opmodeid, soakactivityfraction, soakactivityfractioncv, isuserinput) 
  select sourcetypeid, zoneid, 0 as monthid, 0 as hourdayid, opmodeid,
    sum(soakactivityfraction*actfract) as soakactivityfraction, 
    null as soakactivityfractioncv, 'Y' as isuserinput
  from oldsoakactivityfraction inner join monthweighting using(monthid)
  group by sourcetypeid, zoneid, opmodeid ;

flush table soakactivityfraction;

--
-- totalidlefraction
--
-- select "MAKING TOTALIDLEFRACTION" as marker_point;
create table oldtotalidlefraction
  select * from totalidlefraction;
truncate totalidlefraction;
replace into totalidlefraction(idleregionid, countytypeid, sourcetypeid, monthid, dayid, minmodelyearid, maxmodelyearid, totalidlefraction)
select idleregionid, countytypeid, sourcetypeid, 0 as monthid, 
  0 as dayid, 
  minmodelyearid, maxmodelyearid,
  sum(totalidlefraction*actfract) as totalidlefraction
from oldtotalidlefraction
inner join monthweighting using (monthid)
group by idleregionid, countytypeid, sourcetypeid, minmodelyearid, maxmodelyearid;
flush table totalidlefraction;
  
-- idlemonthadjust
-- 
-- select "MAKING IDLEMONTHADJUST" as marker_point;
create table oldidlemonthadjust
  select * from idlemonthadjust;
truncate idlemonthadjust;
insert into idlemonthadjust (sourcetypeid, monthid, idlemonthadjust)
  select sourcetypeid, 0 as monthid, avg(idlemonthadjust) as idlemonthadjust
  from oldidlemonthadjust
  group by sourcetypeid;
flush table idlemonthadjust;


--
-- drop any new tables created 
--
-- flush tables;

drop table if exists monthweighting;
drop table if exists monthgroupweighting;
drop table if exists aggdayvmtfraction;
drop table if exists aggmonthvmtfraction;
drop table if exists aggsourcetypedayvmt;
drop table if exists agghpmsvtypeday;
drop table if exists oldsho;
drop table if exists oldsourcehours;
drop table if exists oldstarts;
drop table if exists oldstartsmonthadjust;
drop table if exists oldextendedidlehours;
drop table if exists oldhotellingmonthadjust;
drop table if exists aggzonemonthhour;
drop table if exists aggmonthgrouphour;
drop table if exists aggfuelsupply;
drop table if exists oldaveragetanktemperature;
drop table if exists oldsoakactivityfraction;
drop table if exists oldtotalidlefraction;

-- flush tables;
  
