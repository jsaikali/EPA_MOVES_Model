/* ***********************************************************************************************************************
-- mysql script file to aggregate the separate hours of the day
--    out of the movesexecution database
--    after input databases have been merged by the inputdatamanager
--    and before any masterloopable objects are executed.
-- an attempt is made to weight some aggregations by activity.
--
-- author wesley faler
-- author gwo shyu
-- version 2014-04-24
-- change history:
--   modified by gwo shyu, march 26, 2008 to fix the errors of duplicate entry and table existing
   *********************************************************************************************************************** */
drop table if exists hourweighting1;
drop table if exists hourweighting2;
drop table if exists hourweighting3;
drop table if exists hourweighting4;
drop table if exists oldhourday;
drop table if exists oldavgspeeddistribution;
drop table if exists oldopmodedistribution;
drop table if exists oldopmodedistribution2;
drop table if exists oldsourcetypehour;
drop table if exists oldsho;
drop table if exists oldsourcehours;
drop table if exists oldstarts;
drop table if exists oldextendedidlehours;
drop table if exists oldhotellinghourfraction;
drop table if exists aggzonemonthhour;
drop table if exists aggmonthgrouphour;
drop table if exists oldsamplevehicletrip;
drop table if exists oldstartspervehicle;
drop table if exists oldstartsopmodedistribution;
drop table if exists oldstartshourfraction;
drop table if exists oldsoakactivityfraction;
drop table if exists oldaveragetanktemperature;

--
-- create hourweighting1 to be used to weight hourly activity 
--    by sourcetypeid, dayid, and roadtypeid.
--    hourvmtfraction itself could be used, except that it will be modified later
-- 
-- select "making hourweighting1" as marker_point;
create table hourweighting1
  select sourcetypeid, roadtypeid, dayid, hourid, hourvmtfraction as actfract 
    from hourvmtfraction;
create unique index index1 on hourweighting1 (sourcetypeid, roadtypeid, dayid, hourid);

--
-- create hourweighting2 to be used to weight hourly activity by
--    sourcetypeid, and dayid
--
-- select "making hourweighting2" as marker_point;
-- explicit creation of intermediate file found necessary to avoid significance problems
create table  hourweighting2 (
  sourcetypeid smallint, 
  dayid smallint,
  hourid smallint,
  actfract float);
insert into hourweighting2 (sourcetypeid, dayid, hourid, actfract)
  select hvmtf.sourcetypeid, dayid, hourid, 
    ((sum(hourvmtfraction*roadtypevmtfraction))/sum(roadtypevmtfraction)) as actfract 
    from hourvmtfraction as hvmtf inner join roadtypedistribution using (sourcetypeid, roadtypeid)
    group by hvmtf.sourcetypeid, dayid, hourid;
create unique index index1 on hourweighting2 (sourcetypeid, dayid, hourid);

--
-- create hourweighting3 to be used to weight hourly activity 
--    when no keys (besides hourid) are shared with hourvmtfraction
--
-- select "making hourweighting3" as marker_point;
-- explicit creation of intermediate file found necessary to avoid significance problems
create table  hourweighting3 (
  hourid smallint,
  actfract float);
insert into hourweighting3
  select hourid, avg(actfract) as actfract 
    from hourweighting2
    where sourcetypeid=21 
    group by hourid;
create unique index index1 on hourweighting3 (hourid);

-- create hourweighting4 to be used to weight hourly activity
--  when only dayid (besides hourid) is shared with hourvmtfraction
--
select "making hourweighting4" as marker_point;
create table hourweighting4 (
  dayid smallint,
  hourid smallint,
  actfract float);
insert into hourweighting4
  select dayid, hourid, actfract 
  from hourweighting2 where sourcetypeid=21;
create unique index index1 on hourweighting4 (dayid, hourid);

--
-- hourofanyday table
--
-- SELECT "Making HourOfAnyDay" AS MARKER_POINT;
truncate hourofanyday;
insert into hourofanyday (hourid, hourname)
  values (0, "Entire Day");

flush table hourofanyday;
  
--
-- hourday table  (save old version as it is needed later)
--
-- select "making hourday" as marker_point;
create table oldhourday select * from hourday;
create unique index index1 on oldhourday(hourdayid);
truncate hourday;
insert into hourday (hourdayid, dayid, hourid)
  select dayid as hourdayid, dayid, 0 as hourid
  from dayofanyweek;
create unique index index1 on hourday (hourdayid);
flush table hourday;
--
-- hourvmtfraction table
--
-- select "making hourvmtfraction" as marker_point;
truncate hourvmtfraction;
replace into hourvmtfraction (sourcetypeid, roadtypeid, dayid, hourid, hourvmtfraction)
  select sourcetypeid, roadtypeid, dayid, 0 as hourid, 1.0 as hourvmtfraction
  from hourweighting1
  group by sourcetypeid, roadtypeid, dayid;
create unique index index1 on hourvmtfraction (sourcetypeid, roadtypeid, dayid);  
flush table hourvmtfraction;

--
-- avgspeeddistribution table
--
-- select "making avgspeeddistribution" as marker_point;
create table oldavgspeeddistribution
  select sourcetypeid, roadtypeid, dayid, hourid, avgspeedbinid, avgspeedfraction
  from avgspeeddistribution inner join oldhourday using (hourdayid);
truncate avgspeeddistribution;
insert into avgspeeddistribution 
  (sourcetypeid, roadtypeid, hourdayid, avgspeedbinid, avgspeedfraction)
  select asd.sourcetypeid, asd.roadtypeid, asd.dayid as hourdayid, asd.avgspeedbinid, 
    ((sum(asd.avgspeedfraction*hw.actfract))/sum(hw.actfract)) as avgspeedfraction
  from oldavgspeeddistribution as asd inner join hourweighting1 as hw 
      using (sourcetypeid, roadtypeid, dayid, hourid)
    group by sourcetypeid, roadtypeid, asd.dayid, avgspeedbinid;
create unique index index1 on avgspeeddistribution 
    (sourcetypeid, roadtypeid, hourdayid, avgspeedbinid);
flush table avgspeeddistribution;

--
-- opmodedistribution
--
-- select "making opmodedistribution" as marker_point;
create table oldopmodedistribution 
  select omd.*, link.roadtypeid
  from opmodedistribution as omd inner join link using (linkid);
create table oldopmodedistribution2
  select omd.*, oldhourday.dayid,oldhourday.hourid
  from oldopmodedistribution as omd inner join oldhourday using (hourdayid);
create unique index index1 on oldopmodedistribution2 (sourcetypeid, roadtypeid, dayid, hourid);
truncate opmodedistribution;
insert into opmodedistribution (sourcetypeid, hourdayid, linkid, polprocessid, opmodeid, 
    opmodefraction, opmodefractioncv, isuserinput)
  select omd.sourcetypeid, omd.dayid as hourdayid, omd.linkid, omd.polprocessid, omd.opmodeid,
    (sum(omd.opmodefraction * hw.actfract)/sum(hw.actfract)) as opmodefraction, 
    null as opmodefractioncv, "Y" as isuserinput
  from oldopmodedistribution2 as omd inner join hourweighting1 as hw
    using (sourcetypeid, roadtypeid, dayid, hourid )
  group by omd.sourcetypeid, omd.dayid, omd.linkid, omd.polprocessid, omd.opmodeid; 
flush table opmodedistribution;
  
--
-- sourcetypehour table
--
-- note:  idleshofactors are to be summed, not averaged. 
-- select "making sourcetypehour" as marker_point;
create table oldsourcetypehour
  select sourcetypeid, dayid, hourid, idleshofactor, hotellingdist
  from sourcetypehour inner join oldhourday using (hourdayid);
truncate sourcetypehour;
insert into sourcetypehour 
  (sourcetypeid, hourdayid, idleshofactor, hotellingdist)
  select sth.sourcetypeid, sth.dayid as hourdayid, 
    sum(sth.idleshofactor) as idleshofactor, sum(sth.hotellingdist) as hotellingdist
  from oldsourcetypehour as sth 
  group by sourcetypeid, sth.dayid;
create unique index index1 on sourcetypehour (sourcetypeid, hourdayid);  
flush table sourcetypehour;

--
--  sho    
--
-- select "making sho" as marker_point;
create table oldsho
  select sho.*, dayid, hourid
  from sho inner join oldhourday using(hourdayid);
create index index10 on oldsho (dayid, monthid, yearid, ageid, linkid, sourcetypeid);
truncate sho;
insert into sho (hourdayid, monthid, yearid, ageid, linkid, 
    sourcetypeid, sho, shocv, distance, isuserinput)
  select dayid as hourdayid, monthid, yearid, ageid,linkid, sourcetypeid, 
    sum(sho) as sho, null as shocv, sum(distance) as distance, "Y" as isuserinput
  from oldsho 
  group by dayid, monthid, yearid, ageid, linkid, sourcetypeid;
flush table sho;
  
--
--  sourcehours    
--
-- select "making sourcehours" as marker_point;
create table oldsourcehours
  select sourcehours.*, dayid, hourid
  from sourcehours inner join oldhourday using(hourdayid);
create index index10 on oldsourcehours (dayid, monthid, yearid, ageid, linkid, sourcetypeid);
truncate sourcehours;
insert into sourcehours (hourdayid, monthid, yearid, ageid, linkid, 
    sourcetypeid, sourcehours, sourcehourscv, isuserinput)
  select dayid as hourdayid, monthid, yearid, ageid,linkid, sourcetypeid, 
    sum(sourcehours) as sourcehours, null as sourcehourscv,"Y" as isuserinput
  from oldsourcehours 
  group by dayid, monthid, yearid, ageid, linkid, sourcetypeid;
flush table sourcehours;
  
--
--  starts
--
-- select "making starts" as marker_point;
create table  oldstarts
  select starts.*, dayid, hourid
  from starts inner join oldhourday using(hourdayid);
create index index11 on oldstarts (dayid, monthid, yearid, ageid, zoneid, sourcetypeid);
truncate starts;
insert into starts (hourdayid, monthid, yearid, ageid, zoneid, 
    sourcetypeid, starts, startscv, isuserinput)
  select dayid as hourdayid, monthid, yearid, ageid, zoneid, sourcetypeid, 
    sum(starts) as starts, null as startscv, "Y" as isuserinput
  from oldstarts
  group by dayid, monthid, yearid, ageid, zoneid, sourcetypeid;
flush table starts;
  
--
--  extendedidlehours
--
-- select "making extendedidlehours" as marker_point;
create table oldextendedidlehours
  select extendedidlehours.*, dayid, hourid
  from extendedidlehours inner join oldhourday using(hourdayid);
create index index12 on oldextendedidlehours (sourcetypeid, dayid, monthid, yearid, ageid, zoneid);
truncate extendedidlehours;
insert into extendedidlehours (sourcetypeid, hourdayid, monthid, yearid, ageid, zoneid, 
    extendedidlehours, extendedidlehourscv, isuserinput)
  select sourcetypeid, dayid as hourdayid, monthid, yearid, ageid, zoneid,  
    sum(extendedidlehours) as extendedidlehours, null as extendedidlehourscv, "Y" as isuserinput
  from oldextendedidlehours
  group by sourcetypeid, dayid, monthid, yearid, ageid, zoneid;
flush table extendedidlehours;

-- hotellinghourfraction
-- 
-- select "making startshourfraction" as marker_point;
create table oldhotellinghourfraction
  select * from hotellinghourfraction;
truncate hotellinghourfraction;
insert into hotellinghourfraction (zoneid, dayid, hourid, hourfraction)
  select zoneid, dayid, 0 as hourid, sum(hourfraction) as hourfraction
  from oldhotellinghourfraction
  group by zoneid, dayid;
flush table hotellinghourfraction;
  
-- 
-- zonemonthhour
--
-- select "making zonemonthhour" as marker_point;
-- explicit creation of intermediate file found necessary to avoid significance problems
create table  aggzonemonthhour (
  monthid smallint,
  zoneid integer,
  temperature float,
  relhumidity float);
insert into aggzonemonthhour (monthid,zoneid,temperature,relhumidity)
  select monthid, zoneid, 
    (sum(temperature*actfract)/sum(actfract)) as temperature,
    (sum(relhumidity*actfract)/sum(actfract)) as relhumidity
  from zonemonthhour inner join hourweighting3 using (hourid)
  group by monthid, zoneid;
truncate zonemonthhour;
replace into zonemonthhour (monthid, zoneid, hourid, temperature, temperaturecv,
    relhumidity, relativehumiditycv, heatindex, specifichumidity)
  select monthid, zoneid, 0 as hourid, temperature,
    null as temperaturecv, relhumidity, null as relativehumiditycv,
    0.0 as heatindex, 0.0 as specifichumidity 
  from aggzonemonthhour;
flush table zonemonthhour;
  
-- 
-- monthgrouphour
--
-- select "making monthgrouphour" as marker_point;
-- explicit creation of intermediate file found necessary to avoid significance problems
create table aggmonthgrouphour (
  monthgroupid smallint,
  acactivityterma float,
  acactivitytermb float,
  acactivitytermc float);
insert into aggmonthgrouphour (monthgroupid,acactivityterma,acactivitytermb,acactivitytermc)
  select monthgroupid, 
    (sum(acactivityterma*actfract)/sum(actfract)) as acactivityterma,
    (sum(acactivitytermb*actfract)/sum(actfract)) as acactivitytermb,
    (sum(acactivitytermc*actfract)/sum(actfract)) as acactivitytermc
  from monthgrouphour inner join hourweighting3 using (hourid)
  group by monthgroupid;
truncate monthgrouphour;
replace into monthgrouphour (monthgroupid, hourid,
  acactivityterma, acactivitytermacv, 
  acactivitytermb, acactivitytermbcv, 
  acactivitytermc, acactivitytermccv 
  )
  select monthgroupid, 0 as hourid,
    acactivityterma, null as acactivitytermacv,
    acactivitytermb, null as acactivitytermbcv,
    acactivitytermc, null as acactivitytermccv
  from aggmonthgrouphour;  
flush table monthgrouphour;
  
--
-- samplevehicletrip
-- 
-- select "making samplevehicletrip" as marker_point;
create table oldsamplevehicletrip 
  select *
  from samplevehicletrip;
-- ***************************
--  select samplevehicletrip.*, dayid 
--  from samplevehicletrip inner join oldhourday using (hourdayid);
-- ***************************

truncate samplevehicletrip;
insert into samplevehicletrip (vehid, tripid, dayid, hourid, priortripid, 
  keyontime, keyofftime) 
  select vehid, tripid, dayid, 0 as hourid, priortripid, keyontime, keyofftime
  from oldsamplevehicletrip;
-- ***************************
--  select vehid, tripid, dayid as hourdayid, priortripid, keyontime, keyofftime
--  from oldsamplevehicletrip;
-- ***************************
flush table samplevehicletrip;

--
-- startspervehicle
-- 
-- select "making startspervehicle" as marker_point;
create table oldstartspervehicle 
  select startspervehicle.*, dayid 
  from startspervehicle inner join oldhourday using (hourdayid);
truncate startspervehicle;
insert into startspervehicle (sourcetypeid, hourdayid, 
  startspervehicle, startspervehiclecv) 
  select sourcetypeid, dayid as hourdayid, sum(startspervehicle) as startspervehicle,
    null as startspervehiclecv
  from oldstartspervehicle group by sourcetypeid, hourdayid;
flush table startspervehicle;

-- this must happen before changes are made to startshourfraction (jarrod/evan/michael integration sprint 27 march 2019)
-- startsopmodedistribution
-- 
-- select "making startsopmodedistribution" as marker_point;
create table oldstartsopmodedistribution
  select * from startsopmodedistribution;
truncate startsopmodedistribution;
insert into startsopmodedistribution (dayid, hourid, sourcetypeid, ageid,
  opmodeid, opmodefraction, isuserinput)
  select dayid, 0 as hourid, sourcetypeid, ageid, opmodeid,
    sum(opmodefraction * allocationfraction) as opmodefraction, 'Y' as isuserinput
  from oldstartsopmodedistribution inner join startshourfraction using (dayid, hourid, sourcetypeid)
  group by dayid, sourcetypeid, ageid, opmodeid;
flush table startsopmodedistribution;

-- startshourfraction
-- 
-- select "making startshourfraction" as marker_point;
create table oldstartshourfraction
  select * from startshourfraction;
truncate startshourfraction;
insert into startshourfraction (dayid, hourid, sourcetypeid, allocationfraction)
  select dayid, 0 as hourid, sourcetypeid, sum(allocationfraction) as allocationfraction
  from oldstartshourfraction
  group by dayid, sourcetypeid;
flush table startshourfraction;
     
--
-- averagetanktemperature
--
-- select "making averagetanktemperature" as marker_point;
create table oldaveragetanktemperature
  select tanktemperaturegroupid, zoneid, monthid, hourid, dayid, opmodeid, averagetanktemperature
  from averagetanktemperature inner join oldhourday using(hourdayid);
truncate averagetanktemperature;
insert into averagetanktemperature (tanktemperaturegroupid, zoneid, monthid,
    hourdayid, opmodeid, averagetanktemperature, averagetanktemperaturecv, isuserinput) 
  select tanktemperaturegroupid, zoneid, monthid, oatt.dayid as hourdayid, opmodeid,
    sum(averagetanktemperature*actfract) as averagetanktemperature, 
    null as averagetanktemperaturecv, 'Y' as isuserinput
  from oldaveragetanktemperature as oatt inner join hourweighting4 using(dayid, hourid)
  group by tanktemperaturegroupid, zoneid, monthid, oatt.dayid, opmodeid ;
flush table averagetanktemperature;

--
-- soakactivityfraction
--
-- select "making soakactivityfraction" as marker_point;
create table oldsoakactivityfraction
  select sourcetypeid, zoneid, monthid, hourid, dayid, opmodeid, soakactivityfraction
  from soakactivityfraction inner join oldhourday using(hourdayid);
truncate soakactivityfraction;
insert into soakactivityfraction (sourcetypeid, zoneid, monthid,
    hourdayid, opmodeid, soakactivityfraction, soakactivityfractioncv, isuserinput) 
  select osaf.sourcetypeid, zoneid, monthid, osaf.dayid as hourdayid, opmodeid,
    sum(soakactivityfraction*actfract) as soakactivityfraction, 
    null as soakactivityfractioncv, 'Y' as isuserinput
  from oldsoakactivityfraction as osaf inner join hourweighting2 using(sourcetypeid, dayid, hourid)
  group by osaf.sourcetypeid, zoneid, monthid, osaf.dayid, opmodeid ;
flush table soakactivityfraction;
  
--
-- drop any new tables created 
--

-- flush tables;

drop table if exists hourweighting1;
drop table if exists hourweighting2;
drop table if exists hourweighting3;
drop table if exists hourweighting4;
drop table if exists oldhourday;
drop table if exists oldavgspeeddistribution;
drop table if exists oldopmodedistribution;
drop table if exists oldopmodedistribution2;
drop table if exists oldsourcetypehour;
drop table if exists oldsho;
drop table if exists oldsourcehours;
drop table if exists oldstarts;
drop table if exists oldextendedidlehours;
drop table if exists oldhotellinghourfraction;
drop table if exists aggzonemonthhour;
drop table if exists aggmonthgrouphour;
drop table if exists oldsamplevehicletrip;
drop table if exists oldstartspervehicle;
drop table if exists oldstartsopmodedistribution;
drop table if exists oldstartshourfraction;
drop table if exists oldsoakactivityfraction;
drop table if exists oldaveragetanktemperature;

-- flush tables;
