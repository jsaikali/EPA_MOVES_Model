/* ***********************************************************************************************************************
-- mysql script file to aggregate the day level database to the month level
--    performed on the movesexecution database
--    after merging of input databases by the inputdatamanager
--    and before any masterloopable objects are executed.
-- an attempt is made to weight some aggregations by activity.
--
-- author wesley faler
-- author gwo shyu
-- author mitch cumberworth
-- author jarrod brown, michael aldridge, daniel bizer-cox, evan murray
-- version 2019-05-27
-- written by mitch cumberworth, april, 2004
-- change history:
--   updated by mitch cumberworth, july, 2005 for task 208
--   updated by mitch cumberworth, november, 2005 for task 210
--   modified by cimulus, january, 2005 for task 210
--   modified by gwo shyu, march 26, 2008 to fix the errors of duplicate entry and table existing
   *********************************************************************************************************************** */

drop table if exists dayweighting1;
drop table if exists dayweighting1sum;
drop table if exists dayweighting1normalized;
drop table if exists dayweighting2;
drop table if exists dayweighting2normalized;
drop table if exists dayweighting3;
drop table if exists dayweighting3normalized;
drop table if exists oldhourday;
drop table if exists aggdayvmtfraction;
drop table if exists aggsourcetypedayvmt;
drop table if exists agghpmsvtypeday;
drop table if exists oldavgspeeddistribution;
drop table if exists oldopmodedistribution;    
drop table if exists oldopmodedistribution2;
drop table if exists oldsourcetypehour;
drop table if exists oldsho;
drop table if exists oldsourcehours;
drop table if exists oldstarts;
drop table if exists oldextendedidlehours;
drop table if exists oldhotellinghourfraction;
drop table if exists oldhotellinghoursperday;
drop table if exists oldsamplevehiclesoakingday;
drop table if exists oldsamplevehicletrip;
drop table if exists oldsamplevehicleday;
drop table if exists oldstartspervehicle;
drop table if exists oldstartsopmodedistribution;
drop table if exists oldstartshourfraction;
drop table if exists oldstartsperdaypervehicle;
drop table if exists oldstartsperday;
drop table if exists oldaveragetanktemperature;
drop table if exists oldsoakactivityfraction;
drop table if exists oldtotalidlefraction;
drop table if exists oldidledayadjust;

--
-- create dayweighting1 to be used to weight daily activity 
--    by sourcetypeid and roadtypeid.
-- 
-- note:  will not sum to unity if all days not included
-- select "making dayweighting1" as marker_point;
-- create table explicitly to control column types and avoid significance problems
create table dayweighting1 (
  sourcetypeid smallint,
  roadtypeid smallint,
  dayid smallint,
  actfract float);
insert into dayweighting1
select day.sourcetypeid, roadtypeid, day.dayid, 
((sum(dayvmtfraction*noofrealdays*monthvmtfraction))/sum(monthvmtfraction)) as actfract 
from dayvmtfraction as day
inner join dayofanyweek dow on dow.dayid = day.dayid
inner join monthvmtfraction month on month.sourcetypeid = day.sourcetypeid
and month.monthid = day.monthid
group by day.sourcetypeid,roadtypeid,day.dayid;


create unique index index1 on dayweighting1 (sourcetypeid, roadtypeid, dayid);

create table dayweighting1sum (
  sourcetypeid smallint not null,
  roadtypeid smallint not null,
  actfractsum double,
  primary key (sourcetypeid, roadtypeid),
  key (roadtypeid, sourcetypeid)
);

insert into dayweighting1sum (sourcetypeid, roadtypeid, actfractsum)
select sourcetypeid, roadtypeid, sum(actfract) as actfractsum
from dayweighting1
group by sourcetypeid, roadtypeid;

create table dayweighting1normalized (
  sourcetypeid smallint not null,
  roadtypeid smallint not null,
  dayid smallint not null,
  actfract double,
  primary key (sourcetypeid, roadtypeid, dayid)
);

insert into dayweighting1normalized (sourcetypeid, roadtypeid, dayid, actfract)
select dw.sourcetypeid, dw.roadtypeid, dw.dayid, 
  case when ds.actfractsum > 0 then (dw.actfract / ds.actfractsum) else 0.0 end as actfract
from dayweighting1 dw
inner join dayweighting1sum ds using (sourcetypeid, roadtypeid);

--
-- create dayweighting2 to be used to weight daily activity by sourcetypeid only
--
-- note: will not sum to unity if all days not included
-- select "making dayweighting2" as marker_point;
-- create table explicitly to control column types and avoid significance problems
create table dayweighting2 (
  sourcetypeid smallint,
  dayid smallint,
  actfract float);
insert into dayweighting2
  select dw.sourcetypeid, dayid, 
    (sum(actfract*roadtypevmtfraction)/sum(roadtypevmtfraction)) as actfract 
    from dayweighting1 as dw inner join roadtypedistribution using (sourcetypeid, roadtypeid)
    group by dw.sourcetypeid, dayid;
create unique index index1 on dayweighting2 (sourcetypeid, dayid);

--
-- create dayweighting2normalized to be used to weight daily activity by sourcetypeid only
--
-- note: will not sum to unity if all days not included
-- select "making dayweighting2normalized" as marker_point;
-- create table explicitly to control column types and avoid significance problems
create table dayweighting2normalized (
  sourcetypeid smallint,
  dayid smallint,
  actfract float);
insert into dayweighting2normalized
  select dw.sourcetypeid, dayid, 
    (sum(actfract*roadtypevmtfraction)/sum(roadtypevmtfraction)) as actfract 
    from dayweighting1normalized as dw inner join roadtypedistribution using (sourcetypeid, roadtypeid)
    group by dw.sourcetypeid, dayid;
create unique index index1 on dayweighting2normalized (sourcetypeid, dayid);

--
-- create dayweighting3 to be used to weight daily activity when only dayid field present
--
-- note: will not sum to unity if all days not included
select "making dayweighting3" as marker_point;
-- create table explicitly to control column types and avoid significance problems
create table dayweighting3 (
  dayid smallint,
  actfract float);
insert into dayweighting3
  select dayid, actfract from dayweighting2 where sourcetypeid=21;
create unique index index1 on dayweighting3 (dayid);

--
-- create dayweighting3normalized to be used to weight daily activity when only dayid field present
--
-- note: will not sum to unity if all days not included
select "making dayweighting3normalized" as marker_point;
-- create table explicitly to control column types and avoid significance problems
create table dayweighting3normalized (
  dayid smallint,
  actfract float);
insert into dayweighting3normalized
  select dayid, actfract from dayweighting2normalized where sourcetypeid=21;
create unique index index1 on dayweighting3normalized (dayid);

  
--
-- hourday table  (save old version as it is needed later)
--
-- select "making hourday" as marker_point;
create table oldhourday select * from hourday;
create unique index index2 on oldhourday(hourdayid);
truncate hourday;
insert into hourday (hourdayid, dayid, hourid)
    values (0,0,0);
flush table hourday;
  
--
-- hourvmtfraction table
--
-- already has index from day script.
-- select "making hourvmtfraction" as marker_point;
truncate hourvmtfraction;
replace into hourvmtfraction (sourcetypeid, roadtypeid, dayid, hourid, hourvmtfraction)
  select sourcetypeid, roadtypeid, 0 as dayid, 0 as hourid, 1.0 as hourvmtfraction
  from dayweighting1
  group by sourcetypeid, roadtypeid;
flush table hourvmtfraction;
  
--
-- dayvmtfraction table
--
-- select "making dayvmtfraction" as marker_point;
create table aggdayvmtfraction 
  select sourcetypeid, monthid, roadtypeid
    from dayvmtfraction 
    group by sourcetypeid, monthid, roadtypeid;
truncate dayvmtfraction;
replace into dayvmtfraction (sourcetypeid, monthid, roadtypeid, dayid, dayvmtfraction)
  select sourcetypeid, monthid, roadtypeid, 0 as dayid, 1.0 as dayvmtfraction
  from aggdayvmtfraction;
flush table dayvmtfraction;

--
-- sourcetypedayvmt table
--
-- select "making sourcetypedayvmt" as marker_point;
create table aggsourcetypedayvmt 
  select yearid, monthid, 0 as dayid, sourcetypeid, sum(vmt*actfract) as vmt
    from sourcetypedayvmt
    inner join dayweighting2normalized using (sourcetypeid, dayid)
    group by yearid, monthid, sourcetypeid;
truncate sourcetypedayvmt;
replace into sourcetypedayvmt (yearid, monthid, dayid, sourcetypeid, vmt)
  select yearid, monthid, dayid, sourcetypeid, vmt
  from aggsourcetypedayvmt;
flush table sourcetypedayvmt;

--
-- hpmsvtypeday table
--
-- select "making hpmsvtypeday" as marker_point;
create table agghpmsvtypeday 
  select yearid, monthid, 0 as dayid, hpmsvtypeid, sum(vmt*actfract) as vmt
    from hpmsvtypeday
    inner join dayweighting3normalized using (dayid)
    group by yearid, monthid, hpmsvtypeid;
truncate hpmsvtypeday;
replace into hpmsvtypeday (yearid, monthid, dayid, hpmsvtypeid, vmt)
  select yearid, monthid, dayid, hpmsvtypeid, vmt
  from agghpmsvtypeday;
flush table hpmsvtypeday;

--
-- avgspeeddistribution table
--
-- table index already created in day aggregation 
-- select "making avgspeeddistribution" as marker_point;
create table oldavgspeeddistribution
  select sourcetypeid, roadtypeid, dayid, avgspeedbinid, avgspeedfraction
  from avgspeeddistribution inner join oldhourday using (hourdayid);
truncate avgspeeddistribution;
replace into avgspeeddistribution 
  (sourcetypeid, roadtypeid, hourdayid, avgspeedbinid, avgspeedfraction)
  select asd.sourcetypeid, asd.roadtypeid, 0 as hourdayid, asd.avgspeedbinid, 
    (sum(asd.avgspeedfraction*dw.actfract)/sum(dw.actfract)) as avgspeedfraction
  from oldavgspeeddistribution as asd inner join dayweighting1 as dw 
      using (sourcetypeid, roadtypeid, dayid)
    group by sourcetypeid, roadtypeid,  avgspeedbinid;
flush table avgspeeddistribution;

--
-- opmodedistribution
--
-- select "making opmodedistribution" as marker_point;
create table oldopmodedistribution 
  select omd.*, link.roadtypeid
  from opmodedistribution as omd inner join link using (linkid);
create table oldopmodedistribution2
  select omd.*, oldhourday.dayid
  from oldopmodedistribution as omd inner join oldhourday using (hourdayid);
create unique index index1 on oldopmodedistribution2 (sourcetypeid, roadtypeid, dayid);
truncate opmodedistribution;
replace into opmodedistribution (sourcetypeid, hourdayid, linkid, polprocessid, opmodeid, 
    opmodefraction, opmodefractioncv, isuserinput)
  select omd.sourcetypeid, 0 as hourdayid, omd.linkid, omd.polprocessid, omd.opmodeid,
    (sum(omd.opmodefraction * dw.actfract)/sum(dw.actfract)) as opmodefraction, 
    null as opmodefractioncv, "Y" as isuserinput
  from oldopmodedistribution2 as omd inner join dayweighting1 as dw
    using (sourcetypeid, roadtypeid, dayid )
  group by omd.sourcetypeid, omd.linkid, omd.polprocessid, omd.opmodeid; 
flush table opmodedistribution;
  
--
-- sourcetypehour table
--
-- table index already created in day aggregation 
-- select "making sourcetypehour" as marker_point;
create table oldsourcetypehour
  select sourcetypeid, dayid, idleshofactor, hotellingdist
  from sourcetypehour inner join oldhourday using (hourdayid);
truncate sourcetypehour;
replace into sourcetypehour 
  (sourcetypeid, hourdayid, idleshofactor, hotellingdist)
  select sth.sourcetypeid, 0 as hourdayid, 
    (sum(sth.idleshofactor*dw.actfract)/sum(dw.actfract)) as idleshofactor,
    (sum(sth.hotellingdist*dw.actfract)/sum(dw.actfract)) as hotellingdist
  from oldsourcetypehour as sth inner join dayweighting2 as dw 
      using (sourcetypeid, dayid)
    group by sourcetypeid;
flush table sourcetypehour;

--
--  sho    
--
-- select "making sho" as marker_point;
create table oldsho
  select sho.*, dayid
  from sho inner join oldhourday using(hourdayid);
create index index1 on oldsho (monthid, yearid, ageid, linkid, sourcetypeid);
truncate sho;
replace into sho (hourdayid, monthid, yearid, ageid, linkid, 
    sourcetypeid, sho, shocv, distance, isuserinput)
  select 0 as hourdayid, monthid, yearid, ageid, linkid, sourcetypeid, 
    sum(sho) as sho, null as shocv, sum(distance) as distance, "Y" as isuserinput
  from oldsho 
  group by monthid, yearid, ageid, linkid, sourcetypeid;
flush table sho;
  
--
--  sourcehours    
--
-- select "making sourcehours" as marker_point;
create table oldsourcehours
  select sourcehours.*, dayid
  from sourcehours inner join oldhourday using(hourdayid);
create index index1 on oldsourcehours (monthid, yearid, ageid, linkid, sourcetypeid);
truncate sourcehours;
replace into sourcehours (hourdayid, monthid, yearid, ageid, linkid, 
    sourcetypeid, sourcehours, sourcehourscv, isuserinput)
  select 0 as hourdayid, monthid, yearid, ageid, linkid, sourcetypeid, 
    sum(sourcehours) as sourcehours, null as sourcehourscv, "Y" as isuserinput
  from oldsourcehours
  group by monthid, yearid, ageid, linkid, sourcetypeid;
flush table sourcehours;
  
--
--  starts
--
-- select "making starts" as marker_point;
create table oldstarts
  select starts.*, dayid
  from starts inner join oldhourday using(hourdayid);
create index index1 on oldstarts (monthid, yearid, ageid, zoneid, sourcetypeid);
truncate starts;
replace into starts (hourdayid, monthid, yearid, ageid, zoneid, 
    sourcetypeid, starts, startscv, isuserinput)
  select 0 as hourdayid, monthid, yearid, ageid, zoneid, sourcetypeid, 
    sum(starts) as starts, null as startscv, "Y" as isuserinput
  from oldstarts
  group by monthid, yearid, ageid, zoneid, sourcetypeid;
flush table starts;
  
--
--  extendedidlehours
--
-- select "making extendedidlehours" as marker_point;
create table oldextendedidlehours
  select extendedidlehours.*, dayid
  from extendedidlehours inner join oldhourday using(hourdayid);
create index index1 on oldextendedidlehours (sourcetypeid, monthid, yearid, ageid, zoneid);
truncate extendedidlehours;
replace into extendedidlehours (sourcetypeid, hourdayid, monthid, yearid, ageid, zoneid, 
    extendedidlehours, extendedidlehourscv, isuserinput)
  select sourcetypeid, 0 as hourdayid, monthid, yearid, ageid, zoneid,  
    sum(extendedidlehours) as extendedidlehours, null as extendedidlehourscv, "Y" as isuserinput
  from oldextendedidlehours
  group by sourcetypeid, monthid, yearid, ageid, zoneid;
flush table extendedidlehours;

-- hotellinghourfraction
--
-- select "making hotellinghourfraction" as marker_point;
create table oldhotellinghourfraction
  select * from hotellinghourfraction;
truncate hotellinghourfraction;
insert into hotellinghourfraction (zoneid, dayid, hourid, hourfraction)
  select zoneid, 0 as dayid, 0 as hourid, sum(hourfraction) / aggregation.hourfractiontotal as hourfraction
  from oldhotellinghourfraction inner join
       (select zoneid, sum(hourfraction) as hourfractiontotal
        from oldhotellinghourfraction
        group by zoneid) as aggregation
  using (zoneid)
  group by zoneid;
flush table hotellinghourfraction;

-- hotellinghoursperday
-- 
-- select "making hotellinghoursperday" as marker_point;
create table oldhotellinghoursperday
  select * from hotellinghoursperday;
truncate hotellinghoursperday;
insert into hotellinghoursperday (yearid, zoneid, dayid, hotellinghoursperday)
  select yearid, zoneid, 0 as dayid, sum(hotellinghoursperday * noofrealdays / 7) as hotellinghoursperday
  from oldhotellinghoursperday
  inner join dayofanyweek using (dayid)
  group by yearid, zoneid;
flush table hotellinghoursperday;

--
-- samplevehiclesoakingday
-- 
-- select "making samplevehiclesoakingday" as marker_point;
create table oldsamplevehiclesoakingday select * from samplevehiclesoakingday;
truncate samplevehiclesoakingday;
replace into samplevehiclesoakingday (soakdayid, sourcetypeid, dayid, f)
  select soakdayid, sourcetypeid, 0 as dayid, sum(f* (case when dayid=5 then 5 else 2 end))/sum(case when dayid=5 then 5 else 2 end)
  from oldsamplevehiclesoakingday
  group by soakdayid, sourcetypeid
  order by null;
flush table samplevehiclesoakingday;

--
-- samplevehicletrip
-- 
-- select "making samplevehicletrip" as marker_point;
create table oldsamplevehicletrip select * from samplevehicletrip;
truncate samplevehicletrip;
replace into samplevehicletrip (vehid, tripid, dayid, hourid, priortripid, 
  keyontime, keyofftime) 
  select vehid, tripid, 0 as dayid, 0 as hourid, priortripid, keyontime, keyofftime
  from oldsamplevehicletrip;
flush table samplevehicletrip;

--
-- samplevehicleday
-- 
-- select "making samplevehicleday" as marker_point;
create table oldsamplevehicleday select * from samplevehicleday;
truncate samplevehicleday;
replace into samplevehicleday (vehid, dayid, sourcetypeid) 
  select vehid, 0 as dayid, sourcetypeid
  from oldsamplevehicleday;
flush table samplevehicleday;

--
-- startspervehicle
-- 
-- select "making startspervehicle" as marker_point;
create table oldstartspervehicle select * from startspervehicle ;
truncate startspervehicle;
replace into startspervehicle (sourcetypeid, hourdayid, 
  startspervehicle, startspervehiclecv) 
  select sourcetypeid, 0 as hourdayid, sum(startspervehicle) as startspervehicle,
    null as startspervehiclecv
  from oldstartspervehicle group by sourcetypeid;
flush table startspervehicle;

-- startsopmodedistribution
-- 
-- select "making startsopmodedistribution" as marker_point;
create table oldstartsopmodedistribution
  select * from startsopmodedistribution;
truncate startsopmodedistribution;
insert into startsopmodedistribution (dayid, hourid, sourcetypeid, ageid,
  opmodeid, opmodefraction, isuserinput)
  select 0 as dayid, 0 as hourid, sourcetypeid, ageid, opmodeid,
    sum(opmodefraction * dayid * startsperdaypervehicle) / aggregation.opmodefractiontotal as opmodefraction, 'Y' as isuserinput
  from oldstartsopmodedistribution inner join
       (select sourcetypeid, ageid, sum(opmodefraction * dayid * startsperdaypervehicle) as opmodefractiontotal
        from oldstartsopmodedistribution inner join startsperdaypervehicle
        using (dayid, sourcetypeid)
        group by sourcetypeid, ageid) as aggregation
  using (sourcetypeid, ageid) inner join startsperdaypervehicle
  using (dayid, sourcetypeid)
  group by sourcetypeid, ageid, opmodeid;
flush table startsopmodedistribution;

-- startshourfraction
--
-- select "making startshourfraction" as marker_point;
create table oldstartshourfraction
  select * from startshourfraction;
truncate startshourfraction;
insert into startshourfraction (dayid, hourid, sourcetypeid, allocationfraction)
  select 0 as dayid, 0 as hourid, sourcetypeid, sum(allocationfraction) / aggregation.allocationfractiontotal as allocationfraction
  from oldstartshourfraction inner join
       (select sourcetypeid, sum(allocationfraction) as allocationfractiontotal
        from oldstartshourfraction
        group by sourcetypeid) as aggregation
  using (sourcetypeid)
  group by sourcetypeid;
flush table startshourfraction;

-- startsperdaypervehicle
-- 
-- select "making startsperdaypervehicle" as marker_point;
create table oldstartsperdaypervehicle
  select * from startsperdaypervehicle;
truncate startsperdaypervehicle;
insert into startsperdaypervehicle (dayid, sourcetypeid, startsperdaypervehicle)
  select 0 as dayid, sourcetypeid, sum((startsperdaypervehicle * dayid) / 7) as startsperdaypervehicle
  from oldstartsperdaypervehicle
  group by sourcetypeid;
flush table startsperdaypervehicle;

-- startsperday
-- 
-- select "making startsperday" as marker_point;
create table oldstartsperday
  select * from startsperday;
truncate startsperday;
insert into startsperday (dayid, sourcetypeid, startsperday)
  select 0 as dayid, sourcetypeid, sum((startsperday * dayid) / 7) as startsperday
  from oldstartsperday
  group by sourcetypeid;
flush table startsperday;
  
--
-- averagetanktemperature
--
-- select "making averagetanktemperature" as marker_point;
create table oldaveragetanktemperature
  select tanktemperaturegroupid, zoneid, monthid, dayid, opmodeid, averagetanktemperature
  from averagetanktemperature inner join oldhourday using (hourdayid);
truncate averagetanktemperature;
replace into averagetanktemperature (tanktemperaturegroupid, zoneid, monthid,
    hourdayid, opmodeid, averagetanktemperature, averagetanktemperaturecv, isuserinput) 
  select tanktemperaturegroupid, zoneid, monthid, 0 as hourdayid, opmodeid,
    sum(averagetanktemperature*actfract) as averagetanktemperature, 
    null as averagetanktemperaturecv, 'Y' as isuserinput
  from oldaveragetanktemperature as oatt inner join dayweighting3 using(dayid)
  group by tanktemperaturegroupid, zoneid, monthid, opmodeid ;
flush table averagetanktemperature;

--
-- soakactivityfraction
--
-- select "making soakactivityfraction" as marker_point;
create table oldsoakactivityfraction
  select sourcetypeid, zoneid, monthid, dayid, opmodeid, soakactivityfraction
  from soakactivityfraction inner join oldhourday using(hourdayid);
truncate soakactivityfraction;
replace into soakactivityfraction (sourcetypeid, zoneid, monthid,
    hourdayid, opmodeid, soakactivityfraction, soakactivityfractioncv, isuserinput) 
  select osaf.sourcetypeid, zoneid, monthid, 0 as hourdayid, opmodeid,
    sum(soakactivityfraction*actfract) as soakactivityfraction, 
    null as soakactivityfractioncv, 'Y' as isuserinput
  from oldsoakactivityfraction as osaf inner join dayweighting2 using(sourcetypeid, dayid)
  group by osaf.sourcetypeid, zoneid, monthid, opmodeid ;
flush table soakactivityfraction;

--
-- totalidlefraction
--
-- select "making totalidlefraction" as marker_point;
create table oldtotalidlefraction
  select sourcetypeid, minmodelyearid, maxmodelyearid, monthid, dayid, idleregionid, countytypeid, totalidlefraction
    from totalidlefraction;
truncate totalidlefraction;
insert into totalidlefraction (sourcetypeid, minmodelyearid, maxmodelyearid, monthid, dayid, idleregionid, countytypeid, totalidlefraction)
  select sourcetypeid,  minmodelyearid, maxmodelyearid, monthid, 0 as dayid, idleregionid, countytypeid, sum(totalidlefraction*dayweighting2normalized.actfract)
  from oldtotalidlefraction
  left join dayweighting2normalized
  using (sourcetypeid, dayid)
  group by sourcetypeid,  minmodelyearid, maxmodelyearid, monthid, idleregionid, countytypeid;
flush table totalidlefraction;

-- idledayadjust
-- 
-- select "making idledayadjust" as marker_point;
create table oldidledayadjust
  select * from idledayadjust;
truncate idledayadjust;
insert into idledayadjust (sourcetypeid, dayid, idledayadjust)
  select sourcetypeid, 0 as dayid, sum(idledayadjust * noofrealdays / 7) as idledayadjust
  from oldidledayadjust
  join dayofanyweek using(dayid)
  group by sourcetypeid;
flush table idledayadjust;
  
  
--
-- dayofanyweek table
--
-- select "making dayofanyweek" as marker_point;
truncate dayofanyweek;
insert into dayofanyweek (dayid, dayname, noofrealdays)
  values (0, "Whole Week", 7);
flush table dayofanyweek;

--
-- drop any new tables created 
--

-- flush tables;

drop table if exists dayweighting1;
drop table if exists dayweighting1sum;
drop table if exists dayweighting1normalized;
drop table if exists dayweighting2;
drop table if exists dayweighting2normalized;
drop table if exists dayweighting3;
drop table if exists dayweighting3normalized;
drop table if exists oldhourday;
drop table if exists aggdayvmtfraction;
drop table if exists aggsourcetypedayvmt;
drop table if exists agghpmsvtypeday;
drop table if exists oldavgspeeddistribution;
drop table if exists oldopmodedistribution;    
drop table if exists oldopmodedistribution2;
drop table if exists oldsourcetypehour;
drop table if exists oldsho;
drop table if exists oldsourcehours;
drop table if exists oldstarts;
drop table if exists oldextendedidlehours;
drop table if exists oldhotellinghourfraction;
drop table if exists oldhotellinghoursperday;
drop table if exists oldsamplevehiclesoakingday;
drop table if exists oldsamplevehicletrip;
drop table if exists oldsamplevehicleday;
drop table if exists oldstartspervehicle;
drop table if exists oldstartsopmodedistribution;
drop table if exists oldstartshourfraction;
drop table if exists oldstartsperdaypervehicle;
drop table if exists oldstartsperday;
drop table if exists oldaveragetanktemperature;
drop table if exists oldsoakactivityfraction;
drop table if exists oldtotalidlefraction;
drop table if exists oldidledayadjust;

-- flush tables;
  
