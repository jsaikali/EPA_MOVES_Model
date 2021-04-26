/* ***********************************************************************************************************************
-- mysql script preaggregate the movesexecution database
--    to the state level after merging of user input databases by inputdatamanager
--    and before any masterloopable objects are executed.
-- an attempt is made to weight some aggregations by activity.
-- two tables involved have missing data default data handling.
--
-- author mitch cumberworth
-- author gwo shyu
-- author wesley faler
-- author jarrod brown, michael aldridge, daniel bizer-cox, evan murray
-- version 2019-04-22
-- *********************************************************************************************************************** */

-- flush tables;

drop table if exists surrogateactivity;
drop table if exists oldcounty;
drop table if exists oldlink;
drop table if exists oldzone;
drop table if exists aggzone;
drop table if exists oldyear;
drop table if exists oldzonemonthhour;
drop table if exists oldopmodedistribution; 
drop table if exists aggzoneroadtype;
drop table if exists oldfuelsupply;
drop table if exists aggfuelsupply; 
drop table if exists oldimcoverage; 
drop table if exists aggsho;
drop table if exists aggsourcehours;
drop table if exists aggstarts;
drop table if exists aggextendedidlehours; 
drop table if exists oldaveragetanktemperature;
drop table if exists oldsoakactivityfraction;
drop table if exists oldfuelusagefraction;
drop table if exists aggfuelusagefraction;
drop table if exists oldtotalidlefraction;

-- 
-- create a table to be used for activity-weighting by county or zone 
--
-- note that activity factors for a state must sum to unity.
-- first make a version of zone that includes stateid

-- select "MAKING SURROGATEACTIVITY TABLE" as marker_point;

create table oldzone
  select zoneid, county.countyid, stateid, startallocfactor, idleallocfactor, shpallocfactor
  from zone inner join county using (countyid);
create unique index index1 on oldzone (zoneid);
create index index2 on oldzone (countyid);

-- now we can aggregate the zone table by stateid
create table aggzone (
  zoneid integer,
  startallocfactor float,
  idleallocfactor float,
  shpallocfactor float,
  stateid smallint );
insert into aggzone
  select stateid*10000 as zoneid, 
    sum(startallocfactor) as startallocfactor, sum(idleallocfactor) as idleallocfactor,
    sum(shpallocfactor) as shpallocfactor, stateid
  from oldzone group by stateid;
create unique index index1 on aggzone (stateid);

-- finally we can make the table we want
create table surrogateactivity (
  zoneid integer,
  countyid integer,
  actfract float,
  primary key (zoneid, countyid),
  key (countyid) );
insert into surrogateactivity 
  select oz.zoneid, oz.countyid,  
  (round(oz.startallocfactor,6)/round(az.startallocfactor,6)) as actfract
  from oldzone as oz inner join aggzone as az using(stateid);
create unique index index1 on surrogateactivity (zoneid);
create index index2 on surrogateactivity (countyid);

-- 
-- surrogateregionactivity table
-- 
-- select "MAKING SURROGATEREGIONACTIVITY" as marker_point;
drop table if exists surrogateregionactivity;
create table surrogateregionactivity (
  stateid int not null,
  fuelregionid int not null,
  fuelyearid int not null,
  actfract double not null,
  primary key (stateid, fuelyearid, fuelregionid)
);

insert into surrogateregionactivity (stateid, fuelregionid, fuelyearid, actfract)
select stateid, regionid as fuelregionid, fuelyearid, sum(actfract)
from surrogateactivity
inner join regioncounty using (countyid)
inner join county using (countyid)
where regioncodeid=1
group by stateid, regionid, fuelyearid;

drop table if exists surrogateregionactivitytotal;
create table surrogateregionactivitytotal (
  stateid int not null,
  fuelyearid int not null,
  actfracttotal double not null,
  primary key (stateid, fuelyearid)
);
insert into surrogateregionactivitytotal (stateid, fuelyearid, actfracttotal)
select stateid, fuelyearid, sum(actfract)
from surrogateregionactivity
group by stateid, fuelyearid;

update surrogateregionactivity, surrogateregionactivitytotal
set actfract = actfract/actfracttotal
where surrogateregionactivity.fuelyearid = surrogateregionactivitytotal.fuelyearid
and surrogateregionactivity.stateid = surrogateregionactivitytotal.stateid;

--
-- state table  - no changes required
--
  
--
-- county table
--
-- select "MAKING AGGREGATE COUNTY AND ZONE TABLES" as marker_point;
create table oldcounty select county.*, statename 
  from county inner join state using (stateid);
truncate county;
insert into county (countyid, stateid, countyname, altitude, gpafract, 
    barometricpressure, barometricpressurecv, countytypeid)
  select stateid*1000 as countyid, stateid, 
    statename as countyname, "L" as altitude, sum(gpafract*actfract) as gpafract,
    sum(barometricpressure*actfract) as barometricpressure, null as barometricpressurecv,
    1 as countytypeid
  from oldcounty inner join surrogateactivity using (countyid)
  group by stateid;
flush table county;
  
--
-- countyyear table
--
create table oldyear select distinct yearid from countyyear;
truncate countyyear;
insert into countyyear (countyid, yearid)
  select countyid, yearid
  from county, oldyear;
flush table countyyear;
  
--
-- zone table
--
truncate zone;
insert into zone (zoneid, countyid, startallocfactor, idleallocfactor, shpallocfactor)
  select  zoneid, stateid*1000 as countyid, startallocfactor, idleallocfactor, shpallocfactor
  from aggzone;
flush table zone;
  
-- 
-- link table
-- 
-- select "MAKING AGGREGATE LINK TABLE"as marker_point;
create table oldlink
  select link.*, stateid
  from link inner join oldzone using(zoneid);
create unique index index1 on oldlink (linkid); 
truncate link;
insert into link (linkid, countyid, zoneid, roadtypeid, 
    linklength,linkvolume, linkavgspeed, linkdescription, linkavggrade)
  select (stateid*100000+roadtypeid) as linkid,
    stateid*1000 as countyid, 
    stateid*10000 as zoneid,
    roadtypeid as roadtypeid,
    null as linklength, null as linkvolume, 
    sum(linkavgspeed * actfract) as linkavgspeed,
    null as linkdescription,
    sum(linkavggrade * actfract) as linkavggrade
  from oldlink inner join surrogateactivity using(zoneid)
  group by stateid, roadtypeid;
flush table link;


-- 
-- zonemonthhour
--
-- select "MAKING AGGREGATE ZONEMONTHHOUR TABLE" as marker_point;
create table oldzonemonthhour
  select monthid, stateid, zmh.zoneid, hourid, temperature, relhumidity
  from zonemonthhour as zmh inner join oldzone using (zoneid);
truncate zonemonthhour;
insert into zonemonthhour (monthid, zoneid, hourid, temperature, temperaturecv,
    relhumidity, relativehumiditycv, heatindex, specifichumidity)
  select monthid, stateid*10000 as zoneid, hourid, 
  sum(temperature*actfract) as temperature, null as temperaturecv, 
  sum(relhumidity*actfract) as relhumidity, null as relativehumiditycv,
    0.0 as heatindex, 0.0 as specifichumidity
  from oldzonemonthhour inner join surrogateactivity using(zoneid)
  group by monthid, hourid, stateid;
flush table zonemonthhour;

--
-- opmodedistribution
--
-- select "MAKING AGGREGATE OPMODEDISTRIBUTION TABLE" as marker_point;
create table oldopmodedistribution 
  select omd.*, link.roadtypeid, link.stateid, link.zoneid 
  from opmodedistribution as omd inner join oldlink as link using (linkid);
truncate opmodedistribution;
insert into opmodedistribution (sourcetypeid, hourdayid,linkid, polprocessid, opmodeid, 
    opmodefraction, opmodefractioncv, isuserinput)
  select sourcetypeid, hourdayid, 
    (stateid*100000+roadtypeid) as linkid, polprocessid, opmodeid,
    sum(opmodefraction * actfract) as opmodefraction, null as opmodefractioncv,
    "Y" as isuserinput
  from oldopmodedistribution inner join surrogateactivity using (zoneid)
  group by sourcetypeid, hourdayid, stateid, roadtypeid, polprocessid, opmodeid;
flush table opmodedistribution;
  
--
-- zoneroadtype
--
-- select "MAKING AGGREGATE ZONEROADTYPE TABLE" as marker_point;
create table aggzoneroadtype (
  zoneid integer,
  roadtypeid smallint,
  shoallocfactor double);
insert into aggzoneroadtype 
  select (stateid*10000) as zoneid, roadtypeid,
    sum(shoallocfactor) as shoallocfactor
  from zoneroadtype inner join oldzone using(zoneid)
  group by stateid, roadtypeid ;
truncate zoneroadtype;
insert into zoneroadtype (zoneid, roadtypeid, shoallocfactor)
  select * from aggzoneroadtype;
flush table zoneroadtype;
  
--
-- fuel supply
--
-- note: algorithm is specific to particular default values used.
-- select "MAKING AGGREGATE FUELSUPPLY TABLE" as marker_point;
create table oldfuelsupply
  select distinct fs.*, stateid
  from fuelsupply as fs
  inner join regioncounty on (regionid=fuelregionid and regioncodeid=1 and regioncounty.fuelyearid=fs.fuelyearid)
  inner join oldcounty using(countyid);
-- creating table explicitly to control column type and avoid significance problem
create table aggfuelsupply (
  stateid smallint,
  fuelyearid smallint,
  monthgroupid smallint,
  fuelformulationid smallint,
  havefract double);
insert into aggfuelsupply
  select stateid, fs.fuelyearid, monthgroupid,fuelformulationid, 
    sum(marketshare*actfract) as havefract
  from oldfuelsupply as fs
  inner join surrogateregionactivity using (stateid,fuelregionid,fuelyearid)
  group by stateid, fs.fuelyearid, monthgroupid, fuelformulationid;
truncate fuelsupply;  
insert into fuelsupply (fuelregionid, fuelyearid, monthgroupid, fuelformulationid, 
   marketshare, marketsharecv)
  select stateid as fuelregionid, fuelyearid, monthgroupid, fuelformulationid, 
    havefract as marketshare, 
    null as marketsharecv
  from aggfuelsupply;
flush table fuelsupply;

truncate region;
insert into region (regionid,description)
select distinct stateid, 'State region'
from aggfuelsupply;

truncate regioncounty;
insert into regioncounty (regionid, countyid, regioncodeid, fuelyearid)
select distinct fuelregionid as regionid, fuelregionid*1000 as countyid, 1 as regioncodeid, fuelyearid
from fuelsupply;

insert into regioncounty (regionid, countyid, regioncodeid, fuelyearid)
select distinct fuelregionid as regionid, fuelregionid*1000 as countyid, 2 as regioncodeid, fuelyearid
from fuelsupply;

--
-- e10 fuel properties
--
insert ignore into e10fuelproperties (fuelregionid,fuelyearid,monthgroupid,rvp,sulfurlevel,etohvolume,mtbevolume,
                                      etbevolume,tamevolume,aromaticcontent,olefincontent,benzenecontent,e200,e300,
                    biodieselestervolume,cetaneindex,pahcontent,t50,t90)
  select stateid as fuelregionid, fuelyearid, monthgroupid, 
       sum(rvp*actfract) as rvp,
       sum(sulfurlevel*actfract) as sulfurlevel,
       sum(etohvolume*actfract) as etohvolume,
       sum(mtbevolume*actfract) as mtbevolume,
       sum(etbevolume*actfract) as etbevolume,
       sum(tamevolume*actfract) as tamevolume,
       sum(aromaticcontent*actfract) as aromaticcontent,
       sum(olefincontent*actfract) as olefincontent,
       sum(benzenecontent*actfract) as benzenecontent,
       sum(e200*actfract) as e200,
       sum(e300*actfract) as e300,
       sum(biodieselestervolume*actfract) as biodieselestervolume,
       sum(cetaneindex*actfract) as cetaneindex,
       sum(pahcontent*actfract) as pahcontent,
       sum(t50*actfract) as t50,
       sum(t90*actfract) as t90
  from e10fuelproperties
  join surrogateregionactivity using (fuelregionid,fuelyearid)
  group by stateid, fuelyearid, monthgroupid;

--
-- fuel usage
--
-- select "MAKING AGGREGATE FUELUSAGEFRACTION TABLE" as marker_point;
create table oldfuelusagefraction
  select f.*, stateid
  from fuelusagefraction as f inner join oldcounty using(countyid) ;
-- creating table explicitly to control column type and avoid significance problem
create table aggfuelusagefraction (
  stateid smallint,
  fuelyearid smallint,
  modelyeargroupid int,
  sourcebinfueltypeid smallint,
  fuelsupplyfueltypeid smallint,
  usagefraction double
);
insert into aggfuelusagefraction
  select stateid, f.fuelyearid, modelyeargroupid, sourcebinfueltypeid, fuelsupplyfueltypeid,
    sum(usagefraction*actfract) as usagefraction
  from oldfuelusagefraction as f inner join surrogateactivity using(countyid)
  group by stateid, fuelyearid, modelyeargroupid, sourcebinfueltypeid, fuelsupplyfueltypeid;
truncate fuelusagefraction;
insert into fuelusagefraction (countyid, fuelyearid, modelyeargroupid, sourcebinfueltypeid, fuelsupplyfueltypeid,
   usagefraction)
  select (stateid*1000) as countyid, fuelyearid, modelyeargroupid, sourcebinfueltypeid, fuelsupplyfueltypeid,
    least(usagefraction,1.0)
  from aggfuelusagefraction;
flush table fuelusagefraction;

--
-- imcoverage
--
-- select "MAKING IMCOVERAGE TABLE" as marker_point;
create table oldimcoverage select * from imcoverage where useimyn = 'Y';
create index oldimcoverageindex1 on oldimcoverage (countyid);
truncate imcoverage;
-- when aggregated, im programs may overlap.  this is better than extending model years
-- or forcing all to a particular imfactor entry.  the overlap is handled within each
-- calculator.
drop table imcoverage;
create table imcoverage (
  stateid int(11) default null,
  countyid int(11) not null,
  yearid smallint(6) not null,
  polprocessid int not null,
  fueltypeid smallint(6) not null,
  sourcetypeid smallint(6) not null,
  improgramid smallint(6) not null default '0',
  inspectfreq smallint(6) default null,
  teststandardsid smallint(6) default null,
  begmodelyearid smallint(4) default null,
  endmodelyearid smallint(4) default null,
  useimyn char(1) not null default 'N',
  compliancefactor float default null,
  key xpkimcoverage (polprocessid,countyid,yearid,sourcetypeid,fueltypeid,improgramid)
);

-- add back all of the old imcoverage records, but use the pseudo county's id
insert into imcoverage (stateid, countyid, yearid, polprocessid, fueltypeid,
  sourcetypeid, improgramid, inspectfreq, teststandardsid,
  begmodelyearid, endmodelyearid, useimyn,
  compliancefactor)
select stateid, (stateid*1000) as countyid, yearid, polprocessid, fueltypeid,
  sourcetypeid, improgramid, inspectfreq, teststandardsid,
  begmodelyearid, endmodelyearid, useimyn,
  (compliancefactor*actfract) as compliancefactor
from oldimcoverage
inner join surrogateactivity using(countyid);
   
--
--  sho    
--
-- select "MAKING AGGREGATE SHO TABLE" as marker_point;
create table aggsho (
  hourdayid smallint,
  monthid smallint,
  yearid smallint,
  ageid smallint,
  stateid smallint,
  roadtypeid smallint,
  sourcetypeid smallint,
  sho float,
  distance float);
insert into aggsho  
  select hourdayid, monthid, yearid, ageid, stateid, roadtypeid, sourcetypeid, 
    sum(sho) as sho, sum(distance) as distance
  from sho inner join oldlink using(linkid)
  group by hourdayid, monthid, yearid, ageid, stateid, roadtypeid, sourcetypeid;
truncate sho;
insert into sho (hourdayid, monthid, yearid, ageid, linkid, 
    sourcetypeid, sho, shocv, distance, isuserinput)
  select hourdayid, monthid, yearid, ageid,
    (stateid*100000+roadtypeid) as linkid, 
    sourcetypeid, sho, null as shocv, distance, "Y" as isuserinput
  from aggsho;
flush table sho;

--
--  sourcehours    
--
-- select "MAKING AGGREGATE SOURCEHOURS TABLE" as marker_point;
create table aggsourcehours (
  hourdayid smallint,
  monthid smallint,
  yearid smallint,
  ageid smallint,
  stateid smallint,
  roadtypeid smallint,
  sourcetypeid smallint,
  sourcehours float);
insert into aggsourcehours  
  select hourdayid, monthid, yearid, ageid, stateid, roadtypeid, sourcetypeid, 
    sum(sourcehours) as sourcehours
  from sourcehours inner join oldlink using(linkid)
  group by hourdayid, monthid, yearid, ageid, stateid, roadtypeid, sourcetypeid;
truncate sourcehours;
insert into sourcehours (hourdayid, monthid, yearid, ageid, linkid, 
    sourcetypeid, sourcehours, sourcehourscv,isuserinput)
  select hourdayid, monthid, yearid, ageid,
    (stateid*100000+roadtypeid) as linkid, 
    sourcetypeid, sourcehours, null as sourcehourscv,"Y" as isuserinput
  from aggsourcehours;
flush table sourcehours;

    
--
--  starts
--
-- select "MAKING AGGREGATE STARTS TABLE" as marker_point;
create table aggstarts (
  hourdayid smallint,
  monthid smallint,
  yearid smallint,
  ageid smallint,
  stateid smallint,
  sourcetypeid smallint,
  starts float
  );
insert into aggstarts
  select hourdayid, monthid, yearid, ageid, stateid, sourcetypeid, 
    sum(starts) as starts
  from starts inner join oldzone using (zoneid) 
  group by hourdayid, monthid, yearid, ageid, stateid, sourcetypeid;
truncate starts;
insert into starts (hourdayid, monthid, yearid, ageid, zoneid, 
    sourcetypeid, starts, startscv, isuserinput)
  select hourdayid, monthid, yearid, ageid, 
    (stateid*10000) as zoneid, 
    sourcetypeid, starts, null as startscv, "Y" as isuserinput
  from aggstarts;
flush table starts;

--
--  extendedidlehours
--
-- select "MAKING AGGREGATE EXTENDEDIDLEHOURS TABLE" as marker_point;
create table aggextendedidlehours (
  sourcetypeid smallint,
  hourdayid smallint,
  monthid smallint,
  yearid smallint,
  ageid smallint,
  stateid smallint,
  extendedidlehours float);
insert into aggextendedidlehours  
  select sourcetypeid, hourdayid, monthid, yearid, ageid, stateid, 
    sum(extendedidlehours) as extendedidlehours
  from extendedidlehours inner join oldzone using(zoneid)
  group by sourcetypeid, hourdayid, monthid, yearid, ageid, stateid; 
truncate extendedidlehours;
insert into extendedidlehours (sourcetypeid, hourdayid, monthid, yearid, ageid, zoneid, 
     extendedidlehours, extendedidlehourscv, isuserinput)
  select sourcetypeid, hourdayid, monthid, yearid, ageid, 
    (stateid*10000) as zoneid, 
    extendedidlehours, null as extendedidlehourscv, "Y" as isuserinput
  from aggextendedidlehours;  
flush table extendedidlehours;
  
-- 
-- averagetanktemperature
--
-- select "MAKING AVERAGETANKTEMPERATURE TABLE" as marker_point;
create table oldaveragetanktemperature
  select tanktemperaturegroupid, stateid, monthid, att.zoneid, hourdayid,
      opmodeid, averagetanktemperature
  from averagetanktemperature as att inner join oldzone using (zoneid);
truncate averagetanktemperature;
insert into averagetanktemperature (tanktemperaturegroupid, zoneid, monthid, hourdayid,
    opmodeid, averagetanktemperature, averagetanktemperaturecv, isuserinput)
  select tanktemperaturegroupid, stateid*10000 as zoneid, monthid, hourdayid, opmodeid,
  sum(averagetanktemperature*actfract) as averagetanktemperature, 
  null as averagetanktemperaturecv, 'Y' as isuserinput
  from oldaveragetanktemperature inner join surrogateactivity using(zoneid)
  group by tanktemperaturegroupid, stateid, monthid, hourdayid, opmodeid;
flush table averagetanktemperature;

-- 
-- soakactivityfraction
--
-- select "MAKING SOAKACTIVITYFRACTION" as marker_point;
create table oldsoakactivityfraction
  select sourcetypeid, stateid, saf.zoneid, monthid, hourdayid, opmodeid, soakactivityfraction
    from soakactivityfraction as saf inner join oldzone using (zoneid);
truncate soakactivityfraction;
insert into soakactivityfraction (sourcetypeid, zoneid, monthid, hourdayid, opmodeid, 
    soakactivityfraction, soakactivityfractioncv, isuserinput)
  select sourcetypeid, stateid*10000 as zoneid, monthid, hourdayid, opmodeid,
  sum(soakactivityfraction*actfract) as soakactivityfraction, null as soakactivityfractioncv,
  'Y' as isuserinput 
  from oldsoakactivityfraction inner join surrogateactivity using(zoneid)
  group by sourcetypeid, stateid, monthid, hourdayid, opmodeid;
flush table soakactivityfraction;

-- 
-- coldsoaktanktemperature
--
-- select "MAKING AGGREGATE COLDSOAKTANKTEMPERATURE TABLE" as marker_point;
create table oldcoldsoaktanktemperature
  select monthid, stateid, cstt.zoneid, hourid, coldsoaktanktemperature
  from coldsoaktanktemperature as cstt inner join oldzone using (zoneid);
truncate coldsoaktanktemperature;
insert into coldsoaktanktemperature (monthid, zoneid, hourid, coldsoaktanktemperature)
  select monthid, stateid*10000 as zoneid, hourid, 
  sum(coldsoaktanktemperature*actfract) as coldsoaktanktemperature
  from oldcoldsoaktanktemperature inner join surrogateactivity using(zoneid)
  group by monthid, hourid, stateid;
flush table coldsoaktanktemperature;

-- 
-- coldsoakinitialhourfraction
--
-- select "MAKING COLDSOAKINITIALHOURFRACTION" as marker_point;
create table oldcoldsoakinitialhourfraction
  select sourcetypeid, monthid, stateid, old.zoneid, hourdayid, initialhourdayid, coldsoakinitialhourfraction
  from coldsoakinitialhourfraction as old inner join oldzone using (zoneid);
truncate coldsoakinitialhourfraction;
insert into coldsoakinitialhourfraction (sourcetypeid, monthid, zoneid, hourdayid, initialhourdayid, 
  coldsoakinitialhourfraction, isuserinput)
  select sourcetypeid, monthid, stateid*10000 as zoneid, hourdayid, initialhourdayid, 
  sum(coldsoakinitialhourfraction*actfract) as coldsoakinitialhourfraction,
  'Y' as isuserinput
  from oldcoldsoakinitialhourfraction inner join surrogateactivity using(zoneid)
  group by sourcetypeid, monthid, stateid, hourdayid, initialhourdayid;
flush table coldsoakinitialhourfraction;

-- 
-- averagetankgasoline
--
-- select "MAKING AVERAGETANKGASOLINE" as marker_point;
create table oldaveragetankgasoline
  select stateid, old.zoneid, fueltypeid, fuelyearid, monthgroupid, etohvolume, rvp
  from averagetankgasoline as old inner join oldzone using (zoneid);
truncate averagetankgasoline;
insert into averagetankgasoline (zoneid, fueltypeid, fuelyearid, monthgroupid, etohvolume, rvp, isuserinput)
  select stateid*10000 as zoneid, fueltypeid, fuelyearid, monthgroupid, 
  sum(etohvolume*actfract) as etohvolume,
  sum(rvp*actfract) as rvp,
  'Y' as isuserinput
  from oldaveragetankgasoline inner join surrogateactivity using(zoneid)
  group by stateid, fueltypeid, fuelyearid, monthgroupid;
flush table averagetankgasoline;

--
-- totalidlefraction
--
-- select "MAKING TOTALIDLEFRACTION" as marker_point;
create table oldtotalidlefraction
  select sourcetypeid, minmodelyearid, maxmodelyearid, monthid, dayid, idleregionid, countytypeid, totalidlefraction
    from totalidlefraction;
truncate totalidlefraction;
insert into totalidlefraction (sourcetypeid, minmodelyearid, maxmodelyearid, monthid, dayid, idleregionid, countytypeid, totalidlefraction)
  select sourcetypeid,  minmodelyearid, maxmodelyearid, monthid, dayid, idleregionid, 1 as countytypeid, totalidlefraction
  from oldtotalidlefraction
  where countytypeid = 1;
flush table totalidlefraction;



--
-- drop any new tables created 
--
-- select "DROPPING TEMPORARY TABLES";
-- drop table if exists surrogateactivity;

drop table if exists oldcounty;
drop table if exists oldlink;
drop table if exists oldzone;
drop table if exists aggzone;
drop table if exists oldyear;
drop table if exists oldzonemonthhour;
drop table if exists oldopmodedistribution; 
drop table if exists aggzoneroadtype;
drop table if exists oldfuelsupply;
drop table if exists aggfuelsupply; 
drop table if exists oldimcoverage; 
drop table if exists aggsho;
drop table if exists aggsourcehours;
drop table if exists aggstarts;
drop table if exists aggextendedidlehours; 
drop table if exists oldaveragetanktemperature;
drop table if exists oldsoakactivityfraction;
drop table if exists oldfuelusagefraction;
drop table if exists aggfuelusagefraction;
drop table if exists oldtotalidlefraction;

-- flush tables;
