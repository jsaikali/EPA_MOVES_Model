/* ***********************************************************************************************************************
-- mysql script file to pre-aggregate the county level database to the national level
--    (but preserving the roadtype (link) distinctions which exist within counties.)
-- an attempt is made to weight some aggregations by activity.
-- these weightings assume that all states and counties are included
--
-- author wesley faler
-- author mitch cumberworth
-- author gwo shyu
-- author jarrod brown, michael aldridge, daniel bizer-cox, evan murray
-- version 2019-04-22
-- *********************************************************************************************************************** */


drop table if exists surrogateactivity;
drop table if exists oldcounty;
drop table if exists oldyear;
drop table if exists oldlink;
drop table if exists aggzonemonthhour;   
drop table if exists oldopmodedistribution; 
drop table if exists aggzoneroadtype;
drop table if exists aggfuelsupply;
drop table if exists aggnrfuelsupply;
drop table if exists oldimcoverage;  
drop table if exists aggsho;
drop table if exists aggsourcehours;
drop table if exists aggstarts;
drop table if exists aggextendedidlehours; 
drop table if exists aggaveragetanktemperature;
drop table if exists aggsoakactivityfraction;
drop table if exists aggfuelusagefraction;
drop table if exists oldtotalidlefraction;

-- since "nation" does not include the virgin islands or
-- puerto rico, remove their information from state, county, zone,
-- zoneroadtype, and all nonroad tables as well.
delete from zone
using state
inner join county on (county.stateid=state.stateid)
inner join zone on (zone.countyid=county.countyid)
where state.stateid in (72,78);

delete from zoneroadtype
where round(zoneid/10000, 0) in (72,78);

delete from nrbaseyearequippopulation where stateid in (72,78);
delete from nrgrowthpatternfinder where stateid in (72,78);
delete from nrmonthallocation where stateid in (72,78);
delete from nrstatesurrogate where stateid in (72,78);

delete from county
using state
inner join county on (county.stateid=state.stateid)
where state.stateid in (72,78);

delete from state
where state.stateid in (72,78);


--
-- flush tables;
--
-- create a table to be used for activity-weighting by zone or county
--
-- select "making surrogateactivity" as marker_point;

create table surrogateactivity (
  zoneid int not null,
  countyid int not null,
  actfract double not null,
  primary key (zoneid, countyid),
  key (countyid, zoneid)
);

insert into surrogateactivity (zoneid, countyid, actfract)
  select zoneid, countyid, startallocfactor as actfract from zone;
  
drop table if exists surrogateactivitytotal;
create table surrogateactivitytotal
select sum(actfract) as nationalactivityfraction
from surrogateactivity;


-- 
-- surrogatestateactivity table
-- 
-- select "making surrogatestateactivity" as marker_point;

drop table if exists surrogatestateactivity;
create table surrogatestateactivity (
  stateid int not null,
  actfract double not null,
  primary key (stateid)
);

insert into surrogatestateactivity (stateid, actfract)
select c.stateid, 
  case when nationalactivityfraction <= 0 then 0 
  else sum(actfract)/nationalactivityfraction
  end as actfract
from surrogateactivity sa
inner join county c using (countyid)
inner join surrogateactivitytotal t
where t.nationalactivityfraction > 0
group by c.stateid;

-- 
-- surrogatecountyactivity table
-- 
-- select "making surrogatecountyactivity" as marker_point;
drop table if exists surrogatecountyactivity;
create table surrogatecountyactivity (
  countyid int not null,
  actfract double not null,
  primary key (countyid)
);

insert into surrogatecountyactivity (countyid, actfract)
select countyid, 
  case when nationalactivityfraction <= 0 then 0 
  else sum(actfract)/nationalactivityfraction
  end as actfract
from surrogateactivity sa
inner join surrogateactivitytotal t
where t.nationalactivityfraction > 0
group by countyid;

-- 
-- surrogateregionactivity table
-- 
-- select "making surrogateregionactivity" as marker_point;
drop table if exists surrogateregionactivity;
create table surrogateregionactivity (
  fuelregionid int not null,
  fuelyearid int not null,
  actfract double not null,
  primary key (fuelregionid, fuelyearid)
);

insert into surrogateregionactivity (fuelregionid, fuelyearid, actfract)
select regionid as fuelregionid, fuelyearid, sum(actfract)
from surrogateactivity
inner join regioncounty using (countyid)
where regioncodeid=1
group by regionid, fuelyearid;

drop table if exists surrogateregionactivitytotal;
create table surrogateregionactivitytotal (
  fuelyearid int not null primary key,
  actfracttotal double not null
);
insert into surrogateregionactivitytotal (fuelyearid, actfracttotal)
select fuelyearid, sum(actfract)
from surrogateregionactivity
group by fuelyearid;

update surrogateregionactivity, surrogateregionactivitytotal
set actfract = actfract/actfracttotal
where surrogateregionactivity.fuelyearid = surrogateregionactivitytotal.fuelyearid;
    
--
-- state table
--
-- select "making state" as marker_point;
truncate state;
insert into state (stateid, statename, stateabbr, idleregionid)
  values (0, "Nation", "US", 1);
flush table state;
  
--
-- county table
--
-- select "making county" as marker_point;
create table oldcounty select * from county;
truncate county;
insert into county (countyid, stateid, countyname, altitude, gpafract, 
    barometricpressure, barometricpressurecv, countytypeid)
  select 0, 0, "Nation", "L", sum(gpafract*actfract) as gpafract,
      sum(barometricpressure*actfract) as barometricpressure, null as barometricpressurecv,
      1 as countytypeid
  from oldcounty inner join surrogateactivity using (countyid);
flush table county;
  
--
-- countyyear table
--
-- select "making countyyear" as marker_point;
create table oldyear select distinct yearid from countyyear;
truncate countyyear;
replace into countyyear (countyid, yearid)
  select 0 as countyid, yearid
  from oldyear;
flush table countyyear;
  
--
-- zone table
--
-- select "making zone" as marker_point;
truncate zone;
insert into zone (zoneid, countyid, startallocfactor, idleallocfactor, shpallocfactor)
  values (0, 0, 1.0, 1.0, 1.0);
flush table zone;
  
-- 
-- link table
-- 
-- select "making link" as marker_point;
create table oldlink
  select * from link;  
truncate link;
insert into link (linkid, countyid, zoneid, roadtypeid, 
    linklength,linkvolume, linkavgspeed, linkdescription, linkavggrade)
  select roadtypeid as linkid,0 as countyid, 0 as zoneid, roadtypeid as roadtypeid,
    null as linklength, null as linkvolume, sum(linkavgspeed * actfract) as linkavgspeed,
    null as linkdescription,
    sum(linkavggrade * actfract) as linkavggrade
  from oldlink inner join surrogateactivity using(zoneid)
  group by roadtypeid;
flush table link;

-- 
-- zonemonthhour
--
-- select "making zonemonthhour" as marker_point;
create table aggzonemonthhour (
  monthid smallint,
  hourid smallint,
  temperature float,
  relhumidity float);
insert into aggzonemonthhour  
  select monthid, hourid, sum(temperature*actfract)/nationalactivityfraction as temperature,
    sum(relhumidity*actfract)/nationalactivityfraction as relhumidity
  from zonemonthhour inner join surrogateactivity using (zoneid) join surrogateactivitytotal
  group by monthid, hourid;
create unique index index1 on aggzonemonthhour (monthid, hourid);  
truncate zonemonthhour;
replace into zonemonthhour (monthid, zoneid, hourid, temperature, temperaturecv,
    relhumidity, relativehumiditycv, heatindex, specifichumidity)
  select monthid, 0 as zoneid, hourid, temperature,
    null as temperaturecv, relhumidity, null as relativehumiditycv, 
    0.0 as heatindex, 0.0 as specifichumidity 
  from aggzonemonthhour 
  group by monthid, hourid;
flush table zonemonthhour;

--
-- opmodedistribution
--
-- select "making opmodedistribution" as marker_point;
create table oldopmodedistribution 
  select omd.*, link.roadtypeid, link.zoneid 
  from opmodedistribution as omd inner join oldlink as link using (linkid);
truncate opmodedistribution;
insert into opmodedistribution (sourcetypeid, hourdayid, linkid, polprocessid, opmodeid, 
    opmodefraction, opmodefractioncv, isuserinput)
  select sourcetypeid, hourdayid, roadtypeid as linkid, polprocessid, opmodeid,
    sum(opmodefraction * actfract) as opmodefraction, null as opmodefractioncv,
    "Y" as isuserinput
  from oldopmodedistribution inner join surrogateactivity using (zoneid)
  group by sourcetypeid, hourdayid, roadtypeid, polprocessid, opmodeid;
flush table opmodedistribution;
  
--
-- zoneroadtype
--
-- select "making zoneroadtype" as marker_point;
create table aggzoneroadtype (
  roadtypeid smallint,
  shoallocfactor double);
insert into aggzoneroadtype 
  select  roadtypeid, sum(1.0000000000000 * shoallocfactor) as shoallocfactor
  from zoneroadtype 
  group by roadtypeid ;
truncate zoneroadtype;
replace into zoneroadtype (zoneid, roadtypeid, shoallocfactor)
  select 0 as zoneid,  roadtypeid, shoallocfactor
  from aggzoneroadtype;
flush table zoneroadtype;
   
--
-- fuel supply
--
-- note: algorithm is specific to particular default values used.
-- select "making fuelsupply" as marker_point;
--  creating table explicitly to control column types and avoid significance problems
create table aggfuelsupply (
  fuelyearid smallint,
  monthgroupid smallint,
  fuelformulationid smallint,
  havefract double);
insert into aggfuelsupply
  select fuelsupply.fuelyearid, monthgroupid,fuelformulationid, 
    sum(marketshare*actfract) as havefract
  from fuelsupply
  inner join surrogateregionactivity using (fuelregionid,fuelyearid)
  group by fuelyearid, monthgroupid, fuelformulationid;
truncate fuelsupply;  
replace into fuelsupply (fuelregionid, fuelyearid, monthgroupid, fuelformulationid, 
   marketshare, marketsharecv)
  select 0 as fuelregionid, fuelyearid, monthgroupid, fuelformulationid, 
    havefract as marketshare, 
    null as marketsharecv
  from aggfuelsupply; 
flush table fuelsupply;

--
-- nonroad fuel supply
--
-- note: algorithm is specific to particular default values used.
-- select "making nrfuelsupply" as marker_point;
--  creating table explicitly to control column types and avoid significance problems
create table aggnrfuelsupply (
  fuelyearid smallint,
  monthgroupid smallint,
  fuelformulationid smallint,
  havefract double);
insert into aggnrfuelsupply
  select nrfuelsupply.fuelyearid, monthgroupid,fuelformulationid, 
    sum(marketshare*actfract) as havefract
  from nrfuelsupply
  inner join surrogateregionactivity using (fuelregionid,fuelyearid)
  group by fuelyearid, monthgroupid, fuelformulationid;
truncate nrfuelsupply;  
replace into nrfuelsupply (fuelregionid, fuelyearid, monthgroupid, fuelformulationid, 
   marketshare, marketsharecv)
  select 0 as fuelregionid, fuelyearid, monthgroupid, fuelformulationid, 
    havefract as marketshare, 
    null as marketsharecv
  from aggnrfuelsupply; 
flush table nrfuelsupply;

truncate region;
insert into region (regionid, description) values (0,'aggregated nation');

truncate regioncounty;
insert into regioncounty (regionid, countyid, regioncodeid, fuelyearid)
select distinct 0 as regionid, 0 as countyid, 1 as regioncodeid, fuelyearid
from fuelsupply;

insert into regioncounty (regionid, countyid, regioncodeid, fuelyearid)
select distinct 0 as regionid, 0 as countyid, 2 as regioncodeid, fuelyearid
from fuelsupply;

--
-- fuel usage
--
-- select "making fuelusagefraction" as marker_point;
--  creating table explicitly to control column types and avoid significance problems
-- 
-- because fuel usage fraction is used early in the tag, reductions here for pr/vi propagate to all
-- activity calculations (i.e., for every activitytypeid). this is why this table is not normalized
-- to total national activity.
-- 
create table aggfuelusagefraction (
  fuelyearid smallint,
  modelyeargroupid int,
  sourcebinfueltypeid smallint,
  fuelsupplyfueltypeid smallint,
  usagefraction double
);
insert into aggfuelusagefraction
  select f.fuelyearid, modelyeargroupid, sourcebinfueltypeid, fuelsupplyfueltypeid,
    sum(usagefraction*actfract) as usagefraction
  from fuelusagefraction as f inner join surrogateactivity using(countyid)
  group by fuelyearid, modelyeargroupid, sourcebinfueltypeid, fuelsupplyfueltypeid;
truncate fuelusagefraction;
replace into fuelusagefraction (countyid, fuelyearid, modelyeargroupid, sourcebinfueltypeid, fuelsupplyfueltypeid,
   usagefraction)
  select 0 as countyid, fuelyearid, modelyeargroupid, sourcebinfueltypeid, fuelsupplyfueltypeid,
    least(usagefraction,1)
  from aggfuelusagefraction;
flush table fuelusagefraction;
  
--
-- imcoverage
--
-- select "making imcoverage table" as marker_point;
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
  useimyn char(1) not null default 'Y',
  compliancefactor float default null,
  key xpkimcoverage (polprocessid,countyid,yearid,sourcetypeid,fueltypeid,improgramid)
);

-- add back all of the old imcoverage records, but use the pseudo county's id
insert into imcoverage (stateid, countyid, yearid, polprocessid, fueltypeid,
  sourcetypeid, improgramid, inspectfreq, teststandardsid,
  begmodelyearid, endmodelyearid, useimyn,
  compliancefactor)
select 0 as stateid, 0 as countyid, yearid, polprocessid, fueltypeid,
  sourcetypeid, improgramid, inspectfreq, teststandardsid,
  begmodelyearid, endmodelyearid, useimyn,
  (compliancefactor*actfract)/nationalactivityfraction as compliancefactor
from oldimcoverage
inner join surrogateactivity using(countyid) join surrogateactivitytotal;

--
--  sho    
--
-- select "making sho" as marker_point;

create table aggsho (
  hourdayid smallint,
  monthid smallint,
  yearid smallint,
  ageid smallint,
  roadtypeid smallint,
  sourcetypeid smallint,
  sho double,
  distance double);
insert into aggsho
  select hourdayid, monthid, yearid, ageid, roadtypeid, sourcetypeid, 
    sum(sho) as sho, sum(distance) as distance
  from sho inner join oldlink using(linkid)
  group by hourdayid, monthid, yearid, ageid, roadtypeid, sourcetypeid;
truncate sho;
insert into sho (hourdayid, monthid, yearid, ageid, linkid, 
    sourcetypeid, sho, shocv, distance, isuserinput)
  select hourdayid, monthid, yearid, ageid, roadtypeid as linkid, 
    sourcetypeid, sho, null as shocv, distance, "Y" as isuserinput
  from aggsho;
flush table sho;

--
--  sourcehours    
--
-- select "making sourcehours" as marker_point;

create table aggsourcehours (
  hourdayid smallint,
  monthid smallint,
  yearid smallint,
  ageid smallint,
  roadtypeid smallint,
  sourcetypeid smallint,
  sourcehours double);
insert into aggsourcehours
  select hourdayid, monthid, yearid, ageid, roadtypeid, sourcetypeid, 
    sum(sourcehours) as sourcehours
  from sourcehours inner join oldlink using(linkid)
  group by hourdayid, monthid, yearid, ageid, roadtypeid, sourcetypeid;
truncate sourcehours;
insert into sourcehours (hourdayid, monthid, yearid, ageid, linkid, 
    sourcetypeid, sourcehours, sourcehourscv,isuserinput)
  select hourdayid, monthid, yearid, ageid, roadtypeid as linkid, 
    sourcetypeid, sourcehours, null as sourcehourscv, "Y" as isuserinput
  from aggsourcehours;
flush table sourcehours;
  
--
--  starts
--
-- select "making starts" as marker_point;
create table aggstarts (
  hourdayid smallint,
  monthid smallint,
  yearid smallint,
  ageid smallint,
  sourcetypeid smallint,
  starts double);
insert into aggstarts
  select hourdayid, monthid, yearid, ageid, sourcetypeid, 
    sum(starts) as starts
  from starts 
  group by hourdayid, monthid, yearid, ageid, sourcetypeid;
truncate starts;
replace into starts (hourdayid, monthid, yearid, ageid, zoneid, 
    sourcetypeid, starts, startscv, isuserinput)
  select hourdayid, monthid, yearid, ageid, 0 as zoneid, 
    sourcetypeid, starts, null as startscv, "Y" as isuserinput
  from aggstarts;
flush table starts;
  
--
--  extendedidlehours
--
-- select "making extendedidlehours" as marker_point;
create table aggextendedidlehours (
  sourcetypeid smallint,
  hourdayid smallint,
  monthid smallint,
  yearid smallint,
  ageid smallint,
  extendedidlehours double);
insert into aggextendedidlehours
  select sourcetypeid, hourdayid, monthid, yearid, ageid,  
    sum(extendedidlehours) as extendedidlehours
  from extendedidlehours
  group by sourcetypeid, hourdayid, monthid, yearid, ageid; 
truncate extendedidlehours;
replace into extendedidlehours (sourcetypeid, hourdayid, monthid, yearid, ageid, zoneid, 
     extendedidlehours, extendedidlehourscv, isuserinput)
  select sourcetypeid, hourdayid, monthid, yearid, ageid, 0 as zoneid, 
    extendedidlehours, null as extendedidlehourscv, "Y" as isuserinput
  from aggextendedidlehours; 
flush table extendedidlehours;

-- 
-- averagetanktemperature
--
-- select "making averagetanktemperature" as marker_point;
create table aggaveragetanktemperature (
  tanktemperaturegroupid smallint,
  monthid smallint,
  hourdayid smallint,
  opmodeid smallint,
  averagetanktemperature float);
insert into aggaveragetanktemperature
  select tanktemperaturegroupid, monthid, hourdayid, opmodeid,
    sum(averagetanktemperature*actfract)/nationalactivityfraction as averagetanktemperature
  from averagetanktemperature inner join surrogateactivity using (zoneid) join surrogateactivitytotal
  group by tanktemperaturegroupid, monthid, hourdayid, opmodeid;


truncate averagetanktemperature;
replace into averagetanktemperature (tanktemperaturegroupid, zoneid, 
    monthid, hourdayid, opmodeid, averagetanktemperature, averagetanktemperaturecv,
    isuserinput)
  select tanktemperaturegroupid, 0 as zoneid, monthid, hourdayid, opmodeid, 
    averagetanktemperature, null as averagetanktemperaturecv, 'Y' as isuserinput
  from aggaveragetanktemperature;
flush table averagetanktemperature;

-- 
-- soakactivityfraction
--
-- select "making soakactivityfraction" as marker_point;
create table aggsoakactivityfraction (
  sourcetypeid smallint,
  monthid smallint,
  hourdayid smallint,
  opmodeid smallint,
  soakactivityfraction double);
insert into aggsoakactivityfraction
  select sourcetypeid, monthid, hourdayid, opmodeid,
    sum(soakactivityfraction*actfract)/nationalactivityfraction as soakactivityfraction
  from soakactivityfraction inner join surrogateactivity using (zoneid) join surrogateactivitytotal
  group by sourcetypeid, monthid, hourdayid, opmodeid;
truncate soakactivityfraction;
replace into soakactivityfraction (sourcetypeid, zoneid, 
    monthid, hourdayid, opmodeid, soakactivityfraction, soakactivityfractioncv, isuserinput)
  select sourcetypeid, 0 as zoneid, monthid, hourdayid, opmodeid, 
    soakactivityfraction, null as soakactivityfractioncv, 'Y' as isuserinput
  from aggsoakactivityfraction;
flush table soakactivityfraction;

-- 
-- coldsoaktanktemperature
--
-- select "making coldsoaktanktemperature" as marker_point;
create table aggcoldsoaktanktemperature (
  monthid smallint,
  hourid smallint,
  coldsoaktanktemperature float);
insert into aggcoldsoaktanktemperature
  select monthid, hourid, sum(coldsoaktanktemperature*actfract)/nationalactivityfraction as coldsoaktanktemperature
  from coldsoaktanktemperature inner join surrogateactivity using (zoneid) join surrogateactivitytotal
  group by monthid, hourid;
create unique index index1 on aggcoldsoaktanktemperature (monthid, hourid);  
truncate coldsoaktanktemperature;
replace into coldsoaktanktemperature (monthid, zoneid, hourid, coldsoaktanktemperature)
  select monthid, 0 as zoneid, hourid, coldsoaktanktemperature
  from aggcoldsoaktanktemperature
  group by monthid, hourid;
flush table coldsoaktanktemperature;

-- 
-- coldsoakinitialhourfraction
--
-- select "making coldsoakinitialhourfraction" as marker_point;
create table aggcoldsoakinitialhourfraction (
  sourcetypeid smallint,
  monthid smallint,
  hourdayid smallint,
  initialhourdayid smallint,
  coldsoakinitialhourfraction float);
insert into aggcoldsoakinitialhourfraction
  select sourcetypeid, monthid, hourdayid, initialhourdayid, sum(coldsoakinitialhourfraction*actfract)/nationalactivityfraction as coldsoakinitialhourfraction
  from coldsoakinitialhourfraction inner join surrogateactivity using (zoneid) join surrogateactivitytotal
  group by sourcetypeid, monthid, hourdayid, initialhourdayid;
create unique index index1 on aggcoldsoakinitialhourfraction (sourcetypeid, monthid, hourdayid, initialhourdayid);
truncate coldsoakinitialhourfraction;
replace into coldsoakinitialhourfraction (sourcetypeid, monthid, zoneid, hourdayid, initialhourdayid, 
  coldsoakinitialhourfraction, isuserinput)
  select sourcetypeid, monthid, 0 as zoneid, hourdayid, initialhourdayid, coldsoakinitialhourfraction,
    'Y' as isuserinput
  from aggcoldsoakinitialhourfraction 
  group by sourcetypeid, monthid, hourdayid, initialhourdayid;
flush table coldsoakinitialhourfraction;

-- 
-- averagetankgasoline
--
-- select "making averagetankgasoline" as marker_point;
create table aggaveragetankgasoline (
  fueltypeid smallint,
  fuelyearid smallint,
  monthgroupid smallint,
  etohvolume float,
  rvp float);
insert into aggaveragetankgasoline
  select fueltypeid, fuelyearid, monthgroupid, 
    sum(etohvolume*actfract)/nationalactivityfraction as etohvolume,
    sum(rvp*actfract)/nationalactivityfraction as rvp
  from averagetankgasoline inner join surrogateactivity using (zoneid) join surrogateactivitytotal
  group by fueltypeid, fuelyearid, monthgroupid;
create unique index index1 on aggaveragetankgasoline (fueltypeid, fuelyearid, monthgroupid);
truncate averagetankgasoline;
replace into averagetankgasoline (zoneid, fueltypeid, fuelyearid, monthgroupid, etohvolume, rvp, isuserinput)
  select 0 as zoneid, fueltypeid, fuelyearid, monthgroupid, etohvolume, rvp, 'Y' as isuserinput
  from aggaveragetankgasoline 
  group by fueltypeid, fuelyearid, monthgroupid;
flush table averagetankgasoline;

-- 
-- nrbaseyearequippopulation table
-- 
-- select "making nrbaseyearequippopulation" as marker_point;
drop table if exists oldnrbaseyearequippopulation;
create table oldnrbaseyearequippopulation
  select * from nrbaseyearequippopulation;
truncate nrbaseyearequippopulation;
insert into nrbaseyearequippopulation (sourcetypeid, stateid, population, nrbaseyearid)
  select sourcetypeid, 0 as stateid, sum(population) as population, nrbaseyearid
  from oldnrbaseyearequippopulation
  group by sourcetypeid, nrbaseyearid;
flush table nrbaseyearequippopulation;

-- 
-- nrgrowthpatternfinder table
-- 
-- select "making nrgrowthpatternfinder" as marker_point;
drop table if exists oldnrgrowthpatternfinder;
create table oldnrgrowthpatternfinder
  select * from nrgrowthpatternfinder;
truncate nrgrowthpatternfinder;
insert into nrgrowthpatternfinder (scc, stateid, growthpatternid)
  select scc, 0 as stateid, min(growthpatternid) as growthpatternid
  from oldnrgrowthpatternfinder
  group by scc;
flush table nrgrowthpatternfinder;

-- 
-- nrmonthallocation table
-- 
-- select "making nrmonthallocation" as marker_point;
truncate nrmonthallocation;
insert into nrmonthallocation (scc, stateid, monthid, monthfraction)
  select scc, stateid, monthid, monthfraction
  from nrusmonthallocation
  where stateid=0;
flush table nrmonthallocation;

-- 
-- nrstatesurrogate table
-- 
-- select "making nrstatesurrogate" as marker_point;
drop table if exists oldnrstatesurrogate;
create table oldnrstatesurrogate
  select * from nrstatesurrogate;
truncate nrstatesurrogate;
insert into nrstatesurrogate (surrogateid,stateid,countyid,surrogatequant,surrogateyearid)
  select surrogateid, 0 as stateid, 0 as countyid, sum(surrogatequant) as surrogatequant,surrogateyearid
  from oldnrstatesurrogate
  where stateid > 0 and countyid > 0
  and mod(countyid,1000) > 0
  group by surrogateid, surrogateyearid;
flush table nrstatesurrogate;

--
-- totalidlefraction
--
-- select "making totalidlefraction" as marker_point;
create table oldtotalidlefraction
  select sourcetypeid, minmodelyearid, maxmodelyearid, monthid, dayid, idleregionid, countytypeid, totalidlefraction
    from totalidlefraction;
truncate totalidlefraction;
insert into totalidlefraction (sourcetypeid, minmodelyearid, maxmodelyearid, monthid, dayid, idleregionid, countytypeid, totalidlefraction)
  select sourcetypeid,  minmodelyearid, maxmodelyearid, monthid, dayid, 1 as idleregionid, countytypeid, totalidlefraction
  from oldtotalidlefraction
  where idleregionid = 103;
flush table totalidlefraction;

--
-- drop any new tables created 
--
-- drop table if exists surrogateactivity;
drop table if exists oldcounty;
drop table if exists oldyear;
drop table if exists oldlink;
drop table if exists aggzonemonthhour;   
drop table if exists oldopmodedistribution; 
drop table if exists aggzoneroadtype;
drop table if exists aggfuelsupply;
drop table if exists oldimcoverage;  
drop table if exists aggsho;
drop table if exists aggsourcehours;
drop table if exists aggstarts;
drop table if exists aggextendedidlehours; 
drop table if exists aggaveragetanktemperature;
drop table if exists aggsoakactivityfraction;
drop table if exists aggfuelusagefraction;

drop table if exists surrogateactivitytotal;
drop table if exists surrogatestateactivity;
drop table if exists surrogatecountyactivity;
drop table if exists oldnrbaseyearequippopulation;
drop table if exists oldnrgrowthpatternfinder;
drop table if exists oldnrfuelsupply;
drop table if exists oldnrstatesurrogate;
drop table if exists oldtotalidlefraction;

-- FLUSH TABLES;

  
