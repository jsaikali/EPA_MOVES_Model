-- version 2013-11-19
-- author wesley faler
-- author ed glover, epa
-- author epa - mitch c. (performance rewrites)
-- author epa - ahuang (bug 431 - modified crec 8 to disable humidity effects for pollutants other than
--                      nox. add temporary table weightedandadjustedemissionrate_temp1
--                      and weightedandadjustedemissionrate_temp2).
-- data extraction into sho and steps 5,6,and 7b modified by epa-mitch c 
--    in attempt to fix bug 205
-- step 4d join condition to link fixed by epa mitch c and gwo s.

-- section create remote tables for extracted data

##create.agecategory##;
truncate agecategory;

##create.county##;
truncate county;

##create.criteriaratio##;
truncate criteriaratio;

##create.emissionratebyage##;
truncate emissionratebyage;

##create.fuelformulation##;
truncate fuelformulation;

##create.fuelsubtype##;
truncate fuelsubtype;

##create.fuelsupply##;
truncate fuelsupply;

##create.fueltype##;
truncate fueltype;

##create.fullacadjustment##;
truncate fullacadjustment;

##create.hourday##;
truncate hourday;

##create.imcoverage##;
truncate imcoverage;

##create.imfactor##;
truncate imfactor;

##create.link##;
truncate link;

##create.modelyear##;
truncate modelyear;

##create.monthgrouphour##;
truncate monthgrouphour;

##create.monthofanyyear##;
truncate monthofanyyear;

##create.opmodedistribution##;
truncate opmodedistribution;

##create.pollutantprocessassoc##;
truncate pollutantprocessassoc;

##create.pollutantprocessmodelyear##;
truncate pollutantprocessmodelyear;

##create.pollutantprocessmappedmodelyear##;
truncate pollutantprocessmappedmodelyear;

##create.sho##;
truncate sho;

##create.sourcebin##;
truncate sourcebin;

##create.sourcebindistribution##;
truncate sourcebindistribution;

##create.sourcetypeage##;
truncate sourcetypeage;

##create.sourcetypemodelyear##;
truncate sourcetypemodelyear;

##create.temperatureadjustment##;
truncate temperatureadjustment;

##create.year##;
truncate year;

##create.zone##;
truncate zone;

##create.zonemonthhour##;
truncate zonemonthhour;

-- end section create remote tables for extracted data

-- section extract data

cache select * into outfile '##agecategory##'
from agecategory;

cache select * into outfile '##county##'
from county
where countyid = ##context.iterlocation.countyrecordid##;

cache select fueltypeid,
       fuelformulationid,
       polprocessid,
       pollutantid,
       processid,
       sourcetypeid,
       myrmap(modelyearid) as modelyearid,
       ageid,
       ratio,
       ratiogpa,
       rationosulfur
into outfile '##criteriaratio##'
from criteriaratio
where polprocessid in (##pollutantprocessids##)
and modelyearid = mymap(##context.year## - ageid);

-- select distinct emissionratebyage.* into outfile '??emissionratebyage??'
-- from emissionratebyage, sourcebindistribution, sourcetypemodelyear, sourcebin, runspecsourcefueltype
-- where runspecsourcefueltype.fueltypeid = sourcebin.fueltypeid
-- and emissionratebyage.polprocessid = sourcebindistribution.polprocessid
-- and emissionratebyage.sourcebinid = sourcebin.sourcebinid
-- and emissionratebyage.sourcebinid = sourcebindistribution.sourcebinid
-- and sourcebin.sourcebinid = sourcebindistribution.sourcebinid
-- and runspecsourcefueltype.sourcetypeid = sourcetypemodelyear.sourcetypeid
-- and sourcebindistribution.sourcetypemodelyearid = sourcetypemodelyear.sourcetypemodelyearid
-- and sourcetypemodelyear.modelyearid <= ??context.year??
-- and sourcetypemodelyear.modelyearid >= ??context.year?? - 30
-- and emissionratebyage.polprocessid in (??pollutantprocessids??);

-- section firstbundle
drop table if exists criteriarunningemissionratebyage;

create table criteriarunningemissionratebyage
select emissionratebyage.*
from emissionratebyage
where emissionratebyage.polprocessid in (##pollutantprocessids##)
and emissionratebyage.sourcebinid in (##macro.csv.all.sourcebinid##);
-- end section firstbundle

cache select criteriarunningemissionratebyage.* into outfile '##emissionratebyage##' from criteriarunningemissionratebyage;

select ff.* into outfile '##fuelformulation##'
from fuelformulation ff
inner join fuelsupply fs on fs.fuelformulationid = ff.fuelformulationid
inner join year y on y.fuelyearid = fs.fuelyearid
inner join runspecmonthgroup rsmg on rsmg.monthgroupid = fs.monthgroupid
inner join monthofanyyear moy on moy.monthgroupid = rsmg.monthgroupid
where fuelregionid = ##context.fuelregionid##
and yearid = ##context.year##
and moy.monthid = ##context.monthid##
group by ff.fuelformulationid order by null;

cache select * into outfile '##fuelsubtype##'
from fuelsubtype;

select fuelsupply.* into outfile '##fuelsupply##'
from fuelsupply
inner join runspecmonthgroup on (fuelsupply.monthgroupid = runspecmonthgroup.monthgroupid)
inner join year on (fuelsupply.fuelyearid = year.fuelyearid)
inner join monthofanyyear moy on (moy.monthgroupid = runspecmonthgroup.monthgroupid)
where fuelregionid = ##context.fuelregionid##
and yearid = ##context.year##
and moy.monthid = ##context.monthid##;

cache select fueltype.* into outfile '##fueltype##'
from fueltype
where fueltypeid in (##macro.csv.all.fueltypeid##);

cache select faca.* into outfile '##fullacadjustment##'
from fullacadjustment faca
where faca.sourcetypeid in (##macro.csv.all.sourcetypeid##)
and faca.polprocessid in (##macro.csv.all.polprocessid##);

cache select hourday.* into outfile '##hourday##'
from hourday
where hourdayid in (##macro.csv.all.hourdayid##);

cache select distinct imcoverage.* into outfile '##imcoverage##'
from imcoverage
inner join runspecsourcefueltype on (runspecsourcefueltype.fueltypeid = imcoverage.fueltypeid
       and runspecsourcefueltype.sourcetypeid = imcoverage.sourcetypeid)
where polprocessid in (##pollutantprocessids##)
and countyid = ##context.iterlocation.countyrecordid## 
and yearid = ##context.year##
and useimyn = 'Y';

cache select distinct imfactor.* into outfile '##imfactor##'
from imfactor
inner join runspecsourcefueltype on (runspecsourcefueltype.fueltypeid = imfactor.fueltypeid
       and runspecsourcefueltype.sourcetypeid = imfactor.sourcetypeid)
where polprocessid in (##pollutantprocessids##);

select link.* into outfile '##link##'
from link where linkid = ##context.iterlocation.linkrecordid##;

cache select * into outfile '##modelyear##'
from modelyear;

select monthgrouphour.* into outfile '##monthgrouphour##'
from monthgrouphour
inner join monthofanyyear moy on (moy.monthgroupid = monthgrouphour.monthgroupid)
where moy.monthid = ##context.monthid##
and monthgrouphour.hourid in (##macro.csv.all.hourid##);

select monthofanyyear.* into outfile '##monthofanyyear##'
from monthofanyyear
where monthofanyyear.monthid = ##context.monthid##;

cache select opmodedistribution.* into outfile '##opmodedistribution##'
from opmodedistribution
where polprocessid in (##pollutantprocessids##)
and linkid = ##context.iterlocation.linkrecordid##
and sourcetypeid in (##macro.csv.all.sourcetypeid##);

cache select * into outfile '##pollutantprocessassoc##'
from pollutantprocessassoc
where processid=##context.iterprocess.databasekey##;

cache select * into outfile '##pollutantprocessmodelyear##'
from pollutantprocessmodelyear
where modelyearid <= ##context.year##
and modelyearid >= ##context.year## - 30
and polprocessid in (##pollutantprocessids##);

cache select * into outfile '##pollutantprocessmappedmodelyear##'
from pollutantprocessmappedmodelyear
where modelyearid <= ##context.year##
and modelyearid >= ##context.year## - 30
and polprocessid in (##pollutantprocessids##);

select * into outfile '##sho##'
from sho
where linkid = ##context.iterlocation.linkrecordid##
and monthid = ##context.monthid##
and yearid = ##context.year##;

-- select distinct sourcebin.* into outfile '??sourcebin??'
-- from sourcebindistribution, sourcetypemodelyear, sourcebin, runspecsourcefueltype
-- where polprocessid in (??pollutantprocessids??)
-- and sourcebindistribution.sourcetypemodelyearid = sourcetypemodelyear.sourcetypemodelyearid
-- and sourcetypemodelyear.sourcetypeid = runspecsourcefueltype.sourcetypeid
-- and sourcebindistribution.sourcebinid = sourcebin.sourcebinid
-- and sourcebin.fueltypeid = runspecsourcefueltype.fueltypeid;

cache select distinct sourcebin.* into outfile '##sourcebin##'
from sourcebindistribution, sourcetypemodelyear, sourcebin
where polprocessid in (##pollutantprocessids##)
and sourcebindistribution.sourcetypemodelyearid = sourcetypemodelyear.sourcetypemodelyearid
and sourcebindistribution.sourcebinid = sourcebin.sourcebinid
and sourcetypemodelyear.modelyearid <= ##context.year##
and sourcetypemodelyear.modelyearid >= ##context.year## - 30;

-- select distinct sourcebindistribution.* into outfile '??sourcebindistribution??'
-- from sourcebindistribution, sourcetypemodelyear, sourcebin, runspecsourcefueltype
-- where polprocessid in (??pollutantprocessids??)
-- and sourcebindistribution.sourcetypemodelyearid = sourcetypemodelyear.sourcetypemodelyearid
-- and sourcetypemodelyear.modelyearid <= ??context.year??
-- and sourcetypemodelyear.modelyearid >= ??context.year?? - 30
-- and sourcetypemodelyear.sourcetypeid = runspecsourcefueltype.sourcetypeid
-- and sourcebindistribution.sourcebinid = sourcebin.sourcebinid
-- and sourcebin.fueltypeid = runspecsourcefueltype.fueltypeid;

cache select sourcebindistribution.* into outfile '##sourcebindistribution##'
from sourcebindistributionfuelusage_##context.iterprocess.databasekey##_##context.iterlocation.countyrecordid##_##context.year## as sourcebindistribution, sourcetypemodelyear
where polprocessid in (##pollutantprocessids##)
and sourcebindistribution.sourcetypemodelyearid = sourcetypemodelyear.sourcetypemodelyearid
and sourcetypemodelyear.modelyearid <= ##context.year##
and sourcetypemodelyear.modelyearid >= ##context.year## - 30;

cache select sourcetypeage.* into outfile '##sourcetypeage##'
from sourcetypeage
where sourcetypeage.sourcetypeid in (##macro.csv.all.sourcetypeid##);

cache select sourcetypemodelyear.* into outfile '##sourcetypemodelyear##'
from sourcetypemodelyear
where sourcetypemodelyear.sourcetypeid in (##macro.csv.all.sourcetypeid##)
and modelyearid <= ##context.year##
and modelyearid >= ##context.year## - 30;

cache select temperatureadjustment.* into outfile '##temperatureadjustment##'
from temperatureadjustment
where polprocessid in (##pollutantprocessids##)
and fueltypeid in (##macro.csv.all.fueltypeid##);

cache select year.* into outfile '##year##'
from year
where yearid = ##context.year##;

cache select * into outfile '##zone##'
from zone
where zoneid = ##context.iterlocation.zonerecordid##;

select distinct zonemonthhour.* into outfile '##zonemonthhour##'
from zonemonthhour
where zoneid = ##context.iterlocation.zonerecordid##
and zonemonthhour.monthid = ##context.monthid##
and zonemonthhour.hourid in (##macro.csv.all.hourid##);

-- end section extract data

-- section processing

-- 
-- crec 1-a: complete i/m adjustment fraction information
--
drop table if exists imcoveragemergedungrouped;
create table imcoveragemergedungrouped (
       processid smallint not null,
       pollutantid smallint not null,
       modelyearid smallint not null,
       fueltypeid smallint not null,
       sourcetypeid smallint not null,
       imadjustfract float
);

create index xpkimcoveragemergedungrouped on imcoveragemergedungrouped
(
       processid asc,
       pollutantid asc,
       modelyearid asc,
       fueltypeid asc,
       sourcetypeid asc
);

insert into imcoveragemergedungrouped (
       processid,pollutantid,modelyearid,fueltypeid,sourcetypeid,imadjustfract)
select
 ppa.processid,
 ppa.pollutantid,
 ppmy.modelyearid,
 imf.fueltypeid,
 imc.sourcetypeid,
 sum(imfactor*compliancefactor*.01) as imadjustfract
from pollutantprocessmappedmodelyear ppmy
inner join pollutantprocessassoc ppa on (ppa.polprocessid=ppmy.polprocessid)
inner join imfactor imf on (
       imf.polprocessid = ppa.polprocessid
       and imf.immodelyeargroupid = ppmy.immodelyeargroupid)
inner join agecategory ac on (
       ac.agegroupid = imf.agegroupid)
inner join imcoverage imc on (
       imc.polprocessid = imf.polprocessid
       and imc.inspectfreq = imf.inspectfreq
       and imc.teststandardsid = imf.teststandardsid
       and imc.sourcetypeid = imf.sourcetypeid
       and imc.fueltypeid = imf.fueltypeid
       and imc.begmodelyearid <= ppmy.modelyearid
       and imc.endmodelyearid >= ppmy.modelyearid)
where imc.countyid = ##context.iterlocation.countyrecordid##
and imc.yearid = ##context.year##
and ppmy.modelyearid = ##context.year##-ageid
and ppmy.polprocessid in (##pollutantprocessids##)
group by  ppa.processid,
 ppa.pollutantid,
 ppmy.modelyearid,
 imf.fueltypeid,
 imc.sourcetypeid;

-- 
-- crec 2-a: combine gpa and non gpa fuel adjustment factors 
--
drop table if exists countyfueladjustment;
create table countyfueladjustment (
       countyid integer not null,
       polprocessid int not null,
       modelyearid integer not null,
       sourcetypeid smallint not null,
       fuelformulationid smallint not null,
       fueladjustment float
);

create index countyfueladjustment1 on countyfueladjustment
(
       polprocessid asc,
       modelyearid asc
);

create index countyfueladjustment2 on countyfueladjustment
(
       fuelformulationid asc
);

insert into countyfueladjustment
select countyid, polprocessid, modelyearid, sourcetypeid,
fuelformulationid, ratio+gpafract*(ratiogpa-ratio)
from criteriaratio
inner join county c;

-- 
-- crec 2-b: aggregate county fuel adjustments to fuel type
--
drop table if exists fuelsupplywithfueltype;
create table fuelsupplywithfueltype (
       countyid integer not null,
       yearid smallint not null,
       monthid smallint not null,
       fuelformulationid smallint not null,
       fueltypeid smallint not null,
       marketshare float
);

create index fuelsupplywithfueltype1 on fuelsupplywithfueltype
(
       fuelformulationid asc
);

insert into fuelsupplywithfueltype
select ##context.iterlocation.countyrecordid## as countyid, yearid, monthid, fs.fuelformulationid, fueltypeid, marketshare
from fuelsupply fs
inner join fuelformulation ff on ff.fuelformulationid = fs.fuelformulationid
inner join fuelsubtype fst on fst.fuelsubtypeid = ff.fuelsubtypeid
inner join monthofanyyear may on fs.monthgroupid = may.monthgroupid
inner join year y on y.fuelyearid = fs.fuelyearid 
where y.yearid = ##context.year##;

drop table if exists fuelsupplyadjustment;
create table fuelsupplyadjustment (
       countyid integer not null,
       yearid smallint not null,
       monthid smallint not null,
       polprocessid int not null,
       modelyearid smallint not null,
       sourcetypeid smallint not null,
       fueltypeid smallint not null,
       fueladjustment float
);

create unique index xpkfuelsupplyadjustment on fuelsupplyadjustment
(
       countyid asc,
       yearid asc,
       monthid asc,
       polprocessid asc,
       modelyearid asc,
       sourcetypeid asc,
       fueltypeid asc
);

insert into fuelsupplyadjustment
select cfa.countyid, yearid, monthid, cfa.polprocessid, cfa.modelyearid,
       sourcetypeid, fueltypeid, sum(fueladjustment*marketshare)
from countyfueladjustment cfa 
inner join fuelsupplywithfueltype fsft on (
       fsft.fuelformulationid = cfa.fuelformulationid)
group by cfa.countyid, yearid, monthid, cfa.polprocessid, cfa.modelyearid,
       sourcetypeid, fueltypeid order by modelyearid asc;
-- note no need to join fsft and cfa on countyid since both tables already filtered to 1 county
-- 
-- crec 3: calculate temperature adjustment factors
--
drop table if exists metadjustment;
create table metadjustment (
       zoneid integer not null,
       monthid       smallint not null,
       hourid smallint not null,
       polprocessid int not null,
       fueltypeid    smallint not null,
       modelyearid   smallint not null,
       minmodelyearid       int    not null,
       maxmodelyearid       int not null,
       temperatureadjustment float
);

create index metadjustment1 on metadjustment
(
       zoneid asc,
       monthid asc,
       hourid asc,
       modelyearid   asc,
       polprocessid asc
);

insert into metadjustment (zoneid, monthid, hourid, polprocessid, fueltypeid, modelyearid, minmodelyearid, maxmodelyearid, 
                                                 temperatureadjustment)
select zoneid, monthid, hourid, ta.polprocessid, fueltypeid, my.modelyearid, ta.minmodelyearid, ta.maxmodelyearid, 
1.0 + (temperature-75)*(tempadjustterma + (temperature-75)*tempadjusttermb)
from zonemonthhour zmh
inner join temperatureadjustment ta
inner join pollutantprocessassoc ppa on (ppa.polprocessid = ta.polprocessid)
join modelyear my
where zmh.zoneid = ##context.iterlocation.zonerecordid##
and ppa.processid = 1
and my.modelyearid between ta.minmodelyearid and ta.maxmodelyearid;

-- 
-- crec 4-a: calculate ac on fraction
--
drop table if exists aconfraction;
create table aconfraction (
       zoneid integer not null,
       monthid       smallint not null,
       hourid smallint not null, 
       aconfraction float
); 

create unique index xpkaconfraction on aconfraction
(
       zoneid asc,
       monthid asc,
       hourid asc
);

--     least(greatest(acactivityterma+heatindex*(acactivitytermb+acactivitytermc*heatindex),0),1.0,0.0)

insert into aconfraction
select 
       zoneid, zmh.monthid, zmh.hourid,
       least(greatest(acactivityterma+heatindex*(acactivitytermb+acactivitytermc*heatindex),0),1.0) as aconfraction
from zonemonthhour zmh
inner join monthofanyyear may on (may.monthid = zmh.monthid)
inner join monthgrouphour mgh on (mgh.monthgroupid = may.monthgroupid
and mgh.hourid = zmh.hourid);

-- 
-- crec 4-b: calculate ac activity fraction
--
drop table if exists acactivityfraction;
create table acactivityfraction (
       zoneid  integer not null,
       monthid smallint not null,
       hourid smallint not null,
       sourcetypeid smallint not null,
       modelyearid smallint not null,
       acactivityfraction float
);

create index acactivityfraction1 on acactivityfraction (
       hourid asc
);
create index acactivityfraction2 on acactivityfraction (
       sourcetypeid asc
);


insert into acactivityfraction 
select zoneid, monthid, hourid, sta.sourcetypeid, modelyearid, 
aconfraction*acpenetrationfraction*functioningacfraction
from aconfraction acof
inner join sourcetypemodelyear stmy
inner join sourcetypeage sta on (
sta.sourcetypeid = stmy.sourcetypeid and
sta.ageid = ##context.year## - stmy.modelyearid);

-- 
-- crec 4-c: weight fullacadjustment factors by operating mode
--
drop table if exists weightedfullacadjustment;
create table weightedfullacadjustment (
       sourcetypeid smallint not null,
       polprocessid int not null,
       linkid integer not null,
       hourdayid smallint not null,
       opmodeid smallint not null,
       weightedfullacadjustment float
);

create index weightedfullacadjustment1 on weightedfullacadjustment (
       hourdayid
);
create index weightedfullacadjustment2 on weightedfullacadjustment (
       sourcetypeid
);

/*
insert into weightedfullacadjustment
select omd.sourcetypeid, omd.polprocessid, linkid, hourdayid, 
       sum(fullacadjustment*opmodefraction)
from opmodedistribution omd
inner join fullacadjustment faca on (faca.sourcetypeid=omd.sourcetypeid
and faca.polprocessid=omd.polprocessid and faca.opmodeid=omd.opmodeid
and faca.opmodeid < 1000)
group by omd.sourcetypeid, omd.polprocessid, linkid, hourdayid 
order by null
*/

insert into weightedfullacadjustment
select omd.sourcetypeid, omd.polprocessid, linkid, hourdayid, omd.opmodeid,
       fullacadjustment
from opmodedistribution omd
inner join fullacadjustment faca on (faca.sourcetypeid=omd.sourcetypeid
and faca.polprocessid=omd.polprocessid and faca.opmodeid=omd.opmodeid);

-- 
-- crec 4-d: calculate ac adjustment factor
--
drop table if exists acadjustment;
create table acadjustment (
       zoneid  integer not null,
       monthid  smallint not null,
       hourid  smallint not null,
       dayid  smallint not null,
       sourcetypeid smallint not null,
       modelyearid  smallint not null,
       polprocessid int not null,
       opmodeid smallint not null,
       acadjustment float
);

create  index acadjustment1 on acadjustment (
       zoneid asc,
       monthid asc,
       hourid  asc,
       polprocessid asc,
       opmodeid asc
);

-- following statement formerly ordered results by modelyearid descending
--  this seems unnecessary and was removed
insert into acadjustment
select acaf.zoneid, monthid, hd.hourid, hd.dayid, 
acaf.sourcetypeid, modelyearid, polprocessid, opmodeid,
       1+((weightedfullacadjustment-1)*acactivityfraction)
from acactivityfraction acaf
inner join link l on (acaf.zoneid=l.zoneid)
inner join hourday hd on (hd.hourid=acaf.hourid)
inner join weightedfullacadjustment wfaca on (
wfaca.sourcetypeid = acaf.sourcetypeid and
wfaca.linkid = l.linkid and
wfaca.hourdayid = hd.hourdayid);

-- 
-- crec-5: weight emission rates by source bin
--
drop table if exists sbweightedemissionrate;
create table sbweightedemissionrate (
       zoneid integer not null,
       yearid smallint not null,
       polprocessid int not null,
       sourcetypeid smallint not null,
       modelyearid smallint not null,
       fueltypeid smallint not null,
       opmodeid smallint not null,
       meanbaserate float,
       meanbaserateim float
);

create index xpksbweightedemissionrate on sbweightedemissionrate (
       polprocessid asc,
       sourcetypeid asc,
       opmodeid asc
);

insert into sbweightedemissionrate
select ##context.iterlocation.zonerecordid## as zoneid, ##context.year## as yearid, 
       erim.polprocessid, sourcetypeid, (##context.year##-age.ageid) as modelyearid, sb.fueltypeid, erim.opmodeid,
       sum(sourcebinactivityfraction*meanbaserate), sum(sourcebinactivityfraction*meanbaserateim)
from emissionratebyage erim
inner join agecategory age on (age.agegroupid=erim.agegroupid)
inner join sourcetypemodelyear stmy on (stmy.modelyearid=##context.year##-age.ageid)
inner join sourcebindistribution sbd on (sbd.sourcetypemodelyearid=stmy.sourcetypemodelyearid
and sbd.polprocessid=erim.polprocessid and sbd.sourcebinid=erim.sourcebinid)
inner join sourcebin sb on (sbd.sourcebinid=sb.sourcebinid) 
group by erim.polprocessid, sourcetypeid, age.ageid, sb.fueltypeid, erim.opmodeid
order by null;

-- 
-- crec-6: weight emission rates by operating mode
--
drop table if exists fullyweightedemissionrate;
create table fullyweightedemissionrate (
       linkid integer not null,
       yearid smallint not null,
       polprocessid int not null,
       sourcetypeid smallint not null,
       modelyearid smallint not null,
       fueltypeid smallint not null,
       hourdayid smallint not null,
       opmodeid smallint not null,
       meanbaserate float,
       meanbaserateim float,
       opmodefraction float
);

create unique index xpkfullyweightedemissionrate on fullyweightedemissionrate (
       linkid asc,
       yearid asc,
       polprocessid asc,
       sourcetypeid asc,
       modelyearid asc,
       fueltypeid asc,
       hourdayid asc,
       opmodeid asc
);

create index opmodedistributionspecial on opmodedistribution (
       polprocessid asc,
       sourcetypeid asc,
       opmodeid asc
);

insert into fullyweightedemissionrate
select linkid, yearid, sbwer.polprocessid, sbwer.sourcetypeid, modelyearid, fueltypeid, hourdayid, opmodeid,
       meanbaserate, meanbaserateim, opmodefraction
from sbweightedemissionrate sbwer 
inner join opmodedistribution omd 
  using(polprocessid, sourcetypeid, opmodeid);

-- 
-- crec-7-a: combine temperature and ac adjustment factors
--
drop table if exists tempandacadjustment;
create table tempandacadjustment (
       zoneid integer not null,
       polprocessid int not null, 
       sourcetypeid smallint not null, 
       modelyearid smallint not null,
       fueltypeid smallint not null,
       monthid smallint not null,
       hourid smallint not null,
       dayid smallint not null,
       opmodeid smallint not null,
       tempandacadjustment float
);

create unique index xpktempandacadjustment on tempandacadjustment (
       zoneid asc,
       polprocessid asc, 
       sourcetypeid asc, 
       modelyearid asc,
       fueltypeid asc,
       monthid asc,
       hourid asc,
       dayid asc,
       opmodeid asc,
       tempandacadjustment
);

insert into tempandacadjustment
select ma.zoneid, ma.polprocessid, sourcetypeid, aca.modelyearid, 
       fueltypeid, ma.monthid, ma.hourid, aca.dayid, aca.opmodeid,
       temperatureadjustment*acadjustment 
from metadjustment ma
inner join acadjustment aca on (
aca.zoneid=ma.zoneid and
aca.monthid=ma.monthid and
aca.hourid=ma.hourid and
aca.polprocessid=ma.polprocessid and 
aca.modelyearid=ma.modelyearid);

-- 
-- crec 7-b: apply fuel adjustment to fully weighted emission rates
--
drop table if exists fueladjustedrate;
create table fueladjustedrate (
       linkid integer not null,
       yearid smallint not null,
       polprocessid int not null,
       sourcetypeid smallint not null,
       modelyearid smallint not null,
       fueltypeid smallint not null,
       monthid smallint not null,
       hourdayid smallint not null,
       opmodeid smallint not null,
       fueladjustedrate float,
       fueladjustedrateim float,
       opmodefraction float
);

create unique index xpkfueladjustedrate on fueladjustedrate (
       linkid asc,
       yearid asc,
       polprocessid asc,
       sourcetypeid asc,
       modelyearid asc,
       fueltypeid asc,
       monthid asc,
       hourdayid asc,
       opmodeid asc
);

create index xpkfullyweightedemissionrate2 on fullyweightedemissionrate (
       sourcetypeid asc,
       yearid asc,
       polprocessid asc,
       modelyearid asc,
       fueltypeid asc,
       opmodeid asc
);

create index xpkfuelsupplyadjustment2 on fuelsupplyadjustment (
       sourcetypeid asc,
       yearid asc,
       polprocessid asc,
       modelyearid asc,
       fueltypeid asc
);

analyze table fullyweightedemissionrate;
analyze table fuelsupplyadjustment;

insert into fueladjustedrate
select linkid, fwer.yearid, fwer.polprocessid, fwer.sourcetypeid, fwer.modelyearid,
       fwer.fueltypeid, m.monthid, fwer.hourdayid, fwer.opmodeid,
       meanbaserate * ifnull(fueladjustment,1.0), 
       meanbaserateim * ifnull(fueladjustment,1.0),
       opmodefraction
from
monthofanyyear m
inner join fullyweightedemissionrate fwer
left outer join fuelsupplyadjustment fsa on (
       fsa.yearid = fwer.yearid and
       fsa.polprocessid = fwer.polprocessid and
       fsa.modelyearid = fwer.modelyearid and
       fsa.sourcetypeid = fwer.sourcetypeid and
       fsa.fueltypeid = fwer.fueltypeid and
       fsa.monthid = m.monthid
);

-- 
-- crec 7-c: apply temperature and ac adjustment to fuel-adjusted emission rate.
--
drop table if exists weightedandadjustedemissionrate;
create table weightedandadjustedemissionrate (
       linkid integer not null,
       yearid smallint not null,
       polprocessid int not null,
       sourcetypeid smallint not null,
       modelyearid smallint not null,
       fueltypeid smallint not null,
       hourid smallint not null,
       dayid smallint not null,
       monthid smallint not null,
       meanbaserate float,
       meanbaserateim float
);

create unique index xpkweightedandadjustedemissionrate on weightedandadjustedemissionrate (
       linkid asc,
       yearid asc,
       polprocessid asc,
       sourcetypeid asc,
       modelyearid asc,
       fueltypeid asc,
       hourid asc,
       dayid asc,
       monthid asc
);

create index xpkfueladjustedrate2 on fueladjustedrate (
    polprocessid asc,
    sourcetypeid asc,
    modelyearid asc,
    fueltypeid asc,
    monthid asc
);

analyze table fueladjustedrate;
analyze table link;
analyze table hourday;
analyze table tempandacadjustment;

insert into weightedandadjustedemissionrate
select l.linkid, yearid, taca.polprocessid, taca.sourcetypeid, taca.modelyearid, 
       taca.fueltypeid, taca.hourid, taca.dayid, taca.monthid,
       sum(fueladjustedrate*tempandacadjustment*opmodefraction),
       sum(fueladjustedrateim*tempandacadjustment*opmodefraction)
from fueladjustedrate far 
inner join link l on (l.linkid=far.linkid)
inner join hourday hd on (hd.hourdayid=far.hourdayid)
inner join tempandacadjustment taca on (
       taca.zoneid=l.zoneid and
       taca.polprocessid=far.polprocessid and
       taca.sourcetypeid=far.sourcetypeid and
       taca.modelyearid=far.modelyearid and
       taca.fueltypeid=far.fueltypeid and
       taca.monthid=far.monthid and
       taca.dayid=hd.dayid and
       taca.hourid=hd.hourid and
       taca.opmodeid=far.opmodeid)
group by l.linkid, yearid, taca.polprocessid, taca.sourcetypeid, taca.modelyearid, 
       taca.fueltypeid, taca.hourid, taca.dayid, taca.monthid
order by null;

-- 
-- crec 8: calculate and apply humidity correction factor to nox emissions
--

drop table if exists weightedandadjustedemissionrate2_temp1;
create table weightedandadjustedemissionrate2_temp1 (
       linkid integer not null,
       yearid smallint not null,
       polprocessid int not null,
       sourcetypeid smallint not null,
       fueltypeid smallint not null,
       modelyearid smallint not null,
       monthid smallint not null,
       dayid smallint not null,
       hourid smallint not null,
       meanbaserate float,
       meanbaserateim float
);

create unique index xpkweightedandadjustedemissionrate2_temp1 on weightedandadjustedemissionrate2_temp1 (
       linkid asc,
       yearid asc,
       polprocessid asc,
       sourcetypeid asc,
       fueltypeid asc,
       modelyearid asc,
       monthid asc,
       dayid asc,
       hourid asc
);

analyze table weightedandadjustedemissionrate;
analyze table link;
analyze table zonemonthhour;
analyze table fueltype;

insert into weightedandadjustedemissionrate2_temp1
select l.linkid, yearid, polprocessid, sourcetypeid, waer.fueltypeid, modelyearid, waer.monthid, dayid, 
       waer.hourid,
       (1.0 - (greatest(21.0,least(specifichumidity,124.0))-75.0)*humiditycorrectioncoeff)*meanbaserate,
       (1.0 - (greatest(21.0,least(specifichumidity,124.0))-75.0)*humiditycorrectioncoeff)*meanbaserateim
from weightedandadjustedemissionrate waer 
inner join link l on (l.linkid=waer.linkid)
inner join zonemonthhour zmh on (
zmh.monthid=waer.monthid and
zmh.zoneid=l.zoneid and 
zmh.hourid=waer.hourid) and
polprocessid = 301
inner join fueltype ft on (ft.fueltypeid=waer.fueltypeid);

drop table if exists weightedandadjustedemissionrate2_temp2;
create table weightedandadjustedemissionrate2_temp2 (
       linkid integer not null,
       yearid smallint not null,
       polprocessid int not null,
       sourcetypeid smallint not null,
       fueltypeid smallint not null,
       modelyearid smallint not null,
       monthid smallint not null,
       dayid smallint not null,
       hourid smallint not null,
       meanbaserate float,
       meanbaserateim float
);

create unique index xpkweightedandadjustedemissionrate_temp2 on weightedandadjustedemissionrate2_temp2 (
       linkid asc,
       yearid asc,
       polprocessid asc,
       sourcetypeid asc,
    fueltypeid asc,
       modelyearid asc,
       monthid asc,
       dayid asc,
       hourid asc
);

insert into weightedandadjustedemissionrate2_temp2
select linkid, yearid, polprocessid, sourcetypeid, fueltypeid, 
       modelyearid, monthid, dayid, hourid, meanbaserate, meanbaserateim
from weightedandadjustedemissionrate
where polprocessid != 301;

drop table if exists weightedandadjustedemissionrate2;
create table weightedandadjustedemissionrate2 (
       linkid integer not null,
       yearid smallint not null,
       polprocessid int not null,
       sourcetypeid smallint not null,
       fueltypeid smallint not null,
       modelyearid smallint not null,
       monthid smallint not null,
       dayid smallint not null,
       hourid smallint not null,
       meanbaserate float,
       meanbaserateim float
);

create unique index xpkweightedandadjustedemissionrate2 on weightedandadjustedemissionrate2 (
       linkid asc,
       yearid asc,
       polprocessid asc,
       sourcetypeid asc,
    fueltypeid asc,
       modelyearid asc,
       monthid asc,
       dayid asc,
       hourid asc
);

insert into weightedandadjustedemissionrate2
(select * from weightedandadjustedemissionrate2_temp1)
union
(select * from weightedandadjustedemissionrate2_temp2);

-- 
-- crec 9: multiply fully weighted and adjusted emission rates by source hour operating (sho)
--          activity to generate inventory.
--
drop table if exists sho2;
create table sho2 (
       yearid               smallint not null,
       monthid              smallint not null,
       dayid                smallint not null,
       hourid               smallint not null,
       sourcetypeid         smallint not null,
       modelyearid          smallint not null,
       scc                  char(10),
       sho                  float null
);
create index xpksho2 on sho2 (
       yearid asc,
       monthid asc,
       dayid asc,
       hourid asc,
       sourcetypeid asc,
       modelyearid asc
);
analyze table sho;

insert into sho2 select 
yearid, monthid, hd.dayid, hd.hourid, sourcetypeid,
yearid - ageid as modelyearid, null as scc, sho
from sho sho
inner join hourday hd on (hd.hourdayid=sho.hourdayid);

drop table if exists weightedandadjustedemissionrate3;
create table weightedandadjustedemissionrate3 (
              linkid integer not null,
              yearid smallint not null,
              pollutantid smallint not null,
              processid smallint not null,
              sourcetypeid smallint not null,
              fueltypeid smallint not null,
              modelyearid smallint not null,
              monthid smallint not null,
              dayid smallint not null,
              hourid smallint not null,
              meanbaserate float,
              meanbaserateim float
);

create unique index xpkweightedandadjustedemissionrate31 on weightedandadjustedemissionrate3 (
              linkid asc,
              yearid asc,
              pollutantid asc,
              processid asc,
              sourcetypeid asc,
              fueltypeid asc,
              modelyearid asc,
              monthid asc,
              dayid asc,
              hourid asc
);

insert into weightedandadjustedemissionrate3
select linkid, yearid, pollutantid, processid, sourcetypeid, fueltypeid,
       modelyearid, monthid, dayid, hourid, meanbaserate, meanbaserateim
from weightedandadjustedemissionrate2 waer
inner join pollutantprocessassoc ppa on (ppa.polprocessid=waer.polprocessid);

drop table if exists sho3;
create table sho3 (
          linkid                          integer not null,
       yearid               smallint not null,
       monthid              smallint not null,
       dayid                smallint not null,
       hourid               smallint not null,
       sourcetypeid         smallint not null,
       modelyearid          smallint not null,
       pollutantid          smallint not null,
       processid            smallint not null,
       fueltypeid           smallint not null,
       scc                  char(10),
       emissionquant        float null,
       emissionquantim             float null
);
create index xpksho3 on sho3 (
       linkid asc,
       yearid asc,
       monthid asc,
       dayid asc,
       hourid asc,
       sourcetypeid asc,
       modelyearid asc,
       pollutantid asc,
       processid asc,
       fueltypeid asc
);

create index xpksho22 on sho2 (
       yearid asc,
       monthid asc,
       dayid asc,
       hourid asc,
       sourcetypeid asc,
       modelyearid asc
);

create index xpkweightedandadjustedemissionrate32 on weightedandadjustedemissionrate3 (
       yearid asc,
       monthid asc,
       dayid asc,
       hourid asc,
       sourcetypeid asc,
       modelyearid asc
);

analyze table sho2;

insert into sho3
select linkid, sho2.yearid, sho2.monthid, sho2.dayid, sho2.hourid, 
       sho2.sourcetypeid, sho2.modelyearid, pollutantid, processid, fueltypeid, 
       scc, 
       sho * meanbaserate as emissionquant,
       sho * meanbaserateim as emissionquantim
from sho2 sho2
inner join weightedandadjustedemissionrate3 waer on (
       waer.yearid=sho2.yearid and
       waer.monthid=sho2.monthid and
       waer.dayid=sho2.dayid and
       waer.hourid=sho2.hourid and
       waer.sourcetypeid=sho2.sourcetypeid and
       waer.modelyearid=sho2.modelyearid);

alter table movesworkeroutput add emissionquantim float null;

analyze table sho3;
insert into movesworkeroutput (
       stateid, countyid, zoneid, linkid, roadtypeid, yearid, monthid, dayid, hourid,
       pollutantid, processid, sourcetypeid, modelyearid, fueltypeid, scc, emissionquant, emissionquantim)
select ##context.iterlocation.staterecordid##, ##context.iterlocation.countyrecordid##,
       l.zoneid, l.linkid, l.roadtypeid, yearid, monthid, dayid, hourid,
       pollutantid, processid, sourcetypeid, modelyearid, fueltypeid, scc, emissionquant, emissionquantim
from sho3 sho3
inner join link l on (l.linkid = sho3.linkid);

-- apply im
update movesworkeroutput, imcoveragemergedungrouped set emissionquant=greatest(emissionquantim*imadjustfract + emissionquant*(1.0-imadjustfract),0.0)
where movesworkeroutput.processid = imcoveragemergedungrouped.processid
       and movesworkeroutput.pollutantid = imcoveragemergedungrouped.pollutantid
       and movesworkeroutput.modelyearid = imcoveragemergedungrouped.modelyearid
       and movesworkeroutput.fueltypeid = imcoveragemergedungrouped.fueltypeid
       and movesworkeroutput.sourcetypeid = imcoveragemergedungrouped.sourcetypeid;

alter table movesworkeroutput drop emissionquantim;

-- end section processing

-- section cleanup
drop table if exists acactivityfraction;
drop table if exists acadjustment;
drop table if exists aconfraction;
drop table if exists countyfueladjustment;
drop table if exists emissionratewithim;
drop table if exists fueladjustedrate;
drop table if exists fuelsupplyadjustment;
drop table if exists fuelsupplywithfueltype;
drop table if exists fullyweightedemissionrate;
drop table if exists imcoveragemergedungrouped;
drop table if exists metadjustment;
drop table if exists sbweightedemissionrate;
drop table if exists sho2;
drop table if exists sho3;
drop table if exists tempandacadjustment;
drop table if exists weightedandadjustedemissionrate;
drop table if exists weightedandadjustedemissionrate2;
drop table if exists weightedandadjustedemissionrate2_temp1;
drop table if exists weightedandadjustedemissionrate2_temp2;
drop table if exists weightedandadjustedemissionrate3;
drop table if exists weightedfullacadjustment;
-- end section cleanup
