-- author wesley faler
-- author ed campbell
-- version 2014-04-28

-- @algorithm
-- @owner evaporative permeation calculator
-- @calculator

-- section create remote tables for extracted data

##create.agecategory##;
truncate agecategory;

##create.averagetanktemperature##;
truncate averagetanktemperature;

##create.county##;
truncate county;

##create.emissionratebyage##;
truncate emissionratebyage;

##create.etohbin##;
truncate etohbin;

##create.fuelsupply##;
truncate fuelsupply;

##create.fuelsubtype##;
truncate fuelsubtype;

##create.fuelformulation##;
truncate fuelformulation;

##create.hcpermeationcoeff##;
truncate hcpermeationcoeff;

##create.hourday##;
truncate hourday;

##create.link##;
truncate link;

##create.modelyear##;
truncate modelyear;

##create.opmodedistribution##;
truncate opmodedistribution;

##create.pollutantprocessassoc##;
truncate pollutantprocessassoc;

##create.pollutantprocessmodelyear##;
truncate pollutantprocessmodelyear;

##create.pollutantprocessmappedmodelyear##;
truncate pollutantprocessmappedmodelyear;

##create.runspecsourcetype##;
truncate runspecsourcetype;

##create.sourcebindistribution##;
truncate sourcebindistribution;

##create.sourcebin##;
truncate sourcebin;

##create.sourcehours##;
truncate sourcehours;

##create.sourcetypemodelyear##;
truncate sourcetypemodelyear;

##create.sourcetypemodelyeargroup##;
truncate sourcetypemodelyeargroup;

##create.temperatureadjustment##;
truncate temperatureadjustment;

##create.year##;
truncate year;

-- section withregclassid
##create.regclasssourcetypefraction##;
truncate table regclasssourcetypefraction;
-- end section withregclassid

-- end section create remote tables for extracted data

-- section extract data
-- create table if not exists eventlog (eventrowid integer unsigned not null auto_increment, primary key (eventrowid), eventtime datetime, eventname varchar(120));
-- insert into eventlog (eventtime, eventname) select now(), 'extracting data';

cache select * into outfile '##agecategory##'
from agecategory;

cache select * into outfile '##averagetanktemperature##' from averagetanktemperature
where zoneid = ##context.iterlocation.zonerecordid##
and monthid = ##context.monthid##;

cache select * into outfile '##county##'
from county
where countyid = ##context.iterlocation.countyrecordid##;

cache select distinct emissionratebyage.* into outfile '##emissionratebyage##'
from emissionratebyage, sourcebindistribution, sourcetypemodelyear, sourcebin, runspecsourcefueltype
where runspecsourcefueltype.fueltypeid = sourcebin.fueltypeid
and emissionratebyage.polprocessid = sourcebindistribution.polprocessid
and emissionratebyage.sourcebinid = sourcebin.sourcebinid
and emissionratebyage.sourcebinid = sourcebindistribution.sourcebinid
and sourcebin.sourcebinid = sourcebindistribution.sourcebinid
and runspecsourcefueltype.sourcetypeid = sourcetypemodelyear.sourcetypeid
and sourcebindistribution.sourcetypemodelyearid = sourcetypemodelyear.sourcetypemodelyearid
and sourcetypemodelyear.modelyearid <= ##context.year##
and sourcetypemodelyear.modelyearid >= ##context.year## - 30
and emissionratebyage.polprocessid in (##pollutantprocessids##);

cache select * into outfile '##etohbin##'
from etohbin;

cache select fuelsupply.* into outfile '##fuelsupply##'
from fuelsupply
inner join runspecmonthgroup on (fuelsupply.monthgroupid = runspecmonthgroup.monthgroupid)
inner join year on (fuelsupply.fuelyearid = year.fuelyearid)
inner join monthofanyyear on (monthofanyyear.monthgroupid=fuelsupply.monthgroupid)
where fuelregionid = ##context.fuelregionid##
and yearid = ##context.year##
and monthid = ##context.monthid##;

cache select distinct fuelsubtype.* into outfile '##fuelsubtype##'
from fuelsubtype
inner join runspecfueltype on (runspecfueltype.fueltypeid = fuelsubtype.fueltypeid);

cache select ff.* into outfile '##fuelformulation##'
from fuelformulation ff
inner join fuelsupply fs on fs.fuelformulationid = ff.fuelformulationid
inner join year y on y.fuelyearid = fs.fuelyearid
inner join runspecmonthgroup rsmg on rsmg.monthgroupid = fs.monthgroupid
where fuelregionid = ##context.fuelregionid## and
yearid = ##context.year##
group by ff.fuelformulationid order by null;

cache select * into outfile '##hcpermeationcoeff##'
from hcpermeationcoeff
where polprocessid in (##pollutantprocessids##);

cache select distinct hourday.* into outfile '##hourday##'
from hourday,runspechour,runspecday
where hourday.dayid = runspecday.dayid
and hourday.hourid = runspechour.hourid;

cache select link.* into outfile '##link##'
from link where linkid = ##context.iterlocation.linkrecordid##;

cache select * into outfile '##modelyear##'
from modelyear
where modelyearid <= ##context.year##
and modelyearid >= ##context.year## - 30;

cache select opmodedistribution.* into outfile '##opmodedistribution##'
from opmodedistribution, runspecsourcetype
where polprocessid in (##pollutantprocessids##)
and linkid = ##context.iterlocation.linkrecordid##
and runspecsourcetype.sourcetypeid = opmodedistribution.sourcetypeid;

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

cache select * into outfile '##runspecsourcetype##'
from runspecsourcetype;

cache select distinct sourcebindistribution.* into outfile '##sourcebindistribution##'
from sourcebindistributionfuelusage_##context.iterprocess.databasekey##_##context.iterlocation.countyrecordid##_##context.year## as sourcebindistribution, 
sourcetypemodelyear, sourcebin, runspecsourcefueltype
where polprocessid in (##pollutantprocessids##)
and sourcebindistribution.sourcetypemodelyearid = sourcetypemodelyear.sourcetypemodelyearid
and sourcetypemodelyear.modelyearid <= ##context.year##
and sourcetypemodelyear.modelyearid >= ##context.year## - 30
and sourcetypemodelyear.sourcetypeid = runspecsourcefueltype.sourcetypeid
and sourcebindistribution.sourcebinid = sourcebin.sourcebinid
and sourcebin.fueltypeid = runspecsourcefueltype.fueltypeid;

cache select sourcebin.* into outfile '##sourcebin##'
from runspecfueltype
inner join sourcebin on (sourcebin.fueltypeid = runspecfueltype.fueltypeid);

cache select * into outfile '##sourcehours##' from sourcehours
where monthid = ##context.monthid##
and yearid = ##context.year##
and linkid = ##context.iterlocation.linkrecordid##;

cache select sourcetypemodelyear.* into outfile '##sourcetypemodelyear##'
from sourcetypemodelyear,runspecsourcetype
where sourcetypemodelyear.sourcetypeid = runspecsourcetype.sourcetypeid
and modelyearid <= ##context.year##
and modelyearid >= ##context.year## - 30;

cache select sourcetypemodelyeargroup.* into outfile '##sourcetypemodelyeargroup##'
from sourcetypemodelyeargroup,runspecsourcetype
where sourcetypemodelyeargroup.sourcetypeid = runspecsourcetype.sourcetypeid;

cache select distinct temperatureadjustment.* into outfile '##temperatureadjustment##'
from temperatureadjustment
inner join runspecfueltype on (runspecfueltype.fueltypeid = temperatureadjustment.fueltypeid)
where polprocessid in (##pollutantprocessids##);

cache select year.* into outfile '##year##'
from year
where yearid = ##context.year##;

-- section withregclassid
cache select *
into outfile '##regclasssourcetypefraction##'
from regclasssourcetypefraction
where modelyearid <= ##context.year##
and modelyearid >= ##context.year## - 30;
-- end section withregclassid

-- insert into eventlog (eventtime, eventname) select now(), 'end extracting data';

-- end section extract data

-- section processing

-- create tables needed for processing
-- create table if not exists eventlog (eventrowid integer unsigned not null auto_increment, primary key (eventrowid), eventtime datetime, eventname varchar(120));

drop table if exists sourcebindistributionbyage;
create table sourcebindistributionbyage (
    sourcetypeid smallint not null,
    modelyearid smallint not null,
    agegroupid smallint not null,
    polprocessid int not null,
    sourcebinid bigint not null,
    sourcebinactivityfraction float
);
    
create unique index xpksourcebindistributionbyage on sourcebindistributionbyage (
    sourcetypeid asc,
    modelyearid asc,
    agegroupid asc,
    polprocessid asc,
    sourcebinid asc
);

drop table if exists sbweightedpermeationrate;
create table sbweightedpermeationrate (
    zoneid bigint not null, 
    yearid smallint not null, 
    polprocessid int not null,
    sourcetypeid smallint not null, 
    regclassid smallint not null,
    modelyearid smallint not null, 
    fueltypeid smallint not null,
    meanbaserate float
);

create unique index xpksbweightedpermeationrate on sbweightedpermeationrate (
    zoneid asc, 
    yearid asc, 
    polprocessid asc,
    sourcetypeid asc, 
    regclassid asc,
    modelyearid asc, 
    fueltypeid asc
);

drop table if exists temperatureadjustbyopmode;
create table temperatureadjustbyopmode (
    zoneid  integer not null,
    monthid smallint not null,
    hourdayid smallint not null,
    tanktemperaturegroupid smallint not null,
    opmodeid smallint not null,
    polprocessid int not null,
    fueltypeid smallint not null,
    modelyearid smallint not null,
    temperatureadjustbyopmode   float
);

drop table if exists weightedtemperatureadjust;
create table weightedtemperatureadjust (
    linkid integer not null,
    monthid smallint not null,
    hourdayid smallint not null,
    tanktemperaturegroupid smallint not null,
    sourcetypeid smallint not null,
    polprocessid int not null,
    fueltypeid smallint not null,
    modelyearid smallint not null,
    weightedtemperatureadjust float
);

create unique index xpkweightedtemperatureadjust on weightedtemperatureadjust (
    linkid asc,
    monthid asc,
    hourdayid asc,
    tanktemperaturegroupid asc,
    sourcetypeid asc,
    polprocessid asc,
    fueltypeid asc,
    modelyearid asc
);

drop table if exists weightedfueladjustment;
create table weightedfueladjustment (
    countyid integer not null,
    fuelyearid  smallint not null,
    monthgroupid smallint not null,
    polprocessid int not null,
    modelyearid smallint not null,
    sourcetypeid smallint not null,
    fueltypeid smallint not null,
    weightedfueladjustment float
);

create unique index xpkweightedfueladjustment on weightedfueladjustment (
    countyid asc,
    fuelyearid  asc,
    monthgroupid asc,
    polprocessid asc,
    modelyearid asc,
    fueltypeid asc,
    sourcetypeid asc
);

drop table if exists fueladjustedemissionrate;
create table fueladjustedemissionrate (
    zoneid integer not null,
    yearid smallint not null,
    polprocessid int not null,
    sourcetypeid smallint not null,
    regclassid smallint not null,
    modelyearid smallint not null,
    fueltypeid  smallint not null,
    fueladjustedemissionrate float
);

create unique index xpkfueladjustedemissionrate on fueladjustedemissionrate (
    zoneid asc,
    yearid asc,
    polprocessid asc,
    sourcetypeid asc,
    regclassid asc,
    modelyearid asc,
    fueltypeid asc
);

drop table if exists fueladjustedemissionquant;
create table fueladjustedemissionquant (
    linkid integer not null,
    hourdayid smallint not null,
    monthid smallint not null,
    yearid smallint not null,
    modelyearid smallint not null,
    sourcetypeid smallint not null,
    regclassid smallint not null,
    polprocessid int not null,
    fueltypeid smallint not null,
    fueladjustedemissionquant float
);

create unique index xpkfueladjustedemissionquant on fueladjustedemissionquant (
    linkid asc, 
    hourdayid asc,
    monthid asc, 
    yearid asc,
    modelyearid asc,
    sourcetypeid asc,
    regclassid asc,
    polprocessid asc,
    fueltypeid asc
);

update fuelformulation set etohvolume=0 where etohvolume is null;

loop ##loop.sourcetypeid##;
select sourcetypeid from runspecsourcetype;

--
-- pc-1: weight emission rates by source bin
--
-- insert into eventlog (eventtime, eventname) select now(), 'pc-1a: weight emission rates by source bin';

truncate sourcebindistributionbyage;

-- @algorithm add agegroupid to sourcebindistribution.sourcebinactivityfraction using model year and the calendar year.
insert into sourcebindistributionbyage (sourcetypeid, modelyearid, agegroupid,
    polprocessid, sourcebinid, sourcebinactivityfraction)
select sourcetypeid, modelyearid, agegroupid, polprocessid, 
    sourcebinid, sourcebinactivityfraction
from sourcebindistribution sbd
inner join sourcetypemodelyear stmy on (stmy.sourcetypemodelyearid=sbd.sourcetypemodelyearid)
inner join agecategory ac on (ac.ageid = ##context.year## - modelyearid)
where sourcetypeid = ##loop.sourcetypeid##;

analyze table sourcebindistributionbyage;

-- insert into eventlog (eventtime, eventname) select now(), 'pc-1b: weight emission rates by source bin';

truncate sbweightedpermeationrate;

-- section withregclassid

-- @algorithm sbweightedpermeationrate.meanbaserate = sourcebinactivityfraction * emissionratebyage.meanbaserate * regclassfraction
insert into sbweightedpermeationrate (zoneid, yearid, polprocessid, sourcetypeid, regclassid,
    modelyearid, fueltypeid, meanbaserate)
select ##context.iterlocation.zonerecordid##, ##context.year## as yearid, sbda.polprocessid, 
    sbda.sourcetypeid, stf.regclassid, sbda.modelyearid, sb.fueltypeid, 
    sum(sourcebinactivityfraction*meanbaserate*stf.regclassfraction) as meanbaserate
from sourcebindistributionbyage sbda
inner join emissionratebyage era on (era.sourcebinid=sbda.sourcebinid and
    era.polprocessid=sbda.polprocessid and era.agegroupid=sbda.agegroupid)
inner join sourcebin sb on (sb.sourcebinid=era.sourcebinid)
inner join regclasssourcetypefraction stf on (
    stf.sourcetypeid = sbda.sourcetypeid
    and stf.fueltypeid = sb.fueltypeid
    and stf.modelyearid = sbda.modelyearid)
where sbda.sourcetypeid = ##loop.sourcetypeid##
group by sbda.polprocessid, sbda.sourcetypeid, stf.regclassid, sbda.modelyearid, sb.fueltypeid
order by null;
-- end section withregclassid

-- section noregclassid
insert into sbweightedpermeationrate (zoneid, yearid, polprocessid, sourcetypeid, regclassid,
    modelyearid, fueltypeid, meanbaserate)
select ##context.iterlocation.zonerecordid##, ##context.year## as yearid, sbda.polprocessid, 
    sbda.sourcetypeid, 0 as regclassid, modelyearid, sb.fueltypeid, 
    sum(sourcebinactivityfraction*meanbaserate) as meanbaserate
from sourcebindistributionbyage sbda
inner join emissionratebyage era on (era.sourcebinid=sbda.sourcebinid and
    era.polprocessid=sbda.polprocessid and era.agegroupid=sbda.agegroupid)
inner join sourcebin sb on (sb.sourcebinid=era.sourcebinid)
where sbda.sourcetypeid = ##loop.sourcetypeid##
group by sbda.polprocessid, sbda.sourcetypeid, modelyearid, sb.fueltypeid
order by null;
-- end section noregclassid

analyze table sbweightedpermeationrate;

--
-- pc-2: calculate weighted temperature adjustment
--
-- insert into eventlog (eventtime, eventname) select now(), 'pc-2a: calculate weighted temperature adjustment';

truncate temperatureadjustbyopmode;

-- @algorithm temperatureadjustbyopmode = tempadjustterma*exp(tempadjusttermb*averagetanktemperature)
insert into temperatureadjustbyopmode (
    zoneid, monthid, hourdayid, tanktemperaturegroupid, opmodeid, polprocessid,
    fueltypeid, temperatureadjustbyopmode,
    modelyearid )
select zoneid, monthid, hourdayid, tanktemperaturegroupid, opmodeid, polprocessid, fueltypeid,
    tempadjustterma*exp(tempadjusttermb*averagetanktemperature) as temperatureadjustbyopmode,
    modelyearid
from averagetanktemperature
inner join temperatureadjustment
inner join modelyear my on (modelyearid between minmodelyearid and maxmodelyearid);

create index ixtemperatureadjustbyopmode1 on temperatureadjustbyopmode (
    hourdayid asc,
    polprocessid asc,
    opmodeid asc,
    zoneid asc,
    modelyearid asc
);


-- insert into eventlog (eventtime, eventname) select now(), 'pc-2b: calculate weighted temperature adjustment';

truncate weightedtemperatureadjust;

-- @algorithm weightedtemperatureadjust = sum(temperatureadjustbyopmode * opmodefraction) across all operating modes.
insert into weightedtemperatureadjust (
    linkid, monthid, hourdayid, tanktemperaturegroupid, sourcetypeid,
    polprocessid, fueltypeid, modelyearid, weightedtemperatureadjust)
select omd.linkid, monthid, taom.hourdayid, tanktemperaturegroupid,
    sourcetypeid, taom.polprocessid, fueltypeid, modelyearid,
    sum(temperatureadjustbyopmode*opmodefraction) as weightedtemperatureadjust
from temperatureadjustbyopmode taom
inner join opmodedistribution omd on (omd.hourdayid=taom.hourdayid and
    omd.polprocessid=taom.polprocessid and taom.opmodeid=omd.opmodeid)
inner join link l on (l.linkid=omd.linkid and l.zoneid=taom.zoneid)
where sourcetypeid = ##loop.sourcetypeid##
group by omd.linkid, monthid, taom.hourdayid, tanktemperaturegroupid,
    sourcetypeid, taom.polprocessid, fueltypeid, modelyearid
order by null; 

analyze table weightedtemperatureadjust;

--
-- pc-3: calculate weighted fuel adjustment
--
-- insert into eventlog (eventtime, eventname) select now(), 'pc-3: calculate weighted fuel adjustment';

truncate weightedfueladjustment;

-- insert into weightedfueladjustment (
-- countyid, fuelyearid, monthgroupid, polprocessid, modelyearid, fueltypeid,
-- sourcetypeid, weightedfueladjustment)
-- select countyid, fs.fuelyearid, monthgroupid, fa.polprocessid, modelyearid, fst.fueltypeid,
-- sourcetypeid, sum(marketshare*fueladjustment) as weightedfueladjustment
-- from fueladjustment fa
-- inner join pollutantprocessmodelyear ppmy on (ppmy.polprocessid=fa.polprocessid and
-- ppmy.fuelmygroupid=fa.fuelmygroupid)
-- inner join fuelsupply fs on (fs.fuelformulationid=fa.fuelformulationid)
-- inner join year y on (y.fuelyearid=fs.fuelyearid)
-- inner join fuelformulation ff on (ff.fuelformulationid=fs.fuelformulationid)
-- inner join fuelsubtype fst on (fst.fuelsubtypeid=ff.fuelsubtypeid)
-- where y.yearid=??context.year??
-- and sourcetypeid = ##loop.sourcetypeid##
-- group by countyid, fs.fuelyearid, monthgroupid, fa.polprocessid, modelyearid, fst.fueltypeid,
-- sourcetypeid
-- order by null

-- @algorithm weightedfueladjustment = sum(marketshare*(fueladjustment+gpafract*(fueladjustmentgpa-fueladjustment))) across fuel formulations in the fuel supply.
insert into weightedfueladjustment (
    countyid, fuelyearid, monthgroupid, polprocessid, modelyearid, fueltypeid,
    sourcetypeid, weightedfueladjustment)
select c.countyid, fs.fuelyearid, fs.monthgroupid, fa.polprocessid, ppmy.modelyearid, fst.fueltypeid,
    ##loop.sourcetypeid## as sourcetypeid, 
    sum(marketshare*(fueladjustment+gpafract*(fueladjustmentgpa-fueladjustment))) as weightedfueladjustment
from fuelsupply fs
inner join county c
inner join hcpermeationcoeff fa
inner join pollutantprocessmappedmodelyear ppmy on (ppmy.polprocessid=fa.polprocessid
    and ppmy.fuelmygroupid=fa.fuelmygroupid)
inner join year y on (y.fuelyearid=fs.fuelyearid)
inner join fuelformulation ff on (ff.fuelformulationid=fs.fuelformulationid)
inner join etohbin ebin on (ebin.etohthreshid=fa.etohthreshid
    and etohthreshlow <= ff.etohvolume and ff.etohvolume < etohthreshhigh)
inner join fuelsubtype fst on (fst.fuelsubtypeid=ff.fuelsubtypeid)
where y.yearid=##context.year##
and fs.fuelregionid = ##context.fuelregionid##
group by c.countyid, fs.fuelyearid, fs.monthgroupid, fa.polprocessid, ppmy.modelyearid, fst.fueltypeid, sourcetypeid
order by null;

--and sourcetypeid = ##loop.sourcetypeid##  -- sourcetypeid is not in any of the new tables

analyze table weightedfueladjustment;

--
-- pc-4: calculate fuel adjusted mean base rate
--
-- insert into eventlog (eventtime, eventname) select now(), 'pc-4: calculate fuel adjusted mean base rate';

truncate fueladjustedemissionrate;

-- @algorithm fueladjustedemissionrate = sbweightedpermeationrate.meanbaserate * weightedfueladjustment
insert into fueladjustedemissionrate (zoneid, yearid, polprocessid,
    sourcetypeid, regclassid, modelyearid, fueltypeid, fueladjustedemissionrate)
select zoneid, sbwpr.yearid, sbwpr.polprocessid, sbwpr.sourcetypeid, sbwpr.regclassid,
    sbwpr.modelyearid, sbwpr.fueltypeid, meanbaserate*weightedfueladjustment 
from sbweightedpermeationrate sbwpr
inner join weightedfueladjustment wfa on (wfa.polprocessid=sbwpr.polprocessid and
    wfa.modelyearid=sbwpr.modelyearid and wfa.sourcetypeid=sbwpr.sourcetypeid and
    wfa.fueltypeid=sbwpr.fueltypeid)
inner join year y on (y.yearid=sbwpr.yearid and y.fuelyearid=wfa.fuelyearid)
where sbwpr.sourcetypeid = ##loop.sourcetypeid##;

analyze table fueladjustedemissionrate;

--
-- pc-5: calculate fuel adjusted emissionquant
--
-- insert into eventlog (eventtime, eventname) select now(), 'pc-5: calculate fuel adjusted emissionquant';

truncate fueladjustedemissionquant;

-- @algorithm fueladjustedemissionquant = fueladjustedemissionrate * sourcehours
insert into fueladjustedemissionquant (
    linkid, hourdayid, monthid, yearid, modelyearid,
    sourcetypeid, regclassid, polprocessid, fueltypeid, fueladjustedemissionquant) 
select sh.linkid, sh.hourdayid, sh.monthid, fambr.yearid, 
    fambr.modelyearid, fambr.sourcetypeid, fambr.regclassid, fambr.polprocessid, fambr.fueltypeid,
    fueladjustedemissionrate*sourcehours as fueladjustedemissionquant
from sourcehours sh 
inner join fueladjustedemissionrate fambr on (fambr.yearid=sh.yearid 
    and fambr.modelyearid=sh.yearid-sh.ageid and fambr.sourcetypeid=sh.sourcetypeid)
inner join link l on (l.linkid=sh.linkid and l.zoneid=fambr.zoneid)
where sh.yearid=##context.year##
and fambr.sourcetypeid = ##loop.sourcetypeid##;

analyze table fueladjustedemissionquant;

--
-- pc-6: calculate emissionquant with temperature adjustment
--
-- insert into eventlog (eventtime, eventname) select now(), 'pc-6: calculate emissionquant with temperature adjustment';

-- @algorithm emissionquant = weightedtemperatureadjust * fueladjustedemissionquant
insert into movesworkeroutput (
    stateid, countyid, zoneid, linkid, roadtypeid, yearid, monthid, dayid, hourid,
    pollutantid, processid, sourcetypeid, regclassid, modelyearid, fueltypeid, scc, emissionquant)
select stateid, l.countyid, l.zoneid, faeq.linkid, roadtypeid, yearid, faeq.monthid, dayid, hourid, pollutantid,
    processid, faeq.sourcetypeid, faeq.regclassid, faeq.modelyearid, faeq.fueltypeid, null as scc, 
    weightedtemperatureadjust*fueladjustedemissionquant as emissionquant
from fueladjustedemissionquant faeq 
inner join weightedtemperatureadjust wta on (
    wta.linkid=faeq.linkid and wta.hourdayid=faeq.hourdayid and
    wta.monthid=faeq.monthid and wta.sourcetypeid=faeq.sourcetypeid and
    wta.polprocessid=faeq.polprocessid and wta.fueltypeid=faeq.fueltypeid and
    wta.modelyearid=faeq.modelyearid)
inner join pollutantprocessassoc ppa on (
    ppa.polprocessid=faeq.polprocessid)
inner join pollutantprocessmodelyear ppmy on (
    ppmy.polprocessid=ppa.polprocessid and
    ppmy.modelyearid=faeq.modelyearid)
inner join sourcetypemodelyeargroup stmyg on (
    stmyg.sourcetypeid=faeq.sourcetypeid and
    stmyg.modelyeargroupid=ppmy.modelyeargroupid and
    stmyg.tanktemperaturegroupid=wta.tanktemperaturegroupid)
inner join hourday hd on (hd.hourdayid=faeq.hourdayid)
inner join link l on (l.linkid=faeq.linkid)
inner join county c on (c.countyid=l.countyid)
where faeq.sourcetypeid = ##loop.sourcetypeid##;

end loop ##loop.sourcetypeid##;

alter table temperatureadjustbyopmode drop index ixtemperatureadjustbyopmode1;

-- end section processing
-- section cleanup
-- insert into eventlog (eventtime, eventname) select now(), 'section cleanup';

drop table if exists fueladjustedemissionquant;
drop table if exists fueladjustedemissionrate;
drop table if exists sbweightedpermeationrate;
drop table if exists sourcebindistributionbyage;
drop table if exists temperatureadjustbyopmode;
drop table if exists weightedfueladjustment;
drop table if exists weightedtemperatureadjust;
-- end section cleanup
