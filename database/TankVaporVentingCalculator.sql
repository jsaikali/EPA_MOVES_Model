-- version 2013-09-15
-- author wesley faler

-- @algorithm
-- @owner single-day tank vapor venting calculator
-- @notused

-- section create remote tables for extracted data

##create.agecategory##;
truncate agecategory;

##create.averagetankgasoline##;
truncate averagetankgasoline;

##create.averagetanktemperature##;
truncate averagetanktemperature;

##create.coldsoakinitialhourfraction##;
truncate coldsoakinitialhourfraction;

##create.coldsoaktanktemperature##;
truncate coldsoaktanktemperature;

##create.county##;
truncate county;

##create.cumtvvcoeffs##;
truncate cumtvvcoeffs;

##create.emissionratebyage##;
truncate emissionratebyage;

##create.fueltype##;
truncate fueltype;

##create.hourday##;
truncate hourday;

##create.imcoverage##;
truncate imcoverage;

##create.imfactor##;
truncate imfactor;

##create.link##;
truncate link;

##create.monthofanyyear##;
truncate monthofanyyear;

##create.opmodedistribution##;
truncate opmodedistribution;

##create.pollutantprocessassoc##;
truncate pollutantprocessassoc;

##create.pollutantprocessmodelyear##;
truncate pollutantprocessmodelyear;

##create.runspecday##;
truncate runspecday;

##create.runspechourday##;
truncate runspechourday;

##create.runspecmonth##;
truncate runspecmonth;

##create.runspecsourcetype##;
truncate runspecsourcetype;

##create.sourcebin##;
truncate sourcebin;

##create.sourcebindistribution##;
truncate sourcebindistribution;

##create.sourcehours##;
truncate sourcehours;

##create.sourcetypemodelyear##;
truncate sourcetypemodelyear;

##create.sourcetypemodelyeargroup##;
truncate sourcetypemodelyeargroup;

##create.tankvaporgencoeffs##;
truncate tankvaporgencoeffs;

##create.year##;
truncate year;

##create.zone##;
truncate zone;

-- end section create remote tables for extracted data

-- section extract data
-- create table if not exists eventlog (eventrowid integer unsigned not null auto_increment, primary key (eventrowid), eventtime datetime, eventname varchar(120));
-- insert into eventlog (eventtime, eventname) select now(), 'EXTRACTING DATA';

cache select * into outfile '##agecategory##'
from agecategory;

cache select averagetankgasoline.* into outfile '##averagetankgasoline##'
from averagetankgasoline
inner join monthofanyyear on (monthofanyyear.monthgroupid = averagetankgasoline.monthgroupid)
inner join year on (year.yearid = ##context.year##)
where zoneid = ##context.iterlocation.zonerecordid##
and averagetankgasoline.fuelyearid = year.fuelyearid
and monthid = ##context.monthid##;

cache select * into outfile '##averagetanktemperature##' from averagetanktemperature
where zoneid = ##context.iterlocation.zonerecordid##
and monthid = ##context.monthid##;

cache select * into outfile '##coldsoakinitialhourfraction##' from coldsoakinitialhourfraction
where zoneid = ##context.iterlocation.zonerecordid##
and monthid = ##context.monthid##;

cache select * into outfile '##coldsoaktanktemperature##' from coldsoaktanktemperature
where zoneid = ##context.iterlocation.zonerecordid##
and monthid = ##context.monthid##;

cache select * into outfile '##county##'
from county
where countyid = ##context.iterlocation.countyrecordid##;

cache select distinct cumtvvcoeffs.* into outfile '##cumtvvcoeffs##'
from cumtvvcoeffs, sourcebindistribution, sourcetypemodelyear, sourcebin, runspecsourcefueltype
where sourcebindistribution.polprocessid in (##pollutantprocessids##)
and cumtvvcoeffs.polprocessid in (##pollutantprocessids##)
and sourcebindistribution.sourcetypemodelyearid = sourcetypemodelyear.sourcetypemodelyearid
and sourcetypemodelyear.modelyearid <= ##context.year##
and sourcetypemodelyear.modelyearid >= ##context.year## - 30
and sourcetypemodelyear.sourcetypeid = runspecsourcefueltype.sourcetypeid
and sourcebindistribution.sourcebinid = sourcebin.sourcebinid
and sourcebin.fueltypeid = runspecsourcefueltype.fueltypeid
and sourcebin.regclassid = cumtvvcoeffs.regclassid;

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
and emissionratebyage.polprocessid in (##pollutantprocessids##)
and emissionratebyage.opmodeid in (150, 300);

cache select distinct fueltype.* into outfile '##fueltype##'
from fueltype
inner join runspecsourcefueltype on (runspecsourcefueltype.fueltypeid = fueltype.fueltypeid);

cache select distinct hourday.* into outfile '##hourday##'
from hourday,runspechour,runspecday
where hourday.dayid = runspecday.dayid
and hourday.hourid = runspechour.hourid;

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

cache select link.* into outfile '##link##'
from link where linkid = ##context.iterlocation.linkrecordid##;

cache select * into outfile '##monthofanyyear##'
from monthofanyyear
where monthid = ##context.monthid##;

cache(monthid=##context.monthid##) select opmodedistribution.* into outfile '##opmodedistribution##'
from opmodedistribution, runspecsourcetype
where polprocessid in (##pollutantprocessids##)
and linkid = ##context.iterlocation.linkrecordid##
and runspecsourcetype.sourcetypeid = opmodedistribution.sourcetypeid
and opmodeid in (150, 151, 300);

cache select * into outfile '##pollutantprocessassoc##'
from pollutantprocessassoc
where processid=##context.iterprocess.databasekey##;

cache select * into outfile '##pollutantprocessmodelyear##'
from pollutantprocessmodelyear
where modelyearid <= ##context.year##
and modelyearid >= ##context.year## - 30
and polprocessid in (##pollutantprocessids##);

cache select * into outfile '##runspecmonth##'
from runspecmonth
where monthid = ##context.monthid##;

cache select * into outfile '##runspecday##'
from runspecday;

cache select * into outfile '##runspechourday##'
from runspechourday;

cache select * into outfile '##runspecsourcetype##'
from runspecsourcetype;

cache select distinct sourcebin.* into outfile '##sourcebin##'
from sourcebindistribution, sourcetypemodelyear, sourcebin, runspecsourcefueltype
where polprocessid in (##pollutantprocessids##)
and sourcebindistribution.sourcetypemodelyearid = sourcetypemodelyear.sourcetypemodelyearid
and sourcetypemodelyear.modelyearid <= ##context.year##
and sourcetypemodelyear.modelyearid >= ##context.year## - 30
and sourcetypemodelyear.sourcetypeid = runspecsourcefueltype.sourcetypeid
and sourcebindistribution.sourcebinid = sourcebin.sourcebinid
and sourcebin.fueltypeid = runspecsourcefueltype.fueltypeid;

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

cache select * into outfile '##tankvaporgencoeffs##' from tankvaporgencoeffs;

cache select year.* into outfile '##year##'
from year
where yearid = ##context.year##;

cache select * into outfile '##zone##'
from zone
where zoneid = ##context.iterlocation.zonerecordid##;

-- insert into eventlog (eventtime, eventname) select now(), 'END EXTRACTING DATA';

-- end section extract data

-- section processing

alter table coldsoaktanktemperature add key speed1 (hourid);
analyze table coldsoaktanktemperature;

-- create tables needed for processing
-- create table if not exists eventlog (eventrowid integer unsigned not null auto_increment, primary key (eventrowid), eventtime datetime, eventname varchar(120));

-- 
-- tvv-1: complete i/m adjustment fraction information (like crec 1-a)
--
-- insert into eventlog (eventtime, eventname) select now(), 'TVV-1';
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
from pollutantprocessmodelyear ppmy
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
group by ppa.processid,
 ppa.pollutantid,
 ppmy.modelyearid,
 imf.fueltypeid,
 imc.sourcetypeid;

-- 
-- tvv-2: determine hour of peak cold soak tank temperature
--
-- insert into eventlog (eventtime, eventname) select now(), 'TVV-2';
drop table if exists peakhourofcoldsoak;

--zoneid int not null,  note: since calc is at year level, there is only 1 zone
create table peakhourofcoldsoak (
monthid smallint(6) not null,
peakhourid smallint(6) not null,
primary key (monthid),
index (peakhourid)
);

insert into peakhourofcoldsoak (monthid, peakhourid)
select monthid,
999-mod(max(round(coldsoaktanktemperature,2)*100000+(999-hourid)),1000) as peakhourid
from coldsoaktanktemperature
group by monthid
order by null;

analyze table peakhourofcoldsoak;

-- 
-- tvv-3: calculate tankvaporgenerated (tvg) by ethanol level
--
-- insert into eventlog (eventtime, eventname) select now(), 'TVV-3';
drop table if exists tankvaporgenerated;

create table tankvaporgenerated (
	hourdayid smallint(6) not null,
	initialhourdayid smallint(6) not null,
	ethanollevelid smallint(6) not null,
	monthid smallint(6) not null,
	sourcetypeid smallint(6) not null,
	fuelyearid smallint(6) not null,
	fueltypeid smallint(6) not null,
	tankvaporgenerated float null,
	primary key (hourdayid, initialhourdayid, ethanollevelid, monthid, sourcetypeid, fuelyearid, fueltypeid)
);

-- note: "K" is set to 1.0 in the calculation below
insert into tankvaporgenerated (hourdayid, initialhourdayid, ethanollevelid,
monthid, sourcetypeid, fuelyearid, fueltypeid, tankvaporgenerated)
select ihf.hourdayid, ihf.initialhourdayid, coeffs.ethanollevelid,
ihf.monthid, ihf.sourcetypeid, avggas.fuelyearid, avggas.fueltypeid,
case when t1.coldsoaktanktemperature >= t2.coldsoaktanktemperature then 0.0
else
1.0*(tvgterma*exp(tvgtermb*rvp)*(exp(tvgtermc*t2.coldsoaktanktemperature)-exp(tvgtermc*t1.coldsoaktanktemperature)))
end as tankvaporgenerated
from coldsoakinitialhourfraction ihf
inner join hourday hd on (hd.hourdayid = ihf.hourdayid)
inner join hourday ihd on (ihd.hourdayid = ihf.initialhourdayid)
inner join peakhourofcoldsoak ph on (ihf.monthid = ph.monthid)
inner join coldsoaktanktemperature t2 on (ihf.monthid = t2.monthid and hd.hourid = t2.hourid)
inner join coldsoaktanktemperature t1 on (ihf.monthid = t1.monthid and ihd.hourid = t1.hourid)
inner join zone on (ihf.zoneid = zone.zoneid)
inner join county on (zone.countyid = county.countyid)
inner join tankvaporgencoeffs coeffs on (county.altitude = coeffs.altitude)
inner join monthofanyyear m on (ihf.monthid = m.monthid)
inner join averagetankgasoline avggas on (m.monthgroupid = avggas.monthgroupid)
where ihf.hourdayid <> ihf.initialhourdayid
and hd.hourid <= ph.peakhourid
and ihf.coldsoakinitialhourfraction > 0;

analyze table tankvaporgenerated;

-- 
-- tvv-4: calculate ethanol-weighted tvg
--
-- insert into eventlog (eventtime, eventname) select now(), 'TVV-4';
drop table if exists ethanolweightedtvg;

create table ethanolweightedtvg (
	hourdayid smallint(6) not null,
	initialhourdayid smallint(6) not null,
	monthid smallint(6) not null,
	sourcetypeid smallint(6) not null,
	fuelyearid smallint(6) not null,
	fueltypeid smallint(6) not null,
	ethanolweightedtvg float null,
	primary key (hourdayid, initialhourdayid, monthid, sourcetypeid, fuelyearid, fueltypeid)
);

insert into ethanolweightedtvg (hourdayid, initialhourdayid, monthid, sourcetypeid, fuelyearid, fueltypeid, ethanolweightedtvg)
select t0.hourdayid, t0.initialhourdayid, t0.monthid, t0.sourcetypeid, t0.fuelyearid, t0.fueltypeid,
(t10.tankvaporgenerated*(least(10.0,etohvolume)/10.0)
+t0.tankvaporgenerated*(1.0-least(10.0,etohvolume)/10.0)) as ethanolweightedtvg
from tankvaporgenerated t0
inner join tankvaporgenerated t10 on (t0.hourdayid=t10.hourdayid and t0.initialhourdayid=t10.initialhourdayid
and t0.monthid=t10.monthid and t0.sourcetypeid=t10.sourcetypeid and t0.fuelyearid=t10.fuelyearid
and t0.fueltypeid=t10.fueltypeid)
inner join monthofanyyear m on (t10.monthid = m.monthid)
inner join averagetankgasoline avggas on (m.monthgroupid = avggas.monthgroupid and t10.fuelyearid = avggas.fuelyearid
and t10.fueltypeid=avggas.fueltypeid)
where t0.ethanollevelid = 0
and t10.ethanollevelid = 10;

analyze table ethanolweightedtvg;

-- 
-- tvv-5: calculate cummulative tank vapor vented (tvv)
--
-- insert into eventlog (eventtime, eventname) select now(), 'TVV-5';
drop table if exists cummulativetankvaporvented;

create table cummulativetankvaporvented (
	regclassid smallint(6) not null,
	ageid smallint(6) not null,
	polprocessid int not null,
	dayid smallint(6) not null,
	hourid smallint(6) not null,
	initialhourdayid smallint(6) not null,
	monthid smallint(6) not null,
	sourcetypeid smallint(6) not null,
	fueltypeid smallint(6) not null,
	tankvaporvented float null,
	tankvaporventedim float null,
	hourdayid smallint(6) not null,
	priorhourid smallint(6) not null,
	primary key (regclassid, ageid, polprocessid, dayid, hourid, initialhourdayid, monthid, sourcetypeid, fueltypeid),
	index (priorhourid)
);

insert into cummulativetankvaporvented (regclassid, ageid, polprocessid, dayid, hourid, initialhourdayid, 
monthid, sourcetypeid, fueltypeid, tankvaporvented, tankvaporventedim,
hourdayid, priorhourid)
select coeffs.regclassid, acat.ageid, coeffs.polprocessid, hd.dayid, hd.hourid, ew.initialhourdayid,
ew.monthid, ew.sourcetypeid, ew.fueltypeid,
greatest(tvvterma+ethanolweightedtvg*(tvvtermb+tvvtermc*ethanolweightedtvg),0.0) as tankvaporvented,
greatest(tvvtermaim+ethanolweightedtvg*(tvvtermbim+tvvtermcim*ethanolweightedtvg),0.0) as tankvaporventedim,
ew.hourdayid,
mod(hd.hourid-1-1+24,24)+1
from cumtvvcoeffs coeffs
inner join agecategory acat on (coeffs.agegroupid = acat.agegroupid)
inner join pollutantprocessmodelyear ppmy 
on (coeffs.polprocessid=ppmy.polprocessid and coeffs.modelyeargroupid = ppmy.modelyeargroupid)
inner join ethanolweightedtvg ew
inner join hourday hd on (hd.hourdayid = ew.hourdayid)
inner join year y on (y.fuelyearid = ew.fuelyearid)
where coeffs.polprocessid in (##pollutantprocessids##)
and acat.ageid = y.yearid - ppmy.modelyearid;

analyze table cummulativetankvaporvented;

-- 
-- tvv-6: calculate unweighted hourly tvv emission by regulatory class and vehicle age
--
-- insert into eventlog (eventtime, eventname) select now(), 'TVV-6';
drop table if exists unweightedhourlytvv;

create table unweightedhourlytvv (
	zoneid int(11) not null default ##context.iterlocation.zonerecordid##,
	regclassid smallint(6) not null,
	ageid smallint(6) not null,
	polprocessid int not null,
	hourdayid smallint(6) not null,
	initialhourdayid smallint(6) not null,
	monthid smallint(6) not null,
	sourcetypeid smallint(6) not null,
	fueltypeid smallint(6) not null,
	unweightedhourlytvv float null,
	unweightedhourlytvvim float null,
	index (sourcetypeid, zoneid, monthid, hourdayid, initialhourdayid, fueltypeid)
);

insert into unweightedhourlytvv (regclassid, ageid, polprocessid, hourdayid, initialhourdayid, 
monthid, sourcetypeid, fueltypeid, unweightedhourlytvv, unweightedhourlytvvim)
select ctv1.regclassid, ctv1.ageid, ctv1.polprocessid, ctv1.hourdayid, ctv1.initialhourdayid, 
ctv1.monthid, ctv1.sourcetypeid, ctv1.fueltypeid, 
greatest(ctv1.tankvaporvented-coalesce(ctv2.tankvaporvented,0.0),0.0) as unweightedhourlytvv,
greatest(ctv1.tankvaporventedim-coalesce(ctv2.tankvaporventedim,0.0),0.0) as unweightedhourlytvvim 
from cummulativetankvaporvented ctv1 left join cummulativetankvaporvented ctv2 
on (ctv1.regclassid = ctv2.regclassid and ctv1.ageid = ctv2.ageid
and ctv1.polprocessid = ctv2.polprocessid and ctv1.initialhourdayid = ctv2.initialhourdayid 
and ctv1.monthid = ctv2.monthid and ctv1.sourcetypeid = ctv2.sourcetypeid
and ctv1.fueltypeid = ctv2.fueltypeid
and ctv1.priorhourid = ctv2.hourid
and ctv1.dayid = ctv2.dayid);

analyze table unweightedhourlytvv;

-- 
-- tvv-7: calculate weighted hourly tvv across initial/current pair
--
-- insert into eventlog (eventtime, eventname) select now(), 'TVV-7';
drop table if exists hourlytvv;

create table hourlytvv (
	regclassid smallint(6) not null,
	ageid smallint(6) not null,
	polprocessid int not null,
	hourdayid smallint(6) not null,
	monthid smallint(6) not null,
	sourcetypeid smallint(6) not null,
	fueltypeid smallint(6) not null,
	hourlytvv float null,
	hourlytvvim float null,
	primary key (regclassid, ageid, polprocessid, hourdayid, monthid, sourcetypeid, fueltypeid)
);

-- handle hourdayids <= the peak hour
drop table if exists hourlytvvtemp;

create table hourlytvvtemp (
	regclassid smallint(6) not null,
	ageid smallint(6) not null,
	polprocessid int not null,
	hourdayid smallint(6) not null,
	monthid smallint(6) not null,
	sourcetypeid smallint(6) not null,
	fueltypeid smallint(6) not null,
	hourlytvv float null,
	hourlytvvim float null,
	index (regclassid, ageid, polprocessid, hourdayid, monthid, sourcetypeid, fueltypeid)
);

insert into hourlytvvtemp (regclassid, ageid, polprocessid, hourdayid, monthid, sourcetypeid, fueltypeid,
hourlytvv, hourlytvvim)
select uhtvv.regclassid, uhtvv.ageid, uhtvv.polprocessid, uhtvv.hourdayid, uhtvv.monthid, uhtvv.sourcetypeid, uhtvv.fueltypeid,
(unweightedhourlytvv*coldsoakinitialhourfraction) as hourlytvv,
(unweightedhourlytvvim*coldsoakinitialhourfraction) as hourlytvvim
from unweightedhourlytvv uhtvv
inner join coldsoakinitialhourfraction ihf on (uhtvv.sourcetypeid = ihf.sourcetypeid
and uhtvv.zoneid = ihf.zoneid and uhtvv.monthid = ihf.monthid 
and uhtvv.hourdayid = ihf.hourdayid and uhtvv.initialhourdayid = ihf.initialhourdayid);

insert into hourlytvv (regclassid, ageid, polprocessid, hourdayid, monthid, sourcetypeid, fueltypeid, hourlytvv, hourlytvvim)
select regclassid, ageid, polprocessid, hourdayid, monthid, sourcetypeid, fueltypeid,
sum(hourlytvv) as hourlytvv,
sum(hourlytvvim) as hourlytvvim
from hourlytvvtemp
group by regclassid, ageid, polprocessid, hourdayid, monthid, sourcetypeid, fueltypeid
order by null;

drop table if exists hourlytvvtemp;

-- handle hourdayids > the peak hour
drop table if exists copyofhourlytvv;
create table copyofhourlytvv (
	regclassid smallint(6) not null,
	ageid smallint(6) not null,
	polprocessid int not null,
	dayid smallint(6) not null,
	hourid smallint(6) not null,
	monthid smallint(6) not null,
	sourcetypeid smallint(6) not null,
	fueltypeid smallint(6) not null,
	hourlytvv float null,
	hourlytvvim float null,
	primary key (regclassid, ageid, polprocessid, dayid, hourid, monthid, sourcetypeid, fueltypeid)
);
insert into copyofhourlytvv (regclassid, ageid, polprocessid, dayid, hourid, 
monthid, sourcetypeid, fueltypeid, hourlytvv, hourlytvvim)
select regclassid, ageid, polprocessid, dayid, hourid,
monthid, sourcetypeid, fueltypeid, hourlytvv, hourlytvvim 
from hourlytvv
inner join hourday on (hourday.hourdayid=hourlytvv.hourdayid);

insert into hourlytvv (regclassid, ageid, polprocessid, hourdayid, monthid,
sourcetypeid, fueltypeid, hourlytvv, hourlytvvim)
select htvv.regclassid, htvv.ageid, htvv.polprocessid, hd.hourdayid, htvv.monthid, htvv.sourcetypeid, htvv.fueltypeid,
hourlytvv * case 
	when (hd.hourid-ph.peakhourid) >= 4 then 0.0005
	when (hd.hourid-ph.peakhourid) >= 3 then 0.0040
	when (hd.hourid-ph.peakhourid) >= 2 then 0.0100
	when (hd.hourid-ph.peakhourid) >= 1 then 0.0200
	end as hourlytvv,
hourlytvvim * case 
	when (hd.hourid-ph.peakhourid) >= 4 then 0.0005
	when (hd.hourid-ph.peakhourid) >= 3 then 0.0040
	when (hd.hourid-ph.peakhourid) >= 2 then 0.0100
	when (hd.hourid-ph.peakhourid) >= 1 then 0.0200
	end as hourlytvvim
from copyofhourlytvv htvv
inner join peakhourofcoldsoak ph on (htvv.monthid = ph.monthid and htvv.hourid = ph.peakhourid)
inner join hourday hd on (
	hd.hourid > ph.peakhourid 
	and hd.hourid < ph.peakhourid + 5 
	and hd.dayid = htvv.dayid
);

analyze table hourlytvv;

-- tvv-8: calculate i/m-adjusted meanbaserates
--
-- insert into eventlog (eventtime, eventname) select now(), 'TVV-8';
drop table if exists weightedmeanbaserate;

create table weightedmeanbaserate (
	polprocessid int not null,
	sourcetypeid smallint(6) not null,
	fueltypeid smallint(6) not null,
	monthid smallint(6) not null,
	hourdayid smallint(6) not null,
	modelyearid smallint(6) not null,
	opmodeid smallint(6) not null,
	weightedmeanbaserate float not null,
	weightedmeanbaserateim float not null,
	primary key (polprocessid, sourcetypeid, fueltypeid, monthid, hourdayid, modelyearid, opmodeid)
);

-- for cold soak mode (opmodeid=151)
insert into weightedmeanbaserate (polprocessid, sourcetypeid, fueltypeid, monthid, hourdayid, 
	modelyearid, opmodeid, weightedmeanbaserate, weightedmeanbaserateim)
select htvv.polprocessid, htvv.sourcetypeid, sb.fueltypeid, htvv.monthid, htvv.hourdayid, 
	stmy.modelyearid, 151 as opmodeid,
	sum(sourcebinactivityfraction*hourlytvv) as weightedmeanbaserate,
	sum(sourcebinactivityfraction*hourlytvvim) as weightedmeanbaserateim
from hourlytvv htvv
inner join sourcetypemodelyear stmy on (stmy.modelyearid=##context.year##-htvv.ageid
	and stmy.sourcetypeid=htvv.sourcetypeid)
inner join sourcebindistribution sbd on (sbd.sourcetypemodelyearid=stmy.sourcetypemodelyearid
	and sbd.polprocessid=htvv.polprocessid)
inner join sourcebin sb on (sb.sourcebinid=sbd.sourcebinid and sb.fueltypeid=htvv.fueltypeid
	and sb.regclassid=htvv.regclassid)
inner join fueltype on (fueltype.fueltypeid = sb.fueltypeid and subjecttoevapcalculations = 'Y')
inner join pollutantprocessmodelyear ppmy on (ppmy.polprocessid=sbd.polprocessid
	and ppmy.modelyearid=stmy.modelyearid and ppmy.modelyeargroupid=sb.modelyeargroupid)
group by htvv.polprocessid, htvv.sourcetypeid, sb.fueltypeid, htvv.monthid, htvv.hourdayid, 
	stmy.modelyearid
order by null;

-- for operating and hot soak modes (opmodeids 300 and 150)
insert into weightedmeanbaserate (polprocessid, sourcetypeid, fueltypeid, monthid, hourdayid, 
	modelyearid, opmodeid, weightedmeanbaserate, weightedmeanbaserateim)
select er.polprocessid, stmy.sourcetypeid, sb.fueltypeid, rsm.monthid, rshd.hourdayid, 
	stmy.modelyearid, er.opmodeid,
	sum(sourcebinactivityfraction*meanbaserate) as weightedmeanbaserate,
	sum(sourcebinactivityfraction*meanbaserateim) as weightedmeanbaserateim
from emissionratebyage er
inner join agecategory acat on (acat.agegroupid=er.agegroupid)
inner join sourcebin sb on (sb.sourcebinid=er.sourcebinid)
inner join fueltype on (fueltype.fueltypeid = sb.fueltypeid and subjecttoevapcalculations = 'Y')
inner join sourcebindistribution sbd on (sbd.sourcebinid=sb.sourcebinid
	and sbd.polprocessid=er.polprocessid)
inner join sourcetypemodelyear stmy on (stmy.sourcetypemodelyearid=sbd.sourcetypemodelyearid
	and stmy.modelyearid=##context.year##-acat.ageid)
inner join pollutantprocessmodelyear ppmy on (ppmy.polprocessid=sbd.polprocessid
	and ppmy.modelyearid=stmy.modelyearid and ppmy.modelyeargroupid=sb.modelyeargroupid)
inner join runspecsourcetype rsst on (rsst.sourcetypeid=stmy.sourcetypeid)
inner join runspecmonth rsm
inner join runspechourday rshd
where er.polprocessid in (##pollutantprocessids##)
and opmodeid in (150, 300)
group by er.polprocessid, stmy.sourcetypeid, sb.fueltypeid, rsm.monthid, rshd.hourdayid, 
	stmy.modelyearid, er.opmodeid
order by null;

-- analyze table weightedmeanbaserate;

alter table movesworkeroutput add emissionquantim float null;

alter table weightedmeanbaserate add key speed1 (sourcetypeid, hourdayid, polprocessid, opmodeid);
analyze table weightedmeanbaserate;

alter table sourcehours add key speed1 (hourdayid, monthid, sourcetypeid, ageid);
analyze table sourcehours;

-- 
-- tvv-9: calculate movesworkeroutput by source type
--
-- insert into eventlog (eventtime, eventname) select now(), 'TVV-9 WITHOUT SCC';
insert into movesworkeroutput (yearid, monthid, dayid, hourid, stateid, countyid,
zoneid, linkid, pollutantid, processid, sourcetypeid, fueltypeid, modelyearid,
roadtypeid, scc, emissionquant, emissionquantim)
select ##context.year## as yearid, w.monthid, hd.dayid, hd.hourid,
##context.iterlocation.staterecordid## as stateid,
##context.iterlocation.countyrecordid## as countyid,
##context.iterlocation.zonerecordid## as zoneid,
##context.iterlocation.linkrecordid## as linkid,
ppa.pollutantid, ppa.processid, w.sourcetypeid, w.fueltypeid, w.modelyearid,
##context.iterlocation.roadtyperecordid##, null as scc,
(weightedmeanbaserate*sourcehours*opmodefraction) as emissionquant,
(weightedmeanbaserateim*sourcehours*opmodefraction) as emissionquantim
from weightedmeanbaserate w
inner join sourcehours sh on (sh.hourdayid=w.hourdayid and sh.monthid=w.monthid
and sh.yearid=##context.year## and sh.ageid=##context.year##-w.modelyearid
and sh.linkid=##context.iterlocation.linkrecordid## and sh.sourcetypeid=w.sourcetypeid)
inner join opmodedistribution omd on (omd.sourcetypeid=sh.sourcetypeid
and omd.hourdayid=w.hourdayid and omd.linkid=##context.iterlocation.linkrecordid##
and omd.polprocessid=w.polprocessid and omd.opmodeid=w.opmodeid)
inner join pollutantprocessassoc ppa on (ppa.polprocessid=omd.polprocessid)
inner join hourday hd on (hd.hourdayid=omd.hourdayid);

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
-- insert into eventlog (eventtime, eventname) select now(), 'SECTION CLEANUP';

drop table if exists peakhourofcoldsoak;
drop table if exists tankvaporgenerated;
drop table if exists ethanolweightedtvg;
drop table if exists cummulativetankvaporvented;
drop table if exists unweightedhourlytvv;
drop table if exists hourlytvv;
drop table if exists weightedmeanbaserate;
drop table if exists imcoveragemergedungrouped;
drop table if exists copyofhourlytvv;
-- end section cleanup
