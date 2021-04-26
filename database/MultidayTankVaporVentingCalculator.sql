-- author wesley faler
-- author david hawkins
-- version 2014-06-24

-- @algorithm
-- @owner tank vapor venting calculator
-- @calculator

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

##create.evaptemperatureadjustment##;
truncate evaptemperatureadjustment;

##create.evaprvptemperatureadjustment##;
truncate evaprvptemperatureadjustment;

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

##create.pollutantprocessmappedmodelyear##;
truncate pollutantprocessmappedmodelyear;

##create.runspecday##;
truncate runspecday;

##create.runspechourday##;
truncate runspechourday;

##create.runspecmonth##;
truncate runspecmonth;

##create.runspecsourcetype##;
truncate runspecsourcetype;

##create.samplevehiclesoaking##;
truncate samplevehiclesoaking;

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

create table if not exists stmytvvcoeffs (
  sourcetypeid smallint not null,
  modelyearid smallint not null,
  fueltypeid smallint not null,
  polprocessid int not null default '0',
  backpurgefactor double default null,
  averagecanistercapacity double default null,
  leakfraction double default null,
  leakfractionim double default null,
  tanksize double default null,
  tankfillfraction double default null,
  primary key (sourcetypeid,modelyearid,fueltypeid,polprocessid)
);
truncate stmytvvcoeffs;

create table if not exists stmytvvequations (
  sourcetypeid smallint not null,
  modelyearid smallint not null,
  fueltypeid smallint not null,
  polprocessid int not null default '0',
  regclassid smallint not null,
  backpurgefactor double default null,
  averagecanistercapacity double default null,
  regclassfractionofsourcetypemodelyearfuel double not null,
  tvvequation varchar(100) not null default '',
  leakequation varchar(100) not null default '',
  leakfraction double default null,
  leakfractionim double default null,
  tanksize double default null,
  tankfillfraction double default null,
  primary key (sourcetypeid,modelyearid,fueltypeid,polprocessid,regclassid,tvvequation,leakequation)
);
truncate stmytvvequations;

##create.tankvaporgencoeffs##;
truncate tankvaporgencoeffs;

##create.year##;
truncate year;

##create.zone##;
truncate zone;

##create.zonemonthhour##;
truncate zonemonthhour;

-- section withregclassid
##create.regclasssourcetypefraction##;
truncate table regclasssourcetypefraction;
-- end section withregclassid

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

cache select * into outfile '##evaptemperatureadjustment##'
from evaptemperatureadjustment
where processid=12;

cache select * into outfile '##evaprvptemperatureadjustment##'
from evaprvptemperatureadjustment
where processid=12
and fueltypeid in (1,5);

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

cache select * into outfile '##pollutantprocessmappedmodelyear##'
from pollutantprocessmappedmodelyear
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

-- section newtvvyear
drop table if exists regclassfractionofstmy##context.year##;

create table regclassfractionofstmy##context.year## (
  sourcetypeid smallint not null,
  modelyearid smallint not null,
  fueltypeid smallint not null,
  regclassid smallint not null,
  regclassfractionofsourcetypemodelyearfuel double not null,
  primary key (sourcetypeid, modelyearid, fueltypeid, regclassid)
);

insert into regclassfractionofstmy##context.year## (sourcetypeid, modelyearid, fueltypeid, regclassid, regclassfractionofsourcetypemodelyearfuel)
select sourcetypeid, modelyearid, fueltypeid, regclassid, sum(stmyfraction) as regclassfractionofsourcetypemodelyearfuel
from samplevehiclepopulation svp
where sourcetypeid in (##macro.csv.all.sourcetypeid##)
and modelyearid in (##macro.csv.all.modelyearid##)
group by sourcetypemodelyearid, fueltypeid, regclassid
having sum(stmyfraction) > 0;

drop table if exists stmytvvequations##context.year##;

create table stmytvvequations##context.year## (
  sourcetypeid smallint not null,
  modelyearid smallint not null,
  fueltypeid smallint not null,
  polprocessid int not null default '0',
  regclassid smallint not null,
  backpurgefactor double default null,
  averagecanistercapacity double default null,
  regclassfractionofsourcetypemodelyearfuel double not null,
  tvvequation varchar(100) not null default '',
  leakequation varchar(100) not null default '',
  leakfraction double default null,
  leakfractionim double default null,
  tanksize double default null,
  tankfillfraction double default null,
  primary key (sourcetypeid,modelyearid,fueltypeid,polprocessid,regclassid,tvvequation,leakequation)
);

insert into stmytvvequations##context.year## (sourcetypeid, modelyearid, fueltypeid, polprocessid, regclassid,
  backpurgefactor, averagecanistercapacity, regclassfractionofsourcetypemodelyearfuel,
  tvvequation, leakequation, leakfraction, leakfractionim, tanksize, tankfillfraction)
select rf.sourcetypeid, rf.modelyearid, rf.fueltypeid,
  c.polprocessid, c.regclassid,
  sum(backpurgefactor*regclassfractionofsourcetypemodelyearfuel) as backpurgefactor,
  sum(averagecanistercapacity*regclassfractionofsourcetypemodelyearfuel) as averagecanistercapacity,
  sum(regclassfractionofsourcetypemodelyearfuel) as regclassfractionofsourcetypemodelyearfuel,
  c.tvvequation,
  c.leakequation,
  sum(leakfraction*regclassfractionofsourcetypemodelyearfuel) as leakfraction,
  sum(leakfractionim*regclassfractionofsourcetypemodelyearfuel) as leakfractionim,
  sum(tanksize*regclassfractionofsourcetypemodelyearfuel) as tanksize,
  sum(tankfillfraction*regclassfractionofsourcetypemodelyearfuel) as tankfillfraction
from cumtvvcoeffs c
inner join pollutantprocessmappedmodelyear ppmy on (
  ppmy.polprocessid = c.polprocessid
  and ppmy.modelyeargroupid = c.modelyeargroupid)
inner join agecategory a on (
  a.agegroupid = c.agegroupid)
inner join regclassfractionofstmy##context.year## rf on (
  rf.modelyearid = ppmy.modelyearid
  and rf.regclassid = c.regclassid)
where ppmy.modelyearid = ##context.year## - a.ageid
and c.polprocessid in (##pollutantprocessids##)
group by rf.sourcetypeid, rf.modelyearid, rf.fueltypeid,
  c.polprocessid, c.regclassid,
  c.tvvequation,
  c.leakequation;

drop table if exists stmytvvcoeffs##context.year##;

create table stmytvvcoeffs##context.year## (
  sourcetypeid smallint not null,
  modelyearid smallint not null,
  fueltypeid smallint not null,
  polprocessid int not null default '0',
  backpurgefactor double default null,
  averagecanistercapacity double default null,
  leakfraction double default null,
  leakfractionim double default null,
  tanksize double default null,
  tankfillfraction double default null,
  primary key (sourcetypeid,modelyearid,fueltypeid,polprocessid)
);

insert into stmytvvcoeffs##context.year## (sourcetypeid, modelyearid, fueltypeid, polprocessid,
  backpurgefactor, averagecanistercapacity, leakfraction, leakfractionim, tanksize, tankfillfraction)
select sourcetypeid, modelyearid, fueltypeid, polprocessid,
  sum(backpurgefactor),
  sum(averagecanistercapacity),
  sum(leakfraction),
  sum(leakfractionim),
  sum(tanksize),
  sum(tankfillfraction)
from stmytvvequations##context.year##
group by sourcetypeid, modelyearid, fueltypeid, polprocessid;
-- end section newtvvyear

cache(yearid=##context.year##) select * into outfile '##stmytvvequations##' from stmytvvequations##context.year##;
cache(yearid=##context.year##) select * into outfile '##stmytvvcoeffs##' from stmytvvcoeffs##context.year##;

-- section firstbundle

-- section fillsamplevehiclesoaking

-- fill samplevehiclesoaking
truncate samplevehiclesoaking;

-- get total vehicle counts
drop table if exists samplevehiclecount;

create table samplevehiclecount (
  dayid smallint not null,
  sourcetypeid smallint not null,
  totalvehicles int not null,
  primary key (dayid, sourcetypeid),
  unique key (sourcetypeid, dayid)
);

-- @algorithm totalvehicles = count(dayid, sourcetypeid) from samplevehicleday.
-- @condition first bundle only.
insert into samplevehiclecount (dayid, sourcetypeid, totalvehicles)
select svd.dayid, sourcetypeid, count(*)
from samplevehicleday svd
group by svd.dayid, sourcetypeid
order by null;

-- count vehicles that never started
drop table if exists samplevehicleneverstarted;

create table samplevehicleneverstarted (
  dayid smallint not null,
  sourcetypeid smallint not null,
  vehiclesneverstarted int not null,
  fractionneverstarted double,
  primary key (dayid, sourcetypeid),
  unique key (sourcetypeid, dayid)
);

-- @algorithm vehiclesneverstarted = count(dayid, sourcetypeid) from samplevehicleday without any corresponding entry in samplevehicletrip.
-- @condition first bundle only.
insert into samplevehicleneverstarted (dayid, sourcetypeid, vehiclesneverstarted)
select svd.dayid, sourcetypeid, count(*)
from samplevehicleday svd
left outer join samplevehicletrip svt using (vehid, dayid)
where svt.vehid is null and svt.dayid is null
group by svd.dayid, sourcetypeid
order by null;

-- @algorithm fractionneverstarted[dayid,sourcetypeid] = vehiclesneverstarted/totalvehicles.
-- @condition first bundle only.
update samplevehicleneverstarted, samplevehiclecount
  set fractionneverstarted=case when totalvehicles <= 0 then 0 else vehiclesneverstarted*1.0/totalvehicles end
where samplevehicleneverstarted.dayid = samplevehiclecount.dayid
and samplevehicleneverstarted.sourcetypeid = samplevehiclecount.sourcetypeid;

-- count vehicles by when they started
drop table if exists samplevehiclefirststart;

create table samplevehiclefirststart (
  vehid int not null,
  dayid smallint not null,
  sourcetypeid smallint not null,
  hourid smallint not null,
  primary key (dayid, sourcetypeid, hourid, vehid),
  unique key (sourcetypeid, dayid, hourid, vehid)
);

-- @algorithm hour of first start[vehid,dayid,sourcetypeid] = min(samplevehicletrip.hourid).
-- @condition first bundle only.
insert into samplevehiclefirststart (vehid, dayid, sourcetypeid, hourid)
select svd.vehid, svd.dayid, svd.sourcetypeid, min(svt.hourid)
from samplevehicleday svd
inner join samplevehicletrip svt using (vehid, dayid)
where svt.keyontime is not null
group by svd.vehid, svd.dayid
order by null;

drop table if exists samplevehiclesoaktemp;

create table samplevehiclesoaktemp (
  dayid smallint not null,
  sourcetypeid smallint not null,
  hourid smallint not null,
  vehiclesfirststarted int not null,
  primary key (dayid, sourcetypeid, hourid),
  unique key (sourcetypeid, dayid, hourid)
);

-- @algorithm in each day and hour, count the number of vehicles that start for the first time.
-- vehiclesfirststarted[dayid,sourcetypeid,hourid] = count(dayid,sourcetypeid,hourid) by hour of first start.
-- @condition first bundle only.
insert into samplevehiclesoaktemp (dayid, sourcetypeid, hourid, vehiclesfirststarted)
select dayid, sourcetypeid, hourid, count(*) as vehiclesfirststarted
from samplevehiclefirststart
group by dayid, sourcetypeid, hourid
order by null;

drop table if exists samplevehiclesoaktemp2;

create table samplevehiclesoaktemp2 (
  dayid smallint not null,
  sourcetypeid smallint not null,
  hourid smallint not null,
  totalvehiclesstarted int not null,
  primary key (dayid, sourcetypeid, hourid),
  unique key (sourcetypeid, dayid, hourid)
);

-- @algorithm in each day and hour, sum the number of vehicles that have started up to then.
-- totalvehiclesstarted[dayid,sourcetypeid,hourid]=sum(vehiclesfirststarted) for all hours up to hourid.
-- @condition first bundle only.
insert into samplevehiclesoaktemp2 (dayid, sourcetypeid, hourid, totalvehiclesstarted)
select s.dayid, s.sourcetypeid, h.hourid, sum(vehiclesfirststarted) as totalvehiclesstarted
from samplevehiclesoaktemp s, hourofanyday h
where s.hourid <= h.hourid
group by s.dayid, s.sourcetypeid, h.hourid
order by null;

truncate samplevehiclesoaking;

-- @algorithm obtain the fraction of vehicles soaking, awaiting their first start, for each day and hour.
-- soakfraction[soakdayid=0,sourcetypeid,dayid,hourid]=(totalvehicles-totalvehiclesstarted)/totalvehicles.
-- @condition first bundle only.
insert into samplevehiclesoaking (soakdayid, sourcetypeid, dayid, hourid, soakfraction)
select 0, st.sourcetypeid, st.dayid, st.hourid, 
  case when totalvehicles <= 0 then 0
  else (totalvehicles - totalvehiclesstarted)*1.0/totalvehicles end as soakfraction
from samplevehiclesoaktemp2 st
inner join samplevehiclecount c using (dayid, sourcetypeid);

-- @algorithm assume a default soakfraction of 1 for any day and hour that has no vehicles started previously.
-- @condition first bundle only.
insert ignore into samplevehiclesoaking (soakdayid, sourcetypeid, dayid, hourid, soakfraction)
select distinct 0, s.sourcetypeid, s.dayid, h.hourid, 1.0 as soakfraction
from samplevehiclesoaktemp s, hourofanyday h;

drop table if exists samplevehiclesoakingf;

create table if not exists samplevehiclesoakingf (
  soakdayid smallint not null,
  sourcetypeid smallint not null,
  dayid smallint not null,
  f double,
  gammafn1 double,
  gammafn double,
  nextf double,
  p1 double,
  p24 double,
  primary key (soakdayid, sourcetypeid, dayid)
);

-- samplevehiclesoaking day 1

-- @algorithm dayf[soakdayid=1,sourcetypeid,dayid]=soakfraction[soakdayid=0,sourcetypeid,dayid,hourid=24]
-- @condition first bundle only.
insert ignore into samplevehiclesoakingday (soakdayid, sourcetypeid, dayid, f)
select 1, sourcetypeid, dayid, soakfraction as f
from samplevehiclesoaking
where soakdayid=0
and hourid=24;

-- @algorithm soakf[soakdayid=1,sourcetypeid,dayid]=dayf[soakdayid=1,sourcetypeid,dayid].
-- gammafn1[soakdayid=1,sourcetypeid,dayid]=dayf[soakdayid=1,sourcetypeid,dayid].
-- gammafn[soakdayid=1,sourcetypeid,dayid]=dayf[soakdayid=1,sourcetypeid,dayid].
-- @condition first bundle only.
insert into samplevehiclesoakingf (soakdayid, sourcetypeid, dayid, f, gammafn1, gammafn)
select soakdayid, sourcetypeid, dayid, f, f as gammafn1, f as gammafn
from samplevehiclesoakingday
where soakdayid=1;

-- samplevehiclesoaking day 2

-- @algorithm dayf[soakdayid=2,sourcetypeid,dayid]=greatest(dayf[soakdayid=1,sourcetypeid,dayid], basisf[soakdayid=2,dayid]).
-- @condition first bundle only.
insert ignore into samplevehiclesoakingday (soakdayid, sourcetypeid, dayid, f)
select 2, d.sourcetypeid, d.dayid, if(b.f<d.f,d.f,b.f)
from samplevehiclesoakingday d
inner join samplevehiclesoakingdaybasis b on (
  b.soakdayid = d.soakdayid+1
  and b.dayid = d.dayid)
where d.soakdayid=2-1;

-- @algorithm soakf[soakdayid=2,sourcetypeid,dayid]=dayf[soakdayid=2,sourcetypeid,dayid].
-- gammafn1[soakdayid=2,sourcetypeid,dayid]=gammafn[soakdayid=1,sourcetypeid,dayid].
-- gammafn[soakdayid=2,sourcetypeid,dayid]=dayf[soakdayid=2,sourcetypeid,dayid] * gammafn[soakdayid=1,sourcetypeid,dayid].
-- @condition first bundle only.
insert into samplevehiclesoakingf (soakdayid, sourcetypeid, dayid, f, gammafn1, gammafn)
select sd.soakdayid, sd.sourcetypeid, sd.dayid, sd.f, sf.gammafn as gammafn1, sd.f*sf.gammafn as gammafn
from samplevehiclesoakingday sd
inner join samplevehiclesoakingf sf on (
  sf.soakdayid=sd.soakdayid-1
  and sf.sourcetypeid=sd.sourcetypeid
  and sf.dayid=sd.dayid)
where sd.soakdayid=2;

-- samplevehiclesoaking day 3

-- @algorithm dayf[soakdayid=3,sourcetypeid,dayid]=greatest(dayf[soakdayid=2,sourcetypeid,dayid], basisf[soakdayid=3,dayid]).
-- @condition first bundle only.
insert ignore into samplevehiclesoakingday (soakdayid, sourcetypeid, dayid, f)
select 3, d.sourcetypeid, d.dayid, if(b.f<d.f,d.f,b.f)
from samplevehiclesoakingday d
inner join samplevehiclesoakingdaybasis b on (
  b.soakdayid = d.soakdayid+1
  and b.dayid = d.dayid)
where d.soakdayid=3-1;

-- @algorithm soakf[soakdayid=3,sourcetypeid,dayid]=dayf[soakdayid=3,sourcetypeid,dayid].
-- gammafn1[soakdayid=3,sourcetypeid,dayid]=gammafn[soakdayid=2,sourcetypeid,dayid].
-- gammafn[soakdayid=3,sourcetypeid,dayid]=dayf[soakdayid=3,sourcetypeid,dayid] * gammafn[soakdayid=2,sourcetypeid,dayid].
-- @condition first bundle only.
insert into samplevehiclesoakingf (soakdayid, sourcetypeid, dayid, f, gammafn1, gammafn)
select sd.soakdayid, sd.sourcetypeid, sd.dayid, sd.f, sf.gammafn as gammafn1, sd.f*sf.gammafn as gammafn
from samplevehiclesoakingday sd
inner join samplevehiclesoakingf sf on (
  sf.soakdayid=sd.soakdayid-1
  and sf.sourcetypeid=sd.sourcetypeid
  and sf.dayid=sd.dayid)
where sd.soakdayid=3;

-- samplevehiclesoaking day 4

-- @algorithm dayf[soakdayid=4,sourcetypeid,dayid]=greatest(dayf[soakdayid=3,sourcetypeid,dayid], basisf[soakdayid=4,dayid]).
-- @condition first bundle only.
insert ignore into samplevehiclesoakingday (soakdayid, sourcetypeid, dayid, f)
select 4, d.sourcetypeid, d.dayid, if(b.f<d.f,d.f,b.f)
from samplevehiclesoakingday d
inner join samplevehiclesoakingdaybasis b on (
  b.soakdayid = d.soakdayid+1
  and b.dayid = d.dayid)
where d.soakdayid=4-1;

-- @algorithm soakf[soakdayid=4,sourcetypeid,dayid]=dayf[soakdayid=4,sourcetypeid,dayid].
-- gammafn1[soakdayid=4,sourcetypeid,dayid]=gammafn[soakdayid=3,sourcetypeid,dayid].
-- gammafn[soakdayid=4,sourcetypeid,dayid]=dayf[soakdayid=4,sourcetypeid,dayid] * gammafn[soakdayid=3,sourcetypeid,dayid].
-- @condition first bundle only.
insert into samplevehiclesoakingf (soakdayid, sourcetypeid, dayid, f, gammafn1, gammafn)
select sd.soakdayid, sd.sourcetypeid, sd.dayid, sd.f, sf.gammafn as gammafn1, sd.f*sf.gammafn as gammafn
from samplevehiclesoakingday sd
inner join samplevehiclesoakingf sf on (
  sf.soakdayid=sd.soakdayid-1
  and sf.sourcetypeid=sd.sourcetypeid
  and sf.dayid=sd.dayid)
where sd.soakdayid=4;

-- samplevehiclesoaking day 5

-- @algorithm dayf[soakdayid=5,sourcetypeid,dayid]=greatest(dayf[soakdayid=4,sourcetypeid,dayid], basisf[soakdayid=5,dayid]).
-- @condition first bundle only.
insert ignore into samplevehiclesoakingday (soakdayid, sourcetypeid, dayid, f)
select 5, d.sourcetypeid, d.dayid, if(b.f<d.f,d.f,b.f)
from samplevehiclesoakingday d
inner join samplevehiclesoakingdaybasis b on (
  b.soakdayid = d.soakdayid+1
  and b.dayid = d.dayid)
where d.soakdayid=5-1;

-- @algorithm soakf[soakdayid=5,sourcetypeid,dayid]=dayf[soakdayid=5,sourcetypeid,dayid].
-- gammafn1[soakdayid=5,sourcetypeid,dayid]=gammafn[soakdayid=4,sourcetypeid,dayid].
-- gammafn[soakdayid=5,sourcetypeid,dayid]=dayf[soakdayid=5,sourcetypeid,dayid] * gammafn[soakdayid=4,sourcetypeid,dayid].
-- @condition first bundle only.
insert into samplevehiclesoakingf (soakdayid, sourcetypeid, dayid, f, gammafn1, gammafn)
select sd.soakdayid, sd.sourcetypeid, sd.dayid, sd.f, sf.gammafn as gammafn1, sd.f*sf.gammafn as gammafn
from samplevehiclesoakingday sd
inner join samplevehiclesoakingf sf on (
  sf.soakdayid=sd.soakdayid-1
  and sf.sourcetypeid=sd.sourcetypeid
  and sf.dayid=sd.dayid)
where sd.soakdayid=5;


drop table if exists samplevehiclesoakingf2;

create table if not exists samplevehiclesoakingf2 (
  soakdayid smallint not null,
  sourcetypeid smallint not null,
  dayid smallint not null,
  f double,
  primary key (soakdayid, sourcetypeid, dayid)
);

insert into samplevehiclesoakingf2 (soakdayid, sourcetypeid, dayid, f)
select soakdayid, sourcetypeid, dayid, f
from samplevehiclesoakingf;

-- @algorithm nextf[soakdayid,sourcetypeid,dayid]=soakf[soakdayid+1,sourcetypeid,dayid].
-- @condition first bundle only.
update samplevehiclesoakingf, samplevehiclesoakingf2 set samplevehiclesoakingf.nextf=samplevehiclesoakingf2.f
where samplevehiclesoakingf2.soakdayid=samplevehiclesoakingf.soakdayid+1
and samplevehiclesoakingf2.sourcetypeid=samplevehiclesoakingf.sourcetypeid
and samplevehiclesoakingf2.dayid=samplevehiclesoakingf.dayid;

drop table if exists samplevehiclesoakingf2;

drop table if exists samplevehiclesoakingproportion;

create table samplevehiclesoakingproportion (
  soakdayid smallint not null,
  sourcetypeid smallint not null,
  dayid smallint not null,
  hourid smallint not null,
  p double,
  primary key (soakdayid, sourcetypeid, dayid, hourid)
);

-- @algorithm p[n,1] = 1 for each soaking day
-- @condition first bundle only.
insert into samplevehiclesoakingproportion (soakdayid, sourcetypeid, dayid, hourid, p)
select soakdayid, sourcetypeid, dayid, 1, 1
from samplevehiclesoakingday;

-- @algorithm d[5,1] = gamma[f4]
-- @condition first bundle only.
insert ignore into samplevehiclesoaking (soakdayid, sourcetypeid, dayid, hourid, soakfraction)
select soakdayid, sourcetypeid, dayid, 1, gammafn1
from samplevehiclesoakingf
where soakdayid=5;

-- @algorithm d[5,24] = gamma[f5], for soakday 5
-- @condition first bundle only.
insert ignore into samplevehiclesoaking (soakdayid, sourcetypeid, dayid, hourid, soakfraction)
select soakdayid, sourcetypeid, dayid, 24, gammafn
from samplevehiclesoakingf
where soakdayid=5;

-- @algorithm d[n,24] = gamma[fn] * (1-f[n+1]), for soakdays 1-4
-- @condition first bundle only.
insert ignore into samplevehiclesoaking (soakdayid, sourcetypeid, dayid, hourid, soakfraction)
select soakdayid, sourcetypeid, dayid, 24, gammafn * (1-nextf)
from samplevehiclesoakingf
where soakdayid in (1,2,3,4);

-- @algorithm d[1,1] = d1-d24 for soakday 1
-- @condition first bundle only.
insert ignore into samplevehiclesoaking (soakdayid, sourcetypeid, dayid, hourid, soakfraction)
select 1 as soakdayid, d1.sourcetypeid, d1.dayid, 1 as hourid, d1.soakfraction-d24.soakfraction
from samplevehiclesoaking as d1
inner join samplevehiclesoaking as d24 using (sourcetypeid, dayid)
where d1.soakdayid=0 and d1.hourid=1
and d24.soakdayid=0 and d24.hourid=24;

-- @algorithm d[n,1] = d[n-1,24] for soakday 2,3,4
-- @condition first bundle only.
insert ignore into samplevehiclesoaking (soakdayid, sourcetypeid, dayid, hourid, soakfraction)
select soakdayid+1, sourcetypeid, dayid, 1 as hourid, soakfraction
from samplevehiclesoaking
where soakdayid in (2-1,3-1,4-1) and hourid=24;

-- @algorithm p[n,24]=d[n,24]/d[n,1]
-- @condition first bundle only.
insert ignore into samplevehiclesoakingproportion (soakdayid, sourcetypeid, dayid, hourid, p)
select d1.soakdayid, d1.sourcetypeid, d1.dayid, 24 as hourid, 
  case when d1.soakfraction <= 0 then 0 
  else d24.soakfraction/d1.soakfraction end
from samplevehiclesoaking as d1
inner join samplevehiclesoaking as d24 using (soakdayid, sourcetypeid, dayid)
where d1.hourid=1
and d24.hourid=24
and d1.soakdayid>0;

drop table if exists samplevehiclesoakingdh;

create table samplevehiclesoakingdh (
  sourcetypeid smallint not null,
  dayid smallint not null,
  hourid smallint not null,
  dfraction double,
  primary key (sourcetypeid, dayid, hourid)
);

-- @algorithm precalc[h] = (d[0,h]-d[0,24]) / (d[0,1]-d[0,24]) because it is used in p[n,1 < h < 24]
-- @condition first bundle only.
insert into samplevehiclesoakingdh (sourcetypeid, dayid, hourid, dfraction)
select d1.sourcetypeid, d1.dayid, d.hourid, 
  case when (d1.soakfraction-d24.soakfraction) <= 0 then 0 
  else (d.soakfraction-d24.soakfraction)/(d1.soakfraction-d24.soakfraction) end
from samplevehiclesoaking as d
inner join samplevehiclesoaking as d1 using (sourcetypeid, dayid)
inner join samplevehiclesoaking as d24 using (sourcetypeid, dayid)
where d.soakdayid=0
and d1.soakdayid=0 and d1.hourid=1
and d24.soakdayid=0 and d24.hourid=24
and d.hourid > 1 and d.hourid < 24;

-- @algorithm store p[n,1] for each day alongside p[n,24] since they are used together
-- @condition first bundle only.
update samplevehiclesoakingf, samplevehiclesoakingproportion set p1=p
where samplevehiclesoakingproportion.soakdayid=samplevehiclesoakingf.soakdayid
and samplevehiclesoakingproportion.dayid=samplevehiclesoakingf.dayid
and samplevehiclesoakingproportion.sourcetypeid=samplevehiclesoakingf.sourcetypeid
and samplevehiclesoakingproportion.hourid=1;

update samplevehiclesoakingf, samplevehiclesoakingproportion set p24=p
where samplevehiclesoakingproportion.soakdayid=samplevehiclesoakingf.soakdayid
and samplevehiclesoakingproportion.dayid=samplevehiclesoakingf.dayid
and samplevehiclesoakingproportion.sourcetypeid=samplevehiclesoakingf.sourcetypeid
and samplevehiclesoakingproportion.hourid=24;

-- @algorithm p[n,h]=precalc[h]*(p[n,1]-p[n,24]) + p[n,24] for 1 <= n <= 5, 1 < h < 24
-- @condition first bundle only.
insert into samplevehiclesoakingproportion (soakdayid, sourcetypeid, dayid, hourid, p)
select p1.soakdayid, p1.sourcetypeid, p1.dayid, dh.hourid, dfraction*(p1.p-p24.p) + p24.p
from samplevehiclesoakingdh dh
inner join samplevehiclesoakingproportion p1 using (sourcetypeid,dayid)
inner join samplevehiclesoakingproportion p24 using (soakdayid,sourcetypeid,dayid)
where p1.hourid=1
and p24.hourid=24;

-- @algorithm d[n,h] = d[n,1]*p[n,h] for 1 <= n <= 5, 1 < h < 24
-- @condition first bundle only.
insert ignore into samplevehiclesoaking (soakdayid, sourcetypeid, dayid, hourid, soakfraction)
select p.soakdayid, p.sourcetypeid, p.dayid, p.hourid, s.soakfraction * p.p
from samplevehiclesoakingproportion p
inner join samplevehiclesoaking s using (soakdayid, sourcetypeid, dayid)
where s.hourid=1;

-- end section fillsamplevehiclesoaking

-- end section firstbundle

cache select * into outfile '##samplevehiclesoaking##' from samplevehiclesoaking;

cache select distinct zonemonthhour.* into outfile '##zonemonthhour##'
from zonemonthhour,runspechour
where zoneid = ##context.iterlocation.zonerecordid##
and zonemonthhour.monthid = ##context.monthid##
and runspechour.hourid = zonemonthhour.hourid;

-- section withregclassid
cache select *
into outfile '##regclasssourcetypefraction##'
from regclasssourcetypefraction
where modelyearid <= ##context.year##
and modelyearid >= ##context.year## - 30;
-- end section withregclassid

-- end section extract data

-- section processing

-- @algorithm index by hourid to speedup later joins.
-- @input coldsoaktanktemperature
alter table coldsoaktanktemperature add key speed1 (hourid);
analyze table coldsoaktanktemperature;

-- create tables needed for processing
-- create table if not exists eventlog (eventrowid integer unsigned not null auto_increment, primary key (eventrowid), eventtime datetime, eventname varchar(120));

-- @algorithm include soakdayid in coldsoakinitialhourfraction.
-- @input coldsoakinitialhourfraction
alter table coldsoakinitialhourfraction add column soakdayid smallint not null default 1;
alter table coldsoakinitialhourfraction drop primary key;
alter table coldsoakinitialhourfraction add primary key (sourcetypeid,zoneid,monthid,hourdayid,initialhourdayid,soakdayid);

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

-- @algorithm disaggregate imcoverage records, expanding model year ranges into individual model years. 
-- imadjustfract[processid,pollutantid,modelyearid,fueltypeid,sourcetypeid]=imfactor*compliancefactor*0.01.
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

-- @algorithm compute peakhourid by monthid. the maximum value of the expression <coldsoaktanktemperature*
-- 100000 + (999-hourid)> is found. this finds the maximum temperature and the earliest time of day
-- in which the temperature occurs, storing both as a single number.
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

-- section debugtankvaporgenerated
drop table if exists debugtankvaporgenerated;

create table debugtankvaporgenerated (
  hourdayid smallint(6) not null,
  initialhourdayid smallint(6) not null,
  ethanollevelid smallint(6) not null,
  monthid smallint(6) not null,
  sourcetypeid smallint(6) not null,
  fueltypeid smallint(6) not null,
  modelyearid smallint not null,
  polprocessid int not null default '0',
  tankvaporgenerated float null,
  backpurgefactor double default null,
  averagecanistercapacity double default null,
  leakfraction double default null,
  leakfractionim double default null,
  primary key (hourdayid, initialhourdayid, ethanollevelid, monthid, sourcetypeid, fueltypeid, modelyearid, polprocessid)
);

insert into debugtankvaporgenerated (hourdayid, initialhourdayid, ethanollevelid,
  monthid, sourcetypeid, fueltypeid, 
  modelyearid, polprocessid,
  tankvaporgenerated,
  backpurgefactor, averagecanistercapacity, leakfraction, leakfractionim)
select ihf.hourdayid, ihf.initialhourdayid, coeffs.ethanollevelid,
  ihf.monthid, ihf.sourcetypeid, avggas.fueltypeid,
  stmycoeffs.modelyearid, stmycoeffs.polprocessid,
  case when t1.coldsoaktanktemperature >= t2.coldsoaktanktemperature then 0.0
    else
    (stmycoeffs.tanksize*(1-stmycoeffs.tankfillfraction))*(tvgterma*exp(tvgtermb*rvp)*(exp(tvgtermc*t2.coldsoaktanktemperature)-exp(tvgtermc*t1.coldsoaktanktemperature)))
  end as tankvaporgenerated,
  stmycoeffs.backpurgefactor, stmycoeffs.averagecanistercapacity, stmycoeffs.leakfraction, stmycoeffs.leakfractionim
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
inner join stmytvvcoeffs stmycoeffs on (
  stmycoeffs.sourcetypeid = ihf.sourcetypeid
  and stmycoeffs.fueltypeid = avggas.fueltypeid
)
where ihf.hourdayid <> ihf.initialhourdayid
  and hd.hourid <= ph.peakhourid
  and ihf.coldsoakinitialhourfraction > 0;

analyze table debugtankvaporgenerated;

-- end section debugtankvaporgenerated

drop table if exists tankvaporgeneratedhighandlow;

create table tankvaporgeneratedhighandlow (
  altitudeflag varchar(1) not null,
  hourdayid smallint(6) not null,
  initialhourdayid smallint(6) not null,
  ethanollevelid smallint(6) not null,
  monthid smallint(6) not null,
  sourcetypeid smallint(6) not null,
  fueltypeid smallint(6) not null,
  modelyearid smallint not null,
  polprocessid int not null default '0',
  tankvaporgenerated float null,
  backpurgefactor double default null,
  averagecanistercapacity double default null,
  leakfraction double default null,
  leakfractionim double default null,
  primary key (altitudeflag, hourdayid, initialhourdayid, ethanollevelid, monthid, sourcetypeid, fueltypeid, modelyearid, polprocessid)
);

-- @algorithm calculate tankvaporgenerated for high and low altitudes.
-- the time spans t1 (based upon coldsoakinitialhourfraction.initialhourdayid) to t2 (based upon coldsoakinitialhourfraction.hourdayid).
-- tankvaporgenerated[high and low altitudes] = (tanksize*(1-tankfillfraction))*(tvgterma*exp(tvgtermb*rvp)*(exp(tvgtermc*t2.coldsoaktanktemperature)-exp(tvgtermc*t1.coldsoaktanktemperature)))
-- @condition t1.coldsoaktanktemperature < t2.coldsoaktanktemperature, 0 otherwise.
insert into tankvaporgeneratedhighandlow (altitudeflag, hourdayid, initialhourdayid, ethanollevelid,
  monthid, sourcetypeid, fueltypeid, 
  modelyearid, polprocessid,
  tankvaporgenerated,
  backpurgefactor, averagecanistercapacity, leakfraction, leakfractionim)
select coeffs.altitude, ihf.hourdayid, ihf.initialhourdayid, coeffs.ethanollevelid,
  ihf.monthid, ihf.sourcetypeid, avggas.fueltypeid,
  stmycoeffs.modelyearid, stmycoeffs.polprocessid,
  case when t1.coldsoaktanktemperature >= t2.coldsoaktanktemperature then 0.0
    else
    (stmycoeffs.tanksize*(1-stmycoeffs.tankfillfraction))*(tvgterma*exp(tvgtermb*rvp)*(exp(tvgtermc*t2.coldsoaktanktemperature)-exp(tvgtermc*t1.coldsoaktanktemperature)))
  end as tankvaporgenerated,
  stmycoeffs.backpurgefactor, stmycoeffs.averagecanistercapacity, stmycoeffs.leakfraction, stmycoeffs.leakfractionim
from tankvaporgencoeffs coeffs, coldsoakinitialhourfraction ihf
inner join hourday hd on (hd.hourdayid = ihf.hourdayid)
inner join hourday ihd on (ihd.hourdayid = ihf.initialhourdayid)
inner join peakhourofcoldsoak ph on (ihf.monthid = ph.monthid)
inner join coldsoaktanktemperature t2 on (ihf.monthid = t2.monthid and hd.hourid = t2.hourid)
inner join coldsoaktanktemperature t1 on (ihf.monthid = t1.monthid and ihd.hourid = t1.hourid)
inner join monthofanyyear m on (ihf.monthid = m.monthid)
inner join averagetankgasoline avggas on (m.monthgroupid = avggas.monthgroupid)
inner join stmytvvcoeffs stmycoeffs on (
  stmycoeffs.sourcetypeid = ihf.sourcetypeid
  and stmycoeffs.fueltypeid = avggas.fueltypeid
)
where (ihf.hourdayid <> ihf.initialhourdayid or (hd.hourid=1 and ihd.hourid=1 and hd.dayid=ihd.dayid))
  and hd.hourid <= ph.peakhourid
  and ihf.coldsoakinitialhourfraction > 0;

analyze table tankvaporgeneratedhighandlow;

drop table if exists tankvaporgenerated;

create table tankvaporgenerated (
  hourdayid smallint(6) not null,
  initialhourdayid smallint(6) not null,
  ethanollevelid smallint(6) not null,
  monthid smallint(6) not null,
  sourcetypeid smallint(6) not null,
  fueltypeid smallint(6) not null,
  modelyearid smallint not null,
  polprocessid int not null default '0',
  tankvaporgenerated float null,
  backpurgefactor double default null,
  averagecanistercapacity double default null,
  leakfraction double default null,
  leakfractionim double default null,
  primary key (hourdayid, initialhourdayid, ethanollevelid, monthid, sourcetypeid, fueltypeid, modelyearid, polprocessid)
);

-- @algorithm interpolate tankvaporgenerated based upon the county's barometric pressure.
-- don't allow negative values.
-- low altitude is based upon wayne county, michigan (26163) with a barometric pressure of 29.069.
-- high altitude is based upon denver county, colorado (8031) with a barometric pressure of 24.087.
-- tankvaporgenerated = greatest(((barometricpressure - 29.069) / (24.087 - 29.069)) * (high.tankvaporgenerated - low.tankvaporgenerated) + low.tankvaporgenerated,0).
insert into tankvaporgenerated (hourdayid, initialhourdayid, ethanollevelid,
  monthid, sourcetypeid, fueltypeid, 
  modelyearid, polprocessid,
  tankvaporgenerated,
  backpurgefactor, averagecanistercapacity, leakfraction, leakfractionim)
select low.hourdayid, low.initialhourdayid, low.ethanollevelid,
  low.monthid, low.sourcetypeid, low.fueltypeid,
  low.modelyearid, low.polprocessid,
  greatest(((c.barometricpressure - 29.069) / (24.087 - 29.069)) * (high.tankvaporgenerated - low.tankvaporgenerated) + low.tankvaporgenerated,0) as tankvaporgenerated,
  low.backpurgefactor, low.averagecanistercapacity, low.leakfraction, low.leakfractionim
from county c, 
tankvaporgeneratedhighandlow low
inner join tankvaporgeneratedhighandlow high using (hourdayid, initialhourdayid, ethanollevelid, monthid, sourcetypeid, fueltypeid, modelyearid, polprocessid)
where low.altitudeflag='L'
and high.altitudeflag='H';


analyze table tankvaporgenerated;

-- 
-- tvv-4: calculate ethanol-weighted tvg
--
-- insert into eventlog (eventtime, eventname) select now(), 'TVV-4';
drop table if exists ethanolweightedtvg;

create table ethanolweightedtvgtemp (
  hourdayid smallint(6) not null,
  hourid smallint(6) not null,
  dayid smallint(6) not null,
  initialhourdayid smallint(6) not null,
  monthid smallint(6) not null,
  sourcetypeid smallint(6) not null,
  fueltypeid smallint(6) not null,
  modelyearid smallint not null,
  polprocessid int not null default '0',
  ethanolweightedtvg float null,
  backpurgefactor double default null,
  averagecanistercapacity double default null,
  leakfraction double default null,
  leakfractionim double default null,
  primary key (hourdayid, initialhourdayid, monthid, sourcetypeid, fueltypeid, modelyearid, polprocessid),
  key (hourid, dayid, initialhourdayid, monthid, sourcetypeid, fueltypeid, modelyearid, polprocessid)
);

-- @algorithm calculated the ethanol weighted tvg using tankvaporgenerated for e10 fuel (t10) and that of e0 fuel (t0).
-- ethanolweightedtvg=(t10.tankvaporgenerated*(least(10.0,etohvolume)/10.0)+t0.tankvaporgenerated*(1.0-least(10.0,etohvolume)/10.0)).
insert into ethanolweightedtvgtemp (hourdayid, hourid, dayid, initialhourdayid, monthid, sourcetypeid, fueltypeid, 
  modelyearid, polprocessid,
  ethanolweightedtvg,
  backpurgefactor, averagecanistercapacity, leakfraction, leakfractionim)
select t0.hourdayid, floor(t0.hourdayid/10) as hourid, mod(t0.hourdayid,10) as dayid,
  t0.initialhourdayid, t0.monthid, t0.sourcetypeid, t0.fueltypeid,
  t0.modelyearid, t0.polprocessid,
  (t10.tankvaporgenerated*(least(10.0,etohvolume)/10.0)+t0.tankvaporgenerated*(1.0-least(10.0,etohvolume)/10.0)) as ethanolweightedtvg,
  t0.backpurgefactor, t0.averagecanistercapacity, t0.leakfraction, t0.leakfractionim
from tankvaporgenerated t0
inner join tankvaporgenerated t10 on (t0.hourdayid=t10.hourdayid and t0.initialhourdayid=t10.initialhourdayid
  and t0.monthid=t10.monthid and t0.sourcetypeid=t10.sourcetypeid
  and t0.fueltypeid=t10.fueltypeid
  and t0.modelyearid=t10.modelyearid
  and t0.polprocessid=t10.polprocessid)
inner join monthofanyyear m on (t10.monthid = m.monthid)
inner join averagetankgasoline avggas on (m.monthgroupid = avggas.monthgroupid
  and t10.fueltypeid=avggas.fueltypeid)
where t0.ethanollevelid = 0
and t10.ethanollevelid = 10;

analyze table ethanolweightedtvgtemp;

create table ethanolweightedtvg (
  hourdayid smallint(6) not null,
  initialhourdayid smallint(6) not null,
  monthid smallint(6) not null,
  sourcetypeid smallint(6) not null,
  fueltypeid smallint(6) not null,
  modelyearid smallint not null,
  polprocessid int not null default '0',
  ethanolweightedtvg float null,
  backpurgefactor double default null,
  averagecanistercapacity double default null,
  leakfraction double default null,
  leakfractionim double default null,
  primary key (hourdayid, initialhourdayid, monthid, sourcetypeid, fueltypeid, modelyearid, polprocessid)
);

-- @algorithm until now, ethanolweightedtvg has been a cummulative total.
-- convert it to an hourly increment by subtracting the total of the previous hour.
-- ethanolweightedtvg[hourid]=ethanolweightedtvg[hourid]-ethanolweightedtvg[hourid-1].
insert into ethanolweightedtvg (hourdayid, initialhourdayid, monthid, sourcetypeid, fueltypeid, 
  modelyearid, polprocessid,
  ethanolweightedtvg,
  backpurgefactor, averagecanistercapacity, leakfraction, leakfractionim)
select h.hourdayid, h.initialhourdayid, h.monthid, h.sourcetypeid, h.fueltypeid, 
  h.modelyearid, h.polprocessid,
  greatest(0,h.ethanolweightedtvg - coalesce(hm1.ethanolweightedtvg,0)) as ethanolweightedtvg,
  h.backpurgefactor, h.averagecanistercapacity, h.leakfraction, h.leakfractionim
from ethanolweightedtvgtemp h
left outer join ethanolweightedtvgtemp hm1 on (
  h.hourid-1 = hm1.hourid
  and h.dayid = hm1.dayid
  and h.initialhourdayid = hm1.initialhourdayid
  and h.monthid = hm1.monthid
  and h.sourcetypeid = hm1.sourcetypeid
  and h.fueltypeid = hm1.fueltypeid
  and h.modelyearid = hm1.modelyearid
  and h.polprocessid = hm1.polprocessid
);

analyze table ethanolweightedtvg;

drop table if exists tvg;

create table tvg (
  soakdayid smallint(6) not null,
  hourdayid smallint(6) not null,
  initialhourdayid smallint(6) not null,
  monthid smallint(6) not null,
  sourcetypeid smallint(6) not null,
  fueltypeid smallint(6) not null,
  modelyearid smallint not null,
  polprocessid int not null default '0',
  tvgdaily float null,
  xn double null,
  backpurgefactor double default null,
  averagecanistercapacity double default null,
  leakfraction double default null,
  leakfractionim double default null,
  tvgsum1h double default null,
  tvgsumh24 double default null,
  primary key (sourcetypeid, modelyearid, fueltypeid, polprocessid, soakdayid, hourdayid, initialhourdayid, monthid)
);

drop table if exists tvgsumih;
drop table if exists tvgsumi24;
drop table if exists tvgsum1h;
drop table if exists tvgsumh24;

-- sum of tvg hourly from initial hour i to hour h
create table tvgsumih (
  hourdayid smallint(6) not null default '0',
  initialhourdayid smallint(6) not null default '0',
  monthid smallint(6) not null default '0',
  sourcetypeid smallint(6) not null default '0',
  fueltypeid smallint(6) not null default '0',
  modelyearid smallint(6) not null default '0',
  polprocessid int not null default '0',
  tvgsum double,
  primary key (hourdayid, initialhourdayid, monthid, sourcetypeid, fueltypeid, modelyearid, polprocessid)
);

-- sum of tvg hourly from initial hour i to the last hour of the day
create table tvgsumi24 like tvgsumih;

-- sum of tvg hourly from the first hour of the day to hour h
create table tvgsum1h like tvgsumih;

-- sum of tvg hourly from after hour h to the last hour of the day
create table tvgsumh24 like tvgsumih;

-- @algorithm tvgsumih = sum of tvg hourly from initial hour i to hour h
insert into tvgsumih (hourdayid, initialhourdayid, monthid, sourcetypeid, fueltypeid, modelyearid, polprocessid, tvgsum)
select e.hourdayid, e.initialhourdayid, e.monthid, e.sourcetypeid, e.fueltypeid, e.modelyearid, e.polprocessid,
  sum(e2.ethanolweightedtvg) as tvgsum
from ethanolweightedtvg as e
inner join hourday as hd on (hd.hourdayid = e.hourdayid)
inner join hourday as ihd on (ihd.hourdayid = e.initialhourdayid)
inner join hourday as ahd on (ahd.dayid = hd.dayid and ahd.hourid >= ihd.hourid and ahd.hourid <= hd.hourid)
inner join ethanolweightedtvg e2 on (e2.hourdayid = ahd.hourdayid and e2.initialhourdayid = e.initialhourdayid
and e2.monthid = e.monthid and e2.sourcetypeid = e.sourcetypeid and e2.fueltypeid = e.fueltypeid and e2.modelyearid = e.modelyearid and e2.polprocessid = e.polprocessid)
where hd.hourid >= ihd.hourid
group by e.hourdayid, e.initialhourdayid, e.monthid, e.sourcetypeid, e.fueltypeid, e.modelyearid, e.polprocessid;

insert into tvgsumih (hourdayid, initialhourdayid, monthid, sourcetypeid, fueltypeid, modelyearid, polprocessid, tvgsum)
select e.hourdayid, e.initialhourdayid, e.monthid, e.sourcetypeid, e.fueltypeid, e.modelyearid, e.polprocessid,
  0 as tvgsum
from ethanolweightedtvg as e
inner join hourday as hd on (hd.hourdayid = e.hourdayid)
inner join hourday as ihd on (ihd.hourdayid = e.initialhourdayid)
where hd.hourid < ihd.hourid
group by e.hourdayid, e.initialhourdayid, e.monthid, e.sourcetypeid, e.fueltypeid, e.modelyearid, e.polprocessid;

-- @algorithm tvgsumi24 = sum of tvg hourly from initial hour i to the last hour of the day
insert into tvgsumi24 (hourdayid, initialhourdayid, monthid, sourcetypeid, fueltypeid, modelyearid, polprocessid, tvgsum)
select e.hourdayid, e.initialhourdayid, e.monthid, e.sourcetypeid, e.fueltypeid, e.modelyearid, e.polprocessid,
  sum(e2.ethanolweightedtvg) as tvgsum
from ethanolweightedtvg as e
inner join hourday as hd on (hd.hourdayid = e.hourdayid)
inner join hourday as ihd on (ihd.hourdayid = e.initialhourdayid)
inner join hourday as ahd on (ahd.dayid = hd.dayid and ahd.hourid >= ihd.hourid and ahd.hourid <= 24)
inner join ethanolweightedtvg e2 on (e2.hourdayid = ahd.hourdayid and e2.initialhourdayid = e.initialhourdayid
and e2.monthid = e.monthid and e2.sourcetypeid = e.sourcetypeid and e2.fueltypeid = e.fueltypeid and e2.modelyearid = e.modelyearid and e2.polprocessid = e.polprocessid)
group by e.hourdayid, e.initialhourdayid, e.monthid, e.sourcetypeid, e.fueltypeid, e.modelyearid, e.polprocessid;

-- @algorithm tvgsum1h = sum of tvg hourly from the first hour of the day to hour h
insert into tvgsum1h (hourdayid, initialhourdayid, monthid, sourcetypeid, fueltypeid, modelyearid, polprocessid, tvgsum)
select e.hourdayid, e.initialhourdayid, e.monthid, e.sourcetypeid, e.fueltypeid, e.modelyearid, e.polprocessid,
  sum(e2.ethanolweightedtvg) as tvgsum
from ethanolweightedtvg as e
inner join hourday as hd on (hd.hourdayid = e.hourdayid)
inner join hourday as ihd on (ihd.hourdayid = e.initialhourdayid)
inner join hourday as ahd on (ahd.dayid = hd.dayid and ahd.hourid >= 1 and ahd.hourid <= hd.hourid)
inner join hourday as hd1 on (hd1.dayid = hd.dayid and hd1.hourid = 1)
inner join ethanolweightedtvg e2 on (e2.hourdayid = ahd.hourdayid and e2.initialhourdayid = hd1.hourdayid
and e2.monthid = e.monthid and e2.sourcetypeid = e.sourcetypeid and e2.fueltypeid = e.fueltypeid and e2.modelyearid = e.modelyearid and e2.polprocessid = e.polprocessid)
group by e.hourdayid, e.initialhourdayid, e.monthid, e.sourcetypeid, e.fueltypeid, e.modelyearid, e.polprocessid;

-- @algorithm tvgsumh24 = sum of tvg hourly from after hour h to the last hour of the day
insert into tvgsumh24 (hourdayid, initialhourdayid, monthid, sourcetypeid, fueltypeid, modelyearid, polprocessid, tvgsum)
select e.hourdayid, e.initialhourdayid, e.monthid, e.sourcetypeid, e.fueltypeid, e.modelyearid, e.polprocessid,
  sum(e2.ethanolweightedtvg) as tvgsum
from ethanolweightedtvg as e
inner join hourday as hd on (hd.hourdayid = e.hourdayid)
inner join hourday as ihd on (ihd.hourdayid = e.initialhourdayid)
inner join hourday as ahd on (ahd.dayid = hd.dayid and ahd.hourid > hd.hourid and ahd.hourid <= 24)
inner join ethanolweightedtvg e2 on (e2.hourdayid = ahd.hourdayid and e2.initialhourdayid = e.initialhourdayid
and e2.monthid = e.monthid and e2.sourcetypeid = e.sourcetypeid and e2.fueltypeid = e.fueltypeid and e2.modelyearid = e.modelyearid and e2.polprocessid = e.polprocessid)
group by e.hourdayid, e.initialhourdayid, e.monthid, e.sourcetypeid, e.fueltypeid, e.modelyearid, e.polprocessid;

-- fill the first soaking day (soakdayid=1)
-- soak day 1, including x0 (tvgdaily)

-- @algorithm tvgdaily[soakdayid=1] = tvgsumih.
-- xn[soakdayid=1] = tvgsumih.
insert into tvg (soakdayid, hourdayid, initialhourdayid, monthid, sourcetypeid, fueltypeid, 
  modelyearid, polprocessid,
  tvgdaily, xn,
  backpurgefactor, averagecanistercapacity, leakfraction, leakfractionim, tvgsum1h, tvgsumh24)
select 1 as soakdayid, e.hourdayid, e.initialhourdayid, e.monthid, e.sourcetypeid, e.fueltypeid, 
  e.modelyearid, e.polprocessid,
  sih.tvgsum as tvgdaily,
  sih.tvgsum as xn,
  backpurgefactor, averagecanistercapacity, leakfraction, leakfractionim,
  s1h.tvgsum as tvgsum1h,
  coalesce(sh24.tvgsum,0) as tvgsumh24
from ethanolweightedtvg e
inner join tvgsum1h s1h on (s1h.hourdayid = e.hourdayid and s1h.initialhourdayid = e.initialhourdayid and s1h.monthid = e.monthid and s1h.sourcetypeid = e.sourcetypeid and s1h.fueltypeid = e.fueltypeid and s1h.modelyearid = e.modelyearid and s1h.polprocessid = e.polprocessid)
inner join tvgsumih sih on (sih.hourdayid = e.hourdayid and sih.initialhourdayid = e.initialhourdayid and sih.monthid = e.monthid and sih.sourcetypeid = e.sourcetypeid and sih.fueltypeid = e.fueltypeid and sih.modelyearid = e.modelyearid and sih.polprocessid = e.polprocessid)
left outer join tvgsumh24 sh24 on (sh24.hourdayid = e.hourdayid and sh24.initialhourdayid = e.initialhourdayid and sh24.monthid = e.monthid and sh24.sourcetypeid = e.sourcetypeid and sh24.fueltypeid = e.fueltypeid and sh24.modelyearid = e.modelyearid and sh24.polprocessid = e.polprocessid);

-- inner join tvgsumh24 sh24 on (sh24.hourdayid = e.hourdayid and sh24.initialhourdayid = e.initialhourdayid and sh24.monthid = e.monthid and sh24.sourcetypeid = e.sourcetypeid and sh24.fueltypeid = e.fueltypeid and sh24.modelyearid = e.modelyearid and sh24.polprocessid = e.polprocessid)

-- soak day 2

-- @algorithm tvgdaily[soakdayid=2] = tvgdaily[soakdayid=1].
-- xn[soakdayid=2] = ((1-backpurgefactor)*least(tvgsumi24,averagecanistercapacity))+tvgsum1h.
insert into tvg (soakdayid, hourdayid, initialhourdayid, monthid, sourcetypeid, fueltypeid, 
  modelyearid, polprocessid,
  tvgdaily, xn,
  backpurgefactor, averagecanistercapacity, leakfraction, leakfractionim, tvgsum1h, tvgsumh24)
select 2 as soakdayid, e.hourdayid, e.initialhourdayid, e.monthid, e.sourcetypeid, e.fueltypeid, 
  e.modelyearid, e.polprocessid,
  tvgdaily as tvgdaily,
  (((1-backpurgefactor)*least(si24.tvgsum,averagecanistercapacity))+tvgsum1h) as xn,
  backpurgefactor, averagecanistercapacity, leakfraction, leakfractionim, tvgsum1h, tvgsumh24
from tvg e
inner join tvgsumi24 si24 on (si24.hourdayid = e.hourdayid and si24.initialhourdayid = e.initialhourdayid and si24.monthid = e.monthid and si24.sourcetypeid = e.sourcetypeid and si24.fueltypeid = e.fueltypeid and si24.modelyearid = e.modelyearid and si24.polprocessid = e.polprocessid)
where soakdayid=2-1;

-- fill all subsequent soak days
loop ##loop.soakdayid##;
select distinct soakdayid from samplevehiclesoaking where soakdayid > 2 order by soakdayid;

-- insert into tvg (soakdayid, hourdayid, initialhourdayid, monthid, sourcetypeid, fueltypeid, 
--  modelyearid, polprocessid,
--  tvgdaily, xn,
--  backpurgefactor, averagecanistercapacity, leakfraction, leakfractionim)
-- select ##loop.soakdayid## as soakdayid, hourdayid, initialhourdayid, monthid, sourcetypeid, fueltypeid, 
--  modelyearid, polprocessid,
--  tvgdaily as tvgdaily,
--  xn,
--  backpurgefactor, averagecanistercapacity, leakfraction, leakfractionim
-- from tvg
-- where soakdayid=##loop.soakdayid##-1

-- insert into tvg (soakdayid, hourdayid, initialhourdayid, monthid, sourcetypeid, fueltypeid, 
--  modelyearid, polprocessid,
--  tvgdaily, xn,
--  backpurgefactor, averagecanistercapacity, leakfraction, leakfractionim)
-- select ##loop.soakdayid## as soakdayid, hourdayid, initialhourdayid, monthid, sourcetypeid, fueltypeid, 
--  modelyearid, polprocessid,
--  tvgdaily as tvgdaily,
--  (((1-backpurgefactor)*least(xn,averagecanistercapacity))+tvgdaily) as xn,
--  backpurgefactor, averagecanistercapacity, leakfraction, leakfractionim
-- from tvg
-- where soakdayid=##loop.soakdayid##-1

-- @algorithm tvgdaily[soakdayid>2] = tvgdaily[soakdayid-1].
-- xn[soakdayid>2] = ((1-backpurgefactor)*least(xn[soakdayid-1] + tvgsumh24,averagecanistercapacity))+tvgsum1h.
insert into tvg (soakdayid, hourdayid, initialhourdayid, monthid, sourcetypeid, fueltypeid, 
  modelyearid, polprocessid,
  tvgdaily, xn,
  backpurgefactor, averagecanistercapacity, leakfraction, leakfractionim, tvgsum1h, tvgsumh24)
select ##loop.soakdayid## as soakdayid, e.hourdayid, e.initialhourdayid, e.monthid, e.sourcetypeid, e.fueltypeid, 
  e.modelyearid, e.polprocessid,
  tvgdaily as tvgdaily,
  (((1-backpurgefactor)*least(xn + tvgsumh24,averagecanistercapacity))+tvgsum1h) as xn,
  backpurgefactor, averagecanistercapacity, leakfraction, leakfractionim, tvgsum1h, tvgsumh24
from tvg e
where soakdayid=##loop.soakdayid##-1;

end loop ##loop.soakdayid##;

-- 
-- tvv-5: calculate cummulative tank vapor vented (tvv)
--
-- insert into eventlog (eventtime, eventname) select now(), 'TVV-5';
drop table if exists cummulativetankvaporvented;

create table cummulativetankvaporvented (
  soakdayid smallint(6) not null,
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
  primary key (soakdayid, regclassid, ageid, polprocessid, dayid, hourid, initialhourdayid, monthid, sourcetypeid, fueltypeid),
  index (priorhourid)
);

-- @algorithm include leaking using database-driven tvv and leak equations.
-- tankvaporvented = (1-leakfraction)*(tvv equation) + leakfraction*(leak equation).
-- tankvaporventedim = (1-leakfractionim)*(tvv equation) + leakfractionim*(leak equation).
insert into cummulativetankvaporvented (soakdayid, regclassid, ageid, polprocessid, dayid, hourid, initialhourdayid, 
  monthid, sourcetypeid, fueltypeid, tankvaporvented, tankvaporventedim,
  hourdayid, priorhourid)
select t.soakdayid, coeffs.regclassid, acat.ageid, coeffs.polprocessid, hd.dayid, hd.hourid, t.initialhourdayid,
  t.monthid, t.sourcetypeid, t.fueltypeid,
  regclassfractionofsourcetypemodelyearfuel*greatest(0.0,
    (1.0-coeffs.leakfraction)*(##tvvequations##)
    + (coeffs.leakfraction)*(##leakequations##)
  ) as tankvaporvented,
  regclassfractionofsourcetypemodelyearfuel*greatest(0.0,
    (1.0-coalesce(coeffs.leakfractionim,coeffs.leakfraction))*(##tvvequations##)
    + coalesce(coeffs.leakfractionim,coeffs.leakfraction)*(##leakequations##)
  ) as tankvaporventedim,
  t.hourdayid,
  mod(hd.hourid-1-1+24,24)+1
from stmytvvequations coeffs
inner join tvg t on (
  t.sourcetypeid=coeffs.sourcetypeid
  and t.modelyearid=coeffs.modelyearid
  and t.fueltypeid=coeffs.fueltypeid
  and t.polprocessid=coeffs.polprocessid)
inner join agecategory acat on (acat.ageid = ##context.year## - coeffs.modelyearid)
inner join hourday hd on (hd.hourdayid = t.hourdayid);

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
  soakdayid smallint(6) not null,
  unweightedhourlytvv float null,
  unweightedhourlytvvim float null,
  index (sourcetypeid, zoneid, monthid, hourdayid, initialhourdayid, fueltypeid, soakdayid),
  index (soakdayid, sourcetypeid, zoneid, monthid, hourdayid, initialhourdayid, fueltypeid)
);

-- @algorithm unweightedhourlytvv = tankvaporvented[hourid] - tankvaporvented[hourid-1].
-- unweightedhourlytvvim = tankvaporventedim[hourid] - tankvaporventedim[hourid-1].
-- @condition only when coldsoaktanktemperature[hourid] > coldsoaktanktemperature[hourid-1], 0 otherwise.
insert into unweightedhourlytvv (soakdayid, regclassid, ageid, polprocessid, hourdayid, initialhourdayid, 
  monthid, sourcetypeid, fueltypeid, unweightedhourlytvv, unweightedhourlytvvim)
select ctv1.soakdayid, ctv1.regclassid, ctv1.ageid, ctv1.polprocessid, ctv1.hourdayid, ctv1.initialhourdayid, 
  ctv1.monthid, ctv1.sourcetypeid, ctv1.fueltypeid,
    case when (ctt1.coldsoaktanktemperature <= ctt2.coldsoaktanktemperature) then
      0.0
    else greatest(0,(ctv1.tankvaporvented-coalesce(ctv2.tankvaporvented,0.0)))
    end
    as unweightedhourlytvv,
    case when (ctt1.coldsoaktanktemperature <= ctt2.coldsoaktanktemperature) then
    0.0
    else greatest(0,(ctv1.tankvaporventedim-coalesce(ctv2.tankvaporventedim,0.0)))
    end
    as unweightedhourlytvvim
from cummulativetankvaporvented ctv1
left join cummulativetankvaporvented ctv2 on (
  ctv1.soakdayid = ctv2.soakdayid
  and ctv1.regclassid = ctv2.regclassid and ctv1.ageid = ctv2.ageid
  and ctv1.polprocessid = ctv2.polprocessid and ctv1.initialhourdayid = ctv2.initialhourdayid 
  and ctv1.monthid = ctv2.monthid and ctv1.sourcetypeid = ctv2.sourcetypeid
  and ctv1.fueltypeid = ctv2.fueltypeid
  and ctv1.priorhourid = ctv2.hourid
  and ctv1.dayid = ctv2.dayid)
left join coldsoaktanktemperature ctt1 on (
    ctt1.hourid = ctv1.hourid and
    ctt1.monthid = ctv1.monthid)
left join coldsoaktanktemperature ctt2 on (
    ctt2.hourid = ctv1.priorhourid and
    ctt2.monthid = ctv1.monthid);

analyze table unweightedhourlytvv;

-- 
-- tvv-7: calculate weighted hourly tvv across initial/current pair
--
-- insert into eventlog (eventtime, eventname) select now(), 'TVV-7';

-- @algorithm add coldsoakinitialhourfraction entries for soaking days beyond the first day
insert into coldsoakinitialhourfraction (soakdayid, sourcetypeid, zoneid, monthid,
  hourdayid,
  initialhourdayid,
  coldsoakinitialhourfraction)
select s.soakdayid, s.sourcetypeid, ##context.iterlocation.zonerecordid## as zoneid, ##context.monthid## as monthid,
  hd.hourdayid,
  ihd.hourdayid as initialhourdayid,
  soakfraction as coldsoakinitialhourfraction
from samplevehiclesoaking s 
inner join hourday hd on (hd.hourid=s.hourid and hd.dayid=s.dayid)
inner join hourday ihd on (ihd.hourid=1 and ihd.dayid=s.dayid)
where s.soakdayid > 1;

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
  soakdayid smallint not null,
  hourlytvv float null,
  hourlytvvim float null,
  index (regclassid, ageid, polprocessid, hourdayid, monthid, sourcetypeid, fueltypeid, soakdayid)
);

-- @algorithm hourlytvv = unweightedhourlytvv * coldsoakinitialhourfraction.
-- hourlytvvim = unweightedhourlytvvim * coldsoakinitialhourfraction.
insert into hourlytvvtemp (soakdayid, regclassid, ageid, polprocessid, hourdayid, monthid, sourcetypeid, fueltypeid,
  hourlytvv, hourlytvvim)
select uhtvv.soakdayid, uhtvv.regclassid, uhtvv.ageid, uhtvv.polprocessid, uhtvv.hourdayid, uhtvv.monthid, uhtvv.sourcetypeid, uhtvv.fueltypeid,
  (unweightedhourlytvv*coldsoakinitialhourfraction) as hourlytvv,
  (unweightedhourlytvvim*coldsoakinitialhourfraction) as hourlytvvim
from unweightedhourlytvv uhtvv
inner join coldsoakinitialhourfraction ihf on (
  uhtvv.soakdayid = ihf.soakdayid
  and uhtvv.sourcetypeid = ihf.sourcetypeid
  and uhtvv.zoneid = ihf.zoneid and uhtvv.monthid = ihf.monthid 
  and uhtvv.hourdayid = ihf.hourdayid and uhtvv.initialhourdayid = ihf.initialhourdayid);

-- @algorithm hourlytvv = sum(hourlytvv).
-- hourlytvvim = sum(hourlytvvim).
insert into hourlytvv (regclassid, ageid, polprocessid, hourdayid, monthid, sourcetypeid, fueltypeid, hourlytvv, hourlytvvim)
select regclassid, ageid, polprocessid, hourdayid, monthid, sourcetypeid, fueltypeid,
  sum(hourlytvv) as hourlytvv,
  sum(hourlytvvim) as hourlytvvim
from hourlytvvtemp
group by regclassid, ageid, polprocessid, hourdayid, monthid, sourcetypeid, fueltypeid
order by null;

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

-- @algorithm reduce tvv emissions for hours past the hour with the peak temperature.
-- hourlytvv(with and without i/m) = hourlytvv(with and without i/m) * 
-- (0 when >= 5 hours past the peak, 0.0005 when >= 4 hours, 0.0040 when >= 3 hours, 0.0100 when >= 2 hours, 
-- 0.0200 when >= 1 hour, 1.0 otherwise).
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
  regclassid smallint(6) not null,
  fueltypeid smallint(6) not null,
  monthid smallint(6) not null,
  hourdayid smallint(6) not null,
  modelyearid smallint(6) not null,
  opmodeid smallint(6) not null,
  weightedmeanbaserate float not null,
  weightedmeanbaserateim float not null,
  tempadjustment float null,
  rvpadjustment float null,
  unadjustedweightedmeanbaserate float null,
  primary key (polprocessid, sourcetypeid, regclassid, fueltypeid, monthid, hourdayid, modelyearid, opmodeid)
);

-- for cold soak mode (opmodeid=151)
-- section withregclassid

-- @algorithm thc rate[opmodeid=151 cold soak]=sum(sourcebinactivityfraction * hourlytvv).
-- thc i/m rate[opmodeid=151 cold soak]=sum(sourcebinactivityfraction * hourlytvvim).
insert into weightedmeanbaserate (polprocessid, sourcetypeid, regclassid, fueltypeid, monthid, hourdayid, 
  modelyearid, opmodeid, weightedmeanbaserate, weightedmeanbaserateim)
select htvv.polprocessid, htvv.sourcetypeid, sb.regclassid, sb.fueltypeid, htvv.monthid, htvv.hourdayid, 
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
group by htvv.polprocessid, htvv.sourcetypeid, sb.regclassid, sb.fueltypeid, htvv.monthid, htvv.hourdayid, 
  stmy.modelyearid
order by null;
-- end section withregclassid

-- section noregclassid
insert into weightedmeanbaserate (polprocessid, sourcetypeid, regclassid, fueltypeid, monthid, hourdayid, 
  modelyearid, opmodeid, weightedmeanbaserate, weightedmeanbaserateim)
select htvv.polprocessid, htvv.sourcetypeid, 0 as regclassid, sb.fueltypeid, htvv.monthid, htvv.hourdayid, 
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
-- end section noregclassid

-- for operating and hot soak modes (opmodeids 300 and 150)
alter table averagetankgasoline 
  add column adjustterm3 double not null default 0,
  add column adjustterm2 double not null default 0,
  add column adjustterm1 double not null default 0,
  add column adjustconstant double not null default 0;

drop table if exists evaprvptemperatureadjustmentsummary;

-- @algorithm find bounds on rvp-based operating and hot soak adjustments (opmodeids 300 and 150).
create table evaprvptemperatureadjustmentsummary
select processid, fueltypeid, min(rvp) as minrvp, max(rvp) as maxrvp
from evaprvptemperatureadjustment
group by processid, fueltypeid;

alter table evaprvptemperatureadjustmentsummary add key (processid, fueltypeid);

insert ignore into evaprvptemperatureadjustment (processid, fueltypeid, rvp, adjustterm3, adjustterm2, adjustterm1, adjustconstant)
select adj.processid, adj.fueltypeid, -1 as rvp, adj.adjustterm3, adj.adjustterm2, adj.adjustterm1, adj.adjustconstant
from evaprvptemperatureadjustment adj
inner join evaprvptemperatureadjustmentsummary sadj on (
  sadj.processid=adj.processid
  and sadj.fueltypeid=adj.fueltypeid
  and sadj.minrvp=adj.rvp);

insert ignore into evaprvptemperatureadjustment (processid, fueltypeid, rvp, adjustterm3, adjustterm2, adjustterm1, adjustconstant)
select adj.processid, adj.fueltypeid, 1000 as rvp, adj.adjustterm3, adj.adjustterm2, adj.adjustterm1, adj.adjustconstant
from evaprvptemperatureadjustment adj
inner join evaprvptemperatureadjustmentsummary sadj on (
  sadj.processid=adj.processid
  and sadj.fueltypeid=adj.fueltypeid
  and sadj.maxrvp=adj.rvp);

drop table if exists tempatg;
create table tempatg like averagetankgasoline;
insert into tempatg select * from averagetankgasoline;
truncate averagetankgasoline;

-- @algorithm linearly interpolate rvp adjustment terms for each entry in averagetankgasoline, where lowadj is the evaprvptemperatureadjustment
-- record such that lowadj.rvp <= averagetankgasoline.rvp and highadj is similar with highadj.rvp > averagetankgasoline.rvp.
-- adjustterm3 = lowadj.adjustterm3 + (highadj.adjustterm3 - lowadj.adjustterm3)/(highadj.rvp - lowadj.rvp) * (atg.rvp - lowadj.rvp)).
-- adjustterm2 = lowadj.adjustterm2 + (highadj.adjustterm2 - lowadj.adjustterm2)/(highadj.rvp - lowadj.rvp) * (atg.rvp - lowadj.rvp).
-- adjustterm1 = lowadj.adjustterm1 + (highadj.adjustterm1 - lowadj.adjustterm1)/(highadj.rvp - lowadj.rvp) * (atg.rvp - lowadj.rvp).
-- adjustconstant = lowadj.adjustconstant + (highadj.adjustconstant - lowadj.adjustconstant)/(highadj.rvp - lowadj.rvp) * (atg.rvp - lowadj.rvp).
insert ignore into averagetankgasoline (zoneid, fuelyearid, monthgroupid, etohvolume, rvp, fueltypeid, isuserinput,
  adjustterm3, adjustterm2, adjustterm1, adjustconstant)
select atg.zoneid, atg.fuelyearid, atg.monthgroupid, atg.etohvolume, atg.rvp, atg.fueltypeid, atg.isuserinput,
  (lowadj.adjustterm3 + (highadj.adjustterm3 - lowadj.adjustterm3)/(highadj.rvp - lowadj.rvp) * (atg.rvp - lowadj.rvp)) as adjustterm3,
  (lowadj.adjustterm2 + (highadj.adjustterm2 - lowadj.adjustterm2)/(highadj.rvp - lowadj.rvp) * (atg.rvp - lowadj.rvp)) as adjustterm2,
  (lowadj.adjustterm1 + (highadj.adjustterm1 - lowadj.adjustterm1)/(highadj.rvp - lowadj.rvp) * (atg.rvp - lowadj.rvp)) as adjustterm1,
  (lowadj.adjustconstant + (highadj.adjustconstant - lowadj.adjustconstant)/(highadj.rvp - lowadj.rvp) * (atg.rvp - lowadj.rvp)) as adjustconstant
from tempatg atg
inner join evaprvptemperatureadjustment lowadj on (lowadj.rvp <= atg.rvp and lowadj.processid=12 and lowadj.fueltypeid=atg.fueltypeid)
inner join evaprvptemperatureadjustment highadj on (highadj.rvp > atg.rvp and highadj.processid=12 and highadj.fueltypeid=atg.fueltypeid)
where lowadj.rvp = (select max(rvp) from evaprvptemperatureadjustment lowadj2 where lowadj2.rvp <= atg.rvp and lowadj2.processid=12 and lowadj2.fueltypeid=atg.fueltypeid)
and highadj.rvp = (select min(rvp) from evaprvptemperatureadjustment highadj2 where highadj2.rvp > atg.rvp and highadj2.processid=12 and highadj2.fueltypeid=atg.fueltypeid);

-- section withregclassid

-- @algorithm adjust hot soak (opmodeid 150) and running (opmodeid 300) by temperature and rvp effects.
-- tempadjustment[opmodeid=300]=
-- (tempadjustterm3*power(greatest(temperature,40.0),3)
-- + tempadjustterm2*power(greatest(temperature,40.0),2)
-- + tempadjustterm1*greatest(temperature,40.0)
-- + tempadjustconstant).
-- tempadjustment[opmodeid=150]=1.
-- rvpadjustment[opmodeid=300]=1 for temperature < 40f, otherwise for temperature >= 40f:
-- (adjustterm3*power(temperature,3)
-- + adjustterm2*power(temperature,2)
-- + adjustterm1*temperature
-- + adjustconstant).
-- rvpadjustment[opmodeid=150]=1.
-- weightedmeanbaserate=sum(sourcebinactivityfraction*meanbaserate)*rvpadjustment*tempadjustment.
-- weightedmeanbaserateim=sum(sourcebinactivityfraction*meanbaserateim)*rvpadjustment*tempadjustment.
-- @condition only gasoline (1) and ethanol (5) fuel types, all others have no adjustments in this step.
insert into weightedmeanbaserate (polprocessid, sourcetypeid, regclassid, fueltypeid, monthid, hourdayid, 
  modelyearid, opmodeid, weightedmeanbaserate, weightedmeanbaserateim,
  tempadjustment, rvpadjustment, unadjustedweightedmeanbaserate)
select er.polprocessid, stmy.sourcetypeid, sb.regclassid, sb.fueltypeid, zmh.monthid, rshd.hourdayid, 
  stmy.modelyearid, er.opmodeid,
  sum(sourcebinactivityfraction*meanbaserate)
    * case when (er.opmodeid=300 and sb.fueltypeid in(1,5)) then
      (eta.tempadjustterm3*power(greatest(zmh.temperature,40.0),3)
      + eta.tempadjustterm2*power(greatest(zmh.temperature,40.0),2)
      + eta.tempadjustterm1*greatest(zmh.temperature,40.0)
      + eta.tempadjustconstant)
      * case when zmh.temperature >= 40.0 then
        (atg.adjustterm3*power(zmh.temperature,3)
        + atg.adjustterm2*power(zmh.temperature,2)
        + atg.adjustterm1*zmh.temperature
        + atg.adjustconstant)
      else 1.0
      end
    else 1.0
    end 
    as weightedmeanbaserate,
  sum(sourcebinactivityfraction*meanbaserateim)
    * case when (er.opmodeid=300 and sb.fueltypeid in (1,5)) then
      (eta.tempadjustterm3*power(greatest(zmh.temperature,40.0),3)
      + eta.tempadjustterm2*power(greatest(zmh.temperature,40.0),2)
      + eta.tempadjustterm1*greatest(zmh.temperature,40.0)
      + eta.tempadjustconstant)
      * case when zmh.temperature >= 40.0 then
        (atg.adjustterm3*power(zmh.temperature,3)
        + atg.adjustterm2*power(zmh.temperature,2)
        + atg.adjustterm1*zmh.temperature
        + atg.adjustconstant)
      else 1.0
      end
    else 1.0
    end 
    as weightedmeanbaserateim,
  case when (er.opmodeid=300 and sb.fueltypeid in (1,5)) then
      (eta.tempadjustterm3*power(greatest(zmh.temperature,40.0),3)
      + eta.tempadjustterm2*power(greatest(zmh.temperature,40.0),2)
      + eta.tempadjustterm1*greatest(zmh.temperature,40.0)
      + eta.tempadjustconstant)
    else 1.0
    end as tempadjustment,
  case when (er.opmodeid=300 and sb.fueltypeid in (1,5)) then
      case when zmh.temperature >= 40.0 then
        (atg.adjustterm3*power(zmh.temperature,3)
        + atg.adjustterm2*power(zmh.temperature,2)
        + atg.adjustterm1*zmh.temperature
        + atg.adjustconstant)
      else 1.0
      end
    else 1.0
    end as rvpadjustment,
  sum(sourcebinactivityfraction*meanbaserate) as unadjustedweightedmeanbaserate
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
inner join runspechourday rshd
inner join hourday hd on (hd.hourdayid = rshd.hourdayid)
inner join zonemonthhour zmh on (zmh.hourid = hd.hourid)
inner join averagetankgasoline atg on (atg.fueltypeid = fueltype.fueltypeid)
inner join evaptemperatureadjustment eta
where er.polprocessid in (##pollutantprocessids##)
and opmodeid in (150, 300)
group by er.polprocessid, stmy.sourcetypeid, sb.regclassid, sb.fueltypeid, zmh.monthid, rshd.hourdayid, 
  stmy.modelyearid, er.opmodeid
order by null;
-- end section withregclassid

-- section noregclassid
insert into weightedmeanbaserate (polprocessid, sourcetypeid, regclassid, fueltypeid, monthid, hourdayid, 
  modelyearid, opmodeid, weightedmeanbaserate, weightedmeanbaserateim,
  tempadjustment, rvpadjustment, unadjustedweightedmeanbaserate)
select er.polprocessid, stmy.sourcetypeid, 0 as regclassid, sb.fueltypeid, zmh.monthid, rshd.hourdayid, 
  stmy.modelyearid, er.opmodeid,
  sum(sourcebinactivityfraction*meanbaserate)
    * case when (er.opmodeid=300 and sb.fueltypeid in(1,5)) then
      (eta.tempadjustterm3*power(greatest(zmh.temperature,40.0),3)
      + eta.tempadjustterm2*power(greatest(zmh.temperature,40.0),2)
      + eta.tempadjustterm1*greatest(zmh.temperature,40.0)
      + eta.tempadjustconstant)
      * case when zmh.temperature >= 40.0 then
        (atg.adjustterm3*power(zmh.temperature,3)
        + atg.adjustterm2*power(zmh.temperature,2)
        + atg.adjustterm1*zmh.temperature
        + atg.adjustconstant)
      else 1.0
      end
    else 1.0
    end 
    as weightedmeanbaserate,
  sum(sourcebinactivityfraction*meanbaserateim)
    * case when (er.opmodeid=300 and sb.fueltypeid in (1,5)) then
      (eta.tempadjustterm3*power(greatest(zmh.temperature,40.0),3)
      + eta.tempadjustterm2*power(greatest(zmh.temperature,40.0),2)
      + eta.tempadjustterm1*greatest(zmh.temperature,40.0)
      + eta.tempadjustconstant)
      * case when zmh.temperature >= 40.0 then
        (atg.adjustterm3*power(zmh.temperature,3)
        + atg.adjustterm2*power(zmh.temperature,2)
        + atg.adjustterm1*zmh.temperature
        + atg.adjustconstant)
      else 1.0
      end
    else 1.0
    end 
    as weightedmeanbaserateim,
  case when (er.opmodeid=300 and sb.fueltypeid in (1,5)) then
      (eta.tempadjustterm3*power(greatest(zmh.temperature,40.0),3)
      + eta.tempadjustterm2*power(greatest(zmh.temperature,40.0),2)
      + eta.tempadjustterm1*greatest(zmh.temperature,40.0)
      + eta.tempadjustconstant)
    else 1.0
    end as tempadjustment,
  case when (er.opmodeid=300 and sb.fueltypeid in (1,5)) then
      case when zmh.temperature >= 40.0 then
        (atg.adjustterm3*power(zmh.temperature,3)
        + atg.adjustterm2*power(zmh.temperature,2)
        + atg.adjustterm1*zmh.temperature
        + atg.adjustconstant)
      else 1.0
      end
    else 1.0
    end as rvpadjustment,
  sum(sourcebinactivityfraction*meanbaserate) as unadjustedweightedmeanbaserate
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
inner join runspechourday rshd
inner join hourday hd on (hd.hourdayid = rshd.hourdayid)
inner join zonemonthhour zmh on (zmh.hourid = hd.hourid)
inner join averagetankgasoline atg on (atg.fueltypeid = fueltype.fueltypeid)
inner join evaptemperatureadjustment eta
where er.polprocessid in (##pollutantprocessids##)
and opmodeid in (150, 300)
group by er.polprocessid, stmy.sourcetypeid, sb.fueltypeid, zmh.monthid, rshd.hourdayid, 
  stmy.modelyearid, er.opmodeid
order by null;
-- end section noregclassid

analyze table weightedmeanbaserate;

alter table movesworkeroutput add emissionquantim float null;

-- section debug
-- keep debug information by operating mode
drop table if exists debugtvvmovesworkeroutput;

create table debugtvvmovesworkeroutput like movesworkeroutput;
alter table debugtvvmovesworkeroutput add column opmodeid smallint(6) null;

insert into debugtvvmovesworkeroutput (yearid, monthid, dayid, hourid, stateid, countyid,
  zoneid, linkid, pollutantid, processid, sourcetypeid, regclassid, fueltypeid, modelyearid,
  roadtypeid, scc, emissionquant, emissionquantim, opmodeid)
select ##context.year## as yearid, w.monthid, hd.dayid, hd.hourid,
  ##context.iterlocation.staterecordid## as stateid,
  ##context.iterlocation.countyrecordid## as countyid,
  ##context.iterlocation.zonerecordid## as zoneid,
  ##context.iterlocation.linkrecordid## as linkid,
  ppa.pollutantid, ppa.processid, w.sourcetypeid, w.regclassid, w.fueltypeid, w.modelyearid,
  ##context.iterlocation.roadtyperecordid##, null as scc,
  (weightedmeanbaserate*sourcehours*opmodefraction) as emissionquant,
  (weightedmeanbaserateim*sourcehours*opmodefraction) as emissionquantim,
  omd.opmodeid
from weightedmeanbaserate w
inner join sourcehours sh on (sh.hourdayid=w.hourdayid and sh.monthid=w.monthid
  and sh.ageid=##context.year##-w.modelyearid
  and sh.sourcetypeid=w.sourcetypeid)
inner join opmodedistribution omd on (omd.sourcetypeid=sh.sourcetypeid
  and omd.hourdayid=w.hourdayid
  and omd.polprocessid=w.polprocessid and omd.opmodeid=w.opmodeid)
inner join pollutantprocessassoc ppa on (ppa.polprocessid=omd.polprocessid)
inner join hourday hd on (hd.hourdayid=omd.hourdayid);
-- end section debug

alter table weightedmeanbaserate add key speed1 (sourcetypeid, hourdayid, polprocessid, opmodeid);
analyze table weightedmeanbaserate;

alter table sourcehours add key speed1 (hourdayid, monthid, sourcetypeid, ageid);
analyze table sourcehours;

-- 
-- tvv-9: calculate movesworkeroutput by source type
--
-- insert into eventlog (eventtime, eventname) select now(), 'TVV-9 WITHOUT SCC';

-- @algorithm combine cold soaking (opmodeid=151), hot soaking (150), and running (300) evaporative emissions.
-- thc=weightedmeanbaserate*sourcehours*opmodefraction.
-- thc i/m=weightedmeanbaserateim*sourcehours*opmodefraction.
insert into movesworkeroutput (yearid, monthid, dayid, hourid, stateid, countyid,
  zoneid, linkid, pollutantid, processid, sourcetypeid, regclassid, fueltypeid, modelyearid,
  roadtypeid, scc, emissionquant, emissionquantim)
select ##context.year## as yearid, w.monthid, hd.dayid, hd.hourid,
  ##context.iterlocation.staterecordid## as stateid,
  ##context.iterlocation.countyrecordid## as countyid,
  ##context.iterlocation.zonerecordid## as zoneid,
  ##context.iterlocation.linkrecordid## as linkid,
  ppa.pollutantid, ppa.processid, w.sourcetypeid, w.regclassid, w.fueltypeid, w.modelyearid,
  ##context.iterlocation.roadtyperecordid##, null as scc,
  (weightedmeanbaserate*sourcehours*opmodefraction) as emissionquant,
  (weightedmeanbaserateim*sourcehours*opmodefraction) as emissionquantim
from weightedmeanbaserate w
inner join sourcehours sh on (sh.hourdayid=w.hourdayid and sh.monthid=w.monthid
  and sh.ageid=##context.year##-w.modelyearid
  and sh.sourcetypeid=w.sourcetypeid)
inner join opmodedistribution omd on (omd.sourcetypeid=sh.sourcetypeid
  and omd.hourdayid=w.hourdayid
  and omd.polprocessid=w.polprocessid and omd.opmodeid=w.opmodeid)
inner join pollutantprocessassoc ppa on (ppa.polprocessid=omd.polprocessid)
inner join hourday hd on (hd.hourdayid=omd.hourdayid);

-- @algorithm apply i/m programs to the aggregat combination of cold soak, hot soaking, and running evap.
-- thc=thc i/m*imadjustfract + thc*(1-imadjustfract).
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
drop table if exists debugtankvaporgenerated;
drop table if exists tankvaporgeneratedhighandlow;
drop table if exists tankvaporgenerated;
drop table if exists ethanolweightedtvg;
drop table if exists cummulativetankvaporvented;
drop table if exists unweightedhourlytvv;
drop table if exists hourlytvv;
drop table if exists weightedmeanbaserate;
drop table if exists imcoveragemergedungrouped;
drop table if exists copyofhourlytvv;
drop table if exists tvg;
drop table if exists evaprvptemperatureadjustmentsummary;
drop table if exists tempatg;
drop table if exists hourlytvvtemp;

drop table if exists tvgsumih;
drop table if exists tvgsumi24;
drop table if exists tvgsum1h;
drop table if exists tvgsumh24;

drop table if exists debugtvvmovesworkeroutput;
-- end section cleanup
