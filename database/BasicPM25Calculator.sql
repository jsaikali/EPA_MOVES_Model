-- version 2013-09-15
-- wesley faler
-- ed campbell
-- gwo shyu/epa 2008-07-025 - added pm2.5 fuel adjustment
-- supported special section names:
-- 		hasmanyopmodes
-- 		hasoneopmode
-- 		emissionratebyagerates
-- 		emissionraterates
-- 		sourcehoursoperatingactivity
-- 		sourcehoursactivity
--		startsactivity
-- 		applytemperatureadjustment
--		applylineartemperatureadjustment
-- 		notemperatureadjustment

-- @notused

-- section create remote tables for extracted data
##create.agecategory##;
truncate table agecategory;

##create.county##;
truncate table county;

##create.generalfuelratio##;
truncate table generalfuelratio;

##create.hourday##;
truncate table hourday;

##create.link##;
truncate table link;

##create.monthofanyyear##;
truncate monthofanyyear;

##create.runspecsourcetype##;
truncate runspecsourcetype;

##create.zone##;
truncate table zone;

##create.pollutant##;
truncate table pollutant;

##create.emissionprocess##;
truncate table emissionprocess;

-- section emissionraterates
##create.emissionrate##;
truncate table emissionrate;
-- end section emissionraterates

-- section emissionratebyagerates
##create.emissionratebyage##;
truncate table emissionratebyage;
-- end section emissionratebyagerates

##create.year##;
truncate table year;

##create.fuelformulation##;
truncate fuelformulation;

##create.fuelsubtype##;
truncate fuelsubtype;

##create.fuelsupply##;
truncate fuelsupply;

##create.fueltype##;
truncate fueltype;

-- section hasmanyopmodes
##create.opmodedistribution##;
truncate table opmodedistribution;
-- end section hasmanyopmodes

##create.sourcebin##;
truncate table sourcebin;

##create.sourcebindistribution##;
truncate table sourcebindistribution;

##create.sourcetypemodelyear##;
truncate table sourcetypemodelyear;

##create.pollutantprocessassoc##;
truncate table pollutantprocessassoc;

##create.pollutantprocessmodelyear##;
truncate table pollutantprocessmodelyear;

-- section sourcehoursoperatingactivity
##create.sho##;
truncate table sho;
-- end section sourcehoursoperatingactivity

-- section sourcehoursactivity
##create.sourcehours##;
truncate table sourcehours;
-- end section sourcehoursactivity

-- section startsactivity
##create.starts##;
truncate table starts;
-- end section startsactivity

-- section applytemperatureadjustment
##create.temperatureadjustment##;
truncate table temperatureadjustment;

##create.zonemonthhour##;
truncate table zonemonthhour;
-- end section applytemperatureadjustment

-- section applylineartemperatureadjustment
##create.temperatureadjustment##;
truncate table temperatureadjustment;

##create.zonemonthhour##;
truncate table zonemonthhour;
-- end section applylineartemperatureadjustment

-- end section create remote tables for extracted data

-- section extract data
cache select * into outfile '##agecategory##'
from agecategory;

cache select * into outfile '##county##'
from county
where countyid = ##context.iterlocation.countyrecordid##;

drop table if exists tempfuelformulation;
create table if not exists tempfuelformulation (
	fuelformulationid int not null primary key
);
insert into tempfuelformulation (fuelformulationid)
select distinct fuelformulationid
from fuelsupply
inner join runspecmonthgroup on (fuelsupply.monthgroupid = runspecmonthgroup.monthgroupid)
inner join year on (fuelsupply.fuelyearid = year.fuelyearid)
where fuelregionid = ##context.fuelregionid##
and yearid = ##context.year##;

select gfr.* into outfile '##generalfuelratio##'
from generalfuelratio gfr
inner join tempfuelformulation tff on tff.fuelformulationid = gfr.fuelformulationid
where polprocessid in (##pollutantprocessids##)
and minmodelyearid <= ##context.year##;

drop table tempfuelformulation;

cache select * into outfile '##runspecsourcetype##'
from runspecsourcetype;

cache select * into outfile '##zone##'
from zone
where zoneid = ##context.iterlocation.zonerecordid##;

cache select link.*
into outfile '##link##'
from link
where linkid = ##context.iterlocation.linkrecordid##;

cache select monthofanyyear.*
into outfile '##monthofanyyear##'
from monthofanyyear;

cache select * 
into outfile '##emissionprocess##'
from emissionprocess
where processid=##context.iterprocess.databasekey##;

-- section hasmanyopmodes
cache select * 
into outfile '##opmodedistribution##'
from opmodedistribution, runspecsourcetype, runspechourday
where polprocessid in (##pollutantprocessids##)
and linkid = ##context.iterlocation.linkrecordid##
and runspecsourcetype.sourcetypeid = opmodedistribution.sourcetypeid
and runspechourday.hourdayid = opmodedistribution.hourdayid;
-- end section hasmanyopmodes

cache select *
into outfile '##pollutant##'
from pollutant;

cache select distinct sourcebindistribution.* 
into outfile '##sourcebindistribution##'
from sourcebindistributionfuelusage_##context.iterprocess.databasekey##_##context.iterlocation.countyrecordid##_##context.year## as sourcebindistribution, 
sourcetypemodelyear, sourcebin, runspecsourcefueltype
where polprocessid in (##pollutantprocessids##)
and sourcebindistribution.sourcetypemodelyearid = sourcetypemodelyear.sourcetypemodelyearid
and sourcetypemodelyear.modelyearid <= ##context.year##
and sourcetypemodelyear.modelyearid >= ##context.year## - 30
and sourcetypemodelyear.sourcetypeid = runspecsourcefueltype.sourcetypeid
and sourcebindistribution.sourcebinid = sourcebin.sourcebinid
and sourcebin.fueltypeid = runspecsourcefueltype.fueltypeid;

cache select distinct sourcebin.* 
into outfile '##sourcebin##'
from sourcebindistribution, sourcetypemodelyear, sourcebin, runspecsourcefueltype
where polprocessid in (##pollutantprocessids##)
and sourcebindistribution.sourcetypemodelyearid = sourcetypemodelyear.sourcetypemodelyearid
and sourcetypemodelyear.modelyearid <= ##context.year##
and sourcetypemodelyear.modelyearid >= ##context.year## - 30
and sourcetypemodelyear.sourcetypeid = runspecsourcefueltype.sourcetypeid
and sourcebindistribution.sourcebinid = sourcebin.sourcebinid
and sourcebin.fueltypeid = runspecsourcefueltype.fueltypeid;

-- section emissionraterates
cache select distinct emissionrate.* 
into outfile '##emissionrate##'
from emissionrate, sourcebindistribution, sourcetypemodelyear, sourcebin, runspecsourcefueltype
where emissionrate.polprocessid in (##pollutantprocessids##)
and emissionrate.polprocessid = sourcebindistribution.polprocessid
and emissionrate.sourcebinid = sourcebindistribution.sourcebinid
and sourcebindistribution.sourcetypemodelyearid = sourcetypemodelyear.sourcetypemodelyearid
and sourcetypemodelyear.modelyearid <= ##context.year##
and sourcetypemodelyear.modelyearid >= ##context.year## - 30
and sourcetypemodelyear.sourcetypeid = runspecsourcefueltype.sourcetypeid
and sourcebindistribution.sourcebinid = sourcebin.sourcebinid
and sourcebin.fueltypeid = runspecsourcefueltype.fueltypeid;
-- end section emissionraterates

-- section emissionratebyagerates
cache select distinct emissionratebyage.* into outfile '##emissionratebyage##'
from emissionratebyage, sourcebindistribution, sourcetypemodelyear, sourcebin, runspecsourcefueltype
where emissionratebyage.polprocessid in (##pollutantprocessids##)
and emissionratebyage.polprocessid = sourcebindistribution.polprocessid
and emissionratebyage.sourcebinid = sourcebindistribution.sourcebinid
and sourcebindistribution.sourcetypemodelyearid = sourcetypemodelyear.sourcetypemodelyearid
and sourcetypemodelyear.modelyearid <= ##context.year##
and sourcetypemodelyear.modelyearid >= ##context.year## - 30
and sourcetypemodelyear.sourcetypeid = runspecsourcefueltype.sourcetypeid
and sourcebindistribution.sourcebinid = sourcebin.sourcebinid
and sourcebin.fueltypeid = runspecsourcefueltype.fueltypeid;
-- end section emissionratebyagerates

cache select * into outfile '##year##'
from year
where yearid = ##context.year##;

cache select ff.* into outfile '##fuelformulation##'
from fuelformulation ff
inner join fuelsupply fs on fs.fuelformulationid = ff.fuelformulationid
inner join year y on y.fuelyearid = fs.fuelyearid
inner join runspecmonthgroup rsmg on rsmg.monthgroupid = fs.monthgroupid
where fuelregionid = ##context.fuelregionid## and
yearid = ##context.year##
group by ff.fuelformulationid order by null;

cache select * into outfile '##fuelsubtype##'
from fuelsubtype;

cache select fuelsupply.* into outfile '##fuelsupply##'
from fuelsupply
inner join runspecmonthgroup on (fuelsupply.monthgroupid = runspecmonthgroup.monthgroupid)
inner join year on (fuelsupply.fuelyearid = year.fuelyearid)
where fuelregionid = ##context.fuelregionid##
and yearid = ##context.year##;

cache select distinct fueltype.* into outfile '##fueltype##'
from fueltype
inner join runspecsourcefueltype on (runspecsourcefueltype.fueltypeid = fueltype.fueltypeid);

cache select sourcetypemodelyear.* 
into outfile '##sourcetypemodelyear##'
from sourcetypemodelyear,runspecsourcetype
where sourcetypemodelyear.sourcetypeid = runspecsourcetype.sourcetypeid
and modelyearid <= ##context.year##
and modelyearid >= ##context.year## - 30;

cache select distinct hourday.* 
into outfile '##hourday##'
from hourday,runspechour,runspecday
where hourday.dayid = runspecday.dayid
and hourday.hourid = runspechour.hourid;

cache select * 
into outfile '##pollutantprocessassoc##'
from pollutantprocessassoc
where polprocessid in (##pollutantprocessids##);

cache select * 
into outfile '##pollutantprocessmodelyear##'
from pollutantprocessmodelyear
where polprocessid in (##pollutantprocessids##)
and modelyearid <= ##context.year##
and modelyearid >= ##context.year## - 30;

-- section sourcehoursoperatingactivity
select sho.* 
into outfile '##sho##'
from sho
inner join runspecmonth using (monthid)
where yearid = ##context.year##
and linkid = ##context.iterlocation.linkrecordid##;
-- end section sourcehoursoperatingactivity

-- section sourcehoursactivity
select sourcehours.* 
into outfile '##sourcehours##'
from sourcehours
inner join runspecmonth using (monthid)
where yearid = ##context.year##
and linkid = ##context.iterlocation.linkrecordid##;
-- end section sourcehoursactivity

-- section startsactivity
select starts.* into outfile '##starts##'
from starts
where yearid = ##context.year##
and zoneid = ##context.iterlocation.zonerecordid##;
-- end section startsactivity

-- section applytemperatureadjustment
cache select distinct temperatureadjustment.*
into outfile '##temperatureadjustment##'
from temperatureadjustment
inner join runspecsourcefueltype using (fueltypeid)
where polprocessid in (##pollutantprocessids##);

cache select zonemonthhour.*
into outfile '##zonemonthhour##'
from runspecmonth
inner join runspechour
inner join zonemonthhour on (zonemonthhour.monthid = runspecmonth.monthid and zonemonthhour.hourid = runspechour.hourid)
where zoneid = ##context.iterlocation.zonerecordid##;
-- end section applytemperatureadjustment

-- section applylineartemperatureadjustment
cache select distinct temperatureadjustment.*
into outfile '##temperatureadjustment##'
from temperatureadjustment
inner join runspecsourcefueltype using (fueltypeid)
where polprocessid in (##pollutantprocessids##);

cache select zonemonthhour.*
into outfile '##zonemonthhour##'
from runspecmonth
inner join runspechour
inner join zonemonthhour on (zonemonthhour.monthid = runspecmonth.monthid and zonemonthhour.hourid = runspechour.hourid)
where zoneid = ##context.iterlocation.zonerecordid##;
-- end section applylineartemperatureadjustment

-- end section extract data

-- section local data removal
--truncate xxxxxx;
-- end section local data removal

-- section processing

-- -----------------------------------------------------
-- brpmc step 1: weight emission rates by operating mode
-- -----------------------------------------------------

drop table if exists opmodeweightedemissionratetemp;
create table opmodeweightedemissionratetemp (
	hourdayid smallint(6) not null,
	sourcetypeid smallint(6) not null,
	sourcebinid bigint(20) not null,
	agegroupid smallint(6) not null,
	polprocessid int not null,
	opmodeweightedmeanbaserate float
);


-- section emissionratebyagerates
-- section hasoneopmode
insert into opmodeweightedemissionratetemp (hourdayid, sourcetypeid, sourcebinid, agegroupid, polprocessid, opmodeweightedmeanbaserate)
select distinct hd.hourdayid, stmy.sourcetypeid, er.sourcebinid, er.agegroupid, sbd.polprocessid,
meanbaserate as opmodeweightedmeanbaserate
from hourday hd
inner join emissionratebyage er
inner join sourcebindistribution sbd on (sbd.polprocessid=er.polprocessid
and sbd.sourcebinid=er.sourcebinid)
inner join agecategory acat on (acat.agegroupid=er.agegroupid)
inner join sourcetypemodelyear stmy on (stmy.sourcetypemodelyearid=sbd.sourcetypemodelyearid
and stmy.modelyearid=##context.year##-ageid);
-- end section hasoneopmode

-- section hasmanyopmodes
insert into opmodeweightedemissionratetemp (hourdayid, sourcetypeid, sourcebinid, agegroupid, polprocessid, opmodeweightedmeanbaserate)
select distinct omd.hourdayid, omd.sourcetypeid, er.sourcebinid, er.agegroupid, omd.polprocessid,
(opmodefraction * meanbaserate) as opmodeweightedmeanbaserate
from opmodedistribution omd
inner join emissionratebyage er using (polprocessid, opmodeid)
inner join sourcebindistribution sbd on (sbd.polprocessid=er.polprocessid
and sbd.sourcebinid=er.sourcebinid)
inner join agecategory acat on (acat.agegroupid=er.agegroupid)
inner join sourcetypemodelyear stmy on (stmy.sourcetypemodelyearid=sbd.sourcetypemodelyearid
and stmy.sourcetypeid=omd.sourcetypeid
and stmy.modelyearid=##context.year##-ageid);
-- end section hasmanyopmodes
-- end section emissionratebyagerates

-- section emissionraterates
-- section hasoneopmode
insert into opmodeweightedemissionratetemp (hourdayid, sourcetypeid, sourcebinid, agegroupid, polprocessid, opmodeweightedmeanbaserate)
select distinct hd.hourdayid, stmy.sourcetypeid, er.sourcebinid, acat.agegroupid, sbd.polprocessid,
(meanbaserate) as opmodeweightedmeanbaserate
from hourday hd
inner join emissionrate er
inner join sourcebindistribution sbd on (sbd.polprocessid=er.polprocessid
and sbd.sourcebinid=er.sourcebinid)
inner join agecategory acat
inner join sourcetypemodelyear stmy on (stmy.sourcetypemodelyearid=sbd.sourcetypemodelyearid
and stmy.modelyearid=##context.year##-ageid);
-- end section hasoneopmode

-- section hasmanyopmodes
insert into opmodeweightedemissionratetemp (hourdayid, sourcetypeid, sourcebinid, agegroupid, polprocessid, opmodeweightedmeanbaserate)
select distinct omd.hourdayid, omd.sourcetypeid, er.sourcebinid, acat.agegroupid, omd.polprocessid,
(opmodefraction * meanbaserate) as opmodeweightedmeanbaserate
from opmodedistribution omd
inner join emissionrate er using (polprocessid, opmodeid)
inner join sourcebindistribution sbd on (sbd.polprocessid=er.polprocessid
and sbd.sourcebinid=er.sourcebinid)
inner join agecategory acat
inner join sourcetypemodelyear stmy on (stmy.sourcetypemodelyearid=sbd.sourcetypemodelyearid
and stmy.sourcetypeid=omd.sourcetypeid
and stmy.modelyearid=##context.year##-ageid);
-- end section hasmanyopmodes
-- end section emissionraterates

drop table if exists opmodeweightedemissionrate;
create table opmodeweightedemissionrate (
	hourdayid smallint(6) not null,
	sourcetypeid smallint(6) not null,
	sourcebinid bigint(20) not null,
	agegroupid smallint(6) not null,
	polprocessid int not null,
	opmodeweightedmeanbaserate float,
	primary key (hourdayid, sourcetypeid, sourcebinid, agegroupid, polprocessid),
	index (hourdayid),
	index (sourcetypeid),
	index (sourcebinid),
	index (agegroupid),
	index (polprocessid)
);

insert into opmodeweightedemissionrate (hourdayid, sourcetypeid, sourcebinid, agegroupid, polprocessid, opmodeweightedmeanbaserate)
select hourdayid, sourcetypeid, sourcebinid, agegroupid, polprocessid, sum(opmodeweightedmeanbaserate)
from opmodeweightedemissionratetemp
group by hourdayid, sourcetypeid, sourcebinid, agegroupid, polprocessid
order by null;

-- --------------------------------------------------------------
-- brpmc step 2: weight emission rates by source bin
-- --------------------------------------------------------------
drop table if exists fullyweightedemissionrate;
create table fullyweightedemissionrate (
	yearid smallint(6) not null,
	hourdayid smallint(6) not null,
	sourcetypeid smallint(6) not null,
	fueltypeid smallint(6) not null,
	modelyearid smallint(6) not null,
	polprocessid int not null,
	fullyweightedmeanbaserate float,
	ageid smallint(6) not null
);

insert into fullyweightedemissionrate (yearid, hourdayid, sourcetypeid, fueltypeid, modelyearid, polprocessid, fullyweightedmeanbaserate, ageid)
select ##context.year## as yearid, omer.hourdayid, omer.sourcetypeid, sb.fueltypeid, stmy.modelyearid, omer.polprocessid,
sum(sourcebinactivityfraction*opmodeweightedmeanbaserate) as fullyweightedmeanbaserate,
acat.ageid
from opmodeweightedemissionrate omer
inner join sourcebindistribution sbd on (sbd.sourcebinid=omer.sourcebinid and sbd.polprocessid=omer.polprocessid)
inner join agecategory acat on (acat.agegroupid=omer.agegroupid)
inner join sourcetypemodelyear stmy on (stmy.sourcetypemodelyearid=sbd.sourcetypemodelyearid
and stmy.sourcetypeid=omer.sourcetypeid and stmy.modelyearid=##context.year##-acat.ageid)
inner join pollutantprocessmodelyear ppmy on (ppmy.polprocessid=sbd.polprocessid and ppmy.modelyearid=stmy.modelyearid)
inner join sourcebin sb on (sb.sourcebinid=sbd.sourcebinid and sb.modelyeargroupid=ppmy.modelyeargroupid)
group by omer.hourdayid, omer.sourcetypeid, sb.fueltypeid, stmy.modelyearid, omer.polprocessid, acat.ageid
order by null;

create index ixfullyweightedemissionrate1 on fullyweightedemissionrate (
	hourdayid asc, 
	yearid asc, 
	ageid asc, 
	sourcetypeid asc
);	

-- --------------------------------------------------------------
-- brpmc step 3: multiply emission rates by activity
-- --------------------------------------------------------------
drop table if exists unadjustedemissionresults;
create table unadjustedemissionresults (
	yearid smallint(6) not null,
	monthid smallint(6) not null,
	hourdayid smallint(6) not null,
	sourcetypeid smallint(6) not null,
	fueltypeid smallint(6) not null,
	modelyearid smallint(6) not null,
	polprocessid int not null,
	unadjustedemissionquant float
);

-- section sourcehoursoperatingactivity
insert ignore into unadjustedemissionresults (yearid, monthid, hourdayid, sourcetypeid, fueltypeid, modelyearid, polprocessid, unadjustedemissionquant)
select f.yearid, sho.monthid, f.hourdayid, f.sourcetypeid, f.fueltypeid, f.modelyearid, f.polprocessid,
(fullyweightedmeanbaserate*sho.sho) as unadjustedemissionquant
from fullyweightedemissionrate f
inner join sho using (hourdayid, yearid, ageid, sourcetypeid);
-- end section sourcehoursoperatingactivity

-- section sourcehoursactivity
insert into unadjustedemissionresults (yearid, monthid, hourdayid, sourcetypeid, fueltypeid, modelyearid, polprocessid, unadjustedemissionquant)
select f.yearid, sourcehours.monthid, f.hourdayid, f.sourcetypeid, f.fueltypeid, f.modelyearid, f.polprocessid,
(fullyweightedmeanbaserate*sourcehours.sourcehours) as unadjustedemissionquant
from fullyweightedemissionrate f
inner join sourcehours using (hourdayid, yearid, ageid, sourcetypeid);
-- end section sourcehoursactivity

-- section startsactivity
insert into unadjustedemissionresults (yearid, monthid, hourdayid, sourcetypeid, fueltypeid, modelyearid, polprocessid, unadjustedemissionquant)
select f.yearid, starts.monthid, f.hourdayid, f.sourcetypeid, f.fueltypeid, f.modelyearid, f.polprocessid,
(fullyweightedmeanbaserate*starts.starts) as unadjustedemissionquant
from fullyweightedemissionrate f
inner join starts using (hourdayid, yearid, ageid, sourcetypeid);
-- end section startsactivity

create index ixunadjustedemissionresults1 on unadjustedemissionresults (
	yearid asc, 
	monthid asc, 
	sourcetypeid asc, 
	fueltypeid asc, 
	modelyearid asc, 
	polprocessid asc
);

-- --------------------------------------------------------------
-- brpmc step 4: weight emission rates by fuel adjustment
-- --------------------------------------------------------------
-- 
-- brpmc 4-a: combine gpa and non gpa fuel adjustment factors 
--

-- 
-- brpmc 4-b: aggregate county fuel adjustments to fuel type
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
select ##context.iterlocation.countyrecordid## as countyid, yearid, may.monthid, fs.fuelformulationid, fst.fueltypeid, fs.marketshare
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
       yearid asc,
       monthid asc,
       sourcetypeid asc,
       fueltypeid asc,
       modelyearid asc,
       polprocessid asc
);

alter table `generalfuelratio` add index `idx_2fuelformulationid`(`fuelformulationid`),
 add index `idx_3sourcetypeid`(`sourcetypeid`), add index `idx_4fueltypeid`(`fueltypeid`);


insert into fuelsupplyadjustment (countyid, yearid, monthid, polprocessid, modelyearid,
	sourcetypeid, fueltypeid, fueladjustment)
select c.countyid, fsft.yearid, fsft.monthid, ppmy.polprocessid, ppmy.modelyearid, 
	rst.sourcetypeid, fsft.fueltypeid, 
	sum((ifnull(fueleffectratio,1)+gpafract*(ifnull(fueleffectratiogpa,1)-ifnull(fueleffectratio,1)))*marketshare)
from county c
inner join pollutantprocessmodelyear ppmy
inner join fuelsupplywithfueltype fsft
inner join runspecsourcetype rst
left outer join generalfuelratio gfr on (
	gfr.fuelformulationid = fsft.fuelformulationid
	and gfr.polprocessid = ppmy.polprocessid
	and gfr.minmodelyearid <= ppmy.modelyearid
	and gfr.maxmodelyearid >= ppmy.modelyearid
	and gfr.minageid <= ##context.year## - ppmy.modelyearid
	and gfr.maxageid >= ##context.year## - ppmy.modelyearid
	and gfr.sourcetypeid = rst.sourcetypeid
)
group by c.countyid, fsft.yearid, fsft.monthid, ppmy.polprocessid, ppmy.modelyearid, 
	rst.sourcetypeid, fsft.fueltypeid
order by modelyearid asc;

-- 
-- brpmc 4-c: apply fuel adjustment to weighted emission rates
--

drop table if exists fueladjustedemissionrate;
create table fueladjustedemissionrate (
	yearid smallint(6) not null,
	monthid smallint(6) not null,
	hourdayid smallint(6) not null,
	sourcetypeid smallint(6) not null,
	fueltypeid smallint(6) not null,
	modelyearid smallint(6) not null,
	polprocessid int not null,
	unadjustedemissionquant float
);

insert into fueladjustedemissionrate (yearid, monthid, hourdayid, sourcetypeid, fueltypeid, modelyearid, polprocessid, unadjustedemissionquant)
select distinct u.yearid, u.monthid, u.hourdayid, u.sourcetypeid, u.fueltypeid, u.modelyearid, u.polprocessid, 
coalesce((f.fueladjustment * u.unadjustedemissionquant), u.unadjustedemissionquant) as unadjustedemissionquant 
from unadjustedemissionresults u
inner join fuelsupplyadjustment f using (yearid, monthid, sourcetypeid, fueltypeid, modelyearid, polprocessid);

create index ixfueladjustedemissionrate1 on fueladjustedemissionrate (
	hourdayid asc, 
	monthid asc, 
	polprocessid asc, 
	fueltypeid
);

create index ixfueladjustedemissionrate2 on fueladjustedemissionrate (
	hourdayid asc
);		

-- --------------------------------------------------------------
-- brpmc step 5: apply temperature adjustment
-- --------------------------------------------------------------
drop table if exists adjustedemissionresults;
create table adjustedemissionresults (
	yearid smallint(6) not null,
	monthid smallint(6) not null,
	dayid smallint(6) not null,
	hourid smallint(6) not null,
	sourcetypeid smallint(6) not null,
	fueltypeid smallint(6) not null,
	modelyearid smallint(6) not null,
	polprocessid int not null,
	emissionquant float
);


-- section applylineartemperatureadjustment
insert into adjustedemissionresults (yearid, monthid, dayid, hourid, sourcetypeid, fueltypeid, modelyearid, polprocessid, emissionquant)
select u.yearid, u.monthid, hd.dayid, hd.hourid, u.sourcetypeid, u.fueltypeid, u.modelyearid, u.polprocessid,
coalesce(
unadjustedemissionquant*(1.0+
(case when temperature <= 72.0 then (temperature-75.0)*(tempadjustterma+tempadjusttermb*(temperature-75.0)) else 0 end)), unadjustedemissionquant) as emissionquant
from fueladjustedemissionrate u
inner join hourday hd using (hourdayid)
inner join zonemonthhour zmh on (zmh.monthid=u.monthid and zmh.hourid=hd.hourid)
left outer join temperatureadjustment ta on (ta.polprocessid=u.polprocessid and ta.fueltypeid=u.fueltypeid);
-- end section applylineartemperatureadjustment

-- section applytemperatureadjustment
insert into adjustedemissionresults (yearid, monthid, dayid, hourid, sourcetypeid, fueltypeid, modelyearid, polprocessid, emissionquant)
select u.yearid, u.monthid, hd.dayid, hd.hourid, u.sourcetypeid, u.fueltypeid, u.modelyearid, u.polprocessid,
coalesce(
unadjustedemissionquant*exp((case when temperature <= 72.0 and modelyearid between minmodelyearid and maxmodelyearid  
	then tempadjustterma*(72.0-temperature) else 0 end)),  unadjustedemissionquant) as emissionquant
from fueladjustedemissionrate u
inner join hourday hd using (hourdayid)
inner join zonemonthhour zmh on (zmh.monthid=u.monthid and zmh.hourid=hd.hourid)
left outer join temperatureadjustment ta on (
	ta.polprocessid=u.polprocessid
	and ta.fueltypeid=u.fueltypeid
	and u.modelyearid between ta.minmodelyearid and ta.maxmodelyearid);
-- end section applytemperatureadjustment

-- section notemperatureadjustment
insert into adjustedemissionresults (yearid, monthid, dayid, hourid, sourcetypeid, fueltypeid, modelyearid, polprocessid, emissionquant)
select u.yearid, u.monthid, hd.dayid, hd.hourid, u.sourcetypeid, u.fueltypeid, u.modelyearid, u.polprocessid,
unadjustedemissionquant as emissionquant
from fueladjustedemissionrate u
inner join hourday hd using (hourdayid);
-- end section notemperatureadjustment

create index ixadjustedemissionresults1 on adjustedemissionresults (
	polprocessid asc
);

-- -------------------------------------------------------------------------------
-- brpmc step 6: convert results to structure of movesworkeroutput by sourcetypeid
-- -------------------------------------------------------------------------------
insert into movesworkeroutput (yearid, monthid, dayid, hourid, stateid, countyid, zoneid, linkid,
pollutantid, processid, sourcetypeid, fueltypeid, modelyearid, roadtypeid, emissionquant)
select a.yearid, a.monthid, a.dayid, a.hourid,
##context.iterlocation.staterecordid## as stateid,
##context.iterlocation.countyrecordid## as countyid,
##context.iterlocation.zonerecordid## as zoneid,
##context.iterlocation.linkrecordid## as linkid,
ppa.pollutantid, ppa.processid,
a.sourcetypeid, a.fueltypeid, a.modelyearid, l.roadtypeid, a.emissionquant
from adjustedemissionresults a
inner join pollutantprocessassoc ppa on (ppa.polprocessid=a.polprocessid)
inner join link l;

alter table fullyweightedemissionrate drop index ixfullyweightedemissionrate1;
alter table fueladjustedemissionrate drop index ixfueladjustedemissionrate1;
alter table fueladjustedemissionrate drop index ixfueladjustedemissionrate2;
alter table adjustedemissionresults drop index ixadjustedemissionresults1;	
alter table unadjustedemissionresults drop index ixunadjustedemissionresults1;

-- end section processing

-- section cleanup
drop table if exists opmodeweightedemissionratetemp;
drop table if exists opmodeweightedemissionrate;
drop table if exists fullyweightedemissionrate;
drop table if exists unadjustedemissionresults;
drop table if exists adjustedemissionresults;
drop table if exists fueladjustedemissionrate;
-- end section cleanup
