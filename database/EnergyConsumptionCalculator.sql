-- version 2013-09-15
-- two sql statements at lines 828-862 optimized per consultant recommendations

-- @algorithm
-- @owner energy consumption calculator

-- section create remote tables for extracted data

##create.county##;
truncate table county;

##create.emissionprocess##;
truncate table emissionprocess;

##create.emissionrate##;
truncate table emissionrate;

##create.fuelformulation##;
truncate table fuelformulation;

##create.fuelsubtype##;
truncate table fuelsubtype;

##create.fuelsupply##;
truncate table fuelsupply;

##create.fullacadjustment##;
truncate table fullacadjustment;

##create.generalfuelratio##;
truncate table generalfuelratio;

##create.hourday##;
truncate table hourday;

##create.link##;
truncate table link;

##create.modelyear##;
truncate modelyear;

##create.monthgrouphour##;
truncate table monthgrouphour;

##create.monthofanyyear##;
truncate table monthofanyyear;

##create.opmodedistribution##;
truncate table opmodedistribution;

##create.pollutant##;
truncate table pollutant;

##create.pollutantprocessassoc##;
truncate table pollutantprocessassoc;

##create.pollutantprocessmodelyear##;
truncate table pollutantprocessmodelyear;

##create.runspecsourcetype##;
truncate runspecsourcetype;

##create.sourcebin##;
truncate table sourcebin;

##create.sourcebindistribution##;
truncate table sourcebindistribution;

##create.sourcetypeage##;
truncate table sourcetypeage;

##create.sourcetypemodelyear##;
truncate table sourcetypemodelyear;

##create.temperatureadjustment##;
truncate table temperatureadjustment;

##create.year##;
truncate table year;

##create.zone##;
truncate table zone;

##create.zonemonthhour##;
truncate table zonemonthhour;

-- section running exhaust
##create.sho##;
truncate table sho;
-- end section running exhaust
-- section start exhaust
##create.starts##;
truncate table starts;
-- end section start exhaust
-- section extended idle exhaust
##create.extendedidlehours##;
truncate table extendedidlehours;
-- end section extended idle exhaust
-- section auxiliary power exhaust
##create.hotellinghours##;
truncate table hotellinghours;

-- @input hotellingactivitydistribution
##create.hotellingactivitydistribution##;
truncate table hotellingactivitydistribution;

##create.runspechourday##;
truncate table runspechourday;

create table if not exists hotellingoperatingmode (
	opmodeid smallint(6) not null,
	primary key (opmodeid)
);
-- end section auxiliary power exhaust
-- end section create remote tables for extracted data

-- section extract data

cache select * into outfile '##county##'
from county
where countyid = ##context.iterlocation.countyrecordid##;

cache select * into outfile '##zone##'
from zone
where zoneid = ##context.iterlocation.zonerecordid##;

cache select link.*
into outfile '##link##'
from link
where linkid = ##context.iterlocation.linkrecordid##;

cache select * into outfile '##modelyear##'
from modelyear;

cache select * 
into outfile '##emissionprocess##'
from emissionprocess
where processid=##context.iterprocess.databasekey##;

-- @input fuelsupply
-- @input monthofanyyear
-- @input year
-- @input runspecmonthgroup
drop table if exists tempfuelformulation;
create table if not exists tempfuelformulation (
	fuelformulationid int not null primary key
);
insert into tempfuelformulation (fuelformulationid)
select distinct fuelformulationid
from fuelsupply
inner join runspecmonthgroup on (fuelsupply.monthgroupid = runspecmonthgroup.monthgroupid)
inner join monthofanyyear on (monthofanyyear.monthgroupid = runspecmonthgroup.monthgroupid)
inner join year on (fuelsupply.fuelyearid = year.fuelyearid)
where fuelregionid = ##context.fuelregionid##
and yearid = ##context.year##
and monthofanyyear.monthid = ##context.monthid##;

select gfr.* into outfile '##generalfuelratio##'
from generalfuelratio gfr
inner join tempfuelformulation tff on tff.fuelformulationid = gfr.fuelformulationid
where polprocessid in (##pollutantprocessids##)
and minmodelyearid <= ##context.year##;

drop table tempfuelformulation;

cache select monthofanyyear.*
into outfile '##monthofanyyear##'
from monthofanyyear
where monthofanyyear.monthid = ##context.monthid##;

cache select distinct monthgrouphour.* 
into outfile '##monthgrouphour##'
from monthgrouphour, runspecmonthgroup, runspechour, monthofanyyear
where runspecmonthgroup.monthgroupid = monthgrouphour.monthgroupid
and monthgrouphour.hourid = runspechour.hourid
and monthofanyyear.monthgroupid = runspecmonthgroup.monthgroupid
and monthofanyyear.monthid = ##context.monthid##;

cache select *
into outfile '##pollutant##'
from pollutant;

cache select distinct zonemonthhour.* 
into outfile '##zonemonthhour##'
from zonemonthhour,runspechour
where zoneid = ##context.iterlocation.zonerecordid##
and zonemonthhour.monthid = ##context.monthid##
and runspechour.hourid = zonemonthhour.hourid;

cache select distinct hourday.* 
into outfile '##hourday##'
from hourday,runspechour,runspecday
where hourday.dayid = runspecday.dayid
and hourday.hourid = runspechour.hourid;

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

select * 
into outfile '##opmodedistribution##'
from opmodedistribution, runspecsourcetype
where polprocessid in (##pollutantprocessids##)
and linkid = ##context.iterlocation.linkrecordid##
and runspecsourcetype.sourcetypeid = opmodedistribution.sourcetypeid;

cache select temperatureadjustment.* 
into outfile '##temperatureadjustment##'
from temperatureadjustment
where polprocessid in (##pollutantprocessids##);

cache select * 
into outfile '##fullacadjustment##'
from fullacadjustment, runspecsourcetype
where polprocessid in (##pollutantprocessids##)
and runspecsourcetype.sourcetypeid = fullacadjustment.sourcetypeid;

cache select distinct emissionrate.* 
into outfile '##emissionrate##'
from emissionrate, sourcebindistribution, sourcetypemodelyear, sourcebin, runspecsourcefueltype
where runspecsourcefueltype.fueltypeid = sourcebin.fueltypeid
and emissionrate.polprocessid = sourcebindistribution.polprocessid
and emissionrate.sourcebinid = sourcebin.sourcebinid
and emissionrate.sourcebinid = sourcebindistribution.sourcebinid
and sourcebin.sourcebinid = sourcebindistribution.sourcebinid
and runspecsourcefueltype.sourcetypeid = sourcetypemodelyear.sourcetypeid
and sourcebindistribution.sourcetypemodelyearid = sourcetypemodelyear.sourcetypemodelyearid
and sourcetypemodelyear.modelyearid <= ##context.year##
and sourcetypemodelyear.modelyearid >= ##context.year## - 30
and emissionrate.polprocessid in (##pollutantprocessids##);

cache select ff.* into outfile '##fuelformulation##'
from fuelformulation ff
inner join fuelsupply fs on fs.fuelformulationid = ff.fuelformulationid
inner join year y on y.fuelyearid = fs.fuelyearid
inner join runspecmonthgroup rsmg on rsmg.monthgroupid = fs.monthgroupid
inner join monthofanyyear on monthofanyyear.monthgroupid = rsmg.monthgroupid
where fuelregionid = ##context.fuelregionid## and
yearid = ##context.year##
and monthofanyyear.monthid = ##context.monthid##
group by ff.fuelformulationid;

cache select fuelsupply.* 
into outfile '##fuelsupply##'
from fuelsupply, runspecmonthgroup, year, monthofanyyear
where fuelregionid = ##context.fuelregionid##
and yearid = ##context.year##
and fuelsupply.fuelyearid = year.fuelyearid
and fuelsupply.monthgroupid = runspecmonthgroup.monthgroupid
and monthofanyyear.monthgroupid = fuelsupply.monthgroupid
and monthofanyyear.monthid = ##context.monthid##;

cache select * 
into outfile '##fuelsubtype##'
from fuelsubtype;

cache select * 
into outfile '##pollutantprocessassoc##'
from pollutantprocessassoc
where processid=##context.iterprocess.databasekey##;

cache select *
into outfile '##pollutantprocessmodelyear##'
from pollutantprocessmodelyear
where modelyearid <= ##context.year##
and modelyearid >= ##context.year## - 30;

cache select * into outfile '##runspecsourcetype##'
from runspecsourcetype;

cache select sourcetypemodelyear.* 
into outfile '##sourcetypemodelyear##'
from sourcetypemodelyear,runspecsourcetype
where sourcetypemodelyear.sourcetypeid = runspecsourcetype.sourcetypeid
and modelyearid <= ##context.year##
and modelyearid >= ##context.year## - 30;

cache select sourcetypeage.* 
into outfile '##sourcetypeage##'
from sourcetypeage,runspecsourcetype
where sourcetypeage.sourcetypeid = runspecsourcetype.sourcetypeid;

cache select year.*
into outfile '##year##'
from year
where yearid = ##context.year##;

-- section running exhaust
select  sho.* 
into outfile '##sho##'
from sho
where yearid = ##context.year##
and monthid = ##context.monthid##
and linkid = ##context.iterlocation.linkrecordid##;
-- end section running exhaust

-- section start exhaust
select starts.* 
into outfile '##starts##'
from starts
where yearid = ##context.year##
and monthid = ##context.monthid##
and zoneid = ##context.iterlocation.zonerecordid##;
-- end section start exhaust

-- section extended idle exhaust
select extendedidlehours.* 
into outfile '##extendedidlehours##'
from extendedidlehours
where yearid = ##context.year##
and monthid = ##context.monthid##
and zoneid = ##context.iterlocation.zonerecordid##;
-- end section extended idle exhaust

-- section auxiliary power exhaust
select hotellinghours.* 
into outfile '##hotellinghours##'
from hotellinghours
where yearid = ##context.year##
and monthid = ##context.monthid##
and zoneid = ##context.iterlocation.zonerecordid##;

cache select * into outfile '##hotellingactivitydistribution##'
from hotellingactivitydistribution
where (beginmodelyearid <= ##context.year## - 30 and endmodelyearid >= ##context.year## - 30)
or (beginmodelyearid <= ##context.year## and endmodelyearid >= ##context.year##)
or (beginmodelyearid >= ##context.year## - 30 and endmodelyearid <= ##context.year##);

cache select * into outfile '##runspechourday##'
from runspechourday;

cache select opmodeid into outfile '##hotellingoperatingmode##'
from operatingmode
where opmodeid >= 201 and opmodeid <= 299;
-- end section auxiliary power exhaust

-- end section extract data

-- section local data removal
-- (common code here)
-- section running exhaust
-- end section running exhaust
-- section start exhaust
-- end section start exhaust
-- section extended idle exhaust
-- end section extended idle exhaust
-- section auxiliary power exhaust
-- end section auxiliary power exhaust
-- end section local data removal

-- section processing

-- add default opmodedistribution entries if needed
-- section fossil energy without opmodedistribution
-- section running exhaust
insert ignore into opmodedistribution ( sourcetypeid,hourdayid,linkid,polprocessid,opmodeid,opmodefraction,opmodefractioncv)
select distinct sourcetypeid, hourdayid, ##context.iterlocation.linkrecordid##, 9301, ##fossilenergyopmodeid##, 1, 0 from sho;
-- end section running exhaust
-- section start exhaust
insert ignore into opmodedistribution ( sourcetypeid,hourdayid,linkid,polprocessid,opmodeid,opmodefraction,opmodefractioncv)
select distinct sourcetypeid, hourdayid, ##context.iterlocation.linkrecordid##, 9302, ##fossilenergyopmodeid##, 1, 0 from starts;
-- end section start exhaust
-- section extended idle exhaust
insert ignore into opmodedistribution ( sourcetypeid,hourdayid,linkid,polprocessid,opmodeid,opmodefraction,opmodefractioncv)
select distinct sourcetypeid, hourdayid, ##context.iterlocation.linkrecordid##, 9390, ##fossilenergyopmodeid##, 1, 0 from extendedidlehours;
-- end section extended idle exhaust
-- section auxiliary power exhaust
insert ignore into fullacadjustment (sourcetypeid,polprocessid,opmodeid,fullacadjustment,fullacadjustmentcv)
select 62,9391,opmodeid,1,null from hotellingoperatingmode;
-- end section auxiliary power exhaust
-- end section fossil energy without opmodedistribution

-- section petroleum energy without opmodedistribution
-- section running exhaust
insert ignore into opmodedistribution ( sourcetypeid,hourdayid,linkid,polprocessid,opmodeid,opmodefraction,opmodefractioncv)
select distinct sourcetypeid, hourdayid, ##context.iterlocation.linkrecordid##, 9201, ##petroleumenergyopmodeid##, 1, 0 from sho;
-- end section running exhaust
-- section start exhaust
insert ignore into opmodedistribution ( sourcetypeid,hourdayid,linkid,polprocessid,opmodeid,opmodefraction,opmodefractioncv)
select distinct sourcetypeid, hourdayid, ##context.iterlocation.linkrecordid##, 9202, ##petroleumenergyopmodeid##, 1, 0 from starts;
-- end section start exhaust
-- section extended idle exhaust
insert ignore into opmodedistribution ( sourcetypeid,hourdayid,linkid,polprocessid,opmodeid,opmodefraction,opmodefractioncv)
select distinct sourcetypeid, hourdayid, ##context.iterlocation.linkrecordid##, 9290, ##petroleumenergyopmodeid##, 1, 0 from extendedidlehours;
-- end section extended idle exhaust
-- section auxiliary power exhaust
insert ignore into fullacadjustment (sourcetypeid,polprocessid,opmodeid,fullacadjustment,fullacadjustmentcv)
select 62,9291,opmodeid,1,null from hotellingoperatingmode;
-- end section auxiliary power exhaust
-- end section petroleum energy without opmodedistribution

-- section total energy without opmodedistribution
-- section running exhaust
insert ignore into opmodedistribution ( sourcetypeid,hourdayid,linkid,polprocessid,opmodeid,opmodefraction,opmodefractioncv)
select distinct sourcetypeid, hourdayid, ##context.iterlocation.linkrecordid##, 9101, ##totalenergyopmodeid##, 1, 0 from sho;
-- end section running exhaust
-- section start exhaust
insert ignore into opmodedistribution ( sourcetypeid,hourdayid,linkid,polprocessid,opmodeid,opmodefraction,opmodefractioncv)
select distinct sourcetypeid, hourdayid, ##context.iterlocation.linkrecordid##, 9102, ##totalenergyopmodeid##, 1, 0 from starts;
-- end section start exhaust
-- section extended idle exhaust
insert ignore into opmodedistribution ( sourcetypeid,hourdayid,linkid,polprocessid,opmodeid,opmodefraction,opmodefractioncv)
select distinct sourcetypeid, hourdayid, ##context.iterlocation.linkrecordid##, 9190, ##totalenergyopmodeid##, 1, 0 from extendedidlehours;
-- end section extended idle exhaust
-- section auxiliary power exhaust
insert ignore into fullacadjustment (sourcetypeid,polprocessid,opmodeid,fullacadjustment,fullacadjustmentcv)
select 62,9191,opmodeid,1,null from hotellingoperatingmode;
-- end section auxiliary power exhaust

-- end section total energy without opmodedistribution

analyze table opmodedistribution;

-- section petroleum energy
-- @algorithm create petroleumfraction table.
-- petroleumfraction(countyid, yearid, monthgroupid, fueltypeid) = sum(marketshare * fuelsubtypepetroleumfraction)
create table if not exists petroleumfraction (
	countyid		integer		not null,
	yearid			smallint	not null,
	monthgroupid		smallint	not null,
	fueltypeid 		smallint	not null,
	petroleumfraction 	float		null,
	petroleumfractionv	float		null,
	index xpkpetroleumfraction (
		countyid, yearid, monthgroupid, fueltypeid)
);

truncate petroleumfraction;

insert into petroleumfraction (
	countyid,
	yearid,
	monthgroupid,
	fueltypeid,
	petroleumfraction,
    petroleumfractionv )
select 
	##context.iterlocation.countyrecordid## as countyid,
	y.yearid,
	fs.monthgroupid,
	fst.fueltypeid,
	sum(fs.marketshare * fst.fuelsubtypepetroleumfraction),
	null
from
	fuelsupply fs,
	fuelformulation ff,
	year y,
	fuelsubtype fst
where
	ff.fuelformulationid = fs.fuelformulationid and
	fst.fuelsubtypeid = ff.fuelsubtypeid and
	fs.fuelyearid = y.fuelyearid and
	y.yearid = ##context.year##
group by
	fs.fuelregionid,
	y.yearid,
	fs.monthgroupid,
	fst.fueltypeid;

analyze table petroleumfraction;

-- end section petroleum energy

-- section fossil fuel energy
-- @algorithm create fossilfraction table.
-- fossilfraction(countyid, yearid, monthgroupid, fueltypeid) = sum(marketshare * fuelsubtypefossilfraction)
create table if not exists fossilfraction (
	countyid		integer		not null,
	yearid			smallint	not null,
	monthgroupid		smallint	not null,
	fueltypeid		smallint	not null,
	fossilfraction 		float		null,
	fossilfractionv		float		null,
	index xpkfossilfraction (
		countyid, yearid, monthgroupid, fueltypeid)
);

truncate fossilfraction;

insert into fossilfraction (
	countyid,
	yearid,
	monthgroupid,
	fueltypeid,
	fossilfraction,
	fossilfractionv)
select 
	##context.iterlocation.countyrecordid## as countyid,
	y.yearid,
	fs.monthgroupid,
	fst.fueltypeid,
	sum(fs.marketshare * fst.fuelsubtypefossilfraction),
	null
from
	fuelsupply fs,
	fuelformulation ff,
	year y,
	fuelsubtype fst
where
	ff.fuelformulationid = fs.fuelformulationid and
	fst.fuelsubtypeid = ff.fuelsubtypeid and
	y.fuelyearid = fs.fuelyearid and
	y.yearid = ##context.year##
group by
	fs.fuelregionid,
	y.yearid,
	fs.monthgroupid,
	fst.fueltypeid;

analyze table fossilfraction;

-- end section fossil fuel energy

--
-- eccp-1b: convert age to model year for analysis year
--
--  modelyearid = yearid - ageid (or ageid = yearid - modelyearid, where yearid = analysisyear)
--	can't find anything to do here, yet.
--

--
-- eccp-2: calculate adjustments
--
--	note: adjustments are multiplicative, with a value of 1.0 meaning no effect
--
-- eccp-2a: calculate air conditioning adjustment (for analysisyear)
--

-- preliminary calculation (1): aconfraction
-- @algorithm aconfraction(monthid,zoneid,hourid) = acactivityterma+zmh.heatindex*(mgh.acactivitytermb+mgh.acactivitytermc*zmh.heatindex)
-- @condition 0 <= aconfraction <= 1
create table if not exists aconfraction (
	monthid			smallint not null,
	zoneid			integer not null,
	hourid			smallint not null,
	aconfraction	float null,
	index xpkaconfraction (monthid, zoneid, hourid)
);

truncate aconfraction;
insert into aconfraction (
	monthid,
	zoneid,
	hourid,
	aconfraction)
select
	zmh.monthid,
	zmh.zoneid,
	mgh.hourid,
	(acactivityterma+zmh.heatindex*(mgh.acactivitytermb+mgh.acactivitytermc*zmh.heatindex))
from 
	zonemonthhour zmh,
	monthofanyyear may,
	monthgrouphour mgh
where
	may.monthid = zmh.monthid and
	mgh.monthgroupid = may.monthgroupid and
	mgh.hourid = zmh.hourid;

analyze table aconfraction;

update aconfraction set aconfraction = 1 where aconfraction > 1;
update aconfraction set aconfraction = 0 where aconfraction < 0;

-- preliminary calculation (2): acactivityfraction
-- @algorithm acactivityfraction(sourcetypeid,modelyearid,monthid,zoneid,hourid)=aconfraction * acpenetrationfraction * functioningacfraction
create table if not exists acactivityfraction (
	sourcetypeid	smallint not null,
	modelyearid		smallint not null,
	monthid			smallint not null,
	zoneid			integer not null,
	hourid			smallint not null,
	acactivityfraction	float null,
	index xpkacactivityfraction (sourcetypeid, modelyearid, monthid, zoneid, hourid)
);

truncate acactivityfraction;
insert into acactivityfraction (
	sourcetypeid,
	modelyearid,
	monthid,
	zoneid,
	hourid,
	acactivityfraction)
select
	stmy.sourcetypeid,
	stmy.modelyearid,
	af.monthid,
	af.zoneid,
	af.hourid,
	aconfraction * acpenetrationfraction * functioningacfraction
from
	aconfraction af,
	sourcetypemodelyear stmy,
	sourcetypeage sta
where
	sta.sourcetypeid = stmy.sourcetypeid and
	sta.ageid = ##context.year## - stmy.modelyearid and
	stmy.modelyearid <= ##context.year## and
	stmy.modelyearid >= ##context.year## - 30;
	

analyze table acactivityfraction;

-- acadjustment
drop table if exists acadjustment;
create table acadjustment (
	sourcetypeid	smallint not null,
	modelyearid		smallint not null,
	polprocessid	int not null,
	opmodeid		smallint not null,
	monthid			smallint not null,
	zoneid			integer not null,
	hourid			smallint not null,
	acadjustment	float null
);

-- insert default fullacadjustment records in case some were omitted on the master
-- @algorithm add default fullacadjustments of 1.0 for any missing value.  fullacadjustment(sourcetypeid,polprocessid,opmodeid)=1.
-- @condition only for missing fullacadjustment entries.
insert ignore into fullacadjustment (
	sourcetypeid,polprocessid,opmodeid,fullacadjustment,fullacadjustmentcv)
select
	omd.sourcetypeid,
	omd.polprocessid,
	omd.opmodeid,
	1,
	null
from
	opmodedistribution omd;

analyze table fullacadjustment;

-- @algorithm acadjustment(sourcetypeid,modelyearid,polprocessid,opmodeid,monthid,zoneid,hourid)=1+((fullacadjustment-1)*acactivityfraction)
truncate acadjustment;
insert into acadjustment (
	sourcetypeid,
	modelyearid,
	polprocessid,
	opmodeid,
	monthid,
	zoneid,
	hourid,
	acadjustment)
select
	aaf.sourcetypeid,
	aaf.modelyearid,
	faa.polprocessid,
	faa.opmodeid,
	aaf.monthid,
	aaf.zoneid,
	aaf.hourid,
	1+((fullacadjustment-1)*acactivityfraction)
from 
	fullacadjustment faa,
	acactivityfraction aaf
where
	faa.sourcetypeid = aaf.sourcetypeid
	and aaf.modelyearid <= ##context.year##;

create index xpkacadjustment on acadjustment (
	sourcetypeid asc, 
	modelyearid asc, 
	polprocessid asc, 
	opmodeid asc, 
	hourid asc
);	

--
-- eccp-2b: calculate temperature adjustment
--

create table if not exists tempadjustmentbytype (
	polprocessid		int not null,
	fueltypeid			smallint not null,
	modelyearid			smallint not null,
	monthid				smallint not null,
	zoneid				integer not null,
	hourid				smallint not null,
	tempadjustment 		float null,
	tempadjustmentv		float null
);

-- @algorithm tempadjustment(polprocessid,fueltypeid,monthid,zoneid,hourid) = 1 + (temperature - 75) * (tempadjustterma + (temperature - 75) * tempadjusttermb)
truncate tempadjustmentbytype;
insert into tempadjustmentbytype (
	polprocessid,
	fueltypeid,
	modelyearid,
	monthid,
	zoneid,
	hourid,
	tempadjustment,
	tempadjustmentv )
select
	ta.polprocessid,
	ta.fueltypeid,
	my.modelyearid,
	zmh.monthid,
	zmh.zoneid,
	zmh.hourid,
	1 + (zmh.temperature - 75) * (ta.tempadjustterma + (zmh.temperature - 75) * ta.tempadjusttermb),
	null
from
	temperatureadjustment ta,
	zonemonthhour zmh,
	modelyear my
where 
	my.modelyearid between ta.minmodelyearid and ta.maxmodelyearid;

create index xpktempadjustmentbytype on tempadjustmentbytype (
	fueltypeid asc,
	modelyearid asc,
	hourid asc,
	monthid asc,
	polprocessid asc,
	zoneid asc
);	

--
-- eccp-2c: calculate fuel adjustment
--	

--
-- eccp-3a:  aggregate base emission rates to source type/fuel type/model year/operating mode level
--

create table if not exists meanbaseratebytype (
	sourcetypeid		smallint not null,
	polprocessid		int not null,
	modelyearid			smallint not null,
	fueltypeid			smallint not null,
	opmodeid			smallint not null,
	meanbaseratebytype	float null
);

truncate table meanbaseratebytype;

loop ##loop.sourcetypeid##;
select sourcetypeid from runspecsourcetype;

-- @algorithm meanbaseratebytype(sourcetypeid,polprocessid,modelyearid,fueltypeid,opmodeid)=sum(sourcebinactivityfraction * meanbaserate)
insert into meanbaseratebytype (
	sourcetypeid,
	polprocessid,
	modelyearid,
	fueltypeid,
	opmodeid,
	meanbaseratebytype)
select
	stmy.sourcetypeid,
	er.polprocessid,
	stmy.modelyearid,
	sb.fueltypeid,
	er.opmodeid,
	sum(sbd.sourcebinactivityfraction * er.meanbaserate)
from
	emissionrate er,
	pollutantprocessmodelyear ppmy,
	sourcebin sb,
	sourcebindistribution sbd,
	sourcetypemodelyear stmy
where
	ppmy.modelyeargroupid = sb.modelyeargroupid and
	ppmy.modelyearid = stmy.modelyearid and
	er.polprocessid = ppmy.polprocessid and
	er.polprocessid = sbd.polprocessid and
	ppmy.polprocessid = sbd.polprocessid and
	er.sourcebinid = sb.sourcebinid and
	er.sourcebinid = sbd.sourcebinid and
	sb.sourcebinid = sbd.sourcebinid and
	sbd.sourcetypemodelyearid = stmy.sourcetypemodelyearid and
	stmy.modelyearid <= ##context.year## and
	stmy.sourcetypeid = ##loop.sourcetypeid##
group by
	er.polprocessid,
	stmy.modelyearid,
	sb.fueltypeid,
	er.opmodeid;

--group by
--	stmy.sourcetypeid,

end loop ##loop.sourcetypeid##;

create index xpkmeanbaseratebytype on meanbaseratebytype (
	sourcetypeid asc, 
	polprocessid asc, 
	opmodeid asc
);


--
-- eccp-3b:  aggregate emission rates to sourcetype level, apply a/c adjustment
--

create table if not exists sourcetypeenergy (
	sourcetypeid		smallint not null,
	polprocessid		int not null,
	zoneid				integer not null,
	linkid				integer not null,
	modelyearid			smallint not null,
	monthid				smallint not null,
	hourdayid			smallint not null,
	fueltypeid			smallint not null,
	sourcetypeenergy	float null
);

truncate table sourcetypeenergy;

-- this sql statement rewritten as series of simpler statements to improve performance
-- insert into sourcetypeenergy (
-- 	sourcetypeid,
-- 	polprocessid,
-- 	zoneid,
-- 	linkid,
-- 	modelyearid,
-- 	monthid,
-- 	hourdayid,
-- 	fueltypeid,
-- 	sourcetypeenergy)
-- select straight_join
-- 	aca.sourcetypeid,
-- 	aca.polprocessid,
-- 	omd.linkid,
-- 	aca.modelyearid,
-- 	aca.monthid,
-- 	hd.hourdayid,
-- 	mbrt.fueltypeid,
-- 	sum(omd.opmodefraction * mbrt.meanbaseratebytype * aca.acadjustment)
-- from
-- 	hourday hd,
-- 	acadjustment aca,
-- 	meanbaseratebytype mbrt,
-- 	opmodedistribution omd
-- where
-- 	hd.hourdayid = omd.hourdayid and
-- 	aca.hourid = hd.hourid and
-- 	aca.modelyearid = mbrt.modelyearid and
-- 	aca.opmodeid = mbrt.opmodeid and
-- 	aca.opmodeid = omd.opmodeid and
-- 	mbrt.opmodeid = omd.opmodeid and
-- 	aca.polprocessid = mbrt.polprocessid and
-- 	aca.polprocessid = omd.polprocessid and
-- 	mbrt.polprocessid = omd.polprocessid and
-- 	aca.sourcetypeid = mbrt.sourcetypeid and
-- 	aca.sourcetypeid = omd.sourcetypeid and
-- 	mbrt.sourcetypeid = omd.sourcetypeid
-- group by
-- 	hd.hourdayid,
-- 	aca.monthid,
-- 	aca.zoneid,
-- 	aca.polprocessid,
-- 	aca.sourcetypeid,
-- 	aca.modelyearid,
-- 	mbrt.fueltypeid,
-- 	omd.linkid

loop ##loop.sourcetypeid##;
select sourcetypeid from runspecsourcetype;

drop table if exists omd2;
drop table if exists omdmbr;

-- @algorithm add hourid to opmodedistribution.
create table omd2
  select omd.*, hd.hourid
  from opmodedistribution as omd inner join hourday as hd using (hourdayid)
  where omd.sourcetypeid=##loop.sourcetypeid##;
create index index1 on omd2 
  (polprocessid, opmodeid);

-- @algorithm combine meanbaseratebytype keys with opmodefraction keys.
-- @condition all processes except auxiliary power exhaust
create table omdmbr
  select mbrt.sourcetypeid, mbrt.modelyearid, mbrt.polprocessid, mbrt.opmodeid,
         mbrt.fueltypeid, omd.hourdayid, omd.hourid, omd.linkid,
         omd.opmodefraction, mbrt.meanbaseratebytype
  from omd2 as omd inner join meanbaseratebytype as mbrt
         using (polprocessid, opmodeid)
  where mbrt.sourcetypeid=##loop.sourcetypeid##;

create index index1 on omdmbr (modelyearid, polprocessid, opmodeid, hourid);

-- section auxiliary power exhaust
create unique index index2 on omdmbr (sourcetypeid, modelyearid, polprocessid, opmodeid, fueltypeid, hourdayid, hourid, linkid);

-- @algorithm combine meanbaseratebytype keys with opmodefraction keys.
-- @condition for auxiliary power exhaust process
insert ignore into omdmbr (sourcetypeid, modelyearid, polprocessid, opmodeid,
         fueltypeid, hourdayid, hourid, linkid,
         opmodefraction, meanbaseratebytype)
select mbrt.sourcetypeid, mbrt.modelyearid, mbrt.polprocessid, mbrt.opmodeid,
         mbrt.fueltypeid, rshd.hourdayid, hd.hourid, 
         ##context.iterlocation.linkrecordid## as linkid,
         hac.opmodefraction, mbrt.meanbaseratebytype
from hotellingactivitydistribution hac
	inner join meanbaseratebytype mbrt on (
		mbrt.opmodeid = hac.opmodeid
		and hac.beginmodelyearid <= mbrt.modelyearid
		and hac.endmodelyearid >= mbrt.modelyearid),
	runspechourday rshd inner join hourday hd using (hourdayid)
where mbrt.sourcetypeid=##loop.sourcetypeid##
and 62=##loop.sourcetypeid##;
-- end section auxiliary power exhaust

-- @algorithm sourcetypeenergy(sourcetypeid,modelyearid,polprocessid,monthid,zoneid,fueltypeid,linkid,hourdayid) = sum(opmodefraction * meanbaseratebytype * acadjustment)
insert into sourcetypeenergy (
	sourcetypeid,
	modelyearid,
	polprocessid,
	monthid,
	zoneid,
	fueltypeid,
	linkid,
	hourdayid,
	sourcetypeenergy)
select 
	aca.sourcetypeid,
	aca.modelyearid,
	aca.polprocessid,
	aca.monthid,
	aca.zoneid,
	omdmbr.fueltypeid,
	omdmbr.linkid,
	omdmbr.hourdayid,
	sum(omdmbr.opmodefraction * omdmbr.meanbaseratebytype * aca.acadjustment)
from
	omdmbr as omdmbr inner join acadjustment as aca
	using (modelyearid, polprocessid, opmodeid, hourid)
where
	aca.sourcetypeid = ##loop.sourcetypeid##
group by
	omdmbr.modelyearid,
	omdmbr.polprocessid,
	omdmbr.hourdayid,
	omdmbr.fueltypeid,
	omdmbr.linkid,
	aca.monthid,
	aca.zoneid 
 order by null;

end loop ##loop.sourcetypeid##;

create index xpksourcetypeenergy on sourcetypeenergy (
	sourcetypeid asc,
	polprocessid asc,
	zoneid asc,
	linkid asc,
	modelyearid asc,
	monthid asc,
	hourdayid asc,
	fueltypeid asc
);

create index xpksourcetypeenergy2 on sourcetypeenergy (
	fueltypeid asc,
	hourdayid asc,
	linkid asc,
	modelyearid asc,
	monthid asc,
	polprocessid asc,
	zoneid asc,
	sourcetypeid asc
);


--
-- end of rewritten statement
--
analyze table sourcetypeenergy;

create table if not exists movesworkeroutputtemp (
	yearid               smallint unsigned null,
	monthid              smallint unsigned null,
	dayid                smallint unsigned null,
	hourid               smallint unsigned null,
	stateid              smallint unsigned null,
	countyid             integer unsigned null,
	zoneid               integer unsigned null,
	linkid               integer unsigned null,
	pollutantid          smallint unsigned null,
	processid            smallint unsigned null,
	sourcetypeid         smallint unsigned null,
	fueltypeid           smallint unsigned null,
	modelyearid          smallint unsigned null,
	roadtypeid           smallint unsigned null,
	scc                  char(10) null,
	emissionquant        float null
);

-- section running exhaust
--
-- eccp-3c:  calculate running energy consumption
--

truncate movesworkeroutputtemp;

--
-- calculate the total energy
--

loop ##loop.sourcetypeid##;
select sourcetypeid from runspecsourcetype;

-- @algorithm totalenergy(yearid,monthid,dayid,hourid,stateid,countyid,zoneid,linkid,pollutantid,processid,sourcetypeid,fueltypeid,modelyearid,roadtypeid) = sum(sho * sourcetypeenergy * tempadjustment)
-- @condition for sourcetype output
insert into movesworkeroutputtemp (
    yearid,
    monthid,
    dayid,
    hourid,
    stateid,
    countyid,
    zoneid,
    linkid,
    pollutantid,
    processid,
    sourcetypeid,
    fueltypeid,
    modelyearid,
    roadtypeid,
    emissionquant)
select
	sho.yearid,
	ste.monthid,
	hd.dayid,
	hd.hourid,
	c.stateid,
	c.countyid,
	ste.zoneid,
	ste.linkid,
	ppa.pollutantid,
	ppa.processid,
	ste.sourcetypeid,
	ste.fueltypeid,
	ste.modelyearid,
	l.roadtypeid,
	sum(sho.sho * ste.sourcetypeenergy * tat.tempadjustment )
from
	county c,
	emissionprocess ep,
	hourday hd,
	link l,
	monthofanyyear may,
	pollutantprocessassoc ppa,
	sho sho,
	sourcetypeenergy ste,
	sourcetypemodelyear stmy,
	tempadjustmentbytype tat
where
	c.countyid = l.countyid and
	ste.fueltypeid = tat.fueltypeid and
	hd.hourdayid = sho.hourdayid and
	hd.hourdayid = ste.hourdayid and
	sho.hourdayid = ste.hourdayid and
	hd.hourid = tat.hourid and
	l.linkid = sho.linkid and
	l.linkid = ste.linkid and
	sho.linkid = ste.linkid and
	(sho.yearid - sho.ageid) = ste.modelyearid and
	(sho.yearid - sho.ageid) = stmy.modelyearid and
	ste.modelyearid = stmy.modelyearid and
	ste.modelyearid = tat.modelyearid and
	may.monthid = sho.monthid and
	may.monthid = ste.monthid and
	may.monthid = tat.monthid and
	sho.monthid = ste.monthid and
	sho.monthid = tat.monthid and
	ste.monthid = tat.monthid and
	ppa.polprocessid = ste.polprocessid and
	ppa.polprocessid = tat.polprocessid and
	ste.polprocessid = tat.polprocessid and
	ep.processid = ppa.processid and
	l.zoneid = ste.zoneid and
	l.zoneid = tat.zoneid and
	ste.zoneid = tat.zoneid and
	sho.sourcetypeid = ##loop.sourcetypeid## and
	stmy.sourcetypeid = ##loop.sourcetypeid## and
	ste.sourcetypeid = ##loop.sourcetypeid##
group by
	sho.yearid,
	ste.monthid,
	hd.dayid,
	hd.hourid,
	c.stateid,
	c.countyid,
	ste.zoneid,
	ste.linkid,
	ppa.pollutantid,
	ppa.processid,
	ste.fueltypeid,
	ste.modelyearid,
	l.roadtypeid;

--	sho.sourcetypeid = ste.sourcetypeid and
--	sho.sourcetypeid = stmy.sourcetypeid and
--	ste.sourcetypeid = stmy.sourcetypeid and

--	ppa.processid,
--	ste.sourcetypeid,
--	ste.fueltypeid,

end loop ##loop.sourcetypeid##;

analyze table movesworkeroutputtemp;

-- section total energy
--
-- copy temporary results for total energy to final table. we are done for total energy.
--

-- @algorithm emissionquant[totalenergy](yearid,monthid,dayid,hourid,stateid,countyid,zoneid,linkid,pollutantid,processid,sourcetypeid,fueltypeid,modelyearid,roadtypeid,scc) = totalenergy
insert into movesworkeroutput (
    yearid,
    monthid,
    dayid,
    hourid,
    stateid,
    countyid,
    zoneid,
    linkid,
    pollutantid,
    processid,
    sourcetypeid,
    fueltypeid,
    modelyearid,
    roadtypeid,
    scc,
    emissionquant)
select
    yearid,
    monthid,
    dayid,
    hourid,
    stateid,
    countyid,
    zoneid,
    linkid,
    pollutantid,
    processid,
    sourcetypeid,
    fueltypeid,
    modelyearid,
    roadtypeid,
    scc,
    emissionquant
from 
    movesworkeroutputtemp;

-- end section total energy

-- section petroleum energy
--
-- calculate petroleum energy
--

-- @algorithm emissionquant[petroleumenergy](yearid,monthid,dayid,hourid,stateid,countyid,zoneid,linkid,pollutantid,processid,sourcetypeid,fueltypeid,modelyearid,roadtypeid,scc) = totalenergy * petroleumfraction
insert into movesworkeroutput (
    yearid,
    monthid,
    dayid,
    hourid,
    stateid,
    countyid,
    zoneid,
    linkid,
    pollutantid,
    processid,
    sourcetypeid,
    fueltypeid,
    modelyearid,
    roadtypeid,
    scc,
    emissionquant)
select
    mwot.yearid,
    mwot.monthid,
    mwot.dayid,
    mwot.hourid,
    mwot.stateid,
    mwot.countyid,
    mwot.zoneid,
    mwot.linkid,
    92,
    mwot.processid,
    mwot.sourcetypeid,
    mwot.fueltypeid,
    mwot.modelyearid,
    mwot.roadtypeid,
    mwot.scc,
    mwot.emissionquant * pf.petroleumfraction
from 
    movesworkeroutputtemp mwot,
    petroleumfraction pf,
    monthofanyyear may
where
    may.monthid = mwot.monthid and
    pf.countyid = mwot.countyid and
    pf.yearid = mwot.yearid and 
    pf.monthgroupid = may.monthgroupid and
    pf.fueltypeid = mwot.fueltypeid and
    mwot.processid = 1;

-- end section petroleum energy

-- section fossil fuel energy
--
-- calculate fossil energy
--

-- @algorithm emissionquant[fossilenergy](yearid,monthid,dayid,hourid,stateid,countyid,zoneid,linkid,pollutantid,processid,sourcetypeid,fueltypeid,modelyearid,roadtypeid,scc) = totalenergy * fossilfraction
insert into movesworkeroutput (
    yearid,
    monthid,
    dayid,
    hourid,
    stateid,
    countyid,
    zoneid,
    linkid,
    pollutantid,
    processid,
    sourcetypeid,
    fueltypeid,
    modelyearid,
    roadtypeid,
    scc,
    emissionquant)
select
    mwot.yearid,
    mwot.monthid,
    mwot.dayid,
    mwot.hourid,
    mwot.stateid,
    mwot.countyid,
    mwot.zoneid,
    mwot.linkid,
    93,
    mwot.processid,
    mwot.sourcetypeid,
    mwot.fueltypeid,
    mwot.modelyearid,
    mwot.roadtypeid,
    mwot.scc,
    mwot.emissionquant * ff.fossilfraction
from 
    movesworkeroutputtemp mwot,
    fossilfraction ff,
    monthofanyyear may
where
    may.monthid = mwot.monthid and
    ff.countyid = mwot.countyid and
    ff.yearid = mwot.yearid and 
    ff.monthgroupid = may.monthgroupid and
    ff.fueltypeid = mwot.fueltypeid and
    mwot.processid = 1;
-- end section fossil fuel energy
-- end section running exhaust

analyze table movesworkeroutput;

-- section start exhaust
--
-- eccp-4:  calculate start energy consumption
--

truncate movesworkeroutputtemp;

loop ##loop.sourcetypeid##;
select sourcetypeid from runspecsourcetype;

-- @algorithm totalenergy(yearid,monthid,dayid,hourid,stateid,countyid,zoneid,linkid,pollutantid,processid,sourcetypeid,fueltypeid,modelyearid,roadtypeid) = sum(starts * sourcetypeenergy * tempadjustment)
-- @condition for sourcetype output
-- @condition for starts process
insert into movesworkeroutputtemp (
    yearid,
    monthid,
    dayid,
    hourid,
    stateid,
    countyid,
    zoneid,
    linkid,
    pollutantid,
    processid,
    sourcetypeid,
    fueltypeid,
    modelyearid,
    roadtypeid,
    emissionquant)
select
	s.yearid,
	ste.monthid,
	hd.dayid,
	hd.hourid,
	c.stateid,
	c.countyid,
	ste.zoneid,
	ste.linkid,
	ppa.pollutantid,
	ppa.processid,
	ste.sourcetypeid,
	ste.fueltypeid,
	ste.modelyearid,
	l.roadtypeid,
	sum(s.starts * ste.sourcetypeenergy * tat.tempadjustment)
from
	county c,
	emissionprocess ep,
	hourday hd,
	link l,
	monthofanyyear may,
	pollutantprocessassoc ppa,
	starts s,
	sourcetypeenergy ste,
	sourcetypemodelyear stmy,
	tempadjustmentbytype tat
where
	c.countyid = l.countyid and
	ste.fueltypeid = tat.fueltypeid and
	hd.hourdayid = ste.hourdayid and
	hd.hourdayid = s.hourdayid and
	ste.hourdayid = s.hourdayid and
	hd.hourid = tat.hourid and
	l.linkid = ste.linkid and
	ste.modelyearid = stmy.modelyearid and
	ste.modelyearid = (s.yearid - s.ageid) and
	stmy.modelyearid = (s.yearid - s.ageid) and
	ste.modelyearid = tat.modelyearid and
	may.monthid = ste.monthid and
	may.monthid = s.monthid and
	may.monthid = tat.monthid and
	ste.monthid = s.monthid and
	ste.monthid = tat.monthid and
	s.monthid = tat.monthid and
	ppa.polprocessid = ste.polprocessid and
	ppa.polprocessid = tat.polprocessid and
	ste.polprocessid = tat.polprocessid and
	ep.processid = ppa.processid and
	ste.sourcetypeid = ##loop.sourcetypeid## and
	stmy.sourcetypeid = ##loop.sourcetypeid## and
	s.sourcetypeid = ##loop.sourcetypeid## and
	l.zoneid = ste.zoneid and
	l.zoneid = s.zoneid and
	l.zoneid = tat.zoneid and
	ste.zoneid = s.zoneid and
	ste.zoneid = tat.zoneid and
	s.zoneid = tat.zoneid
group by
	s.yearid,
	ste.monthid,
	hd.dayid,
	hd.hourid,
	c.stateid,
	c.countyid,
	ste.zoneid,
	ste.linkid,
	ppa.pollutantid,
	ppa.processid,
	ste.fueltypeid,
	ste.modelyearid,
	l.roadtypeid;

--	ste.sourcetypeid = stmy.sourcetypeid and
--	ste.sourcetypeid = s.sourcetypeid and
--	stmy.sourcetypeid = s.sourcetypeid and

--	ppa.processid,
--	ste.sourcetypeid,
--	ste.fueltypeid,

end loop ##loop.sourcetypeid##;

-- section total energy
--
-- copy temporary results for total energy to final table. we are done for total energy.
--

-- @algorithm emissionquant[totalenergy](yearid,monthid,dayid,hourid,stateid,countyid,zoneid,linkid,pollutantid,processid,sourcetypeid,fueltypeid,modelyearid,roadtypeid,scc) = totalenergy
-- @condition for starts process
insert into movesworkeroutput (
    yearid,
    monthid,
    dayid,
    hourid,
    stateid,
    countyid,
    zoneid,
    linkid,
    pollutantid,
    processid,
    sourcetypeid,
    fueltypeid,
    modelyearid,
    roadtypeid,
    scc,
    emissionquant)
select
    yearid,
    monthid,
    dayid,
    hourid,
    stateid,
    countyid,
    zoneid,
    linkid,
    pollutantid,
    processid,
    sourcetypeid,
    fueltypeid,
    modelyearid,
    roadtypeid,
    scc,
    emissionquant
from 
    movesworkeroutputtemp;
-- end section total energy

-- section petroleum energy
--
-- calculate petroleum energy
--

-- @algorithm emissionquant[petroleumenergy](yearid,monthid,dayid,hourid,stateid,countyid,zoneid,linkid,pollutantid,processid,sourcetypeid,fueltypeid,modelyearid,roadtypeid,scc) = totalenergy * petroleumfraction
-- @condition for starts process
insert into movesworkeroutput (
    yearid,
    monthid,
    dayid,
    hourid,
    stateid,
    countyid,
    zoneid,
    linkid,
    pollutantid,
    processid,
    sourcetypeid,
    fueltypeid,
    modelyearid,
    roadtypeid,
    scc,
    emissionquant)
select
    mwot.yearid,
    mwot.monthid,
    mwot.dayid,
    mwot.hourid,
    mwot.stateid,
    mwot.countyid,
    mwot.zoneid,
    mwot.linkid,
    92,
    mwot.processid,
    mwot.sourcetypeid,
    mwot.fueltypeid,
    mwot.modelyearid,
    mwot.roadtypeid,
    mwot.scc,
    mwot.emissionquant * pf.petroleumfraction
from 
    movesworkeroutputtemp mwot,
    petroleumfraction pf,
    monthofanyyear may
where
    may.monthid = mwot.monthid and
    pf.countyid = mwot.countyid and
    pf.yearid = mwot.yearid and 
    pf.monthgroupid = may.monthgroupid and
    pf.fueltypeid = mwot.fueltypeid and
    mwot.processid = 2;
-- end section petroleum energy

-- section fossil fuel energy
--
-- calculate fossil energy
--

-- @algorithm emissionquant[fossilenergy](yearid,monthid,dayid,hourid,stateid,countyid,zoneid,linkid,pollutantid,processid,sourcetypeid,fueltypeid,modelyearid,roadtypeid,scc) = totalenergy * fossilfraction
-- @condition for starts process
insert into movesworkeroutput (
    yearid,
    monthid,
    dayid,
    hourid,
    stateid,
    countyid,
    zoneid,
    linkid,
    pollutantid,
    processid,
    sourcetypeid,
    fueltypeid,
    modelyearid,
    roadtypeid,
    scc,
    emissionquant)
select
    mwot.yearid,
    mwot.monthid,
    mwot.dayid,
    mwot.hourid,
    mwot.stateid,
    mwot.countyid,
    mwot.zoneid,
    mwot.linkid,
    93,
    mwot.processid,
    mwot.sourcetypeid,
    mwot.fueltypeid,
    mwot.modelyearid,
    mwot.roadtypeid,
    mwot.scc,
    mwot.emissionquant * ff.fossilfraction
from 
    movesworkeroutputtemp mwot,
    fossilfraction ff,
    monthofanyyear may
where
    may.monthid = mwot.monthid and
    ff.countyid = mwot.countyid and
    ff.yearid = mwot.yearid and 
    ff.monthgroupid = may.monthgroupid and
    ff.fueltypeid = mwot.fueltypeid and
    mwot.processid = 2;
-- end section fossil fuel energy
-- end section start exhaust

-- section extended idle exhaust
--
-- eccp-5:  calculate extended idle energy consumption
--

truncate movesworkeroutputtemp;

loop ##loop.sourcetypeid##;
select sourcetypeid from runspecsourcetype;

insert into movesworkeroutputtemp (
    yearid,
    monthid,
    dayid,
    hourid,
    stateid,
    countyid,
    zoneid,
    linkid,
    pollutantid,
    processid,
    sourcetypeid,
    fueltypeid,
    modelyearid,
    roadtypeid,
    emissionquant)
select
	eih.yearid,
	ste.monthid,
	hd.dayid,
	hd.hourid,
	c.stateid,
	c.countyid,
	ste.zoneid,
	ste.linkid,
	ppa.pollutantid,
	ppa.processid,
	ste.sourcetypeid,
	ste.fueltypeid,
	ste.modelyearid,
	l.roadtypeid,
	sum(eih.extendedidlehours * ste.sourcetypeenergy * tat.tempadjustment)
from
	county c,
	emissionprocess ep,
	extendedidlehours eih,
	hourday hd,
	link l,
	monthofanyyear may,
	pollutantprocessassoc ppa,
	sourcetypeenergy ste,
	sourcetypemodelyear stmy,
	tempadjustmentbytype tat
where
	c.countyid = l.countyid and
	ste.fueltypeid = tat.fueltypeid and
	eih.hourdayid = hd.hourdayid and
	eih.hourdayid = ste.hourdayid and
	hd.hourdayid = ste.hourdayid and
	hd.hourid = tat.hourid and
	l.linkid = ste.linkid and
	(eih.yearid - eih.ageid) = ste.modelyearid and
	(eih.yearid - eih.ageid) = stmy.modelyearid and
	ste.modelyearid = stmy.modelyearid and
	ste.modelyearid = tat.modelyearid and
	eih.monthid = may.monthid and
	eih.monthid = ste.monthid and
	eih.monthid = tat.monthid and
	may.monthid = ste.monthid and
	may.monthid = tat.monthid and
	ste.monthid = tat.monthid and
	ppa.polprocessid = ste.polprocessid and
	ppa.polprocessid = tat.polprocessid and
	ste.polprocessid = tat.polprocessid and
	ep.processid = ppa.processid and
	eih.sourcetypeid = ##loop.sourcetypeid## and
	ste.sourcetypeid = ##loop.sourcetypeid## and
	stmy.sourcetypeid = ##loop.sourcetypeid## and
	eih.zoneid = l.zoneid and
	eih.zoneid = ste.zoneid and
	eih.zoneid = tat.zoneid and
	l.zoneid = ste.zoneid and
	l.zoneid = tat.zoneid and
	ste.zoneid = tat.zoneid
group by
	eih.yearid,
	ste.monthid,
	hd.dayid,
	hd.hourid,
	c.stateid,
	c.countyid,
	ste.zoneid,
	ste.linkid,
	ppa.pollutantid,
	ppa.processid,
	ste.fueltypeid,
	ste.modelyearid,
	l.roadtypeid;

--	eih.sourcetypeid = ste.sourcetypeid and
--	eih.sourcetypeid = stmy.sourcetypeid and
--	ste.sourcetypeid = stmy.sourcetypeid and

--	ppa.processid,
--	ste.sourcetypeid,
--	ste.fueltypeid,

end loop ##loop.sourcetypeid##;

-- section total energy
--
-- copy temporary results for total energy to final table. we are done for total energy.
--

insert into movesworkeroutput (
    yearid,
    monthid,
    dayid,
    hourid,
    stateid,
    countyid,
    zoneid,
    linkid,
    pollutantid,
    processid,
    sourcetypeid,
    fueltypeid,
    modelyearid,
    roadtypeid,
    scc,
    emissionquant)
select
    yearid,
    monthid,
    dayid,
    hourid,
    stateid,
    countyid,
    zoneid,
    linkid,
    pollutantid,
    processid,
    sourcetypeid,
    fueltypeid,
    modelyearid,
    roadtypeid,
    scc,
    emissionquant
from 
    movesworkeroutputtemp;
-- end section total energy

-- section petroleum energy
--
-- calculate petroleum energy
--
insert into movesworkeroutput (
    yearid,
    monthid,
    dayid,
    hourid,
    stateid,
    countyid,
    zoneid,
    linkid,
    pollutantid,
    processid,
    sourcetypeid,
    fueltypeid,
    modelyearid,
    roadtypeid,
    scc,
    emissionquant)
select
    mwot.yearid,
    mwot.monthid,
    mwot.dayid,
    mwot.hourid,
    mwot.stateid,
    mwot.countyid,
    mwot.zoneid,
    mwot.linkid,
    92,
    mwot.processid,
    mwot.sourcetypeid,
    mwot.fueltypeid,
    mwot.modelyearid,
    mwot.roadtypeid,
    mwot.scc,
    mwot.emissionquant * pf.petroleumfraction
from 
	movesworkeroutputtemp mwot,
	petroleumfraction pf,
	monthofanyyear may
where
	may.monthid = mwot.monthid and
	pf.countyid = mwot.countyid and
	pf.yearid = mwot.yearid and 
	pf.monthgroupid = may.monthgroupid and
	pf.fueltypeid = mwot.fueltypeid and
	mwot.processid = 90;
-- end section petroleum energy

-- section fossil fuel energy
--
-- calculate fossil energy
--
insert into movesworkeroutput (
    yearid,
    monthid,
    dayid,
    hourid,
    stateid,
    countyid,
    zoneid,
    linkid,
    pollutantid,
    processid,
    sourcetypeid,
    fueltypeid,
    modelyearid,
    roadtypeid,
    scc,
    emissionquant)
select
    mwot.yearid,
    mwot.monthid,
    mwot.dayid,
    mwot.hourid,
    mwot.stateid,
    mwot.countyid,
    mwot.zoneid,
    mwot.linkid,
    93,
    mwot.processid,
    mwot.sourcetypeid,
    mwot.fueltypeid,
    mwot.modelyearid,
    mwot.roadtypeid,
    mwot.scc,
    mwot.emissionquant * ff.fossilfraction
from 
    movesworkeroutputtemp mwot,
    fossilfraction ff,
    monthofanyyear may
where
    may.monthid = mwot.monthid and
    ff.countyid = mwot.countyid and
    ff.yearid = mwot.yearid and 
    ff.monthgroupid = may.monthgroupid and
    ff.fueltypeid = mwot.fueltypeid and
    mwot.processid = 90;
-- end section fossil fuel energy
-- end section extended idle exhaust

-- section auxiliary power exhaust
--
-- eccp-5:  calculate extended idle energy consumption
--

truncate movesworkeroutputtemp;

loop ##loop.sourcetypeid##;
select sourcetypeid from runspecsourcetype where sourcetypeid=62;

insert into movesworkeroutputtemp (
    yearid,
    monthid,
    dayid,
    hourid,
    stateid,
    countyid,
    zoneid,
    linkid,
    pollutantid,
    processid,
    sourcetypeid,
    fueltypeid,
    modelyearid,
    roadtypeid,
    emissionquant)
select
	eih.yearid,
	ste.monthid,
	hd.dayid,
	hd.hourid,
	c.stateid,
	c.countyid,
	ste.zoneid,
	ste.linkid,
	ppa.pollutantid,
	ppa.processid,
	ste.sourcetypeid,
	ste.fueltypeid,
	ste.modelyearid,
	l.roadtypeid,
	sum(eih.hotellinghours * ste.sourcetypeenergy * tat.tempadjustment)
from
	county c,
	emissionprocess ep,
	hotellinghours eih,
	hourday hd,
	link l,
	monthofanyyear may,
	pollutantprocessassoc ppa,
	sourcetypeenergy ste,
	sourcetypemodelyear stmy,
	tempadjustmentbytype tat
where
	c.countyid = l.countyid and
	ste.fueltypeid = tat.fueltypeid and
	eih.hourdayid = hd.hourdayid and
	eih.hourdayid = ste.hourdayid and
	hd.hourdayid = ste.hourdayid and
	hd.hourid = tat.hourid and
	l.linkid = ste.linkid and
	(eih.yearid - eih.ageid) = ste.modelyearid and
	(eih.yearid - eih.ageid) = stmy.modelyearid and
	ste.modelyearid = stmy.modelyearid and
	ste.modelyearid = tat.modelyearid and 
	eih.monthid = may.monthid and
	eih.monthid = ste.monthid and
	eih.monthid = tat.monthid and
	may.monthid = ste.monthid and
	may.monthid = tat.monthid and
	ste.monthid = tat.monthid and
	ppa.polprocessid = ste.polprocessid and
	ppa.polprocessid = tat.polprocessid and
	ste.polprocessid = tat.polprocessid and
	ep.processid = ppa.processid and
	eih.sourcetypeid = ##loop.sourcetypeid## and
	ste.sourcetypeid = ##loop.sourcetypeid## and
	stmy.sourcetypeid = ##loop.sourcetypeid## and
	eih.zoneid = l.zoneid and
	eih.zoneid = ste.zoneid and
	eih.zoneid = tat.zoneid and
	l.zoneid = ste.zoneid and
	l.zoneid = tat.zoneid and
	ste.zoneid = tat.zoneid
group by
	eih.yearid,
	ste.monthid,
	hd.dayid,
	hd.hourid,
	c.stateid,
	c.countyid,
	ste.zoneid,
	ste.linkid,
	ppa.pollutantid,
	ppa.processid,
	ste.fueltypeid,
	ste.modelyearid,
	l.roadtypeid;

--	eih.sourcetypeid = ste.sourcetypeid and
--	eih.sourcetypeid = stmy.sourcetypeid and
--	ste.sourcetypeid = stmy.sourcetypeid and

--	ppa.processid,
--	ste.sourcetypeid,
--	ste.fueltypeid,

end loop ##loop.sourcetypeid##;

-- section total energy
--
-- copy temporary results for total energy to final table. we are done for total energy.
--

insert into movesworkeroutput (
    yearid,
    monthid,
    dayid,
    hourid,
    stateid,
    countyid,
    zoneid,
    linkid,
    pollutantid,
    processid,
    sourcetypeid,
    fueltypeid,
    modelyearid,
    roadtypeid,
    scc,
    emissionquant)
select
    yearid,
    monthid,
    dayid,
    hourid,
    stateid,
    countyid,
    zoneid,
    linkid,
    pollutantid,
    processid,
    sourcetypeid,
    fueltypeid,
    modelyearid,
    roadtypeid,
    scc,
    emissionquant
from 
    movesworkeroutputtemp;
-- end section total energy

-- section petroleum energy
--
-- calculate petroleum energy
--
insert into movesworkeroutput (
    yearid,
    monthid,
    dayid,
    hourid,
    stateid,
    countyid,
    zoneid,
    linkid,
    pollutantid,
    processid,
    sourcetypeid,
    fueltypeid,
    modelyearid,
    roadtypeid,
    scc,
    emissionquant)
select
    mwot.yearid,
    mwot.monthid,
    mwot.dayid,
    mwot.hourid,
    mwot.stateid,
    mwot.countyid,
    mwot.zoneid,
    mwot.linkid,
    92,
    mwot.processid,
    mwot.sourcetypeid,
    mwot.fueltypeid,
    mwot.modelyearid,
    mwot.roadtypeid,
    mwot.scc,
    mwot.emissionquant * pf.petroleumfraction
from 
	movesworkeroutputtemp mwot,
	petroleumfraction pf,
	monthofanyyear may
where
	may.monthid = mwot.monthid and
	pf.countyid = mwot.countyid and
	pf.yearid = mwot.yearid and 
	pf.monthgroupid = may.monthgroupid and
	pf.fueltypeid = mwot.fueltypeid and
	mwot.processid = 91;
-- end section petroleum energy

-- section fossil fuel energy
--
-- calculate fossil energy
--
insert into movesworkeroutput (
    yearid,
    monthid,
    dayid,
    hourid,
    stateid,
    countyid,
    zoneid,
    linkid,
    pollutantid,
    processid,
    sourcetypeid,
    fueltypeid,
    modelyearid,
    roadtypeid,
    scc,
    emissionquant)
select
    mwot.yearid,
    mwot.monthid,
    mwot.dayid,
    mwot.hourid,
    mwot.stateid,
    mwot.countyid,
    mwot.zoneid,
    mwot.linkid,
    93,
    mwot.processid,
    mwot.sourcetypeid,
    mwot.fueltypeid,
    mwot.modelyearid,
    mwot.roadtypeid,
    mwot.scc,
    mwot.emissionquant * ff.fossilfraction
from 
    movesworkeroutputtemp mwot,
    fossilfraction ff,
    monthofanyyear may
where
    may.monthid = mwot.monthid and
    ff.countyid = mwot.countyid and
    ff.yearid = mwot.yearid and 
    ff.monthgroupid = may.monthgroupid and
    ff.fueltypeid = mwot.fueltypeid and
    mwot.processid = 91;
-- end section fossil fuel energy
-- end section auxiliary power exhaust

analyze table movesworkeroutput;

alter table sourcetypeenergy drop index xpksourcetypeenergy;
alter table sourcetypeenergy drop index xpksourcetypeenergy2;	
alter table acadjustment drop index xpkacadjustment;	
alter table meanbaseratebytype drop index xpkmeanbaseratebytype;
alter table tempadjustmentbytype drop index xpktempadjustmentbytype;

-- end section processing

-- section cleanup
drop table if exists aconfraction;
drop table if exists acactivityfraction;
drop table if exists acadjustment;
drop table if exists sourcetypeenergy;
drop table if exists omd2;
drop table if exists omdmbr;
drop table if exists meanbaseratebytype;
drop table if exists fueladjustmentbytype;
drop table if exists tempadjustmentbytype;
drop table if exists movesworkeroutputtemp;
-- section fossil fuel energy
drop table if exists fossilfraction;
-- end section fossil fuel energy
-- section petroleum energy
drop table if exists petroleumfraction;
-- end section petroleum energy
-- section running exhaust
-- end section running exhaust
-- section start exhaust
-- end section start exhaust
-- section extended idle exhaust
-- end section extended idle exhaust
-- end section cleanup
