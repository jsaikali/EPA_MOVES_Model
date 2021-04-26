-- version 2013-09-15

-- section create remote tables for extracted data
drop table if exists adjustfuelsupply;
create table adjustfuelsupply (
	monthid integer not null,
	fueltypeid integer not null,
	fuelformulationid integer not null,
	marketshare double null,
	
	key (monthid, fueltypeid, fuelformulationid),
	key (monthid, fuelformulationid, fueltypeid),
	key (fueltypeid, fuelformulationid, monthid),
	key (fueltypeid, monthid, fuelformulationid),
	key (fuelformulationid, monthid, fueltypeid),
	key (fuelformulationid, fueltypeid, monthid)
) engine=memory;
truncate table adjustfuelsupply;

##memory.create.county##;
truncate table county;

##memory.create.hourday##;
truncate table hourday;

##memory.create.link##;
truncate table link;

##memory.create.zone##;
truncate table zone;

##memory.create.pollutant##;
truncate table pollutant;

##memory.create.emissionprocess##;
truncate table emissionprocess;

##create.emissionrate##;
truncate table emissionrate;

##create.sourcebin##;
truncate table sourcebin;

##create.sourcebindistribution##;
truncate table sourcebindistribution;

##create.sourcetypemodelyear##;
truncate table sourcetypemodelyear;

##memory.create.pollutantprocessassoc##;
truncate table pollutantprocessassoc;

-- section running exhaust

##create.sho##;
truncate table sho;

-- end section running exhaust

-- section start exhaust

##create.starts##;
truncate table starts;

-- end section start exhaust

-- end section create remote tables for extracted data

-- section extract data

select * into outfile '##county##'
from county
where countyid = ##context.iterlocation.countyrecordid##;

cache select monthofanyyear.monthid, fuelsubtype.fueltypeid, fuelsupply.fuelformulationid, marketshare
into outfile '##adjustfuelsupply##'
from fuelsupply
inner join runspecmonthgroup on (fuelsupply.monthgroupid = runspecmonthgroup.monthgroupid)
inner join year on (fuelsupply.fuelyearid = year.fuelyearid)
inner join fuelformulation on (fuelformulation.fuelformulationid = fuelsupply.fuelformulationid)
inner join fuelsubtype on (fuelsubtype.fuelsubtypeid = fuelformulation.fuelsubtypeid)
inner join monthofanyyear on (monthofanyyear.monthgroupid = fuelsupply.monthgroupid)
where fuelsupply.fuelregionid = ##context.fuelregionid##
and yearid = ##context.year##
and monthofanyyear.monthid = ##context.monthid##;

select * into outfile '##zone##'
from zone
where zoneid = ##context.iterlocation.zonerecordid##;

select link.*
into outfile '##link##'
from link
where linkid = ##context.iterlocation.linkrecordid##;

cache select * 
into outfile '##emissionprocess##'
from emissionprocess
where processid=##context.iterprocess.databasekey##;

cache select *
into outfile '##pollutant##'
from pollutant;

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
from sourcebindistributionfuelusage_##context.iterprocess.databasekey##_##context.iterlocation.countyrecordid##_##context.year## as sourcebindistribution, 
sourcetypemodelyear
where polprocessid in (##pollutantprocessids##)
and sourcebindistribution.sourcetypemodelyearid = sourcetypemodelyear.sourcetypemodelyearid
and sourcetypemodelyear.modelyearid <= ##context.year##
and sourcetypemodelyear.modelyearid >= ##context.year## - 30;

-- select distinct sourcebin.* 
-- into outfile '??sourcebin??'
-- from sourcebindistribution, sourcetypemodelyear, sourcebin, runspecsourcefueltype
-- where polprocessid in (??pollutantprocessids??)
-- and sourcebindistribution.sourcetypemodelyearid = sourcetypemodelyear.sourcetypemodelyearid
-- and sourcetypemodelyear.modelyearid <= ??context.year??
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

-- select distinct emissionrate.* 
-- into outfile '??emissionrate??'
-- from emissionrate, sourcebindistribution, sourcetypemodelyear, sourcebin, runspecsourcefueltype
-- where emissionrate.polprocessid in (??pollutantprocessids??)
-- and emissionrate.polprocessid = sourcebindistribution.polprocessid
-- and emissionrate.sourcebinid = sourcebindistribution.sourcebinid
-- and sourcebindistribution.sourcetypemodelyearid = sourcetypemodelyear.sourcetypemodelyearid
-- and sourcetypemodelyear.modelyearid <= ??context.year??
-- and sourcetypemodelyear.sourcetypeid = runspecsourcefueltype.sourcetypeid
-- and sourcebindistribution.sourcebinid = sourcebin.sourcebinid
-- and sourcebin.fueltypeid = runspecsourcefueltype.fueltypeid;

cache select emissionrate.* into outfile '##emissionrate##'
from emissionrate
where emissionrate.polprocessid in (##pollutantprocessids##)
and emissionrate.sourcebinid in (##macro.csv.all.sourcebinid##);

cache select sourcetypemodelyear.* into outfile '##sourcetypemodelyear##'
from sourcetypemodelyear
where sourcetypemodelyear.sourcetypeid in (##macro.csv.all.sourcetypeid##)
and modelyearid <= ##context.year##
and modelyearid >= ##context.year## - 30;

cache select hourday.* into outfile '##hourday##'
from hourday
where hourdayid in (##macro.csv.all.hourdayid##);

cache select * into outfile '##pollutantprocessassoc##'
from pollutantprocessassoc
where processid=##context.iterprocess.databasekey##;

-- section running exhaust

select sho.* 
into outfile '##sho##'
from sho
where yearid = ##context.year##
and linkid = ##context.iterlocation.linkrecordid##
and monthid = ##context.monthid##;

-- end section running exhaust

-- section start exhaust

select starts.* 
into outfile '##starts##'
from starts
where yearid = ##context.year##
and zoneid = ##context.iterlocation.zonerecordid##
and monthid = ##context.monthid##;

-- end section start exhaust

-- end section extract data
--
-- section processing

drop table if exists movesworkeroutputtemp;
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


--
-- section running exhaust
--
-- calculate the running emissions
--  this section originally contained a single sql statement joining 14 tables
--   it was rewritten 10/1/2004 as a series of equivalent statements to improve performance.
--  

drop table if exists sho2 ;
drop table if exists link2 ;
drop table if exists sho3 ;
drop table if exists emissionrate2 ;
drop table if exists sourcebindistribution2 ;
drop table if exists sourcebindistribution3 ;
drop table if exists emissionrate3 ;
drop table if exists workeroutputbysourcetype ;
create table sho2 (
	yearid smallint,
	monthid smallint,
	hourid smallint,
	dayid smallint,
	linkid integer,
	sourcetypeid smallint,
	modelyearid smallint,
	sho float) ;
insert into sho2 
	select sho.yearid, sho.monthid, hd.hourid, hd.dayid, sho.linkid, 
		sho.sourcetypeid, (sho.yearid - sho.ageid), sho.sho
	from sho as sho inner join hourday as hd using(hourdayid);
create table link2 (
	stateid smallint,
	countyid integer,
	zoneid integer,
	linkid integer,
	roadtypeid smallint) engine=memory;
insert into link2
	select	c.stateid, l.countyid, l.zoneid, l.linkid, l.roadtypeid 
	from county as c inner join link as l using (countyid);
create table sho3
	select sho.*, l.stateid, l.countyid, l.zoneid, l.roadtypeid 
	from sho2 as sho inner join link2 as l using(linkid) ;
create table emissionrate2
	select er.*, ppa.pollutantid, ppa.processid 
	from emissionrate as er inner join pollutantprocessassoc as ppa 
	using (polprocessid)  
	where ppa.pollutantid = 6 and er.opmodeid >= 0 and er.opmodeid < 100;
create table sourcebindistribution2
	select sbd.*, sb.fueltypeid
	from sourcebindistribution as sbd inner join sourcebin as sb using (sourcebinid);
create table sourcebindistribution3
	select sbd.*, stmy.sourcetypeid, stmy.modelyearid
	from sourcebindistribution2 as sbd inner join sourcetypemodelyear as stmy
	using (sourcetypemodelyearid);
create index index1 on emissionrate2 (polprocessid, sourcebinid);
create index index1 on sourcebindistribution3 (polprocessid, sourcebinid);
create table emissionrate3 (
	sourcetypeid smallint,
	fueltypeid smallint,
	modelyearid smallint,
	sourcetypemodelyearid integer,
	pollutantid smallint,
	processid smallint,
	sbafxmbr float );
insert into emissionrate3
	select sbd.sourcetypeid, sbd.fueltypeid, sbd.modelyearid, sbd.sourcetypemodelyearid,
		er.pollutantid, er.processid, 
		sum(sbd.sourcebinactivityfraction * er.meanbaserate) as sbafxmbr
	from sourcebindistribution3 as sbd inner join emissionrate2 as er
			using (polprocessid, sourcebinid) 
	group by sbd.sourcetypemodelyearid, sbd.fueltypeid, er.pollutantid, er.processid;
create index index1 on sho3 (sourcetypeid, modelyearid);
create index index1 on emissionrate3 (sourcetypeid, modelyearid);
create table workeroutputbysourcetype
	select sho.*, er.fueltypeid, er.pollutantid, er.processid, 
		er.sourcetypemodelyearid, er.sbafxmbr
	from sho3 as sho inner join emissionrate3 as er 
	  using (sourcetypeid, modelyearid);
create index index1 on workeroutputbysourcetype (sourcetypemodelyearid, fueltypeid);

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
	wobst.yearid,
	wobst.monthid,
	wobst.dayid,
	wobst.hourid,
	wobst.stateid,
	wobst.countyid,
	wobst.zoneid,
	wobst.linkid,
	wobst.pollutantid,
	wobst.processid,
	wobst.sourcetypeid,
	wobst.fueltypeid,
	wobst.modelyearid,
	wobst.roadtypeid,
	(wobst.sbafxmbr * wobst.sho)
from 
	workeroutputbysourcetype as wobst;

-- end section running exhaust

--
-- section start exhaust
--
--
-- calculate the start emissions
--

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
	st.yearid,
	st.monthid,
	hd.dayid,
	hd.hourid,
	c.stateid,
	c.countyid,
	z.zoneid,
	l.linkid,
	ppa.pollutantid,
	ppa.processid,
	st.sourcetypeid,
	sb.fueltypeid,
	stmy.modelyearid,
	l.roadtypeid,
	sum(sbd.sourcebinactivityfraction * st.starts * er.meanbaserate)
from
	sourcebindistribution sbd,
	starts st,
	emissionrate er,
	county c,
	zone z, 
	link l,
	pollutantprocessassoc ppa,
	emissionprocess ep,
	hourday hd,
	sourcetypemodelyear stmy,
	sourcebin sb
where
	sbd.sourcetypemodelyearid = stmy.sourcetypemodelyearid and
	sbd.polprocessid = ppa.polprocessid and
	sbd.polprocessid = er.polprocessid and
	sbd.sourcebinid = er.sourcebinid and
	sbd.sourcebinid = sb.sourcebinid and
	st.hourdayid = hd.hourdayid and
	st.ageid = (st.yearid - stmy.modelyearid) and
	st.zoneid = z.zoneid and
	st.zoneid = l.zoneid and
	st.sourcetypeid = stmy.sourcetypeid and
	er.sourcebinid = sb.sourcebinid and
	er.polprocessid = ppa.polprocessid and
	er.opmodeid = 100 and
	c.countyid = l.countyid and
	c.countyid = z.countyid and
	z.zoneid = l.zoneid and
	ppa.pollutantid = 6 and
	ppa.processid = ep.processid and
	sbd.sourcebinid = sb.sourcebinid
group by
	st.yearid,
	st.monthid,
	hd.dayid,
	hd.hourid,
	c.stateid,
	c.countyid,
	z.zoneid,
	l.linkid,
	ppa.pollutantid,
	ppa.processid,
	st.sourcetypeid,
	sb.fueltypeid,
	stmy.modelyearid,
	l.roadtypeid;

-- end section start exhaust

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
select yearid,
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
from movesworkeroutputtemp;

-- end section processing

-- section cleanup
drop table if exists movesworkeroutputtemp;
drop table if exists sho2 ;
drop table if exists link2 ;
drop table if exists sho3 ;
drop table if exists emissionrate2 ;
drop table if exists sourcebindistribution2 ;
drop table if exists sourcebindistribution3 ;
drop table if exists emissionrate3 ;
drop table if exists workeroutputbysourcetype ;
-- end section cleanup
