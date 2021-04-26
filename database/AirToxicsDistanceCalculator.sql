-- author wesley faler
-- version 2013-09-15

-- @notused

-- section create remote tables for extracted data
##create.county##;
truncate table county;

##create.hourday##;
truncate table hourday;

##create.link##;
truncate table link;

##create.sourcebin##;
truncate table sourcebin;

##create.sourcebindistribution##;
truncate table sourcebindistribution;

##create.sourcetypemodelyear##;
truncate table sourcetypemodelyear;

##create.emissionprocess##;
truncate table emissionprocess;

##create.sho##;
truncate table sho;

-- section usedioxinemissionrate
create table dioxinemissionrate (
  processid smallint(6) not null,
  pollutantid smallint(6) not null,
  fueltypeid smallint(6) not null default '0',
  modelyearid smallint(6) not null,
  meanbaserate double default null,
  primary key (fueltypeid,modelyearid,processid,pollutantid)
);

truncate table dioxinemissionrate;
-- end section usedioxinemissionrate

-- section usemetalemissionrate
create table metalemissionrate (
  processid smallint(6) not null,
  pollutantid smallint(6) not null,
  fueltypeid smallint(6) not null default '0',
  sourcetypeid smallint(6) not null default '0',
  modelyearid smallint(6) not null,
  meanbaserate double default null,
  primary key (sourcetypeid,fueltypeid,modelyearid,processid,pollutantid)
);

truncate table metalemissionrate;
-- end section usemetalemissionrate

-- end section create remote tables for extracted data

-- section extract data
cache select * into outfile '##county##'
from county
where countyid = ##context.iterlocation.countyrecordid##;

cache select link.*
into outfile '##link##'
from link
where linkid = ##context.iterlocation.linkrecordid##;

cache select * 
into outfile '##emissionprocess##'
from emissionprocess
where processid=##context.iterprocess.databasekey##;

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

cache select distinct sho.* 
into outfile '##sho##'
from sho, runspecmonth, runspecday, runspechour, hourday
where yearid = ##context.year##
and sho.linkid = ##context.iterlocation.linkrecordid##
and sho.monthid=runspecmonth.monthid
and sho.hourdayid = hourday.hourdayid
and hourday.dayid = runspecday.dayid
and hourday.hourid = runspechour.hourid;

cache select sourcetypemodelyear.* 
into outfile '##sourcetypemodelyear##'
from sourcetypemodelyear,runspecsourcetype
where sourcetypemodelyear.sourcetypeid = runspecsourcetype.sourcetypeid
and modelyearid <= ##context.year##
and modelyearid >= ##context.year## - 30;

select distinct hourday.* 
into outfile '##hourday##'
from hourday,runspechour,runspecday
where hourday.dayid = runspecday.dayid
and hourday.hourid = runspechour.hourid;

-- section usedioxinemissionrate

-- native distance units are miles
-- native mass units are grams
-- native energy units are kilojoules

cache select ppa.processid, ppa.pollutantid, r.fueltypeid, modelyearid, 
	meanbaserate * (
		case units when 'g/mile' then 1.0
			when 'g/km' then 1.609344
			when 'teq/mile' then 1.0
			when 'teq/km' then 1.609344
			else 1.0
		end
	) as meanbaserate
into outfile '##dioxinemissionrate##'
from dioxinemissionrate r
inner join pollutantprocessassoc ppa using (polprocessid)
inner join modelyear my on (
	mymap(modelyearid) >= round(modelyeargroupid/10000,0)
	and mymap(modelyearid) <= mod(modelyeargroupid,10000)
	and modelyearid <= ##context.year##
	and modelyearid >= ##context.year## - 30
)
where polprocessid in (##outputdioxinemissionrate##);
-- end section usedioxinemissionrate

-- section usemetalemissionrate

-- native distance units are miles
-- native mass units are grams
-- native energy units are kilojoules

cache select ppa.processid, ppa.pollutantid, r.fueltypeid, r.sourcetypeid, modelyearid,
	meanbaserate * (
		case units when 'g/mile' then 1.0
			when 'g/km' then 1.609344
			when 'TEQ/mile' then 1.0
			when 'TEQ/km' then 1.609344
			else 1.0
		end
	) as meanbaserate
into outfile '##metalemissionrate##'
from metalemissionrate r
inner join runspecsourcefueltype rs on (
	rs.sourcetypeid = r.sourcetypeid
	and rs.fueltypeid = r.fueltypeid)
inner join pollutantprocessassoc ppa using (polprocessid)
inner join modelyear my on (
	mymap(modelyearid) >= round(modelyeargroupid/10000,0)
	and mymap(modelyearid) <= mod(modelyeargroupid,10000)
	and modelyearid <= ##context.year##
	and modelyearid >= ##context.year## - 30
)
where polprocessid in (##outputmetalemissionrate##);
-- end section usemetalemissionrate

-- end section extract data
--
-- section processing
--

drop table if exists atactivityoutput;
create table atactivityoutput like movesworkeractivityoutput;

--
-- calculate the distance
--
drop table if exists sbd2;
drop table if exists svtd2;
drop table if exists svtd3;
drop table if exists distfracts;
drop table if exists sho2;
drop table if exists link2;
drop table if exists sho3;

create table sbd2 (
	sourcetypemodelyearid integer,
	fueltypeid smallint,
	fueltypeactivityfraction float);
insert into sbd2 (
	sourcetypemodelyearid,fueltypeid,fueltypeactivityfraction )
	select sbd.sourcetypemodelyearid,sb.fueltypeid,
		sum(sbd.sourcebinactivityfraction)
	from sourcebindistribution as sbd inner join sourcebin as sb
	using (sourcebinid)
	group by sourcetypemodelyearid, fueltypeid;
create index index1 on sbd2 (sourcetypemodelyearid, fueltypeid);

create table distfracts
	select stmy.sourcetypemodelyearid, stmy.sourcetypeid, stmy.modelyearid, sbd.fueltypeid,
	sbd.fueltypeactivityfraction
	from sourcetypemodelyear as stmy inner join sbd2 as sbd using (sourcetypemodelyearid);
	
create table sho2 (
	yearid smallint, 
	monthid smallint,
	dayid smallint,
	hourid smallint,
	modelyearid smallint,
	linkid integer,
	sourcetypeid smallint,
	distance float);
insert into sho2
	select sho.yearid, sho.monthid, hd.dayid, hd.hourid, (sho.yearid - sho.ageid), 
		sho.linkid, sho.sourcetypeid, sho.distance
	from sho as sho inner join hourday as hd using (hourdayid);
	
create table link2
	select link.*, c.stateid
	from link as link inner join county as c using (countyid);
	
create index index1 on sho2 (linkid);
create table sho3
	select sho.*, link.stateid, link.countyid, link.zoneid, link.roadtypeid 
	from sho2 as sho inner join link2 as link using (linkid);
	
create index index1 on sho3 (sourcetypeid, modelyearid, roadtypeid);

insert into atactivityoutput (
    yearid,
    monthid,
    dayid,
    hourid,
    stateid,
    countyid,
    zoneid,
    linkid,
    sourcetypeid,
    fueltypeid,
    modelyearid,
    roadtypeid,
    activitytypeid,
    activity) 
select
	sho.yearid,
	sho.monthid,
	sho.dayid,
	sho.hourid,
	sho.stateid,
	sho.countyid,
	sho.zoneid,
	sho.linkid,
	sho.sourcetypeid,
	df.fueltypeid,
	sho.modelyearid,
	sho.roadtypeid,
	1,
	(sho.distance * df.fueltypeactivityfraction) as activity
from distfracts as df inner join sho3 as sho using (sourcetypeid, modelyearid);

-- section usedioxinemissionrate
insert into movesworkeroutput (
	processid, pollutantid,
	yearid,monthid,dayid,hourid,stateid,countyid,zoneid,linkid,sourcetypeid,fueltypeid,modelyearid,roadtypeid,scc,
	emissionquant) 
select
	r.processid, r.pollutantid,
	yearid,monthid,dayid,hourid,stateid,countyid,zoneid,linkid,sourcetypeid,a.fueltypeid,a.modelyearid,roadtypeid,scc,
	(activity*meanbaserate) as emissionquant
from atactivityoutput a
inner join dioxinemissionrate r on (
	r.fueltypeid = a.fueltypeid
	and r.modelyearid = a.modelyearid
);
-- end section usedioxinemissionrate

-- section usemetalemissionrate
insert into movesworkeroutput (
	processid, pollutantid,
	yearid,monthid,dayid,hourid,stateid,countyid,zoneid,linkid,sourcetypeid,fueltypeid,modelyearid,roadtypeid,scc,
	emissionquant) 
select
	r.processid, r.pollutantid,
	yearid,monthid,dayid,hourid,stateid,countyid,zoneid,linkid,a.sourcetypeid,a.fueltypeid,a.modelyearid,roadtypeid,scc,
	(activity*meanbaserate) as emissionquant
from atactivityoutput a
inner join metalemissionrate r on (
	r.sourcetypeid = a.sourcetypeid
	and r.fueltypeid = a.fueltypeid
	and r.modelyearid = a.modelyearid
);
-- end section usemetalemissionrate

-- end section processing

-- section cleanup
drop table if exists atactivityoutput;
drop table if exists sbd2;
drop table if exists svtd2;
drop table if exists svtd3;
drop table if exists distfracts;
drop table if exists sho2;
drop table if exists link2;
drop table if exists sho3;
-- end section cleanup
