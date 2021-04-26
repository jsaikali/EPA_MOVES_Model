-- version 2014-06-10

-- @algorithm
-- @owner distance calculator

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

-- end section create remote tables for extracted data

-- section extract data
select * into outfile '##county##'
from county
where countyid = ##context.iterlocation.countyrecordid##;

select link.*
into outfile '##link##'
from link
where linkid = ##context.iterlocation.linkrecordid##;

select * 
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

select distinct sho.* 
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

cache select distinct hourday.* 
into outfile '##hourday##'
from hourday,runspechour,runspecday
where hourday.dayid = runspecday.dayid
and hourday.hourid = runspechour.hourid;

-- end section extract data
--
-- section processing
--

truncate movesworkeractivityoutput;
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

-- @algorithm fueltypeactivityfraction[sourcetypeid,modelyearid,regclassid,fueltypeid] = sum(sourcebinactivityfraction[processid=1,pollutantid=any,sourcebinid[fueltypeid,engtechid,regclassid,modelyeargroupid,engsizeid]]).
create table sbd2 (
	sourcetypemodelyearid integer,
	regclassid smallint,
	fueltypeid smallint,
	fueltypeactivityfraction float);
insert into sbd2 (
	sourcetypemodelyearid,regclassid,fueltypeid,fueltypeactivityfraction )
	select sbd.sourcetypemodelyearid,sb.regclassid,sb.fueltypeid,
		sum(sbd.sourcebinactivityfraction)
	from sourcebindistribution as sbd inner join sourcebin as sb
	using (sourcebinid)
	group by sourcetypemodelyearid, regclassid, fueltypeid;
create index index1 on sbd2 (sourcetypemodelyearid, fueltypeid);

-- @algorithm add sourcetypemodelyearid to fueltypeactivityfraction's dimensions.
create table distfracts
	select stmy.sourcetypemodelyearid, stmy.sourcetypeid, sbd.regclassid, stmy.modelyearid, sbd.fueltypeid,
	sbd.fueltypeactivityfraction
	from sourcetypemodelyear as stmy inner join sbd2 as sbd using (sourcetypemodelyearid);

-- @algorithm add modelyearid to sho's dimensions.
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

-- @algorithm distance = distance[sourcetypeid,yearid,monthid,hourdayid,ageid,linkid]*fueltypeactivityfraction.
insert into movesworkeractivityoutput (
	yearid,monthid,dayid,hourid,
	stateid,countyid,zoneid,linkid,
	regclassid,sourcetypeid,fueltypeid,modelyearid,
	roadtypeid,
	activitytypeid,
	activity) 
select
	sho.yearid,sho.monthid,sho.dayid,sho.hourid,
	sho.stateid,sho.countyid,sho.zoneid,sho.linkid,
	df.regclassid,sho.sourcetypeid,df.fueltypeid,sho.modelyearid,
	sho.roadtypeid,
	1,
	(sho.distance * df.fueltypeactivityfraction) as activity
from distfracts as df 
inner join sho3 as sho using (sourcetypeid, modelyearid);

-- end section processing

-- section cleanup
drop table if exists sbd2;
drop table if exists svtd2;
drop table if exists svtd3;
drop table if exists distfracts;
drop table if exists sho2;
drop table if exists link2;
drop table if exists sho3;
-- end section cleanup
