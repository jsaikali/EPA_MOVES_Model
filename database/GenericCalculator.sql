-- version 2013-09-15

-- section create remote tables for extracted data
##create.agecategory##;
truncate table agecategory;

##create.county##;
truncate table county;

##create.hourday##;
truncate table hourday;

##create.link##;
truncate table link;

##create.zone##;
truncate table zone;

##create.pollutant##;
truncate table pollutant;

##create.emissionprocess##;
truncate table emissionprocess;

##create.emissionratebyage##;
truncate table emissionratebyage;

##create.opmodedistribution##;
truncate table opmodedistribution;

##create.sourcebin##;
truncate table sourcebin;

##create.sourcebindistribution##;
truncate table sourcebindistribution;

##create.sourcetypemodelyear##;
truncate table sourcetypemodelyear;

##create.pollutantprocessassoc##;
truncate table pollutantprocessassoc;

-- section running exhaust

##create.sho##;
truncate table sho;

-- end section running exhaust

-- end section create remote tables for extracted data

-- section extract data
cache select * into outfile '##agecategory##'
from agecategory;

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

cache select * 
into outfile '##emissionprocess##'
from emissionprocess
where processid=##context.iterprocess.databasekey##;

cache select * 
into outfile '##opmodedistribution##'
from opmodedistribution, runspecsourcetype
where polprocessid in (##pollutantprocessids##)
and linkid = ##context.iterlocation.linkrecordid##
and runspecsourcetype.sourcetypeid = opmodedistribution.sourcetypeid;

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
and sourcetypemodelyear.sourcetypeid = runspecsourcefueltype.sourcetypeid
and sourcebindistribution.sourcebinid = sourcebin.sourcebinid
and sourcebin.fueltypeid = runspecsourcefueltype.fueltypeid;

cache select distinct sourcebin.* 
into outfile '##sourcebin##'
from sourcebindistribution, sourcetypemodelyear, sourcebin, runspecsourcefueltype
where polprocessid in (##pollutantprocessids##)
and sourcebindistribution.sourcetypemodelyearid = sourcetypemodelyear.sourcetypemodelyearid
and sourcetypemodelyear.modelyearid <= ##context.year##
and sourcetypemodelyear.sourcetypeid = runspecsourcefueltype.sourcetypeid
and sourcebindistribution.sourcebinid = sourcebin.sourcebinid
and sourcebin.fueltypeid = runspecsourcefueltype.fueltypeid;

cache select distinct emissionratebyage.* 
into outfile '##emissionratebyage##'
from emissionratebyage, sourcebindistribution, sourcetypemodelyear, sourcebin, runspecsourcefueltype
where emissionratebyage.polprocessid in (##pollutantprocessids##)
and emissionratebyage.polprocessid = sourcebindistribution.polprocessid
and emissionratebyage.sourcebinid = sourcebindistribution.sourcebinid
and sourcebindistribution.sourcetypemodelyearid = sourcetypemodelyear.sourcetypemodelyearid
and sourcetypemodelyear.modelyearid <= ##context.year##
and sourcetypemodelyear.sourcetypeid = runspecsourcefueltype.sourcetypeid
and sourcebindistribution.sourcebinid = sourcebin.sourcebinid
and sourcebin.fueltypeid = runspecsourcefueltype.fueltypeid;

cache select sourcetypemodelyear.* 
into outfile '##sourcetypemodelyear##'
from sourcetypemodelyear,runspecsourcetype
where sourcetypemodelyear.sourcetypeid = runspecsourcetype.sourcetypeid
and modelyearid <= ##context.year##;

cache select distinct hourday.* 
into outfile '##hourday##'
from hourday,runspechour,runspecday
where hourday.dayid = runspecday.dayid
and hourday.hourid = runspechour.hourid;

cache select * 
into outfile '##pollutantprocessassoc##'
from pollutantprocessassoc
where processid=##context.iterprocess.databasekey##;

-- section running exhaust

select sho.* 
into outfile '##sho##'
from sho
where yearid = ##context.year##
and linkid = ##context.iterlocation.linkrecordid##;

-- end section running exhaust

-- end section extract data
--
-- section processing
--
-- section running exhaust

--
-- calculate the running emissions
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
    emissionquant)
select
	sho.yearid,
	sho.monthid,
	hd.dayid,
	hd.hourid,
	c.stateid,
	c.countyid,
	z.zoneid,
	sho.linkid,
	ppa.pollutantid,
	ppa.processid,
	sho.sourcetypeid,
	sb.fueltypeid,
	stmy.modelyearid,
	l.roadtypeid,
	sum(sbd.sourcebinactivityfraction * sho.sho * era.meanbaserate * omd.opmodefraction)
from
	sho sho,
	agecategory ac,
	sourcebindistribution sbd,
	emissionratebyage era,
	county c,
	zone z, 
	link l,
	pollutantprocessassoc ppa,
	emissionprocess ep,
	hourday hd,
	sourcetypemodelyear stmy,
	sourcebin sb,
	opmodedistribution omd
where
	sho.ageid = ac.ageid and
	ac.agegroupid = era.agegroupid and
	c.countyid = l.countyid and
	c.countyid = z.countyid and
	l.countyid = z.countyid and
	hd.hourdayid = omd.hourdayid and
	hd.hourdayid = sho.hourdayid and
	omd.hourdayid = sho.hourdayid and
	omd.isuserinput = sho.isuserinput and
	omd.isuserinput = sbd.isuserinput and
	sho.isuserinput = sbd.isuserinput and
	l.linkid = omd.linkid and
	l.linkid = sho.linkid and
	omd.linkid = sho.linkid and
	(sho.yearid - sho.ageid) = stmy.modelyearid and
	era.opmodeid = omd.opmodeid and
	era.polprocessid = omd.polprocessid and
	era.polprocessid = ppa.polprocessid and
	era.polprocessid = sbd.polprocessid and
	omd.polprocessid = ppa.polprocessid and
	omd.polprocessid = sbd.polprocessid and
	ppa.polprocessid = sbd.polprocessid and
	ep.processid = ppa.processid and
	era.sourcebinid = sb.sourcebinid and
	era.sourcebinid = sbd.sourcebinid and
	sb.sourcebinid = sbd.sourcebinid and
	omd.sourcetypeid = sho.sourcetypeid and
	omd.sourcetypeid = stmy.sourcetypeid and
	sho.sourcetypeid = stmy.sourcetypeid and
	sbd.sourcetypemodelyearid = stmy.sourcetypemodelyearid and
	l.zoneid = z.zoneid and
	ppa.pollutantid in (##pollutantids##)
group by
	sho.yearid,
	sho.monthid,
	hd.dayid,
	hd.hourid,
	c.stateid,
	c.countyid,
	z.zoneid,
	sho.linkid,
	ppa.pollutantid,
	ppa.processid,
	sho.sourcetypeid,
	sb.fueltypeid,
	stmy.modelyearid,
	l.roadtypeid;

-- end section running exhaust

-- end section processing
