-- version 2013-09-15
-- authors: gwo s. and wes f.
-- the ammonia (nh3) calculator shall not have the ability to calculate fuel formulation effects, 
-- temperature effects, ac on effects or humidity effects.

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

##create.hourday##;
truncate hourday;

##create.imcoverage##;
truncate imcoverage;

##create.imfactor##;
truncate imfactor;

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

##create.runspechour##;
truncate runspechour;

##create.runspecmonth##;
truncate runspecmonth;

##create.sourcebin##;
truncate sourcebin;

##create.sourcebindistribution##;
truncate sourcebindistribution;

##create.sourcetypemodelyear##;
truncate sourcetypemodelyear;

##create.starttempadjustment##;
truncate table starttempadjustment;

##create.starts##;
truncate table starts;

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

cache select monthofanyyear.* into outfile '##monthofanyyear##'
from monthofanyyear,runspecmonth
where monthofanyyear.monthid = runspecmonth.monthid;

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

cache select * into outfile '##runspechour##' from runspechour;

cache select * into outfile '##runspecmonth##' from runspecmonth;

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

cache select sourcetypemodelyear.* into outfile '##sourcetypemodelyear##'
from sourcetypemodelyear,runspecsourcetype
where sourcetypemodelyear.sourcetypeid = runspecsourcetype.sourcetypeid
and modelyearid <= ##context.year##
and modelyearid >= ##context.year## - 30;

cache select starts.* into outfile '##starts##'
from starts
where yearid = ##context.year##
and zoneid = ##context.iterlocation.zonerecordid##;

cache select starttempadjustment.* into outfile '##starttempadjustment##'
from starttempadjustment
where polprocessid in (##pollutantprocessids##);

cache select year.* into outfile '##year##'
from year
where yearid = ##context.year##;

cache select * into outfile '##zone##'
from zone
where zoneid = ##context.iterlocation.zonerecordid##;

cache select distinct zonemonthhour.* into outfile '##zonemonthhour##'
from zonemonthhour,runspecmonth,runspechour
where zoneid = ##context.iterlocation.zonerecordid##
and runspecmonth.monthid = zonemonthhour.monthid
and runspechour.hourid = zonemonthhour.hourid;

-- end section extract data
--
-- section processing

--
-- nh3sec 1: complete i/m adjustment fraction information
--
drop table if exists imcoveragemergedungrouped;
create table imcoveragemergedungrouped (
       polprocessid int not null,
       pollutantid  smallint not null,
       processid  smallint not null,
       modelyearid smallint not null,
       fueltypeid smallint not null,
       sourcetypeid smallint not null,
       imadjustfract float,
       weightfactor float
);


insert into imcoveragemergedungrouped (
 polprocessid, pollutantid, processid, modelyearid,
 fueltypeid,sourcetypeid,imadjustfract,weightfactor)
select
 ppmy.polprocessid, 
 0, 
 0, 
 ppmy.modelyearid,
 imf.fueltypeid,
 imc.sourcetypeid,
 sum(imfactor*compliancefactor*.01) as imadjustfract,
 sum(compliancefactor) as weightfactor
from 
pollutantprocessmappedmodelyear ppmy
inner join imfactor imf on (
	imf.polprocessid = ppmy.polprocessid
	and imf.immodelyeargroupid = ppmy.immodelyeargroupid)
inner join agecategory ac on (
	ac.agegroupid = imf.agegroupid)
inner join imcoverage imc on (
	imc.polprocessid = imf.polprocessid
	and imc.inspectfreq = imf.inspectfreq
	and imc.teststandardsid = imf.teststandardsid
	and imc.sourcetypeid = imf.sourcetypeid
	and imc.fueltypeid = imf.fueltypeid
	and imc.begmodelyearid <= ##context.year##-ageid
	and imc.endmodelyearid >= ##context.year##-ageid)
where imc.countyid = ##context.iterlocation.countyrecordid##
and imc.yearid = ##context.year##
and ppmy.modelyearid = ##context.year##-ageid
and ppmy.polprocessid in (##pollutantprocessids##)
group by ppmy.polprocessid, 
 ppmy.modelyearid,
 imf.fueltypeid,
 imc.sourcetypeid;

update imcoveragemergedungrouped, pollutantprocessassoc
set imcoveragemergedungrouped.pollutantid=pollutantprocessassoc.pollutantid, 
    imcoveragemergedungrouped.processid=pollutantprocessassoc.processid 
where imcoveragemergedungrouped.polprocessid=pollutantprocessassoc.polprocessid
; 


create index xpkimcoveragemergedungrouped on imcoveragemergedungrouped
(
       polprocessid asc,
       pollutantid asc,
       processid asc,
       modelyearid asc,
       fueltypeid asc,
       sourcetypeid asc
);

--
-- nh3sec-2: weight emission rates by source bin.
--

drop table if exists sourcebinemissionrates0;
create table sourcebinemissionrates0 (
	zoneid int not null,
	yearid smallint not null,
	polprocessid int not null,
	sourcetypeid smallint not null,
	modelyearid smallint not null,
	fueltypeid smallint not null,
	opmodeid smallint not null,
	meanbaserate float,
	meanbaserateim float
);

alter table sourcebinemissionrates0 add index sourcebinemissionrates0 (
	zoneid, yearid, polprocessid, sourcetypeid, modelyearid, fueltypeid, opmodeid);

insert into sourcebinemissionrates0 (
      zoneid, yearid, polprocessid,
      sourcetypeid, modelyearid, fueltypeid, opmodeid,
	  meanbaserate, meanbaserateim)
select 
      ##context.iterlocation.zonerecordid## as zoneid,
      ##context.year## as yearid,
      er.polprocessid, 
      stmy.sourcetypeid, stmy.modelyearid, sb.fueltypeid, er.opmodeid,
	  sum(meanbaserate*sourcebinactivityfraction) as meanbaserate,
	  sum(meanbaserateim*sourcebinactivityfraction) as meanbaserateim
from emissionratebyage er
inner join agecategory age on (age.agegroupid=er.agegroupid)
inner join sourcetypemodelyear stmy on (stmy.modelyearid=##context.year##-age.ageid)
inner join sourcebindistribution sbd on (sbd.sourcetypemodelyearid=stmy.sourcetypemodelyearid
	and sbd.polprocessid=er.polprocessid and sbd.sourcebinid=er.sourcebinid)
inner join sourcebin sb on (sbd.sourcebinid=sb.sourcebinid) 
group by er.polprocessid, stmy.sourcetypeid, stmy.modelyearid, sb.fueltypeid, er.opmodeid
order by null;


drop table if exists sourcebinemissionrates;
create table sourcebinemissionrates (
	zoneid int not null,
	monthid smallint not null,
	hourid smallint not null,
	yearid smallint not null,
	polprocessid int not null,
	sourcetypeid smallint not null,
	modelyearid smallint not null,
	fueltypeid smallint not null,
	opmodeid smallint not null,
	meanbaserate float,
	meanbaserateim float
);

alter table sourcebinemissionrates add index sourcebinemissionrates1 (
	zoneid, monthid, hourid, yearid, polprocessid, sourcetypeid, modelyearid, fueltypeid, opmodeid);

insert into sourcebinemissionrates (
      zoneid, monthid, hourid, yearid, polprocessid,
      sourcetypeid, modelyearid, fueltypeid, opmodeid,
	  meanbaserate, meanbaserateim)
select 
      er.zoneid,
      rm.monthid,
      rh.hourid,
      er.yearid,
      er.polprocessid, 
      er.sourcetypeid, 
      er.modelyearid, 
      er.fueltypeid, 
      er.opmodeid,
	    er.meanbaserate,
	    er.meanbaserateim
from sourcebinemissionrates0 er, runspecmonth rm, runspechour rh
order by null;


--
-- nh3sec-3: weight adjusted emission rates by operating mode.
--
drop table if exists activityweightedemissionrate;
create table activityweightedemissionrate (
	zoneid int not null,
	yearid smallint not null,
	monthid smallint not null,
	dayid smallint not null,
	hourid smallint not null,
	polprocessid int not null,
	sourcetypeid smallint not null,
	modelyearid smallint not null,
	fueltypeid smallint not null,
	meanbaserate float,
	meanbaserateim float
);

create unique index xpkactivityweightedemissionrate on activityweightedemissionrate (
	zoneid asc,
	yearid asc,
	monthid asc,
	dayid asc,
	hourid asc,
	polprocessid asc,
	sourcetypeid asc,
	modelyearid asc,
	fueltypeid asc
);

insert into activityweightedemissionrate (
      zoneid, yearid, monthid, dayid, hourid, polprocessid,
      sourcetypeid, modelyearid, fueltypeid, meanbaserate, meanbaserateim )
select 
      zoneid, ##context.year##, monthid, hd.dayid, msber.hourid, msber.polprocessid,
      msber.sourcetypeid, modelyearid, fueltypeid, 
      sum(meanbaserate * opmodefraction), sum(meanbaserateim * opmodefraction)
from sourcebinemissionrates msber
inner join hourday hd on (hd.hourid = msber.hourid)
inner join opmodedistribution omd on (
omd.sourcetypeid = msber.sourcetypeid
and omd.hourdayid = hd.hourdayid 
and omd.polprocessid = msber.polprocessid 
and omd.opmodeid = msber.opmodeid)
group by zoneid, yearid, monthid, hd.dayid, msber.hourid, msber.polprocessid, 
msber.sourcetypeid, modelyearid, fueltypeid
order by null;

-- 
-- nh3sec-4: multiply emission rates by start activity to generate inventory.
--
-- make version of starts table that is optimized to subsequent steps

drop table if exists starts2;
create table starts2 (
	zoneid int not null,
	monthid smallint not null,
	hourid smallint not null,
	dayid smallint not null,
	yearid smallint not null,
	sourcetypeid smallint not null,
	modelyearid smallint not null,
	starts float
);

create unique index xpkstarts2 on starts2 (
      zoneid asc,
      monthid asc,
      hourid asc,
	  dayid asc,
      yearid asc,
      sourcetypeid asc,
      modelyearid asc
);

insert into starts2 (zoneid, monthid, hourid, dayid, yearid, 
	sourcetypeid, modelyearid, starts) 
select zoneid, monthid, hourid, dayid, yearid, sourcetypeid, 
	(##context.year## - ageid) as modelyearid, starts
from starts inner join hourday on (starts.hourdayid= hourday.hourdayid);

alter table movesworkeroutput add emissionquantim float null default 0.0;

insert into movesworkeroutput (
	stateid, countyid, zoneid, linkid, roadtypeid, yearid, monthid, dayid,
      hourid, pollutantid, processid, sourcetypeid, modelyearid, fueltypeid,
	scc, emissionquant, emissionquantim)
select
  ##context.iterlocation.staterecordid##, 
  ##context.iterlocation.countyrecordid##,
  s.zoneid, 
  ##context.iterlocation.linkrecordid##, 
  1 as roadtypeid,
  s.yearid, s.monthid, s.dayid, s.hourid, pollutantid, processid, 
  s.sourcetypeid, s.modelyearid, fueltypeid, null as scc, 
  (meanbaserate * starts) as emissionquant,
  (meanbaserateim * starts) as emissionquantim
from starts2 s, activityweightedemissionrate awer, pollutantprocessassoc ppa
where
     s.zoneid=awer.zoneid and
     s.monthid=awer.monthid and
     s.hourid=awer.hourid and
     s.dayid=awer.dayid and
     s.yearid=awer.yearid and
     s.sourcetypeid=awer.sourcetypeid and
     s.modelyearid=awer.modelyearid and
     awer.polprocessid=ppa.polprocessid;

-- apply im
update movesworkeroutput, imcoveragemergedungrouped 
set emissionquant=greatest(emissionquantim*imadjustfract + emissionquant*(1.0-imadjustfract),0.0)
where movesworkeroutput.processid = imcoveragemergedungrouped.processid
	and movesworkeroutput.pollutantid = imcoveragemergedungrouped.pollutantid
	and movesworkeroutput.modelyearid = imcoveragemergedungrouped.modelyearid
	and movesworkeroutput.fueltypeid = imcoveragemergedungrouped.fueltypeid
	and movesworkeroutput.sourcetypeid = imcoveragemergedungrouped.sourcetypeid;

alter table movesworkeroutput drop emissionquantim;

-- end section processing

-- section cleanup
drop table if exists activityweightedemissionrate;
drop table if exists countyfueladjustment;
drop table if exists countyfueladjustmentwithfueltype;
drop table if exists emissionrateswithim;
drop table if exists fuelsupplyadjustment;
drop table if exists imcoveragemergedungrouped;
drop table if exists imcoveragemerged;
drop table if exists imadjustment;
drop table if exists imadjustmentwithsourcebin;
drop table if exists sourcebinemissionrates0;
drop table if exists sourcebinemissionrates;
drop table if exists starts2;
flush tables;
-- end section cleanup

