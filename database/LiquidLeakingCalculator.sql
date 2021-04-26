-- version 2014-04-28
-- author wesley faler

-- @algorithm
-- @owner liquid leaking calculator
-- @calculator

-- section create remote tables for extracted data

##create.agecategory##;
truncate agecategory;

##create.county##;
truncate county;

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

##create.pollutantprocessmappedmodelyear##;
truncate pollutantprocessmappedmodelyear;

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

##create.year##;
truncate year;

##create.zone##;
truncate zone;

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

cache select * into outfile '##county##'
from county
where countyid = ##context.iterlocation.countyrecordid##;

-- @algorithm filter emissionratebyage to only operating modes 150, 151, and 300.
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
and emissionratebyage.opmodeid in (150, 151, 300);

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

-- @algorithm filter opmodedistribution to only operating modes 150, 151, and 300.
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

cache select year.* into outfile '##year##'
from year
where yearid = ##context.year##;

cache select * into outfile '##zone##'
from zone
where zoneid = ##context.iterlocation.zonerecordid##;

-- section withregclassid
cache select *
into outfile '##regclasssourcetypefraction##'
from regclasssourcetypefraction
where modelyearid <= ##context.year##
and modelyearid >= ##context.year## - 30;
-- end section withregclassid

-- insert into eventlog (eventtime, eventname) select now(), 'END EXTRACTING DATA';

-- end section extract data

-- section processing

-- create tables needed for processing
-- create table if not exists eventlog (eventrowid integer unsigned not null auto_increment, primary key (eventrowid), eventtime datetime, eventname varchar(120));

-- 
-- ll-1: complete i/m adjustment fraction information (like crec 1-a)
--
-- insert into eventlog (eventtime, eventname) select now(), 'LL-1';
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
and imc.yearid = ##context.year##
and ppmy.modelyearid = ##context.year##-ageid
and ppmy.polprocessid in (##pollutantprocessids##)
group by ppa.processid,
 ppa.pollutantid,
 ppmy.modelyearid,
 imf.fueltypeid,
 imc.sourcetypeid;

-- 
-- ll-8: calculate adjusted meanbaserates
--
-- insert into eventlog (eventtime, eventname) select now(), 'LL-8';
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
	primary key (polprocessid, sourcetypeid, regclassid, fueltypeid, monthid, hourdayid, modelyearid, opmodeid)
);

-- now for all operating modes (formerly only for hot soaking and operating)
-- section withregclassid

-- @algorithm weightedmeanbaserate = sourcebinactivityfraction * meanbaserate.
-- weightedmeanbaserateim = sourcebinactivityfraction * meanbaserateim.
insert into weightedmeanbaserate (polprocessid, sourcetypeid, regclassid, fueltypeid, monthid, hourdayid, 
	modelyearid, opmodeid, weightedmeanbaserate, weightedmeanbaserateim)
select er.polprocessid, stmy.sourcetypeid, sb.regclassid, sb.fueltypeid, rsm.monthid, rshd.hourdayid, 
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
group by er.polprocessid, stmy.sourcetypeid, sb.regclassid, sb.fueltypeid, rsm.monthid, rshd.hourdayid, 
	stmy.modelyearid, er.opmodeid
order by null;
-- end section withregclassid

-- section noregclassid
insert into weightedmeanbaserate (polprocessid, sourcetypeid, regclassid, fueltypeid, monthid, hourdayid, 
	modelyearid, opmodeid, weightedmeanbaserate, weightedmeanbaserateim)
select er.polprocessid, stmy.sourcetypeid, 0 as regclassid, sb.fueltypeid, rsm.monthid, rshd.hourdayid, 
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
group by er.polprocessid, stmy.sourcetypeid, sb.fueltypeid, rsm.monthid, rshd.hourdayid, 
	stmy.modelyearid, er.opmodeid
order by null;
-- end section noregclassid

analyze table weightedmeanbaserate;

alter table movesworkeroutput add emissionquantim float null;

-- 
-- ll-9: calculate movesworkeroutput by source type
--
-- insert into eventlog (eventtime, eventname) select now(), 'LL-9 WITHOUT SCC';

-- @algorithm emissionquant = weightedmeanbaserate * sourcehours * opmodefraction.
-- emissionquantim = weightedmeanbaserateim * sourcehours * opmodefraction.
insert into movesworkeroutput (yearid, monthid, dayid, hourid, stateid, countyid,
	zoneid, linkid, pollutantid, processid, sourcetypeid, regclassid, fueltypeid, modelyearid,
	roadtypeid, scc, emissionquant, emissionquantim)
select ##context.year## as yearid, w.monthid, hd.dayid, hd.hourid,
	##context.iterlocation.staterecordid## as stateid,
	##context.iterlocation.countyrecordid## as countyid,
	##context.iterlocation.zonerecordid## as zoneid,
	##context.iterlocation.linkrecordid## as linkid,
	ppa.pollutantid, ppa.processid, w.sourcetypeid, w.regclassid, w.fueltypeid, w.modelyearid,
	l.roadtypeid, null as scc,
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
inner join link l on (l.linkid=##context.iterlocation.linkrecordid##)
inner join hourday hd on (hd.hourdayid=omd.hourdayid);

-- apply im

-- @algorithm apply i/m programs.
-- emissionquant=emissionquantim*imadjustfract + emissionquant*(1-imadjustfract).
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

drop table if exists weightedmeanbaserate;
drop table if exists imcoveragemergedungrouped;
-- end section cleanup
