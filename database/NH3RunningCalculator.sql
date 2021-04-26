-- version 2013-09-15
-- authors: gwo s. and wes f. 
-- the ammonia calculator shall not have the ability to calculate fuel formulation effects, 
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

##create.link##;
truncate link;

##create.monthgrouphour##;
truncate monthgrouphour;

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

##create.sho##;
truncate sho;

##create.sourcebin##;
truncate sourcebin;

##create.sourcebindistribution##;
truncate sourcebindistribution;

##create.sourcetypeage##;
truncate sourcetypeage;

##create.sourcetypemodelyear##;
truncate sourcetypemodelyear;

##create.temperatureadjustment##;
truncate temperatureadjustment;

##create.year##;
truncate year;

##create.zone##;
truncate zone;

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

-- select distinct emissionratebyage.* into outfile '??EMISSIONRATEBYAGE??'
-- from emissionratebyage, sourcebindistribution, sourcetypemodelyear, sourcebin, runspecsourcefueltype
-- where runspecsourcefueltype.fueltypeid = sourcebin.fueltypeid
-- and emissionratebyage.polprocessid = sourcebindistribution.polprocessid
-- and emissionratebyage.sourcebinid = sourcebin.sourcebinid
-- and emissionratebyage.sourcebinid = sourcebindistribution.sourcebinid
-- and sourcebin.sourcebinid = sourcebindistribution.sourcebinid
-- and runspecsourcefueltype.sourcetypeid = sourcetypemodelyear.sourcetypeid
-- and sourcebindistribution.sourcetypemodelyearid = sourcetypemodelyear.sourcetypemodelyearid
-- and sourcetypemodelyear.modelyearid <= ??context.year??
-- and emissionratebyage.polprocessid in (??pollutantprocessids??);

cache select emissionratebyage.* into outfile '##emissionratebyage##'
from emissionratebyage
where emissionratebyage.polprocessid in (##pollutantprocessids##)
and emissionratebyage.sourcebinid in (##macro.csv.all.sourcebinid##);

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
inner join year on (fuelsupply.fuelyearid = year.fuelyearid)
where fuelregionid = ##context.fuelregionid##
and yearid = ##context.year##
and fuelsupply.monthgroupid in (##macro.csv.all.monthgroupid##);

cache select fueltype.* into outfile '##fueltype##'
from fueltype
where fueltypeid in (##macro.csv.all.fueltypeid##);

cache select hourday.* into outfile '##hourday##'
from hourday
where hourdayid in (##macro.csv.all.hourdayid##);

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

cache select monthgrouphour.* into outfile '##monthgrouphour##'
from monthgrouphour
where hourid in (##macro.csv.all.hourid##);

cache select monthofanyyear.* into outfile '##monthofanyyear##'
from monthofanyyear
where monthofanyyear.monthid in (##macro.csv.all.monthid##);

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

select * into outfile '##sho##'
from sho
where linkid = ##context.iterlocation.linkrecordid##
and yearid = ##context.year##;

cache select distinct sourcebin.* into outfile '##sourcebin##'
from sourcebindistribution, sourcetypemodelyear, sourcebin
where polprocessid in (##pollutantprocessids##)
and sourcebindistribution.sourcetypemodelyearid = sourcetypemodelyear.sourcetypemodelyearid
and sourcebindistribution.sourcebinid = sourcebin.sourcebinid
and sourcetypemodelyear.modelyearid <= ##context.year##
and sourcetypemodelyear.modelyearid >= ##context.year## - 30;

cache select sourcebindistribution.* into outfile '##sourcebindistribution##'
from sourcebindistributionfuelusage_##context.iterprocess.databasekey##_##context.iterlocation.countyrecordid##_##context.year## as sourcebindistribution, 
sourcetypemodelyear
where polprocessid in (##pollutantprocessids##)
and sourcebindistribution.sourcetypemodelyearid = sourcetypemodelyear.sourcetypemodelyearid
and sourcetypemodelyear.modelyearid <= ##context.year##
and sourcetypemodelyear.modelyearid >= ##context.year## - 30;

cache select sourcetypeage.* into outfile '##sourcetypeage##'
from sourcetypeage
where sourcetypeage.sourcetypeid in (##macro.csv.all.sourcetypeid##);

cache select sourcetypemodelyear.* into outfile '##sourcetypemodelyear##'
from sourcetypemodelyear
where sourcetypemodelyear.sourcetypeid in (##macro.csv.all.sourcetypeid##)
and modelyearid <= ##context.year##
and modelyearid >= ##context.year## - 30;

cache select temperatureadjustment.* into outfile '##temperatureadjustment##'
from temperatureadjustment
where polprocessid in (##pollutantprocessids##)
and fueltypeid in (##macro.csv.all.fueltypeid##);

cache select year.* into outfile '##year##'
from year
where yearid = ##context.year##;

cache select * into outfile '##zone##'
from zone
where zoneid = ##context.iterlocation.zonerecordid##;

-- end section extract data

-- section processing

-- 
-- nh3rec 1: complete i/m adjustment fraction information
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

-- create index xpkimcoveragemergedungrouped on imcoveragemergedungrouped
-- (
--        polprocessid asc,
--        pollutantid asc,
--        processid asc,
--        modelyearid asc,
--        fueltypeid asc,
--        sourcetypeid asc
-- );

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
-- nh3rec-2: weight emission rates by source bin
--

drop table if exists sourcebinemissionrates0;
create table sourcebinemissionrates0 (
      zoneid int not null,
      linkid int not null,
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
      zoneid, linkid, yearid, polprocessid, sourcetypeid, modelyearid, fueltypeid, opmodeid);

insert into sourcebinemissionrates0 (
      zoneid, linkid, yearid, polprocessid,
      sourcetypeid, modelyearid, fueltypeid, opmodeid,
        meanbaserate, meanbaserateim)
select 
      ##context.iterlocation.zonerecordid## as zoneid,
      ##context.iterlocation.linkrecordid##,
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


drop table if exists sbweightedemissionrate;
create table sbweightedemissionrate (
       zoneid integer not null,
       linkid integer not null,
       monthid smallint not null,
       yearid smallint not null,
       polprocessid int not null,
       sourcetypeid smallint not null,
       modelyearid smallint not null,
       fueltypeid smallint not null,
       opmodeid smallint not null,
       meanbaserate float,
       meanbaserateim float
);

create index xpksbweightedemissionrate on sbweightedemissionrate (
       zoneid asc, 
       linkid asc,
       monthid asc, 
       yearid asc, 
       polprocessid asc, 
       sourcetypeid asc, 
       modelyearid asc, 
       fueltypeid asc, 
       opmodeid asc
);

insert into sbweightedemissionrate(
       zoneid,
       linkid,
       monthid,
       yearid,
       polprocessid,
       sourcetypeid,
       modelyearid,
       fueltypeid,
       opmodeid,
       meanbaserate,
       meanbaserateim
)
select 
      er.zoneid, 
      er.linkid,
      rm.monthid,
      er.yearid, 
          er.polprocessid, 
          er.sourcetypeid, 
          er.modelyearid, 
          er.fueltypeid, 
          er.opmodeid,
          er.meanbaserate, 
          er.meanbaserateim
from sourcebinemissionrates0 er, runspecmonth rm
order by null;

-- 
-- nh3rec-3: weight emission rates by operating mode
--
drop table if exists fullyweightedemissionrate;
create table fullyweightedemissionrate (
       zoneid integer not null,
       linkid integer not null,
       yearid smallint not null,
       monthid smallint not null,
       dayid smallint not null,
       hourid smallint not null,
       polprocessid int not null,
       sourcetypeid smallint not null,
       modelyearid smallint not null,
       fueltypeid smallint not null,
       hourdayid smallint not null,
       meanbaserate float,
       meanbaserateim float
);

create unique index xpkfullyweightedemissionrate on fullyweightedemissionrate (
       zoneid asc,
       linkid asc,
       yearid asc,
       monthid asc,
       polprocessid asc,
       sourcetypeid asc,
       modelyearid asc,
       fueltypeid asc,
       hourdayid asc
);

create index opmodedistributionspecial on opmodedistribution (
      polprocessid asc,
      sourcetypeid asc,
      linkid asc,
      opmodeid asc
);

insert into fullyweightedemissionrate(
       zoneid,
       linkid,
       yearid,
       monthid,
       dayid,
       hourid,
       polprocessid,
       sourcetypeid,
       modelyearid,
       fueltypeid,
       hourdayid,
       meanbaserate,
       meanbaserateim
)
select sbwer.zoneid, sbwer.linkid, sbwer.yearid, sbwer.monthid, 
       0, 0, 
       sbwer.polprocessid, sbwer.sourcetypeid, sbwer.modelyearid, sbwer.fueltypeid, 
       omd.hourdayid,
      sum(opmodefraction*meanbaserate),
      sum(opmodefraction*meanbaserateim)
from sbweightedemissionrate sbwer 
inner join opmodedistribution omd 
  using(polprocessid, sourcetypeid, linkid, opmodeid) 
group by zoneid, linkid, yearid, monthid, polprocessid, sourcetypeid, modelyearid, fueltypeid, hourdayid;

update fullyweightedemissionrate, hourday
set fullyweightedemissionrate.dayid=hourday.dayid,
    fullyweightedemissionrate.hourid=hourday.hourid
where fullyweightedemissionrate.hourdayid=hourday.hourdayid;   

analyze table link;
analyze table hourday;

-- 
-- nh3rec 4: multiply fully weighted and adjusted emission rates by source hour operating (sho)
--         activity to generate inventory.
--
drop table if exists sho2;
create table sho2 (
       yearid               smallint not null,
       monthid              smallint not null,
       dayid                smallint not null,
       hourid               smallint not null,
       sourcetypeid         smallint not null,
       modelyearid          smallint not null,
       scc                  char(10),
       sho                  float null
);
create index xpksho2 on sho2 (
      yearid asc,
      monthid asc,
      dayid asc,
      hourid asc,
      sourcetypeid asc,
      modelyearid asc
);
analyze table sho;

insert into sho2 select 
yearid, monthid, hd.dayid, hd.hourid, sourcetypeid,
yearid - ageid as modelyearid, null as scc, sho
from sho sho
inner join hourday hd on (hd.hourdayid=sho.hourdayid);

drop table if exists weightedandadjustedemissionrate3;
create table weightedandadjustedemissionrate3 (
            zoneid integer not null,
            linkid      integer not null,
            yearid      smallint not null,
            pollutantid smallint not null,
            processid smallint not null,
            sourcetypeid smallint not null,
            fueltypeid smallint not null,
            modelyearid smallint not null,
            monthid smallint not null,
            dayid smallint not null,
            hourid smallint not null,
            meanbaserate float,
            meanbaserateim float
);


create unique index xpkweightedandadjustedemissionrate31 on weightedandadjustedemissionrate3 (
            zoneid asc,
            linkid      asc,
            yearid      asc,
            pollutantid asc,
            processid asc,
            sourcetypeid asc,
            fueltypeid asc,
            modelyearid asc,
            monthid asc,
            dayid asc,
            hourid asc
);

insert into weightedandadjustedemissionrate3(
            zoneid,
            linkid,
            yearid,
            pollutantid,
            processid,
            sourcetypeid,
            fueltypeid,
            modelyearid,
            monthid,
            dayid,
            hourid,
            meanbaserate,
            meanbaserateim
)
select fwer.zoneid, fwer.linkid, fwer.yearid, ppa.pollutantid, ppa.processid, 
       fwer.sourcetypeid, fwer.fueltypeid,
       fwer.modelyearid, fwer.monthid, hd.dayid, hd.hourid, 
       fwer.meanbaserate, fwer.meanbaserateim
from fullyweightedemissionrate fwer
inner join hourday hd on (hd.hourdayid = fwer.hourdayid)
inner join pollutantprocessassoc ppa on (ppa.polprocessid=fwer.polprocessid);


drop table if exists sho3;
create table sho3 (
         linkid                     integer not null,
       yearid               smallint not null,
       monthid              smallint not null,
       dayid                smallint not null,
       hourid               smallint not null,
       sourcetypeid         smallint not null,
       modelyearid          smallint not null,
       pollutantid          smallint not null,
       processid            smallint not null,
       fueltypeid           smallint not null,
       scc                  char(10),
       emissionquant        float null,
       emissionquantim      float null
);
create index xpksho3 on sho3 (
      linkid asc,
      yearid asc,
      monthid asc,
      dayid asc,
      hourid asc,
      sourcetypeid asc,
      modelyearid asc,
      pollutantid asc,
      processid asc,
      fueltypeid asc
);

create index xpksho22 on sho2 (
      yearid asc,
      monthid asc,
      dayid asc,
      hourid asc,
      sourcetypeid asc,
      modelyearid asc
);

create index xpkweightedandadjustedemissionrate32 on weightedandadjustedemissionrate3 (
      yearid asc,
      monthid asc,
      dayid asc,
      hourid asc,
      sourcetypeid asc,
      modelyearid asc
);

analyze table sho2;

insert into sho3
select linkid, sho2.yearid, sho2.monthid, sho2.dayid, sho2.hourid, 
sho2.sourcetypeid, sho2.modelyearid, pollutantid, processid, fueltypeid, 
scc, sho * meanbaserate as emissionquant, sho * meanbaserateim as emissionquantim
from sho2 sho2
inner join weightedandadjustedemissionrate3 waer on (
waer.yearid=sho2.yearid and
waer.monthid=sho2.monthid and
waer.dayid=sho2.dayid and
waer.hourid=sho2.hourid and
waer.sourcetypeid=sho2.sourcetypeid and
waer.modelyearid=sho2.modelyearid);



alter table movesworkeroutput add emissionquantim float null;

analyze table sho3;
insert into movesworkeroutput (
stateid, countyid, zoneid, linkid, roadtypeid, yearid, monthid, dayid, hourid,
pollutantid, processid, sourcetypeid, modelyearid, fueltypeid, scc, emissionquant, emissionquantim)
select ##context.iterlocation.staterecordid##, ##context.iterlocation.countyrecordid##,
l.zoneid, l.linkid, l.roadtypeid, yearid, monthid, dayid, hourid,
pollutantid, processid, sourcetypeid, modelyearid, fueltypeid, scc, emissionquant, emissionquantim
from sho3 sho3 inner join link l on (l.linkid = sho3.linkid);

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
drop table if exists countyfueladjustment;
drop table if exists fueladjustedrate;
drop table if exists fuelsupplyadjustment;
drop table if exists fuelsupplywithfueltype;
drop table if exists fullyweightedemissionrate;
drop table if exists imcoveragemergedungrouped;
drop table if exists imadjustment;
drop table if exists imadjustmentwithsourcebin;
drop table if exists sourcebinemissionrates0;
drop table if exists sbweightedemissionrate;
drop table if exists sho2;
drop table if exists sho3;
drop table if exists weightedandadjustedemissionrate;
drop table if exists weightedandadjustedemissionrate3;
flush tables;
-- end section cleanup
