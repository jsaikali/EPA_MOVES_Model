-- version 2013-09-15
-- author ed glover
-- author wesley faler
-- author ed campbell
-- add deterioration to hc,co and nox for starts - gwo shyu, epa, 11/12/2008
-- modified to add exponential start temperature equation - ed glover & david hawkins  3/26/2013

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
inner join monthofanyyear on monthofanyyear.monthgroupid = rsmg.monthgroupid
where fuelregionid = ##context.fuelregionid## and
yearid = ##context.year##
and monthofanyyear.monthid = ##context.monthid##
group by ff.fuelformulationid order by null;

cache select * into outfile '##fuelsubtype##'
from fuelsubtype;

cache select fuelsupply.* into outfile '##fuelsupply##'
from fuelsupply
inner join runspecmonthgroup on (fuelsupply.monthgroupid = runspecmonthgroup.monthgroupid)
inner join monthofanyyear on (monthofanyyear.monthgroupid = runspecmonthgroup.monthgroupid)
inner join year on (fuelsupply.fuelyearid = year.fuelyearid)
where fuelregionid = ##context.fuelregionid##
and yearid = ##context.year##
and monthofanyyear.monthid = ##context.monthid##;

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
from monthofanyyear
where monthofanyyear.monthid = ##context.monthid##;

select opmodedistribution.* into outfile '##opmodedistribution##'
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

select starts.* into outfile '##starts##'
from starts
where yearid = ##context.year##
and monthid = ##context.monthid##
and zoneid = ##context.iterlocation.zonerecordid##;

select starttempadjustment.* into outfile '##starttempadjustment##'
from starttempadjustment
where polprocessid in (##pollutantprocessids##);

select year.* into outfile '##year##'
from year
where yearid = ##context.year##;

select * into outfile '##zone##'
from zone
where zoneid = ##context.iterlocation.zonerecordid##;

cache select distinct zonemonthhour.* into outfile '##zonemonthhour##'
from zonemonthhour,runspechour
where zoneid = ##context.iterlocation.zonerecordid##
and zonemonthhour.monthid = ##context.monthid##
and runspechour.hourid = zonemonthhour.hourid;

-- end section extract data
--
-- section processing

--
-- csec 1-a complete i/m adjustment fraction information
--
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
-- csec 2-a: combine gpa and non gpa fuel adjustment factors.
--

drop table if exists countyfueladjustment;
create table countyfueladjustment (
	fuelregionid integer not null,
	polprocessid int not null,
	modelyearid integer not null,
	sourcetypeid smallint not null,
	fuelformulationid smallint not null,
	fueladjustment float
);

create unique index xpkcountyfueladjustment on countyfueladjustment
(
	fuelregionid asc,
	polprocessid asc,
	modelyearid asc,
	sourcetypeid asc,
	fuelformulationid asc
);

create index countyfueladjustment1 on countyfueladjustment
(
	polprocessid asc,
	modelyearid asc
);

create index countyfueladjustment2 on countyfueladjustment
(
	fuelformulationid asc
);

insert into countyfueladjustment (
  fuelregionid, polprocessid, modelyearid, sourcetypeid, fuelformulationid, fueladjustment)
select 
	##context.fuelregionid##,
	ppa.polprocessid,
	stmy.modelyearid,
	stmy.sourcetypeid,
	ff. fuelformulationid,
	ifnull(ratio,1) + gpafract * (ifnull(ratiogpa,1)-ifnull(ratio,1))
from county c
inner join pollutantprocessassoc ppa
inner join fuelformulation ff
inner join sourcetypemodelyear stmy
left outer join criteriaratio fa on (fa.polprocessid = ppa.polprocessid
	and fa.fuelformulationid = ff.fuelformulationid
	and fa.sourcetypeid = stmy.sourcetypeid
	and fa.modelyearid = stmy.modelyearid
)
where ppa.polprocessid in (##pollutantprocessids##)
and c.countyid = ##context.iterlocation.countyrecordid##
;


--
-- csec 2-b: aggregate county fuel adjustments to fuel type
--

drop table if exists countyfueladjustmentwithfueltype;
create table countyfueladjustmentwithfueltype (
	fuelregionid integer not null,
	polprocessid int not null,
	modelyearid smallint not null,
	sourcetypeid smallint not null,
	fuelformulationid smallint not null,
	fueltypeid smallint not null,
	fueladjustment float
);

create unique index xpkcountyfueladjustmentwithfueltype on countyfueladjustmentwithfueltype 
(
	fuelregionid asc,
	polprocessid asc,
	modelyearid asc,
	sourcetypeid asc,
	fuelformulationid asc,
	fueltypeid asc
);

insert into countyfueladjustmentwithfueltype (
	fuelregionid, polprocessid, modelyearid, sourcetypeid,
      fuelformulationid, fueltypeid, fueladjustment)
select 
  fuelregionid, cfa.polprocessid, cfa.modelyearid, sourcetypeid,
  cfa.fuelformulationid, fueltypeid, fueladjustment
from countyfueladjustment cfa
inner join fuelformulation ff on (ff.fuelformulationid = cfa.fuelformulationid)
inner join fuelsubtype fst on (fst.fuelsubtypeid = ff.fuelsubtypeid);

drop table if exists fuelsupplyadjustment;
create table fuelsupplyadjustment (
	yearid smallint not null,
	countyid integer not null,
	monthid smallint not null,
	polprocessid int not null,
	modelyearid smallint not null,
	sourcetypeid smallint not null,
	fueltypeid smallint not null,
	fueladjustment float
);

create unique index xpkfuelsupplyadjustment on fuelsupplyadjustment (
	yearid asc,
	countyid asc,
	monthid asc,
	polprocessid asc,
	modelyearid asc,
	sourcetypeid asc,
	fueltypeid asc
);

insert into fuelsupplyadjustment (
	yearid, countyid, monthid, polprocessid, modelyearid,
	sourcetypeid, fueltypeid, fueladjustment)
select
	yearid, ##context.iterlocation.countyrecordid## as countyid, monthid, cfa.polprocessid, cfa.modelyearid, 
	cfa.sourcetypeid, cfa.fueltypeid, sum(fueladjustment * marketshare)
from countyfueladjustmentwithfueltype cfa
inner join year y
inner join monthofanyyear may
inner join fuelsupply fs on (
	fs.fuelregionid = cfa.fuelregionid
	and fs.fuelyearid = y.fuelyearid
	and fs.monthgroupid = may.monthgroupid
	and fs.fuelformulationid = cfa.fuelformulationid)
where y.yearid = ##context.year##
group by yearid, cfa.fuelregionid, monthid, cfa.polprocessid, 
	cfa.modelyearid, cfa.sourcetypeid, cfa.fueltypeid
order by null;

--
-- csec-3 calculate temperature adjustment factors.
--
drop table if exists metstartadjustment;
create table metstartadjustment (
	zoneid int not null,
	monthid smallint not null,
	hourid smallint not null,
	polprocessid int not null,
	modelyearid smallint not null,
	fueltypeid smallint not null,
	opmodeid smallint not null,
	temperatureadjustment float
);


insert into metstartadjustment (
      zoneid, monthid, hourid, polprocessid, modelyearid, 
	fueltypeid, opmodeid, temperatureadjustment)
select
	zmh.zoneid,
	zmh.monthid,
	zmh.hourid,
	sta.polprocessid,
	ppmy.modelyearid,
	sta.fueltypeid,
	sta.opmodeid,
     case          
		   when sta.starttempequationtype = 'LOG' then
				(tempadjusttermb*exp(tempadjustterma*(least(temperature,75)-75))+ tempadjusttermc)	   
		   when sta.starttempequationtype = 'POLY' then
                (least(temperature,75)-75) * (tempadjustterma+(least(temperature,75)-75) * 
                (tempadjusttermb+(least(temperature,75)-75) * tempadjusttermc)) 
           else
                (least(temperature,75)-75) * (tempadjustterma+(least(temperature,75)-75) * 
                (tempadjusttermb+(least(temperature,75)-75) * tempadjusttermc))
           end as temperatureadjustment
from starttempadjustment sta
inner join pollutantprocessmappedmodelyear ppmy on (
	ppmy.polprocessid = sta.polprocessid 
	and ppmy.modelyeargroupid = sta.modelyeargroupid)
inner join zonemonthhour zmh
where zmh.zoneid = ##context.iterlocation.zonerecordid##;

--
-- csec-4: apply start temperature adjustment to emission rates
--

drop table if exists emissionrateswithimandtemp;
create table emissionrateswithimandtemp (
      zoneid int not null,
      monthid smallint not null,
      hourid smallint not null,
      yearid smallint not null,
      polprocessid int not null,
      modelyearid smallint not null,
      sourcebinid bigint not null,
      opmodeid smallint not null,
      fueltypeid smallint not null,
	  meanbaserate float,
	  meanbaserateim float
);

create index metstartadjustment_new1 on metstartadjustment (
	polprocessid asc,
	modelyearid asc,
	opmodeid asc,
	fueltypeid asc,
	zoneid asc, 
	monthid asc, 
	hourid asc,
	temperatureadjustment asc
);
create index emissionratebyage_new1 on emissionratebyage (
	sourcebinid asc,
	agegroupid asc,
	polprocessid asc,
	opmodeid asc,
	meanbaserate asc,
	meanbaserateim
);
create index sourcebin_new1 on sourcebin (
      sourcebinid asc,
      fueltypeid asc
);
create index agecategory_new1 on agecategory (
	agegroupid asc,
	ageid asc
);

-- note: below, add "0*" to make the expressions ".. + 0*msa.temperatureadjustment" to disable starts addititive temperature adjustment.
insert into emissionrateswithimandtemp (
      zoneid, monthid, hourid, yearid, polprocessid, modelyearid, 
	  sourcebinid, opmodeid, fueltypeid, meanbaserate, meanbaserateim )
select 
      msa.zoneid, msa.monthid, msa.hourid, ##context.year## as yearid, msa.polprocessid, 
      msa.modelyearid, erim.sourcebinid, msa.opmodeid, msa.fueltypeid,
	  (erim.meanbaserate + msa.temperatureadjustment) as meanbaserate,
	  (erim.meanbaserateim + msa.temperatureadjustment) as meanbaserateim
from sourcebin sb 
inner join emissionratebyage erim on (erim.sourcebinid=sb.sourcebinid)
inner join agecategory age on (age.agegroupid=erim.agegroupid)
inner join metstartadjustment msa on (msa.polprocessid=erim.polprocessid
	and msa.modelyearid=##context.year##-age.ageid
	and msa.opmodeid=erim.opmodeid
	and msa.fueltypeid=sb.fueltypeid);

--
-- csec-5: weight emission rates by source bin.
--
drop table if exists metsourcebinemissionrates;
create table metsourcebinemissionrates (
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

create index emissionrateswithimandtemp3 on emissionrateswithimandtemp (
      polprocessid asc,
      sourcebinid asc,
      modelyearid asc
);
create index sourcetypemodelyearid_1 on sourcetypemodelyear (
      sourcetypemodelyearid asc,
      modelyearid asc
);
analyze table sourcebindistribution;

insert into metsourcebinemissionrates (
      zoneid, monthid, hourid, yearid, polprocessid,
      sourcetypeid, modelyearid, fueltypeid, opmodeid,
	  meanbaserate, meanbaserateim)
select 
      er.zoneid, er.monthid, er.hourid, er.yearid, er.polprocessid, 
      stmy.sourcetypeid, stmy.modelyearid, er.fueltypeid, er.opmodeid,
	  sum(meanbaserate*sourcebinactivityfraction) as meanbaserate,
	  sum(meanbaserateim*sourcebinactivityfraction) as meanbaserateim
from emissionrateswithimandtemp er, sourcebindistribution sbd, sourcetypemodelyear stmy
where er.polprocessid=sbd.polprocessid and
	  er.sourcebinid=sbd.sourcebinid and
	  sbd.sourcetypemodelyearid=stmy.sourcetypemodelyearid and
	  er.modelyearid=stmy.modelyearid
group by zoneid, monthid, hourid, yearid, polprocessid, sourcetypeid, modelyearid, fueltypeid, opmodeid
order by null;

alter table metsourcebinemissionrates add index metsourcebinemissionrates1 (
	hourid, sourcetypeid, polprocessid, opmodeid);

--
-- csec-6 weight temperature-adjusted emission rates by operating mode.
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

insert into activityweightedemissionrate (
      zoneid, yearid, monthid, dayid, hourid, polprocessid,
      sourcetypeid, modelyearid, fueltypeid, meanbaserate, meanbaserateim )
select 
      zoneid, ##context.year##, monthid, hd.dayid, msber.hourid, msber.polprocessid,
      msber.sourcetypeid, modelyearid, fueltypeid, 
      sum(meanbaserate * opmodefraction),
      sum(meanbaserateim * opmodefraction)
from metsourcebinemissionrates msber
inner join hourday hd on (hd.hourid = msber.hourid)
inner join opmodedistribution omd on (
	omd.sourcetypeid = msber.sourcetypeid
	and omd.hourdayid = hd.hourdayid 
	and omd.polprocessid = msber.polprocessid 
	and omd.opmodeid = msber.opmodeid)
group by zoneid, yearid, monthid, hd.dayid, msber.hourid, msber.polprocessid, 
	msber.sourcetypeid, modelyearid, fueltypeid
order by null;

create unique index xpkactivityweightedemissionrate on activityweightedemissionrate (
	yearid asc,
	monthid asc,
	polprocessid asc,
	modelyearid asc,	
	sourcetypeid asc,
	fueltypeid asc,
	zoneid asc,
	dayid asc,
	hourid asc
);	

-- 
-- csec-7: apply fuel adjustment factor
--

drop table if exists activityweightedemissionrate2;
create table activityweightedemissionrate2 (
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

insert into activityweightedemissionrate2 (
  zoneid, yearid, monthid, dayid, hourid, polprocessid,
  sourcetypeid , modelyearid, fueltypeid, meanbaserate, meanbaserateim)
select 
  awer.zoneid, awer.yearid, awer.monthid, dayid, hourid, awer.polprocessid,
  awer.sourcetypeid, awer.modelyearid, awer.fueltypeid,
  meanbaserate * fueladjustment,
  meanbaserateim * fueladjustment
from activityweightedemissionrate awer 
inner join fuelsupplyadjustment fsa on (
	fsa.yearid = awer.yearid and fsa.monthid = awer.monthid
	and fsa.polprocessid = awer.polprocessid and fsa.modelyearid = awer.modelyearid
	and fsa.sourcetypeid = awer.sourcetypeid and fsa.fueltypeid = awer.fueltypeid)
inner join zone z on (z.countyid = fsa.countyid and z.zoneid = awer.zoneid);

create unique index xpkactivityweightedemissionrate on activityweightedemissionrate2 (
      zoneid asc,
      monthid asc,
      hourid asc,
	  dayid asc,
      yearid asc,
      sourcetypeid asc,
      modelyearid asc,
      fueltypeid asc,
      polprocessid asc
);

-- 
-- csec-8: multiply emission rates by start activity to generate inventory.
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

insert into starts2 (zoneid, monthid, hourid, dayid, yearid, 
	sourcetypeid, modelyearid, starts) 
select zoneid, monthid, hourid, dayid, yearid, sourcetypeid, 
	(##context.year## - ageid) as modelyearid, starts
from starts inner join hourday on (starts.hourdayid= hourday.hourdayid);

create unique index xpkstarts2 on starts2 (
      zoneid asc,
      monthid asc,
      hourid asc,
	  dayid asc,
      yearid asc,
      sourcetypeid asc,
      modelyearid asc
);

alter table movesworkeroutput add emissionquantim float null default 0.0;

-- alter table movesworkeroutput add emissionquantim float null;

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
  (meanbaserateim * starts) as emissionquant
from starts2 s, activityweightedemissionrate2 awer, pollutantprocessassoc ppa
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
update movesworkeroutput, imcoveragemergedungrouped set emissionquant=greatest(emissionquantim*imadjustfract + emissionquant*(1.0-imadjustfract),0.0)
where movesworkeroutput.processid = imcoveragemergedungrouped.processid
	and movesworkeroutput.pollutantid = imcoveragemergedungrouped.pollutantid
	and movesworkeroutput.modelyearid = imcoveragemergedungrouped.modelyearid
	and movesworkeroutput.fueltypeid = imcoveragemergedungrouped.fueltypeid
	and movesworkeroutput.sourcetypeid = imcoveragemergedungrouped.sourcetypeid;

alter table movesworkeroutput drop emissionquantim;

flush tables;

-- end section processing

-- section cleanup

drop table if exists activityweightedemissionrate;
drop table if exists activityweightedemissionrate2;
drop table if exists countyfueladjustment;
drop table if exists countyfueladjustmentwithfueltype;
drop table if exists emissionrateswithim;
drop table if exists emissionrateswithimandtemp;
drop table if exists fuelsupplyadjustment;
drop table if exists imcoveragemergedungrouped;
drop table if exists imcoveragemerged;
drop table if exists imadjustment;
drop table if exists imadjustmentwithsourcebin;
drop table if exists metsourcebinemissionrates;
drop table if exists metstartadjustment;
drop table if exists starts2;
-- end section cleanup


