-- version 2013-09-15
-- wesley faler
-- david brzezinski (epa)

-- @notused

-- special section names invoked by basicstartpmemissioncalculator.java:
--              hasmanyopmodes
--              emissionratebyagerate
--              startsactivity
--              applytemperatureadjustment

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

-- section emissionratebyagerates
##create.emissionratebyage##;
truncate table emissionratebyage;
-- end section emissionratebyagerates

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

##create.pollutantprocessmappedmodelyear##;
truncate pollutantprocessmappedmodelyear;

-- section startsactivity
##create.starts##;
truncate table starts;
-- end section startsactivity

-- section applytemperatureadjustment
##create.modelyeargroup##;
truncate table modelyeargroup;

##create.temperatureadjustment##;
truncate table temperatureadjustment;

##create.starttempadjustment##;
truncate table starttempadjustment;

##create.zonemonthhour##;
truncate table zonemonthhour;
-- end section applytemperatureadjustment

drop table if exists onecountyyeargeneralfuelratio;
create table if not exists onecountyyeargeneralfuelratio (
	fueltypeid int not null,
	sourcetypeid int not null,
	monthid int not null,
	pollutantid int not null,
	processid int not null,
	modelyearid int not null,
	yearid int not null,
	fueleffectratio double not null default '0',
	primary key (fueltypeid, sourcetypeid, monthid, pollutantid, modelyearid, yearid)
);
truncate onecountyyeargeneralfuelratio;

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

-- section hasmanyopmodes
select * 
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

-- section emissionratebyagerates
select distinct emissionratebyage.* 
into outfile '##emissionratebyage##'
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

cache select * into outfile '##pollutantprocessmappedmodelyear##'
from pollutantprocessmappedmodelyear
where modelyearid <= ##context.year##
and modelyearid >= ##context.year## - 30
and polprocessid in (##pollutantprocessids##);

-- section startsactivity
select starts.* into outfile '##starts##'
from starts
where yearid = ##context.year##
and zoneid = ##context.iterlocation.zonerecordid##;
-- end section startsactivity

-- section applytemperatureadjustment
cache select distinct modelyeargroup.*
into outfile '##modelyeargroup##'
from modelyeargroup;

cache select distinct temperatureadjustment.*
into outfile '##temperatureadjustment##'
from temperatureadjustment
inner join runspecsourcefueltype using (fueltypeid)
where polprocessid in (##pollutantprocessids##);

cache select distinct starttempadjustment.*
into outfile '##starttempadjustment##'
from starttempadjustment
inner join runspecsourcefueltype using (fueltypeid)
where polprocessid in (##pollutantprocessids##);

cache select zonemonthhour.*
into outfile '##zonemonthhour##'
from runspecmonth
inner join runspechour
inner join zonemonthhour on (zonemonthhour.monthid = runspecmonth.monthid and zonemonthhour.hourid = runspechour.hourid)
where zoneid = ##context.iterlocation.zonerecordid##;
-- end section applytemperatureadjustment

select gfr.fueltypeid, gfr.sourcetypeid, may.monthid, gfr.pollutantid, gfr.processid, mya.modelyearid, mya.yearid,
	sum((ifnull(fueleffectratio,1)+gpafract*(ifnull(fueleffectratiogpa,1)-ifnull(fueleffectratio,1)))*marketshare) as fueleffectratio
	into outfile '##onecountyyeargeneralfuelratio##'
from runspecmonthgroup rsmg
inner join runspecmodelyearage mya on (mya.yearid = ##context.year##)
inner join county c on (c.countyid = ##context.iterlocation.countyrecordid##)
inner join year y on (y.yearid = mya.yearid)
inner join fuelsupply fs on (fs.fuelregionid = ##context.fuelregionid##
	and fs.fuelyearid = y.fuelyearid
	and fs.monthgroupid = rsmg.monthgroupid)
inner join monthofanyyear may on (may.monthgroupid = fs.monthgroupid)
inner join runspecsourcefueltype rssf
inner join generalfuelratio gfr on (gfr.fuelformulationid = fs.fuelformulationid
	and gfr.polprocessid in (##pollutantprocessids##)
	and gfr.minmodelyearid <= mya.modelyearid
	and gfr.maxmodelyearid >= mya.modelyearid
	and gfr.minageid <= mya.ageid
	and gfr.maxageid >= mya.ageid
	and gfr.fueltypeid = rssf.fueltypeid
	and gfr.sourcetypeid = rssf.sourcetypeid)
group by gfr.fueltypeid, gfr.sourcetypeid, may.monthid, gfr.pollutantid, gfr.processid, mya.modelyearid, mya.yearid
;

-- end section extract data

-- section local data removal
--truncate xxxxxx;
-- end section local data removal

-- section processing

-- --------------------------------------------------------------
-- step 1: weight emission rates by operating mode
-- --------------------------------------------------------------
drop table if exists opmodeweightedemissionratetemp;
create table opmodeweightedemissionratetemp (
	hourdayid smallint(6) not null,
	sourcetypeid smallint(6) not null,
	sourcebinid bigint(20) not null,
	agegroupid smallint(6) not null,
	polprocessid int not null,
	opmodeid smallint(6) not null,
	opmodeweightedmeanbaserate float
);

-- section emissionratebyagerates

-- section hasmanyopmodes
insert into opmodeweightedemissionratetemp (hourdayid, sourcetypeid, sourcebinid, agegroupid, polprocessid, opmodeid, opmodeweightedmeanbaserate)
select distinct omd.hourdayid, omd.sourcetypeid, er.sourcebinid, er.agegroupid, omd.polprocessid, omd.opmodeid,
(opmodefraction * meanbaserate) as opmodeweightedmeanbaserate
from opmodedistribution omd
inner join emissionratebyage er using (polprocessid, opmodeid)
inner join sourcebindistribution sbd on (
	sbd.polprocessid=er.polprocessid and sbd.sourcebinid=er.sourcebinid)
inner join agecategory acat on (acat.agegroupid=er.agegroupid)
inner join sourcetypemodelyear stmy on (stmy.sourcetypemodelyearid=sbd.sourcetypemodelyearid
and stmy.sourcetypeid=omd.sourcetypeid
and stmy.modelyearid=##context.year##-ageid);
-- end section hasmanyopmodes
-- end section emissionratebyagerates

drop table if exists opmodeweightedemissionrate;
create table opmodeweightedemissionrate (
	hourdayid smallint(6) not null,
	sourcetypeid smallint(6) not null,
	sourcebinid bigint(20) not null,
	agegroupid smallint(6) not null,
	polprocessid int not null,
	opmodeid smallint(6) not null,
	opmodeweightedmeanbaserate float,
	primary key (hourdayid, sourcetypeid, sourcebinid, agegroupid, polprocessid, opmodeid),
	index (hourdayid),
	index (sourcetypeid),
	index (sourcebinid),
	index (agegroupid),
	index (polprocessid),
	index (opmodeid)
);

insert into opmodeweightedemissionrate (hourdayid, sourcetypeid, sourcebinid, agegroupid, polprocessid, opmodeid, opmodeweightedmeanbaserate)
select hourdayid, sourcetypeid, sourcebinid, agegroupid, polprocessid, opmodeid, sum(opmodeweightedmeanbaserate)
from opmodeweightedemissionratetemp
group by hourdayid, sourcetypeid, sourcebinid, agegroupid, polprocessid, opmodeid
order by null;

-- --------------------------------------------------------------
-- step 2: weight emission rates by source bin
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
	ageid smallint(6) not null,
	opmodeid smallint(6) not null,
	primary key (yearid, hourdayid, sourcetypeid, fueltypeid, modelyearid, polprocessid, opmodeid),
	index (yearid),
	index (hourdayid),
	index (sourcetypeid),
	index (fueltypeid),
	index (modelyearid),
	index (polprocessid),
	index (ageid),
	index (opmodeid)
);

insert into fullyweightedemissionrate (yearid, hourdayid, sourcetypeid, fueltypeid, modelyearid, polprocessid, fullyweightedmeanbaserate, ageid, opmodeid)
select ##context.year## as yearid, omer.hourdayid, omer.sourcetypeid, sb.fueltypeid, stmy.modelyearid, omer.polprocessid,
sum(sourcebinactivityfraction*opmodeweightedmeanbaserate) as fullyweightedmeanbaserate,
acat.ageid, omer.opmodeid
from opmodeweightedemissionrate omer
inner join sourcebindistribution sbd on (sbd.sourcebinid=omer.sourcebinid and sbd.polprocessid=omer.polprocessid)
inner join agecategory acat on (acat.agegroupid=omer.agegroupid)
inner join sourcetypemodelyear stmy on (stmy.sourcetypemodelyearid=sbd.sourcetypemodelyearid
and stmy.sourcetypeid=omer.sourcetypeid and stmy.modelyearid=##context.year##-acat.ageid)
inner join pollutantprocessmodelyear ppmy on (ppmy.polprocessid=sbd.polprocessid and ppmy.modelyearid=stmy.modelyearid)
inner join sourcebin sb on (sb.sourcebinid=sbd.sourcebinid and sb.modelyeargroupid=ppmy.modelyeargroupid)
group by omer.hourdayid, omer.sourcetypeid, sb.fueltypeid, stmy.modelyearid, omer.polprocessid, acat.ageid
order by null;

-- --------------------------------------------------------------
-- step 3: multiply emission rates by activity
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
	opmodeid smallint(6) not null,
	unadjustedemissionquant float,
	primary key (yearid, monthid, hourdayid, sourcetypeid, fueltypeid, modelyearid, polprocessid, opmodeid),
	index (yearid),
	index (monthid),
	index (hourdayid),
	index (sourcetypeid),
	index (fueltypeid),
	index (modelyearid),
	index (polprocessid),
	index (opmodeid)
);

-- section startsactivity
insert into unadjustedemissionresults (yearid, monthid, hourdayid, sourcetypeid, fueltypeid, modelyearid, polprocessid, opmodeid, unadjustedemissionquant)
select f.yearid, starts.monthid, f.hourdayid, f.sourcetypeid, f.fueltypeid, f.modelyearid, f.polprocessid, f.opmodeid,
(fullyweightedmeanbaserate*starts.starts) as unadjustedemissionquant
from fullyweightedemissionrate f
inner join starts using (hourdayid, yearid, ageid, sourcetypeid);
-- end section startsactivity

-- --------------------------------------------------------------
-- step 4: apply temperature adjustment
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
	emissionquant float,
	primary key (yearid, monthid, dayid, hourid, sourcetypeid, fueltypeid, modelyearid, polprocessid),
	index (yearid),
	index (monthid),
	index (dayid),
	index (hourid),
	index (sourcetypeid),
	index (fueltypeid),
	index (modelyearid),
	index (polprocessid)
);

-- section applytemperatureadjustment
insert into adjustedemissionresults (yearid, monthid, dayid, hourid, sourcetypeid, fueltypeid, modelyearid, polprocessid, emissionquant)
select u.yearid, u.monthid, hd.dayid, hd.hourid, u.sourcetypeid, u.fueltypeid, u.modelyearid, u.polprocessid,
sum(coalesce(
	unadjustedemissionquant*tempadjusttermb*exp(tempadjustterma*(72.0-least(temperature,72)))+tempadjusttermc
	,unadjustedemissionquant)) as emissionquant
from unadjustedemissionresults u
inner join hourday hd using (hourdayid)
inner join zonemonthhour zmh on (zmh.monthid=u.monthid and zmh.hourid=hd.hourid)
inner join pollutantprocessmappedmodelyear ppmy on (ppmy.polprocessid=u.polprocessid and ppmy.modelyearid=u.modelyearid)
left outer join starttempadjustment ta on (
	ta.polprocessid=u.polprocessid
	and ta.fueltypeid=u.fueltypeid
	and ta.opmodeid=u.opmodeid
	and ta.modelyeargroupid=ppmy.modelyeargroupid
)
group by u.yearid, u.monthid, hd.dayid, hd.hourid, u.sourcetypeid, u.fueltypeid, u.modelyearid, u.polprocessid
order by null;
-- end section applytemperatureadjustment

-- --------------------------------------------------------------
-- step 5: convert results to structure of movesworkeroutput by sourcetypeid
-- --------------------------------------------------------------
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

update movesworkeroutput, onecountyyeargeneralfuelratio set emissionquant=emissionquant*fueleffectratio
where onecountyyeargeneralfuelratio.fueltypeid = movesworkeroutput.fueltypeid
and onecountyyeargeneralfuelratio.sourcetypeid = movesworkeroutput.sourcetypeid
and onecountyyeargeneralfuelratio.monthid = movesworkeroutput.monthid
and onecountyyeargeneralfuelratio.pollutantid = movesworkeroutput.pollutantid
and onecountyyeargeneralfuelratio.processid = movesworkeroutput.processid
and onecountyyeargeneralfuelratio.modelyearid = movesworkeroutput.modelyearid
and onecountyyeargeneralfuelratio.yearid = movesworkeroutput.yearid;

-- end section processing

-- section cleanup
drop table if exists opmodeweightedemissionratetemp;
drop table if exists opmodeweightedemissionrate;
drop table if exists fullyweightedemissionrate;
drop table if exists unadjustedemissionresults;
drop table if exists adjustedemissionresults;
drop table if exists onecountyyeargeneralfuelratio;
-- end section cleanup
