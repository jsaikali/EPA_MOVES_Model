-- directly calculate rates.
-- version 2017-09-17
-- author wesley faler

-- @calculator

-- section create remote tables for extracted data

##create.agecategory##;
truncate agecategory;

##create.baseratebyage##;
truncate baseratebyage;

##create.baserate##;
truncate baserate;

##create.county##;
truncate table county;

create table emissionrateadjustmentworker
(	
	polprocessid			int(11)			not null,
	sourcetypeid			smallint(6)		not null,
	regclassid				smallint(6)		not null,
	fueltypeid				smallint(6)		not null,
	modelyearid				smallint(6)		not null,
	emissionrateadjustment	double null default null,
	primary key (polprocessid, sourcetypeid, fueltypeid, regclassid, modelyearid)
);
truncate emissionrateadjustmentworker;

##create.fueltype##;
truncate table fueltype;

create table if not exists localfuelsupply (
	fueltypeid smallint not null,
	fuelsubtypeid smallint not null,
	fuelformulationid smallint not null,
	marketshare double not null,
	primary key (fuelformulationid),
	key (fueltypeid, fuelsubtypeid, fuelformulationid, marketshare),
	key (fuelformulationid)
);
truncate table localfuelsupply;

##create.generalfuelratio##;
truncate table generalfuelratio;

##create.imcoverage##;
truncate imcoverage;

##create.imfactor##;
truncate imfactor;

##create.pollutantprocessassoc##;
truncate pollutantprocessassoc;

##create.pollutantprocessmappedmodelyear##;
truncate pollutantprocessmappedmodelyear;

##create.runspecmodelyearage##;
truncate table runspecmodelyearage;

##create.runspecsourcefueltype##;
truncate table runspecsourcefueltype;

-- section process1_2
##create.criteriaratio##;
truncate criteriaratio;

##create.altcriteriaratio##;
truncate altcriteriaratio;
-- end section process1_2

-- section process2
##create.starttempadjustment##;
truncate table starttempadjustment;
-- end section process2

##create.temperatureadjustment##;
truncate table temperatureadjustment;

-- section getactivity
create table if not exists universalactivity (
	hourdayid smallint not null,
	modelyearid smallint not null,
	sourcetypeid smallint not null,
	activity double,
	primary key (sourcetypeid, hourdayid, modelyearid),
	key (hourdayid, modelyearid)
);

truncate table universalactivity;

##create.runspechourday##;
truncate table runspechourday;

##create.runspecsourcetype##;
truncate table runspecsourcetype;

-- end section getactivity

##create.zonemonthhour##;
truncate zonemonthhour;

create table if not exists zoneacfactor (
	hourid smallint(6) not null default 0,
	sourcetypeid smallint(6) not null default 0,
	modelyearid smallint(6) not null default 0,
	acfactor double not null default 0,
	primary key (hourid, sourcetypeid, modelyearid)
);
truncate zoneacfactor;

-- section aggregatesmfr
create table if not exists smfrsbdsummary (
	sourcetypeid smallint not null,
	modelyearid smallint not null,
	fueltypeid smallint not null,
	regclassid smallint not null,
	sbdtotal double not null,
	primary key (sourcetypeid, modelyearid, fueltypeid, regclassid),
	key (modelyearid, sourcetypeid, fueltypeid, regclassid)
);

truncate smfrsbdsummary;
-- end section aggregatesmfr

-- section process91
-- section adjustapuemissionrate
create table if not exists apuemissionratefraction (
	modelyearid smallint not null,
	hourfractionadjust double not null,
	primary key (modelyearid)
);
-- end section adjustapuemissionrate
-- end section process91


-- end section create remote tables for extracted data

-- section extract data

cache select * into outfile '##agecategory##'
from agecategory;

-- @algorithm create emissionrateadjustment by modelyear.
-- @output emissionrateadjustmentworker
cache select 
	era.polprocessid,
	sourcetypeid,
	regclassid,
	fueltypeid,
	modelyearid,
	emissionrateadjustment
into outfile '##emissionrateadjustmentworker##'
from emissionrateadjustment era 
inner join pollutantprocessassoc using (polprocessid)
inner join modelyear
where processid = ##context.iterprocess.databasekey##
and pollutantid in (##pollutantids##)
and modelyearid >= ##context.year##-30
and modelyearid <= ##context.year##
and endmodelyearid >= modelyearid
and beginmodelyearid <= modelyearid;

-- section inventory

cache(linkid=##context.iterlocation.linkrecordid##) select br.sourcetypeid,
br.roadtypeid,br.avgspeedbinid,br.hourdayid,
br.polprocessid,br.pollutantid,br.processid,
br.modelyearid,br.fueltypeid,br.agegroupid,br.regclassid,
br.opmodeid,br.meanbaserate,br.meanbaserateim,
br.emissionrate,br.emissionrateim,
br.meanbaserateacadj,br.meanbaserateimacadj,
br.emissionrateacadj,br.emissionrateimacadj,
br.opmodefraction,br.opmodefractionrate
into outfile '##baseratebyage##'
from baseratebyage_##context.iterprocess.databasekey##_##context.year## br
inner join agecategory ac on (ac.agegroupid = br.agegroupid)
where processid = ##context.iterprocess.databasekey##
and pollutantid in (##pollutantids##)
and modelyearid <= ##context.year##
and modelyearid >= ##context.year## - 30
and ageid = ##context.year## - modelyearid
and roadtypeid = ##context.iterlocation.roadtyperecordid##;

cache(linkid=##context.iterlocation.linkrecordid##) select br.sourcetypeid,
br.roadtypeid,br.avgspeedbinid,br.hourdayid,
br.polprocessid,br.pollutantid,br.processid,
br.modelyearid,br.fueltypeid,br.regclassid,
br.opmodeid,br.meanbaserate,br.meanbaserateim,
br.emissionrate,br.emissionrateim,
br.meanbaserateacadj,br.meanbaserateimacadj,
br.emissionrateacadj,br.emissionrateimacadj,
br.opmodefraction,br.opmodefractionrate
into outfile '##baserate##'
from baserate_##context.iterprocess.databasekey##_##context.year## br
where processid = ##context.iterprocess.databasekey##
and pollutantid in (##pollutantids##)
and modelyearid <= ##context.year##
and modelyearid >= ##context.year## - 30
and roadtypeid = ##context.iterlocation.roadtyperecordid##;
-- end section inventory

-- section rates
-- section notproject
cache(linkid=##context.iterlocation.linkrecordid##) select br.sourcetypeid,
br.roadtypeid,br.avgspeedbinid,br.hourdayid,
br.polprocessid,br.pollutantid,br.processid,
br.modelyearid,br.fueltypeid,br.agegroupid,br.regclassid,
br.opmodeid,br.meanbaserate,br.meanbaserateim,
br.emissionrate,br.emissionrateim,
br.meanbaserateacadj,br.meanbaserateimacadj,
br.emissionrateacadj,br.emissionrateimacadj,
br.opmodefraction,br.opmodefractionrate
into outfile '##baseratebyage##'
from baseratebyage_##context.iterprocess.databasekey##_##context.year## br
inner join agecategory ac on (ac.agegroupid = br.agegroupid)
where processid = ##context.iterprocess.databasekey##
and pollutantid in (##pollutantids##)
and modelyearid <= ##context.year##
and modelyearid >= ##context.year## - 30
and ageid = ##context.year## - modelyearid
and roadtypeid = ##context.iterlocation.roadtyperecordid##
and avgspeedbinid = mod(##context.iterlocation.linkrecordid##,100);

cache(linkid=##context.iterlocation.linkrecordid##) select br.sourcetypeid,
br.roadtypeid,br.avgspeedbinid,br.hourdayid,
br.polprocessid,br.pollutantid,br.processid,
br.modelyearid,br.fueltypeid,br.regclassid,
br.opmodeid,br.meanbaserate,br.meanbaserateim,
br.emissionrate,br.emissionrateim,
br.meanbaserateacadj,br.meanbaserateimacadj,
br.emissionrateacadj,br.emissionrateimacadj,
br.opmodefraction,br.opmodefractionrate
into outfile '##baserate##'
from baserate_##context.iterprocess.databasekey##_##context.year## br
where processid = ##context.iterprocess.databasekey##
and pollutantid in (##pollutantids##)
and modelyearid <= ##context.year##
and modelyearid >= ##context.year## - 30
and roadtypeid = ##context.iterlocation.roadtyperecordid##
and avgspeedbinid = mod(##context.iterlocation.linkrecordid##,100);
-- end section notproject

-- section project
cache(linkid=##context.iterlocation.linkrecordid##) select br.sourcetypeid,
br.roadtypeid,br.avgspeedbinid,br.hourdayid,
br.polprocessid,br.pollutantid,br.processid,
br.modelyearid,br.fueltypeid,br.agegroupid,br.regclassid,
br.opmodeid,br.meanbaserate,br.meanbaserateim,
br.emissionrate,br.emissionrateim,
br.meanbaserateacadj,br.meanbaserateimacadj,
br.emissionrateacadj,br.emissionrateimacadj,
br.opmodefraction,br.opmodefractionrate
into outfile '##baseratebyage##'
from baseratebyage_##context.iterprocess.databasekey##_##context.year## br
inner join agecategory ac on (ac.agegroupid = br.agegroupid)
where processid = ##context.iterprocess.databasekey##
and pollutantid in (##pollutantids##)
and modelyearid <= ##context.year##
and modelyearid >= ##context.year## - 30
and ageid = ##context.year## - modelyearid
and roadtypeid = ##context.iterlocation.roadtyperecordid##;

cache(linkid=##context.iterlocation.linkrecordid##) select br.sourcetypeid,
br.roadtypeid,br.avgspeedbinid,br.hourdayid,
br.polprocessid,br.pollutantid,br.processid,
br.modelyearid,br.fueltypeid,br.regclassid,
br.opmodeid,br.meanbaserate,br.meanbaserateim,
br.emissionrate,br.emissionrateim,
br.meanbaserateacadj,br.meanbaserateimacadj,
br.emissionrateacadj,br.emissionrateimacadj,
br.opmodefraction,br.opmodefractionrate
into outfile '##baserate##'
from baserate_##context.iterprocess.databasekey##_##context.year## br
where processid = ##context.iterprocess.databasekey##
and pollutantid in (##pollutantids##)
and modelyearid <= ##context.year##
and modelyearid >= ##context.year## - 30
and roadtypeid = ##context.iterlocation.roadtyperecordid##;
-- end section project
-- end section rates

cache select countyid, stateid, countyname, altitude, gpafract, barometricpressure, barometricpressurecv
into outfile '##county##'
from county
where countyid = ##context.iterlocation.countyrecordid##;

-- section process1_2
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
into outfile '##altcriteriaratio##'
from altcriteriaratio
where polprocessid in (##pollutantprocessids##)
and modelyearid = mymap(##context.year## - ageid);
-- end section process1_2

cache select * into outfile '##fueltype##'
from fueltype
where fueltypeid in (##macro.csv.all.fueltypeid##);

cache select fst.fueltypeid, fst.fuelsubtypeid, ff.fuelformulationid, fs.marketshare
into outfile '##localfuelsupply##'
from year
inner join fuelsupply fs on (fs.fuelyearid=year.fuelyearid)
inner join monthofanyyear moay on (moay.monthgroupid=fs.monthgroupid)
inner join fuelformulation ff on (ff.fuelformulationid=fs.fuelformulationid)
inner join fuelsubtype fst on (fst.fuelsubtypeid=ff.fuelsubtypeid)
where yearid = ##context.year##
and fs.fuelregionid = ##context.fuelregionid##
and moay.monthid = ##context.monthid##
and fst.fueltypeid in (##macro.csv.all.fueltypeid##);

cache select fueltypeid, fuelformulationid, polprocessid, pollutantid, processid,
	minmodelyearid, maxmodelyearid,
	minageid, maxageid,
	sourcetypeid,
	ifnull(fueleffectratio,1) as fueleffectratio, ifnull(fueleffectratiogpa,1) as fueleffectratiogpa
into outfile '##generalfuelratio##'
from generalfuelratio gfr
where polprocessid in (##pollutantprocessids##)
and minmodelyearid <= ##context.year##
and maxmodelyearid >= ##context.year##-30
and fuelformulationid in (
	select ff.fuelformulationid
	from year
	inner join fuelsupply fs on (fs.fuelyearid=year.fuelyearid)
	inner join monthofanyyear moay on (moay.monthgroupid=fs.monthgroupid)
	inner join fuelformulation ff on (ff.fuelformulationid=fs.fuelformulationid)
	inner join fuelsubtype fst on (fst.fuelsubtypeid=ff.fuelsubtypeid)
	where yearid = ##context.year##
	and fs.fuelregionid = ##context.fuelregionid##
	and moay.monthid = ##context.monthid##
	and fst.fueltypeid in (##macro.csv.all.fueltypeid##)
);

cache select distinct imcoverage.polprocessid,
	imcoverage.stateid, imcoverage.countyid,
	imcoverage.yearid,
	imcoverage.sourcetypeid,
	imcoverage.fueltypeid,
	imcoverage.improgramid,
	imcoverage.begmodelyearid, imcoverage.endmodelyearid,
	imcoverage.inspectfreq,
	imcoverage.teststandardsid,
	imcoverage.useimyn,
	imcoverage.compliancefactor
into outfile '##imcoverage##'
from imcoverage
inner join runspecsourcefueltype on (runspecsourcefueltype.fueltypeid = imcoverage.fueltypeid
	and runspecsourcefueltype.sourcetypeid = imcoverage.sourcetypeid)
where polprocessid in (##pollutantprocessids##)
and countyid = ##context.iterlocation.countyrecordid## 
and yearid = ##context.year##
and useimyn = 'Y';

cache select distinct imfactor.polprocessid,
	imfactor.inspectfreq, imfactor.teststandardsid,
	imfactor.sourcetypeid, imfactor.fueltypeid,
	imfactor.immodelyeargroupid,
	imfactor.agegroupid,
	imfactor.imfactor
into outfile '##imfactor##'
from imfactor
inner join runspecsourcefueltype on (runspecsourcefueltype.fueltypeid = imfactor.fueltypeid
	and runspecsourcefueltype.sourcetypeid = imfactor.sourcetypeid)
where polprocessid in (##pollutantprocessids##);

cache select polprocessid, modelyearid, modelyeargroupid, fuelmygroupid, immodelyeargroupid
into outfile '##pollutantprocessmappedmodelyear##'
from pollutantprocessmappedmodelyear
where modelyearid <= ##context.year##
and modelyearid >= ##context.year## - 30
and polprocessid in (##pollutantprocessids##);

cache select * into outfile '##pollutantprocessassoc##'
from pollutantprocessassoc
where processid=##context.iterprocess.databasekey##
and polprocessid in (##pollutantprocessids##);

cache select * into outfile '##runspecmodelyearage##'
from runspecmodelyearage
where modelyearid <= ##context.year##
and modelyearid >= ##context.year## - 30
and yearid = ##context.year##;

cache select * into outfile '##runspecsourcefueltype##'
from runspecsourcefueltype;

-- section process2
cache select fueltypeid, polprocessid, modelyeargroupid, opmodeid,
	tempadjustterma, tempadjusttermacv,
	tempadjusttermb, tempadjusttermbcv,
	tempadjusttermc, tempadjusttermccv,
	starttempequationtype
into outfile '##starttempadjustment##'
from starttempadjustment
where polprocessid in (##pollutantprocessids##)
and fueltypeid in (##macro.csv.all.fueltypeid##);
-- end section process2

cache select polprocessid, fueltypeid,
	tempadjustterma, tempadjusttermacv,
	tempadjusttermb, tempadjusttermbcv,
	tempadjusttermc, tempadjusttermccv,
	minmodelyearid, maxmodelyearid
into outfile '##temperatureadjustment##'
from temperatureadjustment
where polprocessid in (##pollutantprocessids##)
and fueltypeid in (##macro.csv.all.fueltypeid##);

select monthid, zoneid, hourid, temperature, temperaturecv, relhumidity, heatindex, specifichumidity, relativehumiditycv
into outfile '##zonemonthhour##'
from zonemonthhour
where zoneid = ##context.iterlocation.zonerecordid##
and zonemonthhour.monthid = ##context.monthid##
and zonemonthhour.hourid in (##macro.csv.all.hourid##);

-- @algorithm acfactor[hourid,sourcetypeid,modelyearid]=least(greatest(acactivityterma+heatindex*(acactivitytermb+acactivitytermc*heatindex),0),1.0)*acpenetrationfraction*functioningacfraction.
-- @output zoneacfactor
-- @input zonemonthhour
-- @input sourcetypeage
cache select zmh.hourid, sta.sourcetypeid, modelyearid, 
	least(greatest(acactivityterma+heatindex*(acactivitytermb+acactivitytermc*heatindex),0),1.0)*acpenetrationfraction*functioningacfraction as acfactor
	into outfile '##zoneacfactor##'
from zonemonthhour zmh
inner join monthofanyyear may on (may.monthid = zmh.monthid)
inner join monthgrouphour mgh on (mgh.monthgroupid = may.monthgroupid and mgh.hourid = zmh.hourid)
inner join sourcetypemodelyear stmy
inner join sourcetypeage sta on (
	sta.sourcetypeid = stmy.sourcetypeid and
	sta.ageid = ##context.year## - stmy.modelyearid)
where zmh.zoneid = ##context.iterlocation.zonerecordid##
and zmh.monthid = ##context.monthid##
and sta.sourcetypeid in (##macro.csv.all.sourcetypeid##);

-- section getactivity
-- extract activity at the month context.

-- section process1_9_10

-- @algorithm activity=sho
-- @condition running exhaust, brakewear, tirewear
select sho.hourdayid, ##context.year##-ageid as modelyearid, sourcetypeid, 
	sho as activity
	into outfile '##universalactivity##'
from sho
inner join runspechourday using (hourdayid)
where monthid = ##context.monthid##
and yearid = ##context.year##
and linkid = ##context.iterlocation.linkrecordid##;
-- end section process1_9_10

-- section process2

-- @algorithm activity=starts
-- @condition starts
select starts.hourdayid, ##context.year##-ageid as modelyearid, sourcetypeid, starts as activity
	into outfile '##universalactivity##'
from starts
inner join runspechourday using (hourdayid)
where monthid = ##context.monthid##
and yearid = ##context.year##
and zoneid = ##context.iterlocation.zonerecordid##;
-- end section process2

-- section process90

-- @algorithm activity=extendedidlehours
-- @condition extended idle exhaust
select extendedidlehours.hourdayid, ##context.year##-ageid as modelyearid, sourcetypeid, extendedidlehours as activity
	into outfile '##universalactivity##'
from extendedidlehours
inner join runspechourday using (hourdayid)
where monthid = ##context.monthid##
and yearid = ##context.year##
and zoneid = ##context.iterlocation.zonerecordid##
and sourcetypeid=62;
-- end section process90

-- section process91

-- @algorithm activity=hotellinghours
-- @condition auxiliary power exhaust
select hotellinghours.hourdayid, ##context.year##-ageid as modelyearid, sourcetypeid, hotellinghours as activity
	into outfile '##universalactivity##'
from hotellinghours
inner join runspechourday using (hourdayid)
where monthid = ##context.monthid##
and yearid = ##context.year##
and zoneid = ##context.iterlocation.zonerecordid##
and sourcetypeid=62;

-- section adjustapuemissionrate

-- @algorithm hourfractionadjust=opmodefraction[opmodeid=201].
-- @input hotellingactivitydistribution
-- @output apuemissionratefraction
-- @condition auxiliary power exhaust
cache select modelyearid, opmodefraction as hourfractionadjust
	into outfile '##apuemissionratefraction##'
from hotellingactivitydistribution
inner join runspecmodelyearage on (
	beginmodelyearid <= modelyearid
	and endmodelyearid >= modelyearid)
where modelyearid <= ##context.year##
and modelyearid >= ##context.year## - 30
and yearid = ##context.year##
and opmodeid = 201
and zoneid = ##hotellingactivityzoneid##;

-- end section adjustapuemissionrate
-- end section process91

cache select * into outfile '##runspechourday##'
from runspechourday;

cache select * into outfile '##runspecsourcetype##'
from runspecsourcetype;

-- end section getactivity

-- section aggregatesmfr
cache select round(sourcebindistribution.sourcetypemodelyearid/10000,0) as sourcetypeid,
	mod(sourcebindistribution.sourcetypemodelyearid,10000) as modelyearid,
	sourcebin.fueltypeid, regclassid,
	sum(sourcebinactivityfraction) as sbdtotal
into outfile '##smfrsbdsummary##'
from sourcebindistributionfuelusage_##context.iterprocess.databasekey##_##context.iterlocation.countyrecordid##_##context.year## as sourcebindistribution, 
sourcetypemodelyear, sourcebin, runspecsourcefueltype
where polprocessid in (##sbdpolprocessid##)
and sourcebindistribution.sourcetypemodelyearid = sourcetypemodelyear.sourcetypemodelyearid
and sourcetypemodelyear.modelyearid <= ##context.year##
and sourcetypemodelyear.modelyearid >= ##context.year## - 30
and sourcetypemodelyear.sourcetypeid = runspecsourcefueltype.sourcetypeid
and sourcebindistribution.sourcebinid = sourcebin.sourcebinid
and sourcebin.fueltypeid = runspecsourcefueltype.fueltypeid
group by sourcebindistribution.sourcetypemodelyearid, sourcebin.fueltypeid, regclassid
order by null;
-- end section aggregatesmfr

-- end section extract data

-- section processing

alter table fueltype add key speed1 (fueltypeid, humiditycorrectioncoeff);
analyze table fueltype;

alter table zonemonthhour add key speed1 (hourid, monthid, zoneid, temperature, specifichumidity, heatindex);
analyze table zonemonthhour;

drop table if exists imcoveragemergedungrouped;
create table imcoveragemergedungrouped (
	processid smallint not null,
	pollutantid smallint not null,
	modelyearid smallint not null,
	fueltypeid smallint not null,
	sourcetypeid smallint not null,
	imadjustfract float,
	key (processid,pollutantid,modelyearid,fueltypeid,sourcetypeid)
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

-- add columns
drop table if exists tempbaserateoutput;
create table tempbaserateoutput like baserateoutput;
alter table tempbaserateoutput add column meanbaserateim float default '0';
alter table tempbaserateoutput add column emissionrateim float default '0';

alter table tempbaserateoutput add column opmodeid smallint not null default '0';
alter table tempbaserateoutput add column generalfraction float not null default '0';
alter table tempbaserateoutput add column generalfractionrate float not null default '0';

alter table tempbaserateoutput add column meanbaserateacadj float default '0';
alter table tempbaserateoutput add column meanbaserateimacadj float default '0';
alter table tempbaserateoutput add column emissionrateacadj float default '0';
alter table tempbaserateoutput add column emissionrateimacadj float default '0';

-- @algorithm add age-based rates
insert into tempbaserateoutput (movesrunid, iterationid,
	zoneid, 
	sourcetypeid, roadtypeid, avgspeedbinid, hourdayid, pollutantid, processid,
	modelyearid, 
	yearid, monthid,
	fueltypeid,
	regclassid,
	opmodeid, generalfraction, generalfractionrate,
	meanbaserate, meanbaserateim,
	emissionrate, emissionrateim,
	meanbaserateacadj, meanbaserateimacadj,
	emissionrateacadj, emissionrateimacadj)
select 0, 0,
	##context.iterlocation.zonerecordid##, 
	br.sourcetypeid, br.roadtypeid, br.avgspeedbinid, br.hourdayid, br.pollutantid, br.processid,
	br.modelyearid, 
	##context.year##, ##context.monthid##,
	fueltypeid,
	br.regclassid,
	br.opmodeid, br.opmodefraction, br.opmodefractionrate,
	br.meanbaserate, br.meanbaserateim,
	br.emissionrate, br.emissionrateim,
	br.meanbaserateacadj, br.meanbaserateimacadj,
	br.emissionrateacadj, br.emissionrateimacadj
from baseratebyage br
inner join agecategory ac on (ac.agegroupid = br.agegroupid)
inner join runspecmodelyearage mya on (
	mya.yearid = ##context.year##
	and mya.modelyearid = br.modelyearid
	and mya.ageid = ac.ageid);

-- @algorithm add rates that don't depend upon age
insert into tempbaserateoutput (movesrunid, iterationid,
	zoneid, 
	sourcetypeid, roadtypeid, avgspeedbinid, hourdayid, pollutantid, processid,
	modelyearid, 
	yearid, monthid,
	fueltypeid,
	regclassid,
	opmodeid, generalfraction, generalfractionrate,
	meanbaserate, meanbaserateim,
	emissionrate, emissionrateim,
	meanbaserateacadj, meanbaserateimacadj,
	emissionrateacadj, emissionrateimacadj)
select 0, 0,
	##context.iterlocation.zonerecordid##, 
	br.sourcetypeid, br.roadtypeid, br.avgspeedbinid, br.hourdayid, br.pollutantid, br.processid,
	br.modelyearid, 
	##context.year##, ##context.monthid##,
	br.fueltypeid,
	br.regclassid,
	br.opmodeid, br.opmodefraction, br.opmodefractionrate,
	br.meanbaserate, br.meanbaserateim,
	br.emissionrate, br.emissionrateim,
	br.meanbaserateacadj, br.meanbaserateimacadj,
	br.emissionrateacadj, br.emissionrateimacadj
from baserate br;

-- section adjustapuemissionrate
insert ignore into apuemissionratefraction (modelyearid, hourfractionadjust)
select modelyearid, 0 from runspecmodelyearage;

-- @algorithm apu hourly rates have not been scaled by the apu operating mode (201) fraction.
-- inventory, but not emission rates, must be multiplied by the opmodefraction for opmode 201.
-- hourfractionadjust is opmodefraction for opmode 201.
-- @condition apu process hourly rates
update tempbaserateoutput, apuemissionratefraction
	set meanbaserate = meanbaserate * hourfractionadjust,
	meanbaserateim = meanbaserateim * hourfractionadjust,
	meanbaserateacadj = meanbaserateacadj * hourfractionadjust,
	meanbaserateimacadj = meanbaserateimacadj * hourfractionadjust
where apuemissionratefraction.modelyearid = tempbaserateoutput.modelyearid
and tempbaserateoutput.processid = 91;
-- end section adjustapuemissionrate

-- add fuel formulation, hour, and polprocess
drop table if exists baserateoutputwithfuel;
create table baserateoutputwithfuel like baserateoutput;
alter table baserateoutputwithfuel add column meanbaserateim float default '0';
alter table baserateoutputwithfuel add column emissionrateim float default '0';
alter table baserateoutputwithfuel add column opmodeid smallint not null default '0';
alter table baserateoutputwithfuel add column generalfraction float not null default '0';
alter table baserateoutputwithfuel add column generalfractionrate float not null default '0';
alter table baserateoutputwithfuel add column fuelformulationid smallint not null default '0';
alter table baserateoutputwithfuel add column fuelsubtypeid smallint not null default '0';
alter table baserateoutputwithfuel add column polprocessid int not null default '0';
alter table baserateoutputwithfuel add column fuelmarketshare double not null default '0';
alter table baserateoutputwithfuel add column hourid smallint not null default '0';
alter table baserateoutputwithfuel add column dayid smallint not null default '0';

alter table baserateoutputwithfuel add column meanbaserateacadj float default '0';
alter table baserateoutputwithfuel add column meanbaserateimacadj float default '0';
alter table baserateoutputwithfuel add column emissionrateacadj float default '0';
alter table baserateoutputwithfuel add column emissionrateimacadj float default '0';

analyze table baserateoutputwithfuel;

-- @algorithm obtain fuel market share.
insert into baserateoutputwithfuel (movesrunid, iterationid,
	zoneid, 
	sourcetypeid, roadtypeid, avgspeedbinid, hourdayid, hourid, dayid,
	pollutantid, processid,
	polprocessid,
	modelyearid, 
	yearid, monthid,
	fueltypeid,
	fuelsubtypeid,
	fuelformulationid,
	fuelmarketshare,
	regclassid,
	opmodeid, generalfraction, generalfractionrate,
	meanbaserate, meanbaserateim,
	emissionrate, emissionrateim,
	meanbaserateacadj, meanbaserateimacadj,
	emissionrateacadj, emissionrateimacadj)
select
	movesrunid, iterationid,
	zoneid, 
	sourcetypeid, roadtypeid, avgspeedbinid, hourdayid, floor(hourdayid/10) as hourid, mod(hourdayid,10) as dayid,
	pollutantid, processid,
	(pollutantid * 100 + processid) as polprocessid,
	modelyearid, 
	yearid, monthid,
	fs.fueltypeid,
	fs.fuelsubtypeid,
	fs.fuelformulationid,
	fs.marketshare as fuelmarketshare,
	regclassid,
	opmodeid, generalfraction, generalfractionrate,
	meanbaserate, meanbaserateim,
	emissionrate, emissionrateim,
	meanbaserateacadj, meanbaserateimacadj,
	emissionrateacadj, emissionrateimacadj
from tempbaserateoutput tbro
inner join localfuelsupply fs on (fs.fueltypeid=tbro.fueltypeid);

-- create table step1 select * from baserateoutputwithfuel;

-- section process2
alter table baserateoutputwithfuel add column temperature float null;
alter table baserateoutputwithfuel add column specifichumidity float null;
alter table baserateoutputwithfuel add column k float null;
alter table baserateoutputwithfuel add column heatindex float null;

-- note: uncomment the following line to disable starts additive temperature adjustment.
-- update baserateoutputwithfuel set generalfraction = 0, generalfractionrate = 0;

-- @algorithm calculate humidity adjustment factor k.
-- k = 1.0 - ((greatest(21.0, least(specifichumidity, 124.0))) - 75.0) * humiditycorrectioncoeff
-- @condition start exhaust (2).
update baserateoutputwithfuel, zonemonthhour, fueltype
set baserateoutputwithfuel.temperature = zonemonthhour.temperature,
	baserateoutputwithfuel.specifichumidity = zonemonthhour.specifichumidity,
	baserateoutputwithfuel.k = 1.0 - ((greatest(21.0, least(zonemonthhour.specifichumidity, 124.0))) - 75.0) * fueltype.humiditycorrectioncoeff,
	baserateoutputwithfuel.heatindex = zonemonthhour.heatindex
where baserateoutputwithfuel.zoneid = zonemonthhour.zoneid
and baserateoutputwithfuel.monthid = zonemonthhour.monthid
and baserateoutputwithfuel.hourid = zonemonthhour.hourid
and baserateoutputwithfuel.fueltypeid = fueltype.fueltypeid;

-- @algorithm do start temperature adjustments by opmodeid. pm uses multiplicative factors.
-- everything else uses additive factors.
-- the additive part needs to be weighted by opmodefraction (stored in generalfraction).  being a rate, sourcebinactivityfraction
-- is not required for the weighting since activity would have been weighted similarly.
-- for polprocessids (11202,11802): rate = rate*tempadjusttermb*exp(tempadjustterma*(72.0-least(temperature,72)))+tempadjusttermc.
-- for all other polprocessids with starttempequationtype of 'LOG': rate = rate + generalfraction * (tempadjusttermb*exp(tempadjustterma*(least(temperature,75)-75))+ tempadjusttermc).
-- for all other polprocessids with starttempequationtype of 'POLY': rate = rate + generalfraction * ((least(temperature,75)-75) * (tempadjustterma+(least(temperature,75)-75) * (tempadjusttermb+(least(temperature,75)-75) * tempadjusttermc))).
-- @condition start exhaust (2) process.
update baserateoutputwithfuel, starttempadjustment, pollutantprocessmappedmodelyear
set
	meanbaserate   = 
		case when baserateoutputwithfuel.polprocessid in (11202,11802) then
			meanbaserate*tempadjusttermb*exp(tempadjustterma*(72.0-least(temperature,72)))+tempadjusttermc
		else
			meanbaserate   + generalfraction * 
			case when starttempequationtype = 'LOG' then
				(tempadjusttermb*exp(tempadjustterma*(least(temperature,75)-75))+ tempadjusttermc)	   
			when starttempequationtype = 'POLY' then
				(least(temperature,75)-75) * (tempadjustterma+(least(temperature,75)-75) * (tempadjusttermb+(least(temperature,75)-75) * tempadjusttermc)) 
			else
				(least(temperature,75)-75) * (tempadjustterma+(least(temperature,75)-75) * (tempadjusttermb+(least(temperature,75)-75) * tempadjusttermc))
			end
		end,
	meanbaserateim = 
		case when baserateoutputwithfuel.polprocessid in (11202,11802) then
			meanbaserateim*tempadjusttermb*exp(tempadjustterma*(72.0-least(temperature,72)))+tempadjusttermc
		else
			meanbaserateim + generalfraction * 
			case when starttempequationtype = 'LOG' then
				(tempadjusttermb*exp(tempadjustterma*(least(temperature,75)-75))+ tempadjusttermc)	   
			when starttempequationtype = 'POLY' then
				(least(temperature,75)-75) * (tempadjustterma+(least(temperature,75)-75) * (tempadjusttermb+(least(temperature,75)-75) * tempadjusttermc)) 
			else
				(least(temperature,75)-75) * (tempadjustterma+(least(temperature,75)-75) * (tempadjusttermb+(least(temperature,75)-75) * tempadjusttermc))
			end
		end,
	emissionrate   = 
		case when baserateoutputwithfuel.polprocessid in (11202,11802) then
			emissionrate*tempadjusttermb*exp(tempadjustterma*(72.0-least(temperature,72)))+tempadjusttermc
		else
			emissionrate   + generalfractionrate * 
			case when starttempequationtype = 'LOG' then
				(tempadjusttermb*exp(tempadjustterma*(least(temperature,75)-75))+ tempadjusttermc)	   
			when starttempequationtype = 'POLY' then
				(least(temperature,75)-75) * (tempadjustterma+(least(temperature,75)-75) * (tempadjusttermb+(least(temperature,75)-75) * tempadjusttermc)) 
			else
				(least(temperature,75)-75) * (tempadjustterma+(least(temperature,75)-75) * (tempadjusttermb+(least(temperature,75)-75) * tempadjusttermc))
			end
		end,
	emissionrateim = 
		case when baserateoutputwithfuel.polprocessid in (11202,11802) then
			emissionrateim*tempadjusttermb*exp(tempadjustterma*(72.0-least(temperature,72)))+tempadjusttermc
		else
			emissionrateim + generalfractionrate * 
			case when starttempequationtype = 'LOG' then
				(tempadjusttermb*exp(tempadjustterma*(least(temperature,75)-75))+ tempadjusttermc)	   
			when starttempequationtype = 'POLY' then
				(least(temperature,75)-75) * (tempadjustterma+(least(temperature,75)-75) * (tempadjusttermb+(least(temperature,75)-75) * tempadjusttermc)) 
			else
				(least(temperature,75)-75) * (tempadjustterma+(least(temperature,75)-75) * (tempadjusttermb+(least(temperature,75)-75) * tempadjusttermc))
			end
		end,
	meanbaserateacadj   = 
		case when baserateoutputwithfuel.polprocessid in (11202,11802) then
			meanbaserateacadj*tempadjusttermb*exp(tempadjustterma*(72.0-least(temperature,72)))+tempadjusttermc
		else
			meanbaserateacadj   + generalfraction * 
			case when starttempequationtype = 'LOG' then
				(tempadjusttermb*exp(tempadjustterma*(least(temperature,75)-75))+ tempadjusttermc)	   
			when starttempequationtype = 'POLY' then
				(least(temperature,75)-75) * (tempadjustterma+(least(temperature,75)-75) * (tempadjusttermb+(least(temperature,75)-75) * tempadjusttermc)) 
			else
				(least(temperature,75)-75) * (tempadjustterma+(least(temperature,75)-75) * (tempadjusttermb+(least(temperature,75)-75) * tempadjusttermc))
			end
		end,
	meanbaserateimacadj = 
		case when baserateoutputwithfuel.polprocessid in (11202,11802) then
			meanbaserateimacadj*tempadjusttermb*exp(tempadjustterma*(72.0-least(temperature,72)))+tempadjusttermc
		else
			meanbaserateimacadj + generalfraction * 
			case when starttempequationtype = 'LOG' then
				(tempadjusttermb*exp(tempadjustterma*(least(temperature,75)-75))+ tempadjusttermc)	   
			when starttempequationtype = 'POLY' then
				(least(temperature,75)-75) * (tempadjustterma+(least(temperature,75)-75) * (tempadjusttermb+(least(temperature,75)-75) * tempadjusttermc)) 
			else
				(least(temperature,75)-75) * (tempadjustterma+(least(temperature,75)-75) * (tempadjusttermb+(least(temperature,75)-75) * tempadjusttermc))
			end
		end,
	emissionrateacadj   = 
		case when baserateoutputwithfuel.polprocessid in (11202,11802) then
			emissionrateacadj*tempadjusttermb*exp(tempadjustterma*(72.0-least(temperature,72)))+tempadjusttermc
		else
			emissionrateacadj   + generalfractionrate * 
			case when starttempequationtype = 'LOG' then
				(tempadjusttermb*exp(tempadjustterma*(least(temperature,75)-75))+ tempadjusttermc)	   
			when starttempequationtype = 'POLY' then
				(least(temperature,75)-75) * (tempadjustterma+(least(temperature,75)-75) * (tempadjusttermb+(least(temperature,75)-75) * tempadjusttermc)) 
			else
				(least(temperature,75)-75) * (tempadjustterma+(least(temperature,75)-75) * (tempadjusttermb+(least(temperature,75)-75) * tempadjusttermc))
			end
		end,
	emissionrateimacadj = 
		case when baserateoutputwithfuel.polprocessid in (11202,11802) then
			emissionrateimacadj*tempadjusttermb*exp(tempadjustterma*(72.0-least(temperature,72)))+tempadjusttermc
		else
			emissionrateimacadj + generalfractionrate * 
			case when starttempequationtype = 'LOG' then
				(tempadjusttermb*exp(tempadjustterma*(least(temperature,75)-75))+ tempadjusttermc)	   
			when starttempequationtype = 'POLY' then
				(least(temperature,75)-75) * (tempadjustterma+(least(temperature,75)-75) * (tempadjusttermb+(least(temperature,75)-75) * tempadjusttermc)) 
			else
				(least(temperature,75)-75) * (tempadjustterma+(least(temperature,75)-75) * (tempadjusttermb+(least(temperature,75)-75) * tempadjusttermc))
			end
		end
where baserateoutputwithfuel.polprocessid=starttempadjustment.polprocessid
and baserateoutputwithfuel.fueltypeid=starttempadjustment.fueltypeid
and baserateoutputwithfuel.opmodeid=starttempadjustment.opmodeid
and baserateoutputwithfuel.modelyearid=pollutantprocessmappedmodelyear.modelyearid
and starttempadjustment.polprocessid=pollutantprocessmappedmodelyear.polprocessid
and starttempadjustment.modelyeargroupid=pollutantprocessmappedmodelyear.modelyeargroupid;
-- end section process2

-- @algorithm apply the county's gpafract to the general fuel adjustment.
-- fueleffectratio=ifnull(fueleffectratio,1)+gpafract*(ifnull(fueleffectratiogpa,1)-ifnull(fueleffectratio,1))
update generalfuelratio, county
set fueleffectratio=ifnull(fueleffectratio,1)+gpafract*(ifnull(fueleffectratiogpa,1)-ifnull(fueleffectratio,1));

-- apply generalfuelratio to baserateoutputwithfuel
alter table generalfuelratio add key (fueltypeid, fuelformulationid, sourcetypeid, polprocessid);

-- @algorithm apply generalfuelratio to baserateoutputwithfuel. rate = rate * fueleffectratio.
-- fueleffectratio is the gpa-weighted generalfuelratio.
update baserateoutputwithfuel, generalfuelratio
set meanbaserate=meanbaserate*fueleffectratio, meanbaserateim=meanbaserateim*fueleffectratio,
	emissionrate=emissionrate*fueleffectratio, emissionrateim=emissionrateim*fueleffectratio,
	meanbaserateacadj=meanbaserateacadj*fueleffectratio, meanbaserateimacadj=meanbaserateimacadj*fueleffectratio,
	emissionrateacadj=emissionrateacadj*fueleffectratio, emissionrateimacadj=emissionrateimacadj*fueleffectratio
where generalfuelratio.fueltypeid = baserateoutputwithfuel.fueltypeid
and generalfuelratio.fuelformulationid = baserateoutputwithfuel.fuelformulationid
and generalfuelratio.polprocessid = baserateoutputwithfuel.polprocessid
and generalfuelratio.minmodelyearid <= baserateoutputwithfuel.modelyearid
and generalfuelratio.maxmodelyearid >= baserateoutputwithfuel.modelyearid
and generalfuelratio.minageid <= baserateoutputwithfuel.yearid - baserateoutputwithfuel.modelyearid
and generalfuelratio.maxageid >= baserateoutputwithfuel.yearid - baserateoutputwithfuel.modelyearid
and generalfuelratio.sourcetypeid = baserateoutputwithfuel.sourcetypeid;

-- create table step2 select * from baserateoutputwithfuel;

-- section process1_2
-- @algorithm apply the county's gpafract to the criteriaratio fuel adjustment.
-- criteria ratio = ifnull(ratio,1)+gpafract*(ifnull(ratiogpa,1)-ifnull(ratio,1))
-- @condition running exhaust (1) and start exhaust (2).
update criteriaratio, county
set ratio=ifnull(ratio,1)+gpafract*(ifnull(ratiogpa,1)-ifnull(ratio,1));

alter table criteriaratio add key speed1 (fueltypeid,fuelformulationid,polprocessid,sourcetypeid,modelyearid,ageid);
analyze table criteriaratio;

-- @algorithm apply criteriaratio to baserateoutputwithfuel. 
-- rate = rate * criteria ratio[fueltypeid,fuelformulationid,polprocessid,sourcetypeid,modelyearid,ageid]
-- @condition running exhaust (1) and start exhaust (2).
update baserateoutputwithfuel, criteriaratio
set meanbaserate=meanbaserate*ratio, meanbaserateim=meanbaserateim*ratio,
	emissionrate=emissionrate*ratio, emissionrateim=emissionrateim*ratio,
	meanbaserateacadj=meanbaserateacadj*ratio, meanbaserateimacadj=meanbaserateimacadj*ratio,
	emissionrateacadj=emissionrateacadj*ratio, emissionrateimacadj=emissionrateimacadj*ratio
where criteriaratio.fueltypeid = baserateoutputwithfuel.fueltypeid
and criteriaratio.fuelformulationid = baserateoutputwithfuel.fuelformulationid
and criteriaratio.polprocessid = baserateoutputwithfuel.polprocessid
and criteriaratio.sourcetypeid = baserateoutputwithfuel.sourcetypeid
and criteriaratio.modelyearid = baserateoutputwithfuel.modelyearid
and criteriaratio.ageid = baserateoutputwithfuel.yearid - baserateoutputwithfuel.modelyearid;

-- create table step3 select * from baserateoutputwithfuel;

-- end section process1_2

-- apply temperature effects
-- section notprocess2
alter table baserateoutputwithfuel add column temperature float null;
alter table baserateoutputwithfuel add column specifichumidity float null;
alter table baserateoutputwithfuel add column k float null;
alter table baserateoutputwithfuel add column heatindex float null;

-- @algorithm calculate humidity adjustment factor k.
-- k = 1.0 - ((greatest(21.0, least(specifichumidity, 124.0))) - 75.0) * humiditycorrectioncoeff
-- @condition not start exhaust (2).
update baserateoutputwithfuel, zonemonthhour, fueltype
set baserateoutputwithfuel.temperature = zonemonthhour.temperature,
	baserateoutputwithfuel.specifichumidity = zonemonthhour.specifichumidity,
	baserateoutputwithfuel.k = 1.0 - ((greatest(21.0, least(zonemonthhour.specifichumidity, 124.0))) - 75.0) * fueltype.humiditycorrectioncoeff,
	baserateoutputwithfuel.heatindex = zonemonthhour.heatindex
where baserateoutputwithfuel.zoneid = zonemonthhour.zoneid
and baserateoutputwithfuel.monthid = zonemonthhour.monthid
and baserateoutputwithfuel.hourid = zonemonthhour.hourid
and baserateoutputwithfuel.fueltypeid = fueltype.fueltypeid;

-- create table step4 select * from baserateoutputwithfuel;

-- end section notprocess2

-- @algorithm apply temperature adjustment.
-- for processes (1,2) and pollutants (118,112): rate=rate*exp((case when temperature <= 72.0 then tempadjustterma*(72.0-temperature) else 0 end)).
-- for all others: rate=rate*((1.0 + (temperature-75)*(tempadjustterma + (temperature-75)*tempadjusttermb))*if(baserateoutputwithfuel.processid in (1,90,91),if(baserateoutputwithfuel.pollutantid=3,k,1.0),1.0)).
-- @input temperatureadjustment
-- @output baserateoutputwithfuel
update baserateoutputwithfuel, temperatureadjustment
set 
	meanbaserate=meanbaserate*
		case when (processid in (1,2) and pollutantid in (118,112) and modelyearid between minmodelyearid and maxmodelyearid) then
			exp((case when temperature <= 72.0 then tempadjustterma*(72.0-temperature) else 0 end))
		else
			(1.0 + (temperature-75)*(tempadjustterma + (temperature-75)*tempadjusttermb))
			*if(baserateoutputwithfuel.processid in (1,90,91),if(baserateoutputwithfuel.pollutantid=3,k,1.0),1.0)
		end,
	meanbaserateim=meanbaserateim*
		case when (processid in (1,2) and pollutantid in (118,112) and modelyearid between minmodelyearid and maxmodelyearid) then
			exp((case when temperature <= 72.0 then tempadjustterma*(72.0-temperature) else 0 end))
		else
			(1.0 + (temperature-75)*(tempadjustterma + (temperature-75)*tempadjusttermb))
			*if(baserateoutputwithfuel.processid in (1,90,91),if(baserateoutputwithfuel.pollutantid=3,k,1.0),1.0)
		end,
	emissionrate=emissionrate*
		case when (processid in (1,2) and pollutantid in (118,112) and modelyearid between minmodelyearid and maxmodelyearid) then
			exp((case when temperature <= 72.0 then tempadjustterma*(72.0-temperature) else 0 end))
		else
			(1.0 + (temperature-75)*(tempadjustterma + (temperature-75)*tempadjusttermb))
			*if(baserateoutputwithfuel.processid in (1,90,91),if(baserateoutputwithfuel.pollutantid=3,k,1.0),1.0)
		end,
	emissionrateim=emissionrateim*
		case when (processid in (1,2) and pollutantid in (118,112) and modelyearid between minmodelyearid and maxmodelyearid) then
			exp((case when temperature <= 72.0 then tempadjustterma*(72.0-temperature) else 0 end))
		else
			(1.0 + (temperature-75)*(tempadjustterma + (temperature-75)*tempadjusttermb))
			*if(baserateoutputwithfuel.processid in (1,90,91),if(baserateoutputwithfuel.pollutantid=3,k,1.0),1.0)
		end,
	meanbaserateacadj=meanbaserateacadj*
		case when (processid in (1,2) and pollutantid in (118,112) and modelyearid between minmodelyearid and maxmodelyearid) then
			exp((case when temperature <= 72.0 then tempadjustterma*(72.0-temperature) else 0 end))
		else
			(1.0 + (temperature-75)*(tempadjustterma + (temperature-75)*tempadjusttermb))
			*if(baserateoutputwithfuel.processid in (1,90,91),if(baserateoutputwithfuel.pollutantid=3,k,1.0),1.0)
		end,
	meanbaserateimacadj=meanbaserateimacadj*
		case when (processid in (1,2) and pollutantid in (118,112) and modelyearid between minmodelyearid and maxmodelyearid) then
			exp((case when temperature <= 72.0 then tempadjustterma*(72.0-temperature) else 0 end))
		else
			(1.0 + (temperature-75)*(tempadjustterma + (temperature-75)*tempadjusttermb))
			*if(baserateoutputwithfuel.processid in (1,90,91),if(baserateoutputwithfuel.pollutantid=3,k,1.0),1.0)
		end,
	emissionrateacadj=emissionrateacadj*
		case when (processid in (1,2) and pollutantid in (118,112) and modelyearid between minmodelyearid and maxmodelyearid) then
			exp((case when temperature <= 72.0 then tempadjustterma*(72.0-temperature) else 0 end))
		else
			(1.0 + (temperature-75)*(tempadjustterma + (temperature-75)*tempadjusttermb))
			*if(baserateoutputwithfuel.processid in (1,90,91),if(baserateoutputwithfuel.pollutantid=3,k,1.0),1.0)
		end,
	emissionrateimacadj=emissionrateimacadj*
		case when (processid in (1,2) and pollutantid in (118,112) and modelyearid between minmodelyearid and maxmodelyearid) then
			exp((case when temperature <= 72.0 then tempadjustterma*(72.0-temperature) else 0 end))
		else
			(1.0 + (temperature-75)*(tempadjustterma + (temperature-75)*tempadjusttermb))
			*if(baserateoutputwithfuel.processid in (1,90,91),if(baserateoutputwithfuel.pollutantid=3,k,1.0),1.0)
		end
where baserateoutputwithfuel.polprocessid = temperatureadjustment.polprocessid
and baserateoutputwithfuel.fueltypeid = temperatureadjustment.fueltypeid 
and modelyearid between temperatureadjustment.minmodelyearid and temperatureadjustment.maxmodelyearid;

-- create table step5_baserateoutputwithfuel select * from baserateoutputwithfuel

-- section notprocess2
-- apply air conditioning to baserateoutputwithfuel
-- build the ac update in two steps.  first set the zoneacfactor (hour, source, modelyear).
-- then multiply the factor by the full ac adjustment addition (i.e. fullacadjustment-1).
-- when all done, change the emissions for any non-zero factor.
update baserateoutputwithfuel set generalfraction = 0, generalfractionrate = 0;

-- @algorithm generalfraction = acfactor[hourid,sourcetypeid,modelyearid].
-- @condition not start exhaust (2).
-- @input zoneacfactor
-- @output baserateoutputwithfuel
update baserateoutputwithfuel, zoneacfactor
set generalfraction = acfactor
where baserateoutputwithfuel.hourid = zoneacfactor.hourid
and baserateoutputwithfuel.sourcetypeid = zoneacfactor.sourcetypeid
and baserateoutputwithfuel.modelyearid = zoneacfactor.modelyearid;

-- @algorithm meanbaserate = meanbaserate + (meanbaserateacadj * generalfraction[hourid,sourcetypeid,modelyearid]).
-- meanbaserateim = meanbaserateim + (meanbaserateimacadj * generalfraction[hourid,sourcetypeid,modelyearid]).
-- emissionrate = emissionrate + (emissionrateacadj * generalfraction[hourid,sourcetypeid,modelyearid]).
-- emissionrateim = emissionrateim + (emissionrateimacadj * generalfraction[hourid,sourcetypeid,modelyearid]).
-- @condition not start exhaust (2).
update baserateoutputwithfuel
set meanbaserate = meanbaserate + (meanbaserateacadj*generalfraction),
	meanbaserateim = meanbaserateim + (meanbaserateimacadj*generalfraction),
	emissionrate = emissionrate + (emissionrateacadj*generalfraction),
	emissionrateim = emissionrateim + (emissionrateimacadj*generalfraction)
where generalfraction <> 0;

-- create table step6_baserateoutputwithfuel select * from baserateoutputwithfuel

-- end section notprocess2

-- @algorithm apply i/m programs to baserateoutputwithfuel.
-- meanbaserate=meanbaserateim*imadjustfract + meanbaserate*(1-imadjustfract).
-- emissionrate=emissionrateim*imadjustfract + emissionrate*(1-imadjustfract).
-- @input imcoveragemergedungrouped
-- @output baserateoutputwithfuel
update baserateoutputwithfuel, imcoveragemergedungrouped
set
	meanbaserate=greatest(meanbaserateim*imadjustfract + meanbaserate*(1.0-imadjustfract),0.0),
	emissionrate=greatest(emissionrateim*imadjustfract + emissionrate*(1.0-imadjustfract),0.0)
where baserateoutputwithfuel.processid = imcoveragemergedungrouped.processid
	and baserateoutputwithfuel.pollutantid = imcoveragemergedungrouped.pollutantid
	and baserateoutputwithfuel.modelyearid = imcoveragemergedungrouped.modelyearid
	and baserateoutputwithfuel.fueltypeid = imcoveragemergedungrouped.fueltypeid
	and baserateoutputwithfuel.sourcetypeid = imcoveragemergedungrouped.sourcetypeid;

-- create table step8 select * from baserateoutputwithfuel;

-- section process1_2
-- handle e85 thc that is created from e10'S RVP INSTEAD OF E85's rvp.

-- @algorithm handle e85 thc that is created from e10'S RVP INSTEAD OF E85's rvp.
-- weight the fuel effect ratio by the county's gpa fraction
-- alt criteria ratio=ifnull(ratio,1)+gpafract*(ifnull(ratiogpa,1)-ifnull(ratio,1))
-- @condition running exhaust (1) and start exhaust (2).
-- @input county
-- @output altcriteriaratio
update altcriteriaratio, county
set ratio=ifnull(ratio,1)+gpafract*(ifnull(ratiogpa,1)-ifnull(ratio,1));

-- @algorithm determine the scaling effect of e10-rvp-based fuel effects to e85-rvp-based fuel effects.
-- alt criteria ratio = alt criteria ratio / criteria ratio.
-- @condition running exhaust (1) and start exhaust (2).
-- @input criteriaratio
-- @output altcriteriaratio
update altcriteriaratio, criteriaratio
set altcriteriaratio.ratio = case when criteriaratio.ratio > 0 then altcriteriaratio.ratio / criteriaratio.ratio else 0 end
where altcriteriaratio.fueltypeid = criteriaratio.fueltypeid
and altcriteriaratio.fuelformulationid = criteriaratio.fuelformulationid
and altcriteriaratio.polprocessid = criteriaratio.polprocessid
and altcriteriaratio.sourcetypeid = criteriaratio.sourcetypeid
and altcriteriaratio.modelyearid = criteriaratio.modelyearid
and altcriteriaratio.ageid = criteriaratio.ageid;

alter table altcriteriaratio add key speed1 (fueltypeid,fuelformulationid,polprocessid,sourcetypeid,modelyearid,ageid);
analyze table altcriteriaratio;

-- @algorithm make thc records from the e10 rvp by using the e85-based thc.
-- the output pollutant is 10001.
-- rate for pollutant 10001 = rate * alt criteria ratio.
-- @condition running exhaust (1) and start exhaust (2).
insert into baserateoutputwithfuel (movesrunid, iterationid,
	zoneid, 
	sourcetypeid, roadtypeid, avgspeedbinid, hourdayid, hourid, dayid,
	pollutantid, processid,
	polprocessid,
	modelyearid, 
	yearid, monthid,
	fueltypeid,
	fuelsubtypeid,
	fuelformulationid,
	fuelmarketshare,
	regclassid,
	opmodeid, generalfraction, generalfractionrate,
	meanbaserate,
	emissionrate)
select b.movesrunid, b.iterationid,
	b.zoneid, 
	b.sourcetypeid, b.roadtypeid, b.avgspeedbinid, b.hourdayid, b.hourid, b.dayid,
	(10000 + b.pollutantid) as pollutantid, b.processid,
	((10000 + b.pollutantid)*100 + b.processid) as polprocessid,
	b.modelyearid, 
	b.yearid, b.monthid,
	b.fueltypeid,
	b.fuelsubtypeid,
	b.fuelformulationid,
	b.fuelmarketshare,
	b.regclassid,
	b.opmodeid, b.generalfraction, b.generalfractionrate,
	meanbaserate*ratio,
	emissionrate*ratio
from baserateoutputwithfuel b
inner join altcriteriaratio a on (
	b.fueltypeid = a.fueltypeid
	and b.fuelformulationid = a.fuelformulationid
	and b.polprocessid = a.polprocessid
	and b.sourcetypeid = a.sourcetypeid
	and b.modelyearid = a.modelyearid
	and a.ageid = b.yearid - b.modelyearid)
where b.fuelsubtypeid in (51,52)
and b.modelyearid >= 2001;

-- end section process1_2

-- section emissionrateadjustment

-- @algorithm emissionrate=emissionrate*emissionrateadjustment,
-- meanbaserate=meanbaserate*emissionrateadjustment
update baserateoutputwithfuel, emissionrateadjustmentworker
set
	emissionrate=emissionrate*emissionrateadjustment,
	meanbaserate=meanbaserate*emissionrateadjustment
where baserateoutputwithfuel.polprocessid = emissionrateadjustmentworker.polprocessid
	and baserateoutputwithfuel.sourcetypeid = emissionrateadjustmentworker.sourcetypeid
	and baserateoutputwithfuel.fueltypeid = emissionrateadjustmentworker.fueltypeid
	and baserateoutputwithfuel.regclassid = emissionrateadjustmentworker.regclassid
	and baserateoutputwithfuel.modelyearid = emissionrateadjustmentworker.modelyearid;

-- end section emissionrateadjustment

alter table baserateoutputwithfuel add key (
	sourcetypeid, avgspeedbinid, hourdayid,
	pollutantid,
	modelyearid, 
	fueltypeid,
	regclassid);

analyze table baserateoutputwithfuel;

-- @algorithm remove fuel formulation and opmodeid from baserateoutputwithfuel, filling baserateoutput.
-- note: this top-level calculator executes at the month level. that means there will be exactly one
-- distinct value in each of these columns:
-- processid, stateid, countyid, zoneid, linkid, roadtypeid, yearid, monthid.
-- as such, these columns do not need to be indexed or included in a group by.
insert into baserateoutput (movesrunid, iterationid,
	zoneid, 
	sourcetypeid, roadtypeid, avgspeedbinid, hourdayid,
	pollutantid, processid,
	modelyearid, 
	yearid, monthid,
	fueltypeid,
	regclassid,
	meanbaserate,
	emissionrate)
select
	movesrunid, iterationid,
	zoneid, 
	sourcetypeid, roadtypeid, avgspeedbinid, hourdayid,
	pollutantid, processid,
	modelyearid, 
	yearid, monthid,
	fueltypeid,
	regclassid,
	sum(fuelmarketshare*meanbaserate) as meanbaserate,
	sum(fuelmarketshare*emissionrate) as emissionrate
from baserateoutputwithfuel
group by 
	sourcetypeid, avgspeedbinid, hourdayid,
	pollutantid,
	modelyearid, 
	fueltypeid,
	regclassid;

-- section getactivity
-- @algorithm ensure all activity slots have data. use a default value of 0
-- when not provided by the input table.
insert ignore into universalactivity (hourdayid, modelyearid, sourcetypeid, activity)
select hourdayid, modelyearid, sourcetypeid, 0
from runspechourday, runspecmodelyearage, runspecsourcetype;
-- end section getactivity

-- section aggregatesmfr
drop table if exists activitytotal;
drop table if exists activitydetail;

create table if not exists activitydetail (
	hourdayid smallint not null,
	modelyearid smallint not null,
	sourcetypeid smallint not null,
	fueltypeid smallint not null,
	regclassid smallint not null,
	activity double,
	activityrates double,
	primary key (sourcetypeid, hourdayid, modelyearid, fueltypeid, regclassid),
	key (hourdayid, modelyearid, fueltypeid, regclassid)
);

insert into activitydetail(hourdayid,modelyearid,sourcetypeid,fueltypeid,regclassid,activity,activityrates)
select u.hourdayid,u.modelyearid,u.sourcetypeid,fueltypeid,regclassid,
	sum(activity*sbdtotal) as activity,
	sum(activity*sbdtotal) as activityrates
from smfrsbdsummary s
inner join universalactivity u using (sourcetypeid, modelyearid)
group by u.sourcetypeid, u.hourdayid, u.modelyearid, fueltypeid, regclassid
order by null;

-- section adjustapuemissionrate

-- @algorithm when aggregating apu emission rates to remove source type, model year, fuel type, or regclass,
-- the activity used to weight the rates must be adjusted. the input activity includes extended idling
-- and instead must be restricted to just hours spent using a diesel apu. this is a model year-based effect.
update activitydetail, apuemissionratefraction
	set activityrates = activityrates / hourfractionadjust
where apuemissionratefraction.modelyearid = activitydetail.modelyearid
and hourfractionadjust != 0;
-- end section adjustapuemissionrate

create table activitytotal (
	hourdayid smallint not null,
	sourcetypeid smallint not null,
	modelyearid smallint not null,
	fueltypeid smallint not null,
	regclassid smallint not null,
	activitytotal double,
	activityratestotal double,
	primary key (hourdayid, modelyearid, sourcetypeid, fueltypeid, regclassid),
	key (hourdayid, sourcetypeid, modelyearid, fueltypeid, regclassid)
);

insert into activitytotal (hourdayid,sourcetypeid,modelyearid,fueltypeid,regclassid,activitytotal,activityratestotal)
select hourdayid
	##activitytotalselect##
	, sum(activity) as activitytotal
	, sum(activityrates) as activityratestotal
from activitydetail u
group by hourdayid
	##activitytotalgroup##
order by null;

drop table if exists activityweight;

create table if not exists activityweight (
	hourdayid smallint not null,
	sourcetypeid smallint not null,
	modelyearid smallint not null,
	fueltypeid smallint not null,
	regclassid smallint not null,
	smfrfraction double,
	smfrratesfraction double,
	primary key (sourcetypeid, hourdayid, modelyearid, fueltypeid, regclassid),
	key (modelyearid, sourcetypeid, hourdayid, fueltypeid, regclassid),
	key (hourdayid, sourcetypeid, modelyearid, fueltypeid, regclassid)
);

-- @algorithm when aggregating rates to remove source type, model year, fuel type, or regclass, calculate an activity distribution.
-- smfrfraction[sourcetypeid,modelyearid,hourdayid,fueltypeid,regclassid] = activity[sourcetypeid,modelyearid,hourdayid,fueltypeid,regclassid] / activitytotal[aggregated]
insert into activityweight (hourdayid,sourcetypeid,modelyearid,fueltypeid,regclassid,smfrfraction,smfrratesfraction)
select u.hourdayid, u.sourcetypeid, u.modelyearid, u.fueltypeid, u.regclassid,
	case when activitytotal > 0 then activity/activitytotal else 0.0 end as smfrfraction,
	case when activityratestotal > 0 then activityrates/activityratestotal else 0.0 end as smfrratesfraction
from activitydetail u
inner join activitytotal t using (hourdayid
	##activityweightjoin##
);

-- section adjustemissionrateonly
-- @algorithm when aggregating rates to remove source type, model year, fuel type, or regclass, weight emissions by the activity distribution.
-- baserateoutput = baserateoutput * smfrfraction[sourcetypeid,modelyearid,hourdayid,fueltypeid,regclassid]
update baserateoutput, activityweight
set
	emissionrate=emissionrate*smfrratesfraction
where baserateoutput.modelyearid = activityweight.modelyearid
	and baserateoutput.sourcetypeid = activityweight.sourcetypeid
	and baserateoutput.hourdayid = activityweight.hourdayid
	and baserateoutput.fueltypeid = activityweight.fueltypeid
	and baserateoutput.regclassid = activityweight.regclassid;
-- end section adjustemissionrateonly

-- section adjustmeanbaserateandemissionrate
-- @algorithm when aggregating rates to remove source type, model year, fuel type, or regclass, weight emissions by the activity distribution.
-- baserateoutput = baserateoutput * smfrfraction[sourcetypeid,modelyearid,hourdayid,fueltypeid,regclassid]
update baserateoutput, activityweight
set
	meanbaserate=meanbaserate*smfrfraction,
	emissionrate=emissionrate*smfrratesfraction
where baserateoutput.modelyearid = activityweight.modelyearid
	and baserateoutput.sourcetypeid = activityweight.sourcetypeid
	and baserateoutput.hourdayid = activityweight.hourdayid
	and baserateoutput.fueltypeid = activityweight.fueltypeid
	and baserateoutput.regclassid = activityweight.regclassid;
-- end section adjustmeanbaserateandemissionrate

-- end section aggregatesmfr

-- section applyactivity

-- @algorithm when creating an inventory or certain rates, convert baserateoutput to an inventory.
-- baserateoutput.meanbaserate = baserateoutput.meanbaserate * activity[processid,hourdayid,modelyearid,sourcetypeid(,month,year,location)]
update baserateoutput, universalactivity
set
	meanbaserate=meanbaserate*activity
where baserateoutput.processid = ##context.iterprocess.databasekey##
	and baserateoutput.hourdayid = universalactivity.hourdayid
	and baserateoutput.modelyearid = universalactivity.modelyearid
	and baserateoutput.sourcetypeid = universalactivity.sourcetypeid;

-- end section applyactivity

-- ***************golang todo*****************
-- ***************golang todo*****************
-- ***************golang todo*****************

-- @algorithm populate movesworkeroutput from baserateoutput.
insert into movesworkeroutput (movesrunid, iterationid,
	zoneid, linkid, countyid, stateid,
	sourcetypeid, scc, roadtypeid,
	hourid, dayid,
	pollutantid, processid,
	modelyearid, 
	yearid, monthid,
	fueltypeid,
	regclassid,
	emissionquant,
	emissionrate)
select movesrunid, iterationid,
	zoneid, 
	##context.iterlocation.linkrecordid## as linkid, 
	##context.iterlocation.countyrecordid## as countyid, 
	##context.iterlocation.staterecordid## as stateid,
	sourcetypeid, scc, roadtypeid, 
	floor(hourdayid/10) as hourid, mod(hourdayid,10) as dayid,
	pollutantid, processid,
	modelyearid, 
	yearid, monthid,
	fueltypeid,
	regclassid,
	meanbaserate as emissionquant,
	emissionrate
from baserateoutput;

-- end section processing

-- section cleanup
drop table if exists tempbaserateoutput;
drop table if exists baserateoutputwithfuel;
drop table if exists imcoveragemergedungrouped;
drop table if exists zoneacfactor;
drop table if exists localfuelsupply;
drop table if exists modelyearweight;
drop table if exists vmtbymyroadhourfraction;
drop table if exists activitytotal;
drop table if exists activityweight;
drop table if exists apuemissionratefraction;

drop table if exists step1;
drop table if exists step2;
drop table if exists step3;
drop table if exists step4;
drop table if exists step5;
drop table if exists step6;
drop table if exists step7;
drop table if exists step8;

-- end section cleanup

-- section final cleanup

-- remove any debugging pollutants.
delete from movesworkeroutput where pollutantid >= 10000;
delete from baserateoutput where pollutantid >= 10000;

-- section haschainedcalculators
-- @algorithm when chained calculators are used, data must be moved back from movesworkeroutput.
-- avgspeedbinid must be recovered in this process.
-- @condition chained calculators are present.

-- @algorithm remove entries from baserateoutput. these will be reinserted later along with
-- the results from chained calculators.
-- @condition chained calculators are present.
truncate table baserateoutput;

-- section rates
-- section notproject
-- @algorithm populate baserateoutput from movesworkeroutput.
-- @condition non-project domain rates chained calculators are present.
insert into baserateoutput(movesrunid,iterationid,
	zoneid,linkid,sourcetypeid,scc,roadtypeid,
	avgspeedbinid,
	monthid,
	hourdayid,
	pollutantid,processid,modelyearid,yearid,fueltypeid,regclassid,
	meanbaserate,emissionrate)
select movesrunid,iterationid,
	zoneid,linkid,sourcetypeid,scc,roadtypeid,
	mod(linkid,100) as avgspeedbinid,
	monthid,
	(hourid*10 + dayid) as hourdayid,
	pollutantid,processid,modelyearid,yearid,fueltypeid,regclassid,
	emissionquant as meanbaserate,emissionrate
from movesworkeroutput;
-- end section notproject

-- section project
-- @algorithm populate baserateoutput from movesworkeroutput. avgspeedbinid is always 0 in project mode.
-- @condition project domain rates chained calculators are present.
insert into baserateoutput(movesrunid,iterationid,
	zoneid,linkid,sourcetypeid,scc,roadtypeid,
	avgspeedbinid,
	monthid,
	hourdayid,
	pollutantid,processid,modelyearid,yearid,fueltypeid,regclassid,
	meanbaserate,emissionrate)
select movesrunid,iterationid,
	zoneid,linkid,sourcetypeid,scc,roadtypeid,
	0 as avgspeedbinid,
	monthid,
	(hourid*10 + dayid) as hourdayid,
	pollutantid,processid,modelyearid,yearid,fueltypeid,regclassid,
	emissionquant as meanbaserate,emissionrate
from movesworkeroutput;
-- end section project
-- end section rates

-- section inventory
-- @algorithm populate baserateoutput from movesworkeroutput. avgspeedbinid is always 0 in inventory mode.
-- @condition inventory chained calculators are present.
insert into baserateoutput(movesrunid,iterationid,
	zoneid,linkid,sourcetypeid,scc,roadtypeid,
	avgspeedbinid,
	monthid,
	hourdayid,
	pollutantid,processid,modelyearid,yearid,fueltypeid,regclassid,
	meanbaserate,emissionrate)
select movesrunid,iterationid,
	zoneid,linkid,sourcetypeid,scc,roadtypeid,
	0 as avgspeedbinid,
	monthid,
	(hourid*10 + dayid) as hourdayid,
	pollutantid,processid,modelyearid,yearid,fueltypeid,regclassid,
	emissionquant as meanbaserate,emissionrate
from movesworkeroutput;
-- end section inventory
-- end section haschainedcalculators

-- section hasnochainedcalculators
update baserateoutput set linkid=##context.iterlocation.linkrecordid##
where linkid is null or linkid=0;
-- end section hasnochainedcalculators

-- end section final cleanup
