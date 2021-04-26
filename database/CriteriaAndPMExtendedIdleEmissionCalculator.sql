-- version 2013-09-15
-- author wes faler
-- author ed glover
-- author gwo shyu, epa

-- section create remote tables for extracted data

##create.runspecyear##;
truncate runspecyear;

##create.agecategory##;
truncate agecategory;

##create.county##;
truncate county;

##create.emissionratebyage##;
truncate emissionratebyage;

##create.emissionrate##;
truncate emissionrate;

##create.fuelsupply##;
truncate fuelsupply;

##create.fueltype##;
truncate fueltype;

##create.fullacadjustment##;
truncate fullacadjustment;

##create.hourday##;
truncate hourday;

##create.imcoverage##;
truncate imcoverage;

##create.imfactor##;
truncate imfactor;

##create.link##;
truncate link;

##create.modelyear##;
truncate modelyear;

##create.monthgrouphour##;
truncate monthgrouphour;

##create.monthofanyyear##;
truncate monthofanyyear;

##create.pollutantprocessassoc##;
truncate pollutantprocessassoc;

##create.extendedidlehours##;
truncate extendedidlehours;

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

##create.zonemonthhour##;
truncate zonemonthhour;

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

select * into outfile '##emissionrate##'
from emissionrate
where  opmodeid = 200 
       and emissionrate.polprocessid in (##pollutantprocessids##);

select distinct emissionratebyage.* into outfile '##emissionratebyage##'
from emissionratebyage, sourcebindistribution, sourcetypemodelyear, sourcebin, runspecsourcefueltype 
where  runspecsourcefueltype.fueltypeid = sourcebin.fueltypeid
       and emissionratebyage.polprocessid = sourcebindistribution.polprocessid
       and emissionratebyage.sourcebinid = sourcebin.sourcebinid
       and emissionratebyage.sourcebinid = sourcebindistribution.sourcebinid
       and sourcebin.sourcebinid = sourcebindistribution.sourcebinid
       and runspecsourcefueltype.sourcetypeid = sourcetypemodelyear.sourcetypeid
       and sourcebindistribution.sourcetypemodelyearid = sourcetypemodelyear.sourcetypemodelyearid
       and sourcetypemodelyear.modelyearid <= ##context.year## 
       and sourcetypemodelyear.modelyearid >= ##context.year## - 30
       and emissionratebyage.polprocessid in (##pollutantprocessids##);

cache select fuelsupply.* into outfile '##fuelsupply##'
from fuelsupply
inner join runspecmonthgroup on (fuelsupply.monthgroupid = runspecmonthgroup.monthgroupid)
inner join year on (fuelsupply.fuelyearid = year.fuelyearid)
where fuelregionid = ##context.fuelregionid##
       and year.yearid = ##context.year##;

cache select distinct fueltype.* into outfile '##fueltype##'
from fueltype
inner join runspecsourcefueltype on (runspecsourcefueltype.fueltypeid = fueltype.fueltypeid);

select faca.* into outfile '##fullacadjustment##'
from fullacadjustment faca
inner join runspecsourcetype rsst on (rsst.sourcetypeid=faca.sourcetypeid)
inner join runspecpollutantprocess rspp on (rspp.polprocessid=faca.polprocessid)
where rspp.polprocessid in (##pollutantprocessids##);

cache select distinct hourday.* into outfile '##hourday##'
from hourday,runspechour,runspecday
where hourday.dayid = runspecday.dayid and hourday.hourid = runspechour.hourid;

select distinct imcoverage.* into outfile '##imcoverage##'
from imcoverage
inner join runspecsourcefueltype on (runspecsourcefueltype.fueltypeid = imcoverage.fueltypeid
       and runspecsourcefueltype.sourcetypeid = imcoverage.sourcetypeid)
where polprocessid in (##pollutantprocessids##)
and countyid = ##context.iterlocation.countyrecordid## 
and yearid = ##context.year##
and useimyn = 'Y';

select distinct imfactor.* into outfile '##imfactor##'
from imfactor
inner join runspecsourcefueltype on (runspecsourcefueltype.fueltypeid = imfactor.fueltypeid
       and runspecsourcefueltype.sourcetypeid = imfactor.sourcetypeid)
where polprocessid in (##pollutantprocessids##);

cache select link.* into outfile '##link##'
from link 
where roadtypeid = 1 and 
       zoneid = ##context.iterlocation.zonerecordid##;

cache select * into outfile '##modelyear##'
from modelyear;

cache select monthgrouphour.* into outfile '##monthgrouphour##' 
from monthgrouphour inner join runspechour using (hourid);

cache select monthofanyyear.* into outfile '##monthofanyyear##'
from monthofanyyear,runspecmonth
where monthofanyyear.monthid = runspecmonth.monthid;

cache select * into outfile '##pollutantprocessassoc##'
from pollutantprocessassoc
where processid=##context.iterprocess.databasekey##;

select * into outfile '##extendedidlehours##' 
from extendedidlehours 
where yearid = ##context.year## 
       and zoneid = ##context.iterlocation.zonerecordid##;

select distinct sourcebin.* into outfile '##sourcebin##'
from sourcebindistribution, sourcetypemodelyear, sourcebin, runspecsourcefueltype
where polprocessid in (##pollutantprocessids##)
       and sourcebindistribution.sourcetypemodelyearid = sourcetypemodelyear.sourcetypemodelyearid
       and sourcetypemodelyear.sourcetypeid = runspecsourcefueltype.sourcetypeid
       and sourcebindistribution.sourcebinid = sourcebin.sourcebinid
       and sourcebin.fueltypeid = runspecsourcefueltype.fueltypeid;

select distinct sourcebindistribution.* into outfile '##sourcebindistribution##'
from sourcebindistributionfuelusage_##context.iterprocess.databasekey##_##context.iterlocation.countyrecordid##_##context.year## as sourcebindistribution, 
sourcetypemodelyear, sourcebin, runspecsourcefueltype 
where polprocessid in (##pollutantprocessids##)
       and sourcebindistribution.sourcetypemodelyearid = sourcetypemodelyear.sourcetypemodelyearid
       and sourcetypemodelyear.modelyearid <= ##context.year## 
       and sourcetypemodelyear.modelyearid >= ##context.year## - 30
       and sourcetypemodelyear.sourcetypeid = runspecsourcefueltype.sourcetypeid
       and sourcebindistribution.sourcebinid = sourcebin.sourcebinid
       and sourcebin.fueltypeid = runspecsourcefueltype.fueltypeid;

cache select sourcetypeage.* into outfile '##sourcetypeage##'
from sourcetypeage,runspecsourcetype
where sourcetypeage.sourcetypeid = runspecsourcetype.sourcetypeid;

cache select sourcetypemodelyear.* into outfile '##sourcetypemodelyear##'
from sourcetypemodelyear,runspecsourcetype 
where  sourcetypemodelyear.sourcetypeid = runspecsourcetype.sourcetypeid  
       and sourcetypemodelyear.modelyearid <= ##context.year##
       and sourcetypemodelyear.modelyearid >= ##context.year## - 30;

select distinct temperatureadjustment.* into outfile '##temperatureadjustment##'
from temperatureadjustment
inner join runspecsourcefueltype on (runspecsourcefueltype.fueltypeid = temperatureadjustment.fueltypeid)
where polprocessid in (##pollutantprocessids##);

cache select year.* into outfile '##year##'
from year 
where yearid = ##context.year##;

cache select runspecyear.* into outfile '##runspecyear##'
from runspecyear;

cache select * into outfile '##zone##'
from zone
where zoneid = ##context.iterlocation.zonerecordid##;

cache select distinct zonemonthhour.* into outfile '##zonemonthhour##'
from zonemonthhour,runspecmonth,runspechour
where zoneid = ##context.iterlocation.zonerecordid##
and runspecmonth.monthid = zonemonthhour.monthid
and runspechour.hourid = zonemonthhour.hourid;

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

-- section processing

-- ceic-1: calculate temperature and nox humidity adjustments


drop table if exists tappaft;
create table tappaft (
       polprocessid  int not null,
       fueltypeid    smallint not null,
       minmodelyearid       int not null,
       maxmodelyearid       int not null,
       humiditycorrectioncoeff float null,
       tempadjustterma      float null,
       tempadjusttermb  float null
);

create unique index xpktappaft on tappaft
(
       polprocessid asc,
       fueltypeid   asc,
       minmodelyearid       asc,
       maxmodelyearid       asc
);

analyze table tappaft;

insert into tappaft(polprocessid, fueltypeid, minmodelyearid, maxmodelyearid, humiditycorrectioncoeff, tempadjustterma, tempadjusttermb) 
select ta.polprocessid, ta.fueltypeid, ta.minmodelyearid, ta.maxmodelyearid, ft.humiditycorrectioncoeff, ta.tempadjustterma, 
                            ta.tempadjusttermb 
from temperatureadjustment ta 
inner join fueltype ft on (ta.fueltypeid = ft.fueltypeid);

drop table if exists metadjustment;
create table metadjustment (
       zoneid        int  not null,
       monthid              smallint not null,
       hourid        smallint not null,
       polprocessid  int not null,
       fueltypeid    smallint not null,
       modelyearid   smallint not null,
       minmodelyearid       int    not null,
       maxmodelyearid       int not null,
       temperatureadjustment float null,
       k float null,
       temperature float null,
       tempadjustterma float null,
       tempadjusttermb float null,
       specifichumidity float null,
       humiditycorrectioncoeff float null 
);

create unique index xpkmetadjustment on metadjustment
(
       polprocessid     asc,
       modelyearid   asc,
       fueltypeid    asc,
       monthid     asc,
       hourid     asc
);

analyze table metadjustment;

insert into metadjustment(zoneid, monthid, hourid ,polprocessid, fueltypeid, modelyearid, minmodelyearid, maxmodelyearid, 
                     temperatureadjustment, k, temperature, tempadjustterma, tempadjusttermb, specifichumidity, humiditycorrectioncoeff) 
select zmh.zoneid, zmh.monthid, zmh.hourid, ta.polprocessid, ta.fueltypeid, my.modelyearid, ta.minmodelyearid, ta.maxmodelyearid, 
       ((zmh.temperature - 75.0) * ta.tempadjustterma) as temperatureadjustment, 
       (greatest(21.0, least(zmh.specifichumidity, 124.0))) as k,
       zmh.temperature, ta.tempadjustterma, ta.tempadjusttermb, zmh.specifichumidity, ta.humiditycorrectioncoeff 
from zonemonthhour zmh
inner join tappaft ta 
join modelyear my
where zmh.zoneid =   ##context.iterlocation.zonerecordid## 
       and mod(ta.polprocessid,100) = 90
       and my.modelyearid between minmodelyearid and maxmodelyearid;

update metadjustment set temperatureadjustment = 1.0 + tempadjusttermb * (temperature - 75.0) * (temperature - 75.0) + temperatureadjustment;
update metadjustment set k = 1.0 - (k - 75.0) * humiditycorrectioncoeff;

-- flush tables;

drop table if exists tappaft;

-- ceic 2  : caculate ac adjustment factor

-- ceic 2-a: calculate ac on fraction

drop table if exists aconfraction;
create table aconfraction (
       zoneid int not null,
       monthid       smallint not null,
       hourid smallint not null, 
       aconfraction float,
       acactivitytermb float,
       acactivitytermc float,
       heatindex float
); 

create unique index xpkaconfraction on aconfraction
(
       zoneid asc,
       monthid asc,
       hourid asc
);

analyze table aconfraction;

insert into aconfraction(zoneid, monthid, hourid, aconfraction, acactivitytermb, acactivitytermc, heatindex)
select zmh.zoneid, zmh.monthid, zmh.hourid, 
       mgh.acactivityterma as aconfraction, mgh.acactivitytermb, mgh.acactivitytermc, zmh.heatindex 
from zonemonthhour zmh
inner join monthofanyyear may on (may.monthid = zmh.monthid)
inner join monthgrouphour mgh on (mgh.monthgroupid = may.monthgroupid and mgh.hourid = zmh.hourid);

-- flush tables;

update aconfraction set aconfraction = aconfraction + (heatindex * acactivitytermb) + (acactivitytermc * heatindex * heatindex);
update aconfraction set aconfraction = if(aconfraction > 1.0, 1.0, aconfraction);
update aconfraction set aconfraction = if(aconfraction < 0.0, 0.0, aconfraction);

-- flush tables;

-- crec 2-b: calculate ac activity fraction

drop table if exists temp1;
create table temp1 (
       zoneid  int not null,
       yearid smallint not null,
       monthid smallint not null,
       hourid smallint not null,
       aconfraction float
);

create unique index xpktemp1 on temp1 (
       zoneid asc,
       yearid asc,
       monthid asc,
       hourid asc
);

insert into temp1 (zoneid, yearid, monthid, hourid, aconfraction)
select acof.zoneid, ry.yearid, acof.monthid, acof.hourid, acof.aconfraction 
from aconfraction acof
inner join runspecyear ry
where ry.yearid = ##context.year##;

drop table if exists temp2;
create table temp2 (
       zoneid  int not null,
       yearid smallint not null,
       monthid smallint not null,
       hourid smallint not null,
       modelyearid smallint not null,
       sourcetypeid smallint not null,
       aconfraction float,
       acpenetrationfraction float, 
       ageid smallint not null
);

create unique index xpktemp2 on temp2 (
       zoneid asc,
       yearid asc,
       monthid asc,
       hourid asc,
       modelyearid asc,
       sourcetypeid asc
);


insert into temp2 (zoneid, yearid, monthid, hourid, modelyearid, sourcetypeid, aconfraction, acpenetrationfraction, ageid)
select acof.zoneid, acof.yearid, acof.monthid, acof.hourid, stmy.modelyearid, stmy.sourcetypeid, acof.aconfraction, stmy.acpenetrationfraction,
       (acof.yearid - stmy.modelyearid) as ageid
from temp1 acof
inner join sourcetypemodelyear stmy;

-- !!! gwo shyu 11/4/2009  the follwoing lines should equal the above.
-- insert into temp2 (zoneid, yearid, monthid, hourid, modelyearid, sourcetypeid, aconfraction, acpenetrationfraction, ageid)
-- select acof.zoneid, acof.yearid, acof.monthid, acof.hourid, stmy.modelyearid, stmy.sourcetypeid, acof.aconfraction, stmy.acpenetrationfraction,
--     (acof.yearid - stmy.modelyearid) as ageid
-- from temp1 acof, sourcetypemodelyear stmy;

drop table if exists acactivityfraction;
create table acactivityfraction (
       zoneid  int not null,
       yearid smallint not null,
       monthid smallint not null,
       hourid smallint not null,
       sourcetypeid smallint not null,
       modelyearid smallint not null,
       acactivityfraction float
);

create unique index xpkacactivityfraction on acactivityfraction (
       zoneid asc,
       yearid asc,
       monthid asc,
       hourid asc,
       sourcetypeid asc,
       modelyearid asc
);
analyze table acactivityfraction;

insert into acactivityfraction (zoneid, yearid, monthid, hourid, sourcetypeid, modelyearid, acactivityfraction)
select acof.zoneid, acof.yearid, acof.monthid, acof.hourid, acof.sourcetypeid, acof.modelyearid, 
       (acof.aconfraction * acof.acpenetrationfraction * sta.functioningacfraction) as acactivityfraction 
from temp2 acof
inner join sourcetypeage sta on (sta.sourcetypeid = acof.sourcetypeid and sta.ageid=acof.ageid)
order by null;

-- flush tables;

-- ceic 2-c: calculate ac adjustment factor

drop table if exists acadjustment;
create table acadjustment (
       zoneid  int not null,
       monthid  smallint not null,
       hourid  smallint not null,
       sourcetypeid smallint not null,
       modelyearid  smallint not null,
       polprocessid int not null,
       acadjustment float
);

create unique index xpkacadjustment on acadjustment (
       sourcetypeid asc,
       polprocessid asc,
       modelyearid asc,
       monthid asc,
       hourid  asc
);
analyze table acadjustment;

insert ignore into fullacadjustment (sourcetypeid, polprocessid, opmodeid, fullacadjustment)
select distinct sourcetypeid, polprocessid, 200, 1.0
from runspecsourcefueltype, pollutantprocessassoc
where (chainedto1 is null or pollutantid in (118));

insert into acadjustment (zoneid,monthid,hourid,sourcetypeid,modelyearid,polprocessid,acadjustment)
select acaf.zoneid, acaf.monthid, acaf.hourid, 
       acaf.sourcetypeid, acaf.modelyearid, faca.polprocessid,
       (((faca.fullacadjustment - 1.0) * acaf.acactivityfraction) + 1.0) as acadjustment 
from acactivityfraction acaf
inner join fullacadjustment faca on (acaf.sourcetypeid=faca.sourcetypeid
              and faca.opmodeid=200 and mod(faca.polprocessid,100)=90)
order by null;
-- flush tables;

-- ceic-3: sourcebin-weighted weight emission rates

drop table if exists emissionrate2;

create table if not exists emissionrate2(
       sourcebinid   bigint(20) not null,
       polprocessid  int not null,
       opmodeid      smallint not null,
       modelyearid   smallint not null,
       fueltypeid    smallint not null,
       sourcetypeid  smallint not null,
       meanbaserate  float null,
       sourcebinactivityfraction   float null,
       primary key (sourcebinid, polprocessid, opmodeid, modelyearid, fueltypeid, sourcetypeid)
);

truncate table emissionrate2;

insert ignore into emissionrate2 (sourcebinid, polprocessid, opmodeid, meanbaserate, 
       modelyearid, fueltypeid, sourcetypeid, sourcebinactivityfraction) 
select distinct er.sourcebinid, er.polprocessid, er.opmodeid, er.meanbaserate, 
       stmy.modelyearid, sb.fueltypeid, stmy.sourcetypeid, sbd.sourcebinactivityfraction
from emissionrate er, sourcebin sb, runspecsourcefueltype rsft, sourcebindistribution sbd, sourcetypemodelyear stmy
where  
       er.opmodeid          = 200 
       and er.sourcebinid   = sb.sourcebinid 
       and sb.fueltypeid    = rsft.fueltypeid 
       and er.polprocessid  in (##pollutantprocessids##) 
       and sbd.sourcebinid  = sb.sourcebinid 
       and sbd.polprocessid        in (##pollutantprocessids##) 
       and sbd.sourcetypemodelyearid = stmy.sourcetypemodelyearid 
       and stmy.modelyearid        <= ##context.year## 
;

drop table if exists sbweightedemissionrate;
create table sbweightedemissionrate (
       sourcebinid      bigint(20) not null,
       polprocessid  int not null,
       sourcetypeid  smallint not null,
       modelyearid   smallint not null,
       fueltypeid    smallint not null,
       meanbaserate  float
);

create unique index xpksbweightedemissionrate on sbweightedemissionrate (
       polprocessid    asc,
       sourcetypeid    asc,
       modelyearid    asc,
       fueltypeid     asc
);
analyze table sbweightedemissionrate;

insert into sbweightedemissionrate (sourcebinid, polprocessid, sourcetypeid, modelyearid, fueltypeid, meanbaserate)
select er.sourcebinid, er.polprocessid, er.sourcetypeid, er.modelyearid, er.fueltypeid,
       sum(er.sourcebinactivityfraction * er.meanbaserate) as meanbaserate 
from emissionrate2 er
group by er.polprocessid, er.sourcetypeid, er.modelyearid, er.fueltypeid 
order by null;

-- flush tables;

-- ceic-4: apply adjustment factors to emission rates

drop table if exists weightedandadjustedemissionrate;
create table weightedandadjustedemissionrate (
       polprocessid  int not null,
       sourcetypeid  smallint not null,
       modelyearid   smallint not null,
       fueltypeid    smallint not null,
       zoneid        int not null,
       monthid              smallint not null,
       hourid        smallint not null,
       meanbaserate  float
);

-- create unique index xpkweightedandadjustedemissionrate on weightedandadjustedemissionrate (
--        modelyearid     asc,
--        zoneid        asc,
--        monthid          asc,
--        hourid        asc,
--        sourcetypeid     asc
-- );

create unique index xpkweightedandadjustedemissionrate on weightedandadjustedemissionrate (
       polprocessid  asc,
       sourcetypeid  asc,
       modelyearid   asc,
       fueltypeid    asc,
       zoneid        asc,
       monthid       asc,
       hourid        asc
);

create index xpkweightedandadjustedemissionrate_a1 on weightedandadjustedemissionrate (
       zoneid        asc,
       monthid          asc,
       hourid        asc,
       sourcetypeid     asc,
       modelyearid asc
);

analyze table weightedandadjustedemissionrate;

create table if not exists criteriaandpmextendedidleemissions ( somevalue int not null primary key );

-- 11/4/2009 by gwo s. - changed from "left join acadjustment" to "inner join acadjustment" 
--                       because modelyearids with ageids > 31 need to be dropped. 
--                       this is to prevent from null in the later tables.
-- 04/30/2013 by ed g. - modelyearid added to metaadjustment table.

insert into weightedandadjustedemissionrate (polprocessid, sourcetypeid, modelyearid, fueltypeid, zoneid, monthid, hourid, meanbaserate)
select er.polprocessid, er.sourcetypeid, er.modelyearid, er.fueltypeid, 
       ##context.iterlocation.zonerecordid## as zoneid, aca.monthid, aca.hourid,
       (er.meanbaserate * aca.acadjustment * meta.temperatureadjustment * if(ppa.pollutantid=3,meta.k,1.0)) as meanbaserate 
from sbweightedemissionrate er
inner join pollutantprocessassoc ppa on (ppa.polprocessid=er.polprocessid)
inner join acadjustment aca on (aca.sourcetypeid=er.sourcetypeid and aca.polprocessid=er.polprocessid and aca.modelyearid=er.modelyearid)
left join metadjustment meta on (meta.polprocessid=er.polprocessid and meta.fueltypeid=er.fueltypeid and meta.modelyearid=er.modelyearid  
              and meta.monthid=aca.monthid and meta.hourid=aca.hourid)
group by er.polprocessid, er.sourcetypeid, er.modelyearid, er.fueltypeid, aca.monthid, aca.hourid 
order by null;

-- meta.zoneid has already been locked down to the context's zone, same with acadjustment's zone

-- apply humidity effects for nox (pollutant/process 390)
-- update weightedandadjustedemissionrate, zonemonthhour, fueltype
--     set meanbaserate=(1.0 - (greatest(21.0,least(specifichumidity,124.0))-75.0)*humiditycorrectioncoeff)*meanbaserate
-- where weightedandadjustedemissionrate.polprocessid=390
-- and weightedandadjustedemissionrate.zoneid=zonemonthhour.zoneid
-- and weightedandadjustedemissionrate.monthid=zonemonthhour.monthid
-- and weightedandadjustedemissionrate.hourid=zonemonthhour.hourid
-- and weightedandadjustedemissionrate.fueltypeid=fueltype.fueltypeid;


-- flush tables;

-- ceic-5: multiply emission rates by activity

drop table if exists eih2;
create table eih2 (
       zoneid        int not null,
       monthid              smallint not null,
       hourdayid        smallint not null,
       hourid        smallint,
       dayid         smallint,
       yearid        smallint not null,
       ageid         smallint not null,
       sourcetypeid  smallint not null,
       extendedidlehours    float
);

create unique index xpkeih2 on eih2 (
       ageid        asc,
       yearid        asc,
       zoneid        asc,
       monthid          asc,
       hourdayid    asc,
       sourcetypeid    asc);

analyze table eih2;

-- flush tables;

truncate table eih2;


insert into eih2 (zoneid, monthid, hourdayid, hourid, dayid, yearid, ageid, sourcetypeid, extendedidlehours) 
select eih.zoneid, eih.monthid, eih.hourdayid, hrdy.hourid, hrdy.dayid, eih.yearid, eih.ageid, eih.sourcetypeid, eih.extendedidlehours 
from extendedidlehours eih 
inner join hourday hrdy on (hrdy.hourdayid=eih.hourdayid);

-- flush tables;

drop table if exists adjustedemissionresults;
create table adjustedemissionresults (
       polprocessid  int not null,
       sourcetypeid  smallint not null,
       modelyearid   smallint not null,
       fueltypeid    smallint not null,
       zoneid        int not null,
       monthid              smallint not null,
       hourid        smallint not null,
       dayid         smallint not null,
       yearid        smallint not null,
       ageid         smallint not null,
       emissionquant        float
);

create unique index xpkadjustedemissionresults on adjustedemissionresults (
       polprocessid  asc,
       sourcetypeid  asc,
       modelyearid   asc,
       fueltypeid    asc,
       zoneid        asc,
       monthid          asc,
       hourid        asc,
       dayid         asc,
       yearid        asc,
       ageid            asc
);
analyze table adjustedemissionresults;

insert into adjustedemissionresults (polprocessid, sourcetypeid, modelyearid, fueltypeid, zoneid, monthid, 
                            hourid, dayid, yearid, ageid, emissionquant)
select waer.polprocessid, waer.sourcetypeid, waer.modelyearid, waer.fueltypeid, waer.zoneid, waer.monthid, 
                            waer.hourid, eih.dayid, eih.yearid, eih.ageid,  
       (waer.meanbaserate * eih.extendedidlehours) as emissionquant 
from weightedandadjustedemissionrate waer
inner join eih2 eih on (eih.zoneid=waer.zoneid and eih.monthid=waer.monthid  
                     and eih.hourid=waer.hourid and eih.sourcetypeid=waer.sourcetypeid)
inner join runspecyear ry on (eih.yearid=ry.yearid)
where eih.ageid = eih.yearid - waer.modelyearid
group by waer.polprocessid, waer.sourcetypeid, waer.modelyearid, waer.fueltypeid, 
              eih.zoneid, eih.monthid, eih.hourid, eih.dayid, yearid 
order by null;
-- flush tables;

-- ceic-6: convert results to structure of movesworkeroutput by sourcetypeid

drop table if exists movesworkeroutputtmp216;
truncate movesworkeroutput;
insert into movesworkeroutput (
       stateid, countyid, zoneid, linkid, roadtypeid, yearid, monthid, dayid, hourid, pollutantid, 
       processid, sourcetypeid, fueltypeid, modelyearid, scc, emissionquant)
select ##context.iterlocation.staterecordid## as stateid, 
       ##context.iterlocation.countyrecordid## as countyid, aer.zoneid, lnk.linkid, lnk.roadtypeid,
       aer.yearid, aer.monthid, aer.dayid, aer.hourid, ppa.pollutantid, ppa.processid, aer.sourcetypeid, 
       aer.fueltypeid, aer.modelyearid, null as scc, aer.emissionquant as emissionquant 
from adjustedemissionresults aer 
inner join pollutantprocessassoc ppa on (ppa.polprocessid = aer.polprocessid)
inner join link lnk on (lnk.zoneid=aer.zoneid)
where lnk.roadtypeid=1 
order by null;

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

drop table if exists eih2;
drop table if exists temp1;
drop table if exists temp2;
drop table if exists tmp216;
drop table if exists tappaft;
drop table if exists emissionrate2;
drop table if exists metadjustment;
drop table if exists aconfraction;
drop table if exists acactivityfraction;
drop table if exists acadjustment;
drop table if exists sbweightedemissionrate;
drop table if exists weightedandadjustedemissionrate;
drop table if exists movesworkeroutputtmp216;
drop table if exists adjustedemissionresults;
drop table if exists onecountyyeargeneralfuelratio;
drop table if exists criteriaandpmextendedidleemissions;
-- end section cleanup

