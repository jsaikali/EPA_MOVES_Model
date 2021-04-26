-- version 2017-08-26
-- wes faler

-- @algorithm
-- @owner hc speciation calculator
-- @calculator
-- @filenotused

-- section create remote tables for extracted data

-- section oldcode
drop table if exists extagecategory;
create table extagecategory (
  ageid smallint(6) not null default '0',
  agegroupid smallint(6) not null default '0',
  primary key (ageid),
  key agegroupid (agegroupid,ageid),
  key ageid (ageid,agegroupid)
);

drop table if exists extfuelsupply;
create table extfuelsupply (
  countyid smallint not null,
  yearid smallint not null,
  monthid smallint not null,
  fueltypeid smallint not null,
  fuelsubtypeid smallint not null,
  fuelformulationid int not null,
  marketshare double not null
);

drop table if exists extfueltype;
create table extfueltype (
       fueltypeid           smallint not null,
       humiditycorrectioncoeff float null,
     fueldensity        float null,
     subjecttoevapcalculations char(1) not null default 'N'
);

drop table if exists extfuelsubtype;
create table extfuelsubtype (
       fuelsubtypeid        smallint not null,
       fueltypeid           smallint not null,
       fuelsubtypepetroleumfraction float null,
       fuelsubtypefossilfraction float null,
       carboncontent        float null,
       oxidationfraction    float null,
     energycontent    float null,
       key (fueltypeid, fuelsubtypeid)
);

drop table if exists extfuelformulation;
create table extfuelformulation (
    fuelformulationid smallint not null primary key,
    fuelsubtypeid smallint not null,
    rvp float null,
    sulfurlevel float null,
    etohvolume float null,
    mtbevolume float null,
    etbevolume float null,
    tamevolume float null,
    aromaticcontent float null,
    olefincontent float null,
    benzenecontent float null,
    e200 float null,
    e300 float null,
  voltowtpercentoxy float null,
  biodieselestervolume float default null,
  cetaneindex float default null,
  pahcontent float default null,
  t50 float default null,
  t90 float default null,
  key (fuelsubtypeid, fuelformulationid)
);

-- end section oldcode

drop table if exists hcagecategory;
create table hcagecategory (
  ageid smallint(6) not null default '0',
  agegroupid smallint(6) not null default '0',
  agecategoryname char(50) default null,
  primary key (ageid),
  key agegroupid (agegroupid,ageid),
  key ageid (ageid,agegroupid)
);

drop table if exists hcetohbin;
create table if not exists hcetohbin (
  etohthreshid smallint(6) not null default '0',
  etohthreshlow float default null,
  etohthreshhigh float default null,
  etohnominalvalue float default null,
  primary key (etohthreshid),
  key (etohthreshlow, etohthreshhigh, etohthreshid)
);

drop table if exists hcfuelsupply;
create table if not exists hcfuelsupply (
  countyid int(11) not null,
  monthid smallint(6) not null,
  fuelformulationid smallint(6) not null,
  marketshare float default null,
  yearid smallint(6) not null,
  fueltypeid smallint(6) not null,
  fuelsubtypeid smallint(6) not null,
  key (countyid,yearid,monthid,fueltypeid,fuelsubtypeid,fuelformulationid)
);
truncate table hcfuelsupply;

drop table if exists hcfuelformulation;
create table if not exists hcfuelformulation (
    fuelformulationid smallint not null primary key,
    fuelsubtypeid smallint not null,
    rvp float null,
    sulfurlevel float null,
    etohvolume float null,
    mtbevolume float null,
    etbevolume float null,
    tamevolume float null,
    aromaticcontent float null,
    olefincontent float null,
    benzenecontent float null,
    e200 float null,
    e300 float null,
  voltowtpercentoxy float null,
  biodieselestervolume float default null,
  cetaneindex float default null,
  pahcontent float default null,
  oxythreshid int null,
  key (fuelformulationid, fuelsubtypeid, oxythreshid),
  key (fuelsubtypeid, fuelformulationid, oxythreshid),
  key (fuelsubtypeid, oxythreshid, fuelformulationid),
  key (oxythreshid, fuelsubtypeid, fuelformulationid)
);
truncate table hcfuelformulation;

drop table if exists hcoxythreshname;
create table if not exists hcoxythreshname 
(
  oxythreshid       smallint(6)   not null  default '0' primary key
);
truncate table hcoxythreshname;

drop table if exists hcspeciation;
##create.hcspeciation##;
truncate table hcspeciation;

drop table if exists hcpollutantprocessmodelyear;
create table if not exists hcpollutantprocessmodelyear (
    polprocessid int not null ,
    modelyearid smallint not null ,
    modelyeargroupid int not null ,
    fuelmygroupid integer null,
    immodelyeargroupid integer null,
    key (polprocessid),
    key (modelyearid),
    key (fuelmygroupid),
    key (polprocessid, modelyearid, fuelmygroupid)
);
truncate table hcpollutantprocessmodelyear;

drop table if exists hcpollutantprocessmappedmodelyear;
create table if not exists hcpollutantprocessmappedmodelyear (
    polprocessid int not null ,
    modelyearid smallint not null ,
    modelyeargroupid int not null ,
    fuelmygroupid integer null,
    immodelyeargroupid integer null,
    key (polprocessid),
    key (modelyearid),
    key (fuelmygroupid),
    key (polprocessid, modelyearid, fuelmygroupid)
);
truncate table hcpollutantprocessmappedmodelyear;

drop table if exists thcpollutantprocessmodelyear;
create table if not exists thcpollutantprocessmodelyear (
    polprocessid int not null ,
    modelyearid smallint not null ,
    modelyeargroupid int not null ,
    fuelmygroupid integer null,
    immodelyeargroupid integer null,
    key (polprocessid),
    key (modelyearid),
    key (fuelmygroupid),
    key (polprocessid, modelyearid, fuelmygroupid)
);
truncate table thcpollutantprocessmodelyear;

drop table if exists thcpollutantprocessmappedmodelyear;
create table if not exists thcpollutantprocessmappedmodelyear (
    polprocessid int not null ,
    modelyearid smallint not null ,
    modelyeargroupid int not null ,
    fuelmygroupid integer null,
    immodelyeargroupid integer null,
    key (polprocessid),
    key (modelyearid),
    key (fuelmygroupid),
    key (polprocessid, modelyearid, fuelmygroupid)
);
truncate table thcpollutantprocessmappedmodelyear;

drop table if exists hcpollutantprocessassoc;
create table if not exists hcpollutantprocessassoc (
       polprocessid         int not null,
       processid            smallint not null,
       pollutantid          smallint not null,
     isaffectedbyexhaustim char(1) not null default "n",
       isaffectedbyevapim char(1) not null default "n",
       chainedto1 int null default null,
       chainedto2 int null default null,
       key (processid),
       key (pollutantid),
       key (polprocessid),
       key (polprocessid, processid, pollutantid),
       key (pollutantid, processid, polprocessid)
);
truncate table hcpollutantprocessassoc;

##create.methanethcratio##;
truncate table methanethcratio;

-- end section create remote tables for extracted data

-- section extract data

-- section oldcode
cache select ageid, agegroupid
into outfile '##extagecategory##'
from agecategory;

cache select ##context.iterlocation.countyrecordid##, ##context.year##, ##context.monthid##, 
    fst.fueltypeid, fst.fuelsubtypeid, ff.fuelformulationid, fs.marketshare
into outfile '##extfuelsupply##'
from year
inner join fuelsupply fs on (fs.fuelyearid=year.fuelyearid)
inner join monthofanyyear moay on (moay.monthgroupid=fs.monthgroupid)
inner join fuelformulation ff on (ff.fuelformulationid=fs.fuelformulationid)
inner join fuelsubtype fst on (fst.fuelsubtypeid=ff.fuelsubtypeid)
where yearid = ##context.year##
and fs.fuelregionid = ##context.fuelregionid##
and moay.monthid = ##context.monthid##
and fst.fueltypeid in (##macro.csv.all.fueltypeid##);

cache select fueltypeid, humiditycorrectioncoeff, fueldensity, subjecttoevapcalculations
into outfile '##extfueltype##'
from fueltype;

cache select fuelsubtypeid, fueltypeid, fuelsubtypepetroleumfraction, fuelsubtypefossilfraction,
  carboncontent, oxidationfraction, energycontent
into outfile '##extfuelsubtype##'
from fuelsubtype;

cache select distinct
  fuelformulation.fuelformulationid,
  fuelformulation.fuelsubtypeid,
  ifnull(fuelformulation.rvp,0),
  ifnull(fuelformulation.sulfurlevel,0),
  ifnull(fuelformulation.etohvolume,0),
  ifnull(fuelformulation.mtbevolume,0),
  ifnull(fuelformulation.etbevolume,0),
  ifnull(fuelformulation.tamevolume,0),
  ifnull(fuelformulation.aromaticcontent,0),
  ifnull(fuelformulation.olefincontent,0),
  ifnull(fuelformulation.benzenecontent,0),
  ifnull(fuelformulation.e200,0),
  ifnull(fuelformulation.e300,0),
  ifnull(fuelformulation.voltowtpercentoxy,0),
  ifnull(fuelformulation.biodieselestervolume,0),
  ifnull(fuelformulation.cetaneindex,0),
  ifnull(fuelformulation.pahcontent,0),
  ifnull(fuelformulation.t50,0),
  ifnull(fuelformulation.t90,0)
into outfile '##extfuelformulation##'
from fuelsupply
inner join runspecmonthgroup on (fuelsupply.monthgroupid = runspecmonthgroup.monthgroupid)
inner join year on (fuelsupply.fuelyearid = year.fuelyearid)
inner join fuelformulation on (fuelformulation.fuelformulationid = fuelsupply.fuelformulationid)
inner join fuelsubtype on (fuelsubtype.fuelsubtypeid = fuelformulation.fuelsubtypeid)
inner join monthofanyyear on (monthofanyyear.monthgroupid = fuelsupply.monthgroupid)
inner join runspecmonth on (runspecmonth.monthid = monthofanyyear.monthid)
where fuelregionid = ##context.fuelregionid##
and yearid = ##context.year##
and monthofanyyear.monthid = ##context.monthid##
and fuelsubtype.fueltypeid in (##macro.csv.all.fueltypeid##);

cache select *
into outfile '##hcetohbin##'
from etohbin;

cache select oxythreshid
into outfile '##hcoxythreshname##'
from oxythreshname;

-- end section oldcode

-- -----------------------------

cache select *
into outfile '##hcagecategory##'
from agecategory;

cache select ##context.iterlocation.countyrecordid## as countyid, monthofanyyear.monthid, fuelsupply.fuelformulationid, fuelsupply.marketshare, year.yearid, fuelsubtype.fueltypeid, fuelsubtype.fuelsubtypeid
into outfile '##hcfuelsupply##'
from fuelsupply
inner join runspecmonthgroup on (fuelsupply.monthgroupid = runspecmonthgroup.monthgroupid)
inner join year on (fuelsupply.fuelyearid = year.fuelyearid)
inner join fuelformulation on (fuelformulation.fuelformulationid = fuelsupply.fuelformulationid)
inner join fuelsubtype on (fuelsubtype.fuelsubtypeid = fuelformulation.fuelsubtypeid)
inner join monthofanyyear on (monthofanyyear.monthgroupid = fuelsupply.monthgroupid)
inner join runspecmonth on (runspecmonth.monthid = monthofanyyear.monthid)
where fuelregionid = ##context.fuelregionid##
and yearid = ##context.year##;

cache select polprocessid, modelyearid, modelyeargroupid, fuelmygroupid, immodelyeargroupid
into outfile '##hcpollutantprocessmodelyear##'
from pollutantprocessmodelyear
where modelyearid <= ##context.year##
and modelyearid >= ##context.year## - 30
and polprocessid in (##hcpolprocessids##);

cache select polprocessid, modelyearid, modelyeargroupid, fuelmygroupid, immodelyeargroupid
into outfile '##hcpollutantprocessmappedmodelyear##'
from pollutantprocessmappedmodelyear
where modelyearid <= ##context.year##
and modelyearid >= ##context.year## - 30
and polprocessid in (##hcpolprocessids##);

cache select ppmy.polprocessid, ppmy.modelyearid, ppmy.modelyeargroupid, ppmy.fuelmygroupid, ppmy.immodelyeargroupid
into outfile '##thcpollutantprocessmodelyear##'
from pollutantprocessmodelyear ppmy
inner join pollutantprocessassoc ppa using (polprocessid)
where modelyearid <= ##context.year##
and modelyearid >= ##context.year## - 30
and pollutantid = 1;

cache select ppmy.polprocessid, ppmy.modelyearid, ppmy.modelyeargroupid, ppmy.fuelmygroupid, ppmy.immodelyeargroupid
into outfile '##thcpollutantprocessmappedmodelyear##'
from pollutantprocessmappedmodelyear ppmy
inner join pollutantprocessassoc ppa using (polprocessid)
where modelyearid <= ##context.year##
and modelyearid >= ##context.year## - 30
and pollutantid = 1;

cache select hcspeciation.*
into outfile '##hcspeciation##'
from hcspeciation
where polprocessid in (##hcpolprocessids##);

cache select polprocessid,processid,pollutantid,isaffectedbyexhaustim,isaffectedbyevapim,chainedto1,chainedto2
into outfile '##hcpollutantprocessassoc##'
from pollutantprocessassoc
where polprocessid in (##hcpolprocessids##)
or pollutantid = 1;

cache select distinct
  fuelformulation.fuelformulationid,
  fuelformulation.fuelsubtypeid,
  ifnull(fuelformulation.rvp,0),
  ifnull(fuelformulation.sulfurlevel,0),
  ifnull(fuelformulation.etohvolume,0),
  ifnull(fuelformulation.mtbevolume,0),
  ifnull(fuelformulation.etbevolume,0),
  ifnull(fuelformulation.tamevolume,0),
  ifnull(fuelformulation.aromaticcontent,0),
  ifnull(fuelformulation.olefincontent,0),
  ifnull(fuelformulation.benzenecontent,0),
  ifnull(fuelformulation.e200,0),
  ifnull(fuelformulation.e300,0),
  ifnull(fuelformulation.voltowtpercentoxy,0),
  ifnull(fuelformulation.biodieselestervolume,0),
  ifnull(fuelformulation.cetaneindex,0),
  ifnull(fuelformulation.pahcontent,0),
  0 as oxythreshid
into outfile '##hcfuelformulation##'
from fuelsupply
inner join runspecmonthgroup on (fuelsupply.monthgroupid = runspecmonthgroup.monthgroupid)
inner join year on (fuelsupply.fuelyearid = year.fuelyearid)
inner join fuelformulation on (fuelformulation.fuelformulationid = fuelsupply.fuelformulationid)
inner join fuelsubtype on (fuelsubtype.fuelsubtypeid = fuelformulation.fuelsubtypeid)
inner join monthofanyyear on (monthofanyyear.monthgroupid = fuelsupply.monthgroupid)
inner join runspecmonth on (runspecmonth.monthid = monthofanyyear.monthid)
where fuelregionid = ##context.fuelregionid##
and yearid = ##context.year##;

cache select methanethcratio.*
into outfile '##methanethcratio##'
from methanethcratio
where processid in (##hcprocessids##);

-- end section extract data

-- section processing

starttimer savemwo;
savemwo;
starttimer hcspeciationcalculator;

update hcfuelformulation set oxythreshid = (
##oxythreshcase##
);

alter table hcfuelformulation add etohthreshid smallint(6) null default '0';

-- @algorithm assign etohthreshid to each fuel formulation.
-- etohthreshlow <= etohvolume < etohthreshhigh
update hcfuelformulation, hcetohbin set hcfuelformulation.etohthreshid = hcetohbin.etohthreshid
where etohthreshlow <= etohvolume and etohvolume < etohthreshhigh;

-- alter table movesworkeroutput add key hcpollutantid (pollutantid);

-- @algorithm fill in missing hcspeciation entries so that joins to the table are valid.
-- use speciationconstant of 0 and oxyspeciation of 0 for missing entries.
insert ignore into hcspeciation (polprocessid, fuelmygroupid, fuelsubtypeid, etohthreshid, oxythreshid, speciationconstant, oxyspeciation)
select distinct ppmy.polprocessid, ppmy.fuelmygroupid, fs.fuelsubtypeid, etohthreshid, oxythreshid, 0.0 as speciationconstant, 0.0 as oxyspeciation
from hcpollutantprocessmodelyear ppmy,
hcfuelsupply fs,
hcetohbin,
hcoxythreshname;

drop table if exists hcworkeroutput;
create table if not exists hcworkeroutput (
  movesrunid           smallint unsigned not null default 0,
  iterationid      smallint unsigned default 1,
  yearid               smallint unsigned null,
  monthid              smallint unsigned null,
  dayid                smallint unsigned null,
  hourid               smallint unsigned null,
  stateid              smallint unsigned null,
  countyid             integer unsigned null,
  zoneid               integer unsigned null,
  linkid               integer unsigned null,
  pollutantid          smallint unsigned null,
  processid            smallint unsigned null,
  sourcetypeid         smallint unsigned null,
  fueltypeid           smallint unsigned null,
  fuelsubtypeid    smallint unsigned null,
  fuelformulationid  smallint unsigned null,
  modelyearid          smallint unsigned null,
  roadtypeid           smallint unsigned null,
  scc                  char(10) null,
  regclassid       smallint unsigned null,
  emissionquant        float null,
  emissionrate     float null,
  
  key (yearid,monthid,dayid,hourid,stateid,countyid,zoneid,linkid,pollutantid,
      processid,sourcetypeid,fueltypeid,modelyearid,roadtypeid,regclassid)
);
truncate table hcworkeroutput;

drop table if exists hcworkeroutputall;

-- @algorithm hcworkeroutputall holds all data generated by hc speciation, plus thc data used for methane and nmhc.
-- it is for quick lookups, avoiding long scans of movesworkeroutput.  data is first placed into hcworkeroutput
-- then copied to both movesworkeroutput and to hcworkeroutputall.
create table if not exists hcworkeroutputall (
  movesrunid           smallint unsigned not null default 0,
  iterationid      smallint unsigned default 1,
  yearid               smallint unsigned null,
  monthid              smallint unsigned null,
  dayid                smallint unsigned null,
  hourid               smallint unsigned null,
  stateid              smallint unsigned null,
  countyid             integer unsigned null,
  zoneid               integer unsigned null,
  linkid               integer unsigned null,
  pollutantid          smallint unsigned null,
  processid            smallint unsigned null,
  sourcetypeid         smallint unsigned null,
  fueltypeid           smallint unsigned null,
  fuelsubtypeid    smallint unsigned null,
  fuelformulationid  smallint unsigned null,
  modelyearid          smallint unsigned null,
  roadtypeid           smallint unsigned null,
  scc                  char(10) null,
  regclassid       smallint unsigned null,
  emissionquant        float null,
  emissionrate     float null,

  key (pollutantid),
  key (pollutantid, processid),
  key (
    pollutantid asc,
    fueltypeid asc,
    countyid asc, 
    yearid asc,
    monthid asc,
    modelyearid asc, 
    processid asc)
);
truncate table hcworkeroutputall;

-- create index movesworkeroutput_a2 on movesworkeroutput (
--  pollutantid asc,
--  fueltypeid asc,
--  countyid asc, 
--  yearid asc,
--  monthid asc,
--  modelyearid asc, 
--  processid asc
-- );

create index hcfuelsupply_a1 on hcfuelsupply (
  fueltypeid asc, 
  countyid asc, 
  yearid asc, 
  monthid asc, 
  fuelformulationid asc
);

create index hcspeciation_a1 on hcspeciation (
  oxythreshid asc, 
  fuelsubtypeid asc, 
  etohthreshid asc, 
  polprocessid asc, 
  fuelmygroupid asc
);

create index hcetohbin_a1 on hcetohbin (
  etohthreshid asc, 
  etohthreshlow asc, 
  etohthreshhigh asc
);

create index hcpollutantprocessassoc_a1 on hcpollutantprocessassoc (
  processid asc, 
  polprocessid asc, 
  pollutantid asc
);

create index hcfuelformulation_a1 on hcfuelformulation (
  fuelformulationid asc,
  fuelsubtypeid asc
);

-- @algorithm extract thc (1) and altthc (10001) into hcworkeroutputall to make tables faster to search
insert into hcworkeroutputall (
  movesrunid,iterationid,yearid,monthid,dayid,hourid,stateid,countyid,zoneid,linkid,pollutantid,
  processid,sourcetypeid,fueltypeid,modelyearid,roadtypeid,scc,emissionquant,
  regclassid,emissionrate,
  fuelformulationid,fuelsubtypeid
)
select movesrunid,iterationid,mwo.yearid,mwo.monthid,dayid,hourid,stateid,mwo.countyid,zoneid,linkid,pollutantid,
  processid,sourcetypeid,mwo.fueltypeid,modelyearid,roadtypeid,scc,marketshare*emissionquant,
  regclassid,marketshare*emissionrate,
  fuelformulationid,fuelsubtypeid
from movesworkeroutput mwo
inner join hcfuelsupply fs using (countyid, monthid, fueltypeid, yearid)
where pollutantid in (1, 10001);

-- section methane
truncate hcworkeroutput;

-- @algorithm methane = thc * ch4thcratio
insert into hcworkeroutput (
  movesrunid,iterationid,yearid,monthid,dayid,hourid,stateid,countyid,zoneid,linkid,pollutantid,
  processid,sourcetypeid,fueltypeid,modelyearid,roadtypeid,scc,emissionquant,
  emissionrate,
  regclassid,fuelformulationid,fuelsubtypeid
)
select mwo.movesrunid,mwo.iterationid,mwo.yearid,mwo.monthid,mwo.dayid,mwo.hourid,mwo.stateid,mwo.countyid,mwo.zoneid,mwo.linkid,
  5 as pollutantid,
  mwo.processid,mwo.sourcetypeid,mwo.fueltypeid,mwo.modelyearid,mwo.roadtypeid,mwo.scc,
  (emissionquant * ch4thcratio) as emissionquant,
  (emissionrate * ch4thcratio) as emissionrate,
  mwo.regclassid,mwo.fuelformulationid,mwo.fuelsubtypeid
from hcworkeroutputall mwo
inner join hcagecategory acat on (mwo.modelyearid=##context.year##-acat.ageid)
inner join hcpollutantprocessassoc ppa on (
  ppa.processid=mwo.processid
  and ppa.pollutantid=1)
inner join thcpollutantprocessmappedmodelyear ppmy on (
  ppmy.polprocessid=ppa.polprocessid
  and ppmy.modelyearid=mwo.modelyearid)
inner join methanethcratio r on (
  r.processid = mwo.processid
  and r.fueltypeid = mwo.fueltypeid
  and r.sourcetypeid = mwo.sourcetypeid
  and r.modelyeargroupid = ppmy.modelyeargroupid
  and r.agegroupid = acat.agegroupid)
where mwo.pollutantid = 1
and mwo.processid in (##methaneprocessids##);

-- move values back into movesworkeroutput
insert into movesworkeroutput (
  movesrunid,iterationid,yearid,monthid,dayid,hourid,stateid,countyid,zoneid,linkid,pollutantid,
  processid,sourcetypeid,fueltypeid,modelyearid,roadtypeid,scc,regclassid,
  emissionquant,emissionrate)
select movesrunid,iterationid,yearid,monthid,dayid,hourid,stateid,countyid,zoneid,linkid,pollutantid,
  processid,sourcetypeid,fueltypeid,modelyearid,roadtypeid,scc,regclassid,
  sum(emissionquant),sum(emissionrate)
from hcworkeroutput
where emissionquant >= 0
group by yearid,monthid,dayid,hourid,stateid,countyid,zoneid,linkid,pollutantid,
  processid,sourcetypeid,fueltypeid,modelyearid,roadtypeid,regclassid;

insert into hcworkeroutputall (
  movesrunid,iterationid,yearid,monthid,dayid,hourid,stateid,countyid,zoneid,linkid,pollutantid,
  processid,sourcetypeid,fueltypeid,modelyearid,roadtypeid,scc,regclassid,
  emissionquant,emissionrate,
  fuelformulationid,fuelsubtypeid
)
select movesrunid,iterationid,yearid,monthid,dayid,hourid,stateid,countyid,zoneid,linkid,pollutantid,
  processid,sourcetypeid,fueltypeid,modelyearid,roadtypeid,scc,regclassid,
  emissionquant,emissionrate,
  fuelformulationid,fuelsubtypeid
from hcworkeroutput
where emissionquant >= 0;
-- end section methane

-- section nmhc
truncate hcworkeroutput;

-- @algorithm nmhc = thc * (1-ch4thcratio)
insert into hcworkeroutput (
  movesrunid,iterationid,yearid,monthid,dayid,hourid,stateid,countyid,zoneid,linkid,pollutantid,
  processid,sourcetypeid,fueltypeid,modelyearid,roadtypeid,scc,emissionquant,
  emissionrate,
  regclassid,fuelformulationid,fuelsubtypeid
)
select mwo.movesrunid,mwo.iterationid,mwo.yearid,mwo.monthid,mwo.dayid,mwo.hourid,mwo.stateid,mwo.countyid,mwo.zoneid,mwo.linkid,
  79 as pollutantid,
  mwo.processid,mwo.sourcetypeid,mwo.fueltypeid,mwo.modelyearid,mwo.roadtypeid,mwo.scc,
  (emissionquant * (1-ch4thcratio)) as emissionquant,
  (emissionrate * (1-ch4thcratio)) as emissionrate,
  regclassid,fuelformulationid,fuelsubtypeid
from hcworkeroutputall mwo
inner join hcagecategory acat on (mwo.modelyearid=##context.year##-acat.ageid)
inner join hcpollutantprocessassoc ppa on (
  ppa.processid=mwo.processid
  and ppa.pollutantid=1)
inner join thcpollutantprocessmappedmodelyear ppmy on (
  ppmy.polprocessid=ppa.polprocessid
  and ppmy.modelyearid=mwo.modelyearid)
inner join methanethcratio r on (
  r.processid = mwo.processid
  and r.fueltypeid = mwo.fueltypeid
  and r.sourcetypeid = mwo.sourcetypeid
  and r.modelyeargroupid = ppmy.modelyeargroupid
  and r.agegroupid = acat.agegroupid)
where mwo.pollutantid = 1
and mwo.processid in (##nmhcprocessids##);

-- and not (mwo.processid in (1,2) and mwo.fueltypeid=5 
--  and mwo.fuelsubtypeid in (??e85e70fuelsubtypeids??) and mwo.modelyearid >= 2001)

-- @algorithm calculate altnmhc (10079) from altthc (10001) using e10's ratios.
-- altnmhc (pollutant 10079) = altthc (10001) * (1-ch4thcratio[e10 fuel subtype]).
-- @condition running exhaust, start exhaust, ethanol fuel type, e70 and e85 fuel subtypes, model years >= 2001.
insert into hcworkeroutput (
  movesrunid,iterationid,yearid,monthid,dayid,hourid,stateid,countyid,zoneid,linkid,pollutantid,
  processid,sourcetypeid,fueltypeid,modelyearid,roadtypeid,scc,emissionquant,
  emissionrate,
  regclassid,fuelformulationid,fuelsubtypeid
)
select mwo.movesrunid,mwo.iterationid,mwo.yearid,mwo.monthid,mwo.dayid,mwo.hourid,mwo.stateid,mwo.countyid,mwo.zoneid,mwo.linkid,
  10079 as pollutantid,
  mwo.processid,mwo.sourcetypeid,mwo.fueltypeid,mwo.modelyearid,mwo.roadtypeid,mwo.scc,
  (emissionquant * (1-ch4thcratio)) as emissionquant,
  (emissionrate * (1-ch4thcratio)) as emissionrate,
  regclassid,fuelformulationid,fuelsubtypeid
from hcworkeroutputall mwo
inner join hcagecategory acat on (mwo.modelyearid=##context.year##-acat.ageid)
inner join hcpollutantprocessassoc ppa on (
  ppa.processid=mwo.processid
  and ppa.pollutantid=1)
inner join thcpollutantprocessmappedmodelyear ppmy on (
  ppmy.polprocessid=ppa.polprocessid
  and ppmy.modelyearid=mwo.modelyearid)
inner join methanethcratio r on (
  r.processid = mwo.processid
  and r.fueltypeid = 1
  and r.sourcetypeid = mwo.sourcetypeid
  and r.modelyeargroupid = ppmy.modelyeargroupid
  and r.agegroupid = acat.agegroupid)
where mwo.pollutantid = 10001
and mwo.processid in (##nmhcprocessids##)
and (mwo.processid in (1,2) and mwo.fueltypeid=5 
  and mwo.fuelsubtypeid in (##e85e70fuelsubtypeids##) and mwo.modelyearid >= 2001);

-- move values back into movesworkeroutput
insert into movesworkeroutput (
  movesrunid,iterationid,yearid,monthid,dayid,hourid,stateid,countyid,zoneid,linkid,pollutantid,
  processid,sourcetypeid,fueltypeid,modelyearid,roadtypeid,scc,regclassid,
  emissionquant,emissionrate)
select movesrunid,iterationid,yearid,monthid,dayid,hourid,stateid,countyid,zoneid,linkid,pollutantid,
  processid,sourcetypeid,fueltypeid,modelyearid,roadtypeid,scc,regclassid,
  sum(emissionquant),sum(emissionrate)
from hcworkeroutput
where emissionquant >= 0
group by yearid,monthid,dayid,hourid,stateid,countyid,zoneid,linkid,pollutantid,
  processid,sourcetypeid,fueltypeid,modelyearid,roadtypeid,regclassid;

insert into hcworkeroutputall (
  movesrunid,iterationid,yearid,monthid,dayid,hourid,stateid,countyid,zoneid,linkid,pollutantid,
  processid,sourcetypeid,fueltypeid,modelyearid,roadtypeid,scc,regclassid,
  emissionquant,emissionrate,
  fuelformulationid,fuelsubtypeid
)
select movesrunid,iterationid,yearid,monthid,dayid,hourid,stateid,countyid,zoneid,linkid,pollutantid,
  processid,sourcetypeid,fueltypeid,modelyearid,roadtypeid,scc,regclassid,
  emissionquant,emissionrate,
  fuelformulationid,fuelsubtypeid
from hcworkeroutput
where emissionquant >= 0;
-- end section nmhc

-- section nmog
truncate hcworkeroutput;

-- @algorithm nmog = nmhc*(speciationconstant + oxyspeciation* voltowtpercentoxy*(mtbevolume+etbevolume+tamevolume+etohvolume)).
-- @condition when (mtbevolume+etbevolume+tamevolume+etohvolume) >= 0. otherwise, nmog = 0.
-- @condition not (running exhaust, start exhaust, ethanol fuel type, e70 and e85 fuel subtypes, model years >= 2001).
insert into hcworkeroutput (
  movesrunid,iterationid,yearid,monthid,dayid,hourid,stateid,countyid,zoneid,linkid,pollutantid,
  processid,sourcetypeid,fueltypeid,modelyearid,roadtypeid,scc,
  regclassid,fuelformulationid,fuelsubtypeid,
  emissionquant,emissionrate
)
select mwo.movesrunid,mwo.iterationid,mwo.yearid,mwo.monthid,mwo.dayid,mwo.hourid,mwo.stateid,mwo.countyid,mwo.zoneid,mwo.linkid,
  ppa.pollutantid,
  mwo.processid,mwo.sourcetypeid,mwo.fueltypeid,mwo.modelyearid,mwo.roadtypeid,mwo.scc,
  mwo.regclassid,mwo.fuelformulationid,mwo.fuelsubtypeid,
  emissionquant*(
    (case when (mtbevolume+etbevolume+tamevolume+etohvolume) >= 0 then
      (speciationconstant + oxyspeciation* voltowtpercentoxy*(mtbevolume+etbevolume+tamevolume+etohvolume))
     else 0 end)
  ) as emissionquant,
  emissionrate*(
    (case when (mtbevolume+etbevolume+tamevolume+etohvolume) >= 0 then
      (speciationconstant + oxyspeciation* voltowtpercentoxy*(mtbevolume+etbevolume+tamevolume+etohvolume))
     else 0 end)
  ) as emissionrate
from hcworkeroutputall mwo
inner join hcfuelformulation ff on (ff.fuelformulationid=mwo.fuelformulationid)
inner join hcspeciation hcs on (hcs.oxythreshid=ff.oxythreshid and hcs.fuelsubtypeid=ff.fuelsubtypeid and hcs.etohthreshid=ff.etohthreshid)
inner join hcpollutantprocessmodelyear ppmy on (ppmy.polprocessid=hcs.polprocessid
  and ppmy.modelyearid=mwo.modelyearid and ppmy.fuelmygroupid=hcs.fuelmygroupid)
inner join hcpollutantprocessassoc ppa on (ppa.processid=mwo.processid
  and ppa.polprocessid=ppmy.polprocessid
  and ppa.processid in (##nmogprocessids##)
  and ppa.pollutantid = 80)
where mwo.pollutantid = 79
and not (mwo.processid in (1,2) and mwo.fueltypeid=5 
  and mwo.fuelsubtypeid in (##e85e70fuelsubtypeids##) and mwo.modelyearid >= 2001);

-- @algorithm calculate nmog from altnmhc (10079) that originates from altthc (10001). use e10's factors even though the fuel is ethanol.
-- this is done by joining to hcspeciation using e10's values rather than the current fuel formulation's values.
-- nmog = altnmhc*(speciationconstant + oxyspeciation* voltowtpercentoxy*(mtbevolume+etbevolume+tamevolume+10)).
-- @condition running exhaust, start exhaust, ethanol fuel type, e70 and e85 fuel subtypes, model years >= 2001.
insert into hcworkeroutput (
  movesrunid,iterationid,yearid,monthid,dayid,hourid,stateid,countyid,zoneid,linkid,pollutantid,
  processid,sourcetypeid,fueltypeid,modelyearid,roadtypeid,scc,
  regclassid,fuelformulationid,fuelsubtypeid,
  emissionquant,emissionrate
)
select mwo.movesrunid,mwo.iterationid,mwo.yearid,mwo.monthid,mwo.dayid,mwo.hourid,mwo.stateid,mwo.countyid,mwo.zoneid,mwo.linkid,
  ppa.pollutantid,
  mwo.processid,mwo.sourcetypeid,mwo.fueltypeid,mwo.modelyearid,mwo.roadtypeid,mwo.scc,
  mwo.regclassid,mwo.fuelformulationid,mwo.fuelsubtypeid,
  emissionquant*(
    (speciationconstant + oxyspeciation* voltowtpercentoxy*(mtbevolume+etbevolume+tamevolume+10))
  ) as emissionquant,
  emissionrate*(
    (speciationconstant + oxyspeciation* voltowtpercentoxy*(mtbevolume+etbevolume+tamevolume+10))
  ) as emissionrate
from hcworkeroutputall mwo
inner join hcfuelformulation ff on (ff.fuelformulationid=mwo.fuelformulationid)
inner join hcspeciation hcs on (hcs.oxythreshid=0 and hcs.fuelsubtypeid=12 and hcs.etohthreshid=3)
inner join hcpollutantprocessmodelyear ppmy on (ppmy.polprocessid=hcs.polprocessid
  and ppmy.modelyearid=mwo.modelyearid and ppmy.fuelmygroupid=hcs.fuelmygroupid)
inner join hcpollutantprocessassoc ppa on (ppa.processid=mwo.processid
  and ppa.polprocessid=ppmy.polprocessid
  and ppa.processid in (##nmogprocessids##)
  and ppa.pollutantid = 80)
where mwo.pollutantid = 10079
and (mwo.processid in (1,2) and mwo.fueltypeid=5 
  and mwo.fuelsubtypeid in (##e85e70fuelsubtypeids##) and mwo.modelyearid >= 2001);

-- move values back into movesworkeroutput
insert into movesworkeroutput (
  movesrunid,iterationid,yearid,monthid,dayid,hourid,stateid,countyid,zoneid,linkid,pollutantid,
  processid,sourcetypeid,fueltypeid,modelyearid,roadtypeid,scc,regclassid,
  emissionquant,emissionrate)
select movesrunid,iterationid,yearid,monthid,dayid,hourid,stateid,countyid,zoneid,linkid,pollutantid,
  processid,sourcetypeid,fueltypeid,modelyearid,roadtypeid,scc,regclassid,
  sum(emissionquant),sum(emissionrate)
from hcworkeroutput
where emissionquant >= 0
group by yearid,monthid,dayid,hourid,stateid,countyid,zoneid,linkid,pollutantid,
  processid,sourcetypeid,fueltypeid,modelyearid,roadtypeid,regclassid;

insert into hcworkeroutputall (
  movesrunid,iterationid,yearid,monthid,dayid,hourid,stateid,countyid,zoneid,linkid,pollutantid,
  processid,sourcetypeid,fueltypeid,modelyearid,roadtypeid,scc,regclassid,
  emissionquant,emissionrate,
  fuelformulationid,fuelsubtypeid
)
select movesrunid,iterationid,yearid,monthid,dayid,hourid,stateid,countyid,zoneid,linkid,pollutantid,
  processid,sourcetypeid,fueltypeid,modelyearid,roadtypeid,scc,regclassid,
  emissionquant,emissionrate,
  fuelformulationid,fuelsubtypeid
from hcworkeroutput
where emissionquant >= 0;
-- end section nmog

-- section voc
truncate hcworkeroutput;

-- @algorithm voc = nmhc*(speciationconstant + oxyspeciation* voltowtpercentoxy*(mtbevolume+etbevolume+tamevolume+etohvolume)).
-- @condition when (mtbevolume+etbevolume+tamevolume+etohvolume) >= 0. otherwise, voc = 0.
-- @condition not (running exhaust, start exhaust, ethanol fuel type, e70 and e85 fuel subtypes, model years >= 2001).
insert into hcworkeroutput (
  movesrunid,iterationid,yearid,monthid,dayid,hourid,stateid,countyid,zoneid,linkid,pollutantid,
  processid,sourcetypeid,fueltypeid,modelyearid,roadtypeid,scc,
  regclassid,fuelformulationid,fuelsubtypeid,
  emissionquant,emissionrate
)
select mwo.movesrunid,mwo.iterationid,mwo.yearid,mwo.monthid,mwo.dayid,mwo.hourid,mwo.stateid,mwo.countyid,mwo.zoneid,mwo.linkid,
  ppa.pollutantid,
  mwo.processid,mwo.sourcetypeid,mwo.fueltypeid,mwo.modelyearid,mwo.roadtypeid,mwo.scc,
  mwo.regclassid,mwo.fuelformulationid,mwo.fuelsubtypeid,
  emissionquant*(
    (case when (mtbevolume+etbevolume+tamevolume+etohvolume) >= 0 then
      (speciationconstant + oxyspeciation* voltowtpercentoxy*(mtbevolume+etbevolume+tamevolume+etohvolume))
     else 0 end)
  ) as emissionquant,
  emissionrate*(
    (case when (mtbevolume+etbevolume+tamevolume+etohvolume) >= 0 then
      (speciationconstant + oxyspeciation* voltowtpercentoxy*(mtbevolume+etbevolume+tamevolume+etohvolume))
     else 0 end)
  ) as emissionrate
from hcworkeroutputall mwo
inner join hcfuelformulation ff on (ff.fuelformulationid=mwo.fuelformulationid)
inner join hcspeciation hcs on (hcs.oxythreshid=ff.oxythreshid and hcs.fuelsubtypeid=ff.fuelsubtypeid and hcs.etohthreshid=ff.etohthreshid)
inner join hcpollutantprocessmappedmodelyear ppmy on (ppmy.polprocessid=hcs.polprocessid
  and ppmy.modelyearid=mwo.modelyearid and ppmy.fuelmygroupid=hcs.fuelmygroupid)
inner join hcpollutantprocessassoc ppa on (ppa.processid=mwo.processid
  and ppa.polprocessid=ppmy.polprocessid
  and ppa.processid in (##vocprocessids##)
  and ppa.pollutantid = 87)
where mwo.pollutantid = 79
and not (mwo.processid in (1,2) and mwo.fueltypeid=5 
  and mwo.fuelsubtypeid in (##e85e70fuelsubtypeids##) and mwo.modelyearid >= 2001);

-- @algorithm calculate voc from altnmhc (10079) that originates from altthc (10001). use e10's factors even though the fuel is ethanol.
-- this is done by joining to hcspeciation using e10's values rather than the current fuel formulation's values.
-- voc = altnmhc*(speciationconstant + oxyspeciation* voltowtpercentoxy*(mtbevolume+etbevolume+tamevolume+10)).
-- @condition running exhaust, start exhaust, ethanol fuel type, e70 and e85 fuel subtypes, model years >= 2001.
insert into hcworkeroutput (
  movesrunid,iterationid,yearid,monthid,dayid,hourid,stateid,countyid,zoneid,linkid,pollutantid,
  processid,sourcetypeid,fueltypeid,modelyearid,roadtypeid,scc,
  regclassid,fuelformulationid,fuelsubtypeid,
  emissionquant,emissionrate
)
select mwo.movesrunid,mwo.iterationid,mwo.yearid,mwo.monthid,mwo.dayid,mwo.hourid,mwo.stateid,mwo.countyid,mwo.zoneid,mwo.linkid,
  ppa.pollutantid,
  mwo.processid,mwo.sourcetypeid,mwo.fueltypeid,mwo.modelyearid,mwo.roadtypeid,mwo.scc,
  mwo.regclassid,mwo.fuelformulationid,mwo.fuelsubtypeid,
  emissionquant*(
    (speciationconstant + oxyspeciation* voltowtpercentoxy*(mtbevolume+etbevolume+tamevolume+10))
  ) as emissionquant,
  emissionrate*(
    (speciationconstant + oxyspeciation* voltowtpercentoxy*(mtbevolume+etbevolume+tamevolume+10))
  ) as emissionrate
from hcworkeroutputall mwo
inner join hcfuelformulation ff on (ff.fuelformulationid=mwo.fuelformulationid)
inner join hcspeciation hcs on (hcs.oxythreshid=0 and hcs.fuelsubtypeid=12 and hcs.etohthreshid=3)
inner join hcpollutantprocessmappedmodelyear ppmy on (ppmy.polprocessid=hcs.polprocessid
  and ppmy.modelyearid=mwo.modelyearid and ppmy.fuelmygroupid=hcs.fuelmygroupid)
inner join hcpollutantprocessassoc ppa on (ppa.processid=mwo.processid
  and ppa.polprocessid=ppmy.polprocessid
  and ppa.processid in (##vocprocessids##)
  and ppa.pollutantid = 87)
where mwo.pollutantid = 10079
and (mwo.processid in (1,2) and mwo.fueltypeid=5 
  and mwo.fuelsubtypeid in (##e85e70fuelsubtypeids##) and mwo.modelyearid >= 2001);

-- move values back into movesworkeroutput
insert into movesworkeroutput (
  movesrunid,iterationid,yearid,monthid,dayid,hourid,stateid,countyid,zoneid,linkid,pollutantid,
  processid,sourcetypeid,fueltypeid,modelyearid,roadtypeid,scc,regclassid,
  emissionquant,emissionrate)
select movesrunid,iterationid,yearid,monthid,dayid,hourid,stateid,countyid,zoneid,linkid,pollutantid,
  processid,sourcetypeid,fueltypeid,modelyearid,roadtypeid,scc,regclassid,
  sum(emissionquant),sum(emissionrate)
from hcworkeroutput
where emissionquant >= 0
group by yearid,monthid,dayid,hourid,stateid,countyid,zoneid,linkid,pollutantid,
  processid,sourcetypeid,fueltypeid,modelyearid,roadtypeid,regclassid;

-- voc is not an input to further hc speciation calculations.
-- if so, uncomment the following.
-- insert into hcworkeroutputall (
--  movesrunid,iterationid,yearid,monthid,dayid,hourid,stateid,countyid,zoneid,linkid,pollutantid,
--  processid,sourcetypeid,fueltypeid,modelyearid,roadtypeid,scc,regclassid,
--  emissionquant,emissionrate,
--  fuelformulationid,fuelsubtypeid
-- )
-- select movesrunid,iterationid,yearid,monthid,dayid,hourid,stateid,countyid,zoneid,linkid,pollutantid,
--  processid,sourcetypeid,fueltypeid,modelyearid,roadtypeid,scc,regclassid,
--  emissionquant,emissionrate,
--  fuelformulationid,fuelsubtypeid
-- from hcworkeroutput
-- where emissionquant >= 0

-- end section voc

-- section tog
truncate hcworkeroutput;
-- @algorithm tog=nmog+methane
insert into hcworkeroutput (
  movesrunid,iterationid,yearid,monthid,dayid,hourid,stateid,countyid,zoneid,linkid,pollutantid,
  processid,sourcetypeid,fueltypeid,modelyearid,roadtypeid,scc,
  regclassid,fuelformulationid,fuelsubtypeid,
  emissionquant,emissionrate
)
select mwo.movesrunid,mwo.iterationid,mwo.yearid,mwo.monthid,mwo.dayid,mwo.hourid,mwo.stateid,mwo.countyid,mwo.zoneid,mwo.linkid,
  ppa.pollutantid,
  mwo.processid,mwo.sourcetypeid,mwo.fueltypeid,mwo.modelyearid,mwo.roadtypeid,mwo.scc,
  mwo.regclassid,mwo.fuelformulationid,mwo.fuelsubtypeid,
  emissionquant,emissionrate
from hcworkeroutputall mwo
inner join hcpollutantprocessassoc ppa on (ppa.processid=mwo.processid
  and ppa.processid in (##togprocessids##)
  and ppa.pollutantid = 86)
where mwo.pollutantid in (80,5);

-- move values back into movesworkeroutput
insert into movesworkeroutput (
  movesrunid,iterationid,yearid,monthid,dayid,hourid,stateid,countyid,zoneid,linkid,pollutantid,
  processid,sourcetypeid,fueltypeid,modelyearid,roadtypeid,scc,regclassid,
  emissionquant,emissionrate)
select movesrunid,iterationid,yearid,monthid,dayid,hourid,stateid,countyid,zoneid,linkid,pollutantid,
  processid,sourcetypeid,fueltypeid,modelyearid,roadtypeid,scc,regclassid,
  sum(emissionquant),sum(emissionrate)
from hcworkeroutput
where emissionquant >= 0
group by yearid,monthid,dayid,hourid,stateid,countyid,zoneid,linkid,pollutantid,
  processid,sourcetypeid,fueltypeid,modelyearid,roadtypeid,regclassid;

-- tog values aren't needed by subsequent steps.
-- if they are needed, uncomment the following.
-- insert into hcworkeroutputall (
--  movesrunid,iterationid,yearid,monthid,dayid,hourid,stateid,countyid,zoneid,linkid,pollutantid,
--  processid,sourcetypeid,fueltypeid,modelyearid,roadtypeid,scc,regclassid,
--  emissionquant,emissionrate,
--  fuelformulationid,fuelsubtypeid
-- )
-- select movesrunid,iterationid,yearid,monthid,dayid,hourid,stateid,countyid,zoneid,linkid,pollutantid,
--  processid,sourcetypeid,fueltypeid,modelyearid,roadtypeid,scc,regclassid,
--  emissionquant,emissionrate,
--  fuelformulationid,fuelsubtypeid
-- from hcworkeroutput
-- where emissionquant >= 0

-- end section tog

-- alter table movesworkeroutput drop key hcpollutantid;
-- alter table movesworkeroutput drop index movesworkeroutput_a2;

alter table hcfuelsupply drop index hcfuelsupply_a1;
alter table hcspeciation drop index hcspeciation_a1;
alter table hcetohbin drop index hcetohbin_a1;
alter table hcpollutantprocessassoc drop index hcpollutantprocessassoc_a1;
alter table hcfuelformulation drop index hcfuelformulation_a1;

starttimer savemwo2;
savemwo2;
starttimer hcspeciationcalculator;

-- end section processing

-- section cleanup
drop table if exists hcworkeroutput;
drop table if exists hcworkeroutputall;
-- end section cleanup

-- section final cleanup
-- end section final cleanup
