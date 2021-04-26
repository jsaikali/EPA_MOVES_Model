-- author wesley faler
-- author harvey michaels
-- author ahuang
-- author ed glover
-- author john covey
-- version 2018-03-06



create table agecategory (
       ageid                smallint not null,
       agegroupid           smallint,
       agecategoryname      char(50) null,
       key (agegroupid, ageid),
       key (ageid, agegroupid)
);

create unique index xpkagecategory on agecategory
(
       ageid                          asc
);

create table agegroup (
  agegroupid   smallint not null,
  agegroupname char(50)
);

create unique index xpkagegroup on agegroup
(
       agegroupid   asc
);

create table atbaseemissions 
(
  polprocessid      int   not null  default '0',
  monthgroupid      int(11)     not null  default '0',
  atbaseemissions     float     not null  default '0',
  datasourceid      smallint(6)   null    default null,
  primary key (polprocessid, monthgroupid)
);

create table atratio (
  fueltypeid int not null,
  fuelformulationid int not null,
  polprocessid int not null,
  minmodelyearid int not null,
  maxmodelyearid int not null,
  ageid int not null,
  monthgroupid int not null,
  atratio double null,
  primary key (fueltypeid, fuelformulationid, polprocessid, minmodelyearid, maxmodelyearid, ageid, monthgroupid),
  key atratio_key1 (fuelformulationid, polprocessid, minmodelyearid),
  key atratio_key2 (polprocessid, fueltypeid, monthgroupid, minmodelyearid, ageid, maxmodelyearid, fuelformulationid)
);

create table atratiogas2
(
  polprocessid int not null default '0',
  sourcetypeid smallint(6) not null default '0',
  fuelsubtypeid smallint(6) default null,
  atratio float default null,
  atratiocv float default null
);

create unique index xpkatratiogas2 on atratiogas2
(
  polprocessid,
  sourcetypeid,
  fuelsubtypeid
);

create table atrationongas
(
  polprocessid int not null default '0',
  sourcetypeid smallint(6) not null default '0',
  fuelsubtypeid smallint(6) not null default '0',
  modelyeargroupid int(11) not null default '0',
  atratio double default null,
  atratiocv double default null,
  datasourceid smallint(6) default null,
  primary key (polprocessid,sourcetypeid,fuelsubtypeid,modelyeargroupid)
);

create table averagetankgasoline (
  zoneid int(11) not null,
  fueltypeid smallint(6) not null,
  fuelyearid int not null,
  monthgroupid smallint(6) not null,
  etohvolume float default null,
  rvp float null,
  isuserinput char(1) not null default 'N',
  primary key  (zoneid,fueltypeid,fuelyearid,monthgroupid),
  index(isuserinput)
);

create table averagetanktemperature (
  tanktemperaturegroupid smallint not null,
  zoneid integer not null,
  monthid smallint not null,
  hourdayid smallint not null,
  opmodeid smallint not null,
  averagetanktemperature float,
  averagetanktemperaturecv float
);

create unique index xpkaveragetanktemperature on averagetanktemperature (
  tanktemperaturegroupid asc,
  zoneid asc,
  monthid asc,
  hourdayid asc,
  opmodeid asc
);

create table avft (
  sourcetypeid smallint(6) not null,
  modelyearid smallint unsigned not null,
  fueltypeid smallint(6) not null,
  engtechid smallint not null,
  fuelengfraction double not null,
  primary key (sourcetypeid, modelyearid, fueltypeid, engtechid),
  key (sourcetypeid),
  key (modelyearid),
  key (fueltypeid),
  key (engtechid)
);

create table avgspeedbin (
       avgspeedbinid        smallint not null,
       avgbinspeed          float null,
       avgspeedbindesc      char(50) null,
       opmodeidtirewear   smallint(6) null default null,
       opmodeidrunning    smallint(6) null default null
);

create unique index xpkavgspeedbin on avgspeedbin
(
       avgspeedbinid                  asc
);

create table avgspeeddistribution (
       sourcetypeid         smallint not null,
       roadtypeid           smallint not null,
       hourdayid            smallint not null,
       avgspeedbinid        smallint not null,
       avgspeedfraction     float null
);

alter table avgspeeddistribution add (
       key (sourcetypeid),
       key (roadtypeid),
       key (hourdayid),
       key (avgspeedbinid)
);

create unique index xpkavgspeeddistribution on avgspeeddistribution
(
       sourcetypeid                   asc,
       roadtypeid                     asc,
       hourdayid                      asc,
       avgspeedbinid                  asc
);

create table basefuel
(
  calculationengine varchar(100) not null,
  fueltypeid smallint(6) not null,
  modelyeargroupid int(11) not null default '0',
  fuelformulationid smallint(6) not null,
  description varchar(255) not null default '',
  datasourceid smallint(6) not null,
  primary key (calculationengine, fueltypeid, modelyeargroupid)
);

create table coldsoakinitialhourfraction (
  sourcetypeid smallint(6) not null,
  zoneid int(11) not null,
  monthid smallint(6) not null,
  hourdayid smallint(6) not null,
  initialhourdayid smallint(6) not null,
  coldsoakinitialhourfraction float not null,
  isuserinput char(1) not null default 'N',
  primary key  (sourcetypeid,zoneid,monthid,hourdayid,initialhourdayid),
  index (isuserinput)
);

create table coldsoaktanktemperature (
  zoneid int(11) not null,
  monthid smallint(6) not null,
  hourid smallint(6) not null,
  coldsoaktanktemperature float not null,
  primary key  (zoneid,monthid,hourid)
);

create table complexmodelparametername 
(
  cmpid       smallint(6)   not null    default '0' primary key,
  cmpname     char(25),
  cmpexpression   varchar(500) not null
);

create table complexmodelparameters 
(
  polprocessid      int         not null  default '0',    
  fuelmodelid         smallint(6)   not null    default '0',
  cmpid           smallint(6)   not null    default '0',  
  coeff1          float     null    default null, 
  coeff2          float     null    default null, 
  coeff3          float     null    default null,
  datasourceid      smallint(6)   null    default null,
  primary key (polprocessid, fuelmodelid, cmpid)
);

create table county (
       countyid             integer not null,
       stateid              smallint not null,
       countyname           char(50) null,
       altitude             char(1) null,
       gpafract             float null,
       barometricpressure   float null,
       barometricpressurecv float null,
       countytypeid int not null default '0',
       msa          char(255),
       key (countyid, stateid),
       key (stateid, countyid)
);

create unique index xpkcounty on county
(
       countyid                       asc
);

create table countytype (
  countytypeid int not null primary key,
  countytypedescription varchar(255) not null default ''
);

create table countyyear (
       countyid             integer not null,
       yearid               smallint not null,
     refuelingvaporprogramadjust float not null default 0.0,
     refuelingspillprogramadjust float not null default 0.0,
     key (yearid, countyid)
);

alter table countyyear add (
       key (countyid),
       key (yearid)
);

create unique index xpkcountyyear on countyyear
(
       countyid                       asc,
       yearid                         asc
);

create table crankcaseemissionratio (
  polprocessid      int not null,
  minmodelyearid      smallint(6) not null,
  maxmodelyearid      smallint(6) not null,
  sourcetypeid      smallint(6) not null,
  fueltypeid        smallint(6) not null, 
  crankcaseratio      float not null,
  crankcaseratiocv    float null,
  primary key (polprocessid, minmodelyearid, maxmodelyearid, sourcetypeid, fueltypeid)
); 

create table criteriaratio (
  fueltypeid int not null,
  fuelformulationid int not null,
  polprocessid int not null,
  pollutantid int not null,
  processid int not null,
  sourcetypeid int not null,
  modelyearid int not null,
  ageid int not null,
  ratio double null,
  ratiogpa double null,
  rationosulfur double null,
  key crfuelformulation (polprocessid, fuelformulationid),
  key crcommon (polprocessid, modelyearid, ageid)
);

create table cumtvvcoeffs (
  regclassid smallint(6) not null,
  modelyeargroupid int(11) not null,
  agegroupid smallint(6) not null,
  polprocessid int not null,
  tvvterma float null,
  tvvtermb float null,
  tvvtermc float null,
  tvvtermacv float null,
  tvvtermbcv float null,
  tvvtermccv float null,
  tvvtermaim float null,
  tvvtermbim float null,
  tvvtermcim float null,
  tvvtermaimcv float null,
  tvvtermbimcv float null,
  tvvtermcimcv float null,
  backpurgefactor double,
  averagecanistercapacity double,
  tvvequation varchar(4096) not null default '',
  leakequation varchar(4096) not null default '',
  leakfraction double,
  tanksize double,
  tankfillfraction double,
  leakfractionim double,
  primary key  (regclassid,modelyeargroupid,agegroupid,polprocessid)
);

create table datasource (
       datasourceid         smallint not null,
       author               char(25) null,
       date                 date null,
       sponsor              char(30) null,
       documentid           char(150) null,
       qualitylevel         char(1) null
);

create unique index xpkdatasou on datasource
(
       datasourceid                   asc
);


create table dayofanyweek (
       dayid                smallint not null,
       dayname              char(10) null,
       noofrealdays         float not null default 1.0
);

create unique index xpkdayofanyweek on dayofanyweek
(
       dayid                          asc
);


create table dayvmtfraction (
       sourcetypeid         smallint not null,
       monthid              smallint not null,
       roadtypeid           smallint not null,
       dayid                smallint not null,
       dayvmtfraction       float null
);

alter table dayvmtfraction add (
       key (sourcetypeid),
       key (monthid),
       key (roadtypeid),
       key (dayid)
);

create unique index xpkdayvmtfraction on dayvmtfraction
(
       sourcetypeid                   asc,
       monthid                        asc,
       roadtypeid                     asc,
       dayid                          asc
);

create table dioxinemissionrate (
  polprocessid int not null default '0',
  fueltypeid smallint(6) not null default '0',
  modelyeargroupid int(11) not null default '0',
  units char(30) default null,
  meanbaserate double default null,
  meanbaseratecv double default null,
  datasourceid smallint(6) default null,
  primary key (polprocessid,fueltypeid,modelyeargroupid),
  unique key xpkdioxinemissionrate (polprocessid,fueltypeid,modelyeargroupid)
);

create table driveschedule (
       drivescheduleid      smallint not null,
       averagespeed         float not null,
       driveschedulename    character(50) null
);

create unique index xpkdriveschedule on driveschedule
(
       drivescheduleid                asc
);


create table drivescheduleassoc (
       sourcetypeid         smallint not null,
       roadtypeid           smallint not null,
       drivescheduleid      smallint not null
);

alter table drivescheduleassoc add (
        key (sourcetypeid),
        key (roadtypeid),
        key (drivescheduleid)
);

create unique index xpkdrivescheduleassoc on drivescheduleassoc
(
       sourcetypeid                   asc,
       roadtypeid                     asc,
       drivescheduleid                asc
);


create table driveschedulesecond (
       drivescheduleid      smallint not null,
       second               smallint not null,
       speed                float null
);

alter table driveschedulesecond add (
        key (drivescheduleid),
        key (second)
);

create unique index xpkdriveschedulesecond on driveschedulesecond
(
       drivescheduleid                asc,
       second                         asc
);

create table driveschedulesecondlink (
  linkid integer not null,
  secondid smallint not null,
  speed float null,
  grade float not null default 0.0,
  primary key (linkid, secondid),
  key (secondid, linkid)
);

create table e10fuelproperties (
  fuelregionid int not null,
  fuelyearid int not null,
  monthgroupid smallint not null,
  rvp double null,
  sulfurlevel double null,
  etohvolume double null,
  mtbevolume double null,
  etbevolume double null,
  tamevolume double null,
  aromaticcontent double null,
  olefincontent double null,
  benzenecontent double null,
  e200 double null,
  e300 double null,
  biodieselestervolume double null,
  cetaneindex double null,
  pahcontent double null,
  t50 double null,
  t90 double null,
  primary key (fuelregionid, fuelyearid, monthgroupid)
);

create table emissionprocess (
       processid            smallint not null,
       processname          char(50) null,
       sccprocid            char(1) null,
       occursonrealroads    char(1) default "y" not null,
       shortname      varchar(50) null,
       processdisplaygroupid smallint(6) unsigned default null,
       isaffectedbyonroad tinyint(1) default '1',
       isaffectedbynonroad tinyint(1) default '0'
);

create unique index xpkemissionprocess on emissionprocess
(
       processid                      asc
);


create table emissionrate (
       sourcebinid          bigint not null,
       polprocessid         int not null,
       opmodeid             smallint not null,
       meanbaserate         float null,
       meanbaseratecv       float null,
       meanbaserateim     float null,
       meanbaserateimcv     float null,
       datasourceid         smallint null
);

alter table emissionrate add (
        key (sourcebinid),
        key (polprocessid),
        key (opmodeid)
);

create unique index xpkemissionrate on emissionrate
(
       sourcebinid                    asc,
       polprocessid                   asc,
       opmodeid                       asc
);

create table emissionrateadjustment
(
  polprocessid      int(11)     not null,
  sourcetypeid      smallint(6)   not null,
  regclassid        smallint(6)   not null,
  fueltypeid        smallint(6)   not null,
  beginmodelyearid    smallint(6)   not null,
  endmodelyearid      smallint(6)   not null,
  emissionrateadjustment  double null default null,
  datasourceid      smallint(6) null default null,
  primary key (polprocessid, sourcetypeid, fueltypeid, regclassid, beginmodelyearid, endmodelyearid),
  key (polprocessid, beginmodelyearid, endmodelyearid)
);

create table emissionratebyage (
       sourcebinid          bigint not null,
       polprocessid         int not null,
       opmodeid             smallint not null,
       agegroupid           smallint not null,
       meanbaserate         float null,
       meanbaseratecv       float null,
       meanbaserateim       float null,
       meanbaserateimcv     float null,
       datasourceid         smallint null
);

alter table emissionratebyage add (
        key (sourcebinid),
        key (polprocessid),
        key (opmodeid),
        key (agegroupid)
);
        
create unique index xpkemissionratebyage on emissionratebyage
(
       sourcebinid                    asc,
       polprocessid                   asc,
       opmodeid                       asc,
       agegroupid                     asc
);

create table emissionratebyagelev (
       sourcebinid          bigint not null,
       polprocessid         int not null,
       opmodeid             smallint not null,
       agegroupid           smallint not null,
       meanbaserate         float null,
       meanbaseratecv       float null,
       meanbaserateim       float null,
       meanbaserateimcv     float null,
       datasourceid         smallint null
);

alter table emissionratebyagelev add (
        key (sourcebinid),
        key (polprocessid),
        key (opmodeid),
        key (agegroupid)
);
        
create unique index xpkemissionratebyagelev on emissionratebyagelev
(
       sourcebinid                    asc,
       polprocessid                   asc,
       opmodeid                       asc,
       agegroupid                     asc
);

create table emissionratebyagenlev (
       sourcebinid          bigint not null,
       polprocessid         int not null,
       opmodeid             smallint not null,
       agegroupid           smallint not null,
       meanbaserate         float null,
       meanbaseratecv       float null,
       meanbaserateim       float null,
       meanbaserateimcv     float null,
       datasourceid         smallint null
);

alter table emissionratebyagenlev add (
        key (sourcebinid),
        key (polprocessid),
        key (opmodeid),
        key (agegroupid)
);
        
create unique index xpkemissionratebyagenlev on emissionratebyagenlev
(
       sourcebinid                    asc,
       polprocessid                   asc,
       opmodeid                       asc,
       agegroupid                     asc
);

create table enginesize (
       engsizeid            smallint not null,
       engsizename          character(50) null
);

create unique index xpkenginesize on enginesize
(
       engsizeid                      asc
);

create table enginetech (
  engtechid smallint(6) not null default '0',
  tierid smallint(6) default '99',
  strokes smallint(6) default '99',
  engtechname char(50) default null,
  engtechdesc char(80) default null,
  primary key (engtechid)
);

create table etohbin 
(
  etohthreshid      smallint(6)   not null  default '0' primary key,
  etohthreshlow     float     null    default null,
  etohthreshhigh      float     null    default null,
  etohnominalvalue    float     null    default null  
);

create table evaptemperatureadjustment (
  processid smallint(6) not null,
  tempadjustterm3 double not null default 0,
  tempadjustterm2 double not null default 0,
  tempadjustterm1 double not null default 0,
  tempadjustconstant double not null default 0,
  primary key (processid)
);

create table evaprvptemperatureadjustment (
  processid smallint(6) not null,
  fueltypeid smallint(6) not null,
  rvp double not null,
  adjustterm3 double not null default 0,
  adjustterm2 double not null default 0,
  adjustterm1 double not null default 0,
  adjustconstant double not null default 0,
  primary key (processid, fueltypeid, rvp),
  key (rvp, processid, fueltypeid),
  key (rvp, fueltypeid, processid)
);

create table extendedidlehours (
       sourcetypeid         smallint not null,
       hourdayid            smallint not null,
       monthid              smallint not null,
       yearid               smallint not null,
       ageid                smallint not null,
       zoneid               integer not null,
       extendedidlehours    float null,
       extendedidlehourscv  float null
);

alter table extendedidlehours add (
        key (sourcetypeid),
        key (hourdayid),
        key (monthid),
        key (yearid),
        key (ageid),
        key (zoneid)
);

create unique index xpkextendedidlehours on extendedidlehours
(
       sourcetypeid                   asc,
       hourdayid                      asc,
       monthid                        asc,
       yearid                         asc,
       ageid                          asc,
       zoneid                         asc
);

create table fuelengtechassoc (
       sourcetypeid         smallint not null,
       fueltypeid           smallint not null,
       engtechid            smallint not null,
       category             char(50) not null,
       categorydisplayorder smallint not null
);

create unique index xpkfuelengtechassoc on fuelengtechassoc
(
       sourcetypeid                   asc,
       fueltypeid                     asc,
       engtechid                      asc
);
  
create table fuelformulation (
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

create table fuelmodelname 
(
  fuelmodelid         smallint(6)   not null    default '0' primary key,
  fuelmodelname     varchar(50) not null,
  fuelmodelabbreviation varchar(10) not null,
  calculationengines      varchar(200) not null default ''
);

create table fuelmodelwtfactor 
(
  fuelmodelid       smallint(6)   not null  default '0',
  modelyeargroupid    int(11)     not null  default '0',
  ageid         smallint(6)   not null  default '0',
  fuelmodelwtfactor   float     null    default null,
  datasourceid      smallint(6)   null    default null,
  primary key (fuelmodelid, modelyeargroupid, ageid)
);

create table fuelparametername 
(
  fuelparameterid       smallint(6)   not null    default '0' primary key,
  fuelparametername   varchar(25)   not null    default '',
  fuelparameterunits      varchar(20)     not null    default '',
  fuelparameterexpression varchar(500)    not null    default ''
);

create table fuelsubtype (
       fuelsubtypeid        smallint not null,
       fueltypeid           smallint not null,
       fuelsubtypedesc      char(50) null,
       fuelsubtypepetroleumfraction float null,
       fuelsubtypepetroleumfractioncv float null,
       fuelsubtypefossilfraction float null,
       fuelsubtypefossilfractioncv float null,
       carboncontent        float null,
       oxidationfraction    float null,
       carboncontentcv      float null,
       oxidationfractioncv  float null,
     energycontent    float null,
       key (fueltypeid, fuelsubtypeid)
);

create unique index xpkfuelsubtype on fuelsubtype
(
       fuelsubtypeid                  asc
);


create table fuelsupply (
       fuelregionid         integer not null,
       fuelyearid           int not null,
       monthgroupid         smallint not null,
       fuelformulationid    smallint not null,
       marketshare          float null,
       marketsharecv        float null
);

alter table fuelsupply add (
        key (fuelregionid),
        key (fuelyearid),
        key (monthgroupid),
        key (fuelformulationid)
);

create unique index xpkfuelsupply on fuelsupply
(
       fuelregionid                   asc,
       fuelyearid                     asc,
       monthgroupid                   asc,
       fuelformulationid              asc
);

create table fuelsupplyyear (
    fuelyearid int not null primary key
);

create table fueltype (
       fueltypeid           smallint not null,
       defaultformulationid smallint not null,
       fueltypedesc         char(50) null,
       humiditycorrectioncoeff float null,
       humiditycorrectioncoeffcv float null,
     fueldensity        float null,
     subjecttoevapcalculations char(1) not null default 'N'
);

create unique index xpkfueltype on fueltype
(
       fueltypeid                     asc
);

alter table fueltype add (
  key (subjecttoevapcalculations, fueltypeid)
);

create table fuelusagefraction (
  countyid int(11) not null,
  fuelyearid int not null,
  modelyeargroupid int(11) not null,
  sourcebinfueltypeid smallint(6) not null,
  fuelsupplyfueltypeid smallint(6) not null,
  usagefraction double,
  primary key (countyid, fuelyearid, modelyeargroupid, sourcebinfueltypeid, fuelsupplyfueltypeid)
);

create table fuelwizardfactors (
  adjustedparameter   varchar(4)    not null,
  minlevel      double      not null,
  maxlevel      double      not null,
  functiontype    varchar(4)    not null,
  monthgroupid    smallint(6)   not null,
  fueltypeid      smallint(6)   not null,
  rvp_factor      double      null,
  sulf_factor     double      null,
  etoh_factor     double      null,
  arom_factor     double      null,
  olef_factor     double      null,
  benz_factor     double      null,
  e200_factor     double      null,
  e300_factor     double      null,
  t50_factor      double      null,
  t90_factor      double      null,
  units       varchar(6)    null,
  datasourceid    smallint(6)   null,
  primary key (fueltypeid, monthgroupid, adjustedparameter, minlevel, maxlevel, functiontype)
);

create table fullacadjustment (
       sourcetypeid         smallint not null,
       polprocessid         int not null,
       opmodeid             smallint not null,
       fullacadjustment     float null,
       fullacadjustmentcv   float null
);

alter table fullacadjustment add (
        key (sourcetypeid),
        key (polprocessid),
        key (opmodeid)
);

create unique index xpkfullacadjustment on fullacadjustment
(
       sourcetypeid                   asc,
       polprocessid                   asc,
       opmodeid                       asc
);

create table generalfuelratio (
  fueltypeid int not null,
  fuelformulationid int not null,
  polprocessid int not null,
  pollutantid int not null,
  processid int not null,
  minmodelyearid int not null default '1960',
  maxmodelyearid int not null default '2060',
  minageid int not null default '0',
  maxageid int not null default '30',
  sourcetypeid int not null,
  fueleffectratio double not null default '0',
  fueleffectratiogpa double not null default '0'
);

create table generalfuelratioexpression (
  fueltypeid int not null,
  polprocessid int not null,
  minmodelyearid int not null default '1960',
  maxmodelyearid int not null default '2060',
  minageid int not null default '0',
  maxageid int not null default '30',
  sourcetypeid int not null default '0',
  fueleffectratioexpression varchar(32000) not null default '',
  fueleffectratiogpaexpression varchar(32000) not null default ''
);

create table greetmanfanddisposal (
       greetvehicletype     smallint not null,
       modelyearid          smallint not null,
       pollutantid          smallint not null,
       emissionstage        char(4) not null,
       emissionpervehicle   float null
);

alter table greetmanfanddisposal add (
        key (greetvehicletype),
        key (modelyearid),
        key (pollutantid),
        key (emissionstage)
);

create unique index xpkgreetmanfanddisposal on greetmanfanddisposal
(
       greetvehicletype               asc,
       modelyearid                   desc,
       pollutantid                    asc,
       emissionstage                  asc
);


create table greetwelltopump (
       yearid               smallint not null,
       pollutantid          smallint not null,
       fuelsubtypeid        smallint not null,
       emissionrate         float null,
       emissionrateuncertainty float null
);

alter table greetwelltopump add (
        key (yearid),
        key (pollutantid),
        key (fuelsubtypeid)
);

create unique index xpkgreetwelltopump on greetwelltopump
(
       yearid                         asc,
       pollutantid                    asc,
       fuelsubtypeid                  asc
);


create table grid (
       gridid               integer not null
);

create unique index xpkgrid on grid
(
       gridid                         asc
);


create table gridzoneassoc (
       zoneid               integer not null,
       gridid               integer not null,
       gridallocfactor      float null
);

alter table gridzoneassoc add (
        key (zoneid),
        key (gridid)
);

create unique index xpkgridzoneassoc on gridzoneassoc
(
       zoneid                         asc,
       gridid                         asc
);

create table hcpermeationcoeff 
(
  polprocessid      int         not null  default '0',
  etohthreshid      smallint(6)   not null  default '0',
  fuelmygroupid     int(11)     not null  default '0',
  fueladjustment      float     null    default null,
  fueladjustmentgpa   float     null    default null,
  datasourceid      smallint(6)   null    default null,
  primary key (polprocessid, etohthreshid, fuelmygroupid)
);

create table hcspeciation (
  polprocessid int not null default '0',
  fuelsubtypeid smallint(6) not null default '0',
  regclassid smallint(6) not null default '0',
  beginmodelyearid smallint(6) not null default '0',
  endmodelyearid smallint(6) not null default '0',
  speciationconstant double not null default '0',
  oxyspeciation double not null default '0',
  datasourceid smallint(6) not null default '0',
  primary key (polprocessid,fuelsubtypeid,regclassid,beginmodelyearid,endmodelyearid)
);

create table hotellingactivitydistribution (
  zoneid        int not null,
  beginmodelyearid  smallint not null,
  endmodelyearid    smallint not null,
  opmodeid      int not null,
  opmodefraction    double not null,
  primary key     (zoneid, beginmodelyearid, endmodelyearid, opmodeid),
  key         (zoneid, opmodeid, beginmodelyearid, endmodelyearid)
);

create table hotellingagefraction (
  zoneid int not null,
  ageid smallint not null,
  agefraction double not null,
  primary key (zoneid, ageid),
  key (ageid, zoneid)
);

create table hotellingcalendaryear (
  yearid smallint not null,
  hotellingrate double not null,
  primary key (yearid)
);

create table hotellinghours (
  sourcetypeid         smallint not null,
  hourdayid            smallint not null,
  monthid              smallint not null,
  yearid               smallint not null,
  ageid                smallint not null,
  zoneid               integer not null,
  hotellinghours       double null,
  isuserinput      char(1) default 'N' not null,
  primary key     (sourcetypeid, hourdayid, monthid, yearid, ageid, zoneid),
  key (sourcetypeid),
  key (hourdayid),
  key (monthid),
  key (yearid),
  key (ageid),
  key (zoneid)
);

create table hotellinghourfraction (
  zoneid int not null,
  dayid smallint not null,
  hourid smallint not null,
  hourfraction double not null,
  primary key (zoneid, dayid, hourid),
  key (hourid, dayid, zoneid)
);

create table hotellinghoursperday (
  yearid smallint not null,
  zoneid int not null,
  dayid smallint not null,
  hotellinghoursperday double not null,
  primary key (yearid, zoneid, dayid),
  key (zoneid, yearid, dayid),
  key (dayid, yearid, zoneid)
);

create table hotellingmonthadjust (
  zoneid int not null,
  monthid smallint not null,
  monthadjustment double not null,
  primary key (zoneid, monthid),
  key (monthid, zoneid)
);

create table hourday (
       hourdayid            smallint not null,
       dayid                smallint not null,
       hourid               smallint not null,
       key (dayid, hourid, hourdayid),
       key (hourid, dayid, hourdayid),
       key (hourdayid, dayid, hourid),
       key (hourdayid, hourid, dayid)
);

alter table hourday add (
        key (dayid),
        key (hourid)
);

create unique index xpkhourday on hourday
(
       hourdayid                      asc
);


create table hourofanyday (
       hourid               smallint not null,
       hourname             char(50) null
);

create unique index xpkhourofanyday on hourofanyday
(
       hourid                         asc
);


create table hourvmtfraction (
       sourcetypeid         smallint not null,
       roadtypeid           smallint not null,
       dayid                smallint not null,
       hourid               smallint not null,
       hourvmtfraction      float null
);

alter table hourvmtfraction add (
        key (sourcetypeid),
        key (roadtypeid),
        key (dayid),
        key (hourid)
);

create unique index xpkhourvmtfraction on hourvmtfraction
(
       sourcetypeid                   asc,
       roadtypeid                     asc,
       dayid                          asc,
       hourid                         asc
);


create table hpmsvtype (
       hpmsvtypeid          smallint not null,
       hpmsvtypename        character(50) null
);

create unique index xpkhpmsvtype on hpmsvtype
(
       hpmsvtypeid                    asc
);

create table hpmsvtypeday (
  yearid smallint not null,
  monthid smallint not null,
  dayid smallint not null,
  hpmsvtypeid smallint not null,
  vmt double not null,
  primary key (yearid, monthid, dayid, hpmsvtypeid),
  key (hpmsvtypeid, yearid, monthid, dayid)
);

create table hpmsvtypeyear (
       hpmsvtypeid          smallint not null,
       yearid               smallint not null,
       vmtgrowthfactor      float null,
       hpmsbaseyearvmt      float null,
       key (yearid, hpmsvtypeid)
);

alter table hpmsvtypeyear add (
        key (hpmsvtypeid),
        key (yearid)
);

create unique index xpkhpmsvtypeyear on hpmsvtypeyear
(
       hpmsvtypeid                    asc,
       yearid                         asc
);

create table idledayadjust (
  sourcetypeid smallint not null,
  dayid smallint not null,
  idledayadjust double not null,
  primary key (sourcetypeid, dayid)
);

create table idlemodelyeargrouping (
  sourcetypeid smallint not null,
  minmodelyearid smallint not null,
  maxmodelyearid smallint not null,
  totalidlefraction double not null,
  primary key (sourcetypeid, minmodelyearid, maxmodelyearid)
);

create table idlemonthadjust (
  sourcetypeid smallint not null,
  monthid smallint not null,
  idlemonthadjust double not null,
  primary key (sourcetypeid, monthid)
);

create table idleregion (
  idleregionid int not null primary key,
  idleregiondescription varchar(255) not null default ''
);

create table imcoverage (
       polprocessid int not null,
       stateid int,
       countyid int not null,
       yearid smallint not null,
       sourcetypeid smallint not null,
       fueltypeid smallint not null,
       improgramid smallint not null,
     inspectfreq smallint(6) null,
     teststandardsid smallint null,
       begmodelyearid smallint,
       endmodelyearid smallint,
       useimyn char(1) not null default 'Y',
       compliancefactor float default null
);

create unique index xpkimcoverage on imcoverage
(
       polprocessid   asc,
       countyid       asc,
       yearid         asc,
       sourcetypeid   asc,
       fueltypeid     asc,
       improgramid    asc
);

create table imfactor
(
  polprocessid int not null,
  inspectfreq smallint(6) not null,
  teststandardsid smallint(6) not null,
  sourcetypeid smallint(6) not null,
  fueltypeid smallint(6) not null,
  immodelyeargroupid int(8) not null,
  agegroupid smallint(6) not null,
  imfactor float not null
);

create unique index xpkimfactor on imfactor
(
  polprocessid  asc,
  inspectfreq   asc,
  teststandardsid asc,
  sourcetypeid  asc,
  fueltypeid    asc,
  immodelyeargroupid  asc,
  agegroupid asc
);

create table iminspectfreq
(
  inspectfreq smallint(6) not null primary key,
  inspectfreqdesc char(50) default null
);

create table immodelyeargroup (
  immodelyeargroupid int(8) not null,
  immodelyeargroupdesc char(40) not null
);

create unique index xpkimmodelyeargroup on immodelyeargroup
(
  immodelyeargroupid asc
);

create table imteststandards
(
  teststandardsid smallint(6) not null,
  teststandardsdesc char(50) not null,
  shortname varchar(50) default null
);

create unique index xpkimteststandards on imteststandards
(
  teststandardsid asc
);


create table startsopmodedistribution (
  dayid smallint(6) not null default 0,
  hourid smallint(6) not null default 0,
  sourcetypeid smallint(6) not null default 0,
  ageid smallint(6) not null default 0,
  opmodeid smallint(6) not null default 0,
  opmodefraction double default null,
  primary key (dayid,hourid,sourcetypeid,ageid,opmodeid),
  key dayid (dayid),
  key hourid (hourid),
  key sourcetypeid (sourcetypeid),
  key ageid (ageid)
);

create table integratedspeciesset  (
  mechanismid         smallint(6)   not null,
  integratedspeciessetid    smallint(6)   not null,
  pollutantid         smallint(6)   not null,
  useissyn          varchar(2)    null,
  primary key (mechanismid, integratedspeciessetid, pollutantid)
);

create table integratedspeciessetname  (
  integratedspeciessetid        smallint(6)   not null, 
  integratedspeciessetname      varchar(40)   null,
  primary key (integratedspeciessetid),
  key (integratedspeciessetname)
);

create table link (
       linkid               integer not null,
       countyid             integer not null,
       zoneid               integer null,
       roadtypeid           smallint not null,
       linklength           float null,
       linkvolume           float null,
       linkavgspeed     float null,
       linkdescription    varchar(50) null,
       linkavggrade     float null
);

alter table link add (
        key (countyid),
        key (zoneid),
        key (roadtypeid)
);

create unique index xpklink on link
(
       linkid                         asc
);


create table linkaveragespeed (
       linkid               integer not null,
       averagespeed         float null
);

create unique index xpklinkaveragespeed on linkaveragespeed
(
       linkid                         asc
);


create table linkhourvmtfraction (
       linkid               integer not null,
       monthid              smallint not null,
       sourcetypeid         smallint not null,
       dayid                smallint not null,
       hourid               smallint not null,
       vmtfraction          float null
);

alter table linkhourvmtfraction add (
        key (linkid),
        key (monthid),
        key (sourcetypeid),
        key (dayid),
        key (hourid)
);

create unique index xpklinkhourvmtfraction on linkhourvmtfraction
(
       linkid                         asc,
       monthid                        asc,
       sourcetypeid                   asc,
       dayid                          asc,
       hourid                         asc
);

create table linksourcetypehour (
  linkid integer not null,
  sourcetypeid smallint not null,
  sourcetypehourfraction float null,
  primary key (linkid, sourcetypeid),
  key (sourcetypeid, linkid)
);

create table lumpedspeciesname  (
  lumpedspeciesid       smallint(6)   not null, 
  lumpedspeciesname     varchar(20)   null,
  primary key (lumpedspeciesid),
  key (lumpedspeciesname)
);

create table m6sulfurcoeff (
  pollutantid int not null,
  minmodelyearid int not null,
  maxmodelyearid int not null,
  minsulfur double not null,
  sulfurlongcoeff double,
  sulfurirfactor double,
  maxirfactorsulfur double,
  key(pollutantid, minmodelyearid, maxmodelyearid)
);

create table meanfuelparameters 
(
  polprocessid      int         not null  default '0',  
  fueltypeid        smallint(6)   not null  default '0' ,
  modelyeargroupid    int(11)     not null  default '0',  
  fuelparameterid       smallint(6)   not null    default '0',  
  basevalue       float     null    default null, 
  centeringvalue      float     null    default null, 
  stddevvalue       float     null    default null,
  datasourceid      smallint(6)   null    default null,
  primary key (polprocessid, fueltypeid, modelyeargroupid, fuelparameterid)
);

create table mechanismname  (
  mechanismid       smallint(6)   not null, 
  mechanismname     varchar(40)   null,
  primary key (mechanismid),
  key (mechanismname)
);

create table metalemissionrate (
  polprocessid int not null default '0',
  fueltypeid smallint(6) not null default '0',
  sourcetypeid smallint(6) not null default '0',
  modelyeargroupid int(11) not null default '0',
  units char(20) default null,
  meanbaserate double default null,
  meanbaseratecv double default null,
  datasourceid smallint(6) default null,
  primary key (polprocessid,fueltypeid,sourcetypeid,modelyeargroupid),
  unique key xpkmetalemissionrate (polprocessid,fueltypeid,sourcetypeid,modelyeargroupid)
);

create table methanethcratio (
  processid smallint(6) not null default '0',
  fuelsubtypeid smallint(6) not null default '0',
  regclassid smallint(6) not null default '0',
  beginmodelyearid smallint(6) not null default '0',
  endmodelyearid smallint(6) not null default '0',
  ch4thcratio double default null,
  datasourceid smallint(6) not null default '0',
  primary key (processid,fuelsubtypeid,regclassid,beginmodelyearid,endmodelyearid)
);

create table minorhapratio (
  polprocessid int not null default '0',
  fueltypeid smallint(6) not null default '0',
  fuelsubtypeid smallint(6) not null default '0',
  modelyeargroupid int(11) not null default '0',
  atratio double default null,
  atratiocv double default null,
  datasourceid smallint(6) default null,
  primary key (fueltypeid,fuelsubtypeid,polprocessid,modelyeargroupid),
  unique key xpkminorhapratio (fueltypeid,fuelsubtypeid,polprocessid,modelyeargroupid)
);

create table modelyear (
       modelyearid          smallint not null
);

create unique index xpkmodelyear on modelyear
(
       modelyearid                    asc
);

create table modelyeargroup (
       modelyeargroupid     integer not null,
       shortmodyrgroupid    smallint null,
       modelyeargroupname   character(50) null,
       modelyeargroupstartyear smallint(6) default null,
       modelyeargroupendyear smallint(6) default null,
       primary key (modelyeargroupid)
);

create table fuelmodelyeargroup (
       fuelmygroupid        int not null,
       fuelmygroupname      char(100) null,
       fuelmygroupfunction  char(200) null,
       maxsulfurlevel   float null,
       maxsulfurlevelcv   float null,
       maxsulfurlevelgpa  float null,
       maxsulfurlevelgpacv  float null
);

create unique index xpkfuelmodelyeargroup on fuelmodelyeargroup
(
       fuelmygroupid        asc
);


create table modelyearcutpoints (
  cutpointname varchar(100) not null,
  modelyearid smallint(6) not null,
  primary key (cutpointname)
);

create table modelyearmapping (
  startusermodelyear smallint(6) not null,
  endusermodelyear smallint(6) not null,
  startstandardmodelyear smallint(6) not null,
  endstandardmodelyear smallint(6) not null,
  primary key (startusermodelyear, endusermodelyear)
);

create table monthgrouphour (
       monthgroupid         smallint not null,
       hourid               smallint not null,
       acactivityterma      float null,
       acactivitytermacv    float null,
       acactivitytermb      float null,
       acactivitytermbcv    float null,
       acactivitytermc      float null,
       acactivitytermccv    float null,
       key (hourid, monthgroupid)
);

alter table monthgrouphour add (
        key (monthgroupid),
        key (hourid)
);

create unique index xpkmonthgrouphour on monthgrouphour
(
       monthgroupid                   asc,
       hourid                         asc
);


create table monthgroupofanyyear (
       monthgroupid         smallint not null,
       monthgroupname       char(50) null
);

create unique index xpkmonthgroupofanyyear on monthgroupofanyyear
(
       monthgroupid                   asc
);


create table monthofanyyear (
       monthid              smallint not null,
       monthname            char(10) null,
       noofdays             smallint null,
       monthgroupid         smallint not null,
       key (monthgroupid, monthid),
       key (monthid, monthgroupid)
);

alter table monthofanyyear add (
        key (monthgroupid)
);

create unique index xpkmonthofanyyear on monthofanyyear
(
       monthid                        asc
);


create table monthvmtfraction (
       sourcetypeid         smallint not null,
       monthid              smallint not null,
       monthvmtfraction     float null
);

alter table monthvmtfraction add (
        key (sourcetypeid),
        key (monthid)
);

create unique index xpkmonthvmtfraction on monthvmtfraction
(
       sourcetypeid                   asc,
       monthid                        asc
);

create table nono2ratio  (
  polprocessid      int         not null,
  sourcetypeid      smallint(6)   not null,
  fueltypeid        smallint(6)   not null,
  modelyeargroupid    int(11)       not null,
  noxratio        float     null,
  noxratiocv        float     null,
  datasourceid      smallint(6)   null,
  primary key       (polprocessid, sourcetypeid, fueltypeid, modelyeargroupid)                
); 

create unique index xpknono2ratio on nono2ratio  
(
       polprocessid       asc,
       sourcetypeid     asc,
       fueltypeid     asc,
       modelyeargroupid   asc
);

create table nragecategory(
  ageid smallint(6) not null,
  agecategoryname char(50) default null,
  primary key (ageid),
  unique index xpknragecategory (ageid)
);

create table nratratio (
  pollutantid smallint(6) not null,
  processid smallint(6) not null,
  engtechid smallint(6) not null,
  fuelsubtypeid smallint(6) not null,
  nrhpcategory char(1) not null,
  atratio double default null,
  atratiocv double default null,
  datasourceid smallint(6) default null,
  primary key (pollutantid,processid,engtechid,fuelsubtypeid,nrhpcategory)
);

create table nrbaseyearequippopulation(
  sourcetypeid smallint(6) not null,
  stateid smallint(6) not null,
  population float default null,
  nrbaseyearid smallint(6) not null,
  primary key (sourcetypeid, stateid),
  unique index xpknrbaseyearequippopulation (sourcetypeid, stateid)
);

create table nrcrankcaseemissionrate (
  polprocessid int not null,
  scc char(10) not null,
  hpmin smallint(6) not null,
  hpmax smallint(6) not null,
  modelyearid smallint(6) not null,
  engtechid smallint(6) not null,
  meanbaserate float default null,
  units varchar(12) default null,
  datasourceid smallint(6) not null,
  primary key (polprocessid,scc,hpmin,hpmax,modelyearid,engtechid),
  index index1 (polprocessid),
  index xpfnrcrankcaseemissionratio (polprocessid),
  unique index xpknrprocessemissionrate (polprocessid)
);

create table nrdayallocation(
  scc char(10) not null,
  dayid smallint(6) not null,
  dayfraction float not null,
  primary key (scc, dayid)
);

create table nrdeterioration(
  polprocessid int not null,
  engtechid smallint(6) not null,
  dfcoefficient float default null,
  dfageexponent float default null,
  emissioncap smallint(6) not null,
  primary key (polprocessid, engtechid),
  unique index xpknrdeterioration (polprocessid, engtechid)
);

create table nrdioxinemissionrate (
  pollutantid smallint(6) not null,
  processid smallint(6) not null,
  fueltypeid smallint(6) not null,
  engtechid smallint(6) not null,
  nrhpcategory char(1) not null,
  units char(30) default null,
  meanbaserate double default null,
  meanbaseratecv double default null,
  datasourceid smallint(6) default null,
  primary key (pollutantid,processid,fueltypeid,engtechid,nrhpcategory)
);

create table  nremissionrate (
  polprocessid int not null,
  scc char(10) not null,
  hpmin smallint(6) not null,
  hpmax smallint(6) not null,
  modelyearid smallint(6) not null,
  engtechid smallint(6) not null,
  meanbaserate float default null,
  units varchar(12) default null,
  datasourceid smallint(6) not null,
  primary key (polprocessid,scc,hpmin,hpmax,modelyearid,engtechid)
);

create table  nrengtechfraction (
  scc char(10) not null,
  hpmin smallint(6) not null,
  hpmax smallint(6) not null,
  modelyearid smallint(6) not null,
  processgroupid smallint(6) not null,
  engtechid smallint(6) not null,
  nrengtechfraction float default null,
  primary key (scc,hpmin,hpmax,modelyearid,processgroupid,engtechid)
);

create table nrequipmenttype(
  nrequiptypeid smallint(6) not null,
  description char(40) default null,
  sectorid smallint(6) not null,
  usedefaultscrappage char(1) default null,
  surrogateid smallint(6) default null,
  primary key (nrequiptypeid),
  unique index xpknrequipmenttype (nrequiptypeid)
);

create table  nrevapemissionrate (
  polprocessid int not null,
  scc char(10) not null,
  hpmin smallint(6) not null,
  hpmax smallint(6) not null,
  modelyearid smallint(6) not null,
  engtechid smallint(6) not null,
  meanbaserate float default null,
  units varchar(12) default null,
  datasourceid smallint(6) not null,
  primary key (polprocessid,scc,hpmin,hpmax,modelyearid,engtechid)
);

create table nrfuelsupply (
  fuelregionid int(11) not null default '0',
  fuelyearid int(11) not null default '0',
  monthgroupid smallint(6) not null default '0',
  fuelformulationid smallint(6) not null default '0',
  marketshare float default null,
  marketsharecv float default null,
  primary key (fuelregionid,fuelformulationid,monthgroupid,fuelyearid),
  key countyid (fuelregionid),
  key yearid (fuelyearid),
  key monthgroupid (monthgroupid),
  key fuelsubtypeid (fuelformulationid)
);

create table nrfueltype(
  fueltypeid smallint(6) not null default 0,
  defaultformulationid smallint(6) not null default 0,
  fueltypedesc char(50) default null,
  humiditycorrectioncoeff float default null,
  humiditycorrectioncoeffcv float default null,
  fueldensity float default null,
  subjecttoevapcalculations char(1) not null default 'N',
  primary key (fueltypeid)
);

create table  nrfuelsubtype (
  fuelsubtypeid smallint(6) not null default '0',
  fueltypeid smallint(6) not null default '0',
  fuelsubtypedesc char(50) default null,
  fuelsubtypepetroleumfraction float default null,
  fuelsubtypepetroleumfractioncv float default null,
  fuelsubtypefossilfraction float default null,
  fuelsubtypefossilfractioncv float default null,
  carboncontent float default null,
  oxidationfraction float default null,
  carboncontentcv float default null,
  oxidationfractioncv float default null,
  energycontent float default null,
  primary key (fuelsubtypeid),
  key fueltypeid (fueltypeid,fuelsubtypeid)
);

create table nrgrowthindex(
  growthpatternid smallint(6) not null,
  yearid smallint(6) not null,
  growthindex smallint(6) default null,
  primary key (growthpatternid, yearid),
  unique index xpknrgrowthindex (growthpatternid, yearid)
);

create table nrgrowthpattern(
  growthpatternid smallint(6) not null,
  description char(80) default null,
  primary key (growthpatternid),
  unique index xpknrgrowthpattern (growthpatternid)
);

create table nrgrowthpatternfinder(
  scc char(10) not null,
  stateid smallint(6) not null,
  growthpatternid smallint(6) not null,
  primary key (scc, stateid),
  unique index xpknrgrowthpatternfinder (scc, stateid)
);

create table nrhcspeciation (
  pollutantid smallint(6) not null,
  processid smallint(6) not null,
  engtechid smallint(6) not null,
  fuelsubtypeid smallint(6) not null,
  nrhpcategory char(1) not null,
  speciationconstant double default null,
  speciationconstantcv double default null,
  datasourceid smallint(6) default null,
  primary key (pollutantid,processid,engtechid,fuelsubtypeid,nrhpcategory)
);

create table nrhourallocation(
  nrhourallocpatternid smallint(6) not null,
  hourid smallint(6) not null,
  hourfraction float not null,
  primary key (nrhourallocpatternid, hourid),
  unique index xpknrhourallocation (nrhourallocpatternid, hourid)
);

create table nrhourallocpattern(
  nrhourallocpatternid smallint(6) not null,
  description char(255) not null,
  primary key (nrhourallocpatternid),
  unique index xpknrhourallocpattern (nrhourallocpatternid)
);

create table nrhourpatternfinder(
  nrequiptypeid smallint(6) not null,
  nrhourallocpatternid smallint(6) default null,
  primary key (nrequiptypeid),
  unique index xpknrhourpatternfinder (nrequiptypeid)
);

create table nrhpcategory (
  nrhprangebinid smallint(6) not null,
  engtechid smallint(6) not null,
  nrhpcategory char(1) default null,
  primary key (nrhprangebinid,engtechid)
);

create table nrhprangebin(
  nrhprangebinid smallint(6) not null,
  binname char(20) default null,
  hpmin smallint(6) default null,
  hpmax smallint(6) default null,
  engsizeid smallint(6) not null,
  primary key (nrhprangebinid),
  unique index xpknrhprangebin (nrhprangebinid)
);

create table nrintegratedspecies (
  pollutantid smallint(6) not null,
  primary key (pollutantid)
);

create table nrmetalemissionrate (
  pollutantid smallint(6) not null,
  processid smallint(6) not null,
  fueltypeid smallint(6) not null,
  engtechid smallint(6) not null,
  nrhpcategory char(1) not null,
  units char(12) default null,
  meanbaserate double default null,
  meanbaseratecv double default null,
  datasourceid smallint(6) default null,
  primary key (pollutantid,processid,fueltypeid,engtechid,nrhpcategory)
);

create table nrmethanethcratio (
  processid smallint(6) not null,
  engtechid smallint(6) not null,
  fuelsubtypeid smallint(6) not null,
  nrhpcategory char(1) not null,
  ch4thcratio double default null,
  ch4thcratiocv double default null,
  datasourceid smallint(6) default null,
  primary key (processid,fuelsubtypeid,engtechid,nrhpcategory)
);

create table nrmonthallocation(
  scc char(10) not null,
  stateid smallint(6) not null,
  monthid smallint(6) not null,
  monthfraction float not null,
  primary key (scc, stateid, monthid)
);

create table nrpahgasratio (
  pollutantid smallint(6) not null,
  processid smallint(6) not null,
  fueltypeid smallint(6) not null,
  engtechid smallint(6) not null,
  nrhpcategory char(1) not null,
  atratio double default null,
  atratiocv double default null,
  datasourceid smallint(6) default null,
  primary key (pollutantid,processid,fueltypeid,engtechid,nrhpcategory)
);

create table nrpahparticleratio (
  pollutantid smallint(6) not null,
  processid smallint(6) not null,
  fueltypeid smallint(6) not null,
  engtechid smallint(6) not null,
  nrhpcategory char(1) not null,
  atratio double default null,
  atratiocv double default null,
  datasourceid smallint(6) default null,
  primary key (pollutantid,processid,fueltypeid,engtechid,nrhpcategory)
);

create table  nrretrofitfactors (
  retrofitstartyear smallint(6) not null,
  retrofitendyear smallint(6) not null,
  startmodelyear smallint(6) not null,
  endmodelyear smallint(6) not null,
  scc char(10) not null,
  engtechid smallint(6) not null,
  hpmin smallint(6) not null,
  hpmax smallint(6) not null,
  pollutantid smallint(6) not null,
  retrofitid smallint(6) not null,
  annualfractionretrofit float default null,
  retrofiteffectivefraction float default null,
  primary key (scc,engtechid,hpmin,hpmax,pollutantid,retrofitid)
);

create table nrscc(
  scc char(10) not null,
  nrequiptypeid smallint(6) not null,
  description char(40) default null,
  fueltypeid smallint(6) not null,
  primary key (scc),
  unique index xpknrscc (scc)
);

create table nrscrappagecurve(
  nrequiptypeid smallint(6) not null,
  fractionlifeused float not null,
  percentagescrapped float default null,
  primary key (nrequiptypeid, fractionlifeused),
  unique index xpknrscrappagecurve (nrequiptypeid, fractionlifeused)
);

create table nrsourceusetype (
  sourcetypeid smallint(6) not null,
  scc char(10) not null,
  nrhprangebinid smallint(6) not null,
  medianlifefullload float default null,
  hoursusedperyear float default null,
  loadfactor float default null,
  hpavg float default null,
  ispumpfilled char(1) default null,
  tankunits char(7) default null,
  tanksize float default null,
  tankfillfrac float default null,
  tankmetalfrac float default null,
  hoselength float default null,
  hosediameter float default null,
  hosemetalfrac float default null,
  marinefillneckhoselength float default null,
  marinefillneckhosediameter float default null,
  marinesupplyhoselength float default null,
  marinesupplyhosediameter float default null,
  marineventhoselength float default null,
  marineventhosediameter float default null,
  hotsoakspersho float default null,
  noninstmarinetankfrac float default null,
  marineinstplastictanktrailfrac float not null,
  marineinstplastictankwaterfrac float default null,
  marineinstmetaltanktrailerfrac float default null,
  marineinstmetaltankwaterfrac float default null,
  e10tankpermeationadjfac float default null,
  e10hosepermeationadjfac float default null,
  e10marinefillneckpermadjfac float default null,
  e10marinesupplyhosepermadjfac float default null,
  e10marineventhosepermadjfac float default null,
  primary key (sourcetypeid),
  unique key xpknrsourceusetype (sourcetypeid)
);

create table nrstatesurrogate(
  surrogateid smallint(6) not null default 0,
  stateid smallint(6) not null default 0,
  countyid int(11) not null default 0,
  surrogatequant float not null default 0,
  surrogateyearid smallint(6) not null default 2002,
  primary key (surrogateid, stateid, countyid, surrogateyearid)
);

create table nrsulfuradjustment (
  fueltypeid smallint(6) not null,
  engtechid smallint(6) not null,
  pmbasesulfur float not null,
  sulfatepmconversionfactor float not null,
  primary key (fueltypeid,engtechid),
  unique key xpknrsulfuradjustment (fueltypeid,engtechid)
);

create table nrsurrogate(
  surrogateid smallint(6) not null,
  description char(255) default null,
  surrogateabbr char(3) default null,
  primary key (surrogateid),
  unique index xpknrsurrogate (surrogateid)
);

create table nrusmonthallocation(
  scc char(10) not null,
  stateid smallint(6) not null,
  monthid smallint(6) not null,
  monthfraction float not null,
  primary key (scc, stateid, monthid)
);

create table offnetworklink (
  sourcetypeid smallint not null,
  vehiclepopulation float null,
  startfraction float null,
  extendedidlefraction float null,
  parkedvehiclefraction float null,
    zoneid integer not null default '0',
  primary key (zoneid, sourcetypeid),
  key (sourcetypeid, zoneid)
);

create table omdgpolprocessrepresented (
  polprocessid int not null,
  representingpolprocessid int not null,
  primary key (polprocessid),
  key (representingpolprocessid)
);

create table onroadretrofit (
  pollutantid               smallint(6) not null,
  processid                 smallint(6) not null,
  fueltypeid                smallint(6) not null,
  sourcetypeid              smallint(6) not null,
  retrofityearid            smallint(6) not null,
  beginmodelyearid          smallint(6) not null,
  endmodelyearid            smallint(6) not null,
  cumfractionretrofit       double not null default 0,
  retrofiteffectivefraction double not null default 0,
  primary key (pollutantid, processid, fueltypeid, sourcetypeid, retrofityearid, beginmodelyearid, endmodelyearid),
  key (retrofityearid)
);

create table operatingmode (
       opmodeid             smallint not null,
       opmodename           character(50) null,
       vsplower             float null,
       vspupper             float null,
       speedlower           float null,
       speedupper           float null,
       brakerate1sec        float null,
       brakerate3sec        float null,
       minsoaktime          smallint null,
       maxsoaktime          smallint null 
);

create unique index xpkoperatingmode on operatingmode
(
       opmodeid                       asc
);


create table opmodedistribution (
       sourcetypeid         smallint not null,
       hourdayid            smallint not null,
       linkid               integer not null,
       polprocessid         int not null,
       opmodeid             smallint not null,
       opmodefraction       float null,
       opmodefractioncv     float null
);

alter table opmodedistribution add (
        key (sourcetypeid),
        key (hourdayid),
        key (linkid),
        key (polprocessid),
        key (opmodeid)
);

create unique index xpkopmodedistribution on opmodedistribution
(
       sourcetypeid                   asc,
       hourdayid                      asc,
       linkid                         asc,
       polprocessid                   asc,
       opmodeid                       asc
);


create table opmodepolprocassoc (
       polprocessid         int not null,
       opmodeid             smallint not null,
       key (opmodeid, polprocessid)
);

alter table opmodepolprocassoc add (
        key (polprocessid),
        key (opmodeid)
);

create unique index xpkopmodepolprocassoc on opmodepolprocassoc
(
       polprocessid                   asc,
       opmodeid                       asc
);

create table oxythreshname 
(
  oxythreshid       smallint(6)   not null  default '0' primary key,
  oxythreshname     char(100)   null    default null  
);

create table pahgasratio (
  polprocessid int not null default '0',
  fueltypeid smallint(6) not null default '0',
  modelyeargroupid int(11) not null default '0',
  atratio double default null,
  atratiocv double default null,
  datasourceid smallint(6) default null,
  primary key (polprocessid,fueltypeid,modelyeargroupid),
  unique key xpkpahgasratio (polprocessid,fueltypeid,modelyeargroupid)
);

create table pahparticleratio (
  polprocessid int not null default '0',
  fueltypeid smallint(6) not null default '0',
  modelyeargroupid int(11) not null default '0',
  atratio double default null,
  atratiocv double default null,
  datasourceid smallint(6) default null,
  primary key (polprocessid,fueltypeid,modelyeargroupid),
  unique key xpkpaparticlehratio (polprocessid,fueltypeid,modelyeargroupid)
);

create table pm10emissionratio (
  polprocessid      int not null,
  sourcetypeid      smallint(6) not null,
  fueltypeid        smallint(6) not null,
  minmodelyearid      smallint(6) not null,
  maxmodelyearid      smallint(6) not null,
  pm10pm25ratio     float not null,
  pm10pm25ratiocv     float null
); 

create unique index xpkpm10emissionratio on pm10emissionratio
(
  polprocessid asc,
  sourcetypeid asc,
  fueltypeid asc,
  minmodelyearid asc,
  maxmodelyearid asc
);

create table pmspeciation (
  processid smallint not null,
  inputpollutantid smallint not null,
  sourcetypeid smallint not null,
  fueltypeid smallint not null,
  minmodelyearid smallint not null,
  maxmodelyearid smallint not null,
  outputpollutantid smallint not null,
  pmspeciationfraction double not null,
  primary key (processid, inputpollutantid, sourcetypeid, fueltypeid, minmodelyearid, maxmodelyearid, outputpollutantid)
);

create table pollutant (
       pollutantid          smallint not null,
       pollutantname        char(50) null,
       energyormass         char(6) null,
       globalwarmingpotential  smallint null,
       neipollutantcode     char(10) null,
       pollutantdisplaygroupid smallint null,
       shortname      varchar(50) null,
       isaffectedbyonroad tinyint(1) default '1',
       isaffectedbynonroad tinyint(1) default '0',
       primary key (pollutantid)
);

create table pollutantprocessassoc (
       polprocessid         int not null,
       processid            smallint not null,
       pollutantid          smallint not null,
       isaffectedbyexhaustim char(1) not null default "n",
       isaffectedbyevapim char(1) not null default "n",
       chainedto1 int null default null,
       chainedto2 int null default null,
       isaffectedbyonroad tinyint(1) not null default 1,
       isaffectedbynonroad tinyint(1) not null default 0,
       nrchainedto1 int null default null,
       nrchainedto2 int null default null,
       key (processid, pollutantid, polprocessid),
       key (pollutantid, processid, polprocessid),
       key (polprocessid, processid, pollutantid),
       key (polprocessid, pollutantid, processid)
);

alter table pollutantprocessassoc add (
        key (processid),
        key (pollutantid)
);

create unique index xpkpollutantprocessassoc on pollutantprocessassoc
(
       polprocessid                   asc
);

create table pollutantprocessmodelyear (
    polprocessid int not null ,
    modelyearid smallint not null ,
    modelyeargroupid int not null ,
    fuelmygroupid integer null,
    immodelyeargroupid integer null,
    key (modelyearid, polprocessid)
);

alter table pollutantprocessmodelyear add (
        key (polprocessid),
        key (modelyearid)
);
create unique index xpkpollutantprocessmodelyear on pollutantprocessmodelyear
(
       polprocessid                   asc,
       modelyearid                    asc
);

create table processdisplaygroup(
  processdisplaygroupid smallint(6) not null,
  processdisplaygroupname char(50) not null,
  displayasgroup char(1) not null,
  primary key (processdisplaygroupid),
  unique index xpkprocessdisplaygroup (processdisplaygroupid)
);

create table processgroupid(
  processgroupid smallint(6) not null,
  processgroupname char(15) not null,
  primary key (processgroupid)
);

create table refuelingfactors (
       fueltypeid           smallint not null primary key,
       defaultformulationid smallint null,
       vaporterma           float not null default 0,
       vaportermb           float not null default 0,
       vaportermc           float not null default 0,
       vaportermd           float not null default 0,
       vaporterme           float not null default 0,
       vaportermf           float not null default 0,
       vaporlowtlimit       float not null default 0,
       vaporhightlimit      float not null default 0,
       tanktdifflimit       float not null default 0,
       minimumrefuelingvaporloss float not null default 0,
       refuelingspillrate   float not null default 0,
       refuelingspillratecv float not null default 0,
       displacedvaporratecv float not null default 0
);

create unique index xpkrefuelingfactors on refuelingfactors
(
       fueltypeid                     asc
);

create table regulatoryclass (
    regclassid smallint not null primary key,
    regclassname char(25) null ,
    regclassdesc char(100) null 
    );
create unique index xpkregulatoryclass on regulatoryclass
(
       regclassid                     asc
);

create table region (
  regionid int not null,
  vv smallint(6),
  ww smallint(6),
  xx smallint(6),
  yy smallint(6),
  zz smallint(6),
  description varchar(150),
  primary key (regionid)
);

create table regioncode (
  regioncodeid int not null,
  regioncodedescription varchar(200) not null default '',
  primary key (regioncodeid)
);

create table regioncounty (
  regionid int not null,
  countyid int not null,
  regioncodeid int not null,
  fuelyearid int not null,
  primary key (regionid, countyid, regioncodeid, fuelyearid),
  key (countyid, fuelyearid, regioncodeid, regionid)
);

create table retrofitinputassociations (
  listname varchar(20) not null,
  commonname varchar(50) not null,
  primary key (listname, commonname),
  idealname varchar(50) not null
);

create unique index xpkretrofitinputassociations on retrofitinputassociations
(
  listname asc,
  commonname asc
);

create table roadidlefraction (
    dayid int,
    sourcetypeid int,
    roadtypeid int,
    avgspeedbinid int,
    roadidlefraction double,
    primary key (dayid,sourcetypeid,roadtypeid,avgspeedbinid)
);

create table roadtype (
       roadtypeid           smallint not null,
       roaddesc             char(50) null,
       isaffectedbyonroad tinyint(1) default 1,
       isaffectedbynonroad tinyint(1) default 0,
       shoulddisplay tinyint(1) default 1,
       primary key (roadtypeid)
);

create table roadtypedistribution (
       sourcetypeid         smallint not null,
       roadtypeid           smallint not null,
       roadtypevmtfraction  float null,
       key (roadtypeid, sourcetypeid)
);

alter table roadtypedistribution add (
        key (sourcetypeid),
        key (roadtypeid)
);

create unique index xpkroadtypedistribution on roadtypedistribution
(
       sourcetypeid                   asc,
       roadtypeid                     asc
);

create table samplevehicleday (
       vehid              integer not null,
       dayid        smallint not null,
       sourcetypeid         smallint not null
);

create unique index xpksamplevehicle on samplevehicleday
(
       vehid                asc,
       dayid        asc
);

create table samplevehiclesoaking (
  soakdayid smallint not null,
  sourcetypeid smallint not null,
  dayid smallint not null,
  hourid smallint not null,
  soakfraction double,
  primary key (soakdayid, sourcetypeid, dayid, hourid)
);

create table samplevehiclesoakingday (
  soakdayid smallint not null,
  sourcetypeid smallint not null,
  dayid smallint not null,
  f double,
  primary key (soakdayid, sourcetypeid, dayid)
);

create table samplevehiclesoakingdaybasis (
  soakdayid smallint not null,
  dayid smallint not null,
  f double,
  primary key (soakdayid, dayid)
);

create table samplevehiclesoakingdayused (
  soakdayid smallint not null,
  sourcetypeid smallint not null,
  dayid smallint not null,
  f double,
  primary key (soakdayid, sourcetypeid, dayid)
);

create table samplevehiclesoakingdaybasisused (
  soakdayid smallint not null,
  dayid smallint not null,
  f double,
  primary key (soakdayid, dayid)
);

create table samplevehicletrip (
        vehid               integer not null,
    dayid       smallint not null, 
        tripid            smallint not null,
        hourid        smallint null,
        priortripid     smallint null,
        keyontime     int null,
        keyofftime      int not null
);

create unique index xpksamplevehicletrip on samplevehicletrip
(
       vehid                asc,
       dayid        asc,
       tripid       asc
);

alter table samplevehicletrip add (
    key (vehid),
    key (dayid),
    key (tripid)
);

create table samplevehiclepopulation (
  sourcetypemodelyearid int(10) unsigned not null,
  sourcetypeid smallint(6) not null default '0',
  modelyearid smallint(6) not null default '0',
  fueltypeid smallint(5) unsigned not null,
  engtechid smallint(6) not null,
  regclassid smallint(5) unsigned not null,
  stmyfuelengfraction double not null,
  stmyfraction double not null,
  primary key (sourcetypemodelyearid,fueltypeid,engtechid,regclassid),
  key stmyft (sourcetypeid, modelyearid, fueltypeid)
);

create table scc (
  scc char(10) not null default '',
  fueltypeid smallint(6) not null default '0',
  sourcetypeid smallint(6) not null default '0',
  roadtypeid smallint(6) not null default '0',
  processid smallint(6) not null default '0',
  primary key (scc),
  key fueltypeid (fueltypeid),
  key sourcetypeid (sourcetypeid),
  key roadtypeid (roadtypeid),
  key processid (processid)
);

create table sector (
  sectorid smallint(6) not null,
  description char(40) default null,
  primary key (sectorid),
  unique key xpksector (sectorid)
);

create table sho (
       hourdayid            smallint not null,
       monthid              smallint not null,
       yearid               smallint not null,
       ageid                smallint not null,
       linkid               integer not null,
       sourcetypeid         smallint not null,
       sho                  float null,
       shocv                float null,
       distance             float null,
       key (linkid, yearid)
);

alter table sho add (
        key (hourdayid),
        key (monthid),
        key (yearid),
        key (ageid),
        key (linkid),
        key (sourcetypeid)
);

create unique index xpksho on sho
(
       hourdayid                      asc,
       monthid                        asc,
       yearid                         asc,
       ageid                          asc,
       linkid                         asc,
       sourcetypeid                   asc
);

create table sizeweightfraction (
       sourcetypemodelyearid integer not null,
       fueltypeid           smallint not null,
       engtechid            smallint not null,
       engsizeid            smallint not null,
       weightclassid        smallint not null,
       sizeweightfraction   float null
);

alter table sizeweightfraction add (
        key (sourcetypemodelyearid),
        key (fueltypeid),
        key (engtechid),
        key (engsizeid),
        key (weightclassid)
);

create unique index xpksizeweightfraction on sizeweightfraction
(
       sourcetypemodelyearid          asc,
       fueltypeid                     asc,
       engtechid                      asc,
       engsizeid                      asc,
       weightclassid                  asc
);

create table soakactivityfraction (
  sourcetypeid smallint not null,
  zoneid integer not null,
  monthid smallint not null,
  hourdayid smallint not null,
  opmodeid smallint not null,
  soakactivityfraction float,
  soakactivityfractioncv float
);

create unique index xpksoakactivityfraction on soakactivityfraction (
  sourcetypeid asc,
  zoneid asc,
  monthid asc,
  hourdayid asc,
  opmodeid asc
);

create table sourcebin (
       sourcebinid          bigint not null,
       engsizeid            smallint null,
       fueltypeid           smallint not null,
       engtechid            smallint not null,
       regclassid           smallint null,
       modelyeargroupid     integer null,
       weightclassid        smallint null,
       key (sourcebinid, fueltypeid, modelyeargroupid),
       key (sourcebinid, modelyeargroupid, fueltypeid),
       key (fueltypeid, modelyeargroupid, sourcebinid),
       key (fueltypeid, sourcebinid, modelyeargroupid),
       key (modelyeargroupid, fueltypeid, sourcebinid),
       key (modelyeargroupid, sourcebinid, fueltypeid)
);

alter table sourcebin add (
        key (fueltypeid),
        key (modelyeargroupid)
);

create unique index xpksourcebin on sourcebin
(
       sourcebinid                    asc
);


create table sourcebindistribution (
       sourcetypemodelyearid integer not null,
       polprocessid         int not null,
       sourcebinid          bigint not null,
       sourcebinactivityfraction float null,
       sourcebinactivityfractioncv float null
);

alter table sourcebindistribution add (
        key (sourcetypemodelyearid),
        key (polprocessid),
        key (sourcebinid)
);

create unique index xpksourcebindistribution on sourcebindistribution
(
       sourcetypemodelyearid          asc,
       polprocessid                   asc,
       sourcebinid                    asc
);

create table sourcehours (
       hourdayid            smallint not null,
       monthid              smallint not null,
       yearid               smallint not null,
       ageid                smallint not null,
       linkid               integer not null,
       sourcetypeid         smallint not null,
       sourcehours          float null,
       sourcehourscv        float null
);

alter table sourcehours add (
        key (hourdayid),
        key (monthid),
        key (yearid),
        key (ageid),
        key (linkid),
        key (sourcetypeid)
);

create unique index xpksourcehours on sourcehours
(
       hourdayid                      asc,
       monthid                        asc,
       yearid                         asc,
       ageid                          asc,
       linkid                         asc,
       sourcetypeid                   asc
);

create table sourcetypeage (
       ageid                smallint not null,
       sourcetypeid         smallint not null,
       survivalrate         float null,
       relativemar          float null,
       functioningacfraction float null,
       functioningacfractioncv float null,
       key (sourcetypeid, ageid)
);

alter table sourcetypeage add (
        key (ageid),
        key (sourcetypeid)
);

create unique index xpksourcetypeage on sourcetypeage
(
       ageid                          asc,
       sourcetypeid                   asc
);


create table sourcetypeagedistribution (
       sourcetypeid         smallint not null,
       yearid               smallint not null,
       ageid                smallint not null,
       agefraction          float null
);

alter table sourcetypeagedistribution add (
        key (sourcetypeid),
        key (yearid),
        key (ageid)
);

create unique index xpksourcetypeagedistribution on sourcetypeagedistribution
(
       sourcetypeid                   asc,
       yearid                         asc,
       ageid                          asc
);

create table sourcetypedayvmt (
  yearid smallint not null,
  monthid smallint not null,
  dayid smallint not null,
  sourcetypeid smallint not null,
  vmt double not null,
  primary key (yearid, monthid, dayid, sourcetypeid),
  key (sourcetypeid, yearid, monthid, dayid)
);

create table sourcetypehour (
       sourcetypeid         smallint not null,
       hourdayid            smallint not null,
       idleshofactor        float null,
       hotellingdist        double default null,
       primary key (sourcetypeid, hourdayid),
       key (hourdayid, sourcetypeid)
);


create table sourcetypemodelyear (
       sourcetypemodelyearid integer not null,
       modelyearid          smallint not null,
       sourcetypeid         smallint not null,
       acpenetrationfraction float null,
       acpenetrationfractioncv float null,
       key (sourcetypemodelyearid, modelyearid, sourcetypeid),
       key (sourcetypemodelyearid, sourcetypeid, modelyearid),
       key (sourcetypeid, modelyearid, sourcetypemodelyearid),
       key (modelyearid, sourcetypeid, sourcetypemodelyearid)
);

alter table sourcetypemodelyear add (
        key (modelyearid),
        key (sourcetypeid)
);

create unique index xpksourcetypemodelyear on sourcetypemodelyear
(
       sourcetypemodelyearid          asc
);

create table sourcetypemodelyeargroup (
  sourcetypeid smallint not null,
  modelyeargroupid integer not null,
  tanktemperaturegroupid smallint not null
);

create unique index xpksourcetypemodelyeargroup on sourcetypemodelyeargroup (
  sourcetypeid asc,
  modelyeargroupid asc  
);

create table sourcetypepolprocess (
       sourcetypeid         smallint not null,
       polprocessid         int not null,
       issizeweightreqd     char(1) null,
       isregclassreqd       char(1) null,
       ismygroupreqd        char(1) null,
       key (polprocessid, sourcetypeid)
);

alter table sourcetypepolprocess add (
        key (sourcetypeid),
        key (polprocessid)
);

create unique index xpksourcetypepolprocess on sourcetypepolprocess
(
       sourcetypeid                   asc,
       polprocessid                   asc
);

create table sourcetypetechadjustment (
       processid            smallint not null,
       sourcetypeid         smallint not null,
       modelyearid          smallint not null,
       refuelingtechadjustment float not null default 0.0
);

alter table sourcetypetechadjustment add (
       key (processid),
       key (sourcetypeid),
       key (modelyearid)
);

create unique index xpksourcetypetechadjustment on sourcetypetechadjustment
(
       processid    asc,
       sourcetypeid asc,
       modelyearid  asc
);

create table sourcetypeyear (
       yearid               smallint not null,
       sourcetypeid         smallint not null,
       salesgrowthfactor    float null,
       sourcetypepopulation float null,
       migrationrate        float null,
       key (sourcetypeid, yearid)
);

alter table sourcetypeyear add (
        key (yearid),
        key (sourcetypeid)
);

create unique index xpksourcetypeyear on sourcetypeyear
(
       yearid                         asc,
       sourcetypeid                   asc
);

create table sourcetypeyearvmt (
  yearid smallint not null,
  sourcetypeid smallint not null,
  vmt double not null,
  primary key (yearid, sourcetypeid),
  key (sourcetypeid, yearid)
);

create table sourceusetype (
       sourcetypeid         smallint not null,
       hpmsvtypeid          smallint not null,
       sourcetypename       char(50) null,

       key (sourcetypeid, hpmsvtypeid),
       key (hpmsvtypeid, sourcetypeid)
);

alter table sourceusetype add (
        key (hpmsvtypeid)
);

create unique index xpksourceusetype on sourceusetype
(
       sourcetypeid                   asc
);

create table sourceusetypephysics (
  sourcetypeid smallint not null,
  regclassid smallint not null,
  beginmodelyearid smallint not null,
  endmodelyearid smallint not null,

  rollingterma float default null,
  rotatingtermb float default null,
  dragtermc float default null,
  sourcemass float default null,
  fixedmassfactor float default null,

  primary key (sourcetypeid, regclassid, beginmodelyearid, endmodelyearid),
  key (beginmodelyearid, endmodelyearid, sourcetypeid, regclassid)
);

create table starts (
       hourdayid            smallint not null,
       monthid              smallint not null,
       yearid               smallint not null,
       ageid                smallint not null,
       zoneid               integer not null,
       sourcetypeid         smallint not null,
       starts               float null,
       startscv             float null
);

alter table starts add (
        key (hourdayid),
        key (monthid),
        key (yearid),
        key (ageid),
        key (zoneid),
        key (sourcetypeid)
);

create unique index xpkstarts on starts
(
       hourdayid                      asc,
       monthid                        asc,
       yearid                         asc,
       ageid                          asc,
       zoneid                         asc,
       sourcetypeid                   asc
);

create table startsageadjustment (
  sourcetypeid smallint(6) not null default 0,
  ageid smallint(6) not null default 0,
  ageadjustment double default null,
  primary key (sourcetypeid,ageid),
  key sourcetypeid (sourcetypeid),
  key ageid (ageid)
);

create table startshourfraction (
  dayid smallint(6) not null,
  hourid smallint(6) not null,
  sourcetypeid smallint(6) not null default '0',
  allocationfraction double not null,
  primary key (dayid,hourid,sourcetypeid)
);

create table startsmonthadjust (
    monthid smallint(6) not null,
    sourcetypeid smallint(6) not null default '0',
    monthadjustment double not null,
    primary key (monthid,sourcetypeid)
);

create table startsperday (
  dayid smallint(6) not null default 0,
  sourcetypeid smallint(6) not null default 0,
  startsperday double default null,
  primary key (sourcetypeid,dayid),
  key hourdayid (dayid),
  key sourcetypeid (sourcetypeid)
);

create table startsperdaypervehicle (
  dayid smallint(6) not null default 0,
  sourcetypeid smallint(6) not null default 0,
  startsperdaypervehicle double default null,
  primary key (sourcetypeid,dayid),
  key hourdayid (dayid),
  key sourcetypeid (sourcetypeid)
);

create table startspervehicle (
       sourcetypeid       smallint not null,
       hourdayid      smallint not null,
       startspervehicle   float null,
       startspervehiclecv float null,
       key (hourdayid, sourcetypeid)
);

create unique index xpkstartspervehicle on startspervehicle
(
       sourcetypeid         asc,
       hourdayid        asc
);

alter table startspervehicle add (
    key (sourcetypeid),
    key (hourdayid)
);

create table starttempadjustment (
       fueltypeid       smallint not null,
       polprocessid     int not null,
       modelyeargroupid   integer not null,
       opmodeid             smallint not null,
       starttempequationtype varchar(4) null,
       tempadjustterma    float null,
       tempadjusttermacv  float null,
       tempadjusttermb    float null,
       tempadjusttermbcv  float null,
       tempadjusttermc    float null,
       tempadjusttermccv  float null
);

create unique index xpkstarttempadjustment on starttempadjustment
(
       fueltypeid       asc,
       polprocessid     asc,
       modelyeargroupid   asc,
       opmodeid       asc
);

alter table starttempadjustment add (
    key (fueltypeid),
    key (polprocessid),
    key (modelyeargroupid),
    key (opmodeid)
);

create table state (
       stateid              smallint not null,
       statename            char(25) null,
       stateabbr            char(2) null,
       idleregionid     integer default '0' not null
);

create unique index xpkstate on state
(
       stateid                        asc
);

create table sulfateemissionrate  (
  polprocessid      int not null,
  fueltypeid        smallint(6) not null,
  modelyeargroupid    int(11)     not null,
  meanbaserate      float null,
  meanbaseratecv      float null,
  datasourceid      smallint(6),
  primary key       (polprocessid, fueltypeid, modelyeargroupid)                
); 

create unique index xpksulfateemissionrate on sulfateemissionrate 
(
       polprocessid       asc,
       fueltypeid     asc,
       modelyeargroupid   asc
);

create table sulfatefractions (
  processid smallint not null,
  fueltypeid smallint not null,
  sourcetypeid smallint not null,
  minmodelyearid smallint not null,
  maxmodelyearid smallint not null,
  sulfatenonecpmfraction double not null,
  h2ononecpmfraction double not null,
  basefuelsulfurlevel double not null,
  basefuelsulfatefraction double not null,
  datasourceid smallint(6) not null default '0',
  primary key (processid, fueltypeid, sourcetypeid, minmodelyearid, maxmodelyearid),
  key (processid, sourcetypeid, fueltypeid, minmodelyearid, maxmodelyearid),
  key (processid, minmodelyearid, maxmodelyearid, fueltypeid, sourcetypeid),
  key (processid, minmodelyearid, maxmodelyearid, sourcetypeid, fueltypeid)
);

create table sulfurbase  (
  modelyeargroupid  int(11)   not null  default '0' primary key,
  sulfurbase      float   null    default null,
  sulfurbasis     float   null    default '30.0',
  sulfurgpamax    float   null    default '330.0'
); 

create table sulfurcapamount (
  fueltypeid int not null primary key,
  sulfurcap double
);

create table sulfurmodelcoeff  (
  processid       smallint(6),
  pollutantid       smallint(6),
  m6emitterid       smallint(6),
  sourcetypeid      smallint(6),
  fuelmygroupid     int(8),
  sulfurfunctionid    smallint(6),
  sulfurcoeff       float,
  lowsulfurcoeff      double,
  primary key       (processid, pollutantid, m6emitterid, sourcetypeid, fuelmygroupid)                
); 

create table sulfurmodelname  (
  m6emitterid       smallint(6),
  sulfurfunctionid    smallint(6),
  m6emittername     char(10),
  sulfurfunctionname    char(10),
  primary key       (m6emitterid, sulfurfunctionid)
); 

create table tankvaporgencoeffs (
  ethanollevelid smallint(6) not null,
  altitude char(1) not null,
  tvgterma float null,
  tvgtermb float null,
  tvgtermc float null,
  primary key  (ethanollevelid,altitude)
);

create table tanktemperaturegroup (
  tanktemperaturegroupid smallint not null,
  tanktemperaturegroupname char(50)
);

create unique index xpktanktemperaturegroup on tanktemperaturegroup (
  tanktemperaturegroupid asc
);

create table tanktemperaturerise (
  tanktemperaturegroupid smallint not null,
  tanktemperatureriseterma float,
  tanktemperaturerisetermacv float,
  tanktemperaturerisetermb float,
  tanktemperaturerisetermbcv float
);

create unique index xpktanktemperaturerise on tanktemperaturerise (
  tanktemperaturegroupid asc
);

create table temperatureadjustment (
       polprocessid         int not null,
       fueltypeid           smallint not null,
       minmodelyearid   smallint not null default '1960',
       maxmodelyearid   smallint not null default '2060',
       tempadjustterma      float null,
       tempadjusttermacv    float null,
       tempadjusttermb      float null,
       tempadjusttermbcv    float null,
       tempadjusttermc      float null,
       tempadjusttermccv    float null,
       key (fueltypeid, polprocessid, minmodelyearid, maxmodelyearid)
);

alter table temperatureadjustment add (
        key (polprocessid),
        key (fueltypeid),
        key (minmodelyearid, maxmodelyearid)
);

create unique index xpktemperatureadjustment on temperatureadjustment
(
       polprocessid                   asc,
       fueltypeid                     asc,
       minmodelyearid         asc,
       maxmodelyearid         asc
);

create table temperaturefactorexpression (
  processid smallint not null,
  pollutantid smallint not null,
  fueltypeid smallint not null,
  sourcetypeid smallint not null,
  minmodelyearid smallint not null,
  maxmodelyearid smallint not null,
  tempcorrectionexpression varchar(5000),
  primary key (processid, pollutantid, fueltypeid, sourcetypeid, minmodelyearid, maxmodelyearid)
);

create table temperatureprofileid (
       temperatureprofileid bigint not null primary key,
       zoneid               integer not null,
       monthid              smallint not null,
       key (zoneid, monthid, temperatureprofileid),
       key (monthid, zoneid, temperatureprofileid)
);

create table togspeciation  (
  fuelsubtypeid     smallint(6) not null,
  regclassid        smallint(6) not null,
  processid       smallint(6) not null,
  modelyeargroupid    int     not null,
  togspeciationprofileid  varchar(10) not null default '0',
  primary key (fuelsubtypeid, regclassid, processid, modelyeargroupid)
);

create table togspeciationprofile (
  mechanismid           smallint(6)   not null, 
  togspeciationprofileid      varchar(10)   not null default '0',
  integratedspeciessetid      smallint(6)   not null,
  pollutantid           smallint(6)   not null,
  lumpedspeciesname       varchar(20)   not null,
  togspeciationdivisor      double      null,
  togspeciationmassfraction   double      null,
  primary key (mechanismid, togspeciationprofileid, integratedspeciessetid, pollutantid, lumpedspeciesname)
);

create table togspeciationprofilename (
  togspeciationprofileid      varchar(10)   not null default '0',
  togspeciationprofilename    varchar(100)  null,
  datasourceid          int       null,
  primary key (togspeciationprofileid),
  key (togspeciationprofilename)
);

create table totalidlefraction (
  idleregionid int not null,
  countytypeid int not null,
  sourcetypeid smallint not null,
  monthid smallint not null,
  dayid smallint not null,
  minmodelyearid smallint not null,
  maxmodelyearid smallint not null,
  totalidlefraction double not null,
  primary key (idleregionid, countytypeid, sourcetypeid, monthid, dayid, minmodelyearid, maxmodelyearid)
);

create table weightclass (
       weightclassid        smallint not null,
       weightclassname      char(50) null,
       midpointweight       float null
);

create unique index xpkweightclass on weightclass
(
       weightclassid                  asc
);


create table year (
       yearid               smallint not null,
       isbaseyear           char(1) null,
       fuelyearid           int not null default '0'
);

alter table year add (
        key (isbaseyear)
);

create unique index xpkyear on year
(
       yearid                         asc
);


create table zone (
       zoneid               integer not null,
       countyid             integer not null,
       startallocfactor     double null,
       idleallocfactor      double null,
       shpallocfactor        double null,
       key (zoneid, countyid),
       key (countyid, zoneid)
);

alter table zone add (
        key (countyid)
);

create unique index xpkzone on zone
(
       zoneid                         asc
);

create table zonemonthhour (
       monthid              smallint not null,
       zoneid               integer not null,
       hourid               smallint not null,
       temperature          float null,
       temperaturecv        float null,
       relhumidity          float null,
       heatindex            float null,
       specifichumidity     float null,
       relativehumiditycv   float null
);

alter table zonemonthhour add (
        key (monthid),
        key (zoneid),
        key (hourid)
);

create unique index xpkzonemonthhour on zonemonthhour
(
       monthid                        asc,
       zoneid                         asc,
       hourid                         asc
);


create table zoneroadtype (
       zoneid               integer not null,
       roadtypeid           smallint not null,
       shoallocfactor       double null,
       key (roadtypeid, zoneid)
);

alter table zoneroadtype add (
        key (zoneid),
        key (roadtypeid)
);

create unique index xpkzoneroadtype on zoneroadtype
(
       zoneid                         asc,
       roadtypeid                     asc
);
