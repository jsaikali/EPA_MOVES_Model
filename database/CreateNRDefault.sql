-- author wesley faler
-- author don smith
-- version 2013-04-15

create table nragecategory (
  ageid smallint(6) not null,
  agecategoryname char(50) default null,
  primary key (ageid),
  unique key xpknragecategory (ageid)
);

create table nrbaseyearequippopulation (
  sourcetypeid smallint(6) not null,
  stateid smallint(6) not null,
  population float default null,
  nrbaseyearid smallint(6) not null,
  primary key (sourcetypeid,stateid),
  unique key xpknrbaseyearequippopulation (sourcetypeid,stateid)
);

create table nrcrankcaseemissionratio (
  sourcetypeid smallint(6) not null,
  polprocessid int not null,
  sourcebinid bigint(20) not null,
  nrprocessmeanbaserate float default null,
  nrprocessmeanbaseratecv float default null,
  datasourceid smallint(6) not null,
  primary key (sourcetypeid,polprocessid,sourcebinid),
  unique key xpknrprocessemissionrate (sourcetypeid,polprocessid,sourcebinid),
  key index1 (sourcetypeid,polprocessid,sourcebinid),
  key xpfnrcrankcaseemissionratio (sourcetypeid,polprocessid,sourcebinid)
);

create table nrdayallocation (
  nrequiptypeid smallint(6) not null,
  dayid smallint(6) not null,
  dayfraction float not null,
  primary key (nrequiptypeid,dayid),
  unique key xpknrdayallocation (nrequiptypeid,dayid)
);

create table nrdeterioration (
  polprocessid int not null,
  engtechid smallint(6) not null,
  dfcoefficient float default null,
  dfageexponent float default null,
  emissioncap smallint(6) not null,
  primary key (polprocessid,engtechid),
  unique key xpknrdeterioration (polprocessid,engtechid)
);

create table nrengtechfraction (
  sourcetypeid smallint(6) not null,
  modelyearid smallint(6) not null,
  processid smallint(6) not null,
  engtechid smallint(6) not null,
  nrengtechfraction float not null,
  primary key (sourcetypeid,modelyearid,processid,engtechid),
  unique key xpknrengtechfraction (sourcetypeid,modelyearid,processid,engtechid)
);

create table nrequipmenttype (
  nrequiptypeid smallint(6) not null,
  description char(40) default null,
  sectorid smallint(6) not null,
  usedefaultscrappage char(1) default null,
  surrogateid smallint(6) default null,
  primary key (nrequiptypeid),
  unique key xpknrequipmenttype (nrequiptypeid)
);

create table nrevapemissionrate (
  sourcetypeid smallint(6) not null,
  polprocessid int not null,
  sourcebinid bigint(20) not null,
  nrprocessmeanbaserate float default null,
  nrprocessmeanbaseratecv float default null,
  datasourceid smallint(6) not null,
  primary key (sourcetypeid,polprocessid,sourcebinid),
  unique key xpknrprocessemissionrate (sourcetypeid,polprocessid,sourcebinid),
  key index1 (sourcetypeid,polprocessid,sourcebinid),
  key xpfnrevapemissionrate (sourcetypeid,polprocessid,sourcebinid)
);

create table nrexhaustemissionrate (
  sourcetypeid smallint(6) not null,
  polprocessid int not null,
  sourcebinid bigint(20) not null,
  nrprocessmeanbaserate float default null,
  nrprocessmeanbaseratecv float default null,
  datasourceid smallint(6) not null,
  primary key (sourcetypeid,polprocessid,sourcebinid),
  unique key xpknrexhaustemissionrate (sourcetypeid,polprocessid,sourcebinid)
);

create table nrfueloxyadjustment (
  strokes tinyint(4) not null,
  polprocessid int not null,
  fueltypeid smallint(6) not null,
  nrfueloxyadjust float default null,
  primary key (strokes,polprocessid,fueltypeid),
  unique key xpknrfueloxyadjustment (strokes,polprocessid,fueltypeid)
);

create table nrgrowthindex (
  growthpatternid smallint(6) not null,
  yearid smallint(6) not null,
  growthindex smallint(6) default null,
  primary key (growthpatternid,yearid),
  unique key xpknrgrowthindex (growthpatternid,yearid)
);

create table nrgrowthpattern (
  growthpatternid smallint(6) not null,
  description char(80) default null,
  primary key (growthpatternid),
  unique key xpknrgrowthpattern (growthpatternid)
);

create table nrgrowthpatternfinder (
  scc char(10) not null,
  stateid smallint(6) not null,
  growthpatternid smallint(6) not null,
  primary key (scc,stateid),
  unique key xpknrgrowthpatternfinder (scc,stateid)
);

create table nrhourallocation (
  nrhourallocpatternid smallint(6) not null,
  hourid smallint(6) not null,
  hourfraction float not null,
  primary key (nrhourallocpatternid,hourid),
  unique key xpknrhourallocation (nrhourallocpatternid,hourid)
);

create table nrhourallocpattern (
  nrhourallocpatternid smallint(6) not null,
  description char(255) not null,
  primary key (nrhourallocpatternid),
  unique key xpknrhourallocpattern (nrhourallocpatternid)
);

create table nrhourpatternfinder (
  nrequiptypeid smallint(6) not null,
  nrhourallocpatternid smallint(6) default null,
  primary key (nrequiptypeid),
  unique key xpknrhourpatternfinder (nrequiptypeid)
);

create table nrhprangebin (
  nrhprangebinid smallint(6) not null,
  binname char(20) default null,
  hpmin smallint(6) default null,
  hpmax smallint(6) default null,
  engsizeid smallint(6) not null,
  primary key (nrhprangebinid),
  unique key xpknrhprangebin (nrhprangebinid)
);

create table nrmonthallocation (
  nrequiptypeid smallint(6) not null,
  stateid smallint(6) not null,
  monthid smallint(6) not null,
  monthfraction float not null,
  primary key (nrequiptypeid,stateid,monthid),
  unique key xpknrmonthallocation (nrequiptypeid,stateid,monthid)
);

create table nrpollutantprocessmodelyear (
  polprocessid int not null,
  modelyearid smallint(6) not null,
  modelyeargroupid int(11) not null,
  primary key (polprocessid,modelyearid),
  unique key xpknrpollutantprocessmodelyear (polprocessid,modelyearid)
);

create table nrprocessemissionrate (
  sourcetypeid smallint(6) not null,
  polprocessid int not null,
  sourcebinid bigint(20) not null,
  nrprocessmeanbaserate float default null,
  nrprocessmeanbaseratecv float default null,
  datasourceid smallint(6) not null,
  primary key (sourcetypeid,polprocessid,sourcebinid),
  unique key xpknrprocessemissionrate (sourcetypeid,polprocessid,sourcebinid)
);

create table nrscc (
  scc char(10) not null,
  nrequiptypeid smallint(6) not null,
  strokes tinyint(4) default null,
  description char(40) default null,
  fueltypeid smallint(6) not null,
  primary key (scc),
  unique key xpknrscc (scc)
);

create table nrscrappagecurve (
  nrequiptypeid smallint(6) not null,
  fractionlifeused float not null,
  percentagescrapped float default null,
  primary key (nrequiptypeid,fractionlifeused),
  unique key xpknrscrappagecurve (nrequiptypeid,fractionlifeused)
);

create table nrsourcebin (
  sourcebinid bigint(20) not null,
  fueltypeid smallint(6) not null default '0',
  engtechid smallint(6) not null default '0',
  modelyeargroupid smallint(5) unsigned not null,
  engsizeid smallint(6) unsigned not null,
  primary key (sourcebinid),
  key index_srcbin (fueltypeid,engtechid,engsizeid)
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

create table nrstatesurrogatetotal (
  surrogateid smallint(6) not null,
  stateid smallint(6) not null,
  surrogatequant float not null,
  surrogateyearid smallint(6) default null,
  primary key (surrogateid,stateid),
  unique key xpknrstatesurrogatetotal (surrogateid,stateid)
);

create table nrsulfuradjustment (
  fueltypeid smallint(6) not null,
  engtechid smallint(6) not null,
  pmbasesulfur float not null,
  sulfatepmconversionfactor float not null,
  primary key (fueltypeid,engtechid),
  unique key xpknrsulfuradjustment (fueltypeid,engtechid)
);

create table nrsurrogate (
  surrogateid smallint(6) not null,
  description char(255) default null,
  primary key (surrogateid),
  unique key xpknrsurrogate (surrogateid)
);

create table nrtemperatureadjustment (
  strokes tinyint(4) not null,
  polprocessid int not null,
  fueltypeid smallint(6) not null,
  nrtemperatureadjustgt75 float default null,
  nrtemperatureadjustlt75 float default null,
  primary key (strokes,polprocessid,fueltypeid),
  unique key xpknrtemperatureadjustment (strokes,polprocessid,fueltypeid)
);

create table nrtransientadjustfactor (
  nrequiptypeid smallint(6) not null,
  nrhprangebinid smallint(6) not null,
  polprocessid int not null,
  fueltypeid smallint(6) not null,
  engtechid smallint(6) not null,
  nrtransientadjustfactor float not null,
  primary key (nrequiptypeid,nrhprangebinid,polprocessid,fueltypeid,engtechid),
  unique key xpknrtransientadjustfactor (nrequiptypeid,nrhprangebinid,polprocessid,fueltypeid,engtechid)
);

create table nryear (
  yearid smallint not null,
  isbaseyear char(1) null,
  fuelyearid smallint not null default '0',
  primary key (yearid),
  key (isbaseyear)
);

create table nrzoneallocation (
  surrogateid smallint(6) not null,
  stateid smallint(6) not null,
  zoneid int(11) not null,
  surrogatequant float not null,
  primary key (surrogateid,stateid,zoneid),
  unique key xpknrzoneallocation (surrogateid,stateid,zoneid)
);

