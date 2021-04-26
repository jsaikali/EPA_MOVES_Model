/*
   Version 2017-09-29
   -- MOVESRun table structure modified by Mitch Cumberworth per Task 206
   -- Foreign keys removed by Wesley Faler Oct. 2007 to speedup Master-side inserts
   -- Output and activity primary keys and unique keys removed by Wesley Faler Jan. 2008 to speedup Master-side inserts
   -- MOVESRun table structure modified by Gwo Shyu per Task 812 "MOVES performance Improvement ...": 
		(1) Added a new table ActivityType
		(2) Structure of MOVESActivityOutput and MOVESOutput were modified - fields changed, and no primary key nor indexes
   -- MOVESRun table structure modified by Wes Faler per Task 902 to add Domain information
   -- MOVESOutput table structure modified by MJimenez 29Feb2012 add engTechID and sectorID for NONROAD
   -- MOVESActivityOutput table structure modified by MJimenez 29Feb2012 add engTechID and sectorID for NONROAD
   -- Merged Michele's changes with the changes done by Wes etc.
*/
/* Creates tables in the MOVESOutput Database */
drop table if exists movesoutput;
drop table if exists movesactivityoutput;
drop table if exists movesrun;
drop table if exists moveserror;
drop table if exists moveseventlog;
drop table if exists movesworkersused;
drop table if exists bundletracking;
drop table if exists activitytype;
drop table if exists movestablesused;
drop table if exists rateperdistance;
drop table if exists ratepervehicle;
drop table if exists rateperprofile;
drop table if exists startspervehicle;
drop table if exists rateperstart;
drop table if exists rateperhour;

create table moveseventlog (
	eventrecordid        int unsigned not null,
	movesrunid           smallint unsigned not null,
    primary key (eventrecordid, movesrunid),
	eventname            char(255) not null,
	whenstarted          int unsigned not null,
	whenstopped          int unsigned null,
	duration             int unsigned null
);

-- ***********************************************************************************
-- ***********************************************************************************
-- MOVESOutput table.  Stores one row for each combination
-- of dimension field values, which includes pollutant. 
--
-- Note that dimension fields will never be null but they
-- may hold a default value that indicates an "all" selection.
-- ***********************************************************************************
-- ***********************************************************************************
-- No PK nor indexes
CREATE TABLE movesoutput (
	movesrunid           smallint unsigned not null,
	iterationid          smallint unsigned null default 1,
	yearid               smallint unsigned null default null,
	monthid              smallint unsigned null default null,
	dayid                smallint unsigned null default null,
	hourid               smallint unsigned null default null,
-- ******************************************************
-- stateid, locationid, zoneid, and linkid can all be default
-- in the case where the user selected "nation" as the 
-- geographic granularity for the output.
-- linkid and/or zoneid will be default otherwise if "county" 
-- level granularity was selected depending upon scale.
-- locationid will be default otherwise if "state" level
-- granularity was selected.
-- ******************************************************
	stateid              smallint unsigned null default null,
	countyid             integer  unsigned null default null,
	zoneid               integer  unsigned null default null,
	linkid               integer  unsigned null default null,
	pollutantid          smallint unsigned null default null,
	processid            smallint unsigned null default null,
	sourcetypeid         smallint unsigned null default null,
	regclassid           smallint unsigned null default null,
	fueltypeid           smallint unsigned null default null,
	fuelsubtypeid        smallint unsigned null default null,
	modelyearid          smallint unsigned null default null,
-- ******************************************************
-- roadtypeid is not redundant with linkid in the cases where
-- the user wants road type as a dimension but does not want
-- geographic detail to the link/zone (or perhaps even to
-- the county) level.
-- ******************************************************
	roadtypeid           smallint unsigned null default null,
-- ******************************************************
-- scc holds both onroad and offroad scc codes and may be
-- all 0's (zeroes) to represent "all" scc codes at once.
-- ******************************************************
	scc                  char(10) null default null,
-- ******************************************************
-- offroad keys
-- ******************************************************
	engtechid            smallint unsigned null default null,
	sectorid             smallint unsigned null default null,
	hpid                 smallint unsigned null default null,
-- ******************************************************
-- the emission* columns are the actual values produced,
-- not dimensions to the data.  these will be null if the
-- user chose not to generate them.
-- ******************************************************
	emissionquant        float null default null,
	emissionquantmean    float null default null,
	emissionquantsigma   float null  default null
) ENGINE=MyISAM DEFAULT CHARSET=latin1 DELAY_KEY_WRITE=1;

CREATE TABLE movesrun (
	movesrunid           smallint unsigned not null auto_increment,
-- ******************************************************
-- outputtimeperiod has values 'hour', 'day', 'month', or 'year'
-- ******************************************************
	outputtimeperiod     char(5) null default null,
	timeunits            char(5) null default null,
	distanceunits        char(5) null default null,
	massunits            char(5) null default null,
	energyunits          char(5) null default null,
-- ******************************************************
-- runspecfilename can be null if the user has not saved
-- their runspec prior to launching the simulation.
-- ******************************************************
	runspecfilename      varchar(500) null default null,
	runspecdescription   text null,
	runspecfiledatetime  datetime null default null,
	rundatetime          datetime null default null,
-- ******************************************************
-- scale has values 'macro', 'meso', 'micro'
-- ******************************************************
	scale                char(5) null default null,
	minutesduration      float null  default null,
	defaultdatabaseused  varchar(200) null default null,
	masterversion        varchar(100) null default null,
	mastercomputerid     varchar(255) null default null,
	masteridnumber       varchar(255) null default null,
-- ******************************************************
-- domain has values 'NATIONAL', 'SINGLE', 'PROJECT'
-- ******************************************************
	domain               CHAR(10) NULL DEFAULT 'NATIONAL',
	domaincountyid		 integer unsigned null default null,
	domaincountyname     varchar(50) null default null,
	domaindatabaseserver varchar(100) null default null,
	domaindatabasename   varchar(200) null default null,

	expecteddonefiles    integer unsigned null default null,
	retrieveddonefiles   integer unsigned null default null,

	models               varchar(40) not null default 'onroad',

	PRIMARY KEY (movesrunid)
) ENGINE=MyISAM DEFAULT CHARSET=latin1 DELAY_KEY_WRITE=1;

CREATE TABLE moveserror (
	moveserrorid         integer  unsigned not null auto_increment,
	movesrunid           smallint unsigned not null,
	yearid               smallint unsigned null default null,
	monthid              smallint unsigned null default null,
	dayid                smallint unsigned null default null,
	hourid               smallint unsigned null default null,
	stateid              smallint unsigned null default null,
	countyid             integer unsigned null default null,
	zoneid               integer unsigned null default null,
	linkid               integer unsigned null default null,
	pollutantid          smallint unsigned null default null,
	processid            smallint unsigned null default null,
	errormessage         varchar(255) not null,
	primary key (moveserrorid),
	key ix_moves_error_id (moveserrorid),
	key ix_moves_run_id (movesrunid)
) ENGINE=MyISAM DEFAULT CHARSET=latin1 DELAY_KEY_WRITE=1;

-- ***********************************************************************************
-- ***********************************************************************************
-- MOVESActivityOutput table. Used for "Additional Outputs" which are not
-- pollutant dependent such as distance. 
-- ***********************************************************************************
-- ***********************************************************************************
-- No PK nor indexes
CREATE TABLE movesactivityoutput (
	movesrunid           smallint unsigned not null,
	iterationid          smallint unsigned null default 1,
	yearid               smallint unsigned null default null,
	monthid              smallint unsigned null default null,
	dayid                smallint unsigned null default null,
	hourid               smallint unsigned null default null,
-- ******************************************************
-- stateID, locationID, zoneID, and linkID can all be default
-- in the case where the user selected "Nation" as the 
-- geographic granularity for the output.
-- linkID and/or zoneID will be default otherwise if "County" 
-- level granularity was selected depending upon scale.
-- locationID will be default otherwise if "State" level
-- granularity was selected.
-- ******************************************************
	stateid              smallint unsigned null default null,
	countyid             integer unsigned null default null,
	zoneid               integer unsigned null default null,
	linkid               integer unsigned null default null,
	sourcetypeid         smallint unsigned null default null,
	regclassid           smallint unsigned null default null,
	fueltypeid           smallint unsigned null default null,
	fuelsubtypeid        smallint unsigned null default null,
	modelyearid          smallint unsigned null default null,
-- ******************************************************
-- roadTypeID is not redundant with linkID in the cases where
-- the user wants road type as a dimension but does not want
-- geographic detail to the link/zone (or perhaps even to
-- the County) level.
-- ******************************************************
	roadtypeid           smallint unsigned null default null,
-- ******************************************************
-- scc holds both onroad and offroad scc codes and may be
-- all 0's (zeroes) to represent "all" scc codes at once.
-- ******************************************************
	scc                  char(10) null default null,
	engtechid            smallint unsigned null default null,
	sectorid             smallint unsigned null default null,
	hpid                 smallint unsigned null default null,
	activitytypeid       smallint not null,
	activity             float null default null,
	activitymean         float null default null,
	activitysigma        float null default null 
) ENGINE=MyISAM DEFAULT CHARSET=latin1 DELAY_KEY_WRITE=1;

CREATE TABLE activitytype (
	activitytypeid       smallint unsigned not null,
	activitytype         char(20) not null,
	activitytypedesc     char(50) null default null,
	primary key (activitytypeid)
) ENGINE=MyISAM DEFAULT CHARSET=latin1 DELAY_KEY_WRITE=1;

-- add records
insert ignore into activitytype (activitytypeid, activitytype, activitytypedesc)
VALUES (1, "distance", "Distance traveled");
insert ignore into activitytype (activitytypeid, activitytype, activitytypedesc)
VALUES (2, "sourcehours", "Source Hours");
insert ignore into activitytype (activitytypeid, activitytype, activitytypedesc)
VALUES (3, "extidle", "Extended Idle Hours");
insert ignore into activitytype (activitytypeid, activitytype, activitytypedesc)
VALUES (4, "sho", "Source Hours Operating");
insert ignore into activitytype (activitytypeid, activitytype, activitytypedesc)
VALUES (5, "shp", "Source Hours Parked");
insert ignore into activitytype (activitytypeid, activitytype, activitytypedesc)
VALUES (6, "population", "Population");
insert ignore into activitytype (activitytypeid, activitytype, activitytypedesc)
VALUES (7, "starts", "Starts");
-- insert ignore into activitytype (activitytypeid, activitytype, activitytypedesc)
-- VALUES (8, "hotelling", "Hotelling Hours")
insert ignore into activitytype (activitytypeid, activitytype, activitytypedesc)
VALUES (9, "avghp", "Average Horsepower");
insert ignore into activitytype (activitytypeid, activitytype, activitytypedesc)
VALUES (10, "retrofrac", "Fraction Retrofitted");
insert ignore into activitytype (activitytypeid, activitytype, activitytypedesc)
VALUES (11, "retrocnt", "Number Units Retrofitted");
insert ignore into activitytype (activitytypeid, activitytype, activitytypedesc)
VALUES (12, "loadfactor", "Load Factor");
insert ignore into activitytype (activitytypeid, activitytype, activitytypedesc)
VALUES (13, "hotellingAux", "Hotelling Diesel Aux");
insert ignore into activitytype (activitytypeid, activitytype, activitytypedesc)
VALUES (14, "hotellingElectric", "Hotelling Battery or AC");
insert ignore into activitytype (activitytypeid, activitytype, activitytypedesc)
VALUES (15, "hotellingOff", "Hotelling All Engines Off");
insert ignore into activitytype (activitytypeid, activitytype, activitytypedesc)
VALUES (16, "shi", "Source Hours Idle");



CREATE TABLE movesworkersused (x
	movesrunid           smallint unsigned not null,
	workerversion        varchar(100) not null,
	workercomputerid     varchar(255) not null,
	workerid             varchar(255) not null default '',
	bundlecount          integer unsigned not null default '0',
	failedbundlecount    integer unsigned not null default '0',
	primary key (movesrunid, workerversion, workercomputerid, workerid)
) ENGINE=MyISAM DEFAULT CHARSET=latin1 DELAY_KEY_WRITE=1;

CREATE TABLE bundletracking (
	movesrunid           smallint unsigned not null,
	-- 'm' for master, 'w' for worker
	hosttype             char(1) not null default ' ',
	loopableclassname 	 varchar(200) not null default '',

	-- worker fields will be blank ('') for tasks done on a master
	workerversion        varchar(100) not null,
	workercomputerid     varchar(255) not null,
	workerid             varchar(255) not null default '',
	-- bundlenumber will be 0 for tasks done on a master, even if the task is done on behalf of a calculator
	bundlenumber		 int not null default '0',
	-- iscleanup is set to 'n' for bundles done on a worker
	iscleanup 			 char(1) not null default 'N', 

	iterationid 		 smallint unsigned null default null,
	processid 			 smallint unsigned null default null,
	roadtypeid		 	 smallint unsigned null default null,
	linkid		 		 integer unsigned null default null,
	zoneid 				 integer unsigned null default null,
	countyid 			 integer unsigned null default null,
	stateid 			 smallint unsigned null default null,
	yearid 				 smallint unsigned null default null,
	monthid 			 smallint unsigned null default null,
	dayid 				 smallint unsigned null default null,
	hourid 				 smallint unsigned null default null,
	executiongranularity varchar(10) null default null,
	executionpriority 	 smallint unsigned null,

	durationseconds		 float null default null,

	-- There is no primary key in this table, but the following KEY is
	-- useful when searching for performance bottlenecks.
	KEY (movesrunid, hosttype, loopableclassname)
) ENGINE=MyISAM DEFAULT CHARSET=latin1 DELAY_KEY_WRITE=1;

CREATE TABLE movestablesused (
	movesrunid           smallint unsigned not null,
	databaseserver		 varchar(100) not null default '',
	databasename		 varchar(200) not null,
	tablename			 varchar(200) not null,
	datafilesize	     integer unsigned null default null,
	datafilemodificationdate datetime null default null,
	tableusesequence	 integer unsigned not null auto_increment,
	primary key (movesrunid, databaseserver, databasename, tablename),
	key (movesrunid, tableusesequence)
) ENGINE=MyISAM DEFAULT CHARSET=latin1 DELAY_KEY_WRITE=1;

-- ***********************************************************************************
-- ***********************************************************************************
-- RatePerDistance table
-- Includes emissions for the processes:  Running exhaust, tire wear, brake wear,
-- crankcase, and refueling
-- ***********************************************************************************
-- ***********************************************************************************
-- No PK nor indexes
CREATE TABLE rateperdistance (
	movesscenarioid		 varchar(40) not null default '',
	movesrunid           smallint unsigned not null,
	yearid               smallint unsigned null default null,
	monthid              smallint unsigned null default null,
	dayid                smallint unsigned null default null,
	hourid               smallint unsigned null default null,
	linkid               integer  unsigned null default null,
	pollutantid          smallint unsigned null default null,
	processid            smallint unsigned null default null,
	sourcetypeid         smallint unsigned null default null,
	regclassid           smallint unsigned null default null,
	scc                  char(10) null default null,
	fueltypeid           smallint unsigned null default null,
	modelyearid          smallint unsigned null default null,
	roadtypeid           smallint unsigned null default null,
	avgspeedbinid        smallint null default null,
	temperature          float null default null,
	relhumidity          float null default null,
	rateperdistance      float null default null
) ENGINE=MyISAM DEFAULT CHARSET=latin1 DELAY_KEY_WRITE=1;

-- ***********************************************************************************
-- ***********************************************************************************
-- RatePerVehicle table
-- Includes emissions for processes:  Start exhaust, start crankcase, permeation,
-- liquid leaks, and extended idle
-- ***********************************************************************************
-- ***********************************************************************************
-- No PK nor indexes
CREATE TABLE ratepervehicle (
	movesscenarioid		 varchar(40) not null default '',
	movesrunid           smallint unsigned not null,
	yearid               smallint unsigned null default null,
	monthid              smallint unsigned null default null,
	dayid                smallint unsigned null default null,
	hourid               smallint unsigned null default null,
	zoneid               integer  unsigned null default null,
	pollutantid          smallint unsigned null default null,
	processid            smallint unsigned null default null,
	sourcetypeid         smallint unsigned null default null,
	regclassid           smallint unsigned null default null,
	scc                  char(10) null default null,
	fueltypeid           smallint unsigned null default null,
	modelyearid          smallint unsigned null default null,
	temperature          float null default null,
	relhumidity          float null default null,
	ratepervehicle       float null default null
) ENGINE=MyISAM DEFAULT CHARSET=latin1 DELAY_KEY_WRITE=1;

-- ***********************************************************************************
-- ***********************************************************************************
-- RatePerProfile table
-- Includes emissions from vapor venting process
-- ***********************************************************************************
-- ***********************************************************************************
-- No PK nor indexes
CREATE TABLE rateperprofile (
	movesscenarioid		 varchar(40) not null default '',
	movesrunid           smallint unsigned not null,
	temperatureprofileid bigint null default null,
	yearid               smallint unsigned null default null,
	dayid                smallint unsigned null default null,
	hourid               smallint unsigned null default null,
	pollutantid          smallint unsigned null default null,
	processid            smallint unsigned null default null,
	sourcetypeid         smallint unsigned null default null,
	regclassid           smallint unsigned null default null,
	scc                  char(10) null default null,
	fueltypeid           smallint unsigned null default null,
	modelyearid          smallint unsigned null default null,
	temperature          float null default null,
	relhumidity          float null default null,
	ratepervehicle       float null default null
) ENGINE=MyISAM DEFAULT CHARSET=latin1 DELAY_KEY_WRITE=1;

-- ***********************************************************************************
-- ***********************************************************************************
-- StartsPerVehicle table. Starts per existing vehicle, even if the vehicle did not start.
-- ***********************************************************************************
-- ***********************************************************************************
-- No PK nor indexes
CREATE TABLE startspervehicle (
	movesscenarioid		 varchar(40) not null default '',
	movesrunid           smallint unsigned not null,
	yearid               smallint unsigned null default null,
	monthid              smallint unsigned null default null,
	dayid                smallint unsigned null default null,
	hourid               smallint unsigned null default null,
	zoneid               integer  unsigned null default null,
	sourcetypeid         smallint unsigned null default null,
	regclassid           smallint unsigned null default null,
	scc                  char(10) null default null,
	fueltypeid           smallint unsigned null default null,
	modelyearid          smallint unsigned null default null,
	startspervehicle     float null default null
) ENGINE=MyISAM DEFAULT CHARSET=latin1 DELAY_KEY_WRITE=1;

-- ***********************************************************************************
-- ***********************************************************************************
-- RatePerStart table. Emissions per start.
-- ***********************************************************************************
-- ***********************************************************************************
-- No PK nor indexes
CREATE TABLE rateperstart (
	movesscenarioid		 varchar(40) not null default '',
	movesrunid           smallint unsigned not null,
	yearid               smallint unsigned null default null,
	monthid              smallint unsigned null default null,
	dayid                smallint unsigned null default null,
	hourid               smallint unsigned null default null,
	zoneid               integer  unsigned null default null,
	sourcetypeid         smallint unsigned null default null,
	regclassid           smallint unsigned null default null,
	scc                  char(10) null default null,
	fueltypeid           smallint unsigned null default null,
	modelyearid          smallint unsigned null default null,
	pollutantid          smallint unsigned null default null,
	processid            smallint unsigned null default null,
	temperature          float null default null,
	relhumidity          float null default null,
	rateperstart         float null default null
) ENGINE=MyISAM DEFAULT CHARSET=latin1 DELAY_KEY_WRITE=1;

-- ***********************************************************************************
-- ***********************************************************************************
-- RatePerHour table
-- Includes emissions for the processes: Extended Idle (90), APU (91)
-- ***********************************************************************************
-- ***********************************************************************************
-- No PK nor indexes
CREATE TABLE rateperhour (
	movesscenarioid		 varchar(40) not null default '',
	movesrunid           smallint unsigned not null,
	yearid               smallint unsigned null default null,
	monthid              smallint unsigned null default null,
	dayid                smallint unsigned null default null,
	hourid               smallint unsigned null default null,
	linkid               integer  unsigned null default null,
	pollutantid          smallint unsigned null default null,
	processid            smallint unsigned null default null,
	sourcetypeid         smallint unsigned null default null,
	regclassid           smallint unsigned null default null,
	scc                  char(10) null default null,
	fueltypeid           smallint unsigned null default null,
	modelyearid          smallint unsigned null default null,
	roadtypeid           smallint unsigned null default null,
	temperature          float null default null,
	relhumidity          float null default null,
	rateperhour          float null default null
) ENGINE=MyISAM DEFAULT CHARSET=latin1 DELAY_KEY_WRITE=1;
