-- author wesley faler
-- version 2017-09-28

create table if not exists drivingidlefraction (
	hourdayid smallint not null,
	yearid smallint not null,
	roadtypeid smallint not null,
	sourcetypeid smallint not null,
	drivingidlefraction double not null,
	primary key (hourdayid, roadtypeid, sourcetypeid, yearid)
);

-- *************************************************************************************
-- the following tables are used in joins for calculations to filter the number of items
-- used in the calculations.
-- *************************************************************************************

drop table if exists runspecsourcetype;

create table if not exists runspecsourcetype (
	sourcetypeid smallint not null,
	unique index ndxsourcetypeid (
	sourcetypeid asc)
);

truncate table runspecsourcetype;

drop table if exists runspecroadtype;

create table if not exists runspecroadtype (
	roadtypeid smallint not null,
	unique index ndxroadtypeid (
	roadtypeid asc)
);

truncate table runspecroadtype;

drop table if exists runspecmonth;

create table if not exists runspecmonth (
	monthid smallint not null,
	unique index ndxmonthid (
	monthid asc)
);

truncate table runspecmonth;

drop table if exists runspecday;

create table if not exists runspecday (
	dayid smallint not null,
	unique index ndxdayid (
	dayid asc)
);

truncate table runspecday;

drop table if exists runspechour;

create table if not exists runspechour (
	hourid smallint not null,
	unique index ndxhourid (
	hourid asc)
);

truncate table runspechour;

drop table if exists runspecmonthgroup;

create table if not exists runspecmonthgroup (
	monthgroupid smallint not null,
	unique index ndxmonthgroupid (
	monthgroupid asc)
);

truncate table runspecmonthgroup;

drop table if exists runspecyear;

create table if not exists runspecyear (
	yearid smallint not null,
	unique index ndxhourid (
	yearid asc)
);

truncate table runspecyear;

drop table if exists runspecmodelyearage;

create table if not exists runspecmodelyearage (
	yearid smallint not null,
	modelyearid smallint not null,
	ageid smallint not null,
	
	primary key (modelyearid, ageid, yearid),
	key (yearid, modelyearid, ageid),
	key (ageid, modelyearid)
);

truncate table runspecmodelyearage;

drop table if exists runspecmodelyearagegroup;

create table if not exists runspecmodelyearagegroup (
	yearid smallint(6) not null,
	modelyearid smallint(6) not null,
	agegroupid smallint(6) not null,
	primary key (modelyearid,agegroupid,yearid),
	key yearid (yearid,modelyearid,agegroupid),
	key yearid2 (yearid,agegroupid,modelyearid),
	key ageid (agegroupid,modelyearid,yearid)
);

truncate table runspecmodelyearagegroup;

drop table if exists runspecmodelyear;

create table if not exists runspecmodelyear (
	modelyearid smallint not null primary key
);

truncate table runspecmodelyear;

drop table if exists runspecsourcefueltype;

create table if not exists runspecsourcefueltype (
	sourcetypeid smallint not null,
	fueltypeid tinyint not null,
	unique index ndxsourcefueltypeid (
	sourcetypeid, fueltypeid),
	unique key (fueltypeid, sourcetypeid)
);

truncate table runspecsourcefueltype;

drop table if exists runspechourday;

create table if not exists runspechourday (
	hourdayid smallint not null,
	unique index ndxhourdayid (
	hourdayid asc)
);

truncate table runspechourday;

drop table if exists runspecstate;

create table if not exists runspecstate (
	stateid smallint not null,
	unique index ndxstate (
	stateid asc)
);

truncate table runspecstate;

drop table if exists runspeccounty;

create table if not exists runspeccounty (
	countyid integer not null,
	unique index ndxcounty (
	countyid asc)
);

truncate table runspeccounty;

drop table if exists runspecfuelregion;

create table if not exists runspecfuelregion (
	fuelregionid integer not null,
	unique index ndxfuelregion (
	fuelregionid asc)
);

truncate table runspecfuelregion;

drop table if exists runspeczone;

create table if not exists runspeczone (
	zoneid integer not null,
	unique index ndxzone (
	zoneid asc)
);

truncate table runspeczone;

drop table if exists runspeclink;

create table if not exists runspeclink (
	linkid integer not null,
	unique index ndxlink (
	linkid asc)
);

truncate table runspeclink;

drop table if exists runspecpollutant;

create table if not exists runspecpollutant (
	pollutantid smallint not null,
	unique index ndxpollutant (
	pollutantid asc)
);

truncate table runspecpollutant;

drop table if exists runspecprocess;

create table if not exists runspecprocess (
	processid smallint not null,
	unique index ndxprocess (
	processid asc)
);

truncate table runspecprocess;

drop table if exists runspecpollutantprocess;

create table if not exists runspecpollutantprocess (
	polprocessid int not null,
	unique index ndxpolprocess (
	polprocessid asc)
);

truncate table runspecpollutantprocess;

drop table if exists runspecchainedto;

create table if not exists runspecchainedto (
	outputpolprocessid int not null,
	outputpollutantid smallint not null,
	outputprocessid smallint not null,
	inputpolprocessid int not null,
	inputpollutantid smallint not null,
	inputprocessid smallint not null,
	index inputchainedtoindex (
		inputpollutantid,
		inputprocessid
	),
	index inputchainedtoprocessindex (
		inputprocessid
	),
	index outputchainedtopolprocessindex (
		outputpolprocessid
	),
	index inputoutputchainedtoindex (
		outputpolprocessid,
		inputpolprocessid
	),
	index inputoutputchainedtoindex2 (
		inputpolprocessid,
		outputpolprocessid
	)
);

truncate table runspecchainedto;

drop table if exists runspecsectorfueltype;

create table if not exists runspecsectorfueltype (
	sectorid smallint not null,
	fueltypeid tinyint not null,
	unique index ndxsectorfueltypeid (
	sectorid, fueltypeid),
	unique key (fueltypeid, sectorid)
);

truncate table runspecsectorfueltype;

drop table if exists runspecsector;

create table if not exists runspecsector (
	sectorid smallint not null,
	unique index ndxsectorid (
	sectorid asc)
);

truncate table runspecsector;

drop table if exists runspecnonroadmodelyearage;

create table if not exists runspecnonroadmodelyearage (
	yearid smallint not null,
	modelyearid smallint not null,
	ageid smallint not null,
	
	primary key (modelyearid, ageid, yearid),
	key (yearid, modelyearid, ageid),
	key (ageid, modelyearid)
);

truncate table runspecnonroadmodelyearage;

drop table if exists runspecnonroadmodelyear;

create table if not exists runspecnonroadmodelyear (
	modelyearid smallint not null primary key
);

truncate table runspecnonroadmodelyear;

drop table if exists runspecnonroadchainedto;

create table if not exists runspecnonroadchainedto (
	outputpolprocessid int not null,
	outputpollutantid smallint not null,
	outputprocessid smallint not null,
	inputpolprocessid int not null,
	inputpollutantid smallint not null,
	inputprocessid smallint not null,
	index inputchainedtoindex (
		inputpollutantid,
		inputprocessid
	),
	index inputchainedtoprocessindex (
		inputprocessid
	),
	index outputchainedtopolprocessindex (
		outputpolprocessid
	),
	index inputoutputchainedtoindex (
		outputpolprocessid,
		inputpolprocessid
	),
	index inputoutputchainedtoindex2 (
		inputpolprocessid,
		outputpolprocessid
	)
);

truncate table runspecnonroadchainedto;

-- example: if a 1995 euro car should be treated as a 1991 us car,
-- then the 1991 pollutantprocessmodelyear should be used for the
-- the 1995 modelyear. so, use reverse model year mapping so the
-- modelyearid in pollutantprocessmappedmodelyear maps older
-- model year groups to a newer model year.

drop table if exists pollutantprocessmappedmodelyear;

create table if not exists pollutantprocessmappedmodelyear (
    polprocessid int not null ,
    modelyearid smallint not null ,
    modelyeargroupid int not null ,
    fuelmygroupid integer null,
    immodelyeargroupid integer null,
    key (modelyearid, polprocessid),
    key (polprocessid),
    key (modelyearid),
    primary key (polprocessid, modelyearid)
);

truncate table pollutantprocessmappedmodelyear;
