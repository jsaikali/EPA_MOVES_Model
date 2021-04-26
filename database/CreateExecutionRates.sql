-- Author Wesley Faler
-- Version 2016-03-14

drop table if exists ratesopmodedistribution;

CREATE TABLE IF NOT EXISTS ratesopmodedistribution (
	sourcetypeid         smallint not null,
	roadtypeid           smallint not null,
	avgspeedbinid        smallint not null default '0',
	hourdayid            smallint not null default '0',
	polprocessid         int not null,
	opmodeid             smallint not null,
	opmodefraction       float null,
	opmodefractioncv     float null,
	avgbinspeed			 float null,
	avgspeedfraction 	 float not null default '0',

	PRIMARY KEY (sourcetypeid, polprocessid, roadtypeid, hourdayid, opmodeid, avgspeedbinid)
);

-- Go-based	P RIMARY KEY (sourceTypeID, polProcessID, roadTypeID, hourDayID, opModeID, avgSpeedBinID)

-- Keys before Go-based speedup of SourceUseTypePhysics:
-- 	P RIMARY KEY (sourceTypeID, roadTypeID, avgSpeedBinID, hourDayID, polProcessID, opModeID)
-- 	K EY (sourceTypeID)
-- 	K EY (roadTypeID)
-- 	K EY (avgSpeedBinID)
-- 	K EY (hourDayID)
-- 	K EY (polProcessID)
-- 	K EY (opModeID)

TRUNCATE TABLE ratesopmodedistribution;

drop table if exists sbweightedemissionratebyage;

CREATE TABLE IF NOT EXISTS sbweightedemissionratebyage (
	sourcetypeid		smallint not null,
	polprocessid		int not null,
	opmodeid			smallint not null,
	modelyearid			smallint not null,
	fueltypeid			smallint not null,
	agegroupid			smallint not null,
	regclassid			smallint not null,

	meanbaserate		float null,
	meanbaserateim		float null,
	meanbaserateacadj	float null,
	meanbaserateimacadj	float null,
	sumsbd				double null,
	sumsbdraw			double null,
	unique key (sourcetypeid, polprocessid, opmodeid, modelyearid, fueltypeid, agegroupid, regclassid)
);

TRUNCATE TABLE sbweightedemissionratebyage;

drop table if exists sbweightedemissionrate;

CREATE TABLE IF NOT EXISTS sbweightedemissionrate (
	sourcetypeid		smallint not null,
	polprocessid		int not null,
	opmodeid			smallint not null,
	modelyearid			smallint not null,
	fueltypeid			smallint not null,
	regclassid			smallint not null,

	meanbaserate		float null,
	meanbaserateim		float null,
	meanbaserateacadj	float null,
	meanbaserateimacadj	float null,
	sumsbd				double null,
	sumsbdraw			double null,
	unique key (sourcetypeid, polprocessid, opmodeid, modelyearid, fueltypeid, regclassid)
);

TRUNCATE TABLE sbweightedemissionrate;

drop table if exists sbweighteddistancerate;

CREATE TABLE IF NOT EXISTS sbweighteddistancerate (
	sourcetypeid		smallint not null,
	polprocessid		int not null,
	modelyearid			smallint not null,
	fueltypeid			smallint not null,
	regclassid			smallint not null,
	avgspeedbinid 		smallint not null,

	meanbaserate		float null,
	meanbaserateim		float null,
	meanbaserateacadj	float null,
	meanbaserateimacadj	float null,
	sumsbd				double null,
	sumsbdraw			double null,
	primary key (sourcetypeid, polprocessid, modelyearid, fueltypeid, regclassid, avgspeedbinid)
);

TRUNCATE TABLE sbweighteddistancerate;

drop table if exists distanceemissionrate;

create table if not exists distanceemissionrate (
	polprocessid int not null,
	fueltypeid smallint not null,
	sourcetypeid smallint not null,
	modelyearid smallint not null,
	avgspeedbinid smallint not null,
	ratepermile double not null,
	ratepersho double not null,
	primary key (sourcetypeid, polprocessid, modelyearid, fueltypeid, avgspeedbinid)
);

--	regclassid smallint not null,
--	primary key (sourcetypeid, polprocessid, modelyearid, fueltypeid, regclassid, avgspeedbinid)

truncate table distanceemissionrate;

drop table if exists baseratebyage;

create table if not exists baseratebyage (
	sourcetypeid         smallint not null,
	roadtypeid           smallint not null,
	avgspeedbinid        smallint not null default '0',
	hourdayid            smallint not null default '0',
	polprocessid         int not null,
	pollutantid          smallint unsigned null default null,
	processid            smallint unsigned null default null,
	modelyearid			 smallint not null,
	fueltypeid			 smallint not null,
	agegroupid			 smallint not null,
	regclassid			 smallint not null,
	opmodeid			 smallint not null,

	meanbaserate		 float null,
	meanbaserateim		 float null,
	emissionrate		 float null,
	emissionrateim		 float null,

	meanbaserateacadj	 float null,
	meanbaserateimacadj	 float null,
	emissionrateacadj    float null,
	emissionrateimacadj  float null,

	opmodefraction       float null,
	opmodefractionrate   float null,
	primary key (sourcetypeid, roadtypeid, avgspeedbinid, hourdayid, polprocessid, modelyearid, fueltypeid, agegroupid, regclassid, opmodeid)
);

truncate table baseratebyage;

drop table if exists baserate;

create table if not exists baserate (
	sourcetypeid         smallint not null,
	roadtypeid           smallint not null,
	avgspeedbinid        smallint not null default '0',
	hourdayid            smallint not null default '0',
	polprocessid         int not null,
	pollutantid          smallint unsigned null default null,
	processid            smallint unsigned null default null,
	modelyearid			 smallint not null,
	fueltypeid			 smallint not null,
	regclassid			 smallint not null,
	opmodeid			 smallint not null,

	meanbaserate		 float null,
	meanbaserateim		 float null,
	emissionrate		 float null,
	emissionrateim		 float null,

	meanbaserateacadj	 float null,
	meanbaserateimacadj	 float null,
	emissionrateacadj    float null,
	emissionrateimacadj  float null,

	opmodefraction       float null,
	opmodefractionrate   float null,
	primary key (sourcetypeid, roadtypeid, avgspeedbinid, hourdayid, polprocessid, modelyearid, fueltypeid, regclassid, opmodeid)
);

truncate table baserate;
