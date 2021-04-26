-- create the movesworker database and schema.
-- author wesley faler
-- version 2015-12-01

drop table if exists movesoutput;
drop table if exists movesworkeroutput;
drop table if exists movesworkeractivityoutput;

-- ***********************************************************************************
-- ***********************************************************************************
-- movesoutput table.  stores one row for each combination
-- of dimension field values, which includes pollutant. 
--
-- note that dimension fields will never be null but they
-- may hold a default value that indicates an "all" selection.
-- ***********************************************************************************
-- ***********************************************************************************
create table if not exists movesworkeroutput (
	movesrunid           smallint unsigned not null default 0,
	iterationid			smallint unsigned default 1,
	
	yearid               smallint unsigned null,
	monthid              smallint unsigned null,
	dayid                smallint unsigned null,
	hourid               smallint unsigned null,

	-- ******************************************************
	-- stateid, locationid, zoneid, and linkid can all be default
	-- in the case where the user selected "nation" as the 
	-- geographic granularity for the output.
	-- linkid and/or zoneid will be default otherwise if "county" 
	-- level granularity was selected depending upon scale.
	-- locationid will be default otherwise if "state" level
	-- granularity was selected.
	-- ******************************************************
	stateid              smallint unsigned null,
	countyid             integer unsigned null,
	zoneid               integer unsigned null,
	linkid               integer unsigned null,
	
	pollutantid          smallint unsigned null,
	processid            smallint unsigned null,
	
	sourcetypeid         smallint unsigned null,
	regclassid           smallint unsigned null,
	fueltypeid           smallint unsigned null,
	fuelsubtypeid        smallint unsigned null,
	modelyearid          smallint unsigned null,

	-- ******************************************************
	-- roadtypeid is not redundant with linkid in the cases where
	-- the user wants road type as a dimension but does not want
	-- geographic detail to the link/zone (or perhaps even to
	-- the county) level.
	-- ******************************************************
	roadtypeid           smallint unsigned null,

	-- ******************************************************
	-- scc holds both onroad and offroad scc codes and may be
	-- all 0's (zeroes) to represent "all" scc codes at once.
	-- ******************************************************
	scc                  char(10) null,

	-- ******************************************************
	-- offroad keys
	-- ******************************************************
	engtechid            smallint unsigned null default null,
	sectorid             smallint unsigned null default null,
	hpid                 smallint unsigned null default null,

	-- ******************************************************
	-- the emission columns are the actual values produced,
	-- not dimensions to the data.  these will be null if the
	-- user chose not to generate them.
	-- ******************************************************

	-- pollutant [mass,energy,moles,etc] in the time period and region.
	-- reflects mixture of i/m and non-i/m vehicles.
	emissionquant        float null,

	-- pollutant [mass,energy,moles,etc] per activity unit such
	-- as distance, start, and idle hour.
	-- reflects mixture of i/m and non-i/m vehicles.
	emissionrate         float null
);

truncate table movesworkeroutput;

create table if not exists movesworkeractivityoutput (
	movesrunid           smallint unsigned not null default 0,
	iterationid			smallint unsigned default 1,
	
	yearid               smallint unsigned null,
	monthid              smallint unsigned null,
	dayid                smallint unsigned null,
	hourid               smallint unsigned null,

	-- ******************************************************
	-- stateid, locationid, zoneid, and linkid can all be default
	-- in the case where the user selected "nation" as the 
	-- geographic granularity for the output.
	-- linkid and/or zoneid will be default otherwise if "county" 
	-- level granularity was selected depending upon scale.
	-- locationid will be default otherwise if "state" level
	-- granularity was selected.
	-- ******************************************************
	stateid              smallint unsigned null,
	countyid             integer unsigned null,
	zoneid               integer unsigned null,
	linkid               integer unsigned null,
	
	sourcetypeid         smallint unsigned null,
	regclassid           smallint unsigned null,
	fueltypeid           smallint unsigned null,
	fuelsubtypeid        smallint unsigned null,
	modelyearid          smallint unsigned null,

	-- ******************************************************
	-- roadtypeid is not redundant with linkid in the cases where
	-- the user wants road type as a dimension but does not want
	-- geographic detail to the link/zone (or perhaps even to
	-- the county) level.
	-- ******************************************************
	roadtypeid           smallint unsigned null,

	-- ******************************************************
	-- scc holds both onroad and offroad scc codes and may be
	-- all 0's (zeroes) to represent "all" scc codes at once.
	-- ******************************************************
	scc                  char(10) null,

	-- ******************************************************
	-- offroad keys
	-- ******************************************************
	engtechid            smallint unsigned null default null,
	sectorid             smallint unsigned null default null,
	hpid                 smallint unsigned null default null,

	activitytypeid       smallint not null,
	activity             float null default null
);

truncate table movesworkeractivityoutput;
