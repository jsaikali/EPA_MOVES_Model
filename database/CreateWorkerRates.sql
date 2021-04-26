/*
   version 2013-10-23
   author wesley faler
*/
drop table if exists baserateoutput;

create table if not exists baserateoutput (
	movesrunid           smallint unsigned not null,
	iterationid          smallint unsigned null default 1,

	zoneid				 integer not null default '0',
	linkid				 integer not null default '0',
	sourcetypeid         smallint not null default '0',
    scc                  char(10) not null default '',
	roadtypeid           smallint not null default '0',
	avgspeedbinid        smallint not null default '0',
	monthid              smallint not null default '0',
	hourdayid            smallint not null default '0',
	pollutantid          smallint unsigned null default null,
	processid            smallint unsigned null default null,
	modelyearid			 smallint not null default '0',
	yearid               smallint not null,
	fueltypeid			 smallint not null default '0',
	regclassid			 smallint not null default '0',

	-- pollutant [mass,energy,moles,etc] in the time period and region.
	-- reflects mixture of i/m and non-i/m vehicles.
	meanbaserate		 float null,

	-- pollutant [mass,energy,moles,etc] per activity unit such
	-- as distance, start, and idle hour.
	-- reflects mixture of i/m and non-i/m vehicles.
	emissionrate		 float null
);
