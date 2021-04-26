/*
   version 2013-10-23
   author wesley faler
*/
drop table if exists baserateoutput;
drop table if exists baserateunits;

create table if not exists baserateunits (
	movesrunid           smallint unsigned not null,

	pollutantid          smallint unsigned null default null,
	processid            smallint unsigned null default null,

	meanbaserateunitsnumerator varchar(50) null default '',
	meanbaserateunitsdenominator varchar(50) null default '',
	emissionbaserateunitsnumerator varchar(50) null default '',
	emissionbaserateunitsdenominator varchar(50) null default ''
) ENGINE=MyISAM DEFAULT CHARSET=latin1 DELAY_KEY_WRITE=1;

CREATE TABLE if not exists baserateoutput (
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

	meanbaserate		 float null,
	emissionrate		 float null
) ENGINE=MyISAM DEFAULT CHARSET=latin1 DELAY_KEY_WRITE=1;
