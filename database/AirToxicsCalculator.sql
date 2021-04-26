-- author wesley faler
-- author ed campbell
-- version 2015-04-07

-- @algorithm
-- @owner air toxics calculator
-- @calculator

-- section create remote tables for extracted data

drop table if exists atmonthgroup;
create table if not exists atmonthgroup (
	monthid smallint(6) not null default '0',
	monthgroupid smallint(6) not null default '0',
	key (monthid, monthgroupid),
	key (monthgroupid, monthid)
) engine=memory;

-- section useminorhapratio
-- minorhapratio has fuelsubtypeid in the execution database, but the version
-- submitted to the calculator has been joined to the fuel supply and weighted
-- by the fuel formulation market share.

create table minorhapratio (
  processid smallint(6) not null,
  outputpollutantid smallint(6) not null,
  fueltypeid smallint(6) not null default '0',
  modelyearid smallint(6) not null,
  monthid smallint(6) not null,
  atratio double default null,
  key (processid,fueltypeid,modelyearid,monthid)
) engine=myisam default charset=latin1 delay_key_write=1;

create table minorhapratiogo (
  processid smallint(6) not null,
  outputpollutantid smallint(6) not null,
  fuelsubtypeid smallint(6) not null default '0',
  modelyearid smallint(6) not null,
  atratio double default null,
  key (processid,fuelsubtypeid,modelyearid)
) engine=myisam default charset=latin1 delay_key_write=1;
-- end section useminorhapratio

-- section usepahgasratio
create table pahgasratio (
  processid smallint(6) not null,
  outputpollutantid smallint(6) not null,
  fueltypeid smallint(6) not null default '0',
  modelyearid smallint(6) not null default '0',
  atratio double default null,
  key (processid,fueltypeid,modelyearid)
);
-- end section usepahgasratio

-- section usepahparticleratio
create table pahparticleratio (
  processid smallint(6) not null,
  outputpollutantid smallint(6) not null,
  fueltypeid smallint(6) not null default '0',
  modelyearid smallint(6) not null default '0',
  atratio double default null,
  key (processid,fueltypeid,modelyearid)
);
-- end section usepahparticleratio

-- section useatratiogas1
drop table if exists atratiogas1chainedto;

create table if not exists atratiogas1chainedto (
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
	),
	key (
		inputpollutantid,
		inputprocessid,
		inputpolprocessid,
		outputpolprocessid
	)
) engine=memory;

create table if not exists atratio (
  fueltypeid smallint(6) not null,
  fuelformulationid smallint(6) not null,
  polprocessid int not null,
  minmodelyearid smallint(6) not null,
  maxmodelyearid smallint(6) not null,
  ageid smallint(6) not null,
  monthid smallint(6) not null,
  atratio double default null,
  modelyearid smallint(6) not null,
  primary key (fueltypeid,fuelformulationid,polprocessid,minmodelyearid,maxmodelyearid,ageid,monthid),
  key atratio_key1 (fuelformulationid,polprocessid,minmodelyearid),
  key atratio_key2 (polprocessid,fueltypeid,monthid,minmodelyearid,ageid,maxmodelyearid,fuelformulationid),
  key atratio_key3 (polprocessid,fueltypeid,monthid,modelyearid,minmodelyearid,maxmodelyearid,fuelformulationid)
) engine=myisam default charset=latin1 delay_key_write=1;

truncate table atratio;

create table if not exists at1fuelsupply (
  countyid int(11) not null,
  monthid smallint(6) not null,
  fuelformulationid smallint(6) not null,
  marketshare double default null,
  yearid smallint(6) not null,
  fueltypeid smallint(6) not null,
  key (monthid,fueltypeid,fuelformulationid,marketshare)
) engine=memory;

truncate table at1fuelsupply;

create table if not exists at1pollutantprocessmodelyear (
  polprocessid int not null default '0',
  modelyearid smallint(6) not null default '0',
  fuelmygroupid int(11) default null,
  key (polprocessid),
  key (modelyearid),
  key (fuelmygroupid),
  key (polprocessid, modelyearid, fuelmygroupid)
) engine=memory;

truncate table at1pollutantprocessmodelyear;

-- end section useatratiogas1

-- section useatratiogas2
drop table if exists atratiogas2chainedto;

create table if not exists atratiogas2chainedto (
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
	),
	key (
		inputpollutantid,
		inputprocessid,
		inputpolprocessid,
		outputpolprocessid
	)
) engine=memory;

##create.atratiogas2##;
truncate table atratiogas2;

create table if not exists at2fuelsupply (
  fuelregionid int(11) not null,
  monthid smallint(6) not null,
  fuelsubtypeid smallint(6) not null,
  marketshare float default null,
  yearid smallint(6) not null,
  fueltypeid smallint(6) not null,
  key (countyid,yearid,monthid,fueltypeid,fuelsubtypeid),
  key (countyid,yearid,monthid,fuelsubtypeid)
) engine=memory;

truncate table at2fuelsupply;
-- end section useatratiogas2

-- section useatrationongas
drop table if exists atrationongaschainedto;

create table if not exists atrationongaschainedto (
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
	),
	key (
		inputpollutantid,
		inputprocessid,
		inputpolprocessid,
		outputpolprocessid
	)
) engine=memory;

create table atrationongas (
  polprocessid int not null default '0',
  sourcetypeid smallint(6) not null default '0',
  fuelsubtypeid smallint(6) not null default '0',
  modelyearid smallint(6) not null default '0',
  atratio double default null,
  primary key (polprocessid,sourcetypeid,fuelsubtypeid,modelyearid),
  key (polprocessid,sourcetypeid,modelyearid,fuelsubtypeid)
);

truncate table atrationongas;

create table if not exists atnongasfuelsupply (
  countyid int(11) not null,
  monthid smallint(6) not null,
  fuelformulationid smallint(6) not null,
  marketshare float default null,
  yearid smallint(6) not null,
  fueltypeid smallint(6) not null,
  fuelsubtypeid smallint(6) not null,
  key (countyid,yearid,monthid,fueltypeid,fuelsubtypeid),
  key (countyid,yearid,monthid,fuelsubtypeid)
);

truncate table atnongasfuelsupply;
-- end section useatrationongas

-- end section create remote tables for extracted data

-- section extract data

cache select monthid, monthgroupid
into outfile '##atmonthgroup##'
from monthofanyyear
where (##context.monthid## <= 0 or monthid = ##context.monthid##);

-- section useminorhapratio

-- minorhapratio has fuelsubtypeid in the execution database, but the version
-- submitted to the calculator has been joined to the fuel supply and weighted
-- by the fuel formulation market share.

cache select ppa.processid, ppa.pollutantid as outputpollutantid, fueltypeid, modelyearid, monthid, sum(atratio*marketshare) as atratio
into outfile '##minorhapratio##'
from minorhapratio r
inner join pollutantprocessassoc ppa using (polprocessid)
inner join modelyear my on (
	mymap(modelyearid) >= round(modelyeargroupid/10000,0)
	and mymap(modelyearid) <= mod(modelyeargroupid,10000)
	and modelyearid <= ##context.year##
	and modelyearid >= ##context.year## - 30
)
inner join fuelformulation ff using (fuelsubtypeid)
inner join fuelsupply fs on (
	fs.fuelregionid=##context.fuelregionid##
	and fuelyearid=(select fuelyearid from year where yearid=##context.year##)
	and fs.fuelformulationid=ff.fuelformulationid
)
inner join monthofanyyear m on (
	m.monthgroupid=fs.monthgroupid
	and (##context.monthid## <= 0 or monthid = ##context.monthid##)
)
where polprocessid in (##outputminorhapratio##)
group by ppa.polprocessid, fueltypeid, modelyearid, monthid;

cache select ppa.processid, ppa.pollutantid as outputpollutantid, fuelsubtypeid, modelyearid, atratio
into outfile '##minorhapratiogo##'
from minorhapratio r
inner join pollutantprocessassoc ppa using (polprocessid)
inner join modelyear my on (
	mymap(modelyearid) >= round(modelyeargroupid/10000,0)
	and mymap(modelyearid) <= mod(modelyeargroupid,10000)
	and modelyearid <= ##context.year##
	and modelyearid >= ##context.year## - 30
)
where polprocessid in (##outputminorhapratio##);
-- end section useminorhapratio

-- section usepahgasratio
cache select ppa.processid, ppa.pollutantid as outputpollutantid, fueltypeid, modelyearid, atratio
into outfile '##pahgasratio##'
from pahgasratio r
inner join pollutantprocessassoc ppa using (polprocessid)
inner join modelyear my on (
	mymap(modelyearid) >= round(modelyeargroupid/10000,0)
	and mymap(modelyearid) <= mod(modelyeargroupid,10000)
	and modelyearid <= ##context.year##
	and modelyearid >= ##context.year## - 30
)
where polprocessid in (##outputpahgasratio##);
-- end section usepahgasratio

-- section usepahparticleratio
cache select ppa.processid, ppa.pollutantid as outputpollutantid, fueltypeid, modelyearid, atratio
into outfile '##pahparticleratio##'
from pahparticleratio r
inner join pollutantprocessassoc ppa using (polprocessid)
inner join modelyear my on (
	mymap(modelyearid) >= round(modelyeargroupid/10000,0)
	and mymap(modelyearid) <= mod(modelyeargroupid,10000)
	and modelyearid <= ##context.year##
	and modelyearid >= ##context.year## - 30
)
where polprocessid in (##outputpahparticleratio##);
-- end section usepahparticleratio

-- section useatratiogas1
cache select distinct
	atratio.fueltypeid,
	atratio.fuelformulationid,
	atratio.polprocessid,
	myrmap(atratio.minmodelyearid) as minmodelyearid,
	myrmap(atratio.maxmodelyearid) as maxmodelyearid,
	atratio.ageid,
	monthofanyyear.monthid,
	atratio.atratio,
	(##context.year## - atratio.ageid) as modelyearid
into outfile '##atratio##'
from atratio
inner join fuelsupply on (fuelsupply.fuelformulationid = atratio.fuelformulationid)
inner join year on (fuelsupply.fuelyearid = year.fuelyearid)
inner join runspecmonthgroup on (runspecmonthgroup.monthgroupid = fuelsupply.monthgroupid)
inner join monthofanyyear on (monthofanyyear.monthgroupid = runspecmonthgroup.monthgroupid)
where polprocessid in (##outputatratiogas1##)
and fuelregionid = ##context.fuelregionid##
and yearid = ##context.year##
and minmodelyearid <= mymap(##context.year## - atratio.ageid)
and maxmodelyearid >= mymap(##context.year## - atratio.ageid);

cache select *
into outfile '##atratiogas1chainedto##'
from runspecchainedto
where outputpolprocessid in (##outputatratiogas1##);

cache select ##context.iterlocation.countyrecordid## as countyid, monthofanyyear.monthid, fuelsupply.fuelformulationid, fuelsupply.marketshare, year.yearid, fuelsubtype.fueltypeid
into outfile '##at1fuelsupply##'
from fuelsupply
inner join runspecmonthgroup on (fuelsupply.monthgroupid = runspecmonthgroup.monthgroupid)
inner join year on (fuelsupply.fuelyearid = year.fuelyearid)
inner join fuelformulation on (fuelformulation.fuelformulationid = fuelsupply.fuelformulationid)
inner join fuelsubtype on (fuelsubtype.fuelsubtypeid = fuelformulation.fuelsubtypeid)
inner join monthofanyyear on (monthofanyyear.monthgroupid = fuelsupply.monthgroupid)
inner join runspecmonth on (runspecmonth.monthid = monthofanyyear.monthid)
where fuelregionid = ##context.fuelregionid##
and yearid = ##context.year##
and (##context.monthid## <= 0 or runspecmonth.monthid = ##context.monthid##);

cache select polprocessid, modelyearid, fuelmygroupid into outfile '##at1pollutantprocessmodelyear##'
from pollutantprocessmodelyear
where modelyearid <= ##context.year##
and modelyearid >= ##context.year## - 30
and polprocessid in (##inputatratiogas1##);

-- end section useatratiogas1

-- section useatratiogas2
cache select *
into outfile '##atratiogas2##'
from atratiogas2
where polprocessid in (##outputatratiogas2##);

cache select *
into outfile '##atratiogas2chainedto##'
from runspecchainedto
where outputpolprocessid in (##outputatratiogas2##);

cache select ##context.iterlocation.countyrecordid## as countyid, monthofanyyear.monthid, fuelsubtype.fuelsubtypeid, fuelsupply.marketshare, year.yearid, fuelsubtype.fueltypeid
into outfile '##at2fuelsupply##'
from fuelsupply
inner join runspecmonthgroup on (fuelsupply.monthgroupid = runspecmonthgroup.monthgroupid)
inner join year on (fuelsupply.fuelyearid = year.fuelyearid)
inner join fuelformulation on (fuelformulation.fuelformulationid = fuelsupply.fuelformulationid)
inner join fuelsubtype on (fuelsubtype.fuelsubtypeid = fuelformulation.fuelsubtypeid)
inner join monthofanyyear on (monthofanyyear.monthgroupid = fuelsupply.monthgroupid)
inner join runspecmonth on (runspecmonth.monthid = monthofanyyear.monthid)
where fuelregionid = ##context.fuelregionid##
and yearid = ##context.year##
and (##context.monthid## <= 0 or runspecmonth.monthid = ##context.monthid##);
-- end section useatratiogas2

-- section useatrationongas
cache select r.polprocessid, r.sourcetypeid, r.fuelsubtypeid, my.modelyearid, r.atratio
into outfile '##atrationongas##'
from atrationongas r
inner join modelyear my on (
	mymap(modelyearid) >= round(modelyeargroupid/10000,0)
	and mymap(modelyearid) <= mod(modelyeargroupid,10000)
	and modelyearid <= ##context.year##
	and modelyearid >= ##context.year## - 30
)
where polprocessid in (##outputatrationongas##);

cache select *
into outfile '##atrationongaschainedto##'
from runspecchainedto
where outputpolprocessid in (##outputatrationongas##);

cache select ##context.iterlocation.countyrecordid## as countyid, monthofanyyear.monthid, fuelsupply.fuelformulationid, fuelsupply.marketshare, year.yearid, fuelsubtype.fueltypeid, fuelsubtype.fuelsubtypeid
into outfile '##atnongasfuelsupply##'
from fuelsupply
inner join runspecmonthgroup on (fuelsupply.monthgroupid = runspecmonthgroup.monthgroupid)
inner join year on (fuelsupply.fuelyearid = year.fuelyearid)
inner join fuelformulation on (fuelformulation.fuelformulationid = fuelsupply.fuelformulationid)
inner join fuelsubtype on (fuelsubtype.fuelsubtypeid = fuelformulation.fuelsubtypeid)
inner join monthofanyyear on (monthofanyyear.monthgroupid = fuelsupply.monthgroupid)
inner join runspecmonth on (runspecmonth.monthid = monthofanyyear.monthid)
where fuelregionid = ##context.fuelregionid##
and yearid = ##context.year##
and (##context.monthid## <= 0 or runspecmonth.monthid = ##context.monthid##);
-- end section useatrationongas

-- end section extract data

-- section processing

-- @algorithm create airtoxicsmovesworkeroutputtemp table
drop table if exists airtoxicsmovesworkeroutputtemp;
create table if not exists airtoxicsmovesworkeroutputtemp (
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
	regclassid			 smallint unsigned null,
	fueltypeid           smallint unsigned null,
	modelyearid          smallint unsigned null,
	roadtypeid           smallint unsigned null,
	scc                  char(10) null,
	emissionquant        double null,
	emissionrate		 double null,
	key (yearid,monthid,dayid,hourid,stateid,countyid,zoneid,linkid,pollutantid,processid,sourcetypeid,fueltypeid,modelyearid,roadtypeid,scc)
);

create index movesworkeroutput_a3 on movesworkeroutput (
	pollutantid asc,
	processid asc,
	sourcetypeid asc,
	yearid asc,
	monthid asc,
	fueltypeid asc
);

-- section useminorhapratio

-- @algorithm minor hap emissions[outputpollutantid] = voc (87) * atratio
insert into airtoxicsmovesworkeroutputtemp (
	monthid, modelyearid, yearid, fueltypeid, dayid, hourid, stateid, countyid, 
	zoneid, linkid, pollutantid, processid, sourcetypeid, regclassid, roadtypeid, scc, 
	emissionquant, emissionrate)
select
   	mwo.monthid, mwo.modelyearid, mwo.yearid, mwo.fueltypeid, mwo.dayid, mwo.hourid, mwo.stateid, mwo.countyid, 
	mwo.zoneid, mwo.linkid,	r.outputpollutantid, mwo.processid, mwo.sourcetypeid, mwo.regclassid, mwo.roadtypeid, mwo.scc,
	r.atratio*emissionquant, r.atratio*emissionrate
from movesworkeroutput mwo
inner join minorhapratio r on (
	r.processid = mwo.processid
	and mwo.pollutantid = 87
	and r.fueltypeid = mwo.fueltypeid
	and r.modelyearid = mwo.modelyearid
	and r.monthid = mwo.monthid
);

-- end section useminorhapratio

-- section usepahgasratio

-- @algorithm pah gas emissions[outputpollutantid] = voc (87) * atratio
insert into airtoxicsmovesworkeroutputtemp (
	monthid, modelyearid, yearid, fueltypeid, dayid, hourid, stateid, countyid, 
	zoneid, linkid, pollutantid, processid, sourcetypeid, regclassid, roadtypeid, scc, 
	emissionquant, emissionrate)
select
   	mwo.monthid, mwo.modelyearid, mwo.yearid, mwo.fueltypeid, mwo.dayid, mwo.hourid, mwo.stateid, mwo.countyid, 
	mwo.zoneid, mwo.linkid,	r.outputpollutantid, mwo.processid, mwo.sourcetypeid, mwo.regclassid, mwo.roadtypeid, mwo.scc,
	r.atratio*emissionquant, r.atratio*emissionrate
from movesworkeroutput mwo
inner join pahgasratio r on (
	r.processid = mwo.processid
	and mwo.pollutantid = 87
	and r.fueltypeid = mwo.fueltypeid
	and r.modelyearid = mwo.modelyearid
);

-- end section usepahgasratio

-- section usepahparticleratio

-- @algorithm pah particle emissions[outputpollutantid] = organic carbon (111) * atratio
insert into airtoxicsmovesworkeroutputtemp (
	monthid, modelyearid, yearid, fueltypeid, dayid, hourid, stateid, countyid, 
	zoneid, linkid, pollutantid, processid, sourcetypeid, regclassid, roadtypeid, scc, 
	emissionquant, emissionrate)
select
   	mwo.monthid, mwo.modelyearid, mwo.yearid, mwo.fueltypeid, mwo.dayid, mwo.hourid, mwo.stateid, mwo.countyid, 
	mwo.zoneid, mwo.linkid,	r.outputpollutantid, mwo.processid, mwo.sourcetypeid, mwo.regclassid, mwo.roadtypeid, mwo.scc,
	r.atratio*emissionquant, r.atratio*emissionrate
from movesworkeroutput mwo
inner join pahparticleratio r on (
	r.processid = mwo.processid
	and mwo.pollutantid = 111
	and r.fueltypeid = mwo.fueltypeid
	and r.modelyearid = mwo.modelyearid
);

-- end section usepahparticleratio

-- section useatratiogas1

-- @algorithm emissions[outputpollutantid] = emissions[inputpollutantid] * marketshare * atratio
-- @input fuelsupply
-- @input pollutantprocessassoc
insert into airtoxicsmovesworkeroutputtemp (
	monthid, modelyearid, yearid, fueltypeid, dayid, hourid, stateid, countyid, 
	zoneid, linkid, pollutantid, processid, sourcetypeid, regclassid, roadtypeid, scc, 
	emissionquant, emissionrate)
select
   	mwo.monthid, mwo.modelyearid, mwo.yearid, mwo.fueltypeid, mwo.dayid, mwo.hourid, mwo.stateid, mwo.countyid, 
	mwo.zoneid, mwo.linkid,	ct.outputpollutantid, ct.outputprocessid, mwo.sourcetypeid, mwo.regclassid, mwo.roadtypeid, mwo.scc,
	r.atratio*fs.marketshare*emissionquant, r.atratio*fs.marketshare*emissionrate
from movesworkeroutput mwo
inner join atratiogas1chainedto ct on (
	ct.inputpollutantid=mwo.pollutantid
	and ct.inputprocessid=mwo.processid)
inner join atratio r on (
	r.minmodelyearid <= mwo.modelyearid and r.maxmodelyearid >= mwo.modelyearid
	and r.modelyearid = mwo.modelyearid
	and r.fueltypeid=mwo.fueltypeid
	and r.polprocessid=ct.outputpolprocessid
	and r.monthid = mwo.monthid
	)
inner join at1fuelsupply fs on (
	fs.monthid = mwo.monthid
	and fs.fueltypeid = mwo.fueltypeid
	and fs.fuelformulationid = r.fuelformulationid
);

-- end section useatratiogas1

-- section useatratiogas2
create index atratiogas2chainedto_a1 on atratiogas2chainedto (
	inputpollutantid asc,
	inputprocessid asc,
	outputpolprocessid asc
);

-- create index at2fuelsupply_a1 on at2fuelsupply (
-- 	yearid asc,
-- 	monthid asc,
-- 	fuelsubtypeid asc,
-- 	countyid asc,
-- 	fueltypeid asc
-- );

-- insert into airtoxicsmovesworkeroutputtemp (
-- yearid, monthid, dayid, hourid, stateid, countyid, zoneid, linkid, pollutantid, processid,
-- sourcetypeid, regclassid, fueltypeid, modelyearid, roadtypeid, scc, emissionquant, emissionrate)
-- select
--     mwo.yearid, mwo.monthid, mwo.dayid, mwo.hourid, mwo.stateid, mwo.countyid, mwo.zoneid, mwo.linkid,
-- 	ct.outputpollutantid,
-- 	ct.outputprocessid,
-- 	mwo.sourcetypeid, mwo.regclassid, mwo.fueltypeid, mwo.modelyearid, mwo.roadtypeid, mwo.scc,
-- 	r.atratio*fs.marketshare*emissionquant, r.atratio*fs.marketshare*emissionrate
-- from movesworkeroutput mwo
-- inner join atratiogas2chainedto ct on (
-- 	ct.inputpollutantid=mwo.pollutantid
-- 	and ct.inputprocessid=mwo.processid)
-- inner join atratiogas2 r on (
-- 	r.polprocessid=ct.outputpolprocessid
-- 	and r.sourcetypeid=mwo.sourcetypeid)
-- inner join at2fuelsupply fs on (
-- 	fs.yearid = mwo.yearid
-- 	and fs.monthid = mwo.monthid
-- 	and fs.fuelsubtypeid = r.fuelsubtypeid
-- 	and fs.countyid = # # context.iterlocation . countyrecordid # #
-- 	and fs.fueltypeid = mwo.fueltypeid);

alter table atratiogas2chainedto add key (inputpollutantid, inputprocessid, outputpolprocessid, outputpollutantid, outputprocessid);
analyze table atratiogas2chainedto;

alter table atratiogas2 add key (polprocessid, sourcetypeid, fuelsubtypeid, atratio);
alter table atratiogas2 add key (sourcetypeid, fuelsubtypeid, polprocessid, atratio);
alter table atratiogas2 add key (fuelsubtypeid, polprocessid, sourcetypeid, atratio);
alter table atratiogas2 add key (polprocessid, fuelsubtypeid, sourcetypeid, atratio);
alter table atratiogas2 add key (sourcetypeid, polprocessid, fuelsubtypeid, atratio);
alter table atratiogas2 add key (fuelsubtypeid, sourcetypeid, polprocessid, atratio);
analyze table atratiogas2;

alter table at2fuelsupply drop key countyid;
alter table at2fuelsupply drop key countyid_2;
alter table at2fuelsupply add key (yearid,monthid,countyid,fueltypeid,fuelsubtypeid, marketshare);
analyze table at2fuelsupply;

drop table if exists at2fuelsupplyratiogas2;

-- @algorithm marketshareatratio[outputpollutantid] = marketshare * atratio
-- @input fuelsupply
-- @input pollutantprocessassoc
create table at2fuelsupplyratiogas2
select ct.inputpollutantid, ct.inputprocessid, r.polprocessid, r.sourcetypeid,
	fs.yearid, fs.monthid, fs.fueltypeid, fs.marketshare*r.atratio as marketshareatratio,
	ct.outputpollutantid,
	ct.outputprocessid
from atratiogas2 r
inner join at2fuelsupply fs on (
	fs.countyid = ##context.iterlocation.countyrecordid##
	and fs.fuelsubtypeid = r.fuelsubtypeid)
inner join atratiogas2chainedto ct on (
	r.polprocessid=ct.outputpolprocessid);

alter table at2fuelsupplyratiogas2 add key (inputpollutantid, inputprocessid, fueltypeid, sourcetypeid, yearid, monthid, outputpollutantid, outputprocessid, marketshareatratio);
analyze table at2fuelsupplyratiogas2;

-- @algorithm emissions[outputpollutantid] = emissions[inputpollutantid] * marketshareatratio[outputpollutantid]
insert into airtoxicsmovesworkeroutputtemp (
	yearid, monthid, dayid, hourid, stateid, countyid, zoneid, linkid, pollutantid, processid,
	sourcetypeid, regclassid, fueltypeid, modelyearid, roadtypeid, scc, emissionquant, emissionrate)
select
    mwo.yearid, mwo.monthid, mwo.dayid, mwo.hourid, mwo.stateid, mwo.countyid, mwo.zoneid, mwo.linkid,
	r.outputpollutantid,
	r.outputprocessid,
	mwo.sourcetypeid, mwo.regclassid, mwo.fueltypeid, mwo.modelyearid, mwo.roadtypeid, mwo.scc,
	r.marketshareatratio*emissionquant, r.marketshareatratio*emissionrate
from movesworkeroutput mwo
inner join at2fuelsupplyratiogas2 r on (
	r.inputpollutantid=mwo.pollutantid
	and r.inputprocessid=mwo.processid
	and r.sourcetypeid=mwo.sourcetypeid
	and r.yearid = mwo.yearid
	and r.monthid = mwo.monthid
	and r.fueltypeid = mwo.fueltypeid);
-- end section useatratiogas2

-- section useatrationongas
create index atrationongaschainedto_a1 on atrationongaschainedto (
	inputpollutantid asc,
	inputprocessid asc,
	outputpolprocessid asc
);

create index atnongasfuelsupply_a1 on atnongasfuelsupply (
	yearid asc,
	monthid asc,
	fuelsubtypeid asc,
	countyid asc, 
	fueltypeid asc 
);

-- @algorithm emissions[outputpollutantid] = emissions[inputpollutantid] * marketshare * atratio
-- @input fuelsupply
-- @input pollutantprocessassoc
insert into airtoxicsmovesworkeroutputtemp (
	yearid, monthid, dayid, hourid, stateid, countyid, zoneid, linkid, pollutantid, processid,
	sourcetypeid, regclassid, fueltypeid, modelyearid, roadtypeid, scc, emissionquant, emissionrate)
select
    mwo.yearid, mwo.monthid, mwo.dayid, mwo.hourid, mwo.stateid, mwo.countyid, mwo.zoneid, mwo.linkid,
	ct.outputpollutantid,
	ct.outputprocessid,
	mwo.sourcetypeid, mwo.regclassid, mwo.fueltypeid, mwo.modelyearid, mwo.roadtypeid, mwo.scc,
	r.atratio*fs.marketshare*emissionquant, r.atratio*fs.marketshare*emissionrate
from movesworkeroutput mwo
inner join atrationongaschainedto ct on (
	ct.inputpollutantid=mwo.pollutantid
	and ct.inputprocessid=mwo.processid)
inner join atrationongas r on (
	r.polprocessid=ct.outputpolprocessid
	and r.sourcetypeid=mwo.sourcetypeid
	and r.modelyearid=mwo.modelyearid)
inner join atnongasfuelsupply fs on (
	fs.yearid = mwo.yearid
	and fs.monthid = mwo.monthid
	and fs.fuelsubtypeid = r.fuelsubtypeid
	and fs.countyid = ##context.iterlocation.countyrecordid##
	and fs.fueltypeid = mwo.fueltypeid);

-- end section useatrationongas

-- insert into movesworkeroutput (
-- yearid,monthid,dayid,hourid,stateid,countyid,zoneid,linkid,pollutantid,processid,
-- sourcetypeid,regclassid,fueltypeid,modelyearid,roadtypeid,scc,emissionquant,emissionrate)
-- select
-- yearid,monthid,dayid,hourid,stateid,countyid,zoneid,linkid,pollutantid,processid,
-- sourcetypeid,regclassid,fueltypeid,modelyearid,roadtypeid,scc,
-- sum(emissionquant) as emissionquant, sum(emissionrate) as emissionrate
-- from airtoxicsmovesworkeroutputtemp
-- group by yearid,monthid,dayid,hourid,stateid,countyid,zoneid,linkid,pollutantid,processid,sourcetypeid,regclassid,fueltypeid,modelyearid,roadtypeid,scc
-- order by null

-- @algorithm add emisions in airtoxicsmovesworkeroutputtemp to movesworkeroutput
insert into movesworkeroutput (
	yearid,monthid,dayid,hourid,stateid,countyid,zoneid,linkid,pollutantid,processid,
	sourcetypeid,regclassid,fueltypeid,modelyearid,roadtypeid,scc,emissionquant,emissionrate)
select
	yearid,monthid,dayid,hourid,stateid,countyid,zoneid,linkid,pollutantid,processid,
	sourcetypeid,regclassid,fueltypeid,modelyearid,roadtypeid,scc,
	emissionquant,emissionrate
from airtoxicsmovesworkeroutputtemp;

alter table movesworkeroutput drop index movesworkeroutput_a3;

-- end section processing

-- section cleanup
drop table if exists airtoxicsmovesworkeroutputtemp;
drop table if exists atratiogas1chainedto;
drop table if exists atratiogas2chainedto;
drop table if exists atrationongaschainedto;
drop table if exists atmonthgroup;
drop table if exists at2fuelsupplyratiogas2;
-- end section cleanup
