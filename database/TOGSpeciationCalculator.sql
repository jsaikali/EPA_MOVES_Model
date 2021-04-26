-- tog speciation calculator
-- version 2015-07-15
-- author wes faler

-- @algorithm
-- @owner tog speciation calculator

-- section create remote tables for extracted data

##create.integratedspeciesset##;
truncate table integratedspeciesset;

##create.lumpedspeciesname##;
truncate table lumpedspeciesname;

##create.togspeciation##;
truncate table togspeciation;

##create.togspeciationprofile##;
truncate table togspeciationprofile;

drop table if exists togspeciationcountyyear;
create table if not exists togspeciationcountyyear (
	mechanismid smallint not null default 0,
	integratedspeciessetid smallint not null default 0,
	countyid int not null default 0,
	monthid smallint not null default 0,
	yearid smallint not null default 0,
	inprocessid smallint not null default 0,
	inpollutantid smallint not null default 0,
	fueltypeid smallint not null default 0,
	minmodelyearid smallint not null default 0,
	maxmodelyearid smallint not null default 0,
	regclassid smallint not null default 0,
	outpollutantid smallint not null default 0,
	factor double not null default 0,
	primary key (mechanismid, integratedspeciessetid,
		countyid, monthid, yearid, inprocessid, inpollutantid,
		fueltypeid, minmodelyearid, maxmodelyearid, regclassid, outpollutantid)
);
truncate table togspeciationcountyyear;

-- end section create remote tables for extracted data

-- section extract data

cache select integratedspeciesset.*
into outfile '##integratedspeciesset##'
from integratedspeciesset
where mechanismid in (##mechanismids##);

cache select lumpedspeciesname.*
into outfile '##lumpedspeciesname##'
from lumpedspeciesname;

cache select togspeciation.*
into outfile '##togspeciation##'
from togspeciation
where processid in (##context.allcurrentprocesses##);

cache select distinct tsp.*
into outfile '##togspeciationprofile##'
from togspeciationprofile tsp
inner join togspeciation ts on (
	ts.togspeciationprofileid = tsp.togspeciationprofileid
 	and ts.processid in (##context.allcurrentprocesses##))
where mechanismid in (##mechanismids##)
and (integratedspeciessetid = 0 or integratedspeciessetid in (
	select distinct integratedspeciessetid
	from integratedspeciesset
	where mechanismid in (##mechanismids##)));

cache select mechanismid, integratedspeciessetid,
		##context.iterlocation.countyrecordid## as countyid, monthid, yearid, 
		ts.processid as inprocessid, tsp.pollutantid as inpollutantid,
		fst.fueltypeid,
		ts.minmodelyearid, ts.maxmodelyearid,
		ts.regclassid,
		1000+(tsp.mechanismid-1)*500+lsn.lumpedspeciesid as outpollutantid,
		sum(fs.marketshare*tsp.togspeciationmassfraction/tsp.togspeciationdivisor) as factor
into outfile '##togspeciationcountyyear##'
from togspeciation ts
inner join togspeciationprofile tsp on (tsp.togspeciationprofileid=ts.togspeciationprofileid)
inner join lumpedspeciesname lsn on (lsn.lumpedspeciesname=tsp.lumpedspeciesname)
inner join fuelformulation ff on (ff.fuelsubtypeid=ts.fuelsubtypeid)
inner join fuelsubtype fst on (fst.fuelsubtypeid=ff.fuelsubtypeid)
inner join fuelsupply fs on (fs.fuelformulationid=ff.fuelformulationid)
inner join monthofanyyear may on (may.monthgroupid=fs.monthgroupid)
inner join year y on (y.fuelyearid=fs.fuelyearid)
where tsp.mechanismid in (##mechanismids##)
and y.yearid=##context.year##
and fs.fuelregionid=##context.fuelregionid##
and ts.minmodelyearid <= ##context.year##
and ts.maxmodelyearid >= ##context.year##-30
and ts.processid in (##context.allcurrentprocesses##)
group by
	mechanismid, integratedspeciessetid,
	monthid, yearid, 
	ts.processid, tsp.pollutantid,
	fst.fueltypeid, ts.minmodelyearid, ts.maxmodelyearid, ts.regclassid,
	lsn.lumpedspeciesid;

-- end section extract data

-- section processing

-- @algorithm
drop table if exists togworkeroutput;
create table if not exists togworkeroutput (
	mechanismid		   smallint not null default 0,
	integratedspeciessetid smallint not null default 0,
	movesrunid           smallint unsigned not null default 0,
	iterationid			 smallint unsigned default 1,
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
	engtechid			 smallint unsigned null,
	sectorid 			 smallint unsigned null,
	hpid 				 smallint unsigned null,
	emissionquant        double null,
	emissionrate		 double null
);
truncate table togworkeroutput;

-- @algorithm add nmog (80) to the set of chained input pollutants.
insert ignore into integratedspeciesset (mechanismid, integratedspeciessetid, pollutantid, useissyn)
select distinct mechanismid, integratedspeciessetid, 80 as pollutantid, 'Y' as useissyn
from integratedspeciesset
where pollutantid <> 80;

-- @algorithm index integratedspeciesset by pollutantid to increase speed.
alter table integratedspeciesset add key iss_pollutantid (pollutantid);

-- @algorithm find the subset of records that are actually needed.
-- these include nmog (80) and anything listed in integratedspeciesset.
insert into togworkeroutput (
	mechanismid,integratedspeciessetid,
	movesrunid,iterationid,
	yearid,monthid,dayid,hourid,
	stateid,countyid,zoneid,linkid,
	pollutantid,processid,
	sourcetypeid,regclassid,fueltypeid,modelyearid,
	roadtypeid,scc,
	engtechid,sectorid,hpid,
	emissionquant,emissionrate)
select 0 as mechanismid, 0 as integratedspeciessetid,
	movesrunid,iterationid,
	yearid,monthid,dayid,hourid,
	stateid,countyid,zoneid,linkid,
	pollutantid,processid,
	sourcetypeid,regclassid,fueltypeid,modelyearid,
	roadtypeid,scc,
	engtechid,sectorid,hpid,
	emissionquant,emissionrate
from movesworkeroutput
where pollutantid in (
	select distinct pollutantid from integratedspeciesset
);

-- @algorithm convert integrated species to negative nonhaptog (88) entries
-- and nmog (80) values to positive nonhaptog (88) entries. when summed, 
-- this will complete the integration.
insert into togworkeroutput (
	mechanismid,integratedspeciessetid,
	movesrunid,iterationid,
	yearid,monthid,dayid,hourid,
	stateid,countyid,zoneid,linkid,
	pollutantid,processid,
	sourcetypeid,regclassid,fueltypeid,modelyearid,
	roadtypeid,scc,
	engtechid,sectorid,hpid,
	emissionquant,emissionrate)
select iss.mechanismid, iss.integratedspeciessetid,
	movesrunid,iterationid,
	yearid,monthid,dayid,hourid,
	stateid,countyid,zoneid,linkid,
	88 as pollutantid,
	processid,
	sourcetypeid,regclassid,fueltypeid,modelyearid,
	roadtypeid,scc,
	engtechid,sectorid,hpid,
	case
		when two.pollutantid=80 then emissionquant
		else -emissionquant
	end as emissionquant,
	case
		when two.pollutantid=80 then emissionrate
		else -emissionrate
	end as emissionrate
from togworkeroutput two
inner join integratedspeciesset iss using (pollutantid)
where two.mechanismid = 0 and two.integratedspeciessetid = 0;

-- @algorithm
drop table if exists togworkeroutputintegrated;
create table if not exists togworkeroutputintegrated (
	mechanismid		   smallint not null default 0,
	integratedspeciessetid smallint not null default 0,
	movesrunid           smallint unsigned not null default 0,
	iterationid			 smallint unsigned default 1,
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
	engtechid			 smallint unsigned null,
	sectorid 			 smallint unsigned null,
	hpid 				 smallint unsigned null,
	emissionquant        double null,
	emissionrate		 double null,
	
	key (mechanismid, integratedspeciessetid, pollutantid)
);
truncate table togworkeroutputintegrated;

-- @algorithm sum the nonhaptog (88) records, reducing record count
-- and completing the species integration.
insert into togworkeroutputintegrated (mechanismid, integratedspeciessetid,
	movesrunid,iterationid,
	yearid,monthid,dayid,hourid,
	stateid,countyid,zoneid,linkid,
	pollutantid,processid,
	sourcetypeid,regclassid,fueltypeid,modelyearid,
	roadtypeid,scc,
	engtechid,sectorid,hpid,
	emissionquant,emissionrate)
select mechanismid, integratedspeciessetid,
	movesrunid,iterationid,
	yearid,monthid,dayid,hourid,
	stateid,countyid,zoneid,linkid,
	pollutantid,processid,
	sourcetypeid,regclassid,fueltypeid,modelyearid,
	roadtypeid,scc,
	engtechid,sectorid,hpid,
	greatest(sum(emissionquant),0) as emissionquant,
	greatest(sum(emissionrate),0) as emissionrate
from togworkeroutput
where mechanismid <> 0
group by mechanismid, integratedspeciessetid,
	movesrunid,
	yearid,monthid,dayid,hourid,
	stateid,countyid,zoneid,linkid,
	pollutantid,processid,
	sourcetypeid,regclassid,fueltypeid,modelyearid,
	roadtypeid,scc
order by null;

--	engtechid,sectorid,hpid

-- @algorithm copy nonhaptog (88) entries into movesworkeroutput.
insert into movesworkeroutput (
	movesrunid,iterationid,yearid,monthid,dayid,hourid,stateid,countyid,zoneid,linkid,pollutantid,
	processid,sourcetypeid,regclassid,fueltypeid,modelyearid,roadtypeid,scc,emissionquant,emissionrate,engtechid,sectorid,hpid
)
select movesrunid,iterationid,yearid,monthid,dayid,hourid,stateid,countyid,zoneid,linkid,pollutantid,
	processid,sourcetypeid,regclassid,fueltypeid,modelyearid,roadtypeid,scc,emissionquant,emissionrate,engtechid,sectorid,hpid
from togworkeroutputintegrated
where pollutantid=88 and integratedspeciessetid>0
and mechanismid = (select min(mechanismid) from togworkeroutputintegrated where mechanismid>0);

-- @algorithm index togspeciationprofile by pollutantid to increase speed.
alter table togspeciationprofile add key tsp_pollutantid (pollutantid);

-- @algorithm
drop table if exists togspeciationpollutants;

create table if not exists togspeciationpollutants (
	mechanismid smallint not null,
	pollutantid smallint not null,
	primary key (pollutantid, mechanismid)
);

-- @algorithm get the set of distinct pollutants that are needed for speciation input.
insert into togspeciationpollutants (mechanismid, pollutantid)
select distinct mechanismid, pollutantid from togspeciationprofile;

-- @algorithm find the subset of records that are needed for speciation input.
-- these are the pollutants listed in the togspeciationprofile table.
insert into togworkeroutputintegrated (
	mechanismid,integratedspeciessetid,
	movesrunid,iterationid,
	yearid,monthid,dayid,hourid,
	stateid,countyid,zoneid,linkid,
	pollutantid,processid,
	sourcetypeid,regclassid,fueltypeid,modelyearid,
	roadtypeid,scc,
	engtechid,sectorid,hpid,
	emissionquant,emissionrate)
select mechanismid, 0 as integratedspeciessetid,
	movesrunid,iterationid,
	yearid,monthid,dayid,hourid,
	stateid,countyid,zoneid,linkid,
	mwo.pollutantid,processid,
	sourcetypeid,regclassid,fueltypeid,modelyearid,
	roadtypeid,scc,
	engtechid,sectorid,hpid,
	emissionquant,emissionrate
from movesworkeroutput mwo
inner join togspeciationpollutants tsp on (tsp.pollutantid = mwo.pollutantid);

-- @algorithm speciate nonhaptog (88) and anything else listed as an input in togspeciationprofile.

create table togtemp like movesworkeroutput;

alter table togtemp add key speed1 (
	yearid,monthid,dayid,hourid,linkid,
	pollutantid,
	processid,sourcetypeid,regclassid,fueltypeid,modelyearid,roadtypeid,scc,
	engtechid,sectorid,hpid
);

insert into togtemp (movesrunid,iterationid,yearid,monthid,dayid,hourid,stateid,countyid,zoneid,linkid,pollutantid,
	processid,sourcetypeid,regclassid,fueltypeid,modelyearid,roadtypeid,scc,emissionquant,emissionrate,engtechid,sectorid,hpid)
select a.movesrunid,a.iterationid,a.yearid,a.monthid,a.dayid,a.hourid,a.stateid,a.countyid,a.zoneid,a.linkid,
	b.outpollutantid as pollutantid,
	a.processid,a.sourcetypeid,a.regclassid,a.fueltypeid,a.modelyearid,a.roadtypeid,a.scc,
	emissionquant*factor as emissionquant,
	emissionrate*factor as emissionrate,
	a.engtechid,a.sectorid,a.hpid
from togworkeroutputintegrated a
inner join togspeciationcountyyear b on (
	a.mechanismid = b.mechanismid
	and a.integratedspeciessetid = b.integratedspeciessetid
	and a.countyid = b.countyid
	and a.monthid = b.monthid
	and a.yearid = b.yearid
	and a.processid = b.inprocessid
	and a.pollutantid = b.inpollutantid
	and a.fueltypeid = b.fueltypeid
	and a.modelyearid >= b.minmodelyearid
	and a.modelyearid <= b.maxmodelyearid
	and (a.regclassid = b.regclassid or b.regclassid=0));

insert into movesworkeroutput (
	movesrunid,iterationid,yearid,monthid,dayid,hourid,stateid,countyid,zoneid,linkid,pollutantid,
	processid,sourcetypeid,regclassid,fueltypeid,modelyearid,roadtypeid,scc,emissionquant,emissionrate,engtechid,sectorid,hpid
)
select a.movesrunid,a.iterationid,a.yearid,a.monthid,a.dayid,a.hourid,a.stateid,a.countyid,a.zoneid,a.linkid,
	pollutantid,
	a.processid,a.sourcetypeid,a.regclassid,a.fueltypeid,a.modelyearid,a.roadtypeid,a.scc,
	sum(emissionquant) as emissionquant,
	sum(emissionrate) as emissionrate,
	a.engtechid,a.sectorid,a.hpid
from togtemp a
group by a.yearid,a.monthid,a.dayid,a.hourid,a.linkid,
	pollutantid,
	a.processid,a.sourcetypeid,a.regclassid,a.fueltypeid,a.modelyearid,a.roadtypeid,a.scc
order by null;

--	a.engtechid,a.sectorid,a.hpid

-- end section processing

-- section cleanup
drop table if exists togworkeroutput;
drop table if exists togworkeroutputintegrated;
drop table if exists togspeciationcountyyear;
-- end section cleanup

-- section final cleanup
-- end section final cleanup
