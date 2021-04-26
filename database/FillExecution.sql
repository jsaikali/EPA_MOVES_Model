-- create utility tables in the movesexecution database.
-- author harvey michaels
-- author wesley faler
-- version 2017-03-22

-- --------------------------------------------------------------------
-- fill the set of regions used by the runspec
-- --------------------------------------------------------------------
insert into runspecfuelregion (fuelregionid)
select distinct regionid as fuelregionid
from regioncounty
inner join runspeccounty using (countyid)
where regioncodeid=1;

-- --------------------------------------------------------------------
-- add indexes and extra columns needed for runtime but not in the default database.
-- --------------------------------------------------------------------
alter table opmodedistribution add key (linkid, polprocessid, sourcetypeid);

-- --------------------------------------------------------------------
-- filter any disabled mechanism and speciation profile data
-- --------------------------------------------------------------------
delete from mechanismname
where not exists (
	select *
	from runspecpollutant
	inner join pollutant using (pollutantid)
	inner join pollutantdisplaygroup using (pollutantdisplaygroupid)
	where pollutantdisplaygroupname='Mechanisms'
	and pollutantid = 1000 + ((mechanismid-1) * 500)
);

delete from integratedspeciesset where useissyn<>'Y';

delete from integratedspeciesset
where not exists (
	select * from mechanismname
	where mechanismname.mechanismid = integratedspeciesset.mechanismid);

delete from integratedspeciessetname
where not exists (
	select * from integratedspeciesset 
	where integratedspeciesset.integratedspeciessetid = integratedspeciessetname.integratedspeciessetid);

-- remove togspeciationprofile entries that use inactive integratedspeciesset entries.
-- keep any togspeciationprofile entries that are not tied to any integratedspeciesset.
delete from togspeciationprofile
where integratedspeciessetid <> 0
and not exists (
	select * from integratedspeciesset 
	where integratedspeciesset.integratedspeciessetid = togspeciationprofile.integratedspeciessetid);

delete from lumpedspeciesname
where not exists (
	select * from togspeciationprofile
	where togspeciationprofile.lumpedspeciesname = lumpedspeciesname.lumpedspeciesname);

delete from togspeciation
where not exists (
	select * from togspeciationprofile
	where togspeciationprofile.togspeciationprofileid = togspeciation.togspeciationprofileid);

-- expand togspeciationprofile entries that use wildcard togspeciationpofileid=0
drop table if exists togtemp;
create table togtemp
select distinct mechanismid, togspeciationprofileid, pollutantid, lumpedspeciesname
from togspeciationprofile;

alter table togtemp add unique key (mechanismid, togspeciationprofileid, pollutantid, lumpedspeciesname);

insert ignore into togspeciationprofile (mechanismid, togspeciationprofileid, integratedspeciessetid, 
	pollutantid, lumpedspeciesname,
	togspeciationdivisor, togspeciationmassfraction)
select distinct tsp.mechanismid, ts.togspeciationprofileid, tsp.integratedspeciessetid, 
	tsp.pollutantid, tsp.lumpedspeciesname,
	tsp.togspeciationdivisor, tsp.togspeciationmassfraction
from togspeciationprofile tsp, togspeciation ts
where tsp.togspeciationprofileid='0'
and not exists (
	select *
	from togtemp t
	where t.mechanismid=tsp.mechanismid
	and t.togspeciationprofileid=ts.togspeciationprofileid
	and t.pollutantid=tsp.pollutantid
	and t.lumpedspeciesname=tsp.lumpedspeciesname
);

drop table if exists togtemp;

-- delete any wildcard togspeciationprofile entries after expansion
delete from togspeciationprofile where togspeciationprofileid='0';

-- delete any togspeciationprofile entries for integratedspeciessetid=0
-- that are represented by a non-zero integratedspeciessetid.
drop table if exists togtemp;
create table togtemp
select distinct mechanismid, togspeciationprofileid, pollutantid, lumpedspeciesname
from togspeciationprofile
where integratedspeciessetid<>0;

alter table togtemp add unique key (mechanismid, togspeciationprofileid, pollutantid, lumpedspeciesname);

delete from togspeciationprofile
where integratedspeciessetid=0
and exists (
	select *
	from togtemp t
	where t.mechanismid=togspeciationprofile.mechanismid
	and t.togspeciationprofileid=togspeciationprofile.togspeciationprofileid
	and t.pollutantid=togspeciationprofile.pollutantid
	and t.lumpedspeciesname=togspeciationprofile.lumpedspeciesname
);

drop table if exists togtemp;

-- remove unused tog profile names
delete from togspeciationprofilename
where not exists (
	select * from togspeciationprofile
	where togspeciationprofile.togspeciationprofileid = togspeciationprofilename.togspeciationprofileid);

-- decode the togspeciation.modelyeargroupid
alter table togspeciation add minmodelyearid smallint not null default 0;
alter table togspeciation add maxmodelyearid smallint not null default 0;

update togspeciation set minmodelyearid = floor(modelyeargroupid/10000),
	maxmodelyearid = mod(modelyeargroupid,10000)
where minmodelyearid=0 or maxmodelyearid=0;

-- --------------------------------------------------------------------
-- provide a place for thc e85/e10 fuel adjustments. these are special cases
-- to match e10. these ratios are made with e85 fuel properties except
-- using an e10 rvp.
-- --------------------------------------------------------------------
create table altcriteriaratio like criteriaratio;
