-- version 2014-05-31
-- purpose: calculate atmospheric co2 and co2 equivalent for 
--	    running, start, and extended idel exhaust emissions
-- gwo shyu, epa
-- wesley faler

-- section create remote tables for extracted data
drop table if exists co2eqpollutant;
create table if not exists co2eqpollutant(
	pollutantid		smallint not null,
	pollutantname		char(50) null,
	energyormass		char(6) null,
	globalwarmingpotential	smallint null, 
	primary key (pollutantid)
);
truncate table co2eqpollutant;

drop table if exists co2monthofanyyear;
create table if not exists co2monthofanyyear (
       	monthid              smallint not null,
       	monthname            char(10) null,
       	noofdays             smallint null,
       	monthgroupid         smallint not null,
	primary key (monthid)
);
truncate table co2monthofanyyear;

-- drop table if exists gwtpco2factorbyfueltype;
-- create table if not exists gwtpco2factorbyfueltype ( 
-- 	countyid		integer not null, 
-- 	yearid			smallint not null, 
-- 	monthgroupid		smallint not null, 
-- 	pollutantid		smallint not null,
-- 	fueltypeid		smallint not null,
-- 	sumco2emissionrate	float,
-- 	primary key (countyid, yearid, monthgroupid, pollutantid, fueltypeid)
-- );
-- truncate table gwtpco2factorbyfueltype;

drop table if exists carbonoxidationbyfueltype;
create table if not exists carbonoxidationbyfueltype ( 
	countyid		integer not null, 
	yearid			smallint not null, 
	monthgroupid		smallint not null, 
	pollutantid		smallint not null,
	fueltypeid		smallint not null,
	sumcarboncontent	float,
	sumoxidationfraction	float,
	primary key (countyid, yearid, monthgroupid, pollutantid, fueltypeid)
);
truncate table carbonoxidationbyfueltype;
-- end section create remote tables for extracted data

-- section extract data

-- drop table if exists gwtpco2factorbyfueltype;
-- create table if not exists gwtpco2factorbyfueltype ( 
-- 	countyid		integer not null, 
-- 	yearid			smallint not null, 
-- 	monthgroupid		smallint not null, 
-- 	pollutantid		smallint not null,
-- 	fueltypeid		smallint not null,
-- 	sumco2emissionrate	float,
-- 	primary key (countyid, yearid, monthgroupid, pollutantid, fueltypeid)
-- );

-- subsection task 122 step 1a: calculate atmospheric co2 from total energy for running exhaust
drop table if exists carbonoxidationbyfueltype;
create table if not exists carbonoxidationbyfueltype ( 
	countyid		integer not null, 
	yearid			smallint not null, 
	monthgroupid		smallint not null, 
	pollutantid		smallint not null,
	fueltypeid		smallint not null,
	sumcarboncontent	float,
	sumoxidationfraction	float,
	primary key (countyid, yearid, monthgroupid, pollutantid, fueltypeid)
);

-- @algorithm sumcarboncontent[countyid,yearid,monthgroupid,pollutantid,fueltypeid]=sum(marketshare * carboncontent).
-- sumoxidationfraction[countyid,yearid,monthgroupid,pollutantid,fueltypeid]=sum(marketshare * oxidationfraction).
insert into carbonoxidationbyfueltype (
	countyid, 
	yearid, 
	monthgroupid, 
	pollutantid, 
	fueltypeid,
	sumcarboncontent,
	sumoxidationfraction)
select 
	##context.iterlocation.countyrecordid## as countyid, y.yearid, fs.monthgroupid, 
	##atmoshpericco2pollutantid## as pollutantid, fst.fueltypeid,
	sum(fs.marketshare*fst.carboncontent) as sumcarboncontent, 
	sum(fs.marketshare*fst.oxidationfraction) as sumoxidationfraction
from fuelsupply fs
inner join fuelformulation ff on ff.fuelformulationid = fs.fuelformulationid
inner join fuelsubtype fst on fst.fuelsubtypeid = ff.fuelsubtypeid
inner join year y on y.fuelyearid = fs.fuelyearid
where y.yearid = ##context.year##
and fs.fuelregionid = ##context.fuelregionid##
group by fs.fuelregionid, y.yearid, fs.monthgroupid, fst.fueltypeid;
-- flush tables;

select * into outfile '##carbonoxidationbyfueltype##'
from carbonoxidationbyfueltype;
-- flush tables;
-- end subsection task 122 step 1a

select pollutantid,pollutantname,energyormass,globalwarmingpotential
into outfile '##co2eqpollutant##' from pollutant where globalwarmingpotential is not null and globalwarmingpotential > 0;

select * into outfile '##co2monthofanyyear##' from monthofanyyear;
-- flush tables;
-- end section extract data

-- section local data removal
-- truncate gwtpco2factorbyfueltype;

-- truncate carbonoxidationbyfueltype;

-- end section local data removal

-- section processing

-- 	subsection task 122 step 1a: calculate atmospheric co2 from total energy from
-- 		running, start, and extended idle exhaust
drop table if exists movesoutputco2temp1a;
-- 	7/31/2007 gwo added "coft.monthgroupid=may.monthgroupid and" in where clause

-- @algorithm atmosphereic co2 = sum(total energy consumption * sumcarboncontent * sumoxidationfraction * (44/12)).
create table movesoutputco2temp1a 
select 
	mwo.movesrunid, mwo.yearid, mwo.monthid, mwo.dayid, 
	mwo.hourid,mwo.stateid,mwo.countyid,mwo.zoneid, 
	mwo.linkid,##atmoshpericco2pollutantid## as pollutantid,mwo.processid, 
	mwo.sourcetypeid,mwo.regclassid,mwo.fueltypeid,mwo.modelyearid, 
	mwo.roadtypeid,mwo.scc, 
	sum(mwo.emissionquant * coft.sumcarboncontent * coft.sumoxidationfraction * (44/12)) as emissionquant,
	sum(mwo.emissionrate  * coft.sumcarboncontent * coft.sumoxidationfraction * (44/12)) as emissionrate
from 
	movesworkeroutput mwo, carbonoxidationbyfueltype coft, co2monthofanyyear may 
where 
	coft.countyid = mwo.countyid and 
	coft.yearid = mwo.yearid and 
	coft.monthgroupid=may.monthgroupid and
	may.monthid = mwo.monthid and 
	coft.fueltypeid = mwo.fueltypeid and
	mwo.pollutantid = ##totalenergyconsumptionid## and
	##co2step1aprocessids## 
group by 
	mwo.movesrunid,mwo.yearid,mwo.monthid,mwo.dayid, mwo.hourid, 
	mwo.stateid,mwo.countyid,mwo.zoneid,mwo.linkid, mwo.processid, 
	mwo.sourcetypeid,mwo.regclassid,mwo.fueltypeid,mwo.modelyearid,mwo.roadtypeid, 
	mwo.scc;
-- flush tables;
-- 	end of subsection task 122 step 1a:

insert into movesworkeroutput ( 
	movesrunid,yearid,monthid,dayid,hourid,stateid,countyid,zoneid, 
	linkid,pollutantid,processid,sourcetypeid,regclassid,fueltypeid,modelyearid, 
	roadtypeid,scc,emissionquant,emissionrate) 
select 
	movesrunid,yearid,monthid,dayid,hourid,stateid,countyid,zoneid, 
	linkid,pollutantid,processid,sourcetypeid,regclassid,fueltypeid,modelyearid, 
	roadtypeid,scc,emissionquant,emissionrate
from movesoutputco2temp1a;
analyze table movesworkeroutput;
-- flush tables;

-- 	subsection task 122 step 2: calculate equivalent co2 from co2, methane, and n2o
drop table if exists movesoutputco2temp2;

-- @algorithm equivalent co2 = sum(emissions[polutant=co2 or methane or n2o] * globalwarmingpotential).
create table movesoutputco2temp2 
select 
	mwo.movesrunid, mwo.yearid, mwo.monthid, mwo.dayid, 
	mwo.hourid,mwo.stateid,mwo.countyid,mwo.zoneid, 
	mwo.linkid,##equivalentco2pollutantid## as pollutantid,mwo.processid, 
	mwo.sourcetypeid,mwo.regclassid,mwo.fueltypeid,mwo.modelyearid, 
	mwo.roadtypeid,mwo.scc, 
	sum(mwo.emissionquant * pol.globalwarmingpotential) as emissionquant,
	sum(mwo.emissionrate  * pol.globalwarmingpotential) as emissionrate
from 
	movesworkeroutput mwo inner join co2eqpollutant pol on mwo.pollutantid = pol.pollutantid 
where 	mwo.pollutantid in (##co2step2pollutantids##) and ##co2step2processids##  
group by 
	mwo.movesrunid,mwo.yearid,mwo.monthid,mwo.dayid, mwo.hourid, 
	mwo.stateid,mwo.countyid,mwo.zoneid,mwo.linkid, mwo.processid, 
	mwo.sourcetypeid,mwo.regclassid,mwo.fueltypeid,mwo.modelyearid,mwo.roadtypeid, mwo.scc;
-- flush tables;
-- 	end of subsection task 122 step 2:

insert into movesworkeroutput ( 
	movesrunid,yearid,monthid,dayid,hourid,stateid,countyid,zoneid, 
	linkid,pollutantid,processid,sourcetypeid,regclassid,fueltypeid,modelyearid, 
	roadtypeid,scc,emissionquant,emissionrate) 
select 
	movesrunid,yearid,monthid,dayid,hourid,stateid,countyid,zoneid, 
	linkid,pollutantid,processid,sourcetypeid,regclassid,fueltypeid,modelyearid, 
	roadtypeid,scc,emissionquant,emissionrate
from movesoutputco2temp2;
analyze table movesworkeroutput;
-- flush tables;


-- end section processing

-- section cleanup
drop table if exists movesoutputco2temp1a;
drop table if exists movesoutputco2temp2;

-- end section cleanup
