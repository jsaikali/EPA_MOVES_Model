-- version 2013-09-14
-- purpose: calculate atmospheric co2 emission from well-to-pump
-- modified 2007-07-31 to fix the error of co2 calculation
-- gwo shyu, epa

-- section create remote tables for extracted data
drop table if exists wtpco2monthofanyyear;
create table if not exists wtpco2monthofanyyear (
       	monthid              smallint not null,
       	monthname            char(10) null,
       	noofdays             smallint null,
       	monthgroupid         smallint not null,
	primary key (monthid)
);
truncate table wtpco2monthofanyyear;

drop table if exists gwtpco2factorbyfueltype;
create table if not exists gwtpco2factorbyfueltype ( 
	countyid		integer not null, 
	yearid			smallint not null, 
	monthgroupid		smallint not null, 
	pollutantid		smallint not null,
	fueltypeid		smallint not null,
	sumco2emissionrate	float,
	primary key (countyid, yearid, monthgroupid, pollutantid, fueltypeid)
);
truncate table gwtpco2factorbyfueltype;
-- end section create remote tables for extracted data

-- section extract data
drop table if exists gwtpco2factorbyfueltype;
create table if not exists gwtpco2factorbyfueltype ( 
	countyid		integer not null, 
	yearid			smallint not null, 
	monthgroupid		smallint not null, 
	pollutantid		smallint not null,
	fueltypeid		smallint not null,
	sumco2emissionrate	float,
	primary key (countyid, yearid, monthgroupid, pollutantid, fueltypeid)
);

insert into gwtpco2factorbyfueltype (
	countyid, 
	yearid, 
	monthgroupid, 
	pollutantid, 
	fueltypeid,
	sumco2emissionrate)
select ##context.iterlocation.countyrecordid## as countyid, y.yearid, fs.monthgroupid, 
	##atmoshpericco2pollutantid## as pollutantid, fst.fueltypeid,  
	sum(fs.marketshare*gwtp.emissionrate) as sumco2emissionrate 
from (fuelsupply fs 
inner join fuelformulation ff on ff.fuelformulationid = fs.fuelformulationid
inner join year y on y.fuelyearid = fs.fuelyearid
inner join greetwelltopump gwtp on gwtp.fuelsubtypeid = ff.fuelsubtypeid
and y.yearid = gwtp.yearid)
inner join fuelsubtype fst on	fst.fuelsubtypeid=ff.fuelsubtypeid
where gwtp.pollutantid = ##atmoshpericco2pollutantid##
and y.yearid = ##context.year##
and fs.fuelregionid = ##context.fuelregionid##
group by fs.fuelregionid, y.yearid, fs.monthgroupid, fst.fueltypeid
limit 10;
-- flush tables;

select * into outfile '##gwtpco2factorbyfueltype##' from gwtpco2factorbyfueltype;
-- flush tables;

select * into outfile '##wtpco2monthofanyyear##' from monthofanyyear;
-- flush tables;
-- end section extract data

-- section local data removal
truncate gwtpco2factorbyfueltype;
-- end section local data removal

-- section processing
-- 	** subsection task 122 step 1b: calculate atmospheric co2 from well-to-pump
drop table if exists movesoutputtemp1b;
-- 	7/31/2007 gwo added "gwtp.monthgroupid=may.monthgroupid and" in where clause
create table movesoutputtemp1b 
select 
	mwo.movesrunid, mwo.yearid, mwo.monthid, mwo.dayid, 
	mwo.hourid,mwo.stateid,mwo.countyid,mwo.zoneid, 
	mwo.linkid, ##atmoshpericco2pollutantid## as pollutantid, ##well-to-pumpid## as processid, 
	mwo.sourcetypeid,mwo.fueltypeid,mwo.modelyearid, 
	mwo.roadtypeid,mwo.scc, 
	sum(mwo.emissionquant * gwtp.sumco2emissionrate) as emissionquant
from 
	movesworkeroutput mwo, gwtpco2factorbyfueltype gwtp, wtpco2monthofanyyear may 
where 
	gwtp.countyid = mwo.countyid and 
	gwtp.yearid = mwo.yearid and 
	gwtp.monthgroupid=may.monthgroupid and
	may.monthid = mwo.monthid and 
	mwo.pollutantid = ##totalenergyconsumptionid## and
	(mwo.processid = ##runningexhaustid## or 
	 mwo.processid = ##startexhaustid## or 
	 mwo.processid = ##extendedidleexhaustid##) and
	gwtp.fueltypeid = mwo.fueltypeid
group by 
	mwo.movesrunid,mwo.yearid,mwo.monthid,mwo.dayid, mwo.hourid, 
	mwo.stateid,mwo.countyid,mwo.zoneid,mwo.linkid,mwo.pollutantid, mwo.processid, 
	mwo.sourcetypeid,mwo.fueltypeid,mwo.modelyearid,mwo.roadtypeid, 
	mwo.scc;
-- flush tables;
-- 	** end of subsection task 122 step 1b:

insert into movesworkeroutput ( 
	movesrunid,yearid,monthid,dayid,hourid,stateid,countyid,zoneid, 
	linkid,pollutantid,processid,sourcetypeid,fueltypeid,modelyearid, 
	roadtypeid,scc,emissionquant) 
select 
	movesrunid,yearid,monthid,dayid,hourid,stateid,countyid,zoneid, 
	linkid,pollutantid,processid,sourcetypeid,fueltypeid,modelyearid, 
	roadtypeid,scc,emissionquant 
from movesoutputtemp1b;
analyze table movesworkeroutput;
-- flush tables;

-- end section processing

-- section cleanup
-- drop table if exists movesoutputtemp1b;
-- end section cleanup
