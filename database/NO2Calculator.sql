-- version 2014-05-20

-- @algorithm
-- @owner no2 calculator
-- @calculator

-- section create remote tables for extracted data
drop table if exists no2copyofsourceusetype;
create table no2copyofsourceusetype (
	sourcetypeid	smallint(6)
);

drop table if exists no2copyofppa;
create table no2copyofppa (
	polprocessid	int,
	processid		smallint(6),	
	pollutantid		smallint(6)
);

drop table if exists no2copyofppmy;
create table no2copyofppmy (
	polprocessid		int,
	modelyearid			smallint(6),	
	modelyeargroupid	int(11),
	fuelmygroupid		int(11)
);

drop table if exists no2copyoffueltype;
create table no2copyoffueltype (
       fueltypeid        smallint(6)
);

drop table if exists no2copyofnono2ratio;
create table no2copyofnono2ratio (
	polprocessid		int,
	sourcetypeid		smallint(6),
	fueltypeid			smallint(6),
	modelyeargroupid	int(11),
	noxratio 			float,
	noxratiocv			float,
	datasourceid		smallint(6)
);
-- end section create remote tables for extracted data

-- section extract data
cache select distinct sourcetypeid  into outfile '##no2copyofsourceusetype##'
	from sourceusetype;

cache select distinct fueltypeid  into outfile '##no2copyoffueltype##'
	from fueltype;

cache select polprocessid,sourcetypeid,fueltypeid,modelyeargroupid,noxratio,noxratiocv,datasourceid
into outfile '##no2copyofnono2ratio##' from nono2ratio 
where polprocessid in (3301, 3302, 3390, 3391);

cache select polprocessid,processid,pollutantid
into outfile '##no2copyofppa##' from pollutantprocessassoc
where processid=##context.iterprocess.databasekey##
and pollutantid=33;

cache select polprocessid,modelyearid,modelyeargroupid,fuelmygroupid
into outfile '##no2copyofppmy##' from pollutantprocessmappedmodelyear 
where polprocessid in (3301, 3302, 3390, 3391)
and modelyearid <= ##context.year##
and modelyearid >= ##context.year## - 30;
-- end section extract data

-- section local data removal
-- end section local data removal

-- section processing
drop table if exists no2calculation1;
create table no2calculation1 (
	polprocessid			int,
	processid				smallint(6),
	pollutantid				smallint(6),
	sourcetypeid			smallint(6),
	fueltypeid				smallint(6),
	modelyearid				smallint(6),
	noxratio				float
);

-- @algorithm to simplify future table joins, add dimensions to noxratio.
insert into no2calculation1 (
	polprocessid,
	processid,
	pollutantid,
	sourcetypeid,
	fueltypeid,
	modelyearid,
	noxratio     ) 
select 
	nnr.polprocessid,
	ppa.processid,
	ppa.pollutantid,
	nnr.sourcetypeid,
	nnr.fueltypeid,
	ppmy.modelyearid,
	nnr.noxratio 
from 	no2copyofnono2ratio nnr  
		inner join 	no2copyofppa  ppa 	 	 	on nnr.polprocessid = ppa.polprocessid 
		inner join 	no2copyofsourceusetype ns 	on nnr.sourcetypeid = ns.sourcetypeid
		inner join 	no2copyofppmy ppmy		 	on nnr.modelyeargroupid = ppmy.modelyeargroupid 
		                                     		and ppa.polprocessid = ppmy.polprocessid;

create index index1 on no2calculation1 (processid, sourcetypeid, pollutantid, modelyearid, fueltypeid);

create index no2calculation1_new1 on no2calculation1 (
	fueltypeid asc,
	modelyearid asc,
	sourcetypeid asc
);

drop table if exists no2movesoutputtemp1;

-- @algorithm emissionquant = noxratio * oxides of nitrogen (3).
create table no2movesoutputtemp1
select 
	mwo.movesrunid, mwo.iterationid, mwo.yearid, mwo.monthid, mwo.dayid, 
	mwo.hourid, mwo.stateid, mwo.countyid, mwo.zoneid, 
	mwo.linkid, noc.pollutantid, noc.processid, 
	noc.sourcetypeid, mwo.regclassid, mwo.fueltypeid, mwo.modelyearid, 
	mwo.roadtypeid, mwo.scc,
	mwo.emissionquant as nox,
	noc.noxratio,
	(noc.noxratio * mwo.emissionquant) as emissionquant,
	(noc.noxratio * mwo.emissionrate) as emissionrate
from
	movesworkeroutput mwo, no2calculation1 noc  
where  
	mwo.fueltypeid			=	noc.fueltypeid		and 
	mwo.modelyearid			=	noc.modelyearid		and
	mwo.sourcetypeid		=	noc.sourcetypeid	and 
	mwo.pollutantid = 3 	and
	mwo.processid = ##context.iterprocess.databasekey##;

insert into movesworkeroutput ( 
	movesrunid,iterationid,yearid,monthid,dayid,hourid,stateid,countyid,zoneid, 
	linkid,pollutantid,processid,sourcetypeid,regclassid,fueltypeid,modelyearid, 
	roadtypeid,scc,emissionquant,emissionrate) 
select 
	movesrunid,iterationid, yearid,monthid,dayid,hourid,stateid,countyid,zoneid, 
	linkid,pollutantid,processid,sourcetypeid,regclassid,fueltypeid,modelyearid, 
	roadtypeid,scc,emissionquant,emissionrate
from no2movesoutputtemp1;
-- end section processing

-- section cleanup
drop table if exists no2copyofsourceusetype;
drop table if exists no2movesoutputtemp1;
drop table if exists no2calculation1;
drop table if exists no2copyofnono2ratio;
drop table if exists no2copyoffueltype;
drop table if exists no2copyofppa;
drop table if exists no2copyofppmy;
-- end section cleanup
