-- author wesley faler
-- author ed glover epa
-- version 2014-08-20

-- @algorithm
-- @owner no calculator
-- @calculator

-- section create remote tables for extracted data
drop table if exists nocopyofsourceusetype;
create table nocopyofsourceusetype (
	sourcetypeid	smallint(6)
);

drop table if exists nocopyofppa;
create table nocopyofppa (
	polprocessid	int,
	processid		smallint(6),	
	pollutantid		smallint(6)
);

drop table if exists nocopyofppmy;
create table nocopyofppmy (
	polprocessid		int,
	modelyearid			smallint(6),	
	modelyeargroupid	int(11),
	fuelmygroupid		int(11)
);

drop table if exists nocopyoffueltype;
create table nocopyoffueltype (
       fueltypeid        smallint(6)
);

drop table if exists nocopyofnono2ratio;
create table nocopyofnono2ratio (
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
cache select distinct sourcetypeid  into outfile '##nocopyofsourceusetype##'
	from sourceusetype;

cache select distinct fueltypeid  into outfile '##nocopyoffueltype##'
	from fueltype;

cache select polprocessid,sourcetypeid,fueltypeid,modelyeargroupid,noxratio,noxratiocv,datasourceid
into outfile '##nocopyofnono2ratio##' from nono2ratio
where polprocessid in (##pollutantprocessids##);

cache select polprocessid,processid,pollutantid
into outfile '##nocopyofppa##' from pollutantprocessassoc 
where processid=##context.iterprocess.databasekey##
and pollutantid in (##pollutantids##);

cache select polprocessid,modelyearid,modelyeargroupid,fuelmygroupid
into outfile '##nocopyofppmy##' from pollutantprocessmappedmodelyear 
where polprocessid in (##pollutantprocessids##)
and modelyearid <= ##context.year##
and modelyearid >= ##context.year## - 30;
-- end section extract data

-- section local data removal
-- end section local data removal

-- section processing
drop table if exists nocalculation1;

create table nocalculation1 (
	polprocessid			int,
	processid				smallint(6),
	pollutantid				smallint(6),
	sourcetypeid			smallint(6),
	fueltypeid				smallint(6),
	modelyearid				smallint(6),
	noxratio				float
);

-- @algorithm to simplify future table joins, add dimensions to noxratio.
insert into nocalculation1 (
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
from 	nocopyofnono2ratio nnr  
		inner join 	nocopyofppa  ppa 	 	 	on nnr.polprocessid = ppa.polprocessid 
		inner join 	nocopyofsourceusetype ns 	on nnr.sourcetypeid = ns.sourcetypeid
		inner join 	nocopyofppmy ppmy		 	on nnr.modelyeargroupid = ppmy.modelyeargroupid 
		                                     		and ppa.polprocessid = ppmy.polprocessid;

create index index1 on nocalculation1 (processid, sourcetypeid, pollutantid, modelyearid, fueltypeid);

create index movesworkeroutput_new1 on movesworkeroutput (
	fueltypeid asc,
	modelyearid asc,
	sourcetypeid asc,
	pollutantid asc,
	processid asc
);
create index nocalculation1_new1 on nocalculation1 (
	fueltypeid asc,
	modelyearid asc,
	sourcetypeid asc
);

drop table if exists nomovesoutputtemp1;

-- @algorithm emissionquant = noxratio * oxides of nitrogen (3).
create table nomovesoutputtemp1
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
	movesworkeroutput mwo, nocalculation1 noc  
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
from nomovesoutputtemp1;

alter table movesworkeroutput drop index movesworkeroutput_new1;
-- end section processing

-- section cleanup
drop table if exists nocopyofsourceusetype;
drop table if exists nomovesoutputtemp1;
drop table if exists nocalculation1;
drop table if exists nocopyofnono2ratio;
drop table if exists nocopyoffueltype;
drop table if exists nocopyofppa;
drop table if exists nocopyofppmy;
-- end section cleanup
