-- version 2013-09-14

-- section create remote tables for extracted data
create table if not exists wtpmonthofanyyearch4n2o (
       monthid              smallint not null,
       monthname            char(10) null,
       noofdays             smallint null,
       monthgroupid         smallint not null,
	unique index xpkwtpmonthofanyyear (
       monthid                        asc)
);
truncate table wtpmonthofanyyearch4n2o;

drop table if exists wtpfactorbyfueltypech4n2o;
create table if not exists wtpfactorbyfueltypech4n2o ( 
	countyid		integer, 
	yearid			smallint, 
	monthgroupid	smallint, 
	pollutantid		smallint, 
	fueltypeid		smallint,
	wtpfactor		float,
    wtpfactorcv              float, 
	unique index xpkwtpfactor ( 
		countyid, yearid, monthgroupid, pollutantid, fueltypeid ) 
);
truncate table wtpfactorbyfueltypech4n2o;
-- end section create remote tables for extracted data

-- section extract data
create table if not exists greetwelltopumpboundsch4n2o ( 
	pollutantid		smallint, 
	fuelsubtypeid	        smallint, 
	minyearid		smallint, 
	maxyearid		smallint, 
	unique index xpkgreetwelltopumpboundsch4n2o ( 
		pollutantid, fuelsubtypeid ) 
);
create table if not exists greetwelltopumploch4n2o ( 
	pollutantid		smallint, 
	fuelsubtypeid	smallint, 
	yearid			smallint, 
	unique index xpkgreetwelltopumploch4n2o ( 
		pollutantid, fuelsubtypeid ) 
);
create table if not exists greetwelltopumphich4n2o ( 
	pollutantid		smallint, 
	fuelsubtypeid	smallint, 
	yearid			smallint, 
	unique index xpkgreetwelltopumphich4n2o ( 
		pollutantid, fuelsubtypeid ) 
);

drop table if exists wtpfactorch4n2o;
create table if not exists wtpfactorch4n2o ( 
	pollutantid		smallint, 
	fuelsubtypeid	smallint, 
	yearid			smallint, 
	wtpfactor		float, 
    wtpfactorv      float,
	unique index xpkwtpfactorch4n2o ( 
		pollutantid, fuelsubtypeid, yearid ) 
);

drop table if exists wtpfactorbyfueltypech4n2o;
create table if not exists wtpfactorbyfueltypech4n2o ( 
	countyid		integer, 
	yearid			smallint, 
	monthgroupid		smallint, 
	pollutantid		smallint, 
	fueltypeid		smallint,
	wtpfactor		float,
    wtpfactorcv              float, 
	unique index xpkwtpfactorch4n2o ( 
		countyid, yearid, monthgroupid, pollutantid, fueltypeid ) 
);

truncate greetwelltopumpboundsch4n2o;

insert into greetwelltopumpboundsch4n2o ( 
pollutantid,fuelsubtypeid,minyearid,maxyearid ) 
select pollutantid,fuelsubtypeid,min(yearid),max(yearid) 
from greetwelltopump
where pollutantid in ( ##pollutantids## ) 
group by pollutantid, fuelsubtypeid;

truncate greetwelltopumploch4n2o;

insert into greetwelltopumploch4n2o (pollutantid,fuelsubtypeid,yearid ) 
select pollutantid,fuelsubtypeid,minyearid 
from greetwelltopumpboundsch4n2o 
where minyearid >=  ##context.year##
group by pollutantid,fuelsubtypeid;

insert ignore into greetwelltopumploch4n2o (pollutantid,fuelsubtypeid,yearid ) 
select pollutantid,fuelsubtypeid,max(yearid) 
from greetwelltopump 
where yearid <=  ##context.year## and 
pollutantid in ( ##pollutantids## ) 
group by pollutantid,fuelsubtypeid;

truncate greetwelltopumphich4n2o;

insert into greetwelltopumphich4n2o (pollutantid,fuelsubtypeid,yearid ) 
select pollutantid,fuelsubtypeid,maxyearid 
from greetwelltopumpboundsch4n2o 
where maxyearid <=  ##context.year##
group by pollutantid,fuelsubtypeid;

insert ignore into greetwelltopumphich4n2o (pollutantid,fuelsubtypeid,yearid ) 
select pollutantid,fuelsubtypeid,min(yearid) 
from greetwelltopump
where yearid >  ##context.year## and 
pollutantid in ( ##pollutantids## ) 
group by pollutantid,fuelsubtypeid;

insert into wtpfactorch4n2o ( 
pollutantid,fuelsubtypeid,yearid,wtpfactor, wtpfactorv) 
select wtpflb.pollutantid,wtpflb.fuelsubtypeid, ##context.year##,
wtpflo.emissionrate + 
(wtpfhi.emissionrate - wtpflo.emissionrate) * 
((##context.year## - wtpflo.yearid)/if(wtpfhi.yearid<>wtpflo.yearid,wtpfhi.yearid-wtpflo.yearid,1)),
null
from greetwelltopump wtpflo,greetwelltopumploch4n2o wtpflb, 
greetwelltopump wtpfhi,greetwelltopumphich4n2o wtpfhb 
where 
wtpflb.pollutantid = wtpfhb.pollutantid and 
wtpflb.fuelsubtypeid = wtpfhb.fuelsubtypeid and 
wtpflb.pollutantid = wtpflo.pollutantid and 
wtpflb.fuelsubtypeid = wtpflo.fuelsubtypeid and 
wtpflb.yearid = wtpflo.yearid and 
wtpfhb.pollutantid = wtpfhi.pollutantid and 
wtpfhb.fuelsubtypeid = wtpfhi.fuelsubtypeid and 
wtpfhb.yearid = wtpfhi.yearid;

insert into wtpfactorbyfueltypech4n2o ( 
countyid, yearid, monthgroupid, pollutantid, fueltypeid, wtpfactor, wtpfactorcv)
select 
##context.iterlocation.countyrecordid## as countyid, y.yearid, fs.monthgroupid, wf.pollutantid,
fst.fueltypeid, sum(wf.wtpfactor * fs.marketshare), null
from 
fuelsubtype fst, fuelformulation ff, fuelsupply fs, year y, wtpfactorch4n2o wf 
where 
fst.fuelsubtypeid = wf.fuelsubtypeid and 
ff.fuelsubtypeid = fst.fuelsubtypeid and
fs.fuelformulationid = ff.fuelformulationid and
fs.fuelyearid = y.fuelyearid and
y.yearid = wf.yearid and
fs.fuelregionid = ##context.fuelregionid##
group by 
fs.fuelregionid,y.yearid,fs.monthgroupid,wf.pollutantid,fst.fueltypeid;

select * into outfile '##wtpfactorbyfueltypech4n2o##'
from wtpfactorbyfueltypech4n2o;

select * into outfile '##wtpmonthofanyyearch4n2o##'
from monthofanyyear;
-- end section extract data

-- section local data removal
truncate greetwelltopumpboundsch4n2o;
truncate greetwelltopumploch4n2o;
truncate greetwelltopumphich4n2o;
truncate wtpfactorch4n2o;
truncate wtpfactorbyfueltypech4n2o;
-- end section local data removal

-- section processing
drop table if exists movesoutputtemp;

create table movesoutputtemp 
select 
	mwo.yearid, mwo.monthid, mwo.dayid, 
	mwo.hourid,mwo.stateid,mwo.countyid,mwo.zoneid, 
	mwo.linkid,wfft.pollutantid,99 as processid, 
	mwo.sourcetypeid,mwo.fueltypeid,mwo.modelyearid, 
	mwo.roadtypeid,mwo.scc, 
	sum(mwo.emissionquant * wfft.wtpfactor) as emissionquant
from 
	movesworkeroutput mwo, wtpfactorbyfueltypech4n2o wfft, wtpmonthofanyyearch4n2o may 
where 
	wfft.countyid = mwo.countyid and 
	wfft.yearid = mwo.yearid and 
	may.monthid = mwo.monthid and 
	wfft.monthgroupid = may.monthgroupid and 
	mwo.pollutantid = 91 and
	mwo.fueltypeid = wfft.fueltypeid and
	mwo.processid <> 99
group by 
	mwo.yearid,mwo.monthid,mwo.dayid, mwo.hourid, 
	mwo.stateid,mwo.countyid,mwo.zoneid,mwo.linkid,wfft.pollutantid, 
	mwo.sourcetypeid,mwo.fueltypeid,mwo.modelyearid,mwo.roadtypeid, 
	mwo.scc;

insert into movesworkeroutput ( 
	yearid,monthid,dayid,hourid,stateid,countyid,zoneid, 
	linkid,pollutantid,processid,sourcetypeid,fueltypeid,modelyearid, 
	roadtypeid,scc,emissionquant) 
select 
	yearid,monthid,dayid,hourid,stateid,countyid,zoneid, 
	linkid,pollutantid,processid,sourcetypeid,fueltypeid,modelyearid, 
	roadtypeid,scc,emissionquant 
from 
	movesoutputtemp;
-- end section processing

-- section cleanup
drop table if exists movesoutputtemp;
-- end section cleanup
