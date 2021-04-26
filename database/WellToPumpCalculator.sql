-- version 2013-09-15

-- @notused

-- section create remote tables for extracted data
create table if not exists wtpmonthofanyyear (
       monthid              smallint not null,
       monthname            char(10) null,
       noofdays             smallint null,
       monthgroupid         smallint not null,
	unique index xpkwtpmonthofanyyear (
       monthid                        asc)
);
truncate table wtpmonthofanyyear;

drop table if exists wtpfactorbyfueltype;
create table if not exists wtpfactorbyfueltype ( 
	countyid		integer, 
	yearid			smallint, 
	monthgroupid	smallint, 
	pollutantid		smallint, 
	fueltypeid		smallint,
	wtpfactor		float,
    wtpfactorcv     float, 
	unique index xpkwtpfactor ( 
		countyid, yearid, monthgroupid, pollutantid, fueltypeid ) 
);
truncate table wtpfactorbyfueltype;
-- end section create remote tables for extracted data

-- section extract data
create table if not exists greetwelltopumpbounds ( 
	pollutantid		smallint, 
	fuelsubtypeid	smallint, 
	minyearid		smallint, 
	maxyearid		smallint, 
	unique index xpkgreetwelltopumpbounds ( 
		pollutantid, fuelsubtypeid ) 
);
create table if not exists greetwelltopumplo ( 
	pollutantid		smallint, 
	fuelsubtypeid	smallint, 
	yearid			smallint, 
	unique index xpkgreetwelltopumplo ( 
		pollutantid, fuelsubtypeid ) 
);
create table if not exists greetwelltopumphi ( 
	pollutantid		smallint, 
	fuelsubtypeid	smallint, 
	yearid			smallint, 
	unique index xpkgreetwelltopumphi ( 
		pollutantid, fuelsubtypeid ) 
);

drop table if exists wtpfactor;
create table if not exists wtpfactor ( 
	pollutantid		smallint, 
	fuelsubtypeid	smallint, 
	yearid			smallint, 
	wtpfactor		float, 
    wtpfactorv      float,
	unique index xpkwtpfactor ( 
		pollutantid, fuelsubtypeid, yearid ) 
);

drop table if exists wtpfactorbyfueltype;
create table if not exists wtpfactorbyfueltype ( 
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

truncate greetwelltopumpbounds;

insert into greetwelltopumpbounds ( 
pollutantid,fuelsubtypeid,minyearid,maxyearid )
select pollutantid,fuelsubtypeid,min(yearid),max(yearid) 
from greetwelltopump 
where pollutantid in ( ##pollutantids## ) 
group by pollutantid, fuelsubtypeid;

truncate greetwelltopumplo;

insert into greetwelltopumplo (pollutantid,fuelsubtypeid,yearid ) 
select pollutantid,fuelsubtypeid,minyearid 
from greetwelltopumpbounds 
where minyearid >=  ##context.year##
group by pollutantid,fuelsubtypeid;

insert ignore into greetwelltopumplo (pollutantid,fuelsubtypeid,yearid ) 
select pollutantid,fuelsubtypeid,max(yearid) 
from greetwelltopump 
where yearid <=  ##context.year## and 
pollutantid in ( ##pollutantids## ) 
group by pollutantid,fuelsubtypeid;

truncate greetwelltopumphi;
insert into greetwelltopumphi (pollutantid,fuelsubtypeid,yearid ) 
select pollutantid,fuelsubtypeid,maxyearid 
from greetwelltopumpbounds 
where maxyearid <=  ##context.year##
group by pollutantid,fuelsubtypeid;

insert ignore into greetwelltopumphi (pollutantid,fuelsubtypeid,yearid ) 
select pollutantid,fuelsubtypeid,min(yearid) 
from greetwelltopump 
where yearid >  ##context.year## and 
pollutantid in ( ##pollutantids## ) 
group by pollutantid,fuelsubtypeid;

insert into wtpfactor ( 
pollutantid,fuelsubtypeid,yearid,wtpfactor, wtpfactorv) 
select wtpflb.pollutantid,wtpflb.fuelsubtypeid, ##context.year##,
wtpflo.emissionrate + 
(wtpfhi.emissionrate - wtpflo.emissionrate) * 
((##context.year## - wtpflo.yearid)/if(wtpfhi.yearid<>wtpflo.yearid,wtpfhi.yearid-wtpflo.yearid,1)),
null
from greetwelltopump wtpflo,greetwelltopumplo wtpflb, 
greetwelltopump wtpfhi,greetwelltopumphi wtpfhb 
where 
wtpflb.pollutantid = wtpfhb.pollutantid and 
wtpflb.fuelsubtypeid = wtpfhb.fuelsubtypeid and 
wtpflb.pollutantid = wtpflo.pollutantid and 
wtpflb.fuelsubtypeid = wtpflo.fuelsubtypeid and 
wtpflb.yearid = wtpflo.yearid and 
wtpfhb.pollutantid = wtpfhi.pollutantid and 
wtpfhb.fuelsubtypeid = wtpfhi.fuelsubtypeid and 
wtpfhb.yearid = wtpfhi.yearid;

insert into wtpfactorbyfueltype ( 
countyid,yearid,monthgroupid,pollutantid,fueltypeid,wtpfactor,wtpfactorcv) 
select 
##context.iterlocation.countyrecordid## as countyid,y.yearid,fs.monthgroupid, 
wf.pollutantid,fst.fueltypeid,sum(wf.wtpfactor * fs.marketshare), null 
from 
fuelsubtype fst, fuelformulation ff, year y, fuelsupply fs, wtpfactor wf 
where 
fs.fuelyearid = y.fuelyearid and
y.yearid = wf.yearid and
fs.fuelregionid = ##context.fuelregionid## and
fs.fuelformulationid = ff.fuelformulationid and
ff.fuelsubtypeid = wf.fuelsubtypeid and 
ff.fuelsubtypeid = fst.fuelsubtypeid
group by 
fs.fuelregionid,y.yearid,fs.monthgroupid,wf.pollutantid,fst.fueltypeid;

select * into outfile '##wtpfactorbyfueltype##'
from wtpfactorbyfueltype;

select * into outfile '##wtpmonthofanyyear##'
from monthofanyyear;
-- end section extract data

-- section local data removal
truncate greetwelltopumpbounds;
truncate greetwelltopumplo;
truncate greetwelltopumphi;
truncate wtpfactor;
truncate wtpfactorbyfueltype;
-- end section local data removal

-- section processing
drop table if exists movesoutputtemp;

create table movesoutputtemp 
select 
	mwo.movesrunid, mwo.yearid, mwo.monthid, mwo.dayid, 
	mwo.hourid,mwo.stateid,mwo.countyid,mwo.zoneid, 
	mwo.linkid,wfft.pollutantid,99 as processid, 
	mwo.sourcetypeid,mwo.fueltypeid,mwo.modelyearid, 
	mwo.roadtypeid,mwo.scc,
	sum(mwo.emissionquant) * wfft.wtpfactor as emissionquant
from
	movesworkeroutput mwo, wtpfactorbyfueltype wfft, wtpmonthofanyyear may 
where 
	wfft.countyid = mwo.countyid and 
	wfft.yearid = mwo.yearid and 
	may.monthid = mwo.monthid and 
	wfft.monthgroupid = may.monthgroupid and 
	mwo.pollutantid = 91 and
	mwo.fueltypeid = wfft.fueltypeid and
	mwo.processid <> 99
group by 
	mwo.movesrunid,mwo.yearid,mwo.monthid,mwo.dayid, mwo.hourid, 
	mwo.stateid,mwo.countyid,mwo.zoneid,mwo.linkid,wfft.pollutantid, 
	mwo.sourcetypeid,mwo.fueltypeid,mwo.modelyearid,mwo.roadtypeid, 
	mwo.scc;

insert into movesworkeroutput ( 
	movesrunid,yearid,monthid,dayid,hourid,stateid,countyid,zoneid, 
	linkid,pollutantid,processid,sourcetypeid,fueltypeid,modelyearid, 
	roadtypeid,scc,emissionquant) 
select 
	movesrunid,yearid,monthid,dayid,hourid,stateid,countyid,zoneid, 
	linkid,pollutantid,processid,sourcetypeid,fueltypeid,modelyearid, 
	roadtypeid,scc,emissionquant 
from 
	movesoutputtemp;
-- end section processing

-- section cleanup
drop table if exists movesoutputtemp;
-- end section cleanup
