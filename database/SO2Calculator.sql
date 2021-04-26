-- version 2014-05-31

-- @algorithm
-- @owner so2 calculator
-- @calculator

-- section create remote tables for extracted data

drop table if exists so2runspecmodelyear;
create table if not exists so2runspecmodelyear (
	modelyearid smallint not null primary key
);
truncate so2runspecmodelyear;

drop table if exists so2pmonecountyyeargeneralfuelratio;
create table if not exists so2pmonecountyyeargeneralfuelratio (
	fueltypeid int not null,
	sourcetypeid int not null,
	monthid int not null,
	pollutantid int not null,
	processid int not null,
	modelyearid int not null,
	yearid int not null,
	fueleffectratio double not null default '0',
	primary key (fueltypeid, sourcetypeid, monthid, pollutantid, modelyearid, yearid)
);
truncate so2pmonecountyyeargeneralfuelratio;

drop table if exists so2copyofmonthofanyyear;
create table so2copyofmonthofanyyear (
	monthid 		smallint(6),
	monthgroupid 	smallint(6)
);

drop table if exists so2copyofppa;
create table so2copyofppa (
	polprocessid	int,
	processid		smallint(6),	
	pollutantid		smallint(6)
);

drop table if exists so2copyofyear;
create table so2copyofyear (
       yearid        smallint(6),
       isbaseyear    char(1),
       fuelyearid    smallint(6)
);

drop table if exists so2copyoffuelformulation;
create table so2copyoffuelformulation (
       fuelformulationid        smallint(6),
       fuelsubtypeid    		smallint(6),
       sulfurlevel  			float,
       primary key (fuelformulationid),
       key (fuelformulationid, fuelsubtypeid),
       key (fuelsubtypeid, fuelformulationid)
);

drop table if exists so2copyoffuelsupply;
create table so2copyoffuelsupply (
	fuelregionid			int(11),
	fuelyearid				smallint(6),
	monthgroupid			smallint(6),
	fuelformulationid		smallint(6),
	marketshare				float,
	marketsharecv 			float
);

drop table if exists so2copyoffueltype;
create table so2copyoffueltype (
       fueltypeid        smallint(6),
       primary key (fueltypeid)
);

drop table if exists so2copyoffuelsubtype;
create table so2copyoffuelsubtype (
	   fuelsubtypeid	 smallint(6),
       fueltypeid        smallint(6),
       energycontent	 float,
       primary key (fuelsubtypeid),
       key (fueltypeid, fuelsubtypeid),
       key (fuelsubtypeid, fueltypeid)
);

drop table if exists copyofso2emissionrate;
create table copyofso2emissionrate (
	polprocessid		int,
	fueltypeid			smallint(6),
	modelyeargroupid	int(11),
	meanbaserate 		float,
	meanbaseratecv		float,
	datasourceid		smallint(6)
);

-- end section create remote tables for extracted data

-- section extract data

cache select *
	into outfile '##so2runspecmodelyear##'
from runspecmodelyear;

cache select gfr.fueltypeid, gfr.sourcetypeid, may.monthid, gfr.pollutantid, gfr.processid, mya.modelyearid, mya.yearid,
	sum((ifnull(fueleffectratio,1)+gpafract*(ifnull(fueleffectratiogpa,1)-ifnull(fueleffectratio,1)))*marketshare) as fueleffectratio
	into outfile '##so2pmonecountyyeargeneralfuelratio##'
from runspecmonthgroup rsmg
inner join runspecmodelyearage mya on (mya.yearid = ##context.year##)
inner join county c on (c.countyid = ##context.iterlocation.countyrecordid##)
inner join year y on (y.yearid = mya.yearid)
inner join fuelsupply fs on (fs.fuelregionid = ##context.fuelregionid##
	and fs.fuelyearid = y.fuelyearid
	and fs.monthgroupid = rsmg.monthgroupid)
inner join monthofanyyear may on (may.monthgroupid = fs.monthgroupid)
inner join runspecsourcefueltype rssf
inner join generalfuelratio gfr on (gfr.fuelformulationid = fs.fuelformulationid
	and gfr.pollutantid = 31
	and gfr.processid = ##context.iterprocess.databasekey##
	and gfr.minmodelyearid <= mya.modelyearid
	and gfr.maxmodelyearid >= mya.modelyearid
	and gfr.minageid <= mya.ageid
	and gfr.maxageid >= mya.ageid
	and gfr.fueltypeid = rssf.fueltypeid
	and gfr.sourcetypeid = rssf.sourcetypeid)
group by gfr.fueltypeid, gfr.sourcetypeid, may.monthid, gfr.pollutantid, gfr.processid, mya.modelyearid, mya.yearid;

cache select 	monthofanyyear.monthid, 
		monthofanyyear.monthgroupid 
into outfile '##so2copyofmonthofanyyear##'  
from monthofanyyear  inner join runspecmonth 
on		monthofanyyear.monthid = runspecmonth.monthid;

cache select yearid, isbaseyear, fuelyearid into outfile '##so2copyofyear##'  from year where yearid = ##context.year##;

cache select 	fuelformulationid, 
		fuelsubtypeid, 
		sulfurlevel 
into outfile 		'##so2copyoffuelformulation##'  
from fuelformulation;

cache select fuelsupply.*  into outfile '##so2copyoffuelsupply##'  
	from fuelsupply
	inner join year on fuelsupply.fuelyearid = year.fuelyearid 
		and year.yearid = ##context.year##    
	where fuelregionid = ##context.fuelregionid##;

cache select fueltypeid into outfile '##so2copyoffueltype##'
	from fueltype;

cache select  fuelsubtypeid, fueltypeid, energycontent into outfile '##so2copyoffuelsubtype##'
	from fuelsubtype;

cache select 	*  into outfile '##copyofso2emissionrate##'  from sulfateemissionrate 
	where polprocessid in (3101, 3102, 3190, 3191);

cache select polprocessid,processid,pollutantid
into outfile '##so2copyofppa##' from pollutantprocessassoc 
where processid=##context.iterprocess.databasekey## 
and pollutantid=31;

-- end section extract data

-- section local data removal
-- end section local data removal

-- section processing

drop table if exists so2fuelcalculation1;
create table so2fuelcalculation1 (
	countyid				int(11),
	yearid					smallint(6),
	monthgroupid			smallint(6),
	fueltypeid				smallint(6), 
	energycontent			float,
	wsulfurlevel			float
);

-- @algorithm energycontent = sum(marketshare * energycontent) across the fuel supply.
-- wsulfurlevel = sum(marketshare * sulfurlevel) across the fuel supply.
insert into so2fuelcalculation1 (
	countyid,
	yearid,
	monthgroupid,
	fueltypeid, 
	energycontent,
	wsulfurlevel   ) 
select 
	##context.iterlocation.countyrecordid## as countyid, 
	y.yearid,
	fs.monthgroupid,
	ft.fueltypeid, 
	sum(fs.marketshare * fst.energycontent) as energycontent,
	sum(fs.marketshare * ff.sulfurlevel) as wsulfurlevel  
from so2copyoffuelsupply fs 
	inner join so2copyoffuelformulation ff 		on fs.fuelformulationid = ff.fuelformulationid 
	inner join so2copyoffuelsubtype fst 		on fst.fuelsubtypeid = ff.fuelsubtypeid 
	inner join so2copyoffueltype ft 			on fst.fueltypeid = ft.fueltypeid 
	inner join so2copyofyear y 					on y.fuelyearid = fs.fuelyearid 
group by fs.fuelregionid, y.yearid, fs.monthgroupid, fst.fueltypeid;

create index index1 on so2fuelcalculation1 (countyid, yearid, monthgroupid, fueltypeid);

drop table if exists so2fuelcalculation2;
create table so2fuelcalculation2 (
	polprocessid			int,
	processid				smallint(6),
	pollutantid				smallint(6),
	fueltypeid				smallint(6),
	modelyearid				smallint(6),
	meanbaserate			float
);

alter table copyofso2emissionrate add column minmodelyearid smallint null;
alter table copyofso2emissionrate add column maxmodelyearid smallint null;

update copyofso2emissionrate set
	minmodelyearid = floor(modelyeargroupid / 10000),
	maxmodelyearid = mod(modelyeargroupid, 10000);

insert into so2fuelcalculation2 (
	polprocessid,
	processid,
	pollutantid,
	fueltypeid,
	modelyearid,
	meanbaserate     ) 
select 
	ser.polprocessid,
	ppa.processid,
	ppa.pollutantid,
	ser.fueltypeid,
	rsmy.modelyearid,
	ser.meanbaserate 
from 	copyofso2emissionrate ser  
	inner join 	so2copyofppa  ppa 	 	on ser.polprocessid = ppa.polprocessid
	inner join  so2runspecmodelyear rsmy on (
		rsmy.modelyearid >= ser.minmodelyearid
		and rsmy.modelyearid <= ser.maxmodelyearid
	);

create index index1 on so2fuelcalculation2 (processid, pollutantid, modelyearid, fueltypeid);

drop table if exists so2movesoutputtemp1;

-- @algorithm so2 (31) = (meanbaserate * wsulfurlevel * total energy consumption (91)) / energycontent.
create table so2movesoutputtemp1
select 
	mwo.movesrunid, mwo.iterationid, mwo.yearid, mwo.monthid, mwo.dayid, 
	mwo.hourid, mwo.stateid, mwo.countyid, mwo.zoneid, 
	mwo.linkid, fc2.pollutantid, fc2.processid, 
	mwo.sourcetypeid, mwo.regclassid, mwo.fueltypeid, mwo.modelyearid, 
	mwo.roadtypeid, mwo.scc,
	mwo.emissionquant as energy,
	mwo.emissionrate as energyrate,
	fc1.wsulfurlevel,
	fc1.energycontent,
	fc2.meanbaserate,
	( (fc2.meanbaserate * fc1.wsulfurlevel * mwo.emissionquant ) / fc1.energycontent ) as emissionquant,
	( (fc2.meanbaserate * fc1.wsulfurlevel * mwo.emissionrate )  / fc1.energycontent ) as emissionrate
from
	movesworkeroutput mwo, so2fuelcalculation1 fc1, so2fuelcalculation2 fc2, so2copyofmonthofanyyear may  
where 
	mwo.countyid			=	fc1.countyid 		and
	mwo.yearid				=	fc1.yearid			and 
	mwo.monthid				= 	may.monthid			and
	fc1.monthgroupid		=	may.monthgroupid 	and  
	mwo.fueltypeid			=	fc1.fueltypeid		and 
	mwo.fueltypeid			=	fc2.fueltypeid		and 
	mwo.modelyearid			=	fc2.modelyearid		and
	mwo.pollutantid = 91 	and
	mwo.processid = ##context.iterprocess.databasekey##;

-- @algorithm apply general fuel effects.
-- emissionquant = emissionquant * fueleffectratio.
update so2movesoutputtemp1, so2pmonecountyyeargeneralfuelratio set 
	emissionquant=emissionquant*fueleffectratio,
	emissionrate =emissionrate *fueleffectratio
where so2pmonecountyyeargeneralfuelratio.fueltypeid = so2movesoutputtemp1.fueltypeid
and so2pmonecountyyeargeneralfuelratio.sourcetypeid = so2movesoutputtemp1.sourcetypeid
and so2pmonecountyyeargeneralfuelratio.monthid 		= so2movesoutputtemp1.monthid
and so2pmonecountyyeargeneralfuelratio.pollutantid 	= so2movesoutputtemp1.pollutantid
and so2pmonecountyyeargeneralfuelratio.processid 	= so2movesoutputtemp1.processid
and so2pmonecountyyeargeneralfuelratio.modelyearid 	= so2movesoutputtemp1.modelyearid
and so2pmonecountyyeargeneralfuelratio.yearid 		= so2movesoutputtemp1.yearid;

insert into movesworkeroutput ( 
	movesrunid,iterationid,yearid,monthid,dayid,hourid,stateid,countyid,zoneid, 
	linkid,pollutantid,processid,sourcetypeid,regclassid,fueltypeid,modelyearid, 
	roadtypeid,scc,emissionquant,emissionrate) 
select 
	movesrunid,iterationid, yearid,monthid,dayid,hourid,stateid,countyid,zoneid, 
	linkid,pollutantid,processid,sourcetypeid,regclassid,fueltypeid,modelyearid, 
	roadtypeid,scc,emissionquant,emissionrate
from so2movesoutputtemp1;

-- end section processing

-- section cleanup

drop table if exists so2movesoutputtemp1;
drop table if exists so2fuelcalculation1;
drop table if exists so2fuelcalculation2;
drop table if exists so2copyofmonthofanyyear;
drop table if exists so2copyofppa;
drop table if exists so2copyofyear;
drop table if exists so2copyoffuelformulation;
drop table if exists so2copyoffuelsupply;
drop table if exists so2copyoffueltype;
drop table if exists so2copyoffuelsubtype;
drop table if exists copyofso2emissionrate;
drop table if exists so2runspecmodelyear;

-- end section cleanup
