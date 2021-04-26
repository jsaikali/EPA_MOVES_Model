-- version 2013-09-15
-- author wesley faler
-- purpose: add calculation of ammonia (nh3) pollutant auxiliary power exhaust emissions to moves2010

-- section create remote tables for extracted data

##create.runspecyear##;
truncate runspecyear;

##create.agecategory##;
truncate agecategory;

##create.county##;
truncate county;

##create.emissionratebyage##;
truncate emissionratebyage;

##create.hourday##;
truncate hourday;

##create.imcoverage##;
truncate imcoverage;

##create.imfactor##;
truncate imfactor;

##create.link##;
truncate link;

##create.monthgrouphour##;
truncate monthgrouphour;

##create.monthofanyyear##;
truncate monthofanyyear;

##create.hotellingactivitydistribution##;
truncate hotellingactivitydistribution;

##create.pollutantprocessassoc##;
truncate pollutantprocessassoc;

##create.hotellinghours##;
truncate hotellinghours;

##create.runspecsourcefueltype##;
truncate runspecsourcefueltype;

##create.sourcebin##;
truncate sourcebin;

##create.sourcebindistribution##;
truncate sourcebindistribution;

##create.sourcetypeage##;
truncate sourcetypeage;

##create.sourcetypemodelyear##;
truncate sourcetypemodelyear;

##create.year##;
truncate year;

##create.zone##;
truncate zone;

drop table if exists onecountyyeargeneralfuelratio;
create table if not exists onecountyyeargeneralfuelratio (
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
truncate onecountyyeargeneralfuelratio;

-- end section create remote tables for extracted data

-- section extract data

cache select * into outfile '##agecategory##'
from agecategory;

cache select * into outfile '##county##'
from county
where countyid = ##context.iterlocation.countyrecordid##;

cache select distinct emissionratebyage.* into outfile '##emissionratebyage##'
from emissionratebyage, sourcebindistribution, sourcetypemodelyear, sourcebin, runspecsourcefueltype 
where 
    emissionratebyage.opmodeid >= 201 and emissionratebyage.opmodeid <= 299
    and runspecsourcefueltype.fueltypeid = sourcebin.fueltypeid
	and emissionratebyage.polprocessid = sourcebindistribution.polprocessid
	and emissionratebyage.sourcebinid = sourcebin.sourcebinid
	and emissionratebyage.sourcebinid = sourcebindistribution.sourcebinid
	and sourcebin.sourcebinid = sourcebindistribution.sourcebinid
	and runspecsourcefueltype.sourcetypeid = sourcetypemodelyear.sourcetypeid
	and sourcebindistribution.sourcetypemodelyearid = sourcetypemodelyear.sourcetypemodelyearid
	and sourcetypemodelyear.modelyearid <= ##context.year## 
	and sourcetypemodelyear.modelyearid >= ##context.year## - 30
	and emissionratebyage.polprocessid in (##pollutantprocessids##);

cache select distinct hourday.* into outfile '##hourday##'
from hourday,runspechour,runspecday
where hourday.dayid = runspecday.dayid and hourday.hourid = runspechour.hourid;

cache select distinct imcoverage.* into outfile '##imcoverage##'
from imcoverage
inner join runspecsourcefueltype on (runspecsourcefueltype.fueltypeid = imcoverage.fueltypeid
	and runspecsourcefueltype.sourcetypeid = imcoverage.sourcetypeid)
where polprocessid in (##pollutantprocessids##)
and countyid = ##context.iterlocation.countyrecordid## 
and yearid = ##context.year##
and useimyn = 'Y';

cache select distinct imfactor.* into outfile '##imfactor##'
from imfactor
inner join runspecsourcefueltype on (runspecsourcefueltype.fueltypeid = imfactor.fueltypeid
	and runspecsourcefueltype.sourcetypeid = imfactor.sourcetypeid)
where polprocessid in (##pollutantprocessids##);

cache select link.* into outfile '##link##'
from link 
where roadtypeid = 1 and 
	zoneid = ##context.iterlocation.zonerecordid##;

cache select monthgrouphour.* into outfile '##monthgrouphour##' 
from monthgrouphour inner join runspechour using (hourid);

cache select monthofanyyear.* into outfile '##monthofanyyear##'
from monthofanyyear,runspecmonth
where monthofanyyear.monthid = runspecmonth.monthid;

cache select * into outfile '##hotellingactivitydistribution##'
from hotellingactivitydistribution
where (beginmodelyearid <= ##context.year## - 30 and endmodelyearid >= ##context.year## - 30)
or (beginmodelyearid <= ##context.year## and endmodelyearid >= ##context.year##)
or (beginmodelyearid >= ##context.year## - 30 and endmodelyearid <= ##context.year##);

cache select * into outfile '##pollutantprocessassoc##'
from pollutantprocessassoc
where processid=##context.iterprocess.databasekey##;

cache select * into outfile '##hotellinghours##' 
from hotellinghours 
where yearid = ##context.year## 
	and zoneid = ##context.iterlocation.zonerecordid##;

cache select * into outfile '##runspecsourcefueltype##'
from runspecsourcefueltype;

cache select distinct sourcebin.* into outfile '##sourcebin##'
from sourcebindistribution, sourcetypemodelyear, sourcebin, runspecsourcefueltype
where polprocessid in (##pollutantprocessids##)
	and sourcebindistribution.sourcetypemodelyearid = sourcetypemodelyear.sourcetypemodelyearid
	and sourcetypemodelyear.sourcetypeid = runspecsourcefueltype.sourcetypeid
	and sourcebindistribution.sourcebinid = sourcebin.sourcebinid
	and sourcebin.fueltypeid = runspecsourcefueltype.fueltypeid
	and sourcetypemodelyear.modelyearid <= ##context.year##
	and sourcetypemodelyear.modelyearid >= ##context.year## - 30;

cache select distinct sourcebindistribution.* into outfile '##sourcebindistribution##'
from sourcebindistributionfuelusage_##context.iterprocess.databasekey##_##context.iterlocation.countyrecordid##_##context.year## as sourcebindistribution, 
sourcetypemodelyear, sourcebin, runspecsourcefueltype 
where polprocessid in (##pollutantprocessids##)
	and sourcebindistribution.sourcetypemodelyearid = sourcetypemodelyear.sourcetypemodelyearid
	and sourcetypemodelyear.modelyearid <= ##context.year## 
	and sourcetypemodelyear.modelyearid >= ##context.year## - 30
	and sourcetypemodelyear.sourcetypeid = runspecsourcefueltype.sourcetypeid
	and sourcebindistribution.sourcebinid = sourcebin.sourcebinid
	and sourcebin.fueltypeid = runspecsourcefueltype.fueltypeid;

cache select sourcetypeage.* into outfile '##sourcetypeage##'
from sourcetypeage,runspecsourcetype
where sourcetypeage.sourcetypeid = runspecsourcetype.sourcetypeid;

cache select sourcetypemodelyear.* into outfile '##sourcetypemodelyear##'
from sourcetypemodelyear,runspecsourcetype 
where 	sourcetypemodelyear.sourcetypeid = runspecsourcetype.sourcetypeid  
	and sourcetypemodelyear.modelyearid <= ##context.year##
	and sourcetypemodelyear.modelyearid >= ##context.year## - 30;

cache select year.* into outfile '##year##'
from year 
where yearid = ##context.year##;

cache select runspecyear.* into outfile '##runspecyear##'
from runspecyear;

cache select * into outfile '##zone##'
from zone
where zoneid = ##context.iterlocation.zonerecordid##;

select gfr.fueltypeid, gfr.sourcetypeid, may.monthid, gfr.pollutantid, gfr.processid, mya.modelyearid, mya.yearid,
	sum((ifnull(fueleffectratio,1)+gpafract*(ifnull(fueleffectratiogpa,1)-ifnull(fueleffectratio,1)))*marketshare) as fueleffectratio
	into outfile '##onecountyyeargeneralfuelratio##'
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
	and gfr.polprocessid in (##pollutantprocessids##)
	and gfr.minmodelyearid <= mya.modelyearid
	and gfr.maxmodelyearid >= mya.modelyearid
	and gfr.minageid <= mya.ageid
	and gfr.maxageid >= mya.ageid
	and gfr.fueltypeid = rssf.fueltypeid
	and gfr.sourcetypeid = rssf.sourcetypeid)
group by gfr.fueltypeid, gfr.sourcetypeid, may.monthid, gfr.pollutantid, gfr.processid, mya.modelyearid, mya.yearid
;

-- end section extract data

-- section processing

-- nh3eic-1: sourcebin-weighted weight emission rates

drop table if exists emissionratebyage2;

create table if not exists emissionratebyage2(
	sourcebinid	bigint(20) not null,
	polprocessid	int not null,
	opmodeid	smallint not null,
	agegroupid	smallint not null,
	modelyearid	smallint not null,
	fueltypeid	smallint not null,
	sourcetypeid	smallint not null,
	meanbaserate	float null,
	sourcebinactivityfraction	float null,
	opmodefraction float not null,
	primary key (sourcebinid, polprocessid, opmodeid, agegroupid, modelyearid, fueltypeid, sourcetypeid)
);

truncate table emissionratebyage2;

insert ignore into emissionratebyage2 (sourcebinid, polprocessid, opmodeid, agegroupid, meanbaserate, 
	modelyearid, fueltypeid, sourcetypeid, sourcebinactivityfraction, opmodefraction) 
select distinct er.sourcebinid, er.polprocessid, er.opmodeid, er.agegroupid, er.meanbaserate, 
	stmy.modelyearid, sb.fueltypeid, stmy.sourcetypeid, sbd.sourcebinactivityfraction, hac.opmodefraction
from emissionratebyage er, sourcebin sb, runspecsourcefueltype rsft, sourcebindistribution sbd, sourcetypemodelyear stmy, 
	hotellingactivitydistribution hac
where 	
	er.sourcebinid 	= sb.sourcebinid 
	and sb.fueltypeid 	= rsft.fueltypeid 
	and er.polprocessid in (##pollutantprocessids##) 
	and sbd.sourcebinid = sb.sourcebinid 
	and sbd.polprocessid in (##pollutantprocessids##) 
	and sbd.sourcetypemodelyearid = stmy.sourcetypemodelyearid 
	and hac.beginmodelyearid <= stmy.modelyearid
	and hac.endmodelyearid >= stmy.modelyearid
	and hac.opmodeid = er.opmodeid
;

drop table if exists sbweightedemissionrate;
create table sbweightedemissionrate (
       sourcebinid      bigint(20) not null,
       polprocessid 	int not null,
       sourcetypeid 	smallint not null,
       modelyearid 	smallint not null,
       fueltypeid 	smallint not null,
       agegroupid	smallint not null,
       meanbaserate 	float
);


create unique index xpksbweightedemissionrate on sbweightedemissionrate (
       sourcebinid	asc,
       polprocessid 	asc,
       sourcetypeid 	asc,
       modelyearid 	asc,
       fueltypeid 	asc,
       agegroupid	asc
);
analyze table sbweightedemissionrate;

insert into sbweightedemissionrate (sourcebinid, polprocessid, sourcetypeid, modelyearid, fueltypeid, agegroupid, meanbaserate)
select er.sourcebinid, er.polprocessid, er.sourcetypeid, er.modelyearid, er.fueltypeid, er.agegroupid, 
	sum(er.sourcebinactivityfraction * er.meanbaserate * er.opmodefraction) as meanbaserate 
from emissionratebyage2 er
group by er.polprocessid, er.sourcetypeid, er.modelyearid, er.fueltypeid, er.agegroupid 
order by null;


-- flush tables;

-- nh3eic-2: multiply emission rates by activity

drop table if exists hh2;
create table hh2 (
       zoneid		int not null,
       monthid		smallint not null,
       hourdayid        smallint not null,
       hourid		smallint,
       dayid		smallint,
       yearid		smallint not null,
       ageid		smallint not null,
       agegroupid		smallint not null,
       sourcetypeid 	smallint not null,
       hotellinghours 	float
);

create unique index xpkhh2 on hh2 (
      zoneid		asc,
       monthid          asc,
       hourdayid	asc,
       yearid		asc,
       ageid		asc,
       agegroupid   asc,
       sourcetypeid	asc);

analyze table hh2;

-- flush tables;

truncate table hh2;


insert into hh2 (zoneid, monthid, hourdayid, hourid, dayid, yearid, ageid, agegroupid, sourcetypeid, hotellinghours) 
select hh.zoneid, hh.monthid, hh.hourdayid, hrdy.hourid, hrdy.dayid, hh.yearid, hh.ageid, ac.agegroupid, hh.sourcetypeid, hh.hotellinghours 
from hotellinghours hh 
inner join hourday hrdy on (hrdy.hourdayid=hh.hourdayid)
inner join agecategory ac on (hh.ageid=ac.ageid)
;

-- flush tables;

drop table if exists emissionresultswithtime;
create table emissionresultswithtime (
       polprocessid 	int not null,
       sourcetypeid 	smallint not null,
       modelyearid 	smallint not null,
       fueltypeid 	smallint not null,
       zoneid		int not null,
       monthid		smallint not null,
       hourid		smallint not null,
       dayid		smallint not null,
       yearid		smallint not null,
       ageid		smallint not null,
       agegroupid		smallint not null,
       emissionquant 	float
);

create unique index xpkemissionresultswithtime on emissionresultswithtime (
       polprocessid 	asc,
       sourcetypeid 	asc,
       modelyearid 	asc,
       fueltypeid 	asc,
       zoneid		asc,
       monthid      asc,
       hourid		asc,
       dayid		asc,
       yearid		asc,
       ageid        asc,
       agegroupid   asc
);
analyze table emissionresultswithtime;

-- ------------------------------

insert into emissionresultswithtime (polprocessid, sourcetypeid, modelyearid, fueltypeid, zoneid, monthid, 
				hourid, dayid, yearid, ageid, agegroupid, emissionquant)
select waer.polprocessid, waer.sourcetypeid, waer.modelyearid, waer.fueltypeid, hh.zoneid, hh.monthid, 
				hh.hourid, hh.dayid, hh.yearid, hh.ageid, waer.agegroupid,  
	(waer.meanbaserate * hh.hotellinghours) as emissionquant 
from sbweightedemissionrate waer, hh2 hh
where hh.sourcetypeid=waer.sourcetypeid and hh.agegroupid=waer.agegroupid 
		and hh.ageid = hh.yearid - waer.modelyearid
;

-- nh3eic-3: multiply emission rates for the year


drop table if exists emissionresults;
create table emissionresults (
       polprocessid 	int not null,
       sourcetypeid 	smallint not null,
       modelyearid 	smallint not null,
       fueltypeid 	smallint not null,
       zoneid		int not null,
       monthid		smallint not null,
       hourid		smallint not null,
       dayid		smallint not null,
       yearid		smallint not null,
       ageid		smallint not null,
       emissionquant 	float
);

create unique index xpkemissionresults on emissionresults (
       polprocessid 	asc,
       sourcetypeid 	asc,
       modelyearid 	asc,
       fueltypeid 	asc,
       zoneid		asc,
       monthid          asc,
       hourid		asc,
       dayid		asc,
       yearid		asc,
       ageid            asc
);
analyze table emissionresults;

insert into emissionresults (polprocessid, sourcetypeid, modelyearid, fueltypeid, zoneid, monthid, 
				hourid, dayid, yearid, ageid, emissionquant)
select erwt.polprocessid, erwt.sourcetypeid, erwt.modelyearid, erwt.fueltypeid, erwt.zoneid, erwt.monthid, 
				erwt.hourid, erwt.dayid, erwt.yearid, erwt.ageid,  
	sum(erwt.emissionquant) as emissionquant 
from emissionresultswithtime erwt inner join runspecyear ry using (yearid)
group by erwt.polprocessid, erwt.sourcetypeid, erwt.modelyearid, erwt.fueltypeid, 
		erwt.zoneid, erwt.monthid, erwt.hourid, erwt.dayid, erwt.yearid 
order by null;

-- nh3eic-4: convert results to structure of movesworkeroutput by sourcetypeid

truncate movesworkeroutput;
insert into movesworkeroutput (
	stateid, countyid, zoneid, linkid, roadtypeid, yearid, monthid, dayid, hourid, pollutantid, 
	processid, sourcetypeid, fueltypeid, modelyearid, scc, emissionquant)
select ##context.iterlocation.staterecordid## as stateid, 
	##context.iterlocation.countyrecordid## as countyid, aer.zoneid, lnk.linkid, lnk.roadtypeid,
	aer.yearid, aer.monthid, aer.dayid, aer.hourid, ppa.pollutantid, ppa.processid, aer.sourcetypeid, 
	aer.fueltypeid, aer.modelyearid, null as scc, aer.emissionquant as emissionquant 
from emissionresults aer 
inner join pollutantprocessassoc ppa on (ppa.polprocessid = aer.polprocessid)
inner join link lnk on (lnk.zoneid=aer.zoneid)
where lnk.roadtypeid=1 
order by null;

update movesworkeroutput, onecountyyeargeneralfuelratio set emissionquant=emissionquant*fueleffectratio
where onecountyyeargeneralfuelratio.fueltypeid = movesworkeroutput.fueltypeid
and onecountyyeargeneralfuelratio.sourcetypeid = movesworkeroutput.sourcetypeid
and onecountyyeargeneralfuelratio.monthid = movesworkeroutput.monthid
and onecountyyeargeneralfuelratio.pollutantid = movesworkeroutput.pollutantid
and onecountyyeargeneralfuelratio.processid = movesworkeroutput.processid
and onecountyyeargeneralfuelratio.modelyearid = movesworkeroutput.modelyearid
and onecountyyeargeneralfuelratio.yearid = movesworkeroutput.yearid;

-- end section processing

-- section cleanup
drop table if exists hh2;
drop table if exists emissionratebyage2;
drop table if exists sbweightedemissionrate;
drop table if exists emissionresults;
drop table if exists onecountyyeargeneralfuelratio;
-- end section cleanup
