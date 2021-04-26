-- version 2014-10-14
-- author wesley faler
-- supported special section names:
--		refuelingdisplacementvaporloss
--		refuelingspillageloss

-- @algorithm
-- @owner refueling loss calculator
-- @calculator

-- supported special variable names:
--		##refuelingdisplacement.pollutantids##
--		##refuelingspillage.pollutantids##
--		##refuelingprocessids##

-- section create remote tables for extracted data
##create.refuelingfactors##;
truncate table refuelingfactors;

##create.sourcetypetechadjustment##;
truncate table sourcetypetechadjustment;

drop table if exists refuelingfueltype;
create table refuelingfueltype (
	fueltypeid           smallint not null,
	defaultformulationid smallint not null,
	fueltypedesc         char(50) null,
	humiditycorrectioncoeff float null,
	humiditycorrectioncoeffcv float null,
	energycontent			float	null,
	fueldensity				float	null,
	monthid smallint not null,
	unique index xpkrefuelingfueltype (fueltypeid, monthid),
	key (monthid, fueltypeid)
);
truncate table refuelingfueltype;

drop table if exists refuelingrunspechour;
create table refuelingrunspechour (
	hourid smallint(6) not null,
	unique index xpkrefuelingrunspechour ( hourid )
);
truncate table refuelingrunspechour;

drop table if exists refuelingrunspecmonth;
create table refuelingrunspecmonth (
	monthid smallint(6) not null,
	unique index xpkrefuelingrunspecmonth ( monthid )
);
truncate table refuelingrunspecmonth;

drop table if exists refuelingcountyyear;
create table refuelingcountyyear (
	countyid             integer not null,
	yearid               smallint not null,
	refuelingvaporprogramadjust float not null default 0.0,
	refuelingspillprogramadjust float not null default 0.0
);
truncate table refuelingcountyyear;

-- section refuelingdisplacementvaporloss
drop table if exists refuelingdisplacementpollutant;
create table refuelingdisplacementpollutant (
	pollutantid smallint(6) not null,
	unique index xpkrefuelingdisplacementpollutant (pollutantid)
);
truncate table refuelingdisplacementpollutant;
-- end section refuelingdisplacementvaporloss

-- section refuelingspillageloss
drop table if exists refuelingspillagepollutant;
create table refuelingspillagepollutant (
	pollutantid smallint(6) not null,
	unique index xpkrefuelingspillagepollutant (pollutantid)
);
truncate table refuelingspillagepollutant;
-- end section refuelingspillageloss

drop table if exists refuelingfuelformulation;
create table refuelingfuelformulation (
  fuelformulationid smallint(6) not null default '0',
  fuelsubtypeid smallint(6) not null default '0',
  rvp float default null,
  sulfurlevel float not null default '30',
  etohvolume float default null,
  mtbevolume float default null,
  etbevolume float default null,
  tamevolume float default null,
  aromaticcontent float default null,
  olefincontent float default null,
  benzenecontent float default null,
  e200 float default null,
  e300 float default null,
  voltowtpercentoxy float default null,
  biodieselestervolume float default null,
  cetaneindex float default null,
  pahcontent float default null,
  t50 float default null,
  t90 float default null,
  primary key (fuelformulationid)
);
truncate table refuelingfuelformulation;

drop table if exists refuelingfuelsubtype;
create table refuelingfuelsubtype (
  fuelsubtypeid smallint(6) not null default '0',
  fueltypeid smallint(6) not null default '0',
  fuelsubtypedesc char(50) default null,
  fuelsubtypepetroleumfraction float default null,
  fuelsubtypepetroleumfractioncv float default null,
  fuelsubtypefossilfraction float default null,
  fuelsubtypefossilfractioncv float default null,
  carboncontent float default null,
  oxidationfraction float default null,
  carboncontentcv float default null,
  oxidationfractioncv float default null,
  energycontent float default null,
  primary key (fuelsubtypeid),
  key fueltypeid (fueltypeid,fuelsubtypeid)
);
truncate table refuelingfuelsubtype;

drop table if exists refuelingfuelsupply;
create table refuelingfuelsupply (
  fuelregionid int(11) not null default '0',
  fuelyearid int(11) not null default '0',
  monthgroupid smallint(6) not null default '0',
  fuelformulationid smallint(6) not null default '0',
  marketshare float default null,
  marketsharecv float default null,
  primary key (fuelregionid,fuelformulationid,monthgroupid,fuelyearid),
  key countyid (fuelregionid),
  key yearid (fuelyearid),
  key monthgroupid (monthgroupid),
  key fuelsubtypeid (fuelformulationid)
);
truncate table refuelingfuelsupply;

drop table if exists refuelingmonthofanyyear;
create table refuelingmonthofanyyear (
  monthid smallint(6) not null default '0',
  monthname char(10) default null,
  noofdays smallint(6) default null,
  monthgroupid smallint(6) not null default '0',
  primary key (monthid),
  key monthgroupid (monthgroupid),
  key monthgroupid_2 (monthgroupid,monthid),
  key monthid (monthid,monthgroupid)
);
truncate table refuelingmonthofanyyear;

drop table if exists refuelingzonemonthhour;
create table refuelingzonemonthhour (
  monthid smallint(6) not null default '0',
  zoneid int(11) not null default '0',
  hourid smallint(6) not null default '0',
  temperature float default null,
  temperaturecv float default null,
  relhumidity float default null,
  heatindex float default null,
  specifichumidity float default null,
  relativehumiditycv float default null,
  primary key (hourid,monthid,zoneid),
  key monthid (monthid),
  key zoneid (zoneid),
  key hourid (hourid)
);
truncate table refuelingzonemonthhour;

-- end section create remote tables for extracted data

-- section extract data
cache select * into outfile '##refuelingfactors##'
from refuelingfactors;

-- @algorithm energycontent = sum(marketshare * energycontent) across all the fuel supply.
cache select ft.fueltypeid, defaultformulationid, fueltypedesc, humiditycorrectioncoeff, humiditycorrectioncoeffcv,
	sum(marketshare*energycontent) as energycontent, fueldensity, rsm.monthid
into outfile '##refuelingfueltype##'
from fueltype ft
inner join fuelsubtype fst on (fst.fueltypeid=ft.fueltypeid)
inner join fuelformulation ff on (ff.fuelsubtypeid=fst.fuelsubtypeid)
inner join fuelsupply fs on (
	fs.fuelregionid=##context.fuelregionid##
	and fs.fuelformulationid=ff.fuelformulationid)
inner join year y on (
	y.yearid=##context.year##
	and y.fuelyearid=fs.fuelyearid)
inner join monthofanyyear m on (m.monthgroupid=fs.monthgroupid)
inner join runspecmonth rsm on (rsm.monthid=m.monthid)
where energycontent is not null
and energycontent > 0
and fueldensity is not null
and fueldensity > 0
group by ft.fueltypeid, rsm.monthid;

cache select
	processid, sourcetypeid, 
	myrmap(modelyearid) as modelyearid, 
	refuelingtechadjustment
into outfile '##sourcetypetechadjustment##'
from sourcetypetechadjustment
where processid in (##refuelingprocessids##)
and modelyearid <= mymap(##context.year##)
and modelyearid >= mymap(##context.year## - 30);

cache select * into outfile '##refuelingrunspechour##'
from runspechour;

cache select * into outfile '##refuelingrunspecmonth##'
from runspecmonth;

cache select * into outfile '##refuelingcountyyear##'
from countyyear
where countyid=##context.iterlocation.countyrecordid##
and yearid=##context.year##;

-- section refuelingdisplacementvaporloss
cache select pollutantid into outfile '##refuelingdisplacementpollutant##'
from pollutant
where pollutantid in (##refuelingdisplacement.pollutantids##);
-- end section refuelingdisplacementvaporloss

-- section refuelingspillageloss
cache select pollutantid into outfile '##refuelingspillagepollutant##'
from pollutant
where pollutantid in (##refuelingspillage.pollutantids##);
-- end section refuelingspillageloss

cache select ff.* into outfile '##refuelingfuelformulation##'
from fuelformulation ff
inner join fuelsupply fs on fs.fuelformulationid = ff.fuelformulationid
inner join year y on y.fuelyearid = fs.fuelyearid
inner join runspecmonthgroup rsmg on rsmg.monthgroupid = fs.monthgroupid
where fuelregionid = ##context.fuelregionid## and
yearid = ##context.year##
group by ff.fuelformulationid order by null;

cache select * into outfile '##refuelingfuelsubtype##'
from fuelsubtype;

cache select fuelsupply.* into outfile '##refuelingfuelsupply##'
from fuelsupply
inner join runspecmonthgroup on (fuelsupply.monthgroupid = runspecmonthgroup.monthgroupid)
inner join year on (fuelsupply.fuelyearid = year.fuelyearid)
where fuelregionid = ##context.fuelregionid##
and yearid = ##context.year##;

cache select monthofanyyear.*
into outfile '##refuelingmonthofanyyear##'
from monthofanyyear;

cache select zonemonthhour.*
into outfile '##refuelingzonemonthhour##'
from runspecmonth
inner join runspechour
inner join zonemonthhour on (zonemonthhour.monthid = runspecmonth.monthid and zonemonthhour.hourid = runspechour.hourid)
where zoneid = ##context.iterlocation.zonerecordid##;

-- end section extract data

-- section local data removal
--truncate xxxxxx;
-- section refuelingdisplacementvaporloss
-- end section refuelingdisplacementvaporloss

-- section refuelingspillageloss
-- end section refuelingspillageloss
-- end section local data removal

-- section processing

-- section refuelingdisplacementvaporloss
-- --------------------------------------------------------------
-- refec-1: determine the refueling temperatures
-- --------------------------------------------------------------

-- @algorithm determine the refueling temperatures
drop table if exists refuelingtemp;

create table refuelingtemp (
	monthid smallint(6) not null,
	hourid smallint(6) not null,
	fueltypeid smallint(6) not null,

	refuelingtemperature float not null default 0.0,
	tanktemperaturedif float not null default 0.0,
	displacedvaporrate float not null default 0.0,

	key(monthid),
	key(hourid),
	key(fueltypeid),
	unique index xpkrefuelingtemp (monthid,hourid,fueltypeid)
);

-- @algorithm refuelingtemperature = temperature subject to lower bound of vaporlowlimit and upper bound of vaporhighlimit.
insert into refuelingtemp (monthid, hourid, fueltypeid, refuelingtemperature)
select monthid, hourid, fueltypeid,
	case
		when temperature <= vaporlowtlimit then vaporlowtlimit
		when temperature >= vaporhightlimit then vaporhightlimit
		else temperature
	end as refuelingtemperature
from refuelingzonemonthhour, refuelingfactors;

analyze table refuelingtemp;

-- @algorithm tanktemperaturedif = ((vaporterme*refuelingtemperature) + vaportermf) subject to lower bound of 0 and upper bound of tankdifflimit.
update refuelingtemp, refuelingfactors
set refuelingtemp.tanktemperaturedif = 
	case
		when ((vaporterme*refuelingtemperature) + vaportermf) >= tanktdifflimit then tanktdifflimit
		when ((vaporterme*refuelingtemperature) + vaportermf) <= 0.0 then 0.0
		else ((vaporterme*refuelingtemperature) + vaportermf)
	end
where refuelingfactors.fueltypeid=refuelingtemp.fueltypeid;

-- --------------------------------------------------------------
-- refec-2: determine the unadjusted refueling displacement vapor loss rate
-- --------------------------------------------------------------

-- @algorithm determine the unadjusted refueling displacement vapor loss rate
drop table if exists refuelingaveragervp;

create table refuelingaveragervp (
	monthid smallint(6) not null,
	fueltypeid smallint(6) not null,
	averagervp float not null,
	key(monthid),
	key(fueltypeid),
	unique index xpkrefuelingaveragervp (monthid, fueltypeid)
);

-- @algorithm averagervp = sum(rvp * marketshare) across fuel formulations.
insert into refuelingaveragervp (monthid, fueltypeid, averagervp)
select monthid, fueltypeid, coalesce(sum(rvp*marketshare),0.0) as averagervp
from refuelingfuelsupply
inner join refuelingfuelformulation on refuelingfuelformulation.fuelformulationid=refuelingfuelsupply.fuelformulationid
inner join refuelingmonthofanyyear on refuelingmonthofanyyear.monthgroupid=refuelingfuelsupply.monthgroupid
inner join refuelingfuelsubtype on refuelingfuelsubtype.fuelsubtypeid=refuelingfuelformulation.fuelsubtypeid
group by monthid, fueltypeid;

analyze table refuelingaveragervp;

-- @algorithm provide default averagervp of 0.
insert ignore into refuelingaveragervp (monthid, fueltypeid, averagervp)
select monthid, fueltypeid, 0.0 as averagervp
from refuelingfactors, refuelingmonthofanyyear;

analyze table refuelingaveragervp;

-- @algorithm displacedvaporrate = exp(vaporterma + vaportermb * tanktemperaturedif + vaportermc * refuelingtemperature + vaportermd * averagervp)
update refuelingtemp, refuelingfactors, refuelingaveragervp
set displacedvaporrate = exp(vaporterma
	+ vaportermb * tanktemperaturedif
	+ vaportermc * refuelingtemperature
	+ vaportermd * averagervp)
where refuelingfactors.fueltypeid=refuelingtemp.fueltypeid
and refuelingaveragervp.fueltypeid=refuelingtemp.fueltypeid
and refuelingaveragervp.monthid=refuelingtemp.monthid;

-- @algorithm limit displacedvaporrate to no less than minimumrefuelingvaporloss.
update refuelingtemp, refuelingfactors
set displacedvaporrate = case when minimumrefuelingvaporloss <= -1 then 0 else minimumrefuelingvaporloss end
where refuelingfactors.fueltypeid=refuelingtemp.fueltypeid
and (displacedvaporrate < minimumrefuelingvaporloss or minimumrefuelingvaporloss <= -1);

-- --------------------------------------------------------------
-- refec-3: technology adjustment of the refueling displacement vapor loss rate
-- refec-4: program adjustment of the refueling displacement vapor loss rate
-- --------------------------------------------------------------
drop table if exists refuelingdisplacement;
create table refuelingdisplacement (
	fueltypeid smallint(6) not null,
	sourcetypeid smallint(6) not null,
	modelyearid smallint(6) not null,
	monthid smallint(6) not null,
	hourid smallint(6) not null,

	adjustedvaporrate float not null default 0.0,

	key(fueltypeid),
	key(sourcetypeid),
	key(modelyearid),
	key(monthid),
	key(hourid),
	unique index xpkrefuelingdisplacement (fueltypeid, sourcetypeid, modelyearid, monthid, hourid)
);

-- @algorithm technology and program adjustment of the refueling displacement vapor loss rate.
-- adjustedvaporrate = (1.0-refuelingvaporprogramadjust)*((1.0-refuelingtechadjustment)*displacedvaporrate).
-- @condition refueling displacement vapor loss (18).
insert into refuelingdisplacement (fueltypeid, sourcetypeid, modelyearid, monthid, hourid, adjustedvaporrate)
select fueltypeid, sourcetypeid, modelyearid, monthid, hourid,
	(1.0-refuelingvaporprogramadjust)*((1.0-refuelingtechadjustment)*displacedvaporrate) as adjustedvaporrate
from refuelingcountyyear, sourcetypetechadjustment, refuelingtemp
where sourcetypetechadjustment.processid=18;

-- end section refuelingdisplacementvaporloss

-- section refuelingspillageloss
-- --------------------------------------------------------------
-- refec-5: technology adjustment of the refueling spillage rate
-- refec-6: program adjustment of the refueling spillage rate
-- --------------------------------------------------------------
drop table if exists refuelingspillage;
create table refuelingspillage (
	fueltypeid smallint(6) not null,
	sourcetypeid smallint(6) not null,
	modelyearid smallint(6) not null,

	adjustedspillrate float not null default 0.0,

	key(fueltypeid),
	key(sourcetypeid),
	key(modelyearid),
	unique index xpkrefuelingspillage (fueltypeid, sourcetypeid, modelyearid)
);

-- @algorithm technology and program adjustment of the refueling spillage rate.
-- adjustedspillrate = (1.0-refuelingspillprogramadjust)*((1.0-refuelingtechadjustment)*refuelingspillrate).
-- @condition refueling spillage loss (19).
insert into refuelingspillage (fueltypeid, sourcetypeid, modelyearid, adjustedspillrate)
select fueltypeid, sourcetypeid, modelyearid, 
	(1.0-refuelingspillprogramadjust)*((1.0-refuelingtechadjustment)*refuelingspillrate) as adjustedspillrate
from refuelingcountyyear, sourcetypetechadjustment, refuelingfactors
where sourcetypetechadjustment.processid=19;

-- end section refuelingspillageloss

-- --------------------------------------------------------------
-- refec-7: calculate total fuel consumption from total energy
-- refec-8: refueling loss emission results
-- --------------------------------------------------------------
drop table if exists refuelingworkeroutputtemp;
create table refuelingworkeroutputtemp (
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
	emissionquant        double null,
	emissionrate		 double null
);

-- section refuelingdisplacementvaporloss
truncate refuelingworkeroutputtemp;

-- @algorithm emissions = (adjustedvaporrate * total energy consumption (91)) / (energycontent * fueldensity).
-- @condition refueling displacement vapor loss (18).
insert into refuelingworkeroutputtemp (
	yearid, monthid, dayid, hourid, stateid, countyid, zoneid, linkid, pollutantid, processid,
	sourcetypeid, regclassid, fueltypeid, modelyearid, roadtypeid, scc, emissionquant, emissionrate
)
select
	mwo.yearid, mwo.monthid, mwo.dayid, mwo.hourid, mwo.stateid, mwo.countyid, mwo.zoneid, mwo.linkid, 0 as pollutantid, 18 as processid,
	mwo.sourcetypeid, mwo.regclassid, mwo.fueltypeid, mwo.modelyearid, mwo.roadtypeid, 
	null as scc,
	(adjustedvaporrate * mwo.emissionquant / (energycontent * fueldensity)) as emissionquant,
	(adjustedvaporrate * mwo.emissionrate  / (energycontent * fueldensity)) as emissionrate
from movesworkeroutput mwo
inner join refuelingfueltype rft on (rft.fueltypeid=mwo.fueltypeid and rft.monthid=mwo.monthid)
inner join refuelingdisplacement rd on (
	rd.fueltypeid=rft.fueltypeid and rd.sourcetypeid=mwo.sourcetypeid and rd.modelyearid=mwo.modelyearid
	and rd.monthid=mwo.monthid and rd.hourid=mwo.hourid)
where mwo.processid in (1,2,90,91)
and mwo.pollutantid=91;

insert into movesworkeroutput (
	yearid, monthid, dayid, hourid, stateid, countyid, zoneid, linkid, pollutantid, processid,
	sourcetypeid, regclassid, fueltypeid, modelyearid, roadtypeid, scc, emissionquant, emissionrate
)
select
	yearid, monthid, dayid, hourid, stateid, countyid, zoneid, linkid, rdp.pollutantid, processid,
	sourcetypeid, regclassid, fueltypeid, modelyearid, roadtypeid, scc, emissionquant, emissionrate
from refuelingworkeroutputtemp, refuelingdisplacementpollutant rdp;
-- end section refuelingdisplacementvaporloss

-- section refuelingspillageloss
truncate refuelingworkeroutputtemp;

-- @algorithm emissions = (adjustedspillrate * total energy consumption (91)) / (energycontent * fueldensity).
-- @condition refueling spillage loss (19).
insert into refuelingworkeroutputtemp (
	yearid, monthid, dayid, hourid, stateid, countyid, zoneid, linkid, pollutantid, processid,
	sourcetypeid, regclassid, fueltypeid, modelyearid, roadtypeid, scc, emissionquant, emissionrate
)
select
	mwo.yearid, mwo.monthid, mwo.dayid, mwo.hourid, mwo.stateid, mwo.countyid, mwo.zoneid, mwo.linkid, 0 as pollutantid, 19 as processid,
	mwo.sourcetypeid, mwo.regclassid, mwo.fueltypeid, mwo.modelyearid, mwo.roadtypeid, 
	null as scc,
	(adjustedspillrate * mwo.emissionquant / (energycontent * fueldensity)) as emissionquant,
	(adjustedspillrate * mwo.emissionrate  / (energycontent * fueldensity)) as emissionrate
from movesworkeroutput mwo
inner join refuelingfueltype rft on (rft.fueltypeid=mwo.fueltypeid and rft.monthid=mwo.monthid)
inner join refuelingspillage rs on (
	rs.fueltypeid=rft.fueltypeid and rs.sourcetypeid=mwo.sourcetypeid and rs.modelyearid=mwo.modelyearid)
where mwo.processid in (1,2,90,91)
and mwo.pollutantid=91;

insert into movesworkeroutput (
	yearid, monthid, dayid, hourid, stateid, countyid, zoneid, linkid, pollutantid, processid,
	sourcetypeid, regclassid, fueltypeid, modelyearid, roadtypeid, scc, emissionquant, emissionrate
)
select
	yearid, monthid, dayid, hourid, stateid, countyid, zoneid, linkid, rsp.pollutantid, processid,
	sourcetypeid, regclassid, fueltypeid, modelyearid, roadtypeid, scc, emissionquant, emissionrate
from refuelingworkeroutputtemp, refuelingspillagepollutant rsp;
-- end section refuelingspillageloss

-- end section processing

-- section cleanup
drop table if exists refuelingrunspechour;
drop table if exists refuelingrunspecmonth;
drop table if exists refuelingcountyyear;
drop table if exists refuelingfueltype;

-- section refuelingdisplacementvaporloss
drop table if exists refuelingtemp;
drop table if exists refuelingaveragervp;
drop table if exists refuelingdisplacement;
drop table if exists refuelingdisplacementpollutant;
-- end section refuelingdisplacementvaporloss

-- section refuelingspillageloss
drop table if exists refuelingspillage;
drop table if exists refuelingspillagepollutant;
-- end section refuelingspillageloss

drop table if exists refuelingworkeroutputtemp;
-- end section cleanup
