-- author wesley faler
-- version 2016-10-04

-- mark any years in hpmsvtypeyear as base years in the year table

drop table if exists tempnewyear;

create table if not exists tempnewyear (
  yearid smallint(6) not null default '0',
  primary key  (yearid)
);

insert into tempnewyear (yearid)
select distinct yearid
from hpmsvtypeyear;

drop table if exists tempyear;

create table if not exists tempyear (
  yearid smallint(6) not null default '0',
  isbaseyear char(1) default null,
  fuelyearid smallint(6) not null default '0',
  primary key  (yearid),
  key isbaseyear (isbaseyear)
);

create table if not exists year (
  yearid smallint(6) not null default '0',
  isbaseyear char(1) default null,
  fuelyearid smallint(6) not null default '0',
  primary key  (yearid),
  key isbaseyear (isbaseyear)
);

insert into tempyear (yearid, isbaseyear, fuelyearid)
select y.yearid, 'Y' as isbaseyear, y.fuelyearid
from tempnewyear ny
inner join ##defaultdatabase##.year y on (y.yearid=ny.yearid);

-- insert ignore into year (yearid, isbaseyear, fuelyearid)
-- select yearid, isbaseyear, fuelyearid
-- from tempyear

update year, tempnewyear set year.isbaseyear='Y'
where year.yearid=tempnewyear.yearid;

drop table if exists tempyear;
drop table if exists tempnewyear;

-- set vmtgrowthfactor to 0 instead of null
update hpmsvtypeyear set vmtgrowthfactor=0 where vmtgrowthfactor is null;

-- complain about any years outside of moves's range
insert into importtempmessages (message)
select distinct concat('error: year ',yearid,' in hpmsvtypeyear is outside the range of 1990-2060 and cannot be used') as errormessage
from hpmsvtypeyear
where yearid < 1990 or yearid > 2060;

insert into importtempmessages (message)
select distinct concat('error: year ',yearid,' in hpmsvtypeday is outside the range of 1990-2060 and cannot be used') as errormessage
from hpmsvtypeday
where yearid < 1990 or yearid > 2060;

insert into importtempmessages (message)
select distinct concat('error: year ',yearid,' in sourcetypeyearvmt is outside the range of 1990-2060 and cannot be used') as errormessage
from sourcetypeyearvmt
where yearid < 1990 or yearid > 2060;

insert into importtempmessages (message)
select distinct concat('error: year ',yearid,' in sourcetypedayvmt is outside the range of 1990-2060 and cannot be used') as errormessage
from sourcetypedayvmt
where yearid < 1990 or yearid > 2060;

-- monthvmtfraction
-- fill with 0's for entries that were not imported
insert ignore into monthvmtfraction (sourcetypeid, monthid, monthvmtfraction)
select sourcetypeid, monthid, 0.0
from ##defaultdatabase##.sourceusetype, ##defaultdatabase##.monthofanyyear
where (select count(*) from monthvmtfraction where monthvmtfraction > 0) > 0;

-- check sum to 1
insert into importtempmessages (message)
select distinct concat('error: source type ',sourcetypeid,' monthvmtfraction is greater than 1.0') as errormessage
from monthvmtfraction
group by sourcetypeid
having round(sum(monthvmtfraction),4)>1.0000;

-- for non-zero fractions supplied, make sure they sum to 1
insert into importtempmessages (message)
select distinct concat('warning: source type ',sourcetypeid,' monthvmtfraction is less than 1.0') as errormessage
from monthvmtfraction
group by sourcetypeid
having round(sum(monthvmtfraction),4)<1.0000 and sum(monthvmtfraction)>0.0000;


-- dayvmtfraction
-- fill with 0's for entries that were not imported
insert ignore into dayvmtfraction (sourcetypeid, monthid, roadtypeid, dayid, dayvmtfraction)
select sourcetypeid, monthid, roadtypeid, dayid, 0.0
from ##defaultdatabase##.sourceusetype, ##defaultdatabase##.monthofanyyear,
  ##defaultdatabase##.roadtype, ##defaultdatabase##.dayofanyweek
where (select count(*) from dayvmtfraction where dayvmtfraction > 0) > 0;

-- check sum to 1
insert into importtempmessages (message)
select distinct concat('error: source type ',sourcetypeid,', month ',monthid,', road type ',roadtypeid,' dayvmtfraction is greater than 1.0') as errormessage
from dayvmtfraction
group by sourcetypeid, monthid, roadtypeid
having round(sum(dayvmtfraction),4)>1.0000;

-- for non-zero fractions supplied, make sure they sum to 1
insert into importtempmessages (message)
select distinct concat('warning: source type ',sourcetypeid,', month ',monthid,', road type ',roadtypeid,' dayvmtfraction is less than 1.0') as errormessage
from dayvmtfraction
group by sourcetypeid, monthid, roadtypeid
having round(sum(dayvmtfraction),4)<1.0000 and sum(dayvmtfraction)>0.0000;


-- hourvmtfraction
-- fill with 0's for entries that were not imported
insert ignore into hourvmtfraction (sourcetypeid, roadtypeid, dayid, hourid, hourvmtfraction)
select sourcetypeid, roadtypeid, dayid, hourid, 0.0
from ##defaultdatabase##.sourceusetype, ##defaultdatabase##.roadtype, ##defaultdatabase##.hourday
where (select count(*) from hourvmtfraction where hourvmtfraction > 0) > 0;

-- check sum to 1
insert into importtempmessages (message)
select distinct concat('error: source type ',sourcetypeid,', day ',dayid,', road type ',roadtypeid,' hourvmtfraction is greater than 1.0') as errormessage
from hourvmtfraction
group by sourcetypeid, dayid, roadtypeid
having round(sum(hourvmtfraction),4)>1.0000;

-- for non-zero fractions supplied, make sure they sum to 1
insert into importtempmessages (message)
select distinct concat('warning: source type ',sourcetypeid,', day ',dayid,', road type ',roadtypeid,' hourvmtfraction is less than 1.0') as errormessage
from hourvmtfraction
group by sourcetypeid, dayid, roadtypeid
having round(sum(hourvmtfraction),4)<1.0000 and sum(hourvmtfraction)>0.0000;
