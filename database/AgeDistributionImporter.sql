-- author wesley faler
-- version 2016-10-04

-- mark any years in sourcetypeagedistribution as base years in the year table

drop table if exists tempnewyear;

create table if not exists tempnewyear (
  yearid smallint(6) not null default '0',
  primary key  (yearid)
);

insert into tempnewyear (yearid)
select distinct yearid
from sourcetypeagedistribution;

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

-- complain about any years outside of moves's range
insert into importtempmessages (message)
select distinct concat('error: year ',yearid,' is outside the range of 1990-2060 and cannot be used') as errormessage
from sourcetypeagedistribution
where yearid < 1990 or yearid > 2060;

-- ensure distributions sum to 1.0 for all sourcetypeid, yearid combinations.
drop table if exists tempnotunity;

create table tempnotunity
select sourcetypeid, yearid, sum(agefraction) as sumagefraction
from sourcetypeagedistribution
group by sourcetypeid, yearid
having round(sum(agefraction),4) <> 1.0000;

insert into importtempmessages (message)
select concat('error: source ',sourcetypeid,', year ',yearid,' agefraction sum is not 1.0 but instead ',round(sumagefraction,4))
from tempnotunity;

drop table if exists tempnotunity;
