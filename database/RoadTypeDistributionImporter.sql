-- version 2008-10-28

-- ensure distributions sum to 1.0 for all sourcetypeid combinations. added a check for nulls,
-- because otherwise it could try to evaluate null <> 1.0000, which would not result in any rows being identified.
drop table if exists tempnotunity;

create table tempnotunity
select sourcetypeid, sum(ifnull(roadtypevmtfraction, 0)) as sumroadtypevmtfraction
from roadtypedistribution
group by sourcetypeid
having round(sum(ifnull(roadtypevmtfraction, 0)),4) <> 1.0000;

insert into importtempmessages (message)
select concat('error: source ',sourcetypeid,' roadtypevmtfraction sum is not 1.0 but instead ',round(sumroadtypevmtfraction,4))
from tempnotunity;

drop table if exists tempnotunity;
