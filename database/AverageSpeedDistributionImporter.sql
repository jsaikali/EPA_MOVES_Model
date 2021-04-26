-- version 2008-10-28

-- ensure distributions sum to 1.0 for all sourcetypeid, roadtypeid, hourdayid combinations.
drop table if exists tempnotunity;

create table tempnotunity
select sourcetypeid, roadtypeid, hourdayid, sum(avgspeedfraction) as sumavgspeedfraction
from avgspeeddistribution
group by sourcetypeid, roadtypeid, hourdayid
having round(sum(avgspeedfraction),4) <> 1.0000;

insert into importtempmessages (message)
select concat('error: source ',sourcetypeid,', road ',roadtypeid,', hour/day ',hourdayid,' avgspeedfraction sum is not 1.0 but instead ',round(sumavgspeedfraction,4))
from tempnotunity;

drop table if exists tempnotunity;
