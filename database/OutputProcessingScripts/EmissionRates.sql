-- this mysql script produces an output table which reports your
-- onroad emission results in units of mass per distance.  this is done
-- by joining the activity table with the inventory output results.
-- version 20091191 djb.
-- updated 20150602 kjr.
-- the mysql table produced is called: movesrates
-- this script requires that users check the "distance traveled"
-- check box general output panel of the moves graphical user
-- interface.  users must also select
-- the inventory calculation type in the scale panel.
-- **************************************************************
-- only onroad emission rates will be calculated. (no nonroad)
-- the user *must* select distance traveled.
-- the user *must* select inventory calculation type.
-- **************************************************************
--
--
--
--  create outputtemp   from movesoutput,
--  create activitytemp from movesactivityoutput,
--
--  update to zero 15 fields of outputtemp,
--  update to zero 15 fields of activitytemp,
--
--  create distancetemp sum from activitytemp,
--
--  insert sum into activitytemp from distancetemp (roadtype = 0 )
--    "      "   "        "        "      "        (roadtype = 1 )
--
--  add master key to outputtemp,
--  add master key to activitytemp,
--
--  join into movesrates from outputtemp,
--                            activitytemp,
--                            using master key,
--                            and activitytype = 1
--                           (with rate calculation)
--
-- drop table outputtemp,
-- drop table activitytemp,
-- drop distancetemp.
--
--

flush tables ;

-- create the table to hold the calculation results.
drop table if exists movesrates;
create table `movesrates` (
  `masterkey`   char(60)                default null,
  `movesrunid`  smallint(5) unsigned        not null,  -- mas key
  `iterationid` smallint(5) unsigned    default  '1',  -- mas key
  `yearid`      smallint(5) unsigned    default null,  -- mas key
  `monthid`     smallint(5) unsigned    default null,  -- mas key
  `dayid`       smallint(5) unsigned    default null,  -- mas key
  `hourid`      smallint(5) unsigned    default null,  -- mas key
  `stateid`     smallint(5) unsigned    default null,  -- mas key
  `countyid`    int(10)     unsigned    default null,  -- mas key
  `zoneid`      int(10)     unsigned    default null,  -- mas key
  `linkid`      int(10)     unsigned    default null,  -- mas key
  `pollutantid` smallint(5) unsigned    default null,
  `processid`      smallint(5) unsigned default null,
  `sourcetypeid`   smallint(5) unsigned default null,  -- mas key
  `regclassid`     smallint(5) unsigned default null,  -- mas key
  `fueltypeid`     smallint(5) unsigned default null,  -- mas key
  `modelyearid`    smallint(5) unsigned default null,  -- mas key
  `roadtypeid`     smallint(5) unsigned default null,  -- mas key
  `scc`            char(10)             default null,  -- mas key
  `emissionquant`  double               default null,
  `activitytypeid` smallint(6)              not null,
  `activity`       double               default null,
  `emissionrate`   double               default null,
  `massunits`      char(5)              default null,
  `distanceunits`  char(5)              default null
) engine=myisam default charset=latin1 delay_key_write=1
 ;

-- check to see that there is activity output in the table.

-- create copies of the results tables.
drop   table if exists outputtemp ;
create table outputtemp select * from movesoutput ;


drop table if exists activitytemp ;
create table activitytemp select * from movesactivityoutput;



-- eliminate any null values. null values prevent joining of the tables.
update outputtemp   set movesrunid  =0 where isnull(movesrunid) ;
update outputtemp   set iterationid =0 where isnull(iterationid) ;
update outputtemp   set yearid      =0 where isnull(yearid) ;
update outputtemp   set monthid     =0 where isnull(monthid) ;
update outputtemp   set dayid       =0 where isnull(dayid) ;
update outputtemp   set hourid      =0 where isnull(hourid) ;
update outputtemp   set stateid     =0 where isnull(stateid) ;
update outputtemp   set countyid    =0 where isnull(countyid) ;
update outputtemp   set zoneid      =0 where isnull(zoneid) ;
update outputtemp   set linkid      =0 where isnull(linkid) ;
update outputtemp   set sourcetypeid=0 where isnull(sourcetypeid) ;
update outputtemp   set regclassid  =0 where isnull(regclassid  ) ;
update outputtemp   set fueltypeid  =0 where isnull(fueltypeid) ;
update outputtemp   set modelyearid =0 where isnull(modelyearid) ;
update outputtemp   set roadtypeid  =0 where isnull(roadtypeid) ;
update outputtemp   set scc         =0 where isnull(scc) ;

update activitytemp set movesrunid  =0 where isnull(movesrunid) ;
update activitytemp set iterationid =0 where isnull(iterationid) ;
update activitytemp set yearid      =0 where isnull(yearid) ;
update activitytemp set monthid     =0 where isnull(monthid) ;
update activitytemp set dayid       =0 where isnull(dayid) ;
update activitytemp set hourid      =0 where isnull(hourid) ;
update activitytemp set stateid     =0 where isnull(stateid) ;
update activitytemp set countyid    =0 where isnull(countyid) ;
update activitytemp set zoneid      =0 where isnull(zoneid) ;
update activitytemp set linkid      =0 where isnull(linkid) ;
update activitytemp set sourcetypeid=0 where isnull(sourcetypeid) ;
update activitytemp set regclassid  =0 where isnull(regclassid  ) ;
update activitytemp set fueltypeid  =0 where isnull(fueltypeid) ;
update activitytemp set modelyearid =0 where isnull(modelyearid) ;
update activitytemp set roadtypeid  =0 where isnull(roadtypeid) ;
update activitytemp set scc         =0 where isnull(scc) ;
update activitytemp set scc         =0 where scc="NOTHING" ;

-- alter the scc values to eliminate the text suffix.
update outputtemp   set scc=concat(mid(scc,1,9),"0");
update activitytemp set scc=concat(mid(scc,1,9),"0");

-- create a table with the distance summed over road type.
drop table if exists distancetemp;
create table distancetemp
select
	a.movesrunid,
	a.iterationid,
	a.yearid,
	a.monthid,

	a.dayid,
	a.hourid,
	a.stateid,
	a.countyid,

	a.zoneid,
	a.linkid,
	a.sourcetypeid,
  a.regclassid,

	a.fueltypeid,
	a.modelyearid,
	concat(mid(a.scc,1,7),"000") as scc,
	a.activitytypeid,

	sum(a.activity) as activitysum

from	activitytemp as a
group by
	a.movesrunid,
	a.iterationid,
	a.yearid,
	a.monthid,

	a.dayid,
	a.hourid,
	a.stateid,
	a.countyid,

	a.zoneid,
	a.linkid,
	a.sourcetypeid,
  a.regclassid,

	a.fueltypeid,
	a.modelyearid,
	concat(mid(a.scc,1,7),"000"),
	a.activitytypeid;


-- set the distance for roadtypeid=1 to be the distance sum.
-- scc case: roadtypeid=1 and scc=scc with 00 road type.
-- records without scc will also be added, but will not match
-- with the emission output records and will be ignored.


-- updated for moves 2014:
--   (for user having selected scc output):
--   include roadtypeid,
--   and used new scc definition.

insert into activitytemp (
	movesrunid,
	iterationid,
	yearid,
	monthid,
	dayid,
	hourid,
	stateid,
	countyid,
	zoneid,
	linkid,
	sourcetypeid,
  regclassid,
	fueltypeid,
	modelyearid,
	roadtypeid,
	scc,
	activitytypeid,
	activity,
	activitymean,
	activitysigma )
select
	movesrunid,
	iterationid,
	yearid,
	monthid,
	dayid,
	hourid,
	stateid,
	countyid,
	zoneid,
	linkid,
	sourcetypeid,
  regclassid,
	fueltypeid,
	modelyearid,
	1 as roadtypeid,                     -- for moves 2014 scc now includes roadtypeid
	concat(mid(scc,1,6),'0100') as scc,  -- for moves 2014 (new) scc
	activitytypeid,
	activitysum as activity,
	0.0 as activitymean,
	0.0 as activitysigma
from	distancetemp;


-- set the distance for roadtypeid=1 to be the distance sum.
-- source type case: roadtypeid=1 and scc='00'.
-- where scc is not selected

insert into activitytemp (
	movesrunid,
	iterationid,
	yearid,
	monthid,
	dayid,
	hourid,
	stateid,
	countyid,
	zoneid,
	linkid,
	sourcetypeid,
  regclassid,
	fueltypeid,
	modelyearid,
	roadtypeid,
	scc,
	activitytypeid,
	activity,
	activitymean,
	activitysigma )
select
	movesrunid,
	iterationid,
	yearid,
	monthid,
	dayid,
	hourid,
	stateid,
	countyid,
	zoneid,
	linkid,
	sourcetypeid,
  regclassid,
	fueltypeid,
	modelyearid,
	1 as roadtypeid,
	'00' as scc,         -- moves 2014 uses '00' for no scc usage
	activitytypeid,
	activitysum as activity,
	0.0 as activitymean,
	0.0 as activitysigma
from	distancetemp;


-- add master keys to each table for joining.
alter table outputtemp add masterkey char(60) default null ;
update outputtemp set masterkey=concat_ws(",",
	movesrunid,
	iterationid,
	yearid,
	monthid,
	dayid,
	hourid,
	stateid,
	countyid,
	zoneid,
	linkid,
	sourcetypeid,
  regclassid,
	fueltypeid,
	modelyearid,
	roadtypeid,
	mid(scc,1,8) );



create index index1 on outputtemp (masterkey) ;

alter table activitytemp add masterkey char(60) default null ;
update activitytemp set masterkey=concat_ws(",",
	movesrunid,
	iterationid,
	yearid,
	monthid,
	dayid,
	hourid,
	stateid,
	countyid,
	zoneid,
	linkid,
	sourcetypeid,
  regclassid,
	fueltypeid,
	modelyearid,
	roadtypeid,
	mid(scc,1,8) );

create index index1 on activitytemp (masterkey) ;


-- join the tables.
truncate movesrates;
insert into movesrates (
	masterkey,
	movesrunid,
	iterationid,
	yearid,
	monthid,
	dayid,
	hourid,
	stateid,
	countyid,
	zoneid,
	linkid,
	pollutantid,
	processid,
	sourcetypeid,
  regclassid,
	fueltypeid,
	modelyearid,
	roadtypeid,
	scc,
	emissionquant,
	activitytypeid,
	activity,
	emissionrate )
select
	a.masterkey,
	a.movesrunid,
	a.iterationid,
	a.yearid,
	a.monthid,
	a.dayid,
	a.hourid,
	a.stateid,
	a.countyid,
	a.zoneid,
	a.linkid,
	a.pollutantid,
	a.processid,
	a.sourcetypeid,
  a.regclassid,
	a.fueltypeid,
	a.modelyearid,
	a.roadtypeid,
	a.scc,
	a.emissionquant,
	b.activitytypeid,
	b.activity,
	(a.emissionquant/b.activity) as emissionrate
from
	outputtemp as a,
	activitytemp as b
where
	a.masterkey = b.masterkey
and b.activitytypeid = 1;




-- eliminate the temporary tables.
drop table outputtemp ;
drop table activitytemp ;
drop table distancetemp ;

-- add the units to the table.
update movesrates, movesrun set
	movesrates.distanceunits=movesrun.distanceunits,
	movesrates.massunits=movesrun.massunits
where
	movesrates.movesrunid=movesrun.movesrunid;

flush tables;