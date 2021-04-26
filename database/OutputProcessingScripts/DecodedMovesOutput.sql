-- moves post-processing mysql script
-- decodes most key fields of movesoutput and movesactivityoutput tables
-- creating tables decodedmovesoutput and decodedmovesactivityoutput
--   that contain the additional descriptive character fields.
-- this version written 9/22/2005
-- this version rewritten 2/12/2015 to include additional fields.
-- updated 5/20/2015 to restore output for activity.
-- updated 9/10/2015 to add fuelsubtype.

--
--
-- uses text replacement of moves where ##defaultdb## is replaced with the current default database,
-- to serve as a source of the relevant category tables listing legal values.  updated 6/20/2014

--
--
-- decodemovesoutput.sql

flush tables;
select current_time;

--
-- make decodedmovesactivityoutput
--

drop table if exists       decodedmovesactivityoutput;
create table if not exists decodedmovesactivityoutput
select movesrunid,
       iterationid,
       yearid,
       monthid,
       dayid,
       cast(' ' as char( 10)) as dayname,
       hourid,
       stateid,
       cast(' ' as char(  2)) as stateabbr,
       countyid,
       cast(' ' as char( 45)) as countyname,
       zoneid,
       linkid,
       sourcetypeid,
       cast(' ' as char( 30)) as sourcetypename,
       regclassid,
       cast(' ' as char(100)) as regclassname,
       fueltypeid,
       cast(' ' as char( 30)) as fueltypedesc,
       modelyearid,
       roadtypeid,
       cast(' ' as char( 25)) as roaddesc,
       scc,
       engtechid,
       cast(' ' as char( 50)) as engtechname,
       sectorid,
       cast(' ' as char( 40)) as sectordesc,
       hpid,
       activitytypeid,
       cast(' ' as char( 50)) as activitytypedesc,
       activity,
       activitymean,
       activitysigma
from   movesactivityoutput;

update decodedmovesactivityoutput as a set dayname        = (select b.dayname
                                                             from   ##defaultdb##.dayofanyweek as b
                                                             where  a.dayid = b.dayid);

update decodedmovesactivityoutput as a set stateabbr      = (select b.stateabbr
                                                             from   ##defaultdb##.state as b
                                                             where  a.stateid = b.stateid);

update decodedmovesactivityoutput as a set countyname     = (select b.countyname
                                                             from   ##defaultdb##.county as b
                                                             where  a.countyid = b.countyid);

update decodedmovesactivityoutput as a set sourcetypename = (select b.sourcetypename
                                                             from   ##defaultdb##.sourceusetype as b
                                                             where  a.sourcetypeid = b.sourcetypeid);

update decodedmovesactivityoutput as a set regclassname   = (select b.regclassname
                                                              from  ##defaultdb##.regulatoryclass as b
                                                              where a.regclassid = b.regclassid);

update decodedmovesactivityoutput as a set fueltypedesc   = (select b.fueltypedesc
                                                             from   ##defaultdb##.fueltype as b
                                                             where  a.fueltypeid = b.fueltypeid);


update decodedmovesactivityoutput as a set roaddesc       = (select b.roaddesc
                                                             from   ##defaultdb##.roadtype as b
                                                             where  a.roadtypeid = b.roadtypeid);

update decodedmovesactivityoutput as a set engtechname    = (select b.engtechname
                                                             from   ##defaultdb##.enginetech as b
                                                             where  a.engtechid = b.engtechid);

update decodedmovesactivityoutput as a set sectordesc     = (select b.description
                                                             from   ##defaultdb##.sector as b
                                                             where  a.sectorid = b.sectorid);

update decodedmovesactivityoutput as a set activitytypedesc
                                                          = (select b.activitytypedesc
                                                             from   .activitytype as b
                                                             where  a.activitytypeid = b.activitytypeid);

-- select * from decodedmovesactivityoutput;



--
-- make decodedmovesoutput table
--

drop   table if     exists decodedmovesoutput;
create table if not exists decodedmovesoutput
select movesrunid,
       iterationid,
       yearid,
       monthid,
       dayid,
       cast(' ' as char(10))  as dayname,
       hourid,
       stateid,
       cast(' ' as char(  2)) as stateabbr,
       countyid,
       cast(' ' as char( 45)) as countyname,
       zoneid,
       linkid,
       pollutantid,
       cast(' ' as char( 50)) as pollutantname,
       processid,
       cast(' ' as char( 50)) as processname,
       sourcetypeid,
       cast(' ' as char( 30)) as sourcetypename,
       regclassid,
       cast(' ' as char(100)) as regclassname,
       fueltypeid,
       cast(' ' as char( 30)) as fueltypedesc,
       fuelsubtypeid,
       cast(' ' as char(50))  as fuelsubtypedesc,
       modelyearid,
       roadtypeid,
       cast(' ' as char( 25)) as roaddesc,
       scc,
       engtechid,
       cast(' ' as char( 50)) as engtechname,
       sectorid,
       cast(' ' as char( 40)) as sectordesc,
       hpid,
       emissionquant,
       emissionquantmean,
       emissionquantsigma
from   movesoutput;

update decodedmovesoutput as a set dayname        = (select b.dayname
                                                     from   ##defaultdb##.dayofanyweek as b
                                                     where  a.dayid = b.dayid);

update decodedmovesoutput as a set stateabbr      = (select b.stateabbr
                                                     from   ##defaultdb##.state as b
                                                             where  a.stateid = b.stateid);

update decodedmovesoutput as a set countyname     = (select b.countyname
                                                     from   ##defaultdb##.county as b
                                                     where  a.countyid = b.countyid);

update decodedmovesoutput as a set pollutantname  = (select b.pollutantname
                                                     from   ##defaultdb##.pollutant as b
                                                     where  a.pollutantid = b.pollutantid);

update decodedmovesoutput as a set processname    = (select b.processname
                                                     from   ##defaultdb##.emissionprocess as b
                                                     where  a.processid = b.processid);

update decodedmovesoutput as a set sourcetypename = (select b.sourcetypename
                                                     from   ##defaultdb##.sourceusetype as b
                                                     where  a.sourcetypeid = b.sourcetypeid);

update decodedmovesoutput as a set regclassname   = (select b.regclassname
                                                     from  ##defaultdb##.regulatoryclass as b
                                                     where a.regclassid = b.regclassid);

update decodedmovesoutput as a set fueltypedesc   = (select b.fueltypedesc
                                                     from   ##defaultdb##.fueltype as b
                                                     where  a.fueltypeid = b.fueltypeid);

update decodedmovesoutput as a set fuelsubtypedesc =(select b.fuelsubtypedesc
                                                     from   ##defaultdb##.fuelsubtype as b
                                                     where  a.fuelsubtypeid = b.fuelsubtypeid);

update decodedmovesoutput as a set roaddesc       = (select b.roaddesc
                                                     from   ##defaultdb##.roadtype as b
                                                     where  a.roadtypeid = b.roadtypeid);

update decodedmovesoutput as a set engtechname    = (select b.engtechname
                                                     from   ##defaultdb##.enginetech as b
                                                     where  a.engtechid = b.engtechid);

update decodedmovesoutput as a set sectordesc     = (select b.description
                                                     from   ##defaultdb##.sector as b
                                                     where  a.sectorid = b.sectorid);



-- select * from decodedmovesoutput;

flush tables;