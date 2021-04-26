-- version 2008-03-29

-- section create remote tables for extracted data

-- end section create remote tables for extracted data

-- section extract data

-- end section extract data

-- section processing

drop table if exists pmtotalmovesworkeroutputtemp;
create table if not exists pmtotalmovesworkeroutputtemp (
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
    fueltypeid           smallint unsigned null,
    modelyearid          smallint unsigned null,
    roadtypeid           smallint unsigned null,
    scc                  char(10) null,
    emissionquant        float null
);

-- section pm10total

insert into pmtotalmovesworkeroutputtemp (
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
    fueltypeid,
    modelyearid,
    roadtypeid,
    scc,
    emissionquant)
select
    yearid,
    monthid,
    dayid,
    hourid,
    stateid,
    countyid,
    zoneid,
    linkid,
    100 as pollutantid,
    processid,
    sourcetypeid,
    fueltypeid,
    modelyearid,
    roadtypeid,
    scc,
    emissionquant
from movesworkeroutput mwo
where mwo.pollutantid in (101,102,105);

-- end section pm10total

-- section pm25total

insert into pmtotalmovesworkeroutputtemp (
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
    fueltypeid,
    modelyearid,
    roadtypeid,
    scc,
    emissionquant)
select
    yearid,
    monthid,
    dayid,
    hourid,
    stateid,
    countyid,
    zoneid,
    linkid,
    110 as pollutantid,
    processid,
    sourcetypeid,
    fueltypeid,
    modelyearid,
    roadtypeid,
    scc,
    emissionquant
from movesworkeroutput mwo
where mwo.pollutantid in (111,112,115);

-- end section pm25total

insert into movesworkeroutput (
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
    fueltypeid,
    modelyearid,
    roadtypeid,
    scc,
    emissionquant)
select
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
    fueltypeid,
    modelyearid,
    roadtypeid,
    scc,
    emissionquant
from pmtotalmovesworkeroutputtemp;

-- end section processing

-- section cleanup
drop table if exists pmtotalmovesworkeroutputtemp;
-- end section cleanup
