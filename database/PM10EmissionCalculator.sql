-- author wesley faler
-- version 2013-09-23

-- @algorithm
-- @owner pm10 emission calculator
-- @calculator

-- section create remote tables for extracted data
##create.pm10emissionratio##;
truncate table pm10emissionratio;

create table if not exists pm10pollutantprocessassoc (
       polprocessid         int not null,
       processid            smallint not null,
       pollutantid          smallint not null,
       isaffectedbyexhaustim char(1) not null default 'N',
       isaffectedbyevapim char(1) not null default 'N',
       primary key (polprocessid, processid, pollutantid),
       key (processid, pollutantid, polprocessid),
       key (pollutantid, processid, polprocessid)
);
truncate table pm10pollutantprocessassoc;

-- end section create remote tables for extracted data

-- section extract data
cache select p.* into outfile '##pm10emissionratio##'
from pollutantprocessassoc ppa
inner join pm10emissionratio p on (p.polprocessid=ppa.polprocessid)
inner join runspecsourcefueltype r on (r.sourcetypeid=p.sourcetypeid and r.fueltypeid=p.fueltypeid)
where ppa.pollutantid in (##pollutantids##)
and ppa.processid in (##context.iterprocess.databasekey##, 15, 16, 17);

cache select distinct ppa.polprocessid, ppa.processid, ppa.pollutantid, ppa.isaffectedbyexhaustim, ppa.isaffectedbyevapim
into outfile '##pm10pollutantprocessassoc##'
from pollutantprocessassoc ppa
inner join pm10emissionratio p on (p.polprocessid=ppa.polprocessid)
inner join runspecsourcefueltype r on (r.sourcetypeid=p.sourcetypeid and r.fueltypeid=p.fueltypeid)
where ppa.pollutantid in (##pollutantids##)
and ppa.processid in (##context.iterprocess.databasekey##, 15, 16, 17);

-- end section extract data

-- section processing

drop table if exists pm10movesworkeroutputtemp;
create table if not exists pm10movesworkeroutputtemp (
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
    regclassid           smallint unsigned null,
    fueltypeid           smallint unsigned null,
    modelyearid          smallint unsigned null,
    roadtypeid           smallint unsigned null,
    scc                  char(10) null,
    emissionquant        double null,
    emissionrate         double null,
    
    index (fueltypeid),
    index (sourcetypeid),
    index (roadtypeid),
    index (zoneid)
);

-- @algorithm pm10 total = pm2.5 total * pm10pm25ratio.
insert into pm10movesworkeroutputtemp (
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
    emissionrate)
select
    yearid,
    monthid,
    dayid,
    hourid,
    stateid,
    countyid,
    zoneid,
    linkid,
    ppa.pollutantid,
    ppa.processid,
    r.sourcetypeid,
    regclassid,
    r.fueltypeid,
    modelyearid,
    roadtypeid,
    scc,
    (emissionquant * pm10pm25ratio) as emissionquant,
    (emissionrate * pm10pm25ratio) as emissionrate
from movesworkeroutput mwo
inner join pm10pollutantprocessassoc ppa on (ppa.processid=mwo.processid)
inner join pm10emissionratio r on (
    r.polprocessid=ppa.polprocessid 
    and r.sourcetypeid=mwo.sourcetypeid 
    and r.fueltypeid=mwo.fueltypeid
    and r.minmodelyearid <= mwo.modelyearid
    and r.maxmodelyearid >= mwo.modelyearid)
where ((mwo.pollutantid=112 and ppa.pollutantid=102)
    or (mwo.pollutantid=111 and ppa.pollutantid=101)
    or (mwo.pollutantid=110 and ppa.pollutantid=100)
    or (mwo.pollutantid=115 and ppa.pollutantid=105 and mwo.processid in (1,2,90,91))
    )
    and mwo.pollutantid in (##sourcepollutantids##);

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
    regclassid,
    fueltypeid,
    modelyearid,
    roadtypeid,
    scc,
    emissionquant,
    emissionrate)
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
    regclassid,
    fueltypeid,
    modelyearid,
    roadtypeid,
    scc,
    emissionquant,
    emissionrate
from pm10movesworkeroutputtemp;

-- end section processing

-- section cleanup
drop table if exists pm10movesworkeroutputtemp;
drop table if exists pm10pollutantprocessassoc;
-- end section cleanup
