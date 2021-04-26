-- author wesley faler
-- version 2013-09-23

-- @algorithm
-- @owner crankcase emission calculator
-- @calculator

-- section create remote tables for extracted data
create table if not exists ##prefix##crankcaseemissionratio (
  polprocessid int not null,
  minmodelyearid smallint(6) not null,
  maxmodelyearid smallint(6) not null,
  sourcetypeid smallint(6) not null,
  fueltypeid smallint(6) not null,
  crankcaseratio float not null,
  crankcaseratiocv float default null,
  primary key (polprocessid, minmodelyearid, maxmodelyearid, sourcetypeid, fueltypeid)
);
truncate table ##prefix##crankcaseemissionratio;

create table if not exists ##prefix##crankcasepollutantprocessassoc (
       polprocessid         int not null,
       processid            smallint not null,
       pollutantid          smallint not null,
       isaffectedbyexhaustim char(1) not null default "N",
       isaffectedbyevapim char(1) not null default "N",
       primary key (polprocessid),
       key (processid),
       key (pollutantid)
);
truncate table ##prefix##crankcasepollutantprocessassoc;

-- end section create remote tables for extracted data

-- section extract data

-- section pm
cache select c.polprocessid,
    myrmap(c.minmodelyearid) as minmodelyearid,
    myrmap(c.maxmodelyearid) as maxmodelyearid,
    c.sourcetypeid,
    c.fueltypeid,
    c.crankcaseratio,
    c.crankcaseratiocv
into outfile '##pmcrankcaseemissionratio##'
from pollutantprocessassoc ppa
inner join crankcaseemissionratio c on (c.polprocessid=ppa.polprocessid)
inner join runspecsourcefueltype r on (r.sourcetypeid=c.sourcetypeid and r.fueltypeid=c.fueltypeid)
where ppa.pollutantid in (##pollutantids##)
and ppa.processid = ##outputprocessid##
and (
    (c.minmodelyearid >= mymap(##context.year## - 30) and c.minmodelyearid <= mymap(##context.year##))
    or
    (c.maxmodelyearid >= mymap(##context.year## - 30) and c.maxmodelyearid <= mymap(##context.year##))
    or
    (c.minmodelyearid < mymap(##context.year## - 30) and c.maxmodelyearid > mymap(##context.year##))
);

cache select distinct ppa.polprocessid, ppa.processid, ppa.pollutantid, ppa.isaffectedbyexhaustim, ppa.isaffectedbyevapim
into outfile '##pmcrankcasepollutantprocessassoc##'
from pollutantprocessassoc ppa
inner join crankcaseemissionratio c on (c.polprocessid=ppa.polprocessid)
inner join runspecsourcefueltype r on (r.sourcetypeid=c.sourcetypeid and r.fueltypeid=c.fueltypeid)
where ppa.pollutantid in (##pollutantids##)
and ppa.processid = ##outputprocessid##
and (
    (c.minmodelyearid >= mymap(##context.year## - 30) and c.minmodelyearid <= mymap(##context.year##))
    or
    (c.maxmodelyearid >= mymap(##context.year## - 30) and c.maxmodelyearid <= mymap(##context.year##))
    or
    (c.minmodelyearid < mymap(##context.year## - 30) and c.maxmodelyearid > mymap(##context.year##))
);
-- end section pm

-- section nonpm
cache select c.polprocessid,
    myrmap(c.minmodelyearid) as minmodelyearid,
    myrmap(c.maxmodelyearid) as maxmodelyearid,
    c.sourcetypeid,
    c.fueltypeid,
    c.crankcaseratio,
    c.crankcaseratiocv
into outfile '##nonpmcrankcaseemissionratio##'
from pollutantprocessassoc ppa
inner join crankcaseemissionratio c on (c.polprocessid=ppa.polprocessid)
inner join runspecsourcefueltype r on (r.sourcetypeid=c.sourcetypeid and r.fueltypeid=c.fueltypeid)
where ppa.pollutantid in (##pollutantids##)
and ppa.processid = ##outputprocessid##
and (
    (c.minmodelyearid >= mymap(##context.year## - 30) and c.minmodelyearid <= mymap(##context.year##))
    or
    (c.maxmodelyearid >= mymap(##context.year## - 30) and c.maxmodelyearid <= mymap(##context.year##))
    or
    (c.minmodelyearid < mymap(##context.year## - 30) and c.maxmodelyearid > mymap(##context.year##))
);

cache select distinct ppa.polprocessid, ppa.processid, ppa.pollutantid, ppa.isaffectedbyexhaustim, ppa.isaffectedbyevapim
into outfile '##nonpmcrankcasepollutantprocessassoc##'
from pollutantprocessassoc ppa
inner join crankcaseemissionratio c on (c.polprocessid=ppa.polprocessid)
inner join runspecsourcefueltype r on (r.sourcetypeid=c.sourcetypeid and r.fueltypeid=c.fueltypeid)
where ppa.pollutantid in (##pollutantids##)
and ppa.processid = ##outputprocessid##
and (
    (c.minmodelyearid >= mymap(##context.year## - 30) and c.minmodelyearid <= mymap(##context.year##))
    or
    (c.maxmodelyearid >= mymap(##context.year## - 30) and c.maxmodelyearid <= mymap(##context.year##))
    or
    (c.minmodelyearid < mymap(##context.year## - 30) and c.maxmodelyearid > mymap(##context.year##))
);
-- end section nonpm

-- end section extract data

-- section processing

drop table if exists ##prefix##crankcasemovesworkeroutputtemp;
create table if not exists ##prefix##crankcasemovesworkeroutputtemp (
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

create index ##prefix##movesworkeroutput_new2 on movesworkeroutput (
    pollutantid asc,
    sourcetypeid asc,
    fueltypeid asc,
    modelyearid asc,
    processid asc
);
create index ##prefix##crankcasepollutantprocessassoc_new1 on ##prefix##crankcasepollutantprocessassoc (
    pollutantid asc,
    polprocessid asc,
    processid asc
);
create index ##prefix##crankcaseemissionratio_new1 on ##prefix##crankcaseemissionratio (
    polprocessid asc,
    sourcetypeid asc,
    fueltypeid asc,
    minmodelyearid asc,
    maxmodelyearid asc
);

-- @algorithm crankcase emissions[output pollutantid,processid,modelyearid,sourcetypeid,fueltypeid] = emissions[input pollutantid,processid,modelyearid,sourcetypeid,fueltypeid] *
-- crankcaseratio[output pollutantid,input polluantid,processid,modelyearid,sourcetypeid,fueltypeid]
insert into ##prefix##crankcasemovesworkeroutputtemp (
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
    mwo.regclassid,
    r.fueltypeid,
    mwo.modelyearid,
    roadtypeid,
    scc,
    (emissionquant * crankcaseratio) as emissionquant,
    (emissionrate * crankcaseratio) as emissionrate
from movesworkeroutput mwo
inner join ##prefix##crankcasepollutantprocessassoc ppa on (ppa.pollutantid=mwo.pollutantid)
inner join ##prefix##crankcaseemissionratio r on (
    r.polprocessid=ppa.polprocessid
    and r.sourcetypeid=mwo.sourcetypeid
    and r.fueltypeid=mwo.fueltypeid
    and r.minmodelyearid <= mwo.modelyearid
    and r.maxmodelyearid >= mwo.modelyearid
    )
where ((mwo.processid=1 and ppa.processid=15)
    or (mwo.processid=2 and ppa.processid=16)
    or (mwo.processid=90 and ppa.processid=17))
    and mwo.pollutantid in (##pollutantids##);

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
from ##prefix##crankcasemovesworkeroutputtemp;

-- section sulfatepm10
truncate table ##prefix##crankcasemovesworkeroutputtemp;
insert into ##prefix##crankcasemovesworkeroutputtemp (
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
    105 as pollutantid,
    processid,
    sourcetypeid,
    regclassid,
    fueltypeid,
    modelyearid,
    roadtypeid,
    scc,
    emissionquant,
    emissionrate
from movesworkeroutput
where pollutantid=115
and processid in (15,16,17);

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
from ##prefix##crankcasemovesworkeroutputtemp;
-- end section sulfatepm10

alter table movesworkeroutput drop index ##prefix##movesworkeroutput_new2;
alter table ##prefix##crankcasepollutantprocessassoc drop index ##prefix##crankcasepollutantprocessassoc_new1;
alter table ##prefix##crankcaseemissionratio drop index ##prefix##crankcaseemissionratio_new1;

-- end section processing

-- section cleanup
drop table if exists ##prefix##crankcasemovesworkeroutputtemp;
drop table if exists ##prefix##crankcasepollutantprocessassoc;
-- end section cleanup
