-- author wesley faler
-- version 2013-09-23

-- @algorithm
-- @owner pm10 brake tire calculator
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
where ppa.pollutantid in (106,107)
and ppa.processid in (##context.iterprocess.databasekey##);

cache select distinct ppa.polprocessid, ppa.processid, ppa.pollutantid, ppa.isaffectedbyexhaustim, ppa.isaffectedbyevapim
into outfile '##pm10pollutantprocessassoc##'
from pollutantprocessassoc ppa
inner join pm10emissionratio p on (p.polprocessid=ppa.polprocessid)
inner join runspecsourcefueltype r on (r.sourcetypeid=p.sourcetypeid and r.fueltypeid=p.fueltypeid)
where ppa.pollutantid in (106,107)
and ppa.processid in (##context.iterprocess.databasekey##);

-- end section extract data

-- section processing

drop table if exists pm10braketiremovesworkeroutputtemp;
create table if not exists pm10braketiremovesworkeroutputtemp (
	movesrunid           smallint unsigned not null default 0,
	iterationid			smallint unsigned default 1,
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

-- @algorithm pm10 brakewear (106) = pm2.5 brakewear (116) * pm10pm25ratio.
-- pm10 tirewear (107) = pm2.5 tirewear (117) * pm10pm25ratio.
insert into pm10braketiremovesworkeroutputtemp (
	movesrunid, iterationid,
    yearid, monthid, dayid, hourid, stateid, countyid, zoneid, linkid,
	pollutantid,
	processid, sourcetypeid, regclassid, fueltypeid, modelyearid, roadtypeid, scc,
	emissionquant, emissionrate)
select
	movesrunid, iterationid,
    yearid, monthid, dayid, hourid, stateid, countyid, zoneid, linkid,
	ppa.pollutantid as pollutantid,
	mwo.processid, mwo.sourcetypeid, mwo.regclassid, mwo.fueltypeid, modelyearid, roadtypeid, scc,
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
where ((mwo.pollutantid=116 and ppa.pollutantid=106)
	or (mwo.pollutantid=117 and ppa.pollutantid=107));

insert into movesworkeroutput (
	movesrunid, iterationid,
    yearid, monthid, dayid, hourid, stateid, countyid, zoneid, linkid,
	pollutantid,
	processid, sourcetypeid, regclassid, fueltypeid, modelyearid, roadtypeid, scc,
	emissionquant, emissionrate)
select
	movesrunid, iterationid,
    yearid, monthid, dayid, hourid, stateid, countyid, zoneid, linkid,
	pollutantid,
	processid, sourcetypeid, regclassid, fueltypeid, modelyearid, roadtypeid, scc,
	emissionquant, emissionrate
from pm10braketiremovesworkeroutputtemp;

-- end section processing

-- section cleanup
drop table if exists pm10braketiremovesworkeroutputtemp;
drop table if exists pm10pollutantprocessassoc;
-- end section cleanup
