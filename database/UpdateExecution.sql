-- update utility tables in the movesexecution database.
-- author wesley faler
-- version 2016-03-15
--
-- author daniel bizer-cox
-- version 2019-12-05

-- --------------------------------------------------------------------
-- create regclasssourcetypefraction
-- --------------------------------------------------------------------
drop table if exists runspecsourcetypemodelyearid;
create table if not exists runspecsourcetypemodelyearid (
	sourcetypemodelyearid int not null primary key
);

insert into runspecsourcetypemodelyearid (sourcetypemodelyearid)
select sourcetypeid*10000 + modelyearid
from runspecsourcetype, runspecmodelyear;

drop table if exists regclasssourcetypefraction;
create table if not exists regclasssourcetypefraction (
	fueltypeid smallint not null,
	modelyearid smallint not null,
	sourcetypeid smallint not null,
	regclassid smallint not null,
	regclassfraction double not null default 0,
	primary key (fueltypeid, modelyearid, sourcetypeid, regclassid),
	key (fueltypeid),
	key (fueltypeid, sourcetypeid),
	key (modelyearid, fueltypeid, sourcetypeid),
	key (modelyearid),
	key (sourcetypeid, modelyearid, fueltypeid)
);

-- regclassfraction is fraction of a [source type,fuel used,modelyear] that a regclass covers, (accounting for fuel type usage)
-- fix for mtest-92: this table was originally not accounting for fuel usage fraction
insert into regclasssourcetypefraction (fueltypeid, modelyearid, sourcetypeid, regclassid, regclassfraction)
select fuf.fuelsupplyfueltypeid, svp.modelyearid, svp.sourcetypeid, svp.regclassid, 
       sum(usagefraction * stmyfraction) / mystftfraction as regclassfraction
from fuelusagefraction fuf
join samplevehiclepopulation svp on (fuf.sourcebinfueltypeid = svp.fueltypeid)
join (
    select sourcetypemodelyearid, fuelsupplyfueltypeid, modelyearid, sum(usagefraction * stmyfraction) as mystftfraction
    from fuelusagefraction fuf
    join samplevehiclepopulation svp on (fuf.sourcebinfueltypeid = svp.fueltypeid)
    inner join runspecsourcetypemodelyearid using (sourcetypemodelyearid)
    group by fuelsupplyfueltypeid, sourcetypemodelyearid
    having mystftfraction <> 0
) as t1 on fuf.fuelsupplyfueltypeid = t1.fuelsupplyfueltypeid and svp.sourcetypemodelyearid = t1.sourcetypemodelyearid
inner join runspecsourcetypemodelyearid rsstmy on svp.sourcetypemodelyearid = rsstmy.sourcetypemodelyearid
group by sourcetypeid, fuelsupplyfueltypeid, modelyearid, regclassid
having regclassfraction <> 0 and regclassfraction is not null;
-- --------------------------------------------------------------------
-- done creating regclasssourcetypefraction
-- --------------------------------------------------------------------

-- --------------------------------------------------------------------
-- add indexes that improve sourceusetypephysics
-- --------------------------------------------------------------------
alter table emissionratebyage add key sutphys (polprocessid, opmodeid);
alter table emissionrate add key sutphys (polprocessid, opmodeid);

