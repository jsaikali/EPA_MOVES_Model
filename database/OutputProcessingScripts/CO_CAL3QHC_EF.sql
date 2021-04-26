-- This MySQL script produces emission rates for CO for use in the CAL3QHC air quality model 
-- 
-- For free-flow links (i.e., where the average speed is greater than 0 mph)
-- emission factors will be generated as grams per vehicle-mile
-- 
-- For queue links (i.e., where the average speed is 0 mph)
-- emission factors will be generated as grams per vehicle-hour
-- 
-- The MySQL table produced in the output database is called: CO_EmissionFactors
-- 
-- Users should consult Section 2 of EPA's "Using MOVES in Project-Level Carbon Monoxide Analyses"
-- for guidance on filling out the MOVES RunSpec and importing the appropriate inputs
--

flush tables;
select current_time;

drop   table if exists co_emissionfactorst;
create table co_emissionfactorst
select   a.movesrunid,
         a.yearid,
         a.monthid,
         a.dayid,
         a.hourid,
         a.linkid,
         'CO' as pollutant,
         sum(a.emissionquant) as co
from     movesoutput          as a
group by a.movesrunid,
         a.yearid,
         a.linkid;


alter table co_emissionfactorst add column distance        real;
alter table co_emissionfactorst add column population      real;
alter table co_emissionfactorst add column gramspervehmile real;
alter table co_emissionfactorst add column gramspervehhour real;


update co_emissionfactorst as a set distance = (select sum(b.activity)
                                                from   movesactivityoutput as b
                                                where  a.movesrunid = b.movesrunid
                                                  and  a.yearid     = b.yearid
                                                  and  a.linkid     = b.linkid
                                                  and  b.activitytypeid = 1);

update co_emissionfactorst as a set population = (select sum(b.activity)
                                    from   movesactivityoutput as b
                                    where  a.movesrunid = b.movesrunid
                                      and  a.yearid     = b.yearid
                                      and  a.linkid     = b.linkid
                                      and  b.activitytypeid = 6);

update co_emissionfactorst set gramspervehmile = co / distance    where distance >  0.0;
update co_emissionfactorst set gramspervehhour = co / population  where distance <= 0.0;

drop   table if exists co_emissionfactors;
create table co_emissionfactors
select movesrunid,
       yearid,
       monthid,
       dayid,
       hourid,
       linkid,
       pollutant,
       gramspervehmile,
       gramspervehhour
from   co_emissionfactorst;

drop   table if exists co_emissionfactorst;  
