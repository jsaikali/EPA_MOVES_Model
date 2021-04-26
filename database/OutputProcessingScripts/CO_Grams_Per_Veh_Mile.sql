-- This MySQL script produces emission rates for CO as grams per vehicle-mile for each defined link 
-- 
-- The MySQL table produced in the output database is called: CO_Grams_Per_Veh_Mile
-- 
-- Users should consult Section 2 of EPA's document "Using MOVES in Project-Level Carbon Monoxide Analyses"
-- for guidance on filling out the MOVES RunSpec and importing the appropriate inputs
--

flush tables;
select current_time;

drop   table if exists co_grams_per_veh_mile;
create table co_grams_per_veh_mile
select   movesrunid,
         yearid,
         monthid,
         hourid,
         linkid,
         'Total CO' as pollutant,
         sum(rateperdistance) as gramspervehmile
from     rateperdistance
where    pollutantid in (2)
group by movesrunid,
         yearid,
         monthid,
         hourid,
         linkid;
