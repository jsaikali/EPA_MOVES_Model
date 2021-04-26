-- This MySQL script produces emission rates for PM2.5 as grams per vehicle-mile for each defined link 
-- 
-- The MySQL table produced in the output database is called: pm25_Grams_Per_Veh_Mile
-- 
-- Users should consult Section 4 of EPA's "Transportation Conformity Guidance for Quantitative Hot-spot
-- Analyses in PM2.5 and PM10 Nonattainment and Maintenance Areas" for guidance on filling out
-- the MOVES RunSpec and importing the appropriate inputs
--


drop   table if exists pm25_grams_per_veh_mile;
create table pm25_grams_per_veh_mile
select   movesrunid,
         yearid,
         monthid,
         hourid,
         linkid,
         'Total PM2.5' as pollutant,
         sum(rateperdistance) as gramspervehmile
from     rateperdistance
where    pollutantid in (110,116,117)
group by movesrunid,
         yearid,
         monthid,
         hourid,
         linkid;


