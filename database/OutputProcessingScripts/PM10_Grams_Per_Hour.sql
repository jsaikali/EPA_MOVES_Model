-- This MySQL script produces emission rates for PM10 as grams per hour for each defined link 
-- 
-- The MySQL table produced in the output database is called: pm10_Grams_Per_Hour
-- 
-- Users should consult Section 4 of EPA's "Transportation Conformity Guidance for Quantitative Hot-spot
-- Analyses in PM2.5 and PM10 Nonattainment and Maintenance Areas" for guidance on filling out
-- the MOVES RunSpec and importing the appropriate inputs
--

flush tables;
select current_time;

drop   table if exists pm10_grams_per_hour;
create table pm10_grams_per_hour
select   movesrunid,
         yearid,
         monthid,
         hourid,
         linkid,
         'Total PM10' as pollutant,
         sum(emissionquant) as gramsperhour
from     movesoutput
where    pollutantid in (100,106,107)
group by movesrunid,
         yearid,
         monthid,
         hourid,
         linkid;

