-- version 2008-01-21
-- purpose: calculate co2 equivalent for well-tp-pump
-- gwo shyu, epa

-- section create remote tables for extracted data
drop table if exists co2eqwtpstep2pollutant;
create table if not exists co2eqwtpstep2pollutant(
	pollutantid		smallint not null,
	pollutantname		char(50) null,
	energyormass		char(6) null,
	globalwarmingpotential	smallint null, 
	primary key (pollutantid)
);
truncate table co2eqwtpstep2pollutant;
-- end section create remote tables for extracted data

-- section extract data
select * into outfile '##co2eqwtpstep2pollutant##' from pollutant;
-- flush tables;
-- end section extract data

-- section local data removal
-- end section local data removal

-- section processing
-- 	subsection task 122 step 2 for wtp: calculate equivalent co2 from
-- 					co2, methane, and n2o for wtp
-- 	movesworkeroutput mwo inner join co2eqwtpstep2pollutant pol on mwo.pollutantid = pol.pollutantid 

drop table if exists movesoutputco2temp2eq;
create table movesoutputco2temp2eq 
select 
	mwo.movesrunid, mwo.yearid, mwo.monthid, mwo.dayid, 
	mwo.hourid,mwo.stateid,mwo.countyid,mwo.zoneid, 
	mwo.linkid,##co2eqpollutantid## as pollutantid,mwo.processid, 
	mwo.sourcetypeid,mwo.fueltypeid,mwo.modelyearid, 
	mwo.roadtypeid,mwo.scc, 
	sum(mwo.emissionquant * pol.globalwarmingpotential) as emissionquant
from 
 	movesworkeroutput mwo inner join co2eqwtpstep2pollutant pol on mwo.pollutantid = pol.pollutantid 
where 	mwo.pollutantid in (##co2step2eqpollutantids##) and ##co2step2eqprocessids##  
group by 
	mwo.movesrunid,mwo.yearid,mwo.monthid,mwo.dayid, mwo.hourid, 
	mwo.stateid,mwo.countyid,mwo.zoneid,mwo.linkid,mwo.pollutantid, mwo.processid, 
	mwo.sourcetypeid,mwo.fueltypeid,mwo.modelyearid,mwo.roadtypeid, mwo.scc;
-- flush tables;
-- 	end of subsection task 122 step 2 for wtp:

insert into movesworkeroutput ( 
	movesrunid,yearid,monthid,dayid,hourid,stateid,countyid,zoneid, 
	linkid,pollutantid,processid,sourcetypeid,fueltypeid,modelyearid, 
	roadtypeid,scc,emissionquant) 
select 
	movesrunid,yearid,monthid,dayid,hourid,stateid,countyid,zoneid, 
	linkid,pollutantid,processid,sourcetypeid,fueltypeid,modelyearid, 
	roadtypeid,scc,emissionquant 
from movesoutputco2temp2eq;
analyze table movesworkeroutput;
-- flush tables;

-- end section processing

-- section cleanup
drop table if exists movesoutputco2temp2eq;
-- end section cleanup
