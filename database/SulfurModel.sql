-- sulfur model
-- version 2016-10-04
-- authors wesely faler, ed campbell

-- subst ##sulfurinputtable## tempsulfurir;
-- subst ##sulfuroutputtable## tempsulfuror;

-- this is the form of the input table, named in ##sulfurinputtable##
-- drop table if exists ##sulfurinputtable##;
-- create table if not exists ##sulfurinputtable## (
--  	fueltypeid int not null,
--  	fuelformulationid int not null,
-- 		basefuelformulationid int not null,
-- 		polprocessid int not null,
--  	pollutantid int not null,
--  	processid int not null,
--  	modelyeargroupid int not null,
--  	minmodelyearid int not null,
--  	maxmodelyearid int not null,
--  	ageid int not null,
--  	rationosulfur double
-- );

-- this is the form of the output table, named in ##sulfuroutputtable##
-- drop table if exists ##sulfuroutputtable##;
-- create table if not exists ##sulfuroutputtable## (
--  	fueltypeid int not null,
--  	fuelformulationid int not null,
--  	polprocessid int not null,
--  	pollutantid int not null,
--  	processid int not null,
--  	sourcetypeid int not null,
--  	modelyearid int not null,
--  	ageid int not null,
--  	ratio double null,
--  	ratiogpa double null,
--  	rationosulfur double null
-- );

drop table if exists tempsulfurcalcs1;
create table if not exists tempsulfurcalcs1 (
	fueltypeid int, 
	basefuelformulationid int,
	fuelformulationid int, 
	polprocessid int, 
	pollutantid int, 
	processid int,
	modelyearid int, 
	ageid int, 
	m6emitterid int, 
	sourcetypeid int,
	sulfurcoeff double, 
	sulfurlevel double, 
	sulfurbasis double, 
	rationosulfur double,
	sulfurgpamax float,
	sulfshorttarget double, 
	sulfshort30 double,
	lowsulfurcoeff double
);

drop table if exists tempsulfurcalcs2;
create table if not exists tempsulfurcalcs2 (
	fueltypeid int, 
	basefuelformulationid int,
	fuelformulationid int, 
	polprocessid int, 
	pollutantid int, 
	processid int,
	modelyearid int, 
	ageid int, 
	m6emitterid int, 
	sourcetypeid int,
	sulfurcoeff double, 
	sulfurlevel double, 
	sulfurbasis double, 
	rationosulfur double,
	sulfurgpamax float,
	sulfurirfactor double,
	sulfshorttarget double, 
	sulfshort30 double,
	sulfshortadj double, 
	sulfadj2 double, 
	sulfirr double,
	sulfurlongcoeff double,
	minsulfadjust double,
	lowsulfurcoeff double
);

drop table if exists tempsulfurcalcs3;
create table if not exists tempsulfurcalcs3 (
	fueltypeid int, 
	basefuelformulationid int,
	fuelformulationid int, 
	polprocessid int, 
	pollutantid int, 
	processid int,
	modelyearid int, 
	ageid int, 
	m6emitterid int, 
	sourcetypeid int,
	sulfurcoeff double, 
	sulfurlevel double, 
	sulfurbasis double, 
	rationosulfur double,
	sulfurgpamax float,
	sulfurirfactor double,
	sulfshorttarget double, 
	sulfshort30 double,
	sulfshortadj double, 
	sulfadj2 double, 
	sulfirr double,
	sulfurlongcoeff double,
	sulfmax double,
	sulfadj3 double,
	sulfgpa1 double,
	ssulfgpa double,
	sulfgpa double,
	gpasulfadj double,
	minsulfadjust double,
	lowsulfurcoeff double
);

drop table if exists tempsulfurcalcs3high;
create table if not exists tempsulfurcalcs3high (
	fueltypeid int, 
	basefuelformulationid int,
	fuelformulationid int, 
	polprocessid int, 
	pollutantid int, 
	processid int,
	modelyearid int, 
	ageid int, 
	sourcetypeid int,
	sulfurcoeff double, 
	sulfurlevel double, 
	sulfurbasis double, 
	rationosulfur double,
	sulfurgpamax float,
	sulfurirfactor double,
	sulfshorttarget double, 
	sulfshort30 double,
	sulfshortadj double, 
	sulfadj2 double, 
	sulfirr double,
	sulfurlongcoeff double,
	sulfmax double,
	sulfadj3 double,
	sulfgpa1 double,
	ssulfgpa double,
	sulfgpa double,
	gpasulfadj double,
	minsulfadjust double,
	lowsulfurcoeff double
);

drop table if exists tempsulfurcalcs3normal;
create table if not exists tempsulfurcalcs3normal (
	fueltypeid int, 
	basefuelformulationid int,
	fuelformulationid int, 
	polprocessid int, 
	pollutantid int, 
	processid int,
	modelyearid int, 
	ageid int, 
	sourcetypeid int,
	sulfurcoeff double, 
	sulfurlevel double, 
	sulfurbasis double, 
	rationosulfur double,
	sulfurgpamax float,
	sulfurirfactor double,
	sulfshorttarget double, 
	sulfshort30 double,
	sulfshortadj double, 
	sulfadj2 double, 
	sulfirr double,
	sulfurlongcoeff double,
	sulfmax double,
	sulfadj3 double,
	sulfgpa1 double,
	ssulfgpa double,
	sulfgpa double,
	gpasulfadj double,
	minsulfadjust double,
	lowsulfurcoeff double
);

drop table if exists tempsulfurcalcs4;
create table if not exists tempsulfurcalcs4 (
	fueltypeid int,
	fuelformulationid int, 
	modelyearid int, 
	rationosulfur double,
	pollutantid int,
	polprocessid int, 
	processid int,
	sourcetypeid int,
	ageid int,  
	basefuelformulationid int,
	sulfadj3 double,
	gpasulfadj3	double,
	sulfadj3normal	double,
	sulfadjhigh	double,
	gpasulfadjnormal double,
	gpasulfadjhigh	double,
	minsulfadjust double,
	sulfurlevel double,
	lowsulfurcoeff double,
	sulfurbasis double
);

-- left joins should be used with m6sulfurcoeff, using a default value of 1.0 for sulfurlongcoeff
-- if a record is not found.

drop table if exists tempyear;
create table if not exists tempyear (
	year int not null primary key
);

insert into tempyear(year) values(1960),(1961),(1962),(1963),(1964),(1965),(1966),(1967)
	,(1968),(1969),(1970),(1971),(1972),(1973),(1974),(1975),(1976),(1977)
	,(1978),(1979),(1980),(1981),(1982),(1983),(1984),(1985),(1986),(1987)
	,(1988),(1989),(1990),(1991),(1992),(1993),(1994),(1995),(1996),(1997)
	,(1998),(1999),(2000),(2001),(2002),(2003),(2004),(2005),(2006),(2007)
	,(2008),(2009),(2010),(2011),(2012),(2013),(2014),(2015),(2016),(2017)
	,(2018),(2019),(2020),(2021),(2022),(2023),(2024),(2025),(2026),(2027)
	,(2028),(2029),(2030),(2031),(2032),(2033),(2034),(2035),(2036),(2037)
	,(2038),(2039),(2040),(2041),(2042),(2043),(2044),(2045),(2046),(2047)
	,(2048),(2049),(2050),(2051),(2052),(2053),(2054),(2055),(2056),(2057)
	,(2058),(2059),(2060)
;

drop table if exists tempsulfurbaselookup;
create table if not exists tempsulfurbaselookup (
	sulfurbasis int not null,
	modelyearid int not null,
	sulfurgpamax float,
	sulfurbase float
);

drop table if exists tempsulfurcoefflookup;
create table if not exists tempsulfurcoefflookup (
	sulfurcoeff float,
	processid smallint,
	pollutantid smallint,
	m6emitterid smallint,
	sourcetypeid smallint,
	modelyearid int not null,
	sulfurfunctionname char(10),
	sulfurfunctionid smallint,
	lowsulfurcoeff double
);

insert into tempsulfurbaselookup (sulfurbasis, modelyearid, sulfurgpamax, sulfurbase)
select distinct sulfurbasis, year, sulfurgpamax, sulfurbase
from tempyear
inner join sulfurbase on tempyear.year >= 
	case round(sulfurbase.modelyeargroupid / 10000,0) when 0 then 1960 else round(sulfurbase.modelyeargroupid / 10000,0) end
	and tempyear.year <= mod(sulfurbase.modelyeargroupid,10000)
inner join runspecmodelyear on runspecmodelyear.modelyearid = tempyear.year
;

alter table tempsulfurbaselookup add key idxall(sulfurbasis, modelyearid);
alter table tempsulfurbaselookup add key idxall2(modelyearid, sulfurbasis);

insert into tempsulfurcoefflookup (sulfurcoeff, processid, pollutantid, m6emitterid,
	sourcetypeid, modelyearid, sulfurfunctionname, sulfurfunctionid, lowsulfurcoeff
)
select sulfurcoeff, processid, pollutantid, sulfurmodelcoeff.m6emitterid,
	sulfurmodelcoeff.sourcetypeid, year, sulfurfunctionname, sulfurmodelcoeff.sulfurfunctionid, sulfurmodelcoeff.lowsulfurcoeff
from tempyear
inner join sulfurmodelcoeff on tempyear.year >= 
	case round(sulfurmodelcoeff.fuelmygroupid / 10000,0) when 0 then 1960 else round(sulfurmodelcoeff.fuelmygroupid / 10000,0) end 
	and tempyear.year <= mod(sulfurmodelcoeff.fuelmygroupid,10000)
inner join runspecmodelyear on runspecmodelyear.modelyearid = tempyear.year
inner join sulfurmodelname on sulfurmodelcoeff.m6emitterid = sulfurmodelname.m6emitterid
	and sulfurmodelcoeff.sulfurfunctionid = sulfurmodelname.sulfurfunctionid
inner join runspecsourcetype rsst on rsst.sourcetypeid = sulfurmodelcoeff.sourcetypeid
;

alter table tempsulfurcoefflookup add key(processid, pollutantid, modelyearid);

insert into tempsulfurcalcs1 (fueltypeid, basefuelformulationid, fuelformulationid, polprocessid, pollutantid, processid,
	modelyearid, ageid, m6emitterid, sourcetypeid,
	sulfurcoeff, sulfurlevel, sulfurbasis, rationosulfur, sulfurgpamax,
	sulfshorttarget, sulfshort30, lowsulfurcoeff
)
select 
	ir.fueltypeid, ir.basefuelformulationid, ir.fuelformulationid, ir.polprocessid, ir.pollutantid, ir.processid,
	rsmya.modelyearid, ir.ageid, m6emitterid, sourcetypeid,
	sulfurcoeff, sulfurlevel, sulfurbasis, rationosulfur, sulfurgpamax,
	case sulfurfunctionname when 'log-log' then
			case when sulfurlevel > 0 then exp(sulfurcoeff * ln(sulfurlevel)) else 0 end
	else
		exp(sulfurcoeff * sulfurlevel)
	end as sulfshorttarget,
	case sulfurfunctionname when 'log-log' then
		exp(sulfurcoeff * ln(sulfurbasis))
	else
		exp(sulfurcoeff * sulfurbasis)
	end as sulfshort30,
	lowsulfurcoeff
from ##sulfurinputtable## ir 
inner join tempsulfurcoefflookup on 
	ir.processid = tempsulfurcoefflookup.processid and ir.pollutantid = tempsulfurcoefflookup.pollutantid
inner join fuelformulation on ir.fuelformulationid = fuelformulation.fuelformulationid
inner join tempsulfurbaselookup on tempsulfurcoefflookup.modelyearid = tempsulfurbaselookup.modelyearid
	and ir.minmodelyearid <= tempsulfurbaselookup.modelyearid
	and ir.maxmodelyearid >= tempsulfurbaselookup.modelyearid
inner join runspecmodelyearage rsmya on rsmya.modelyearid = mymap(tempsulfurbaselookup.modelyearid)
	and rsmya.ageid = ir.ageid
;

alter table tempsulfurcalcs1 add key(pollutantid, modelyearid, fueltypeid);

insert into tempsulfurcalcs2 (fueltypeid, basefuelformulationid, fuelformulationid, polprocessid, pollutantid, processid,
	modelyearid, ageid, m6emitterid, sourcetypeid,
	sulfurcoeff, sulfurlevel, sulfurbasis, rationosulfur, sulfurgpamax, sulfurirfactor,
	sulfshorttarget, sulfshort30, sulfshortadj, sulfadj2, sulfirr, sulfurlongcoeff, lowsulfurcoeff
)
select 
	sc1.fueltypeid, basefuelformulationid, sc1.fuelformulationid, sc1.polprocessid, sc1.pollutantid, sc1.processid,
	sc1.modelyearid, ageid, sc1.m6emitterid, sourcetypeid,
	sulfurcoeff, sulfurlevel, sulfurbasis, rationosulfur, sulfurgpamax, 
	case when sulfurirfactor is null then
		0
	else
		case when sulfurlevel <= maxirfactorsulfur then
			sulfurirfactor
		else
			0
		end
	end as	
	sulfurirfactor,
	sulfshorttarget, sulfshort30, 
	(sulfshorttarget - sulfshort30) / sulfshort30 as sulfshortadj,
	((sulfshorttarget - sulfshort30) / sulfshort30) * ifnull(sulfurlongcoeff,1.0) as sulfadj2,
	case when (maxirfactorsulfur is null or maxirfactorsulfur <= 0) then -- or sulfurlevel < 30
		0
	when sulfurlevel <= maxirfactorsulfur then -- was <= maxirfactorsulfur
		exp(sulfurcoeff * ln(maxirfactorsulfur))
	else
		case when sulfurlevel > 0 then exp(sulfurcoeff * ln(sulfurlevel)) else 0 end
	end as sulfirr, 
	
	case when sulfurlongcoeff is null then
		1.0
	else
		sulfurlongcoeff
	end as
	sulfurlongcoeff,
	lowsulfurcoeff
from tempsulfurcalcs1 sc1
left join m6sulfurcoeff on 
	sc1.pollutantid = m6sulfurcoeff.pollutantid and
	sc1.modelyearid >= m6sulfurcoeff.minmodelyearid and
	sc1.modelyearid <= m6sulfurcoeff.maxmodelyearid
inner join sulfurcapamount on sc1.fueltypeid = sulfurcapamount.fueltypeid
;

-- 2010a:
-- 	(case when (tempsulfurbaselookup.sulfurbase <= 30) then 0.85 else 0.50 end)

-- revised internal:
--  (case when (tempsulfurbaselookup.sulfurbase <= 30) then 0.40 else 0.40 end)

update tempsulfurcalcs2, tempsulfurbaselookup set minsulfadjust=
    (case when (tempsulfurbaselookup.sulfurbase <= 30) then 0.85 else 0.50 end)
where tempsulfurbaselookup.modelyearid=tempsulfurcalcs2.modelyearid;

insert into tempsulfurcalcs3 (fueltypeid, basefuelformulationid, fuelformulationid, polprocessid, pollutantid, processid,
	modelyearid, ageid, m6emitterid, sourcetypeid,
	sulfurcoeff, sulfurlevel, sulfurbasis, rationosulfur, sulfurgpamax, sulfurirfactor,
	sulfshorttarget, sulfshort30, sulfshortadj, sulfadj2, sulfirr, sulfurlongcoeff,
	sulfmax, sulfadj3, sulfgpa1, ssulfgpa, sulfgpa, gpasulfadj, minsulfadjust, lowsulfurcoeff
)
select 
	sc2.fueltypeid, basefuelformulationid, sc2.fuelformulationid, sc2.polprocessid, sc2.pollutantid, sc2.processid,
	sc2.modelyearid, ageid, sc2.m6emitterid, sourcetypeid,
	sulfurcoeff, sulfurlevel, sulfurbasis, rationosulfur, sulfurgpamax, sulfurirfactor,
	sulfshorttarget, sulfshort30, sulfshortadj, sulfadj2, sulfirr, sulfurlongcoeff,
	((sulfirr - sulfshort30) / sulfshort30) as sulfmax,
	case when 1.0 + (sulfurirfactor * ((sulfirr - sulfshort30) / sulfshort30) 
			+ (1.0 - sulfurirfactor) * sulfadj2) <= minsulfadjust then
		minsulfadjust
	else
		1.0 + (sulfurirfactor * ((sulfirr - sulfshort30) / sulfshort30) 
		+ (1.0 - sulfurirfactor) * sulfadj2)
	end as sulfadj3,
	case when (modelyearid >= ##cutoff.sulfurmodelgpaphaseinstart## and modelyearid <= ##cutoff.sulfurmodelgpaphaseinend## and sulfurlevel >= 0) then
		((case when (sc2.pollutantid=3 and sc2.m6emitterid=2) then 0.60 else 1.0 end)*exp(sulfurcoeff * ln(sulfurgpamax)))
	else
		0
	end as sulfgpa1,
	case when (modelyearid >= ##cutoff.sulfurmodelgpaphaseinstart## and modelyearid <= ##cutoff.sulfurmodelgpaphaseinstart## and sulfurlevel >= 0) then
		-- nox high emitters get sulfgpa1*0.6 and sulfshort30*0.6, which cancel and make no difference in the ratio here
		(exp(sulfurcoeff * ln(sulfurgpamax)) - sulfshort30) / sulfshort30
	else
		0
	end as ssulfgpa,
	case when (modelyearid >= ##cutoff.sulfurmodelgpaphaseinstart## and modelyearid <= ##cutoff.sulfurmodelgpaphaseinend## and sulfurlevel >= 0) then
		-- nox high emitters get sulfgpa1*0.6 and sulfshort30*0.6, which cancel and make no difference in the ratio here
		((exp(sulfurcoeff * ln(sulfurgpamax)) - sulfshort30) / sulfshort30)
		* sulfurlongcoeff
	else
		0
	end as sulfgpa,
	case when (modelyearid >= ##cutoff.sulfurmodelgpaphaseinstart## and modelyearid <= ##cutoff.sulfurmodelgpaphaseinend## and sulfurlevel >= 0) then
		-- nox high emitters get sulfgpa1*0.6 and sulfshort30*0.6, which cancel and make no difference in the ratio here
		1.0 + (sulfurirfactor * (((exp(sulfurcoeff * ln(sulfurgpamax)) - sulfshort30) / sulfshort30)
		* sulfurlongcoeff) + (1.0 - sulfurirfactor) * sulfadj2)
	else
		case when 1.0 + (sulfurirfactor * ((sulfirr - sulfshort30) / sulfshort30) 
				+ (1.0 - sulfurirfactor) * sulfadj2) <= minsulfadjust then
			minsulfadjust
		else
			1.0 + (sulfurirfactor * ((sulfirr - sulfshort30) / sulfshort30) 
			+ (1.0 - sulfurirfactor) * sulfadj2)
		end
	end as gpasulfadj,
	minsulfadjust,
	lowsulfurcoeff
from tempsulfurcalcs2 sc2
;

alter table tempsulfurcalcs3 add key(m6emitterid);

insert into tempsulfurcalcs3high (fueltypeid, fuelformulationid, basefuelformulationid, polprocessid, pollutantid, processid,
	modelyearid, ageid, sourcetypeid,
	sulfurcoeff, sulfurlevel, sulfurbasis, rationosulfur, sulfurgpamax, sulfurirfactor,
	sulfshorttarget, sulfshort30, sulfshortadj, sulfadj2, sulfirr, sulfurlongcoeff,
	sulfmax, sulfadj3, sulfgpa1, ssulfgpa, sulfgpa, gpasulfadj,minsulfadjust,lowsulfurcoeff)
select 
	fueltypeid, fuelformulationid, basefuelformulationid, polprocessid, pollutantid, processid,
	modelyearid, ageid, sourcetypeid,
	sulfurcoeff, sulfurlevel, sulfurbasis, rationosulfur, sulfurgpamax, sulfurirfactor,
	sulfshorttarget, sulfshort30, sulfshortadj, sulfadj2, sulfirr, sulfurlongcoeff,
	sulfmax, sulfadj3, sulfgpa1, ssulfgpa, sulfgpa, gpasulfadj, minsulfadjust, lowsulfurcoeff
from tempsulfurcalcs3 where m6emitterid = 2
;

insert into tempsulfurcalcs3normal (fueltypeid, fuelformulationid, polprocessid, pollutantid, processid,
	modelyearid, ageid, sourcetypeid,
	sulfurcoeff, sulfurlevel, sulfurbasis, rationosulfur, sulfurgpamax, sulfurirfactor,
	sulfshorttarget, sulfshort30, sulfshortadj, sulfadj2, sulfirr, sulfurlongcoeff,
	sulfmax, sulfadj3, sulfgpa1, ssulfgpa, sulfgpa, gpasulfadj, minsulfadjust, lowsulfurcoeff)
select 
	fueltypeid, fuelformulationid, polprocessid, pollutantid, processid,
	modelyearid, ageid, sourcetypeid,
	sulfurcoeff, sulfurlevel, sulfurbasis, rationosulfur, sulfurgpamax, sulfurirfactor,
	sulfshorttarget, sulfshort30, sulfshortadj, sulfadj2, sulfirr, sulfurlongcoeff,
	sulfmax, sulfadj3, sulfgpa1, ssulfgpa, sulfgpa, gpasulfadj, minsulfadjust, lowsulfurcoeff
from tempsulfurcalcs3 where m6emitterid = 1
;

alter table tempsulfurcalcs3high add key(fuelformulationid, polprocessid, sourcetypeid, ageid, modelyearid);
alter table tempsulfurcalcs3normal add key(fuelformulationid, polprocessid, sourcetypeid, ageid, modelyearid);

insert into tempsulfurcalcs4 (fueltypeid, fuelformulationid, modelyearid, rationosulfur, pollutantid,
	polprocessid, processid, sourcetypeid, ageid, basefuelformulationid, sulfadj3,
	gpasulfadj3, sulfadj3normal, sulfadjhigh, gpasulfadjnormal, gpasulfadjhigh, minsulfadjust, lowsulfurcoeff, sulfurlevel, sulfurbasis)
select 
	tempsulfurcalcs3high.fueltypeid, tempsulfurcalcs3high.fuelformulationid, tempsulfurcalcs3high.modelyearid, tempsulfurcalcs3high.rationosulfur, 
	tempsulfurcalcs3high.pollutantid, tempsulfurcalcs3high.polprocessid, tempsulfurcalcs3high.processid, tempsulfurcalcs3high.sourcetypeid, 
	tempsulfurcalcs3high.ageid, tempsulfurcalcs3high.basefuelformulationid,
	(1 - 0.5) * tempsulfurcalcs3normal.sulfadj3 + 0.5 * tempsulfurcalcs3high.sulfadj3 as sulfadj3,
	(1 - 0.5) * tempsulfurcalcs3normal.gpasulfadj + 0.5 * tempsulfurcalcs3high.gpasulfadj as gpasulfadj3,
	tempsulfurcalcs3normal.sulfadj3 as sulfadj3normal,
	tempsulfurcalcs3high.sulfadj3 as sulfadjhigh,
	tempsulfurcalcs3normal.gpasulfadj as gpasulfadjnormal,
	tempsulfurcalcs3high.gpasulfadj as gpasulfadjhigh,
	tempsulfurcalcs3normal.minsulfadjust as minsulfadjust,
	tempsulfurcalcs3normal.lowsulfurcoeff as lowsulfurcoeff,
	tempsulfurcalcs3normal.sulfurlevel as sulfurlevel,
	tempsulfurcalcs3normal.sulfurbasis as sulfurbasis
from tempsulfurcalcs3high
inner join tempsulfurcalcs3normal on tempsulfurcalcs3high.fuelformulationid = tempsulfurcalcs3normal.fuelformulationid and
	tempsulfurcalcs3high.polprocessid = tempsulfurcalcs3normal.polprocessid and
	tempsulfurcalcs3high.modelyearid = tempsulfurcalcs3normal.modelyearid and
	tempsulfurcalcs3high.ageid = tempsulfurcalcs3normal.ageid and
	tempsulfurcalcs3high.sourcetypeid = tempsulfurcalcs3normal.sourcetypeid;

alter table tempsulfurcalcs4 add key (fuelformulationid, modelyearid, polprocessid, sourcetypeid, ageid);

insert ignore into ##sulfuroutputtable## (fueltypeid, fuelformulationid, polprocessid, pollutantid, processid,
sourcetypeid, modelyearid, ageid, ratio, ratiogpa, rationosulfur)
select
	target.fueltypeid, target.fuelformulationid, target.polprocessid, target.pollutantid, 
	target.processid, target.sourcetypeid, target.modelyearid, target.ageid,
	case when (target.lowsulfurcoeff is not null and target.sulfurlevel <= 30 and target.sulfurbasis=30) then
		greatest(1.0-target.lowsulfurcoeff*(30.0-target.sulfurlevel),0) * target.rationosulfur
	else
		case when (target.modelyearid >= ##cutoff.sulfurmodelthcnoxstart## and target.modelyearid <= ##cutoff.sulfurmodelthcnoxend## and target.pollutantid in (1, 3)) 
				or (target.modelyearid >= ##cutoff.sulfurmodelcostart## and target.pollutantid = 2) then
			greatest(target.sulfadj3 / base.sulfadj3, target.minsulfadjust) * target.rationosulfur
		else
			case when target.modelyearid > ##cutoff.sulfurmodelthcnoxend## and target.pollutantid in (1, 3) then
				greatest(target.sulfadj3 / base.sulfadj3, target.minsulfadjust)
			else
				1.0
			end
		end
	end as fueladjustment,
	case when (target.lowsulfurcoeff is not null and target.sulfurlevel <= 30 and target.sulfurbasis=30) then
		greatest(1.0-target.lowsulfurcoeff*(30.0-target.sulfurlevel),0) * target.rationosulfur
	else
		case when (target.modelyearid >= ##cutoff.sulfurmodelthcnoxstart## and target.modelyearid <= ##cutoff.sulfurmodelthcnoxend## and target.pollutantid in (1, 3))
				or (target.modelyearid >= ##cutoff.sulfurmodelcostart## and target.pollutantid = 2) then
			greatest(target.sulfadj3 / base.sulfadj3, target.minsulfadjust) * target.rationosulfur
		else
			case when target.modelyearid > ##cutoff.sulfurmodelthcnoxend## and target.pollutantid in (1,3) then
				greatest(target.sulfadj3/ base.sulfadj3, target.minsulfadjust)
			else
				1.0
			end
		end
	end * 
	case when (target.modelyearid >= ##cutoff.sulfurmodelgpaphaseinstart## and target.modelyearid <= ##cutoff.sulfurmodelgpaphaseinend## and (target.gpasulfadj3/ base.sulfadj3) > 1.0) then
		greatest(target.gpasulfadj3/ base.sulfadj3, target.minsulfadjust)
	else
		1.0
	end	as fueladjustmentgpa,
	target.rationosulfur
from tempsulfurcalcs4 target
inner join tempsulfurcalcs4 base on base.fuelformulationid = target.basefuelformulationid
	and base.modelyearid = target.modelyearid
	and base.polprocessid = target.polprocessid
	and base.sourcetypeid = target.sourcetypeid
	and base.ageid = target.ageid
;

drop table if exists debugsulfuroutputtable;

create table debugsulfuroutputtable
select
	target.fueltypeid, target.fuelformulationid, target.polprocessid, target.pollutantid, 
	target.processid, target.sourcetypeid, target.modelyearid, target.ageid,
	case when (target.lowsulfurcoeff is not null and target.sulfurlevel <= 30 and target.sulfurbasis=30) then
		greatest(1.0-target.lowsulfurcoeff*(30.0-target.sulfurlevel),0) * target.rationosulfur
	else
		case when (target.modelyearid >= ##cutoff.sulfurmodelthcnoxstart## and target.modelyearid <= ##cutoff.sulfurmodelthcnoxend## and target.pollutantid in (1, 3)) 
				or (target.modelyearid >= ##cutoff.sulfurmodelcostart## and target.pollutantid = 2) then
			greatest(target.sulfadj3 / base.sulfadj3, target.minsulfadjust) * target.rationosulfur
		else
			case when target.modelyearid > ##cutoff.sulfurmodelthcnoxend## and target.pollutantid in (1, 3) then
				greatest(target.sulfadj3 / base.sulfadj3, target.minsulfadjust)
			else
				1.0
			end
		end
	end as fueladjustment,
	case when (target.lowsulfurcoeff is not null and target.sulfurlevel <= 30 and target.sulfurbasis=30) then
		greatest(1.0-target.lowsulfurcoeff*(30.0-target.sulfurlevel),0) * target.rationosulfur
	else
		case when (target.modelyearid >= ##cutoff.sulfurmodelthcnoxstart## and target.modelyearid <= ##cutoff.sulfurmodelthcnoxend## and target.pollutantid in (1, 3))
				or (target.modelyearid >= ##cutoff.sulfurmodelcostart## and target.pollutantid = 2) then
			greatest(target.sulfadj3 / base.sulfadj3, target.minsulfadjust) * target.rationosulfur
		else
			case when target.modelyearid > ##cutoff.sulfurmodelthcnoxend## and target.pollutantid in (1,3) then
				greatest(target.sulfadj3/ base.sulfadj3, target.minsulfadjust)
			else
				1.0
			end
		end
	end * 
	case when (target.modelyearid >= ##cutoff.sulfurmodelgpaphaseinstart## and target.modelyearid <= ##cutoff.sulfurmodelgpaphaseinend## and (target.gpasulfadj3/ base.sulfadj3) > 1.0) then
		greatest(target.gpasulfadj3/ base.sulfadj3, target.minsulfadjust)
	else
		1.0
	end	as fueladjustmentgpa,
	target.rationosulfur,
	target.lowsulfurcoeff as target_lowsulfurcoeff,
	target.sulfurlevel as target_sulfurlevel,
	target.rationosulfur as target_rationosulfur,
	target.sulfadj3 as target_sulfadj3,
	base.sulfadj3 as base_sulfadj3,
	target.minsulfadjust as target_minsulfadjust,
	target.sulfurbasis as target_sulfurbasis
from tempsulfurcalcs4 target
inner join tempsulfurcalcs4 base on base.fuelformulationid = target.basefuelformulationid
	and base.modelyearid = target.modelyearid
	and base.polprocessid = target.polprocessid
	and base.sourcetypeid = target.sourcetypeid
	and base.ageid = target.ageid
;
