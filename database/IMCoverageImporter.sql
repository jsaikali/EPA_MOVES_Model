-- author wesley faler
-- version 2016-10-04

drop procedure if exists spcheckimimporter;

beginblock
create procedure spcheckimimporter()
begin
	-- mode 0 is run after importing
	-- mode 1 is run to check overall success/failure for national domain
	-- mode 2 is run to check overall success/failure for county and project domains
	declare mode int default ##mode##;
	declare isok int default 1;
	declare howmany int default 0;
-- 	declare defaultrecordcount int default 0;

	update imcoverage set useimyn='Y' where useimyn='y';

	drop table if exists tempsourcefueltype;
	create table if not exists tempsourcefueltype (
		sourcetypeid smallint not null,
		fueltypeid smallint not null,
		primary key (sourcetypeid, fueltypeid),
		key (fueltypeid, sourcetypeid)
	);
	insert into tempsourcefueltype (sourcetypeid, fueltypeid)
	select sourcetypeid, fueltypeid
	from ##defaultdatabase##.sourceusetype,
	##defaultdatabase##.fueltype
	where (sourcetypeid*100 + fueltypeid) in (##sourcefueltypeids##);

	drop table if exists tempmodelyear;
	create table if not exists tempmodelyear (
		modelyearid smallint not null primary key
	);

	insert into tempmodelyear(modelyearid) values(1960),(1961),(1962),(1963),(1964),(1965),(1966),(1967)
		,(1968),(1969),(1970),(1971),(1972),(1973),(1974),(1975),(1976),(1977),(1978),(1979),(1980),(1981),(1982),(1983),(1984),(1985),(1986),(1987)
		,(1988),(1989),(1990),(1991),(1992),(1993),(1994),(1995),(1996),(1997),(1998),(1999),(2000),(2001),(2002),(2003),(2004),(2005),(2006),(2007)
		,(2008),(2009),(2010),(2011),(2012),(2013),(2014),(2015),(2016),(2017),(2018),(2019),(2020),(2021),(2022),(2023),(2024),(2025),(2026),(2027)
		,(2028),(2029),(2030),(2031),(2032),(2033),(2034),(2035),(2036),(2037),(2038),(2039),(2040),(2041),(2042),(2043),(2044),(2045),(2046),(2047)
		,(2048),(2049),(2050),(2051),(2052),(2053),(2054),(2055),(2056),(2057),(2058),(2059),(2060);

	drop table if exists expandedimportrecords;
	create table if not exists expandedimportrecords (
		polprocessid int not null,
		countyid int not null,
		yearid smallint not null,
		sourcetypeid smallint not null,
		fueltypeid smallint not null,
		modelyearid smallint not null,
		key (polprocessid, countyid, yearid, sourcetypeid, fueltypeid, modelyearid)
	);

-- 	select count(*) into defaultrecordcount
-- 	from ##defaultdatabase##.imcoverage
-- 	inner join tempsourcefueltype using (sourcetypeid, fueltypeid)
-- 	where countyid in (##countyids##) and yearid in (##yearids##) and polprocessid in (##polprocessids##);
-- 
-- 	if(defaultrecordcount > 0) then
-- 		-- expand the imported records regardless of active/inactive
-- 		truncate table expandedimportrecords;
-- 		insert into expandedimportrecords (polprocessid, countyid, yearid, sourcetypeid, fueltypeid, modelyearid)
-- 		select distinct imc.polprocessid, imc.countyid, imc.yearid, imc.sourcetypeid, imc.fueltypeid, tempmodelyear.modelyearid
-- 		from imcoverage imc
-- 		inner join tempmodelyear on (begmodelyearid <= modelyearid and modelyearid <= endmodelyearid)
-- 		inner join tempsourcefueltype using (sourcetypeid, fueltypeid)
-- 		where countyid in (##countyids##) and yearid in (##yearids##) and polprocessid in (##polprocessids##);
-- 
-- 		drop table if exists expandeddefaultrecords;
-- 		create table if not exists expandeddefaultrecords (
-- 			polprocessid int not null,
-- 			countyid int not null,
-- 			yearid smallint not null,
-- 			sourcetypeid smallint not null,
-- 			fueltypeid smallint not null,
-- 			modelyearid smallint not null,
-- 			key (polprocessid, countyid, yearid, sourcetypeid, fueltypeid, modelyearid)
-- 		);
-- 		insert into expandeddefaultrecords (polprocessid, countyid, yearid, sourcetypeid, fueltypeid, modelyearid)
-- 		select distinct imc.polprocessid, imc.countyid, imc.yearid, imc.sourcetypeid, imc.fueltypeid, tempmodelyear.modelyearid
-- 		from ##defaultdatabase##.imcoverage imc
-- 		inner join tempmodelyear on (begmodelyearid <= modelyearid and modelyearid <= endmodelyearid)
-- 		inner join tempsourcefueltype using (sourcetypeid, fueltypeid)
-- 		where countyid in (##countyids##) and yearid in (##yearids##) and polprocessid in (##polprocessids##);
-- 
-- 		-- if any of the default records has no corresponding imported record, isok=0
-- 		set howmany = 0;
-- 		select d.polprocessid into howmany
-- 		from expandeddefaultrecords d
-- 		left outer join expandedimportrecords i using (polprocessid, countyid, yearid, sourcetypeid, fueltypeid, modelyearid)
-- 		where i.polprocessid is null
-- 		limit 1;
-- 		if(howmany > 0) then
-- 			set isok = 0;
-- 			insert into importtempmessages (message) values ('error: imported data does not yet cover all of the default cases');
-- 		end if;
-- 	end if;

	-- complain about imported active records that overlap
	-- expand the imported active records
	truncate table expandedimportrecords;
	insert into expandedimportrecords (polprocessid, countyid, yearid, sourcetypeid, fueltypeid, modelyearid)
	select imc.polprocessid, imc.countyid, imc.yearid, imc.sourcetypeid, imc.fueltypeid, tempmodelyear.modelyearid
	from imcoverage imc
	inner join tempmodelyear on (begmodelyearid <= modelyearid and modelyearid <= endmodelyearid)
	inner join tempsourcefueltype using (sourcetypeid, fueltypeid)
	where countyid in (##countyids##) and yearid in (##yearids##) and polprocessid in (##polprocessids##)
	and imc.useimyn='Y';

	insert into importtempmessages (message)
	select concat('error: duplicate active program for pol/proc ',polprocessid,' in county ',countyid,', year ',yearid,' for source type ',sourcetypeid,', fuel ',fueltypeid,', model year ',modelyearid) as errormessage
	from expandedimportrecords
	group by polprocessid, countyid, yearid, sourcetypeid, fueltypeid, modelyearid
	having count(*) > 1;

	if(isok=1) then
		set howmany=0;
		select count(*) into howmany from importtempmessages where message like 'error: duplicate active program%';
		set howmany=ifnull(howmany,0);
		if(howmany > 0) then
			set isok=0;
		end if;
	end if;

	-- complain about any years outside of moves's range
	insert into importtempmessages (message)
	select distinct concat('error: year ',yearid,' is outside the range of 1990-2060 and cannot be used') as errormessage
	from imcoverage
	where yearid < 1990 or yearid > 2060;
	if(isok=1) then
		set howmany=0;
		select count(*) into howmany from importtempmessages where message like 'error: year%';
		set howmany=ifnull(howmany,0);
		if(howmany > 0) then
			set isok=0;
		end if;
	end if;

	-- complain about evap/exhaust process and test mismatches
	insert into importtempmessages (message)
	select distinct concat('error: pollutant/process ',imc.polprocessid,' cannot use test standard ',imc.teststandardsid,' (',ts.teststandardsdesc,')') as errormessage
	from imcoverage imc
	inner join ##defaultdatabase##.imteststandards ts using (teststandardsid)
	inner join ##defaultdatabase##.pollutantprocessassoc ppa using (polprocessid)
	inner join ##defaultdatabase##.pollutant using (pollutantid)
	inner join ##defaultdatabase##.emissionprocess using (processid)
	where (left(teststandardsdesc,4)='Evap' and isaffectedbyevapim<>'Y')
	or (left(teststandardsdesc,4)<>'Evap' and isaffectedbyexhaustim<>'Y');
	if(isok=1) then
		set howmany=0;
		select count(*) into howmany from importtempmessages where message like '% cannot use test standard %';
		set howmany=ifnull(howmany,0);
		if(howmany > 0) then
			set isok=0;
		end if;
	end if;

	-- for county and project domains, the imcoverage table cannot be empty.
	if(mode = 2 and isok=1) then
		set howmany=0;
		select count(*) into howmany from imcoverage;
		set howmany=ifnull(howmany,0);
		if(howmany <= 0) then
			set isok=0;
		end if;
	end if;

	-- insert 'not_ready' or 'ok' to indicate iconic success
	if(mode >= 1) then
		insert into importtempmessages (message) values (case when isok=1 then 'OK' else 'NOT_READY' end);
	end if;

	drop table if exists tempsourcefueltype;
	drop table if exists tempmodelyear;
	drop table if exists expandedimportrecords;
	drop table if exists expandeddefaultrecords;
end
endblock

call spcheckimimporter();
drop procedure if exists spcheckimimporter;
