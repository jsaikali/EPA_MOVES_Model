-- author wesley faler
-- version 2017-09-30

drop procedure if exists spcheckidleimporter;

beginblock
create procedure spcheckidleimporter()
begin
	-- mode 0 is run after importing
	-- mode 1 is run to check overall success/failure
	declare mode int default ##mode##;
	declare isok int default 1;
	declare howmany int default 0;

	-- scale 0 is national
	-- scale 1 is single county
	-- scale 2 is project domain
	declare scale int default ##scale##;

	-- rate 0 is inventory
	-- rate 1 is rates
	declare rate int default ##rate##;

	-- complain if any model year ranges are inverted
	insert into importtempmessages (message)
	select distinct concat('error: totalidlefraction minmodelyearid (',minmodelyearid,') must be <= maxmodelyearid (',maxmodelyearid,')') as errormessage
	from totalidlefraction
	where minmodelyearid > maxmodelyearid;

	insert into importtempmessages (message)
	select distinct concat('error: idlemodelyeargrouping minmodelyearid (',minmodelyearid,') must be <= maxmodelyearid (',maxmodelyearid,')') as errormessage
	from idlemodelyeargrouping
	where minmodelyearid > maxmodelyearid;

	-- expand to full set of model years
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

	drop table if exists temptotalidlefraction;
	create table if not exists temptotalidlefraction (
		idleregionid int not null,
		countytypeid int not null,
		sourcetypeid smallint not null,
		monthid smallint not null,
		dayid smallint not null,
		modelyearid smallint not null,
		totalidlefraction double not null,
		key (idleregionid, countytypeid, sourcetypeid, monthid, dayid, modelyearid)
	);

	insert into temptotalidlefraction (idleregionid,countytypeid,sourcetypeid,monthid,dayid,modelyearid,totalidlefraction)
	select idleregionid,countytypeid,sourcetypeid,monthid,dayid,year as modelyearid,totalidlefraction
	from totalidlefraction, tempyear
	where minmodelyearid <= year
	and maxmodelyearid >= year;

	drop table if exists tempidlemodelyeargrouping;
	create table if not exists tempidlemodelyeargrouping (
		sourcetypeid smallint not null,
		modelyearid smallint not null,
		totalidlefraction double not null,
		key (sourcetypeid, modelyearid)
	);

	insert into tempidlemodelyeargrouping (sourcetypeid,modelyearid,totalidlefraction)
	select sourcetypeid,year as modelyearid,totalidlefraction
	from idlemodelyeargrouping, tempyear
	where minmodelyearid <= year
	and maxmodelyearid >= year;

	-- complain about model years that appear more than once
	insert into importtempmessages (message)
	select distinct concat('error: totalidlefraction model year ',modelyearid,
		' appears more than once (',count(*),') for idle region ',idleregionid,
		', county type ',countytypeid,', source type ',sourcetypeid,
		', month ',monthid,', day ',dayid) as errormessage
	from temptotalidlefraction
	group by idleregionid, countytypeid, sourcetypeid, monthid, dayid, modelyearid
	having count(*) > 1;

	insert into importtempmessages (message)
	select distinct concat('error: idlemodelyeargrouping model year ',modelyearid,
		' appears more than once (',count(*),') for source type ',sourcetypeid) as errormessage
	from tempidlemodelyeargrouping
	group by sourcetypeid, modelyearid
	having count(*) > 1;

	-- complain about a tif of 1
	insert into importtempmessages (message)
	select distinct concat('error: totalidlefraction is >= 1 for source type ', sourcetypeid) as errormessage
	from totalidlefraction tif
	where tif.totalidlefraction >= 1
	group by sourcetypeid;
	
	insert into importtempmessages (message)
	select distinct concat('error: totalidlefraction is >= 1 for source type ', sourcetypeid) as errormessage
	from idlemodelyeargrouping
	where totalidlefraction >= 1
	group by sourcetypeid;

	-- cleanup
	drop table if exists tempyear;
	drop table if exists temptotalidlefraction;
	drop table if exists tempidlemodelyeargrouping;

	-- check final status
	if(isok=1) then
		set howmany=0;
		select count(*) into howmany from importtempmessages where message like 'error: %';
		set howmany=ifnull(howmany,0);
		if(howmany > 0) then
			set isok=0;
		end if;
	end if;

	-- insert 'not_ready' or 'ok' to indicate iconic success
	if(mode = 1) then
		insert into importtempmessages (message) values (case when isok=1 then 'OK' else 'NOT_READY' end);
	end if;
end
endblock

call spcheckidleimporter();
drop procedure if exists spcheckidleimporter;
