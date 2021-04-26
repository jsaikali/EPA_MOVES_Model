-- author wesley faler
-- version 2017-09-19

drop procedure if exists spcheckhotellingimporter;

beginblock
create procedure spcheckhotellingimporter()
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

	-- complain about an empty table in project domain
	if(scale = 2 and (90 in (##processids##) or 91 in (##processids##))) then
		set howmany=0;
		select count(*) into howmany from hotellingactivitydistribution;
		set howmany=ifnull(howmany,0);
		if(howmany <= 0) then
			insert into importtempmessages (message) values ('error: hotellingactivitydistribution must be provided.');
		end if;
	end if;

	-- check hotellinghourfraction if any entries are provided
	set howmany=0;
	select count(*) into howmany from hotellinghourfraction;
	set howmany=ifnull(howmany,0);
	if(howmany > 0) then
		-- complain about zone/days with distributions that don't sum to exactly 1.0000
		insert into importtempmessages (message)
		select concat('error: total hotellinghourfraction.hourfraction for zone ',zoneid,', day ',dayid,' should be 1 but instead is ',round(sum(hourfraction),4)) as errormessage
		from hotellinghourfraction
		group by zoneid, dayid
		having round(sum(hourfraction),4) <> 1.0000;
	end if;

	-- check hotellingagefraction if any entries are provided
	set howmany=0;
	select count(*) into howmany from hotellingagefraction;
	set howmany=ifnull(howmany,0);
	if(howmany > 0) then
		-- complain about zones with distributions that don't sum to exactly 1.0000
		insert into importtempmessages (message)
		select concat('error: total hotellingagefraction.agefraction for zone ',zoneid,' should be 1 but instead is ',round(sum(agefraction),4)) as errormessage
		from hotellingagefraction
		group by zoneid
		having round(sum(agefraction),4) <> 1.0000;
	end if;

	-- complain about invalid operating modes
	insert into importtempmessages (message)
	select distinct concat('error: unknown opmodeid (',opmodeid,'). hotelling operating modes are 200-299.') as errormessage
	from hotellingactivitydistribution
	where opmodeid < 200 || opmodeid > 299;

	-- complain if any model year ranges are inverted
	insert into importtempmessages (message)
	select distinct concat('error: beginmodelyearid (',beginmodelyearid,') must be <= endmodelyearid (',endmodelyearid,')') as errormessage
	from hotellingactivitydistribution
	where beginmodelyearid > endmodelyearid;

	-- complain about entries with negative fractions
	insert into importtempmessages (message)
	select concat('error: opmodefraction is less than zero (',opmodefraction,') for model years ',beginmodelyearid,' to ',endmodelyearid) as errormessage
	from hotellingactivitydistribution
	where opmodefraction < 0;

	-- complain about entries with fractions greater than 1
	insert into importtempmessages (message)
	select concat('error: opmodefraction is greater than 1 (',round(opmodefraction,4),') for model years ',beginmodelyearid,' to ',endmodelyearid) as errormessage
	from hotellingactivitydistribution
	where round(opmodefraction,4) > 1;

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

	drop table if exists temphotellingactivitydistribution;
	create table if not exists temphotellingactivitydistribution (
		zoneid int not null,
		modelyearid smallint(6) not null,
		opmodeid smallint(6) not null,
		opmodefraction float not null,
		key (zoneid, modelyearid, opmodeid),
		key (zoneid, opmodeid, modelyearid)
	);

	insert into temphotellingactivitydistribution (zoneid, modelyearid, opmodeid, opmodefraction)
	select zoneid, year, opmodeid, opmodefraction
	from hotellingactivitydistribution, tempyear
	where beginmodelyearid <= year
	and endmodelyearid >= year;

	-- complain about model years that appear more than once
	insert into importtempmessages (message)
	select distinct concat('error: model year ',modelyearid,' appears more than once (',count(*),') for zone ',zoneid) as errormessage
	from temphotellingactivitydistribution
	group by zoneid, modelyearid, opmodeid
	having count(*) > 1;

	-- complain about model years with distributions that don't sum to exactly 1.0000
	insert into importtempmessages (message)
	select concat('error: total opmodefraction for zone ',zoneid,', model year ',modelyearid,' should be 1 but instead ',round(sum(opmodefraction),4)) as errormessage
	from temphotellingactivitydistribution
	group by zoneid, modelyearid
	having round(sum(opmodefraction),4) <> 1.0000;

	-- cleanup
	drop table if exists tempyear;
	drop table if exists temphotellingactivitydistribution;

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

call spcheckhotellingimporter();
drop procedure if exists spcheckhotellingimporter;
