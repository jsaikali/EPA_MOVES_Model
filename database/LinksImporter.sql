-- version 2012-02-18
-- author wesley faler

drop procedure if exists spchecklinkimporter;

beginblock
create procedure spchecklinkimporter()
begin
	-- mode 0 is run after importing
	-- mode 1 is run to check overall success/failure
	declare mode int default ##mode##;
	declare isok int default 1;
	declare howmany int default 0;

	-- check for missing road types
	insert into importtempmessages (message)
	select concat('error: link ',linkid,' is missing its roadtypeid') as errormessage
	from link
	where (roadtypeid is null or roadtypeid <= 0)
	order by linkid;

	if(isok=1) then
		set howmany=0;
		select count(*) into howmany from importtempmessages where message like 'error: link % is missing its roadtypeid';
		set howmany=ifnull(howmany,0);
		if(howmany > 0) then
			set isok=0;
		end if;
	end if;

	-- check for negative average speeds
	insert into importtempmessages (message)
	select concat('error: link ',linkid,' average speed (',linkavgspeed,') cannot be negative') as errormessage
	from link
	where (linkavgspeed < 0)
	order by linkid;

	if(isok=1) then
		set howmany=0;
		select count(*) into howmany from importtempmessages where message like 'error: link % average speed (%) cannot be negative';
		set howmany=ifnull(howmany,0);
		if(howmany > 0) then
			set isok=0;
		end if;
	end if;

	-- remind users that drive schedules will override any link average speed or grade
	insert into importtempmessages (message)
	select concat('info: link ',linkid,' will obtain average speed and grade from its driving schedule') as errormessage
	from link
	where linkid in (select distinct linkid from driveschedulesecondlink)
	order by linkid;

	-- note missing data
	insert into importtempmessages (message)
	select concat('error: link ',linkid,' is missing average speed, operating modes, and/or a drive schedule') as errormessage
	from link
	where linkavgspeed is null
	and linkid not in (select distinct linkid from opmodedistribution)
	and linkid not in (select distinct linkid from driveschedulesecondlink)
	order by linkid;

	insert into importtempmessages (message)
	select concat('error: link ',linkid,' is missing average speed but has operating mode data') as errormessage
	from link
	where linkavgspeed is null
	and linkid in (select distinct linkid from opmodedistribution)
	and linkid not in (select distinct linkid from driveschedulesecondlink)
	order by linkid;

	insert into importtempmessages (message)
	select concat('error: link ',linkid,' is missing average grade and cannot interpolate a drive schedule') as errormessage
	from link
	where linkavgspeed is not null and linkavggrade is null
	and linkid not in (select distinct linkid from opmodedistribution)
	and linkid not in (select distinct linkid from driveschedulesecondlink)
	order by linkid;

	insert into importtempmessages (message)
	select distinct concat('error: zone ',zoneid,' is missing off-network data') as errormessage
	from link
	where roadtypeid = 1
	and zoneid not in (select distinct zoneid from offnetworklink)
	order by zoneid;

	insert into importtempmessages (message)
	select concat('error: zone ',zoneid,' has more than 1 off-network link') as message
	from link
	where roadtypeid = 1
	group by zoneid
	having count(*) > 1;

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

call spchecklinkimporter();
drop procedure if exists spchecklinkimporter;
