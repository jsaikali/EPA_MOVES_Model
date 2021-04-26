-- author wesley faler
-- author don smith
-- version 2014-04-08

drop procedure if exists spcheckzoneimporter;

beginblock
create procedure spcheckzoneimporter()
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

	declare desiredallocfactor double default 1;
	declare howmanyzones int default 0;

	-- build links for imported zones but not for the project domain (scale=2)
	if(scale <> 2) then
		delete from link;

		insert ignore into link (linkid, countyid, zoneid, roadtypeid)
		select (z.zoneid*10 + roadtypeid) as linkid, z.countyid, z.zoneid, roadtypeid
		from ##defaultdatabase##.roadtype, zone z;
	end if;

	-- complain if alloc factors are not 1.0.
	set howmanyzones=0;
	select count(*) into howmanyzones from zone z inner join county c on c.countyid=z.countyid;
	set howmanyzones=ifnull(howmanyzones,0);
	if(howmanyzones > 0) then
		set desiredallocfactor = 1.0;
	else
		insert into importtempmessages (message)
		select concat('error: no zones imported for county ',countyid) as errormessage
		from county;
	end if;

	insert into importtempmessages (message)
	select concat('error: zone startallocfactor is not ',round(desiredallocfactor,4),' but instead ',round(sum(startallocfactor),4)) as errormessage
	from zone
	having round(sum(startallocfactor),4) <> round(desiredallocfactor,4);

	insert into importtempmessages (message)
	select concat('error: zone idleallocfactor is not ',round(desiredallocfactor,4),' but instead ',round(sum(idleallocfactor),4)) as errormessage
	from zone
	having round(sum(idleallocfactor),4) <> round(desiredallocfactor,4);

	insert into importtempmessages (message)
	select concat('error: zone shpallocfactor is not ',round(desiredallocfactor,4),' but instead ',round(sum(shpallocfactor),4)) as errormessage
	from zone
	having round(sum(shpallocfactor),4) <> round(desiredallocfactor,4);

	insert into importtempmessages (message)
	select concat('error: road type ',roadtypeid,' shoallocfactor is not ',round(desiredallocfactor,4),' but instead ',round(sum(shoallocfactor),4)) as errormessage
	from zoneroadtype
	group by roadtypeid
	having round(sum(shoallocfactor),4) <> round(desiredallocfactor,4);

	-- complain if sums exceed 1.0000
	insert into importtempmessages (message)
	select concat('error: zone startallocfactor exceeds 1.0, being instead ',round(sum(startallocfactor),4)) as errormessage
	from zone
	having round(sum(startallocfactor),4) > 1.0000;

	insert into importtempmessages (message)
	select concat('error: zone idleallocfactor exceeds 1.0, being instead ',round(sum(idleallocfactor),4)) as errormessage
	from zone
	having round(sum(idleallocfactor),4) > 1.0000;

	insert into importtempmessages (message)
	select concat('error: zone shpallocfactor exceeds 1.0, being instead ',round(sum(shpallocfactor),4)) as errormessage
	from zone
	having round(sum(shpallocfactor),4) > 1.0000;

	insert into importtempmessages (message)
	select concat('error: road type ',roadtypeid,' shoallocfactor exceeds 1.0, being instead ',round(sum(shoallocfactor),4)) as errormessage
	from zoneroadtype
	group by roadtypeid
	having round(sum(shoallocfactor),4) > 1.0000;

	-- complain if sums are 0.0 or less
	insert into importtempmessages (message)
	select concat('error: zone startallocfactor should not be zero, being instead ',round(sum(startallocfactor),4)) as errormessage
	from zone
	having round(sum(startallocfactor),4) <= 0.0000;

	insert into importtempmessages (message)
	select concat('error: zone idleallocfactor should not be zero, being instead ',round(sum(idleallocfactor),4)) as errormessage
	from zone
	having round(sum(idleallocfactor),4) <= 0.0000;

	insert into importtempmessages (message)
	select concat('error: zone shpallocfactor should not be zero, being instead ',round(sum(shpallocfactor),4)) as errormessage
	from zone
	having round(sum(shpallocfactor),4) <= 0.0000;

	insert into importtempmessages (message)
	select concat('error: road type ',roadtypeid,' shoallocfactor should not be zero, being instead ',round(sum(shoallocfactor),4)) as errormessage
	from zoneroadtype
	group by roadtypeid
	having round(sum(shoallocfactor),4) <= 0.0000;

	-- complain about negative allocation factors
	insert into importtempmessages (message)
	select concat('error: zone ',zoneid,' startallocfactor is negative, being ',round(startallocfactor,4)) as errormessage
	from zone
	where round(startallocfactor,4) < 0.0000;

	insert into importtempmessages (message)
	select concat('error: zone ',zoneid,' idleallocfactor is negative, being ',round(idleallocfactor,4)) as errormessage
	from zone
	where round(idleallocfactor,4) < 0.0000;

	insert into importtempmessages (message)
	select concat('error: zone ',zoneid,' shpallocfactor is negative, being ',round(shpallocfactor,4)) as errormessage
	from zone
	where round(shpallocfactor,4) < 0.0000;

	insert into importtempmessages (message)
	select concat('error: zone ',zoneid,' road type ',roadtypeid,' shoallocfactor is negative, being ',round(shoallocfactor,4)) as errormessage
	from zoneroadtype
	where round(shoallocfactor,4) < 0.0000;

	-- zoneroadtype table should not be empty
	insert into importtempmessages (message)
	select concat('error: zoneroadtype references ',zrtzonecount,' zones but should reference ',zonecount,' instead') as errormessage
	from (
	select (select count(*) from zone) as zonecount,
		(select count(distinct zoneid)
		from zoneroadtype
		inner join zone using (zoneid)) as zrtzonecount
	) t
	where zonecount <> zrtzonecount;

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

call spcheckzoneimporter();
drop procedure if exists spcheckzoneimporter;
