-- author wesley faler
-- version 2009-12-05

drop procedure if exists spcheckzoneroadtypeimporter;

beginblock
create procedure spcheckzoneroadtypeimporter()
begin
	-- mode 0 is run after importing
	-- mode 1 is run to check overall success/failure
	declare mode int default ##mode##;
	declare isok int default 1;
	declare howmany int default 0;

	insert into importtempmessages (message)
	select concat('error: road type ',roadtypeid,' shoallocfactor is not 1.0 but instead ',round(sum(shoallocfactor),4)) as errormessage
	from zoneroadtype
	group by roadtypeid
	having round(sum(shoallocfactor),4) <> 1.0000;

	if(isok=1) then
		set howmany=0;
		select count(*) into howmany from importtempmessages where message like 'error: road type%';
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

call spcheckzoneroadtypeimporter();
drop procedure if exists spcheckzoneroadtypeimporter;
