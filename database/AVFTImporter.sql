-- author wesley faler
-- author don smith
-- version 2011-08-17

drop procedure if exists spcheckavftimporter;

beginblock
create procedure spcheckavftimporter()
begin
	-- mode 0 is run after importing
	-- mode 1 is run to check overall success/failure
	declare mode int default ##mode##;
	declare isok int default 1;
	declare howmany int default 0;

	insert into importtempmessages (message)
	select concat('error: source type ',sourcetypeid,', model year ',modelyearid,', fuel engine fraction is more than 1.0, being ',round(sum(fuelengfraction),4))
	from avft 
	group by sourcetypeid, modelyearid
	having round(sum(fuelengfraction),4) > 1.0000;

	insert into importtempmessages (message)
	select concat('error: source type ',sourcetypeid,', model year ',modelyearid,', fuel ',fueltypeid,', engine ',engtechid,', fuel engine fraction is less than 0.0, being ',round(fuelengfraction,4))
	from avft
	where round(fuelengfraction,4) < 0.0000;

	insert into importtempmessages (message)
	select concat('warning: source type ',sourcetypeid,', model year ',modelyearid,', fuel engine fraction is not 1.0 but instead ',round(sum(fuelengfraction),4))
	from avft 
	group by sourcetypeid, modelyearid
	having round(sum(fuelengfraction),4) < 1.0000 and round(sum(fuelengfraction),4) > 0.0000;

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
		insert into importtempmessages (message) values (case when isok=1 then 'ok' else 'not_ready' end);
	end if;
end
endblock

call spcheckavftimporter();
drop procedure if exists spcheckavftimporter;
