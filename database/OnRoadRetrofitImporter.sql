-- author wesley faler
-- version 2012-10-01

drop procedure if exists spcheckonroadretrofitimporter;

beginblock
create procedure spcheckonroadretrofitimporter()
begin
	-- mode 0 is run after importing
	-- mode 1 is run to check overall success/failure for national domain
	-- mode 2 is run to check overall success/failure for county and project domains
	declare mode int default ##mode##;
	declare isok int default 1;
	declare howmany int default 0;

	-- the beginmodelyearid must be earlier or the same as the retrofityearid.  if not, an error message shall appear and the tab header shall show a red x.
	insert into importtempmessages (message)
	select distinct concat('error: beginmodelyearid ',beginmodelyearid,' must be the same or before the calendar year ',retrofityearid) as errormessage
	from onroadretrofit
	where beginmodelyearid > retrofityearid;

	-- the endmodelyearid must be earlier or the same as the retrofityearid.  if not, an error message shall appear and the tab header shall show a red x.
	insert into importtempmessages (message)
	select distinct concat('error: endmodelyearid ',endmodelyearid,' must be the same or before the calendar year ',retrofityearid) as errormessage
	from onroadretrofit
	where endmodelyearid > retrofityearid;

	-- the endmodelyearid must be the same or after the beginmodelyearid.  if not, an error message shall appear and the tab header shall show a red x.
	insert into importtempmessages (message)
	select distinct concat('error: endmodelyearid ',endmodelyearid,' must be the same or after beginmodelyearid ',beginmodelyearid) as errormessage
	from onroadretrofit
	where endmodelyearid < beginmodelyearid;

	-- the new input is cumfractionretrofit and is the total retrofit coverage at a given calendar year. it cannot be greater than 1.0 or less than 0.0.  if it is, an error message shall appear and the tab header shall show a red x.
	insert into importtempmessages (message)
	select distinct concat('error: cumfractionretrofit (',round(cumfractionretrofit,4),') must be less than or equal to 1.0000') as errormessage
	from onroadretrofit
	where round(cumfractionretrofit,4) > 1.0000;

	insert into importtempmessages (message)
	select distinct concat('error: cumfractionretrofit (',round(cumfractionretrofit,4),') must be greater than or equal to 0.0000') as errormessage
	from onroadretrofit
	where round(cumfractionretrofit,4) < 0.0000;

	-- the retrofiteffectivefraction must not be greater than 1.  if it is, an error message shall appear and the tab header shall show a red x.
	insert into importtempmessages (message)
	select distinct concat('error: retrofiteffectivefraction (',round(retrofiteffectivefraction,4),') must be less than or equal to 1.0000') as errormessage
	from onroadretrofit
	where round(retrofiteffectivefraction,4) > 1.0000;

	-- the retrofiteffectivefraction can be less than 0 (i.e. negative).  if it is, a note message shall appear stating that the retrofit program will result in increased emissions.
	insert into importtempmessages (message)
	select distinct concat('note: retrofiteffectivefraction (',round(retrofiteffectivefraction,4),') will increase emissions') as errormessage
	from onroadretrofit
	where round(retrofiteffectivefraction,4) < 0.0000;

	if(isok=1) then
		set howmany=0;
		select count(*) into howmany from importtempmessages where message like 'error:%';
		set howmany=ifnull(howmany,0);
		if(howmany > 0) then
			set isok=0;
		end if;
	end if;

	-- insert 'not_ready' or 'ok' to indicate iconic success
	if(mode >= 1) then
		insert into importtempmessages (message) values (case when isok=1 then 'OK' else 'NOT_READY' end);
	end if;
end
endblock

call spcheckonroadretrofitimporter();
drop procedure if exists spcheckonroadretrofitimporter;
