-- adjust the values within the totalidlefraction table using
-- user-supplied adjustments by model year range, month, sourcetype, and day.
-- author wesley faler
-- version 2017-09-29

drop procedure if exists spadjusttotalidlefraction;

beginblock
create procedure spadjusttotalidlefraction()
begin
	declare howmanyimyg int default 0;
	
	-- create table totalidlefractioninitial select * from totalidlefraction;

	set howmanyimyg=0;
	select count(*) into howmanyimyg from idlemodelyeargrouping;
	set howmanyimyg=ifnull(howmanyimyg,0);

	if(howmanyimyg > 0) then
		-- this means the user supplied shaping tables and we can eliminate the default data
		truncate totalidlefraction;
		
		-- populate totalidlefraction from idlemodelyeargrouping
		insert into totalidlefraction(idleregionid,countytypeid,
			sourcetypeid,
			monthid,dayid,
			minmodelyearid, maxmodelyearid,
			totalidlefraction)
		select distinct st.idleregionid,c.countytypeid,
			imyg.sourcetypeid,
			m.monthid, d.dayid,
			imyg.minmodelyearid, imyg.maxmodelyearid,
			imyg.totalidlefraction
		from idlemodelyeargrouping imyg,
		county c, state st,
		runspecmonth m, runspecday d;
	end if;
	
	-- apply idlemonthadjust
	update totalidlefraction
	inner join idlemonthadjust using (sourcetypeid, monthid)
	set totalidlefraction = totalidlefraction * idlemonthadjust;

	-- apply idledayadjust
	update totalidlefraction
	inner join idledayadjust using (sourcetypeid, dayid)
	set totalidlefraction = totalidlefraction * idledayadjust;
end
endblock

call spadjusttotalidlefraction();
drop procedure if exists spadjusttotalidlefraction;
