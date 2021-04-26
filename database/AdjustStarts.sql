-- adjust the distribution within the starts table using
-- user-supplied adjustments by month, hour, day, sourcetype, and age.

-- author wesley faler
-- author john covey
-- version 2018-07-09

-- @algorithm starts = population * startsperdaypervehicle * combinedageeffectfraction * monthadjustment * allocationfraction
-- @owner starts

drop procedure if exists spadjuststarts;

beginblock
create procedure spadjuststarts()
begin
	declare targetyearid int default ##yearid##;
	declare targetzoneid int default ##zoneid##;
	
	declare startscnt int default 0;
	declare startsperdaycnt int default 0;
	declare startsperdaypervehiclecnt int default 0;
	select count(*) into startscnt from starts where isuserinput = 'Y';
	select count(*) into startsperdaycnt from startsperday;
	select count(*) into startsperdaypervehiclecnt from startsperdaypervehicle;
	
	if (startscnt > 0 and startsperdaypervehiclecnt > 0) then
		-- startsperdaypervehiclecnt is always >0 because it contains default data (which could be overwritten by user;
		-- 		in either case, it has more than 0 rows).
		-- starts will have more than 0 rows only if it was directly imported by the user. if this is the case, do not
		--      calculate the starts table (this is done in the else clause).
		-- note: used to emit a warning message when this case was triggered. however, this case will always be triggered
		--      when importing the starts table, and it is impossible to tell if a user imported both the starts table and
		--      the startsperdaypervehicle table, or just the starts table, so this message could be misleading.
		-- insert into tempmessages (message)
		--		select 'warning : detected that user has imported starts table. these values will be used instead of startsperdaypervehicle or startsperday.' from dual;					
		
		-- em - in order to avoid errors, we need to do something in this if-then. for now, i think we want to keep it around in case we do want to 
		-- log this warning at some point, so i just put in a select 1; to make sql feel happy about doing something.
		select 1;
		
	else 
		-- tempstartsperday will hold startsperday data, which could either be directly imported by the user, or calculated from
		-- startsperdaypervehicle (which has either user imported data or default data)
		drop table if exists tempstartsperday;
		create table tempstartsperday (
		  dayid smallint(6) not null default 0,
		  sourcetypeid smallint(6) not null default 0,
		  startsperday double default null,
		  primary key (sourcetypeid,dayid),
		  key hourdayid (dayid),
		  key sourcetypeid (sourcetypeid)
		);
		
		if (startsperdaycnt > 0) then
			-- this data was directly imported by the user; just need to transfer from user input table to temp table
			-- note: used to emit a warning message when this happened. however, this case is no longer possible to
			--       trigger accidentally as importer gui forbids it.
			-- insert into tempmessages (message)
			--   select 'warning : detected that user has imported startsperday. these values will be used instead of startsperdaypervehicle.' from dual;					
			insert into tempstartsperday (dayid, sourcetypeid, startsperday) 
				select dayid, sourcetypeid, startsperday from startsperday;			
		else
			-- need to calculate startsperday from startsperdaypervehicle and vehicle populations
			insert into tempstartsperday (dayid, sourcetypeid, startsperday)
				  select distinct spdpv.dayid as dayid,
					spdpv.sourcetypeid as sourcetypeid,
					sty.sourcetypepopulation * spdpv.startsperdaypervehicle as startsperday
				from startsperdaypervehicle spdpv
				inner join sourcetypeyear sty on
					sty.sourcetypeid = spdpv.sourcetypeid
				where sty.yearid = targetyearid;
		
		end if;
		
		-- calculate the combined age effects (age distribution and age adjustments)
		-- first, normalize the age adjustment (so it becomes a distributive effect instead of a multiplicative adjustment)
		drop table if exists tempstartsnormalizedageadjust;
		create table tempstartsnormalizedageadjust (
		  sourcetypeid smallint(6) not null default 0,
		  ageid smallint(6) not null default 0,
		  normalizedageadjustment double default null,
		  primary key (sourcetypeid,ageid),
		  key sourcetypeid (sourcetypeid),
		  key ageid (ageid)
		);
		insert into tempstartsnormalizedageadjust (sourcetypeid, ageid, normalizedageadjustment)
			select sourcetypeid, ageid, ageadjustment/totalageadjustment as normalizedageadjustment
			from startsageadjustment saa
			join (select sourcetypeid, sum(ageadjustment) as totalageadjustment 
				  from startsageadjustment group by sourcetypeid) as taa using (sourcetypeid);
			
		-- then, combine the age distribution with the normalized age adjustment and renormalize so that the new parameter
		-- distributes the calculated starts across ages. the window function calculates the sumproduct of the age distribution and the normalized
		-- age adjustment factor (element-wise multiplication and summing across all ages for each source type).
		drop table if exists tempstartscombinedageeffect;
		create table tempstartscombinedageeffect (
		  sourcetypeid smallint(6) not null default 0,
		  ageid smallint(6) not null default 0,
		  combinedageeffectfraction double default null,
		  primary key (sourcetypeid,ageid),
		  key sourcetypeid (sourcetypeid),
		  key ageid (ageid)
		);
		insert into tempstartscombinedageeffect (sourcetypeid, ageid, combinedageeffectfraction)
			select stad.sourcetypeid, stad.ageid, agefraction * normalizedageadjustment / sumproduct
			from sourcetypeagedistribution stad
			join tempstartsnormalizedageadjust as tsnaa on (stad.sourcetypeid = tsnaa.sourcetypeid and stad.ageid = tsnaa.ageid and stad.yearid = targetyearid)
			join (select stad.sourcetypeid, sum(agefraction * normalizedageadjustment) as sumproduct
				  from sourcetypeagedistribution stad
				  join tempstartsnormalizedageadjust as tsnaa on (stad.sourcetypeid = tsnaa.sourcetypeid and stad.ageid = tsnaa.ageid and stad.yearid = targetyearid)
				  group by stad.sourcetypeid
				  ) as sumproduct on (stad.sourcetypeid = sumproduct.sourcetypeid);
			
		
		-- starts = startsperday * combinedageeffectfraction * monthadjustment * hourallocationfraction * startallocfactor
		insert into starts (
			hourdayid, monthid, yearid, ageid,
			zoneid, sourcetypeid, starts, startscv, isuserinput
			)
		select distinct
			(shf.hourid*10+shf.dayid) as hourdayid,
			sma.monthid as monthid,
			targetyearid as yearid,
			tscae.ageid as ageid,
			targetzoneid as zoneid,
			tspd.sourcetypeid as sourcetypeid,
		   (tspd.startsperday * tscae.combinedageeffectfraction * sma.monthadjustment * shf.allocationfraction
			   * ifnull(z.startallocfactor, 1)) as starts,
		    0 as startscv,
		   'N' as isuserinput
		from tempstartsperday tspd
		inner join startsmonthadjust sma on
			sma.sourcetypeid = tspd.sourcetypeid  
		inner join startshourfraction shf on
			shf.sourcetypeid = tspd.sourcetypeid
			and shf.dayid = tspd.dayid
		inner join tempstartscombinedageeffect tscae on
			tscae.sourcetypeid = tspd.sourcetypeid
		left join zone z on
			z.zoneid = targetzoneid;
	end if;
	
	-- starts algorithm assumes that the starts table contains starts as a portion of the week
	-- (i.e., starts for weekdays need to be multiplied by 5)
	update starts s
		inner join hourday hd on s.hourdayid = hd.hourdayid
		inner join dayofanyweek doa on hd.dayid = doa.dayid
		set s.starts = s.starts * doa.noofrealdays;
	
	drop table if exists tempstartsperday;
	drop table if exists tempstartsnormalizedageadjust;
	drop table if exists tempstartscombinedageeffect;
end
endblock

call spadjuststarts();
drop procedure if exists spadjuststarts;
