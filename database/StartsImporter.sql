-- author wesley faler
-- author john covey
-- version 2018-03-20

drop procedure if exists spcheckstartsimporter;

beginblock
create procedure spcheckstartsimporter()
begin
	-- scale 0 is national
	-- scale 1 is single county
	-- scale 2 is project domain
	declare scale int default ##scale##;

	declare howmany int default 0;
	declare startscnt int default 0;
	declare startsperdaycnt int default 0;
	declare startsperdaypervehiclecnt int default 0;
	
	-- startsageadjustment
	set howmany=0;
	select count(*) into howmany from startsageadjustment;
	set howmany=ifnull(howmany,0);
	if(howmany > 0) then
		insert into importtempmessages (message)	
		select distinct
			case
			  when ac.ageid is null then  concat('error: startsageadjustment with ageid ',s.ageid,' is unknown')
			  when sut.sourcetypeid is null then  concat('error: startsageadjustment with sourcetypeid ',s.sourcetypeid,' is unknown')
		    else null end as message
		from startsageadjustment s
		left join ##defaultdatabase##.agecategory ac
		  on s.ageid = ac.ageid
		left join ##defaultdatabase##.sourceusetype sut
		  on s.sourcetypeid = sut.sourcetypeid
		  where ac.ageid is null or sut.sourcetypeid is null;

		insert into importtempmessages (message)
		select concat('error: startsageadjustment with age ',ageid,', source type ',sourcetypeid,' has ageadjustment < 0') as message
		from startsageadjustment
		where ageadjustment < 0;
		
		insert into importtempmessages (message)
		select concat('error: startsageadjustment with age ',ageid,', source type ',sourcetypeid,' has null ageadjustment') as message
		from startsageadjustment
		where ageadjustment is null;
		
	end if;
	
	-- startshourfraction
	set howmany=0;
	select count(*) into howmany from startshourfraction;
	set howmany=ifnull(howmany,0);
	if(howmany > 0) then
		insert into importtempmessages (message)	
		select distinct
			case
			  when d.dayid is null then  concat('error: startshourfraction with dayid ',s.dayid,' is unknown')
			  when hoad.hourid is null then  concat('error: startshourfraction with hourid ',s.hourid,' is unknown')
			  when sut.sourcetypeid is null then  concat('error: startshourfraction with sourcetypeid ',s.sourcetypeid,' is unknown')
		    else null end as message
		from startshourfraction s
		left join ##defaultdatabase##.dayofanyweek d
		  on s.dayid = d.dayid
		left join ##defaultdatabase##.hourofanyday hoad
		  on s.hourid = hoad.hourid
		left join ##defaultdatabase##.sourceusetype sut
		  on s.sourcetypeid = sut.sourcetypeid
		  where d.dayid is null or hoad.hourid is null or sut.sourcetypeid is null;
		
		-- if not all hours are selected, you don't need to import data for all of them
		-- therefore, only check to make sure it doesn't sum to more than 1
		insert into importtempmessages (message)
		select concat('error: startshourfraction with day ',dayid,', source type ',sourcetypeid,' allocation fraction sums to ',round(sum(allocationfraction),4)) as message
		from startshourfraction
		group by dayid, sourcetypeid
		having round(sum(allocationfraction),4) > 1.0000;

		insert into importtempmessages (message)
		select concat('error: startshourfraction with day ',dayid,', source type ',sourcetypeid,', hour ',hourid,' has allocationfraction < 0') as message
		from startshourfraction
		where allocationfraction < 0;
		
		insert into importtempmessages (message)
		select concat('error: startshourfraction with day ',dayid,', source type ',sourcetypeid,', hour ',hourid,' has null allocationfraction') as message
		from startshourfraction
		where allocationfraction is null;
		
		-- if not all hours are selected, you don't need to import data for all of them, so this is just a warning
		insert into importtempmessages (message)
		select concat('warning: startshourfraction with source type ',sourcetypeid,' and day ',dayid,' has missing hours') as message
		  from (
			select shf.sourcetypeid,shf.dayid,count(distinct shf.hourid) houridcnt from startshourfraction shf
			join ##defaultdatabase##.hourday hd
			  on hd.dayid = shf.dayid
			  and hd.hourid = shf.hourid
			group by shf.sourcetypeid, shf.dayid) a
		where houridcnt <> 24;
		
		-- if all hours are imported, then do the check for < 1 (check for >1 always happens)
		insert into importtempmessages (message)
		select concat('error: startshourfraction with day ',shf.dayid,', source type ',shf.sourcetypeid,' allocation fraction sums to ',round(sum(allocationfraction),4),
		              ' even though all hours are imported') as message
		from startshourfraction shf
			join ##defaultdatabase##.hourday hd
			  on hd.dayid = shf.dayid
			  and hd.hourid = shf.hourid
		group by shf.dayid, shf.sourcetypeid
		having round(sum(allocationfraction),4) < 1.0000 and count(distinct shf.hourid) = 24;

	end if;

	-- startsperdaypervehicle
	set howmany=0;
	select count(*) into howmany from startsperdaypervehicle;
	set howmany=ifnull(howmany,0);
	if(howmany > 0) then
		insert into importtempmessages (message)
		select distinct
			case
			  when d.dayid is null then  concat('error: startsperdaypervehicle with dayid ',s.dayid,' is unknown')
			  when sut.sourcetypeid is null then  concat('error: startsperdaypervehicle with sourcetypeid ',s.sourcetypeid,' is unknown')
		    else null end as message
		from startsperdaypervehicle s
		left join ##defaultdatabase##.dayofanyweek d
		  on s.dayid = d.dayid
		left join ##defaultdatabase##.sourceusetype sut
		  on s.sourcetypeid = sut.sourcetypeid
		  where d.dayid is null or sut.sourcetypeid is null;
	  
		insert into importtempmessages (message)
		select concat('error: startsperdaypervehicle with day ',dayid,', source type ',sourcetypeid,' has startsperdaypervehicle < 0') as message
		from startsperdaypervehicle
		where startsperdaypervehicle < 0;

		insert into importtempmessages (message)
		select concat('error: startsperdaypervehicle with day ',dayid,', source type ',sourcetypeid,' has null startsperdaypervehicle') as message
		from startsperdaypervehicle
		where startsperdaypervehicle is null;
		
	end if;

	-- startsperday
	set howmany=0;
	select count(*) into howmany from startsperday;
	set howmany=ifnull(howmany,0);
	if(howmany > 0) then
		insert into importtempmessages (message)
		select distinct
			case
			  when d.dayid is null then  concat('error: startsperday with dayid ',s.dayid,' is unknown')
			  when sut.sourcetypeid is null then  concat('error: startsperday with sourcetypeid ',s.sourcetypeid,' is unknown')
		    else null end as message
		from startsperday s
		left join ##defaultdatabase##.dayofanyweek d
		  on s.dayid = d.dayid
		left join ##defaultdatabase##.sourceusetype sut
		  on s.sourcetypeid = sut.sourcetypeid
		  where d.dayid is null or sut.sourcetypeid is null;
	  
		insert into importtempmessages (message)
		select concat('error: startsperday with day ',dayid,', source type ',sourcetypeid,' has startsperday < 0') as message
		from startsperday
		where startsperday < 0;
	  
		insert into importtempmessages (message)
		select concat('error: startsperday with day ',dayid,', source type ',sourcetypeid,' has null startsperday') as message
		from startsperday
		where startsperday is null;
	end if;

	-- startsmonthadjust
	set howmany=0;
	select count(*) into howmany from startsmonthadjust;
	set howmany=ifnull(howmany,0);
	if(howmany > 0) then
		insert into importtempmessages (message)
		select distinct
			case
			  when m.monthid is null then  concat('error: startsmonthadjust with monthid ',s.monthid,' is unknown')
			  when sut.sourcetypeid is null then  concat('error: startsmonthadjust with sourcetypeid ',s.sourcetypeid,' is unknown')
		    else null end as message
		from startsmonthadjust s
		left join ##defaultdatabase##.monthofanyyear m
		  on s.monthid = m.monthid
		left join ##defaultdatabase##.sourceusetype sut
		  on s.sourcetypeid = sut.sourcetypeid
		  where m.monthid is null or sut.sourcetypeid is null;
	  
		insert into importtempmessages (message)
		select concat('error: startsmonthadjust with month ',monthid,', source type ',sourcetypeid,' has monthadjustment < 0') as message
		from startsmonthadjust
		where monthadjustment < 0;
	  
		insert into importtempmessages (message)
		select concat('error: startsmonthadjust with month ',monthid,', source type ',sourcetypeid,' has null monthadjustment') as message
		from startsmonthadjust
		where monthadjustment is null;
	end if;

	-- startsopmodedistribution
	set howmany=0;
	select count(*) into howmany from startsopmodedistribution;
	set howmany=ifnull(howmany,0);
	if(howmany > 0) then
		insert into importtempmessages (message)
		select distinct
			case
			  when d.dayid is null then  concat('error: startsopmodedistribution with dayid ',s.dayid,' is unknown')
			  when hoad.hourid is null then  concat('error: startsopmodedistribution with hourid ',s.hourid,' is unknown')
			  when sut.sourcetypeid is null then  concat('error: startsopmodedistribution with sourcetypeid ',s.sourcetypeid,' is unknown')
			  when ac.ageid is null then  concat('error: startsopmodedistribution with ageid ',s.ageid,' is unknown')
			  when om.opmodeid is null then  concat('error: startsopmodedistribution with opmodeid ',s.opmodeid,' is unknown')
		    else null end as message
		from startsopmodedistribution s
		left join ##defaultdatabase##.dayofanyweek d
		  on s.dayid = d.dayid
		left join ##defaultdatabase##.hourofanyday hoad
		  on s.hourid = hoad.hourid
		left join ##defaultdatabase##.sourceusetype sut
		  on s.sourcetypeid = sut.sourcetypeid
		left join ##defaultdatabase##.agecategory ac
		  on s.ageid = ac.ageid
		left join ##defaultdatabase##.operatingmode om
		  on s.opmodeid = om.opmodeid
		  where d.dayid is null or hoad.hourid is null or sut.sourcetypeid is null or ac.ageid is null or om.opmodeid is null;
	  
		insert into importtempmessages (message)
		select concat('error: startsopmodedistribution with day ',dayid,', hour ,',hourid,'source type ',sourcetypeid,', age ',ageid,' op mode fraction is not 1.0 but instead ',round(sum(opmodefraction),4)) as message
		from startsopmodedistribution
		group by dayid,hourid,sourcetypeid, ageid
		having round(sum(opmodefraction),4) <> 1.0000;
		
		insert into importtempmessages (message)
		select concat('error: startsopmodedistribution with day ',dayid,',hour ',hourid,',source type ',sourcetypeid,',age ',ageid,',op mode ',opmodeid,' has value < 0') as message
		from startsopmodedistribution
		where opmodefraction < 0;		

		insert into importtempmessages (message)
		select concat('error: startsopmodedistribution with day ',dayid,',hour ',hourid,',source type ',sourcetypeid,',age ',ageid,',op mode ',opmodeid,' has null value') as message
		from startsopmodedistribution
		where opmodefraction is null;		

		select count(*) into howmany from ##defaultdatabase##.operatingmode
		where minsoaktime is not null or maxsoaktime is not null;

		insert into importtempmessages (message)
		select concat('error: startsopmodedistribution with day ',dayid,', hour ',hourid, ', source type ',sourcetypeid,', and age ',ageid,' has missing op mode') as message
		  from (
			select somd.dayid, somd.hourid,somd.sourcetypeid,somd.ageid,count(distinct somd.opmodeid) opmodeidcnt 
			from startsopmodedistribution somd
			join ##defaultdatabase##.operatingmode om
			  on om.opmodeid = somd.opmodeid
      	   where om.minsoaktime is not null or om.maxsoaktime is not null
           group by somd.dayid, somd.hourid,somd.sourcetypeid,somd.ageid) a
		where opmodeidcnt <> howmany;

	end if;
	
	-- starts
	set howmany=0;
	select count(*) into howmany from starts;
	set howmany=ifnull(howmany,0);
	if(howmany > 0) then
		insert into importtempmessages (message)
		select distinct
			case
			  when hd.hourdayid is null then  concat('error: starts with hourdayid ',s.hourdayid,' is unknown')
			  when m.monthid is null then  concat('error: starts with monthid ',s.monthid,' is unknown')
			  when ac.ageid is null then  concat('error: starts with ageid ',s.ageid,' is unknown')
			  when sut.sourcetypeid is null then  concat('error: starts with sourcetypeid ',s.sourcetypeid,' is unknown')
		    else null end as message
		from starts s
		left join ##defaultdatabase##.hourday hd
		  on s.hourdayid = hd.hourdayid
		left join ##defaultdatabase##.monthofanyyear m
		  on s.monthid = m.monthid
		left join ##defaultdatabase##.agecategory ac
		  on s.ageid = ac.ageid
		left join ##defaultdatabase##.sourceusetype sut
		  on s.sourcetypeid = sut.sourcetypeid
		  where hd.hourdayid is null or m.monthid is null or ac.ageid is null or sut.sourcetypeid is null;
	  
		insert into importtempmessages (message)
		select concat('error: starts with hourday ',hourdayid,', month ',monthid,', year ',yearid,', age ',ageid,
		              ', zone ',zoneid,', source type ',sourcetypeid,' has starts < 0') as message
		from starts
		where starts < 0;
	  
		insert into importtempmessages (message)
		select concat('error: starts with hourday ',hourdayid,', month ',monthid,', year ',yearid,', age ',ageid,
		              ', zone ',zoneid,', source type ',sourcetypeid,' has null starts') as message
		from starts
		where starts is null;
	end if;
	
	select count(*) into startscnt from starts where isuserinput = 'Y';
	select count(*) into startsperdaycnt from startsperday;
	select count(*) into startsperdaypervehiclecnt from startsperdaypervehicle;
	
	-- see stackoverflow.com/questions/33378732 for "dual" table
	if (startscnt > 0 and startsperdaycnt > 0 and startsperdaypervehiclecnt > 0) then
		insert into importtempmessages (message)
			select 'error: all three of the starts, startsperday, and startsperdaypervehicle tables have been imported. moves can only run with one of them.' from dual;
	elseif (startscnt > 0 and startsperdaypervehiclecnt > 0) then
		insert into importtempmessages (message)
			select 'error: the starts and startsperdaypervehicle tables have both been imported. moves can only run with one of them.' from dual;
	elseif (startscnt > 0 and startsperdaycnt > 0) then
		insert into importtempmessages (message)
			select 'error: the starts and startsperday tables have both been imported. moves can only run with one of them.' from dual;
	elseif (startsperdaycnt > 0 and startsperdaypervehiclecnt > 0) then
		insert into importtempmessages (message)
			select 'error: the startsperday and startsperdaypervehicle tables have both been imported. moves can only run with one of them.' from dual;
	end if;

end
endblock

call spcheckstartsimporter();
drop procedure if exists spcheckstartsimporter;
