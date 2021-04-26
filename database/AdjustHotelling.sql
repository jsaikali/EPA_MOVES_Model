-- adjust the distribution within the hotellinghours table using
-- user-supplied adjustments.
-- author wesley faler
-- version 2017-09-19

drop procedure if exists spadjusthotelling;

beginblock
create procedure spadjusthotelling()
begin
	declare targetzoneid int default ##zoneid##;
	declare targetyearid int default ##yearid##;
	declare activityzoneid int default ##activityzoneid##;
	declare howmany int default 0;
	
	-- hotellinghoursperday
	set howmany=0;
	select count(*) into howmany from hotellinghoursperday where zoneid=targetzoneid;
	set howmany=ifnull(howmany,0);
	if(howmany > 0) then
		alter table hotellinghours add column dayid smallint not null default 0;
		alter table hotellinghours add column hourid smallint not null default 0;

		drop table if exists newhotellinghours_hhpd;
		drop table if exists defaulthotellinghoursperday;

		create table newhotellinghours_hhpd like hotellinghours;
		create table defaulthotellinghoursperday (
			yearid int,
			zoneid int,
			dayid smallint,
			defaulthotellinghours double -- this is units of hotelling hours in a typical day
		);

		update hotellinghours set dayid=mod(hourdayid,10), hourid=floor(hourdayid/10)
		where zoneid = targetzoneid and yearid = targetyearid;
		
		-- calculate the defaults
		insert into defaulthotellinghoursperday 
		select yearid,zoneid,dayid,sum(hotellinghours / noofrealdays) 
		from hotellinghours
		join dayofanyweek using (dayid)
		group by yearid,zoneid,dayid;
		
		
		-- we use the ratio between the defaults and user input to calculate the new hotellinghours
		insert into newhotellinghours_hhpd (sourcetypeid,yearid,monthid,dayid,hourid,hourdayid,zoneid,ageid,hotellinghours)
		-- units of hotellinghours : hours per portion of week
		-- units of (hotellinghoursperday / defaulthotellinghours): hours per typical day / hours per typical day, month combination (implictly day*month)
		-- expression below becomes hours per potion of week * (1/ typical month) * months
		select sourcetypeid,yearid,monthid,dayid,hourid,hourid*10+dayid as hourdayid,zoneid,ageid,
			(hotellinghoursperday / defaulthotellinghours) * hotellinghours * 12 as hotellinghours
		from hotellinghours
		join defaulthotellinghoursperday using (yearid,zoneid,dayid)
		join hotellinghoursperday using (yearid,zoneid,dayid)
		join dayofanyweek using (dayid)
		join monthofanyyear using (monthid);
		
		-- replace into with the new data
		replace into hotellinghours select * from newhotellinghours_hhpd;

		drop table newhotellinghours_hhpd;
		drop table defaulthotellinghoursperday;
		
		alter table hotellinghours drop column dayid;
		alter table hotellinghours drop column hourid;
	end if;


	-- hotellinghourfraction
	set howmany=0;
	select count(*) into howmany from hotellinghourfraction where zoneid=targetzoneid;
	set howmany=ifnull(howmany,0);
	if(howmany > 0) then
		alter table hotellinghours add column dayid smallint not null default 0;
		alter table hotellinghours add column hourid smallint not null default 0;

		drop table if exists newhotellinghours_hhf;
		drop table if exists defaulthotellinghourfraction;

		create table newhotellinghours_hhf like hotellinghours;
		-- alter table hotellingtemp drop primary key;

		update hotellinghours set dayid=mod(hourdayid,10), hourid=floor(hourdayid/10);
		
		-- calculate the default hour fractions
		create table defaulthotellinghourfraction (
			zoneid int,
			dayid smallint,
			hourid smallint,
			defaulthourfraction double
		);
		insert into defaulthotellinghourfraction
		select zoneid,dayid,hourid,sum(hotellinghours) / total.total as defaulthourfraction
		from hotellinghours
		join (
			select zoneid,dayid,sum(hotellinghours) as total from hotellinghours
			group by zoneid,dayid
		) as total using (zoneid,dayid)
		group by zoneid,dayid,hourid;
		
		-- we use the ratio between the defaults and user input to calculate the new hotellinghours
		insert into newhotellinghours_hhf (sourcetypeid,yearid,monthid,dayid,hourid,hourdayid,zoneid,ageid,hotellinghours)
		-- because we are just scaling by new fractions, the hotellinghours units are preserved
		-- (hourfraction / defaulthourfraction) is unitless
		select sourcetypeid,yearid,monthid,dayid,hourid,hourid*10+dayid as hourdayid,zoneid,ageid, 
			hotellinghours * (hourfraction / defaulthourfraction) as hotellinghours
		from hotellinghours
		join defaulthotellinghourfraction using (zoneid,dayid,hourid)
		join hotellinghourfraction using (zoneid,dayid,hourid);

		-- replace into with the new values
		replace into hotellinghours select * from newhotellinghours_hhf;

		drop table newhotellinghours_hhf;
		drop table defaulthotellinghourfraction;

		alter table hotellinghours drop column dayid;
		alter table hotellinghours drop column hourid;
	end if;

	-- hotellingagefraction
	set howmany=0;
	select count(*) into howmany from hotellingagefraction where zoneid=targetzoneid;
	set howmany=ifnull(howmany,0);
	if(howmany > 0) then
		drop table if exists newhotellinghours_had;
		drop table if exists defaulthotellingagefraction;
		
		alter table hotellinghours add column dayid smallint not null default 0;
		alter table hotellinghours add column hourid smallint not null default 0;
		update hotellinghours set dayid=mod(hourdayid,10), hourid=floor(hourdayid/10);

		create table newhotellinghours_had like hotellinghours;
		create table defaulthotellingagefraction (
			zoneid int,
			ageid smallint,
			defaultagefraction double
		);
		
		
		-- calculate the default age fraction
		insert into defaulthotellingagefraction
		select zoneid,ageid,sum(hotellinghours) / total.total as defaultagefraction
		from hotellinghours
		join (
			select zoneid,sum(hotellinghours) as total from hotellinghours
			group by zoneid
		) as total using (zoneid)
		group by zoneid,ageid;
		
		-- use the ratio of the new age fractions to the defaults to scale the hotelling horus
		insert into newhotellinghours_had (sourcetypeid,yearid,monthid,dayid,hourid,hourdayid,zoneid,ageid,hotellinghours)
		-- because we are just scaling by new fractions, the hotellinghours units are preserved
		-- (agefraction / defaultagefraction) is unitless
		select sourcetypeid,yearid,monthid,dayid,hourid,hourid*10+dayid as hourdayid,zoneid,ageid, 
			hotellinghours * (agefraction / defaultagefraction) as hotellinghours
		from hotellinghours
		join defaulthotellingagefraction using (zoneid,ageid)
		join hotellingagefraction using (zoneid,ageid);
			
			
		-- replace into with the new data
		replace into hotellinghours select * from newhotellinghours_had;
		
		-- drop table newhotellinghours_had;
		drop table defaulthotellingagefraction;
		
		alter table hotellinghours drop column dayid;
		alter table hotellinghours drop column hourid;
	end if;

	
	-- hotellingmonthadjust
	set howmany=0;
	select count(*) into howmany from hotellingmonthadjust where zoneid=targetzoneid;
	set howmany=ifnull(howmany,0);
	if(howmany > 0) then
		alter table hotellinghours add column dayid smallint not null default 0;
		alter table hotellinghours add column hourid smallint not null default 0;
		update hotellinghours set dayid=mod(hourdayid,10), hourid=floor(hourdayid/10);
		
		drop table if exists newhotellinghours_hma;
		drop table if exists defaulthotellingmonthadjust;

		create table newhotellinghours_hma like hotellinghours;
		create table defaulthotellingmonthadjust (
			zoneid int,
			monthid smallint,
			defaultmonthadjustment double
		);
		
		-- calculate the default month adjustments
		insert into defaulthotellingmonthadjust
		-- the month adjustment is the ratio between the month's average hours and the average across all months (aka the full year)
		select zoneid,monthid,avg(hotellinghours)/average.yearaverage as defaultmonthadjustment
		from hotellinghours
		join (
			select zoneid,avg(hotellinghours) as yearaverage 
			from hotellinghours
			group by zoneid
		) as average
		using (zoneid)
		group by zoneid,monthid;
		
		-- use the ratio of the new month adjustments to the old ones to calculate the new hotelling hours
		insert into newhotellinghours_hma (sourcetypeid,yearid,monthid,dayid,hourid,hourdayid,zoneid,ageid,hotellinghours)
		-- because we are just scaling by new fractions, the hotellinghours units are preserved
		-- (monthadjustment / defaultmonthadjustment) is unitless
		select sourcetypeid,yearid,monthid,dayid,hourid,hourid*10+dayid as hourdayid,zoneid,ageid, 
			hotellinghours * (monthadjustment / defaultmonthadjustment) as hotellinghours
		from hotellinghours
		join defaulthotellingmonthadjust using (zoneid,monthid)
		join hotellingmonthadjust using (zoneid,monthid);
		
		-- replace into with the new data
		replace into hotellinghours select * from newhotellinghours_hma;
		
		drop table newhotellinghours_hma;
		drop table defaulthotellingmonthadjust;
		
		alter table hotellinghours drop column dayid;
		alter table hotellinghours drop column hourid;
	end if;
end
endblock

call spadjusthotelling();
drop procedure if exists spadjusthotelling;
