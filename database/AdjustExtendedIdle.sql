-- adjust the distribution within the extendedidlehours table using
-- user-supplied adjustments.
-- author wesley faler
-- version 2017-09-19

drop procedure if exists spadjustextendedidle;

beginblock
create procedure spadjustextendedidle()
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
		alter table extendedidlehours add column dayid smallint not null default 0;
		alter table extendedidlehours add column hourid smallint not null default 0;

		drop table if exists newextendedidlehours_hhpd;
		drop table if exists defaultextendedidlehoursperday;

		create table newextendedidlehours_hhpd like extendedidlehours;
		create table defaultextendedidlehoursperday (
			yearid int,
			zoneid int,
			dayid smallint,
			defaultextendedidlehours double -- this is units of hotelling hours in a typical day
		);

		update extendedidlehours set dayid=mod(hourdayid,10), hourid=floor(hourdayid/10)
		where zoneid = targetzoneid and yearid = targetyearid;
		
		-- calculate the defaults
		insert into defaultextendedidlehoursperday 
		select yearid,zoneid,dayid,sum(extendedidlehours / noofrealdays) 
		from extendedidlehours
		join dayofanyweek using (dayid)
		group by yearid,zoneid,dayid;
		
		
		-- we use the ratio between the defaults and user input to calcualte the new extendedidlehours
		insert into newextendedidlehours_hhpd (sourcetypeid,yearid,monthid,dayid,hourid,hourdayid,zoneid,ageid,extendedidlehours)
		-- units of extendedidlehours : hours per portion of week
		-- units of (extendedidlehoursperday / defaultextendedidlehours): hours per typical day / hours per typical day, month combination (implictly day*month)
		-- expression below becomes hours per potion of week * (1/ typical month) * months
		select sourcetypeid,yearid,monthid,dayid,hourid,hourid*10+dayid as hourdayid,eih.zoneid,ageid,
			(hotellinghoursperday / defaultextendedidlehours) * extendedidlehours * 12 * opmodefraction as extendedidlehours
			-- sum(case when opmodefraction>0 then (hotellinghoursperday / defaultextendedidlehours) * extendedidlehours * 12 * opmodefraction else 0 end)
		from extendedidlehours as eih
		join defaultextendedidlehoursperday using (yearid,zoneid,dayid)
		join hotellinghoursperday using (yearid,zoneid,dayid)
		join dayofanyweek using (dayid)
		join monthofanyyear using (monthid)
		join hotellingactivitydistribution had on (
			beginmodelyearid <= yearid - ageid
			and endmodelyearid >= yearid - ageid
			and opmodeid = 200
			and had.zoneid = activityzoneid
		);
		
		-- replace into with the new data
		replace into extendedidlehours select * from newextendedidlehours_hhpd;


		drop table newextendedidlehours_hhpd;
		drop table defaultextendedidlehoursperday;
		
		alter table extendedidlehours drop column dayid;
		alter table extendedidlehours drop column hourid;
	end if;


	-- hotellinghourfraction
	set howmany=0;
	select count(*) into howmany from hotellinghourfraction where zoneid=targetzoneid;
	set howmany=ifnull(howmany,0);
	if(howmany > 0) then
		alter table extendedidlehours add column dayid smallint not null default 0;
		alter table extendedidlehours add column hourid smallint not null default 0;

		drop table if exists newextendedidlehours_hhf;
		drop table if exists defaulthotellinghourfraction;

		create table newextendedidlehours_hhf like extendedidlehours;
		-- alter table hotellingtemp drop primary key;

		update extendedidlehours set dayid=mod(hourdayid,10), hourid=floor(hourdayid/10);
		
		-- calculate the default hour fractions
		create table defaulthotellinghourfraction (
			zoneid int,
			dayid smallint,
			hourid smallint,
			defaulthourfraction double
		);
		insert into defaulthotellinghourfraction
		select zoneid,dayid,hourid,sum(extendedidlehours) / total.total as defaulthourfraction
		from extendedidlehours
		join (
			select zoneid,dayid,sum(extendedidlehours) as total from extendedidlehours
			group by zoneid,dayid
		) as total using (zoneid,dayid)
		group by zoneid,dayid,hourid;
		
		-- we use the ratio between the defaults and user input to calcualte the new extendedidlehours
		insert into newextendedidlehours_hhf (sourcetypeid,yearid,monthid,dayid,hourid,hourdayid,zoneid,ageid,extendedidlehours)
		-- becuase we are just scaling by new fractions, the extendedidlehours units are preserved
		-- (hourfraction / defaulthourfraction) is unitless
		select sourcetypeid,yearid,monthid,dayid,hourid,hourid*10+dayid as hourdayid,zoneid,ageid, 
			extendedidlehours * (hourfraction / defaulthourfraction) as extendedidlehours
		from extendedidlehours
		join defaulthotellinghourfraction using (zoneid,dayid,hourid)
		join hotellinghourfraction using (zoneid,dayid,hourid);

		-- replace into with the new values
		replace into extendedidlehours select * from newextendedidlehours_hhf;
		
		drop table newextendedidlehours_hhf;
		drop table defaulthotellinghourfraction;

		alter table extendedidlehours drop column dayid;
		alter table extendedidlehours drop column hourid;
	end if;

	-- hotellingagefraction
	set howmany=0;
	select count(*) into howmany from hotellingagefraction where zoneid=targetzoneid;
	set howmany=ifnull(howmany,0);
	if(howmany > 0) then
		drop table if exists newextendedidlehours_had;
		drop table if exists defaulthotellingagefraction;
		
		alter table extendedidlehours add column dayid smallint not null default 0;
		alter table extendedidlehours add column hourid smallint not null default 0;
		update extendedidlehours set dayid=mod(hourdayid,10), hourid=floor(hourdayid/10);

		create table newextendedidlehours_had like extendedidlehours;
		create table defaulthotellingagefraction (
			zoneid int,
			ageid smallint,
			defaultagefraction double
		);
		
		
		-- calculate the default age fraction
		insert into defaulthotellingagefraction
		select zoneid,ageid,sum(extendedidlehours) / total.total as defaultagefraction
		from extendedidlehours
		join (
			select zoneid,sum(extendedidlehours) as total from extendedidlehours
			group by zoneid
		) as total using (zoneid)
		group by zoneid,ageid;
		
		-- use the ratio of the new age fractions to the defaults to scale the hotelling horus
		insert into newextendedidlehours_had (sourcetypeid,yearid,monthid,dayid,hourid,hourdayid,zoneid,ageid,extendedidlehours)
		-- becuase we are just scaling by new fractions, the extendedidlehours units are preserved
		-- (agefraction / defaultagefraction) is unitless
		select sourcetypeid,yearid,monthid,dayid,hourid,hourid*10+dayid as hourdayid,zoneid,ageid, 
			extendedidlehours * (agefraction / defaultagefraction) as extendedidlehours
		from extendedidlehours
		join defaulthotellingagefraction using (zoneid,ageid)
		join hotellingagefraction using (zoneid,ageid);
			
			
		-- replace into with the new data
		replace into extendedidlehours select * from newextendedidlehours_had;
				
		drop table newextendedidlehours_had;
		drop table defaulthotellingagefraction;
		
		alter table extendedidlehours drop column dayid;
		alter table extendedidlehours drop column hourid;
	end if;

	
	-- hotellingmonthadjust
	set howmany=0;
	select count(*) into howmany from hotellingmonthadjust where zoneid=targetzoneid;
	set howmany=ifnull(howmany,0);
	if(howmany > 0) then
		alter table extendedidlehours add column dayid smallint not null default 0;
		alter table extendedidlehours add column hourid smallint not null default 0;
		update extendedidlehours set dayid=mod(hourdayid,10), hourid=floor(hourdayid/10);
		
		drop table if exists newextendedidlehours_hma;
		drop table if exists defaulthotellingmonthadjust;

		create table newextendedidlehours_hma like extendedidlehours;
		create table defaulthotellingmonthadjust (
			zoneid int,
			monthid smallint,
			defaultmonthadjustment double
		);
		
		-- calculate the default month adjustments
		insert into defaulthotellingmonthadjust
		-- the month adjustment is the ratio between the month's average hours and the average across all months (aka the full year)
		select zoneid,monthid,avg(extendedidlehours)/average.yearaverage as defaultmonthadjustment
		from extendedidlehours
		join (
			select zoneid,avg(extendedidlehours) as yearaverage 
			from extendedidlehours
			group by zoneid
		) as average
		using (zoneid)
		group by zoneid,monthid;
		
		-- use the ratio of the new month adjustments to the old ones to calcualte the new hotelling hours
		insert into newextendedidlehours_hma (sourcetypeid,yearid,monthid,dayid,hourid,hourdayid,zoneid,ageid,extendedidlehours)
		-- becuase we are just scaling by new fractions, the extendedidlehours units are preserved
		-- (monthadjustment / defaultmonthadjustment) is unitless
		select sourcetypeid,yearid,monthid,dayid,hourid,hourid*10+dayid as hourdayid,zoneid,ageid, 
			extendedidlehours * (monthadjustment / defaultmonthadjustment) as extendedidlehours
		from extendedidlehours
		join defaulthotellingmonthadjust using (zoneid,monthid)
		join hotellingmonthadjust using (zoneid,monthid);
		
		-- replace into with the new data
		replace into extendedidlehours select * from newextendedidlehours_hma;
		
		drop table newextendedidlehours_hma;
		drop table defaulthotellingmonthadjust;
		
		alter table extendedidlehours drop column dayid;
		alter table extendedidlehours drop column hourid;
	end if;
end
endblock

call spadjustextendedidle();
drop procedure if exists spadjustextendedidle;
