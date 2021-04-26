-- --------------------------------------------------------------------------------------
-- this script is run by the moves gui to calculate default hours of off-network idling
--     (oni) activity for use with a rates mode run. it must be run with a fully populated
--     county input database.
-- the results are temporarily stored in a table called onitooloutput in the input database.
--     this table is managed by the moves gui and deleted after the results are saved elsewhere.
-- the moves gui replaces:
--     ##defaultdb## with the name of the default database
--     ##inputdb## with the name of the county input database
--     ##tempdb## with a temporary database name for all intermediate calculations
-- messages to be displayed to the user are inserted into the onitempmessages table, which
--     has one varchar(1000) column. this table is managed by the moves gui.
-- --------------------------------------------------------------------------------------

drop procedure if exists onitool;

beginblock
create procedure onitool()
onitool_procedure: begin
	declare howmany int default 0;

	-- ------------------------------
	-- start ready to calculate block
	-- ------------------------------
	set howmany=0;
	select count(*) into howmany from ##inputdb##.roadtypedistribution;
	set howmany=ifnull(howmany,0);
	if(howmany = 0) then
		insert into ##tempdb##.onitempmessages (message) values ('error: roadtypedistribution must be provided.');
	end if;

	set howmany=0;
	select count(*) into howmany from ##inputdb##.county;
	set howmany=ifnull(howmany,0);
	if(howmany = 0) then
		insert into ##tempdb##.onitempmessages (message) values ('error: county table must be provided.');
	end if;

	set howmany=0;
	select count(*) into howmany from ##inputdb##.state;
	set howmany=ifnull(howmany,0);
	if(howmany = 0) then
		insert into ##tempdb##.onitempmessages (message) values ('error: state table must be provided.');
	end if;

	set howmany=0;
	select count(*) into howmany from ##inputdb##.avgspeeddistribution;
	set howmany=ifnull(howmany,0);
	if(howmany = 0) then
		insert into ##tempdb##.onitempmessages (message) values ('error: avgspeeddistribution must be provided.');
	end if;

	set howmany=0;
	select count(*) into howmany from ##inputdb##.sourcetypeyear;
	set howmany=ifnull(howmany,0);
	if(howmany = 0) then
		insert into ##tempdb##.onitempmessages (message) values ('error: sourcetypeyear must be provided.');
	end if;

	set howmany=0;
	select count(*) into howmany from ##inputdb##.sourcetypeagedistribution;
	set howmany=ifnull(howmany,0);
	if(howmany = 0) then
		insert into ##tempdb##.onitempmessages (message) values ('error: sourcetypeagedistribution must be provided.');
	end if;

	set howmany=0;
	select count(*) into howmany from ##inputdb##.hourvmtfraction;
	set howmany=ifnull(howmany,0);
	if(howmany = 0) then
		insert into ##tempdb##.onitempmessages (message) values ('error: hourvmtfraction must be provided.');
	end if;

	set howmany=0;
	select sum(c) into howmany from (select count(*) as c from ##inputdb##.sourcetypedayvmt
											  union select count(*) as c from ##inputdb##.sourcetypeyearvmt
										   union select count(*) as c from ##inputdb##.hpmsvtypeday
										   union select count(*) as c from ##inputdb##.hpmsvtypeyear) as t1;
	set howmany=ifnull(howmany,0);
	if(howmany = 0) then
		insert into ##tempdb##.onitempmessages (message) values ('error: vmt input must be provided via sourcetypedayvmt, sourcetypeyearvmt, hpmsvtypeday, or hpmsvtypeyear.');
	end if;

	set howmany=0;
	select count(*) into howmany from ##inputdb##.sourcetypeyearvmt;
	set howmany=ifnull(howmany,0);
	if(howmany > 0) then
		set howmany=0;
		select count(*) into howmany from ##inputdb##.monthvmtfraction;
		set howmany=ifnull(howmany,0);
		if(howmany = 0) then
			insert into ##tempdb##.onitempmessages (message) values ('error: monthvmtfraction must be provided when using sourcetypeyearvmt.');
		end if;
		set howmany=0;
		select count(*) into howmany from ##inputdb##.dayvmtfraction;
		set howmany=ifnull(howmany,0);
		if(howmany = 0) then
			insert into ##tempdb##.onitempmessages (message) values ('error: dayvmtfraction must be provided when using sourcetypeyearvmt.');
		end if;
	end if;

	set howmany=0;
	select count(*) into howmany from ##inputdb##.hpmsvtypeyear;
	set howmany=ifnull(howmany,0);
	if(howmany > 0) then
		set howmany=0;
		select count(*) into howmany from ##inputdb##.monthvmtfraction;
		set howmany=ifnull(howmany,0);
		if(howmany = 0) then
			insert into ##tempdb##.onitempmessages (message) values ('error: monthvmtfraction must be provided when using hpmsvtypeyear.');
		end if;
		set howmany=0;
		select count(*) into howmany from ##inputdb##.dayvmtfraction;
		set howmany=ifnull(howmany,0);
		if(howmany = 0) then
			insert into ##tempdb##.onitempmessages (message) values ('error: dayvmtfraction must be provided when using hpmsvtypeyear.');
		end if;
	end if;

	set howmany=0;
	select count(*) into howmany from ##inputdb##.idlemodelyeargrouping;
	set howmany=ifnull(howmany,0);
	if(howmany > 0) then
		set howmany=0;
		select count(*) into howmany from ##inputdb##.totalidlefraction;
		set howmany=ifnull(howmany,0);
		if(howmany > 0) then
			insert into ##tempdb##.onitempmessages (message) values ('error: cannot use both totalidlefraction and idlemodelyeargrouping (choose one or the other).');
		end if;
	end if;
	
	-- exit this stored procedure if there are any error messages at this point
	set howmany=0;
	select count(*) into howmany from ##tempdb##.onitempmessages where message like '%error%';
	set howmany=ifnull(howmany,0);
	if(howmany > 0) then
		leave onitool_procedure;
	end if;
	-- ----------------------------
	-- end ready to calculate block
	-- ----------------------------

	-- ---------------------------------
	-- start get totalidlefraction block (from /database/adjusttotalidlefraction.sql)
	-- ---------------------------------
	
	-- start with user totalidlefractions
	create table if not exists ##tempdb##.totalidlefraction like ##inputdb##.totalidlefraction;
	insert into ##tempdb##.totalidlefraction select * from ##inputdb##.totalidlefraction;
	
	-- grab the default totalidlefractions if the user did not enter their own
	set howmany=0;
	select count(*) into howmany from ##tempdb##.totalidlefraction;
	set howmany=ifnull(howmany,0);
	if(howmany = 0) then
		insert into ##tempdb##.totalidlefraction (sourcetypeid, minmodelyearid, maxmodelyearid, monthid, dayid, idleregionid, countytypeid, totalidlefraction)
			select sourcetypeid, minmodelyearid, maxmodelyearid, monthid, dayid, idleregionid, countytypeid, totalidlefraction
			from ##inputdb##.county
			join ##inputdb##.state
			join ##defaultdb##.totalidlefraction using (countytypeid, idleregionid);
	end if;
	
	-- apply shaping tables if the user supplied them
	set howmany=0;
	select count(*) into howmany from ##inputdb##.idlemodelyeargrouping;
	set howmany=ifnull(howmany,0);
	if(howmany > 0) then		
		-- eliminate the default data
		truncate table ##tempdb##.totalidlefraction;
		
		-- populate totalidlefraction from idlemodelyeargrouping
		insert into ##tempdb##.totalidlefraction (idleregionid,countytypeid,sourcetypeid,monthid,dayid,minmodelyearid,maxmodelyearid, totalidlefraction)
		select distinct st.idleregionid,c.countytypeid,imyg.sourcetypeid, m.monthid, d.dayid, imyg.minmodelyearid, imyg.maxmodelyearid, imyg.totalidlefraction
		from ##inputdb##.idlemodelyeargrouping imyg
		join ##inputdb##.county c
		join ##inputdb##.state st
		join ##defaultdb##.monthofanyyear m
		join ##defaultdb##.dayofanyweek d;
		
		-- apply idlemonthadjust
		update ##tempdb##.totalidlefraction
		inner join ##inputdb##.idlemonthadjust using (sourcetypeid, monthid)
		set totalidlefraction = totalidlefraction * idlemonthadjust;

		-- apply idledayadjust
		update ##tempdb##.totalidlefraction
		inner join ##inputdb##.idledayadjust using (sourcetypeid, dayid)
		set totalidlefraction = totalidlefraction * idledayadjust;
	end if;
	-- ---------------------------------
	-- end get totalidlefraction block
	-- ---------------------------------

	-- -----------------------------------------
	-- start totalactivitygenerator calculations (from /gov/otaq/moves/master/implementation/ghg/totalactivitygenerator.java)
	-- -----------------------------------------

	-- create all intermediate tables
	create table if not exists ##tempdb##.shobyageroadwayhour (
		yearid         smallint not null,
		roadtypeid     smallint not null,
		sourcetypeid   smallint not null,
		ageid          smallint not null,
		monthid        smallint not null,
		dayid          smallint not null,
		hourid         smallint not null,
		hourdayid      smallint not null default 0,
		sho            double not null,
		vmt            double not null,
		unique index xpkshobyageroadwayhour (yearid, roadtypeid, sourcetypeid, ageid, monthid, dayid, hourid));
	truncate ##tempdb##.shobyageroadwayhour;

	create table if not exists ##tempdb##.startsbyagehour (
		yearid         smallint not null,
		sourcetypeid   smallint not null,
		ageid          smallint not null,
		monthid        smallint not null,
		dayid          smallint not null,
		hourid         smallint not null,
		starts         double not null,
		unique index xpkstartsbyagehour (yearid, sourcetypeid, ageid, monthid, dayid, hourid));
	truncate ##tempdb##.startsbyagehour;

	create table if not exists ##tempdb##.vmtbyageroadwayhour (
		yearid        smallint not null,
		roadtypeid    smallint not null,
		sourcetypeid  smallint not null,
		ageid         smallint not null,
		monthid       smallint not null,
		dayid         smallint not null,
		hourid        smallint not null,
		vmt           double not null,
		hourdayid     smallint not null default 0,
		unique index xpkvmtbyageroadwayhour(yearid, roadtypeid, sourcetypeid, ageid, monthid, dayid, hourid));
	truncate ##tempdb##.vmtbyageroadwayhour;

	create table if not exists ##tempdb##.vmtbymyroadhourfraction (
		yearid smallint not null,
		roadtypeid smallint not null,
		sourcetypeid smallint not null,
		modelyearid smallint not null,
		monthid smallint not null,
		dayid smallint not null,
		hourid smallint not null,
		hourdayid smallint not null,
		vmtfraction double,
		unique key (yearid, roadtypeid, sourcetypeid, modelyearid, monthid, hourid, dayid),
		unique key (yearid, roadtypeid, sourcetypeid, modelyearid, monthid, hourdayid));
	truncate ##tempdb##.vmtbymyroadhourfraction;

	create table if not exists ##tempdb##.hpmsvtypepopulation (
		yearid       smallint not null,
		hpmsvtypeid  smallint not null,
		population   float not null,
		unique index xpkhpmsvtypepopulation(yearid, hpmsvtypeid));
	truncate ##tempdb##.hpmsvtypepopulation;

	create table if not exists ##tempdb##.fractionwithinhpmsvtype (
		yearid       smallint not null,
		sourcetypeid smallint not null,
		ageid        smallint not null,
		fraction     float not null,
		unique index xpkfractionwithinhpmsvtype (yearid, sourcetypeid, ageid));
	truncate ##tempdb##.fractionwithinhpmsvtype;

	create table if not exists ##tempdb##.hpmstravelfraction (
		yearid      smallint not null,
		hpmsvtypeid smallint not null,
		fraction    float not null,
		unique index xpkhpmstravelfraction (yearid, hpmsvtypeid));
	truncate ##tempdb##.hpmstravelfraction;

	create table if not exists ##tempdb##.travelfraction (
		yearid        smallint not null,
		sourcetypeid  smallint not null,
		ageid         smallint not null,
		fraction      float not null,
		unique index xpktravelfraction(yearid, sourcetypeid, ageid));
	truncate ##tempdb##.travelfraction;

	create table if not exists ##tempdb##.annualvmtbyageroadway (
		yearid        smallint not null,
		roadtypeid    smallint not null,
		sourcetypeid  smallint not null,
		ageid         smallint not null,
		vmt           float not null,
		unique index xpkannualvmtbyageroadway(yearid, roadtypeid, sourcetypeid, ageid));
	truncate ##tempdb##.annualvmtbyageroadway;

	create table if not exists ##tempdb##.averagespeed (
		roadtypeid    smallint not null,
		sourcetypeid  smallint not null,
		dayid         smallint not null,
		hourid        smallint not null,
		averagespeed  float not null,
		unique index xpkaveragespeed (roadtypeid, sourcetypeid, dayid, hourid));
	truncate ##tempdb##.averagespeed;

	create table if not exists ##tempdb##.shobyageday (
		yearid         smallint not null,
		sourcetypeid   smallint not null,
		ageid          smallint not null,
		monthid        smallint not null,
		dayid          smallint not null,
		sho            double not null,
		vmt            double not null,
		unique index xpkshobyageday(yearid, sourcetypeid, ageid, monthid, dayid));
	truncate ##tempdb##.shobyageday;
		
	create table if not exists ##tempdb##.analysisyearvmt (
		yearid      smallint not null,
		hpmsvtypeid smallint not null,
		vmt         float not null,
		unique index xpkanalysisyearvmt (yearid, hpmsvtypeid));
	truncate ##tempdb##.analysisyearvmt;

	create table if not exists ##tempdb##.sourcetypeagepopulation (
		yearid         smallint not null,
		sourcetypeid   smallint not null,
		ageid          smallint not null,
		population     float not null,
		unique index xpksourcetypeagepopulation (yearid, sourcetypeid, ageid));
	truncate ##tempdb##.sourcetypeagepopulation;

	create table if not exists ##tempdb##.vmtbymyroadhoursummary (
		yearid smallint not null,
		roadtypeid smallint not null,
		sourcetypeid smallint not null,
		monthid smallint not null,
		dayid smallint not null,
		hourid smallint not null,
		hourdayid smallint not null,
		totalvmt double,
		unique key (yearid, roadtypeid, sourcetypeid, monthid, hourid, dayid),
		unique key (yearid, roadtypeid, sourcetypeid, monthid, hourdayid));
	truncate ##tempdb##.vmtbymyroadhoursummary;

	create table if not exists ##tempdb##.zoneroadtypelinktemp (
		roadtypeid smallint not null,
		linkid int(11) not null,
		shoallocfactor double,
		unique index xpkzoneroadtypelinktemp (roadtypeid, linkid));
	truncate ##tempdb##.zoneroadtypelinktemp;
	
	create table if not exists ##tempdb##.drivingidlefraction (
		hourdayid smallint(6),
		yearid smallint(6),
		roadtypeid smallint(6),
		sourcetypeid smallint(6),
		drivingidlefraction double,
		primary key (hourdayid,yearid,roadtypeid,sourcetypeid));
	truncate ##tempdb##.drivingidlefraction;
	
	create table if not exists ##tempdb##.link like ##defaultdb##.link;
	truncate ##tempdb##.link;
	
	create table if not exists ##tempdb##.sho like ##defaultdb##.sho;
	truncate ##tempdb##.sho;

	-- perform intermediate calculations
	insert into ##tempdb##.sourcetypeagepopulation (yearid,sourcetypeid,ageid,population) 
		select sty.yearid, sty.sourcetypeid, stad.ageid, sty.sourcetypepopulation * stad.agefraction 
		from ##inputdb##.sourcetypeyear sty,
			 ##inputdb##.sourcetypeagedistribution stad 
		where sty.sourcetypeid = stad.sourcetypeid and 
			  sty.yearid = stad.yearid;
						
	insert into ##tempdb##.hpmsvtypepopulation (yearid,hpmsvtypeid,population) 
		select stap.yearid, sut.hpmsvtypeid, sum(stap.population) 
		from ##tempdb##.sourcetypeagepopulation stap,
			 ##defaultdb##.sourceusetype sut 
		where stap.sourcetypeid = sut.sourcetypeid
		group by stap.yearid, sut.hpmsvtypeid;
			
	insert into ##tempdb##.fractionwithinhpmsvtype (yearid,sourcetypeid,ageid,fraction) 
		select stap.yearid, stap.sourcetypeid, stap.ageid, stap.population / hvtp.population 
		from ##tempdb##.sourcetypeagepopulation stap,
			 ##defaultdb##.sourceusetype sut,
			 ##tempdb##.hpmsvtypepopulation hvtp 
		where stap.sourcetypeid = sut.sourcetypeid and 
			  sut.hpmsvtypeid = hvtp.hpmsvtypeid and 
			  stap.yearid = hvtp.yearid and 
			  hvtp.population <> 0;
						
	insert into ##tempdb##.hpmstravelfraction (yearid,hpmsvtypeid,fraction) 
		select fwhvt.yearid, sut.hpmsvtypeid, sum(fwhvt.fraction * sta.relativemar) 
		from ##tempdb##.fractionwithinhpmsvtype fwhvt,
			 ##defaultdb##.sourceusetype sut,
			 ##defaultdb##.sourcetypeage sta 
		where sta.sourcetypeid = fwhvt.sourcetypeid and 
			  sta.ageid = fwhvt.ageid and 
			  fwhvt.sourcetypeid = sut.sourcetypeid 
		group by fwhvt.yearid, sut.hpmsvtypeid;	
						
	insert into ##tempdb##.travelfraction (yearid,sourcetypeid,ageid,fraction) 
		select fwhvt.yearid, fwhvt.sourcetypeid, fwhvt.ageid, (fwhvt.fraction*sta.relativemar)/hpmstf.fraction 
		from ##tempdb##.fractionwithinhpmsvtype fwhvt,
			 ##defaultdb##.sourceusetype sut,
			 ##defaultdb##.sourcetypeage sta,
			 ##tempdb##.hpmstravelfraction hpmstf 
		where sta.sourcetypeid = fwhvt.sourcetypeid and 
			  sta.ageid = fwhvt.ageid and 
			  fwhvt.sourcetypeid = sut.sourcetypeid and 
			  hpmstf.yearid = fwhvt.yearid and 
			  hpmstf.hpmsvtypeid = sut.hpmsvtypeid and 
			  hpmstf.fraction <> 0;

	-- if vmt by source type has been provided, instead of by hpmsvtype, then
	-- normalize travelfraction by year and sourcetype.
	set howmany=0;
	select sum(nrows) into howmany from (select count(*) as nrows from ##inputdb##.sourcetypedayvmt union select count(*) as nrows from ##inputdb##.sourcetypeyearvmt) as t1;
	set howmany=ifnull(howmany,0);
	if(howmany > 0) then
		drop table if exists ##tempdb##.travelfractionsourcetypesum;

		create table ##tempdb##.travelfractionsourcetypesum
			select yearid, sourcetypeid, sum(fraction) as totaltravelfraction
			from ##tempdb##.travelfraction
			group by yearid, sourcetypeid
			order by null;
		
		update ##tempdb##.travelfraction, ##tempdb##.travelfractionsourcetypesum
			set fraction = case when totaltravelfraction > 0 then fraction / totaltravelfraction else 0 end
			where ##tempdb##.travelfraction.yearid = ##tempdb##.travelfractionsourcetypesum.yearid
			and ##tempdb##.travelfraction.sourcetypeid = ##tempdb##.travelfractionsourcetypesum.sourcetypeid;
	end if;
	
	-- if user vmt option is hpms by year (otherwise, this does nothing)
	insert ignore into ##tempdb##.analysisyearvmt (yearid,hpmsvtypeid,vmt) 
		select hvty.yearid, hvty.hpmsvtypeid, hvty.hpmsbaseyearvmt 
		from ##defaultdb##.sourceusetype sut,
			 ##inputdb##.hpmsvtypeyear hvty 
		where sut.hpmsvtypeid = hvty.hpmsvtypeid;
								
	insert into ##tempdb##.annualvmtbyageroadway (yearid,roadtypeid,sourcetypeid,ageid,vmt) 
		select tf.yearid, rtd.roadtypeid, tf.sourcetypeid, tf.ageid, ayv.vmt*rtd.roadtypevmtfraction*tf.fraction 
		from ##defaultdb##.roadtype rsrt,
			##tempdb##.travelfraction tf,
			##tempdb##.analysisyearvmt ayv,
			##inputdb##.roadtypedistribution rtd,
			##defaultdb##.sourceusetype sut 
		where rsrt.roadtypeid = rtd.roadtypeid and 
			  ayv.yearid = tf.yearid and 
			  tf.sourcetypeid = sut.sourcetypeid and 
			  sut.hpmsvtypeid = ayv.hpmsvtypeid and 
			  rtd.sourcetypeid = tf.sourcetypeid;
	
	-- if user vmt option is sourcetype by year otherwise, this does nothing)
	insert into ##tempdb##.annualvmtbyageroadway (yearid, roadtypeid, sourcetypeid, ageid, vmt)
		select tf.yearid, rtd.roadtypeid, tf.sourcetypeid, tf.ageid, v.vmt*rtd.roadtypevmtfraction*tf.fraction
		from ##defaultdb##.roadtype rsrt,
			 ##tempdb##.travelfraction tf,
			 ##inputdb##.sourcetypeyearvmt v,
			 ##inputdb##.roadtypedistribution rtd
		where rsrt.roadtypeid = rtd.roadtypeid and
			  v.yearid = tf.yearid and
			  tf.sourcetypeid = v.sourcetypeid and
			  rtd.sourcetypeid = tf.sourcetypeid;

	drop table if exists ##tempdb##.avarmonth;
	create table ##tempdb##.avarmonth
		select avar.*, monthid, monthvmtfraction
		from ##tempdb##.annualvmtbyageroadway avar
		inner join ##inputdb##.monthvmtfraction mvf using (sourcetypeid);

	drop table if exists ##tempdb##.avarmonthday;
	create table ##tempdb##.avarmonthday
		select avarm.*, dayid, dayvmtfraction, monthvmtfraction*dayvmtfraction as monthdayfraction
		from ##tempdb##.avarmonth avarm
		inner join ##inputdb##.dayvmtfraction dvf using (sourcetypeid, monthid, roadtypeid);

	insert into ##tempdb##.vmtbyageroadwayhour (yearid, roadtypeid, sourcetypeid, ageid, monthid, dayid, hourid, vmt, hourdayid)
		select avar.yearid, avar.roadtypeid, avar.sourcetypeid, avar.ageid, avar.monthid, avar.dayid, hvf.hourid,
			   avar.vmt*avar.monthdayfraction*hvf.hourvmtfraction / (noofdays / 7), -- replaced weekspermonthclause in tag with "(noofdays / 7)", added moay join
			   hd.hourdayid
		from ##tempdb##.avarmonthday avar
		inner join ##inputdb##.hourvmtfraction hvf using(sourcetypeid, roadtypeid, dayid)
		inner join ##defaultdb##.hourday hd on (hd.hourid=hvf.hourid and hd.dayid=avar.dayid)
		inner join ##defaultdb##.monthofanyyear moay using (monthid);

	-- if user vmt option is sourcetype by day (otherwise, this does nothing)
	insert ignore into ##tempdb##.vmtbyageroadwayhour (yearid, roadtypeid, sourcetypeid, ageid, monthid, dayid, hourid, vmt, hourdayid)
		select vmt.yearid, rtd.roadtypeid, vmt.sourcetypeid, tf.ageid, vmt.monthid, vmt.dayid, h.hourid,
			   vmt.vmt*h.hourvmtfraction*rtd.roadtypevmtfraction*tf.fraction*dow.noofrealdays as vmt,
			   hd.hourdayid
		from ##inputdb##.sourcetypedayvmt vmt
		inner join ##inputdb##.roadtypedistribution rtd on (rtd.sourcetypeid=vmt.sourcetypeid)
		inner join ##defaultdb##.hourday hd on (hd.dayid=vmt.dayid)
		inner join ##inputdb##.hourvmtfraction h on (h.hourid=hd.hourid and h.roadtypeid=rtd.roadtypeid and h.sourcetypeid=rtd.sourcetypeid)
		inner join ##tempdb##.travelfraction tf on (tf.yearid=vmt.yearid and tf.sourcetypeid=rtd.sourcetypeid)
		inner join ##defaultdb##.dayofanyweek dow on (dow.dayid=vmt.dayid);

	-- if user vmt option is hpms by day (otherwise, this does nothing)
	insert ignore into ##tempdb##.vmtbyageroadwayhour (yearid, roadtypeid, sourcetypeid, ageid, monthid, dayid, hourid, vmt, hourdayid)
		select vmt.yearid, rtd.roadtypeid, sut.sourcetypeid, tf.ageid, vmt.monthid, vmt.dayid, h.hourid,
			   vmt.vmt*h.hourvmtfraction*rtd.roadtypevmtfraction*tf.fraction*dow.noofrealdays as vmt,
			   hd.hourdayid
		from ##inputdb##.hpmsvtypeday vmt
		inner join ##defaultdb##.sourceusetype sut on (sut.hpmsvtypeid=vmt.hpmsvtypeid)
		inner join ##inputdb##.roadtypedistribution rtd on (rtd.sourcetypeid=sut.sourcetypeid)
		inner join ##defaultdb##.hourday hd on (hd.dayid=vmt.dayid)
		inner join ##inputdb##.hourvmtfraction h on (h.hourid=hd.hourid and h.roadtypeid=rtd.roadtypeid and h.sourcetypeid=rtd.sourcetypeid)
		inner join ##tempdb##.travelfraction tf on (tf.yearid=vmt.yearid and tf.sourcetypeid=rtd.sourcetypeid)
		inner join ##defaultdb##.dayofanyweek dow on (dow.dayid=vmt.dayid);
	
	-- regardless of how we got here, group over ageid for the summary table
	insert into ##tempdb##.vmtbymyroadhoursummary (yearid, roadtypeid, sourcetypeid, monthid, hourid, dayid, hourdayid, totalvmt)
		select yearid, roadtypeid, sourcetypeid, monthid, hourid, dayid, hourdayid, sum(vmt) as totalvmt
		from ##tempdb##.vmtbyageroadwayhour
		group by yearid, roadtypeid, sourcetypeid, monthid, hourid, dayid
		having sum(vmt) > 0;
					
	insert into ##tempdb##.vmtbymyroadhourfraction (yearid, roadtypeid, sourcetypeid, modelyearid, monthid, hourid, dayid, hourdayid, vmtfraction)
		select s.yearid, s.roadtypeid, s.sourcetypeid, (v.yearid-v.ageid) as modelyearid, s.monthid, s.hourid, s.dayid, s.hourdayid,  (vmt/totalvmt) as vmtfraction
		from ##tempdb##.vmtbymyroadhoursummary s
		inner join ##tempdb##.vmtbyageroadwayhour v using (yearid, roadtypeid, sourcetypeid, monthid, dayid, hourid);

	insert into ##tempdb##.averagespeed (roadtypeid,sourcetypeid,dayid,hourid,averagespeed) 
		select asd.roadtypeid, asd.sourcetypeid, hd.dayid, hd.hourid, sum(asb.avgbinspeed*asd.avgspeedfraction) 
		from ##defaultdb##.roadtype rsrt,
			 ##defaultdb##.hourofanyday hoad,
			 ##defaultdb##.avgspeedbin asb,
			 ##inputdb##.avgspeeddistribution asd,
			 ##defaultdb##.hourday hd 
		where rsrt.roadtypeid = asd.roadtypeid and 
			  hd.hourid = hoad.hourid and 
			  asb.avgspeedbinid = asd.avgspeedbinid and 
			  asd.hourdayid = hd.hourdayid 
		group by asd.roadtypeid, asd.sourcetypeid, hd.dayid, hd.hourid;

	insert into ##tempdb##.shobyageroadwayhour (yearid,roadtypeid,sourcetypeid,ageid,monthid,dayid,hourid,hourdayid,sho,vmt)
		select varh.yearid, varh.roadtypeid, varh.sourcetypeid, varh.ageid, varh.monthid, varh.dayid, varh.hourid, varh.hourdayid,
			   if(asp.averagespeed<>0,
				  coalesce(varh.vmt/asp.averagespeed,0.0),
				  0.0),
			   varh.vmt 
		from ##tempdb##.vmtbyageroadwayhour varh 
		left join ##tempdb##.averagespeed asp on (asp.roadtypeid = varh.roadtypeid and 
												  asp.sourcetypeid = varh.sourcetypeid and 
												  asp.dayid = varh.dayid and 
												  asp.hourid = varh.hourid);
	
	insert into ##tempdb##.drivingidlefraction
		select hourdayid,yearid,roadtypeid,sourcetypeid,sum(roadidlefraction*avgspeedfraction) as drivingidlefraction
		from ##defaultdb##.roadidlefraction
		join ##defaultdb##.hourday using (dayid)
		join ##inputdb##.avgspeeddistribution using (hourdayid,avgspeedbinid,sourcetypeid,roadtypeid)
		join ##inputdb##.year
		group by hourdayid,yearid,roadtypeid,sourcetypeid
		order by yearid,sourcetypeid,roadtypeid,hourdayid;
		
	insert into ##tempdb##.link (linkid,countyid,zoneid,roadtypeid)
		select zoneid*10 + roadtypeid as linkid,countyid,zoneid,roadtypeid from ##inputdb##.zone
		join ##defaultdb##.roadtype
		where roadtypeid < 100;
		
	insert into ##tempdb##.sho	
		select hourdayid,monthid,yearid,ageid,linkid,sourcetypeid,sho as sho, null as shocv, vmt as distance, 'N' as isuserinput
		from ##tempdb##.shobyageroadwayhour
		join ##tempdb##.link using (roadtypeid)
        where sho > 0 or vmt > 0
        order by sourcetypeid,linkid,ageid,monthid,hourdayid;
		
	
		
	-- ---------------------------------------
	-- end totalactivitygenerator calculations
	-- ---------------------------------------

	-- ---------------------------
	-- start oni calculation block
	-- ---------------------------

	-- oni calculation is slightly different than tag because we don't have drivingidlefraction (that is calculated at run time)
    -- instead, join with the pre-calculated roadidlefraction and weight the calculation by the avgspeedfraction 
    -- 		(roadidlefraction is by avgspeedbin, which drivingidlefraction does not have)
    -- also need to divide by number of days, because the ##tempdb##.shobyageroadwayhour table is by portion of week, whereas
    --      the moves output is by typical day
    drop table if exists ##tempdb##.onitooloutput_temp;
	
	create table if not exists ##tempdb##.onitooloutput_temp
	select s.yearid,s.monthid,hd.hourid,hd.dayid,s.sourcetypeid,s.ageid,tif.minmodelyearid,tif.maxmodelyearid,
	sum(s.sho) / doaw.noofrealdays as onroadsho,
    sum(s.sho * avgs.averagespeed) / doaw.noofrealdays as vmt,
	(case when totalidlefraction <> 1 then 
				greatest(
					sum((s.sho))*
					(totalidlefraction-sum(s.sho*drivingidlefraction) /sum((s.sho)))
					/(1-totalidlefraction),0) 
			else 0 
			end 
		) / doaw.noofrealdays as oni
		from ##tempdb##.sho s 
		inner join ##tempdb##.link l on ( 
			l.linkid = s.linkid 
			and l.roadtypeid <> 1) 
		inner join ##tempdb##.link lo on ( 
			l.zoneid = lo.zoneid 
			and lo.roadtypeid = 1) 
		inner join ##inputdb##.county c on (lo.countyid = c.countyid) 
		inner join ##inputdb##.state st using (stateid) 
		inner join ##defaultdb##.hourday hd on (s.hourdayid = hd.hourdayid) 
		inner join ##tempdb##.totalidlefraction tif on ( 
			tif.idleregionid = st.idleregionid 
			and tif.countytypeid = c.countytypeid 
			and tif.sourcetypeid = s.sourcetypeid 
			and tif.monthid = s.monthid 
			and tif.dayid = hd.dayid 
			and tif.minmodelyearid <= s.yearid - s.ageid 
			and tif.maxmodelyearid >= s.yearid - s.ageid) 
		inner join ##tempdb##.drivingidlefraction dif on ( 
			dif.hourdayid = s.hourdayid 
			and dif.yearid = s.yearid 
			and dif.roadtypeid = l.roadtypeid 
			and dif.sourcetypeid = s.sourcetypeid) 
		inner join ##tempdb##.averagespeed avgs on (
			avgs.roadtypeid = l.roadtypeid 
            and avgs.dayid = hd.dayid
            and avgs.hourid = hd.hourid
            and avgs.sourcetypeid = s.sourcetypeid)
		inner join ##inputdb##.year y on (s.yearid = y.yearid)
		inner join ##inputdb##.zone z on (s.linkid div 10 = z.zoneid)
		inner join ##defaultdb##.dayofanyweek doaw on (doaw.dayid = hd.dayid)
		group by s.yearid,s.monthid,hd.hourid,hd.dayid,s.sourcetypeid,s.ageid,tif.minmodelyearid,tif.maxmodelyearid
        order by s.yearid,s.monthid,hd.hourid,hd.dayid,s.sourcetypeid,s.ageid,tif.minmodelyearid,tif.maxmodelyearid;
	
    -- sum over ageid and calculate oni per vmt and per sho	
    insert into ##tempdb##.onitooloutput (yearid, monthid, dayid, hourid, sourcetypeid, minmodelyearid, maxmodelyearid, 
										  `onroadsho (hr)`, `vmt (mi)`, `oni (hr)`,
										  `oni per vmt (hr idle/mi)`, `oni per sho (hr idle/hr operating)`)
		select yearid, monthid, dayid, hourid, sourcetypeid, minmodelyearid, maxmodelyearid,
			   sum(onroadsho), sum(vmt), sum(oni),
			   sum(oni) / sum(vmt), sum(oni) / sum(onroadsho)
		from ##tempdb##.onitooloutput_temp
        group by yearid, monthid, dayid, hourid, sourcetypeid, minmodelyearid, maxmodelyearid;
    
	insert into ##tempdb##.onitempmessages (message) values ('info: successfully calculated default oni hours from tables in ##inputdb##');
	
	-- -------------------------
	-- end oni calculation block
	-- -------------------------	


	-- ----------------------------------------
	-- final clean up (currently commented out because ##tempdb## is dropped by moves gui)
	-- ----------------------------------------
	-- drop table if exists ##tempdb##.totalidlefraction;
	-- drop table if exists ##tempdb##.shobyageroadwayhour;
	-- drop table if exists ##tempdb##.startsbyagehour;
	-- drop table if exists ##tempdb##.vmtbyageroadwayhour;
	-- drop table if exists ##tempdb##.vmtbymyroadhourfraction;
	-- drop table if exists ##tempdb##.hpmsvtypepopulation;
	-- drop table if exists ##tempdb##.fractionwithinhpmsvtype;
	-- drop table if exists ##tempdb##.hpmstravelfraction;
	-- drop table if exists ##tempdb##.travelfraction;
	-- drop table if exists ##tempdb##.annualvmtbyageroadway;
	-- drop table if exists ##tempdb##.averagespeed;
	-- drop table if exists ##tempdb##.shobyageday;
	-- drop table if exists ##tempdb##.analysisyearvmt;
	-- drop table if exists ##tempdb##.sourcetypeagepopulation;
	-- drop table if exists ##tempdb##.vmtbymyroadhoursummary;
	-- drop table if exists ##tempdb##.zoneroadtypelinktemp;
	-- drop table if exists ##tempdb##.travelfractionsourcetypesum;
	-- drop table if exists ##tempdb##.avarmonth;
	-- drop table if exists ##tempdb##.avarmonthday;
	-- drop table if exists ##tempdb##.onitooloutput_temp;
	
end onitool_procedure
endblock

call onitool();
drop procedure if exists onitool;