-- fuelsupplyimporter.sql - script to check import errors for the
-- tables: avft, fuelformulation, fuelsupply, and fuelusagefraction.
-- author wesley faler
-- version 2016-10-04

drop procedure if exists spcheckfuelsupplyimporter;

beginblock
create procedure spcheckfuelsupplyimporter()
begin
	-- mode 0 is run after importing
	-- mode 1 is run to check overall success/failure, allowing data from the default database
	-- mode 2 is run to check overall success/failure, requiring no data from the default database
	declare mode int default ##mode##;
	declare isok int default 1;
	declare howmany int default 0;
	declare defaultfuelcount int default 0;
	declare avftfuelcount int default 0;

	-- scale 0 is national
	-- scale 1 is single county
	-- scale 2 is project domain
	-- scale 3 is nonroad
	declare scale int default ##scale##;

	-- usefuelusagefraction 0 when the fuelusagefraction table is not used in the model
	declare usefuelusagefraction int default ##use_fuelusagefraction##;

	if(mode = 0) then
		-- create any new fuel years and associate them to the required years
		drop table if exists tempnewfuelyear;

		if(scale = 3) then
			create table tempnewfuelyear
			select distinct fuelyearid
			from nrfuelsupply fs
			left outer join ##defaultdatabase##.fuelsupplyyear fsy using (fuelyearid)
			where fsy.fuelyearid is null;
		else
			create table tempnewfuelyear
			select distinct fuelyearid
			from fuelsupply fs
			left outer join ##defaultdatabase##.fuelsupplyyear fsy using (fuelyearid)
			where fsy.fuelyearid is null;
		end if;
		
		drop table if exists fuelsupplyyear;
		
		create table if not exists fuelsupplyyear (
		  fuelyearid smallint(6) not null default '0',
		  primary key (fuelyearid)
		);
		
		insert ignore into fuelsupplyyear (fuelyearid)
		select fuelyearid from tempnewfuelyear;
		
		drop table if exists tempyear;
		
		create table if not exists tempyear (
		  yearid smallint(6) not null default '0',
		  isbaseyear char(1) default null,
		  fuelyearid smallint(6) not null default '0',
		  primary key  (yearid),
		  key isbaseyear (isbaseyear)
		);
		
		create table if not exists year (
		  yearid smallint(6) not null default '0',
		  isbaseyear char(1) default null,
		  fuelyearid smallint(6) not null default '0',
		  primary key  (yearid),
		  key isbaseyear (isbaseyear)
		);
		
		insert into tempyear (yearid, isbaseyear, fuelyearid)
		select yearid, isbaseyear, nfy.fuelyearid
		from tempnewfuelyear nfy
		inner join ##defaultdatabase##.year y on (y.yearid=nfy.fuelyearid);
		
-- 		insert ignore into year (yearid, isbaseyear, fuelyearid)
-- 		select yearid, isbaseyear, fuelyearid
-- 		from tempyear
		
		update year, tempyear set year.fuelyearid=tempyear.fuelyearid
		where year.yearid=tempyear.yearid;
		
		drop table if exists tempyear;
		drop table if exists tempnewfuelyear;
	end if;
	
	-- complain about any years outside of moves's range
	if(scale = 3) then
		insert into importtempmessages (message)
		select distinct concat('error: fuel year ',fuelyearid,' is outside the range of 1990-2060 and cannot be used') as errormessage
		from nrfuelsupply
		where fuelyearid < 1990 or fuelyearid > 2060
		and marketshare > 0;
	else
		insert into importtempmessages (message)
		select distinct concat('error: fuel year ',fuelyearid,' is outside the range of 1990-2060 and cannot be used') as errormessage
		from fuelsupply
		where fuelyearid < 1990 or fuelyearid > 2060
		and marketshare > 0;
	end if;
	
	if(mode = 0) then
		if(scale = 3) then
			-- remove records with zero market shares
			delete from nrfuelsupply where marketshare < 0.0001;
		else
			-- remove records with zero market shares
			delete from fuelsupply where marketshare < 0.0001;
		end if;
	end if;
	
	-- complain about unknown fuel formulations
	if(scale = 3) then
		insert into importtempmessages (message)
		select distinct concat('error: fuel formulation ',fuelformulationid,' is unknown') as message
		from nrfuelsupply
		where fuelformulationid not in (
			select fuelformulationid
			from fuelformulation
			union
			select fuelformulationid
			from ##defaultdatabase##.fuelformulation
		)
		and marketshare > 0;
	else 
		if(mode = 2 or scale in (1,2)) then
			insert into importtempmessages (message)
			select distinct concat('error: fuel formulation ',fuelformulationid,' is unknown') as message
			from fuelsupply
			where fuelformulationid not in (
				select fuelformulationid
				from fuelformulation
			)
			and marketshare > 0;
		else
			insert into importtempmessages (message)
			select distinct concat('error: fuel formulation ',fuelformulationid,' is unknown') as message
			from fuelsupply
			where fuelformulationid not in (
				select fuelformulationid
				from fuelformulation
				union
				select fuelformulationid
				from ##defaultdatabase##.fuelformulation
			)
			and marketshare > 0;
		end if;
	end if;

	if (scale = 3) then
		insert into importtempmessages (message)
		select concat('warning: fuel formulation ',fuelformulationid,' is gasoline with ethanol volume greater than 10%') as message
		from fuelformulation 
		join nrfuelsupply using (fuelformulationid)
		where etohvolume > 10 and fuelsubtypeid in (10,11,12,13,14,15,18) and marketshare > 0;
	else
		insert into importtempmessages (message)
		select concat('error: fuel formulation ',fuelformulationid,' is gasoline with ethanol volume greater than 15%') as message
		from fuelformulation 
		join fuelsupply using (fuelformulationid)
		where etohvolume > 15 and fuelsubtypeid in (10,11,12,13,14,15,18) and marketshare > 0;
	end if;

	insert into importtempmessages (message)
	select concat('error: fuel formulation ',fuelformulationid,' has non-zero value for mtbe volume') as message
	from fuelformulation where mtbevolume <> 0;

	insert into importtempmessages (message)
	select concat('error: fuel formulation ',fuelformulationid,' has non-zero value for etbe volume') as message
	from fuelformulation where etbevolume <> 0;

	insert into importtempmessages (message)
	select concat('error: fuel formulation ',fuelformulationid,' has non-zero value for tame volume') as message
	from fuelformulation where tamevolume <> 0;

	-- correct fuelformulation.fuelsubtypeid for gasoline and ethanol fuels
	-- note: rfg (sub type 11) and conventional gasoline (sub type 10) cannot be distinguished by etohvolume, so anything with
	-- ----- a low etohvolume and not already assigned as rfg is assigned to conventional gasoline.

	insert into importtempmessages (message)
	select distinct concat('warning: fuel formulation ',fuelformulationid,' changed fuelsubtypeid from ',fuelsubtypeid, ' to 10 based on etohvolume') as message
	from fuelformulation where fuelsubtypeid <> 10 and etohvolume < 0.10  and fuelsubtypeid <> 11 and fuelsubtypeid in (10,11,12,13,14,15,51,52,18);

	insert into importtempmessages (message)
	select distinct concat('warning: fuel formulation ',fuelformulationid,' changed fuelsubtypeid from ',fuelsubtypeid, ' to 12 based on etohvolume') as message
	from fuelformulation where fuelsubtypeid <> 12 and etohvolume >= 9    and etohvolume < 12.5 and fuelsubtypeid in (10,11,12,13,14,15,51,52,18);

	insert into importtempmessages (message)
	select distinct concat('warning: fuel formulation ',fuelformulationid,' changed fuelsubtypeid from ',fuelsubtypeid, ' to 13 based on etohvolume') as message
	from fuelformulation where fuelsubtypeid <> 13 and etohvolume >= 6    and etohvolume < 9 and fuelsubtypeid in (10,11,12,13,14,15,51,52,18);

	insert into importtempmessages (message)
	select distinct concat('warning: fuel formulation ',fuelformulationid,' changed fuelsubtypeid from ',fuelsubtypeid, ' to 14 based on etohvolume') as message
	from fuelformulation where fuelsubtypeid <> 14 and etohvolume >= 0.10 and etohvolume < 6 and fuelsubtypeid in (10,11,12,13,14,15,51,52,18);

	insert into importtempmessages (message)
	select distinct concat('warning: fuel formulation ',fuelformulationid,' changed fuelsubtypeid from ',fuelsubtypeid, ' to 15 based on etohvolume') as message
	from fuelformulation where fuelsubtypeid <> 15 and etohvolume >= 12.5 and etohvolume < 17.5 and fuelsubtypeid in (10,11,12,13,14,15,51,52,18);

	insert into importtempmessages (message)
	select distinct concat('warning: fuel formulation ',fuelformulationid,' changed fuelsubtypeid from ',fuelsubtypeid, ' to 51 based on etohvolume') as message
	from fuelformulation where fuelsubtypeid <> 51 and etohvolume >= 70.5 and etohvolume <= 100 and fuelsubtypeid in (10,11,12,13,14,15,51,52,18);

	insert into importtempmessages (message)
	select distinct concat('warning: fuel formulation ',fuelformulationid,' changed fuelsubtypeid from ',fuelsubtypeid, ' to 52 based on etohvolume') as message
	from fuelformulation where fuelsubtypeid <> 52 and etohvolume >= 50.5   and etohvolume < 70.5 and fuelsubtypeid in (10,11,12,13,14,15,51,52,18);

	insert into importtempmessages (message)
	select distinct concat('warning: fuel formulation ',fuelformulationid,' changed fuelsubtypeid from ',fuelsubtypeid, ' to 18 based on etohvolume') as message
	from fuelformulation where fuelsubtypeid <> 18 and etohvolume >= 17.5 and etohvolume < 50.5 and fuelsubtypeid in (10,11,12,13,14,15,51,52,18);

	update fuelformulation set fuelsubtypeid = 10 where fuelsubtypeid <> 10 and etohvolume < 0.10  and fuelsubtypeid <> 11 and fuelsubtypeid in (10,11,12,13,14,15,51,52,18);
	update fuelformulation set fuelsubtypeid = 12 where fuelsubtypeid <> 12 and etohvolume >= 9    and etohvolume < 12.5 and fuelsubtypeid in (10,11,12,13,14,15,51,52,18);
	update fuelformulation set fuelsubtypeid = 13 where fuelsubtypeid <> 13 and etohvolume >= 6    and etohvolume < 9 and fuelsubtypeid in (10,11,12,13,14,15,51,52,18);
	update fuelformulation set fuelsubtypeid = 14 where fuelsubtypeid <> 14 and etohvolume >= 0.10 and etohvolume < 6 and fuelsubtypeid in (10,11,12,13,14,15,51,52,18);
	update fuelformulation set fuelsubtypeid = 15 where fuelsubtypeid <> 15 and etohvolume >= 12.5 and etohvolume < 17.5 and fuelsubtypeid in (10,11,12,13,14,15,51,52,18);
	update fuelformulation set fuelsubtypeid = 51 where fuelsubtypeid <> 51 and etohvolume >= 70.5 and etohvolume <= 100 and fuelsubtypeid in (10,11,12,13,14,15,51,52,18);
	update fuelformulation set fuelsubtypeid = 52 where fuelsubtypeid <> 52 and etohvolume >= 50.5 and etohvolume < 70.5 and fuelsubtypeid in (10,11,12,13,14,15,51,52,18);
	update fuelformulation set fuelsubtypeid = 18 where fuelsubtypeid <> 18 and etohvolume >= 17.5 and etohvolume < 50.5 and fuelsubtypeid in (10,11,12,13,14,15,51,52,18);

	-- complain about fuel types that were imported but won't be used
	if(scale = 3) then
		insert into importtempmessages (message)
		select distinct concat('warning: fuel type ',fueltypeid,' is imported but will not be used') as message
		from nrfuelsupply fs
		inner join ##defaultdatabase##.fuelformulation ff using (fuelformulationid)
		inner join ##defaultdatabase##.fuelsubtype fst using (fuelsubtypeid)
		where fueltypeid not in (##fueltypeids##)
		union
		select distinct concat('warning: fuel type ',fueltypeid,' is imported but will not be used') as message
		from nrfuelsupply fs
		inner join fuelformulation ff using (fuelformulationid)
		inner join ##defaultdatabase##.fuelsubtype fst using (fuelsubtypeid)
		where fueltypeid not in (##fueltypeids##);
	else
		if(mode = 2 or scale in (1,2)) then
			insert into importtempmessages (message)
			select distinct concat('warning: fuel type ',fueltypeid,' is imported but will not be used') as message
			from fuelsupply fs
			inner join fuelformulation ff using (fuelformulationid)
			inner join ##defaultdatabase##.fuelsubtype fst using (fuelsubtypeid)
			where fueltypeid not in (##fueltypeids##);
		else
			insert into importtempmessages (message)
			select distinct concat('warning: fuel type ',fueltypeid,' is imported but will not be used') as message
			from fuelsupply fs
			inner join ##defaultdatabase##.fuelformulation ff using (fuelformulationid)
			inner join ##defaultdatabase##.fuelsubtype fst using (fuelsubtypeid)
			where fueltypeid not in (##fueltypeids##)
			union
			select distinct concat('warning: fuel type ',fueltypeid,' is imported but will not be used') as message
			from fuelsupply fs
			inner join fuelformulation ff using (fuelformulationid)
			inner join ##defaultdatabase##.fuelsubtype fst using (fuelsubtypeid)
			where fueltypeid not in (##fueltypeids##);
		end if;
	end if;

	-- complain about fixable gaps in t50/t90/e200/e300 data (only for gasoline & gasohol)
	insert into importtempmessages (message)
	select distinct concat('warning: fuel formulation ',fuelformulationid,' is using calculated e200') as message
	from fuelformulation where t50 is not null and t50 > 0 and (e200 is null or e200 <= 0)
	and fuelsubtypeid in (10, 11, 12, 13, 14, 15);

	insert into importtempmessages (message)
	select distinct concat('warning: fuel formulation ',fuelformulationid,' is using calculated e300') as message
	from fuelformulation where t90 is not null and t90 > 0 and (e300 is null or e300 <= 0)
	and fuelsubtypeid in (10, 11, 12, 13, 14, 15);

	insert into importtempmessages (message)
	select distinct concat('warning: fuel formulation ',fuelformulationid,' is using calculated t50') as message
	from fuelformulation where e200 is not null and e200 > 0 and (t50 is null or t50 <= 0)
	and fuelsubtypeid in (10, 11, 12, 13, 14, 15);

	insert into importtempmessages (message)
	select distinct concat('warning: fuel formulation ',fuelformulationid,' is using calculated t90') as message
	from fuelformulation where e300 is not null and e300 > 0 and (t90 is null or t90 <= 0)
	and fuelsubtypeid in (10, 11, 12, 13, 14, 15);

	-- complain about unfixable gaps in t50/t90/e200/e300 data (only for gasoline & gasohol)
	insert into importtempmessages (message)
	select distinct concat('error: fuel formulation ',fuelformulationid,' is missing both e200 and t50') as message
	from fuelformulation where (t50 is null or t50 <= 0) and (e200 is null or e200 <= 0)
	and fuelsubtypeid in (10, 11, 12, 13, 14, 15);

	insert into importtempmessages (message)
	select distinct concat('error: fuel formulation ',fuelformulationid,' is missing both e300 and t90') as message
	from fuelformulation where (t90 is null or t90 <= 0) and (e300 is null or e300 <= 0)
	and fuelsubtypeid in (10, 11, 12, 13, 14, 15);

	-- fill gaps in t50/t90/e200/e300 data (only for gasoline & gasohol)
	update fuelformulation set t50 = 2.0408163 * (147.91 - e200) where e200 is not null and e200 > 0 and (t50 is null or t50 <= 0) and fuelsubtypeid in (10, 11, 12, 13, 14, 15);
	update fuelformulation set t90 = 4.5454545 * (155.47 - e300) where e300 is not null and e300 > 0 and (t90 is null or t90 <= 0) and fuelsubtypeid in (10, 11, 12, 13, 14, 15);
	update fuelformulation set e200 = 147.91-(t50/2.0408163) where t50 is not null and t50 > 0 and (e200 is null or e200 <= 0) and fuelsubtypeid in (10, 11, 12, 13, 14, 15);
	update fuelformulation set e300 = 155.47-(t90/4.5454545) where t90 is not null and t90 > 0 and (e300 is null or e300 <= 0) and fuelsubtypeid in (10, 11, 12, 13, 14, 15);
	
	-- ensure market shares sum to 1.0 for all fuel types, year, month, counties.
	drop table if exists tempfuelsupplynotunity;
	
	drop table if exists tempfuelsupplyunion;

	if(scale = 3) then
		create table tempfuelsupplyunion
		select fueltypeid, fuelregionid, fuelyearid, monthgroupid, marketshare, fuelformulationid
		from nrfuelsupply fs
		inner join ##defaultdatabase##.fuelformulation ff using (fuelformulationid)
		inner join ##defaultdatabase##.fuelsubtype fst using (fuelsubtypeid)
		union
		select fueltypeid, fuelregionid, fuelyearid, monthgroupid, marketshare, fuelformulationid
		from nrfuelsupply fs
		inner join fuelformulation ff using (fuelformulationid)
		inner join ##defaultdatabase##.fuelsubtype fst using (fuelsubtypeid);
	else
		if(mode = 2 or scale in (1,2)) then
			create table tempfuelsupplyunion
			select fueltypeid, fuelregionid, fuelyearid, monthgroupid, marketshare, fuelformulationid
			from fuelsupply fs
			inner join fuelformulation ff using (fuelformulationid)
			inner join ##defaultdatabase##.fuelsubtype fst using (fuelsubtypeid);
		else
			create table tempfuelsupplyunion
			select fueltypeid, fuelregionid, fuelyearid, monthgroupid, marketshare, fuelformulationid
			from fuelsupply fs
			inner join ##defaultdatabase##.fuelformulation ff using (fuelformulationid)
			inner join ##defaultdatabase##.fuelsubtype fst using (fuelsubtypeid)
			union
			select fueltypeid, fuelregionid, fuelyearid, monthgroupid, marketshare, fuelformulationid
			from fuelsupply fs
			inner join fuelformulation ff using (fuelformulationid)
			inner join ##defaultdatabase##.fuelsubtype fst using (fuelsubtypeid);
		end if;
	end if;

	create table tempfuelsupplynotunity
	select fueltypeid, fuelregionid, fuelyearid, monthgroupid, sum(marketshare) as summarketshare
	from tempfuelsupplyunion fs
	group by fueltypeid, fuelregionid, fuelyearid, monthgroupid
	having round(sum(marketshare),4) <> 1.0000;

	drop table if exists tempfuelsupplyunion;

	insert into importtempmessages (message)
	select concat('error: region ',fuelregionid,', year ',fuelyearid,', month ',monthgroupid,', fuel type ',fueltypeid,' market share is not 1.0 but instead ',round(summarketshare,4))
	from tempfuelsupplynotunity;
	
	drop table if exists tempfuelsupplynotunity;

	if(scale < 3 and usefuelusagefraction > 0) then
		-- -----------------------------------------------------------------------------------------------------
		-- check fuelusagefraction table
		-- -----------------------------------------------------------------------------------------------------
	
		-- complain about any years outside of moves's range
		insert into importtempmessages (message)
		select distinct concat('error: fuel year ',fuelyearid,' is outside the range of 1990-2060 and cannot be used') as errormessage
		from fuelusagefraction
		where fuelyearid < 1990 or fuelyearid > 2060
		and usagefraction > 0;
		
		-- if(mode = 0) then
		-- 	-- remove records with zero usage
		-- 	delete from fuelusagefraction where usagefraction < 0.0001;
		-- end if;
		
		-- ensure usage fractions sum to 1.0 for all counties, fuel years, model year groups, and sourcebin fuel types.
		drop table if exists tempfuelusagefractionnotunity;
		
		drop table if exists tempfuelusagefractionnotunity;
	
		create table tempfuelusagefractionnotunity
		select countyid, fuelyearid, modelyeargroupid, sourcebinfueltypeid, sum(usagefraction) as sumusagefraction
		from fuelusagefraction
		group by countyid, fuelyearid, modelyeargroupid, sourcebinfueltypeid
		having round(sum(usagefraction),4) <> 1.0000;
	
		insert into importtempmessages (message)
		select concat('error: county ',countyid,', year ',fuelyearid,', model year group ',modelyeargroupid,', source fuel type ',sourcebinfueltypeid,' usage fraction is not 1.0 but instead ',round(sumusagefraction,4))
		from tempfuelusagefractionnotunity;
		
		drop table if exists tempfuelusagefractionnotunity;
	end if;

	-- check avft
	if(scale < 3) then
		set howmany=0;
		select count(*) into howmany from avft;
		set howmany=ifnull(howmany,0);
		if(howmany > 0) then
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
	
			insert into importtempmessages (message)
			select distinct concat('error: imported avft is missing source type ',sourcetypeid, ', model year ',modelyearid,', fuel ',fueltypeid) as message
			from (
				select distinct sourcetypeid, modelyearid, fueltypeid
				from ##defaultdatabase##.samplevehiclepopulation
				where sourcetypeid in (##sourcetypeids##)
			) t1
			left outer join avft using (sourcetypeid, modelyearid, fueltypeid)
			where avft.sourcetypeid is null 
				and avft.modelyearid is null 
				and avft.fueltypeid is null
				and t1.sourcetypeid in (##sourcetypeids##)
			and t1.modelyearid in (
				select distinct modelyearid
				from ##defaultdatabase##.modelyear,
				##defaultdatabase##.year
				where yearid in (##yearids##)
				and modelyearid >= yearid - 30
				and modelyearid <= yearid
			)
			order by t1.sourcetypeid, t1.modelyearid, t1.fueltypeid;
			
			insert into importtempmessages (message)
			select distinct concat('warning: no emission rates exist for avft source type ',sourcetypeid, ', model year ',modelyearid,', fuel ',fueltypeid) as message
			from avft
			left outer join (
				select distinct sourcetypeid, modelyearid, fueltypeid
				from ##defaultdatabase##.samplevehiclepopulation
				where sourcetypeid in (##sourcetypeids##)
			) t1 using (sourcetypeid, modelyearid, fueltypeid)
			where t1.sourcetypeid is null 
				and t1.modelyearid is null 
				and t1.fueltypeid is null
				and avft.sourcetypeid in (##sourcetypeids##)
			and avft.modelyearid in (
				select distinct modelyearid
				from ##defaultdatabase##.modelyear,
				##defaultdatabase##.year
				where yearid in (##yearids##)
				and modelyearid >= yearid - 30
				and modelyearid <= yearid
			)
			order by avft.sourcetypeid, avft.modelyearid, avft.fueltypeid;
		end if;
	end if;

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

call spcheckfuelsupplyimporter();
drop procedure if exists spcheckfuelsupplyimporter;
