-- populate rateperdistance, ratepervehicle, rateperprofile.
-- run from an output database.
--
-- expected replacement parameters:
--		##maindatabase##	ideally will be movesexecution, but could be a fully filled default database
--		##runid##
--		##scenarioid##
--		##isprojectdomain##	1 or 0
--		##isscc## 1 or 0
--
-- author wesley faler
-- version 2014-02-13

drop procedure if exists spratepostprocessor;

beginblock
create procedure spratepostprocessor()
begin
	-- there will be no null columns in movesoutput or movesactivityoutput.  all null values
	-- have been replaced with 0's to facilitate joining.

	-- create a temporary table, good for exactly one type of activity and tuned to the needs
	-- of joins for the rate tables.

	-- populate rateperdistance
	--	first, get just the distance activity into its own indexed table
	drop table if exists tempactivityoutput;
	create table tempactivityoutput (
		yearid               smallint unsigned null default null,
		monthid              smallint unsigned null default null,
		dayid                smallint unsigned null default null,
		hourid               smallint unsigned null default null,
		linkid               integer unsigned null default null,
		sourcetypeid         smallint unsigned null default null,
		regclassid           smallint unsigned null default null,
		scc					 char(10) null default '',
		fueltypeid           smallint unsigned null default null,
		modelyearid          smallint unsigned null default null,
		roadtypeid           smallint unsigned null default null,
		activity             float null default null,
		zoneid               integer unsigned null default null,
		key (yearid, monthid, dayid, hourid,
			linkid,
			sourcetypeid, regclassid, fueltypeid, modelyearid,
			roadtypeid)
	) engine=myisam default charset=latin1 delay_key_write=1;

	insert into tempactivityoutput (
		yearid, monthid, dayid, hourid,
		linkid,
		sourcetypeid, regclassid, scc, fueltypeid, modelyearid,
		roadtypeid,
		activity,
		zoneid)
	select yearid, monthid, dayid, hourid,
		linkid,
		sourcetypeid, regclassid, scc, fueltypeid, modelyearid,
		roadtypeid,
		activity,
		zoneid
	from movesactivityoutput
	where movesrunid = ##runid##
	and activitytypeid=1;
	
	analyze table tempactivityoutput;

	--	now the inventory can be efficiently joined with the indexed activity
	insert into rateperdistance (movesscenarioid, movesrunid,
		yearid, monthid, dayid, hourid,
		linkid,
		sourcetypeid, regclassid, scc, fueltypeid, modelyearid,
		roadtypeid,
		pollutantid, processid,
		avgspeedbinid,temperature, relhumidity,
		rateperdistance)
	select '##scenarioid##' as movesscenarioid, ##runid## as movesrunid,
		o.yearid, o.monthid, o.dayid, o.hourid,
		o.linkid,
		o.sourcetypeid, o.regclassid, o.scc, o.fueltypeid, o.modelyearid,
		o.roadtypeid,
		o.pollutantid, o.processid,
		if(##isprojectdomain##>0,0,mod(o.linkid,100)) as avgspeedbinid,
		z.temperature, z.relhumidity,
		case when a.activity > 0 then (o.emissionquant / a.activity)
		else null
		end as rateperdistance
	from movesoutput as o
	inner join tempactivityoutput as a using (
		yearid, monthid, dayid, hourid,
		linkid,
		sourcetypeid, regclassid, fueltypeid, modelyearid,
		roadtypeid)
	inner join ##maindatabase##.zonemonthhour as z on (
		z.zoneid=a.zoneid
		and z.monthid=a.monthid
		and z.hourid=a.hourid
	)
	where o.movesrunid=##runid##
	and (o.processid in (1,9,10,15,18,19)
	or (o.processid in (11,12,13) and o.roadtypeid<>1));

	-- populate ratepervehicle
	--	first, get just the population into its own indexed table
	drop table if exists tempactivityoutput;
	create table tempactivityoutput (
		zoneid               integer unsigned null default null,
		yearid               smallint unsigned null default null,
		sourcetypeid         smallint unsigned null default null,
		regclassid	         smallint unsigned null default null,
		scc					 char(10) null default null,
		fueltypeid           smallint unsigned null default null,
		modelyearid          smallint unsigned null default null,
		activity             float null default null,
		key (zoneid,
			yearid, 
			sourcetypeid, regclassid, fueltypeid, modelyearid)
	) engine=myisam default charset=latin1 delay_key_write=1;

	if(##isprojectdomain##>0) then
		-- project population is by link and needs to be aggregated to a single zone-level population.
		insert into tempactivityoutput (
			zoneid,
			yearid,
			sourcetypeid, regclassid, scc, fueltypeid, modelyearid,
			activity)
		select zoneid,
			yearid,
			sourcetypeid, regclassid, scc, fueltypeid, modelyearid,
			sum(activity)
		from movesactivityoutput
		where movesrunid = ##runid##
		and activitytypeid=6
		and roadtypeid=1
		group by zoneid,
			yearid,
			sourcetypeid, regclassid, fueltypeid, modelyearid;
	else
		-- non-project domains generate a single population entry per zone/month/day/hour and don't need to be aggregated.
		insert into tempactivityoutput (
			zoneid,
			yearid,
			sourcetypeid, regclassid, scc, fueltypeid, modelyearid,
			activity)
		select distinct zoneid,
			yearid,
			sourcetypeid, regclassid, scc, fueltypeid, modelyearid,
			activity
		from movesactivityoutput
		where movesrunid = ##runid##
		and activitytypeid=6;
	end if;

	--	now the inventory can be efficiently joined with the indexed activity
	insert into ratepervehicle (movesscenarioid, movesrunid,
		yearid, monthid, dayid, hourid,
		zoneid,
		sourcetypeid, regclassid, scc, fueltypeid, modelyearid,
		pollutantid, processid,
		temperature,
		ratepervehicle)
	select '##scenarioid##' as movesscenarioid, ##runid## as movesrunid,
		o.yearid, o.monthid, o.dayid, o.hourid,
		o.zoneid,
		o.sourcetypeid, o.regclassid, o.scc, o.fueltypeid, o.modelyearid,
		o.pollutantid, o.processid,
		z.temperature,
		case when a.activity > 0 then (o.emissionquant / a.activity)
		else null
		end as ratepervehicle
	from movesoutput as o
	inner join tempactivityoutput as a using (
		zoneid,
		yearid,
		sourcetypeid, regclassid, fueltypeid, modelyearid)
	inner join ##maindatabase##.zonemonthhour as z on (
		z.zoneid=a.zoneid
		and z.monthid=o.monthid
		and z.hourid=o.hourid
	)
	where o.movesrunid=##runid##
	and (o.processid in (2,16,17,90,91)
	or (o.processid in (11,13,18,19) and o.roadtypeid=1));

	--	now the inventory can be efficiently joined with the indexed activity
	insert into startspervehicle (movesscenarioid, movesrunid,
		yearid, monthid, dayid, hourid,
		zoneid,
		sourcetypeid, regclassid, scc, fueltypeid, modelyearid,
		startspervehicle)
	select '##scenarioid##' as movesscenarioid, ##runid## as movesrunid,
		o.yearid, o.monthid, o.dayid, o.hourid,
		o.zoneid,
		o.sourcetypeid, o.regclassid, o.scc, o.fueltypeid, o.modelyearid,
		case when a.activity > 0 then (o.activity / a.activity)
		else null
		end as startspervehicle
	from movesactivityoutput as o
	inner join tempactivityoutput as a using (
		zoneid,
		yearid,
		sourcetypeid, regclassid, fueltypeid, modelyearid)
	where o.movesrunid=##runid##
	and o.activitytypeid = 7;

-- cannot make project starts as "rate per started vehicle" because the sourcetypeid/scc connection is many-to-many
-- and cannot be done for the scc portion of rates.  in addition, this would make project report rates differently
-- than other scales, which report starts as "rate per existing vehicle".
-- 	if(##isprojectdomain##>0) then
-- 		update ratepervehicle, ##maindatabase##.offnetworklink
-- 		set ratepervehicle = case when startfraction > 0 then (ratepervehicle / startfraction) else 0 end
-- 		where movesrunid=##runid##
-- 		and processid = 2
-- 		and ratepervehicle.sourcetypeid = ##maindatabase##.offnetworklink.sourcetypeid;
-- 	end if;

	-- populate rateperprofile
	-- use the population activity from the prior step, already stored in tempactivityoutput
	--	now the inventory can be efficiently joined with the indexed activity
	insert into rateperprofile (movesscenarioid, movesrunid,
		temperatureprofileid,
		yearid, dayid, hourid,
		pollutantid, processid,
		sourcetypeid, regclassid, scc, fueltypeid, modelyearid,
		temperature,
		ratepervehicle)
	select '##scenarioid##' as movesscenarioid, ##runid## as movesrunid,
		t.temperatureprofileid,
		o.yearid, o.dayid, o.hourid,
		o.pollutantid, o.processid,
		o.sourcetypeid, o.regclassid, o.scc, o.fueltypeid, o.modelyearid,
		z.temperature,
		case when a.activity > 0 then (o.emissionquant / a.activity)
		else null
		end as ratepervehicle
	from movesoutput as o
	inner join tempactivityoutput as a using (
		zoneid,
		yearid,
		sourcetypeid, regclassid, fueltypeid, modelyearid)
	inner join ##maindatabase##.zonemonthhour as z on (
		z.zoneid=a.zoneid
		and z.monthid=o.monthid
		and z.hourid=o.hourid
	)
	inner join ##maindatabase##.temperatureprofileid as t on (
		t.zoneid=a.zoneid
		and t.monthid=o.monthid
	)
	where o.movesrunid=##runid##
	and o.processid=12 and o.roadtypeid=1;

	-- done
	drop table if exists tempactivityoutput;
end
endblock

call spratepostprocessor();
drop procedure if exists spratepostprocessor;
