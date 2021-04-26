-- remove effects of the clean air act by propagating 1993 emission rates into the future.
-- most rates are left untouched, instead population information is altered future vehicles
-- use the model year groups that include 1993, thus tieing them to 1993 emissions.
--
-- author wesley faler
-- version 2016-10-04

drop procedure if exists spdorateofprogress;

beginblock
create procedure spdorateofprogress()
begin
	-- mode 0 is run after importing
	-- mode 1 is run to check overall success/failure
	declare mode int default ##mode##;
	declare isok int default 1;
	declare howmany int default 0;
	declare cutpoint int default 1993;
	declare cutpointfuelyear int default 1990;
	declare fuelyeartoreplace int default 1990;

	select modelyearid into cutpoint
	from modelyearcutpoints
	where cutpointname='RateOfProgress';

	set cutpoint=ifnull(cutpoint,1993);

	select fuelyearid into cutpointfuelyear
	from year
	where yearid=(select max(yearid) from year where yearid <= cutpoint);

	set cutpointfuelyear=ifnull(cutpointfuelyear,1990);

	-- update the calendar year's fuel year. this has a required ripple effect
	-- upon the fuelsupply and fuelusagefraction tables.
	update year set fuelyearid=cutpointfuelyear where yearid > cutpoint;

	drop table if exists tempmodelyear;
	create table if not exists tempmodelyear (
		modelyearid smallint not null primary key
	);

	insert into tempmodelyear(modelyearid) values(1960),(1961),(1962),(1963),(1964),(1965),(1966),(1967)
		,(1968),(1969),(1970),(1971),(1972),(1973),(1974),(1975),(1976),(1977),(1978),(1979),(1980),(1981),(1982),(1983),(1984),(1985),(1986),(1987)
		,(1988),(1989),(1990),(1991),(1992),(1993),(1994),(1995),(1996),(1997),(1998),(1999),(2000),(2001),(2002),(2003),(2004),(2005),(2006),(2007)
		,(2008),(2009),(2010),(2011),(2012),(2013),(2014),(2015),(2016),(2017),(2018),(2019),(2020),(2021),(2022),(2023),(2024),(2025),(2026),(2027)
		,(2028),(2029),(2030),(2031),(2032),(2033),(2034),(2035),(2036),(2037),(2038),(2039),(2040),(2041),(2042),(2043),(2044),(2045),(2046),(2047)
		,(2048),(2049),(2050),(2051),(2052),(2053),(2054),(2055),(2056),(2057),(2058),(2059),(2060);

	-- decode model year groups
	-- single years represent single years, unless it is 1972 which represents 1960-1972
	-- 0 represents 1960-2060
	drop table if exists tempmodelyeargroupdecode;
	create table tempmodelyeargroupdecode (
		modelyeargroupid int(11) not null primary key,
		modelyeargroupname char(50) default null,
		minmodelyearid smallint(6) default null,
		maxmodelyearid smallint(6) default null,
		cutoffflag smallint(6) default null
	);

	insert ignore into tempmodelyeargroupdecode (modelyeargroupid, modelyeargroupname)
	select modelyeargroupid, modelyeargroupname from modelyeargroup;

	insert ignore into tempmodelyeargroupdecode (modelyeargroupid, modelyeargroupname)
	select distinct modelyeargroupid, 'basefuel' from basefuel;

	insert ignore into tempmodelyeargroupdecode (modelyeargroupid, modelyeargroupname)
	select distinct modelyeargroupid, 'cumtvvcoeffs' from cumtvvcoeffs;

	insert ignore into tempmodelyeargroupdecode (modelyeargroupid, modelyeargroupname)
	select distinct modelyeargroupid, 'fuelmodelwtfactor' from fuelmodelwtfactor;

	insert ignore into tempmodelyeargroupdecode (modelyeargroupid, modelyeargroupname)
	select distinct modelyeargroupid, 'meanfuelparameters' from meanfuelparameters;

	insert ignore into tempmodelyeargroupdecode (modelyeargroupid, modelyeargroupname)
	select distinct modelyeargroupid, 'nono2ratio' from nono2ratio;

	insert ignore into tempmodelyeargroupdecode (modelyeargroupid, modelyeargroupname)
	select distinct modelyeargroupid, 'pollutantprocessmodelyear' from pollutantprocessmodelyear;

	insert ignore into tempmodelyeargroupdecode (modelyeargroupid, modelyeargroupname)
	select distinct modelyeargroupid, 'sourcebin' from sourcebin;

	insert ignore into tempmodelyeargroupdecode (modelyeargroupid, modelyeargroupname)
	select distinct modelyeargroupid, 'sourcetypemodelyeargroup' from sourcetypemodelyeargroup;

	insert ignore into tempmodelyeargroupdecode (modelyeargroupid, modelyeargroupname)
	select distinct modelyeargroupid, 'starttempadjustment' from starttempadjustment;

	insert ignore into tempmodelyeargroupdecode (modelyeargroupid, modelyeargroupname)
	select distinct modelyeargroupid, 'sulfateemissionrate' from sulfateemissionrate;

	insert ignore into tempmodelyeargroupdecode (modelyeargroupid, modelyeargroupname)
	select distinct modelyeargroupid, 'sulfurbase' from sulfurbase;

	insert ignore into tempmodelyeargroupdecode (modelyeargroupid, modelyeargroupname)
	select distinct fuelmygroupid, 'sulfurmodelcoeff' from sulfurmodelcoeff;

	insert ignore into tempmodelyeargroupdecode (modelyeargroupid, modelyeargroupname)
	select distinct fuelmygroupid, 'hcpermeationcoeff' from hcpermeationcoeff;

	insert ignore into tempmodelyeargroupdecode (modelyeargroupid, modelyeargroupname)
	select distinct fuelmygroupid, 'hcspeciation' from hcspeciation;

	insert ignore into tempmodelyeargroupdecode (modelyeargroupid, modelyeargroupname)
	select distinct modelyeargroupid, 'dioxinemissionrate' from dioxinemissionrate;
	
	insert ignore into tempmodelyeargroupdecode (modelyeargroupid, modelyeargroupname)
	select distinct modelyeargroupid, 'metalemissionrate' from metalemissionrate;
	
	insert ignore into tempmodelyeargroupdecode (modelyeargroupid, modelyeargroupname)
	select distinct modelyeargroupid, 'methanethcratio' from methanethcratio;
	
	insert ignore into tempmodelyeargroupdecode (modelyeargroupid, modelyeargroupname)
	select distinct modelyeargroupid, 'minorhapratio' from minorhapratio;
	
	insert ignore into tempmodelyeargroupdecode (modelyeargroupid, modelyeargroupname)
	select distinct modelyeargroupid, 'pahgasratio' from pahgasratio;
	
	insert ignore into tempmodelyeargroupdecode (modelyeargroupid, modelyeargroupname)
	select distinct modelyeargroupid, 'pahparticleratio' from pahparticleratio;

	insert ignore into tempmodelyeargroupdecode (modelyeargroupid, modelyeargroupname)
	select distinct modelyeargroupid, 'atrationongas' from atrationongas;


	update tempmodelyeargroupdecode set minmodelyearid=1960, maxmodelyearid=2060
	where minmodelyearid is null and maxmodelyearid is null
	and modelyeargroupid=0;

	update tempmodelyeargroupdecode set minmodelyearid=1960, maxmodelyearid=1972
	where minmodelyearid is null and maxmodelyearid is null
	and modelyeargroupid=1972;

	update tempmodelyeargroupdecode set minmodelyearid=modelyeargroupid, maxmodelyearid=modelyeargroupid
	where minmodelyearid is null and maxmodelyearid is null
	and modelyeargroupid < 9999 and modelyeargroupid > 1960;

	update tempmodelyeargroupdecode set minmodelyearid=round(modelyeargroupid/10000,0), maxmodelyearid=mod(modelyeargroupid,10000)
	where minmodelyearid is null and maxmodelyearid is null
	and modelyeargroupid > 9999;

	update tempmodelyeargroupdecode set cutoffflag=0 where cutoffflag is null and minmodelyearid <= cutpoint and maxmodelyearid >= cutpoint;
	update tempmodelyeargroupdecode set cutoffflag= -1 where cutoffflag is null and maxmodelyearid < cutpoint;
	update tempmodelyeargroupdecode set cutoffflag= +1 where cutoffflag is null and minmodelyearid > cutpoint;

	-- basefuel
	drop table if exists tempbasefuel;
	create table tempbasefuel
	select b.calculationengine, b.fueltypeid, b.fuelformulationid, b.description, b.datasourceid
	from basefuel b
		inner join tempmodelyeargroupdecode d on (d.modelyeargroupid=b.modelyeargroupid and d.cutoffflag=0);

	update basefuel, tempbasefuel, tempmodelyeargroupdecode
	set basefuel.fuelformulationid = tempbasefuel.fuelformulationid, basefuel.description = tempbasefuel.description, basefuel.datasourceid = tempbasefuel.datasourceid
	where basefuel.calculationengine = tempbasefuel.calculationengine
		and basefuel.fueltypeid = tempbasefuel.fueltypeid
		and tempmodelyeargroupdecode.modelyeargroupid = basefuel.modelyeargroupid
		and tempmodelyeargroupdecode.cutoffflag = 1;
	drop table if exists tempbasefuel;
	insert into tempmessages (message) values ('updated basefuel table');

	-- crankcaseemissionratio
	drop table if exists tempcrankcaseemissionratio;
	create table tempcrankcaseemissionratio
	select b.polprocessid, b.sourcetypeid, b.fueltypeid, b.crankcaseratio, b.crankcaseratiocv
	from crankcaseemissionratio b
	where minmodelyearid <= cutpoint
	and maxmodelyearid >= cutpoint;
	
	update crankcaseemissionratio, tempcrankcaseemissionratio
	set crankcaseemissionratio.crankcaseratio=tempcrankcaseemissionratio.crankcaseratio, crankcaseemissionratio.crankcaseratiocv=tempcrankcaseemissionratio.crankcaseratiocv
	where crankcaseemissionratio.polprocessid=tempcrankcaseemissionratio.polprocessid and crankcaseemissionratio.sourcetypeid=tempcrankcaseemissionratio.sourcetypeid and crankcaseemissionratio.fueltypeid=tempcrankcaseemissionratio.fueltypeid
	and crankcaseemissionratio.minmodelyearid > cutpoint;
	drop table if exists tempcrankcaseemissionratio;
	insert into tempmessages (message) values ('updated crankcaseemissionratio table');

	-- cumtvvcoeffs
	drop table if exists tempcumtvvcoeffs;
	create table tempcumtvvcoeffs
	select b.regclassid, b.agegroupid, b.polprocessid, b.tvvterma, b.tvvtermb, b.tvvtermc, b.tvvtermacv, b.tvvtermbcv, b.tvvtermccv, b.tvvtermaim, b.tvvtermbim, b.tvvtermcim, b.tvvtermaimcv, b.tvvtermbimcv, b.tvvtermcimcv, b.backpurgefactor, b.averagecanistercapacity, b.tvvequation, b.leakequation, b.leakfraction, b.tanksize, b.tankfillfraction
	from cumtvvcoeffs b
	inner join tempmodelyeargroupdecode d on (d.modelyeargroupid=b.modelyeargroupid and d.cutoffflag=0);
	
	update cumtvvcoeffs, tempcumtvvcoeffs, tempmodelyeargroupdecode
	set cumtvvcoeffs.tvvterma=tempcumtvvcoeffs.tvvterma, cumtvvcoeffs.tvvtermb=tempcumtvvcoeffs.tvvtermb, cumtvvcoeffs.tvvtermc=tempcumtvvcoeffs.tvvtermc, cumtvvcoeffs.tvvtermacv=tempcumtvvcoeffs.tvvtermacv, cumtvvcoeffs.tvvtermbcv=tempcumtvvcoeffs.tvvtermbcv, cumtvvcoeffs.tvvtermccv=tempcumtvvcoeffs.tvvtermccv, cumtvvcoeffs.tvvtermaim=tempcumtvvcoeffs.tvvtermaim, cumtvvcoeffs.tvvtermbim=tempcumtvvcoeffs.tvvtermbim, cumtvvcoeffs.tvvtermcim=tempcumtvvcoeffs.tvvtermcim, cumtvvcoeffs.tvvtermaimcv=tempcumtvvcoeffs.tvvtermaimcv, cumtvvcoeffs.tvvtermbimcv=tempcumtvvcoeffs.tvvtermbimcv, cumtvvcoeffs.tvvtermcimcv=tempcumtvvcoeffs.tvvtermcimcv, cumtvvcoeffs.backpurgefactor=tempcumtvvcoeffs.backpurgefactor, cumtvvcoeffs.averagecanistercapacity=tempcumtvvcoeffs.averagecanistercapacity, cumtvvcoeffs.tvvequation=tempcumtvvcoeffs.tvvequation, cumtvvcoeffs.leakequation=tempcumtvvcoeffs.leakequation, cumtvvcoeffs.leakfraction=tempcumtvvcoeffs.leakfraction, cumtvvcoeffs.tanksize=tempcumtvvcoeffs.tanksize, cumtvvcoeffs.tankfillfraction=tempcumtvvcoeffs.tankfillfraction
	where cumtvvcoeffs.regclassid=tempcumtvvcoeffs.regclassid
	and cumtvvcoeffs.agegroupid=tempcumtvvcoeffs.agegroupid
	and cumtvvcoeffs.polprocessid=tempcumtvvcoeffs.polprocessid
	and tempmodelyeargroupdecode.modelyeargroupid = cumtvvcoeffs.modelyeargroupid
	and tempmodelyeargroupdecode.cutoffflag = 1;
	drop table if exists tempcumtvvcoeffs;
	insert into tempmessages (message) values ('updated cumtvvcoeffs table');

	-- fuelmodelwtfactor
	drop table if exists tempfuelmodelwtfactor;
	create table tempfuelmodelwtfactor
	select b.fuelmodelid, b.ageid, b.fuelmodelwtfactor, b.datasourceid
	from fuelmodelwtfactor b
	inner join tempmodelyeargroupdecode d on (d.modelyeargroupid=b.modelyeargroupid and d.cutoffflag=0);

	update fuelmodelwtfactor, tempfuelmodelwtfactor, tempmodelyeargroupdecode
	set fuelmodelwtfactor.fuelmodelwtfactor = tempfuelmodelwtfactor.fuelmodelwtfactor, fuelmodelwtfactor.datasourceid = tempfuelmodelwtfactor.datasourceid
	where fuelmodelwtfactor.fuelmodelid = tempfuelmodelwtfactor.fuelmodelid
		and fuelmodelwtfactor.ageid = tempfuelmodelwtfactor.ageid
		and tempmodelyeargroupdecode.modelyeargroupid = fuelmodelwtfactor.modelyeargroupid
		and tempmodelyeargroupdecode.cutoffflag = 1;
	drop table if exists tempfuelmodelwtfactor;
	insert into tempmessages (message) values ('updated fuelmodelwtfactor table');

	-- fuelsupply
	select min(fuelyearid) into fuelyeartoreplace from fuelsupply where fuelyearid >= cutpointfuelyear;
	set fuelyeartoreplace=ifnull(fuelyeartoreplace,1990);
	delete from fuelsupply where fuelyearid > fuelyeartoreplace;
	update fuelsupply set fuelyearid=cutpointfuelyear where fuelyearid=fuelyeartoreplace;

	-- fuelusagefraction
	select min(fuelyearid) into fuelyeartoreplace from fuelusagefraction where fuelyearid >= cutpointfuelyear;
	set fuelyeartoreplace=ifnull(fuelyeartoreplace,1990);
	delete from fuelusagefraction where fuelyearid > fuelyeartoreplace;
	update fuelusagefraction set fuelyearid=cutpointfuelyear where fuelyearid=fuelyeartoreplace;

	-- generalfuelratioexpression
	-- delete anything that starts after cutpoint.
	delete from generalfuelratioexpression where minmodelyearid > cutpoint;

	-- anything that applies prior to cutpoint should go unchanged.
	-- anything that applies to cutpoint should apply to all years afterwards.  this is safe
	-- to do as any equation that used to begin after cutpoint was deleted above.
	update generalfuelratioexpression set maxmodelyearid=2060
	where minmodelyearid <= cutpoint and maxmodelyearid >= cutpoint and maxmodelyearid < 2060;

	insert into tempmessages (message) values ('updated generalfuelratioexpression table');

	-- meanfuelparameters
	drop table if exists tempmeanfuelparameters;
	create table tempmeanfuelparameters
	select b.polprocessid, b.fueltypeid, b.fuelparameterid, b.basevalue, b.centeringvalue, b.stddevvalue, b.datasourceid
	from meanfuelparameters b
	inner join tempmodelyeargroupdecode d on (d.modelyeargroupid=b.modelyeargroupid and d.cutoffflag=0);
	
	update meanfuelparameters, tempmeanfuelparameters, tempmodelyeargroupdecode
	set meanfuelparameters.basevalue=tempmeanfuelparameters.basevalue, meanfuelparameters.centeringvalue=tempmeanfuelparameters.centeringvalue, meanfuelparameters.stddevvalue=tempmeanfuelparameters.stddevvalue, meanfuelparameters.datasourceid=tempmeanfuelparameters.datasourceid
	where meanfuelparameters.polprocessid=tempmeanfuelparameters.polprocessid and meanfuelparameters.fueltypeid=tempmeanfuelparameters.fueltypeid and meanfuelparameters.fuelparameterid=tempmeanfuelparameters.fuelparameterid
	and tempmodelyeargroupdecode.modelyeargroupid = meanfuelparameters.modelyeargroupid
	and tempmodelyeargroupdecode.cutoffflag = 1;
	drop table if exists tempmeanfuelparameters;
	insert into tempmessages (message) values ('updated meanfuelparameters table');

	-- nono2ratio
	drop table if exists tempnono2ratio;
	create table tempnono2ratio
	select b.polprocessid, b.sourcetypeid, b.fueltypeid, b.noxratio, b.noxratiocv, b.datasourceid
	from nono2ratio b
	inner join tempmodelyeargroupdecode d on (d.modelyeargroupid=b.modelyeargroupid and d.cutoffflag=0);
	
	update nono2ratio, tempnono2ratio, tempmodelyeargroupdecode
	set nono2ratio.noxratio=tempnono2ratio.noxratio, nono2ratio.noxratiocv=tempnono2ratio.noxratiocv, nono2ratio.datasourceid=tempnono2ratio.datasourceid
	where nono2ratio.polprocessid=tempnono2ratio.polprocessid and nono2ratio.sourcetypeid=tempnono2ratio.sourcetypeid and nono2ratio.fueltypeid=tempnono2ratio.fueltypeid
	and tempmodelyeargroupdecode.modelyeargroupid = nono2ratio.modelyeargroupid
	and tempmodelyeargroupdecode.cutoffflag = 1;
	drop table if exists tempnono2ratio;
	insert into tempmessages (message) values ('updated nono2ratio table');

	-- pollutantprocessmodelyear
	drop table if exists temppollutantprocessmodelyear;
	create table temppollutantprocessmodelyear
	select polprocessid, modelyeargroupid, fuelmygroupid, immodelyeargroupid
	from pollutantprocessmodelyear
	where modelyearid = cutpoint;

	update pollutantprocessmodelyear, temppollutantprocessmodelyear
	set pollutantprocessmodelyear.modelyeargroupid = temppollutantprocessmodelyear.modelyeargroupid,
		pollutantprocessmodelyear.fuelmygroupid = temppollutantprocessmodelyear.fuelmygroupid,
		pollutantprocessmodelyear.immodelyeargroupid = temppollutantprocessmodelyear.immodelyeargroupid
	where pollutantprocessmodelyear.polprocessid = temppollutantprocessmodelyear.polprocessid
		and pollutantprocessmodelyear.modelyearid > cutpoint;
	drop table if exists temppollutantprocessmodelyear;
	insert into tempmessages (message) values ('updated pollutantprocessmodelyear table');

	-- sourcetypemodelyear
	drop table if exists tempsourcetypemodelyear;
	create table tempsourcetypemodelyear
	select b.sourcetypeid, b.acpenetrationfraction, b.acpenetrationfractioncv
	from sourcetypemodelyear b
	where modelyearid=cutpoint;
	
	update sourcetypemodelyear, tempsourcetypemodelyear
	set sourcetypemodelyear.acpenetrationfraction=tempsourcetypemodelyear.acpenetrationfraction, sourcetypemodelyear.acpenetrationfractioncv=tempsourcetypemodelyear.acpenetrationfractioncv
	where sourcetypemodelyear.sourcetypeid=tempsourcetypemodelyear.sourcetypeid
	and sourcetypemodelyear.modelyearid > cutpoint;
	drop table if exists tempsourcetypemodelyear;
	insert into tempmessages (message) values ('updated sourcetypemodelyear table');

	-- sourcetypemodelyeargroup
	drop table if exists tempsourcetypemodelyeargroup;
	create table tempsourcetypemodelyeargroup
	select b.sourcetypeid, b.tanktemperaturegroupid
	from sourcetypemodelyeargroup b
	inner join tempmodelyeargroupdecode d on (d.modelyeargroupid=b.modelyeargroupid and d.cutoffflag=0);
	
	update sourcetypemodelyeargroup, tempsourcetypemodelyeargroup, tempmodelyeargroupdecode
	set sourcetypemodelyeargroup.tanktemperaturegroupid=tempsourcetypemodelyeargroup.tanktemperaturegroupid
	where sourcetypemodelyeargroup.sourcetypeid=tempsourcetypemodelyeargroup.sourcetypeid
	and tempmodelyeargroupdecode.modelyeargroupid = sourcetypemodelyeargroup.modelyeargroupid
	and tempmodelyeargroupdecode.cutoffflag = 1;
	drop table if exists tempsourcetypemodelyeargroup;
	insert into tempmessages (message) values ('updated sourcetypemodelyeargroup table');

	-- sourcetypetechadjustment
	drop table if exists tempsourcetypetechadjustment;
	create table tempsourcetypetechadjustment
	select processid, sourcetypeid, refuelingtechadjustment
	from sourcetypetechadjustment
	where modelyearid=cutpoint;

	update sourcetypetechadjustment, tempsourcetypetechadjustment
	set sourcetypetechadjustment.refuelingtechadjustment = tempsourcetypetechadjustment.refuelingtechadjustment
	where sourcetypetechadjustment.processid = tempsourcetypetechadjustment.processid
		and sourcetypetechadjustment.sourcetypeid = tempsourcetypetechadjustment.sourcetypeid
		and sourcetypetechadjustment.modelyearid > cutpoint;
	drop table if exists tempsourcetypetechadjustment;
	insert into tempmessages (message) values ('updated sourcetypetechadjustment table');

	-- starttempadjustment
	drop table if exists tempstarttempadjustment;
	create table tempstarttempadjustment
	select b.fueltypeid, b.polprocessid, b.opmodeid, b.tempadjustterma, b.tempadjusttermacv, b.tempadjusttermb, b.tempadjusttermbcv, b.tempadjusttermc, b.tempadjusttermccv
	from starttempadjustment b
	inner join tempmodelyeargroupdecode d on (d.modelyeargroupid=b.modelyeargroupid and d.cutoffflag=0);
	
	update starttempadjustment, tempstarttempadjustment, tempmodelyeargroupdecode
	set starttempadjustment.tempadjustterma=tempstarttempadjustment.tempadjustterma, starttempadjustment.tempadjusttermacv=tempstarttempadjustment.tempadjusttermacv, starttempadjustment.tempadjusttermb=tempstarttempadjustment.tempadjusttermb, starttempadjustment.tempadjusttermbcv=tempstarttempadjustment.tempadjusttermbcv, starttempadjustment.tempadjusttermc=tempstarttempadjustment.tempadjusttermc, starttempadjustment.tempadjusttermccv=tempstarttempadjustment.tempadjusttermccv
	where starttempadjustment.fueltypeid=tempstarttempadjustment.fueltypeid and starttempadjustment.polprocessid=tempstarttempadjustment.polprocessid and starttempadjustment.opmodeid=tempstarttempadjustment.opmodeid
	and tempmodelyeargroupdecode.modelyeargroupid = starttempadjustment.modelyeargroupid
	and tempmodelyeargroupdecode.cutoffflag = 1;
	drop table if exists tempstarttempadjustment;
	insert into tempmessages (message) values ('updated starttempadjustment table');

	-- sulfateemissionrate
	drop table if exists tempsulfateemissionrate;
	create table tempsulfateemissionrate
	select b.polprocessid, b.fueltypeid, b.meanbaserate, b.meanbaseratecv, b.datasourceid
	from sulfateemissionrate b
	inner join tempmodelyeargroupdecode d on (d.modelyeargroupid=b.modelyeargroupid and d.cutoffflag=0);
	
	update sulfateemissionrate, tempsulfateemissionrate, tempmodelyeargroupdecode
	set sulfateemissionrate.meanbaserate=tempsulfateemissionrate.meanbaserate, sulfateemissionrate.meanbaseratecv=tempsulfateemissionrate.meanbaseratecv, sulfateemissionrate.datasourceid=tempsulfateemissionrate.datasourceid
	where sulfateemissionrate.polprocessid=tempsulfateemissionrate.polprocessid and sulfateemissionrate.fueltypeid=tempsulfateemissionrate.fueltypeid
	and tempmodelyeargroupdecode.modelyeargroupid = sulfateemissionrate.modelyeargroupid
	and tempmodelyeargroupdecode.cutoffflag = 1;
	drop table if exists tempsulfateemissionrate;
	insert into tempmessages (message) values ('updated sulfateemissionrate table');

	-- sulfurbase
	drop table if exists tempsulfurbase;
	create table tempsulfurbase
	select b.sulfurbase, b.sulfurbasis, b.sulfurgpamax
	from sulfurbase b
	inner join tempmodelyeargroupdecode d on (d.modelyeargroupid=b.modelyeargroupid and d.cutoffflag=0);
	
	update sulfurbase, tempsulfurbase, tempmodelyeargroupdecode
	set sulfurbase.sulfurbase=tempsulfurbase.sulfurbase, sulfurbase.sulfurbasis=tempsulfurbase.sulfurbasis, sulfurbase.sulfurgpamax=tempsulfurbase.sulfurgpamax
	where 
	tempmodelyeargroupdecode.modelyeargroupid = sulfurbase.modelyeargroupid
	and tempmodelyeargroupdecode.cutoffflag = 1;
	drop table if exists tempsulfurbase;
	insert into tempmessages (message) values ('updated sulfurbase table');

	-- sulfurmodelcoeff
	drop table if exists tempsulfurmodelcoeff;
	create table tempsulfurmodelcoeff
	select b.processid, b.pollutantid, b.m6emitterid, b.sourcetypeid, b.sulfurfunctionid, b.sulfurcoeff, b.lowsulfurcoeff
	from sulfurmodelcoeff b
	inner join tempmodelyeargroupdecode d on (d.modelyeargroupid=b.fuelmygroupid and d.cutoffflag=0);
	
	update sulfurmodelcoeff, tempsulfurmodelcoeff, tempmodelyeargroupdecode
	set sulfurmodelcoeff.sulfurcoeff=tempsulfurmodelcoeff.sulfurcoeff, sulfurmodelcoeff.lowsulfurcoeff=tempsulfurmodelcoeff.lowsulfurcoeff, sulfurmodelcoeff.sulfurfunctionid=tempsulfurmodelcoeff.sulfurfunctionid
	where sulfurmodelcoeff.processid=tempsulfurmodelcoeff.processid 
	and sulfurmodelcoeff.pollutantid=tempsulfurmodelcoeff.pollutantid 
	and sulfurmodelcoeff.m6emitterid=tempsulfurmodelcoeff.m6emitterid 
	and sulfurmodelcoeff.sourcetypeid=tempsulfurmodelcoeff.sourcetypeid 
	and tempmodelyeargroupdecode.modelyeargroupid = sulfurmodelcoeff.fuelmygroupid
	and tempmodelyeargroupdecode.cutoffflag = 1;
	drop table if exists tempsulfurmodelcoeff;
	insert into tempmessages (message) values ('updated sulfurmodelcoeff table');

	-- hcpermeationcoeff
	drop table if exists temphcpermeationcoeff;
	create table temphcpermeationcoeff
	select polprocessid, etohthreshid, fueladjustment, fueladjustmentgpa, datasourceid
	from hcpermeationcoeff b
	inner join tempmodelyeargroupdecode d on (d.modelyeargroupid=b.fuelmygroupid and d.cutoffflag=0);

	update hcpermeationcoeff, temphcpermeationcoeff, tempmodelyeargroupdecode
	set hcpermeationcoeff.fueladjustment=temphcpermeationcoeff.fueladjustment, 
		hcpermeationcoeff.fueladjustmentgpa=temphcpermeationcoeff.fueladjustmentgpa, 
		hcpermeationcoeff.datasourceid=temphcpermeationcoeff.datasourceid
	where hcpermeationcoeff.polprocessid=temphcpermeationcoeff.polprocessid 
	and hcpermeationcoeff.etohthreshid=temphcpermeationcoeff.etohthreshid 
	and tempmodelyeargroupdecode.modelyeargroupid = hcpermeationcoeff.fuelmygroupid
	and tempmodelyeargroupdecode.cutoffflag = 1;
	drop table if exists temphcpermeationcoeff;
	insert into tempmessages (message) values ('updated hcpermeationcoeff table');

	-- hcspeciation
	drop table if exists temphcspeciation;
	create table temphcspeciation
	select polprocessid, fuelsubtypeid, etohthreshid, oxythreshid, speciationconstant, oxyspeciation
	from hcspeciation b
	inner join tempmodelyeargroupdecode d on (d.modelyeargroupid=b.fuelmygroupid and d.cutoffflag=0);

	update hcspeciation, temphcspeciation, tempmodelyeargroupdecode
	set hcspeciation.speciationconstant=temphcspeciation.speciationconstant, 
		hcspeciation.oxyspeciation=temphcspeciation.oxyspeciation
	where hcspeciation.polprocessid=temphcspeciation.polprocessid 
	and hcspeciation.fuelsubtypeid=temphcspeciation.fuelsubtypeid
	and hcspeciation.etohthreshid=temphcspeciation.etohthreshid 
	and hcspeciation.oxythreshid=temphcspeciation.oxythreshid 
	and tempmodelyeargroupdecode.modelyeargroupid = hcspeciation.fuelmygroupid
	and tempmodelyeargroupdecode.cutoffflag = 1;
	drop table if exists temphcspeciation;
	insert into tempmessages (message) values ('updated hcspeciation table');

	-- dioxinemissionrate
	drop table if exists tempdioxinemissionrate;
	create table tempdioxinemissionrate
	select b.polprocessid, b.fueltypeid, b.units, b.meanbaserate, b.meanbaseratecv, b.datasourceid
	from dioxinemissionrate b
	inner join tempmodelyeargroupdecode d on (d.modelyeargroupid=b.modelyeargroupid and d.cutoffflag=0);
	
	update dioxinemissionrate, tempdioxinemissionrate, tempmodelyeargroupdecode
	set dioxinemissionrate.units=tempdioxinemissionrate.units, dioxinemissionrate.meanbaserate=tempdioxinemissionrate.meanbaserate, dioxinemissionrate.meanbaseratecv=tempdioxinemissionrate.meanbaseratecv, dioxinemissionrate.datasourceid=tempdioxinemissionrate.datasourceid
	where dioxinemissionrate.polprocessid=tempdioxinemissionrate.polprocessid
	and dioxinemissionrate.fueltypeid=tempdioxinemissionrate.fueltypeid
	and tempmodelyeargroupdecode.modelyeargroupid = dioxinemissionrate.modelyeargroupid
	and tempmodelyeargroupdecode.cutoffflag = 1;
	drop table if exists tempdioxinemissionrate;
	insert into tempmessages (message) values ('updated dioxinemissionrate table');
	
	-- metalemissionrate
	drop table if exists tempmetalemissionrate;
	create table tempmetalemissionrate
	select b.polprocessid, b.fueltypeid, b.sourcetypeid, b.units, b.meanbaserate, b.meanbaseratecv, b.datasourceid
	from metalemissionrate b
	inner join tempmodelyeargroupdecode d on (d.modelyeargroupid=b.modelyeargroupid and d.cutoffflag=0);
	
	update metalemissionrate, tempmetalemissionrate, tempmodelyeargroupdecode
	set metalemissionrate.units=tempmetalemissionrate.units, metalemissionrate.meanbaserate=tempmetalemissionrate.meanbaserate, metalemissionrate.meanbaseratecv=tempmetalemissionrate.meanbaseratecv, metalemissionrate.datasourceid=tempmetalemissionrate.datasourceid
	where metalemissionrate.polprocessid=tempmetalemissionrate.polprocessid
	and metalemissionrate.fueltypeid=tempmetalemissionrate.fueltypeid
	and metalemissionrate.sourcetypeid=tempmetalemissionrate.sourcetypeid
	and tempmodelyeargroupdecode.modelyeargroupid = metalemissionrate.modelyeargroupid
	and tempmodelyeargroupdecode.cutoffflag = 1;
	drop table if exists tempmetalemissionrate;
	insert into tempmessages (message) values ('updated metalemissionrate table');
	
	-- methanethcratio
	drop table if exists tempmethanethcratio;
	create table tempmethanethcratio
	select b.processid, b.fueltypeid, b.sourcetypeid, b.agegroupid, b.ch4thcratio, b.ch4thcratiocv
	from methanethcratio b
	inner join tempmodelyeargroupdecode d on (d.modelyeargroupid=b.modelyeargroupid and d.cutoffflag=0);
	
	update methanethcratio, tempmethanethcratio, tempmodelyeargroupdecode
	set methanethcratio.ch4thcratio=tempmethanethcratio.ch4thcratio, methanethcratio.ch4thcratiocv=tempmethanethcratio.ch4thcratiocv
	where methanethcratio.processid=tempmethanethcratio.processid
	and methanethcratio.fueltypeid=tempmethanethcratio.fueltypeid
	and methanethcratio.sourcetypeid=tempmethanethcratio.sourcetypeid
	and methanethcratio.agegroupid=tempmethanethcratio.agegroupid
	and tempmodelyeargroupdecode.modelyeargroupid = methanethcratio.modelyeargroupid
	and tempmodelyeargroupdecode.cutoffflag = 1;
	drop table if exists tempmethanethcratio;
	insert into tempmessages (message) values ('updated methanethcratio table');
	
	-- minorhapratio
	drop table if exists tempminorhapratio;
	create table tempminorhapratio
	select b.polprocessid, b.fueltypeid, b.fuelsubtypeid, b.atratio, b.atratiocv, b.datasourceid
	from minorhapratio b
	inner join tempmodelyeargroupdecode d on (d.modelyeargroupid=b.modelyeargroupid and d.cutoffflag=0);
	
	update minorhapratio, tempminorhapratio, tempmodelyeargroupdecode
	set minorhapratio.atratio=tempminorhapratio.atratio, minorhapratio.atratiocv=tempminorhapratio.atratiocv, minorhapratio.datasourceid=tempminorhapratio.datasourceid
	where minorhapratio.polprocessid=tempminorhapratio.polprocessid
	and minorhapratio.fueltypeid=tempminorhapratio.fueltypeid
	and minorhapratio.fuelsubtypeid=tempminorhapratio.fuelsubtypeid
	and tempmodelyeargroupdecode.modelyeargroupid = minorhapratio.modelyeargroupid
	and tempmodelyeargroupdecode.cutoffflag = 1;
	drop table if exists tempminorhapratio;
	insert into tempmessages (message) values ('updated minorhapratio table');
	
	-- pahgasratio
	drop table if exists temppahgasratio;
	create table temppahgasratio
	select b.polprocessid, b.fueltypeid, b.atratio, b.atratiocv, b.datasourceid
	from pahgasratio b
	inner join tempmodelyeargroupdecode d on (d.modelyeargroupid=b.modelyeargroupid and d.cutoffflag=0);
	
	update pahgasratio, temppahgasratio, tempmodelyeargroupdecode
	set pahgasratio.atratio=temppahgasratio.atratio, pahgasratio.atratiocv=temppahgasratio.atratiocv, pahgasratio.datasourceid=temppahgasratio.datasourceid
	where pahgasratio.polprocessid=temppahgasratio.polprocessid
	and pahgasratio.fueltypeid=temppahgasratio.fueltypeid
	and tempmodelyeargroupdecode.modelyeargroupid = pahgasratio.modelyeargroupid
	and tempmodelyeargroupdecode.cutoffflag = 1;
	drop table if exists temppahgasratio;
	insert into tempmessages (message) values ('updated pahgasratio table');
	
	-- pahparticleratio
	drop table if exists temppahparticleratio;
	create table temppahparticleratio
	select b.polprocessid, b.fueltypeid, b.atratio, b.atratiocv, b.datasourceid
	from pahparticleratio b
	inner join tempmodelyeargroupdecode d on (d.modelyeargroupid=b.modelyeargroupid and d.cutoffflag=0);
	
	update pahparticleratio, temppahparticleratio, tempmodelyeargroupdecode
	set pahparticleratio.atratio=temppahparticleratio.atratio, pahparticleratio.atratiocv=temppahparticleratio.atratiocv, pahparticleratio.datasourceid=temppahparticleratio.datasourceid
	where pahparticleratio.polprocessid=temppahparticleratio.polprocessid
	and pahparticleratio.fueltypeid=temppahparticleratio.fueltypeid
	and tempmodelyeargroupdecode.modelyeargroupid = pahparticleratio.modelyeargroupid
	and tempmodelyeargroupdecode.cutoffflag = 1;
	drop table if exists temppahparticleratio;
	insert into tempmessages (message) values ('updated pahparticleratio table');

	-- atrationongas
	drop table if exists tempatrationongas;
	create table tempatrationongas
	select b.polprocessid, b.sourcetypeid, b.fuelsubtypeid, b.atratio, b.atratiocv, b.datasourceid
	from atrationongas b
	inner join tempmodelyeargroupdecode d on (d.modelyeargroupid=b.modelyeargroupid and d.cutoffflag=0);
	
	update atrationongas, tempatrationongas, tempmodelyeargroupdecode
	set atrationongas.atratio=tempatrationongas.atratio, atrationongas.atratiocv=tempatrationongas.atratiocv, atrationongas.datasourceid=tempatrationongas.datasourceid
	where atrationongas.polprocessid=tempatrationongas.polprocessid
	and atrationongas.sourcetypeid=tempatrationongas.sourcetypeid
	and atrationongas.fuelsubtypeid=tempatrationongas.fuelsubtypeid
	and tempmodelyeargroupdecode.modelyeargroupid = atrationongas.modelyeargroupid
	and tempmodelyeargroupdecode.cutoffflag = 1;
	drop table if exists tempatrationongas;
	insert into tempmessages (message) values ('updated atrationongas table');




	drop table if exists tempmodelyear;
	-- drop table if exists tempmodelyeargroupdecode;
end
endblock

drop procedure if exists spcheckrateofprogressimprograms;

beginblock
create procedure spcheckrateofprogressimprograms()
begin
	-- insert messages beginning with warning: or error: to notify the user
	-- of im programs that do not make sense if the clean air act had not
	-- been enacted.
	-- insert into tempmessages (message) values ('warning: this is a test rop warning');
	-- insert into tempmessages (message) values ('error: this is a test rop error');

	insert into tempmessages (message) values ('checked im programs for rate of progress suitability');
end
endblock

call spdorateofprogress();
drop procedure if exists spdorateofprogress;

call spcheckrateofprogressimprograms();
drop procedure if exists spcheckrateofprogressimprograms;
