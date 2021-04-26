-- pm2.5 speciation calculator
-- author wesley faler
-- version 2014-07-15

-- @algorithm
-- @owner sulfate pm calculator
-- @calculator

-- section create remote tables for extracted data

drop table if exists crankcasesplit;
create table if not exists crankcasesplit (
	processid smallint not null,
	pollutantid smallint not null,
	sourcetypeid smallint not null,
	fueltypeid smallint not null,
	minmodelyearid smallint not null,
	maxmodelyearid smallint not null,
	crankcaseratio double not null,
	primary key (pollutantid, sourcetypeid, fueltypeid, minmodelyearid, maxmodelyearid, processid)
);
truncate table crankcasesplit;

drop table if exists spmonecountyyeargeneralfuelratio;
create table if not exists spmonecountyyeargeneralfuelratio (
	fueltypeid int not null,
	sourcetypeid int not null,
	monthid int not null,
	pollutantid int not null,
	processid int not null,
	modelyearid int not null,
	yearid int not null,
	fueleffectratio double not null default '0',
	primary key (fueltypeid, sourcetypeid, monthid, pollutantid, modelyearid, yearid)
);
truncate table spmonecountyyeargeneralfuelratio;

drop table if exists onecountyyearsulfatefractions;
create table if not exists onecountyyearsulfatefractions (
	processid smallint not null,
	fueltypeid smallint not null,
	sourcetypeid smallint not null,
	monthid smallint not null,
	modelyearid smallint not null,
	sulfatenonecpmfraction double not null default '0',
	h2ononecpmfraction double not null default '0',
	unadjustedsulfatenonecpmfraction double not null default '0',
	unadjustedh2ononecpmfraction double not null default '0',
	primary key (processid, fueltypeid, sourcetypeid, monthid, modelyearid)
);
truncate table onecountyyearsulfatefractions;

drop table if exists onezoneyeartemperaturefactor;
create table if not exists onezoneyeartemperaturefactor (
	zoneid int not null,
	monthid smallint not null,
	hourid smallint not null,
	processid smallint not null,
	pollutantid smallint not null,
	fueltypeid smallint not null,
	sourcetypeid smallint not null,
	minmodelyearid smallint not null,
	maxmodelyearid smallint not null,
	correctionfactor double not null,
	primary key (zoneid, monthid, hourid, processid, pollutantid, fueltypeid, sourcetypeid, minmodelyearid, maxmodelyearid)
);
truncate table onezoneyeartemperaturefactor;

##create.pmspeciation##;
truncate table pmspeciation;

-- end section create remote tables for extracted data

-- section extract data

-- @algorithm get fuel effects for nonecnonso4pm (120).
cache select gfr.fueltypeid, gfr.sourcetypeid, may.monthid, gfr.pollutantid, gfr.processid, mya.modelyearid, mya.yearid,
	sum((ifnull(fueleffectratio,1)+gpafract*(ifnull(fueleffectratiogpa,1)-ifnull(fueleffectratio,1)))*marketshare) as fueleffectratio
	into outfile '##spmonecountyyeargeneralfuelratio##'
from runspecmonthgroup rsmg
inner join runspecmodelyearage mya on (mya.yearid = ##context.year##)
inner join county c on (c.countyid = ##context.iterlocation.countyrecordid##)
inner join year y on (y.yearid = mya.yearid)
inner join fuelsupply fs on (fs.fuelregionid = ##context.fuelregionid##
	and fs.fuelyearid = y.fuelyearid
	and fs.monthgroupid = rsmg.monthgroupid)
inner join monthofanyyear may on (may.monthgroupid = fs.monthgroupid)
inner join runspecsourcefueltype rssf
inner join generalfuelratio gfr on (gfr.fuelformulationid = fs.fuelformulationid
	and gfr.pollutantid in (120)
	and gfr.processid = ##context.iterprocess.databasekey##
	and gfr.minmodelyearid <= mya.modelyearid
	and gfr.maxmodelyearid >= mya.modelyearid
	and gfr.minageid <= mya.ageid
	and gfr.maxageid >= mya.ageid
	and gfr.fueltypeid = rssf.fueltypeid
	and gfr.sourcetypeid = rssf.sourcetypeid)
group by gfr.fueltypeid, gfr.sourcetypeid, may.monthid, gfr.pollutantid, gfr.processid, mya.modelyearid, mya.yearid
;

-- @algorithm calculate adjusted sulfatenonecpmfraction and h2ononecpmfraction 
-- using the sulfurlevel of available fuel formulations. weight each adjusted fraction
-- by formulation market share.
cache select
	sf.processid,
	sf.fueltypeid,
	sf.sourcetypeid,
	may.monthid,
	mya.modelyearid,
	sum(fs.marketshare * sulfatenonecpmfraction * (1 + basefuelsulfatefraction * ((coalesce(ff.sulfurlevel,0) / sf.basefuelsulfurlevel) - 1))) as sulfatenonecpmfraction,
	sum(fs.marketshare * h2ononecpmfraction * (1 + basefuelsulfatefraction * ((coalesce(ff.sulfurlevel,0) / sf.basefuelsulfurlevel) - 1))) as h2ononecpmfraction,
	sulfatenonecpmfraction as unadjustedsulfatenonecpmfraction,
	h2ononecpmfraction as unadjustedh2ononecpmfraction
	into outfile '##onecountyyearsulfatefractions##'
from runspecmonthgroup rsmg
inner join runspecmodelyearage mya on (mya.yearid = ##context.year##)
inner join county c on (c.countyid = ##context.iterlocation.countyrecordid##)
inner join year y on (y.yearid = mya.yearid)
inner join fuelsupply fs on (fs.fuelregionid = ##context.fuelregionid##
	and fs.fuelyearid = y.fuelyearid
	and fs.monthgroupid = rsmg.monthgroupid)
inner join monthofanyyear may on (may.monthgroupid = fs.monthgroupid)
inner join runspecsourcefueltype rssf
inner join fuelformulation ff on (ff.fuelformulationid = fs.fuelformulationid)
inner join fuelsubtype fst on (
	fst.fuelsubtypeid = ff.fuelsubtypeid
	and fst.fueltypeid = rssf.fueltypeid)
inner join sulfatefractions sf on (
	sf.minmodelyearid <= mya.modelyearid
	and sf.maxmodelyearid >= mya.modelyearid
	and sf.fueltypeid = rssf.fueltypeid
	and sf.sourcetypeid = rssf.sourcetypeid)
group by 
	sf.processid,
	sf.fueltypeid,
	sf.sourcetypeid,
	may.monthid,
	mya.modelyearid
order by null;

-- @algorithm collect speciation data.
cache select *
	into outfile '##pmspeciation##'
from pmspeciation
where processid in (##primaryandcrankcaseprocessids##)
and (outputpollutantid*100+processid) in (##polprocessids##);

-- @algorithm create temperature effects for sulfate (115), h2o (aersol) (119), and nonecnonso4pm (120).
cache select zoneid, monthid, hourid, processid, pollutantid, fueltypeid, sourcetypeid, minmodelyearid, maxmodelyearid,
	##context.temperaturefactorexpression##
	as correctionfactor
	into outfile '##onezoneyeartemperaturefactor##'
from zonemonthhour zmh, temperaturefactorexpression tfe
where zmh.zoneid = ##context.iterlocation.zonerecordid##
and tfe.minmodelyearid <= ##context.year##
and tfe.maxmodelyearid >= ##context.year## - 30
and tfe.processid = ##context.iterprocess.databasekey##
and tfe.pollutantid in (115, 119, 120);

-- @algorithm create crankcase split fractions for ec (112), sulfate (115), h2o (aersol) (119), and nonecnonso4pm (120).
-- the query must account for the lack of nonecnonso4pm in the pollutant table.
cache select processid, floor(r.polprocessid/100) as pollutantid, sourcetypeid, fueltypeid,
	minmodelyearid, maxmodelyearid, crankcaseratio
	into outfile '##crankcasesplit##'
from crankcaseemissionratio r, emissionprocess ep
where ep.processid in (##primaryandcrankcaseprocessids##)
and r.polprocessid in (112*100 + ep.processid, 115*100 + ep.processid, 119*100 + ep.processid, 120*100 + ep.processid);

-- end section extract data

-- section local data removal
-- end section local data removal

-- section processing

-- @algorithm
drop table if exists spmoutput;
create table spmoutput like movesworkeroutput;

-- @algorithm copy unadjusted ec (112) so it can be adjusted.
insert into spmoutput(movesrunid,iterationid,
	yearid,monthid,dayid,hourid,
	stateid,countyid,zoneid,linkid,
	pollutantid,processid,
	sourcetypeid,regclassid,fueltypeid,modelyearid,
	roadtypeid,scc,
	engtechid,sectorid,hpid,
	emissionquant,emissionrate)
select movesrunid,iterationid,
	yearid,monthid,dayid,hourid,
	stateid,countyid,zoneid,linkid,
	pollutantid,processid,
	sourcetypeid,regclassid,fueltypeid,modelyearid,
	roadtypeid,scc,
	engtechid,sectorid,hpid,
	emissionquant,emissionrate
from movesworkeroutput mwo
where pollutantid=112;

-- @algorithm remove unadjusted ec. the adjusted ec will be added later.
delete from movesworkeroutput where pollutantid=112;

-- @algorithm
drop table if exists spmsplit1;
create table spmsplit1 (
	processid smallint not null,
	fueltypeid smallint not null,
	sourcetypeid smallint not null,
	monthid smallint not null,
	modelyearid smallint not null,
	outputpollutantid smallint,
	conversionfraction double not null,
	primary key (processid, fueltypeid, sourcetypeid, monthid, modelyearid, outputpollutantid)
);

-- @algorithm specify the split to make sulfate (115) from nonecpm (118). sulfate = nonecpm * sulfatenonecpmfraction.
insert into spmsplit1 (processid, fueltypeid, sourcetypeid, monthid, modelyearid, outputpollutantid, conversionfraction)
select processid, fueltypeid, sourcetypeid, monthid, modelyearid, 115 as outputpollutantid,
	sulfatenonecpmfraction as conversionfraction
from onecountyyearsulfatefractions;

-- @algorithm specify the split to make h2o (aerosol) (119) from nonecpm (118). h2o = nonecpm * h2ononecpmfraction.
insert into spmsplit1 (processid, fueltypeid, sourcetypeid, monthid, modelyearid, outputpollutantid, conversionfraction)
select processid, fueltypeid, sourcetypeid, monthid, modelyearid, 119 as outputpollutantid,
	h2ononecpmfraction as conversionfraction
from onecountyyearsulfatefractions;

-- @algorithm specify the split to make nonecnonso4pm (120) from nonecpm (118). nonecnonso4pm = nonecpm * (1 - unadjustedh2ononecpmfraction - unadjustedsulfatenonecpmfraction).
insert into spmsplit1 (processid, fueltypeid, sourcetypeid, monthid, modelyearid, outputpollutantid, conversionfraction)
select processid, fueltypeid, sourcetypeid, monthid, modelyearid, 120 as outputpollutantid,
	greatest(1-unadjustedh2ononecpmfraction-unadjustedsulfatenonecpmfraction,0) as conversionfraction
from onecountyyearsulfatefractions;

-- @algorithm apply the splits, making sulfate (115), h2o (aerosol) (119), and nonecnonso4pm (120) from nonecpm (118).
insert into spmoutput(movesrunid,iterationid,
	yearid,monthid,dayid,hourid,
	stateid,countyid,zoneid,linkid,
	pollutantid,processid,
	sourcetypeid,regclassid,fueltypeid,modelyearid,
	roadtypeid,scc,
	engtechid,sectorid,hpid,
	emissionquant,emissionrate)
select mwo.movesrunid,mwo.iterationid,
	mwo.yearid,mwo.monthid,mwo.dayid,mwo.hourid,
	mwo.stateid,mwo.countyid,mwo.zoneid,mwo.linkid,
	s.outputpollutantid as pollutantid,mwo.processid,
	mwo.sourcetypeid,mwo.regclassid,mwo.fueltypeid,mwo.modelyearid,
	mwo.roadtypeid,mwo.scc,
	mwo.engtechid,mwo.sectorid,mwo.hpid,
	mwo.emissionquant*s.conversionfraction as emissionquant,
	mwo.emissionrate *s.conversionfraction as emissionrate
from movesworkeroutput mwo
inner join spmsplit1 s on (
	s.processid = mwo.processid
	and s.fueltypeid = mwo.fueltypeid
	and s.sourcetypeid = mwo.sourcetypeid
	and s.monthid = mwo.monthid
	and s.modelyearid = mwo.modelyearid)
where pollutantid=118;

-- @algorithm apply fuel effects. only nonecnonso4pm (120) is affected.
update spmoutput, spmonecountyyeargeneralfuelratio set 
	emissionquant=emissionquant*fueleffectratio,
	emissionrate =emissionrate *fueleffectratio
where spmonecountyyeargeneralfuelratio.fueltypeid = spmoutput.fueltypeid
and spmonecountyyeargeneralfuelratio.sourcetypeid = spmoutput.sourcetypeid
and spmonecountyyeargeneralfuelratio.monthid = spmoutput.monthid
and spmonecountyyeargeneralfuelratio.pollutantid = spmoutput.pollutantid
and spmonecountyyeargeneralfuelratio.processid = spmoutput.processid
and spmonecountyyeargeneralfuelratio.modelyearid = spmoutput.modelyearid
and spmonecountyyeargeneralfuelratio.yearid = spmoutput.yearid;

-- @algorithm apply temperature effects to sulfate, h2o (aersol), and nonecnonso4pm.
update spmoutput, onezoneyeartemperaturefactor set 
	emissionquant=emissionquant*correctionfactor,
	emissionrate =emissionrate *correctionfactor
where spmoutput.zoneid = onezoneyeartemperaturefactor.zoneid
and spmoutput.monthid = onezoneyeartemperaturefactor.monthid
and spmoutput.hourid = onezoneyeartemperaturefactor.hourid
and spmoutput.processid = onezoneyeartemperaturefactor.processid
and spmoutput.pollutantid = onezoneyeartemperaturefactor.pollutantid
and spmoutput.fueltypeid = onezoneyeartemperaturefactor.fueltypeid
and spmoutput.sourcetypeid = onezoneyeartemperaturefactor.sourcetypeid
and spmoutput.modelyearid >= onezoneyeartemperaturefactor.minmodelyearid
and spmoutput.modelyearid <= onezoneyeartemperaturefactor.maxmodelyearid;

-- @algorithm
drop table if exists spmoutput2;
create table spmoutput2 like spmoutput;
alter table spmoutput2 add key polproc (pollutantid, processid);
alter table spmoutput2 add key procpol (processid, pollutantid);

-- @algorithm split ec, sulfate, h2o (aersol), and nonecnonso4pm by crankcase effects.
insert into spmoutput2(movesrunid,iterationid,
	yearid,monthid,dayid,hourid,
	stateid,countyid,zoneid,linkid,
	pollutantid,processid,
	sourcetypeid,regclassid,fueltypeid,modelyearid,
	roadtypeid,scc,
	engtechid,sectorid,hpid,
	emissionquant,emissionrate)
select mwo.movesrunid,mwo.iterationid,
	mwo.yearid,mwo.monthid,mwo.dayid,mwo.hourid,
	mwo.stateid,mwo.countyid,mwo.zoneid,mwo.linkid,
	s.pollutantid as pollutantid,s.processid,
	mwo.sourcetypeid,mwo.regclassid,mwo.fueltypeid,mwo.modelyearid,
	mwo.roadtypeid,mwo.scc,
	mwo.engtechid,mwo.sectorid,mwo.hpid,
	mwo.emissionquant*s.crankcaseratio as emissionquant,
	mwo.emissionrate *s.crankcaseratio as emissionrate
from spmoutput mwo
inner join crankcasesplit s on (
	s.pollutantid = mwo.pollutantid
	and s.fueltypeid = mwo.fueltypeid
	and s.sourcetypeid = mwo.sourcetypeid
	and s.minmodelyearid <= mwo.modelyearid
	and s.maxmodelyearid >= mwo.modelyearid);

-- section makepm2.5total
-- @algorithm sum ec, nonecnonso4pm, sulfate, and h2o (aerosol) to make total pm 2.5 (110)
insert into movesworkeroutput(movesrunid,iterationid,
	yearid,monthid,dayid,hourid,
	stateid,countyid,zoneid,linkid,
	pollutantid,processid,
	sourcetypeid,regclassid,fueltypeid,modelyearid,
	roadtypeid,scc,
	engtechid,sectorid,hpid,
	emissionquant,emissionrate)
select movesrunid,iterationid,
	yearid,monthid,dayid,hourid,
	stateid,countyid,zoneid,linkid,
	110 as pollutantid,processid,
	sourcetypeid,regclassid,fueltypeid,modelyearid,
	roadtypeid,scc,
	engtechid,sectorid,hpid,
	sum(emissionquant),
	sum(emissionrate)
from spmoutput2
where pollutantid in (112,120,115,119)
and processid in (##primaryandcrankcaseprocessidsforpm25total##)
group by 
	processid,
	yearid,monthid,dayid,hourid,
	stateid,countyid,zoneid,linkid,
	sourcetypeid,regclassid,fueltypeid,modelyearid,
	roadtypeid,scc,
	engtechid,sectorid,hpid;
-- end section makepm2.5total

-- @algorithm copy ec, sulfate, h2o to the output.
insert into movesworkeroutput(movesrunid,iterationid,
	yearid,monthid,dayid,hourid,
	stateid,countyid,zoneid,linkid,
	pollutantid,processid,
	sourcetypeid,regclassid,fueltypeid,modelyearid,
	roadtypeid,scc,
	engtechid,sectorid,hpid,
	emissionquant,emissionrate)
select movesrunid,iterationid,
	yearid,monthid,dayid,hourid,
	stateid,countyid,zoneid,linkid,
	pollutantid,processid,
	sourcetypeid,regclassid,fueltypeid,modelyearid,
	roadtypeid,scc,
	engtechid,sectorid,hpid,
	emissionquant,emissionrate
from spmoutput2
where pollutantid in (112,115,119);

-- note: to get nonecnonso4pm in the output for debugging purposes, add 120 to the
-- list of pollutantids above.

-- @algorithm remove unadjusted nonecpm (118).
delete from movesworkeroutput where pollutantid=118 and processid in (##primaryandcrankcaseprocessids##);

-- @algorithm sum the adjusted nonecnonso4pm, sulfate, and h2o (aerosol) to make the adjusted nonecpm (118).
insert into movesworkeroutput(movesrunid,iterationid,
	yearid,monthid,dayid,hourid,
	stateid,countyid,zoneid,linkid,
	pollutantid,processid,
	sourcetypeid,regclassid,fueltypeid,modelyearid,
	roadtypeid,scc,
	engtechid,sectorid,hpid,
	emissionquant,emissionrate)
select movesrunid,iterationid,
	yearid,monthid,dayid,hourid,
	stateid,countyid,zoneid,linkid,
	118 as pollutantid,processid,
	sourcetypeid,regclassid,fueltypeid,modelyearid,
	roadtypeid,scc,
	engtechid,sectorid,hpid,
	sum(emissionquant),
	sum(emissionrate)
from spmoutput2
where pollutantid in (120,115,119)
and processid in (##primaryandcrankcaseprocessids##)
group by 
	processid,
	yearid,monthid,dayid,hourid,
	stateid,countyid,zoneid,linkid,
	sourcetypeid,regclassid,fueltypeid,modelyearid,
	roadtypeid,scc,
	engtechid,sectorid,hpid;

-- @algorithm speciate the remaining pollutants. species output = pmspeciationfraction * species input
insert into movesworkeroutput(movesrunid,iterationid,
	yearid,monthid,dayid,hourid,
	stateid,countyid,zoneid,linkid,
	pollutantid,processid,
	sourcetypeid,regclassid,fueltypeid,modelyearid,
	roadtypeid,scc,
	engtechid,sectorid,hpid,
	emissionquant,emissionrate)
select spm.movesrunid,spm.iterationid,
	spm.yearid,spm.monthid,spm.dayid,spm.hourid,
	spm.stateid,spm.countyid,spm.zoneid,spm.linkid,
	ps.outputpollutantid as pollutantid, spm.processid,
	spm.sourcetypeid,spm.regclassid,spm.fueltypeid,spm.modelyearid,
	spm.roadtypeid,spm.scc,
	spm.engtechid,spm.sectorid,spm.hpid,
	spm.emissionquant * ps.pmspeciationfraction as emissionquant,
	spm.emissionrate  * ps.pmspeciationfraction as emissionrate
from spmoutput2 spm
inner join pmspeciation ps on (
	ps.processid = spm.processid
	and ps.inputpollutantid = spm.pollutantid
	and ps.sourcetypeid = spm.sourcetypeid
	and ps.fueltypeid = spm.fueltypeid
	and ps.minmodelyearid <= spm.modelyearid
	and ps.maxmodelyearid >= spm.modelyearid
);

-- end section processing

-- section cleanup

drop table if exists crankcasesplit;
drop table if exists spmonecountyyeargeneralfuelratio;
drop table if exists onecountyyearsulfatefractions;
drop table if exists spmoutput;
drop table if exists spmoutput2;
drop table if exists spmsplit1;

-- end section cleanup
