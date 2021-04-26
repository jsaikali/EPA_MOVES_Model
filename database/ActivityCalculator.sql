-- version 2017-09-29
-- author wesley faler

-- @algorithm
-- @owner activity calculator
-- @calculator

-- section create remote tables for extracted data

##create.hourday##;
truncate table hourday;

##create.link##;
truncate table link;

##create.sourceusetype##;
truncate table sourceusetype;

##create.runspecsourcetype##;
truncate table runspecsourcetype;

##create.runspecsourcefueltype##;
truncate table runspecsourcefueltype;

-- the sections are listed here according to activty type, not alphabetically
-- section sourcehours
##create.sourcehours##;
truncate table sourcehours;
-- end section sourcehours

-- section extendedidlehours
##create.extendedidlehours##;
truncate table extendedidlehours;
-- end section extendedidlehours

-- section hotellinghours
##create.hotellinghours##;
truncate table hotellinghours;

##create.hotellingactivitydistribution##;
truncate table hotellingactivitydistribution;
-- end section hotellinghours

-- section sho
##create.sho##;
truncate table sho;
-- end section sho

-- section oni
##create.sho##;
truncate table sho;
-- end section oni

-- section shp
##create.shp##;
truncate table shp;
-- end section shp

-- section population

-- section nonprojectdomain
##create.sourcetypeagepopulation##;
truncate table sourcetypeagepopulation;

##create.fractionwithinhpmsvtype##;
truncate table fractionwithinhpmsvtype;

##create.analysisyearvmt##;
truncate table analysisyearvmt;

##create.roadtypedistribution##;
truncate table roadtypedistribution;

##create.zoneroadtype##;
truncate table zoneroadtype;
-- end section nonprojectdomain

-- section projectdomain
##create.offnetworklink##;
truncate table offnetworklink;

##create.linksourcetypehour##;
truncate table linksourcetypehour;

##create.sourcetypeagedistribution##;
truncate table sourcetypeagedistribution;
-- end section projectdomain

-- end section population

-- section starts
##create.starts##;
truncate table starts;
-- end section starts

create table if not exists sourcetypefuelfraction (
	sourcetypeid smallint not null,
	modelyearid smallint not null,
	fueltypeid smallint not null,
	fuelfraction double not null,
	primary key (sourcetypeid, modelyearid, fueltypeid),
	key (modelyearid, sourcetypeid, fueltypeid),
	key (modelyearid, fueltypeid, sourcetypeid)
);
truncate table sourcetypefuelfraction;

-- section withregclassid
##create.regclasssourcetypefraction##;
truncate table regclasssourcetypefraction;
-- end section withregclassid

-- end section create remote tables for extracted data

-- section extract data

select *
into outfile '##hourday##'
from hourday;

-- select all links in the current zone. this is required population calculations in project domain.
-- link is further filtered where needed.
select *
into outfile '##link##'
from link
where zoneid = ##context.iterlocation.zonerecordid##;

select *
into outfile '##sourceusetype##'
from sourceusetype;

select *
into outfile '##runspecsourcetype##'
from runspecsourcetype;

select *
into outfile '##runspecsourcefueltype##'
from runspecsourcefueltype;

-- section sourcehours
select sourcehours.* 
into outfile '##sourcehours##'
from sourcehours
inner join runspecmonth using (monthid)
where yearid = ##context.year##
and linkid = ##context.iterlocation.linkrecordid##;
-- end section sourcehours

-- section extendedidlehours
select extendedidlehours.*
into outfile '##extendedidlehours##'
from extendedidlehours
where yearid = ##context.year##
and zoneid = ##context.iterlocation.zonerecordid##;
-- end section extendedidlehours

-- section hotellinghours
select hotellinghours.*
into outfile '##hotellinghours##'
from hotellinghours
where yearid = ##context.year##
and zoneid = ##context.iterlocation.zonerecordid##;

cache select *
into outfile '##hotellingactivitydistribution##'
from hotellingactivitydistribution
where opmodeid <> 200
and beginmodelyearid <= ##context.year##
and endmodelyearid >= ##context.year## - 30
and zoneid = ##hotellingactivityzoneid##;
-- end section hotellinghours

-- section sho
select sho.* 
into outfile '##sho##'
from sho
inner join runspecmonth using (monthid)
where yearid = ##context.year##
and linkid = ##context.iterlocation.linkrecordid##;
-- end section sho

-- section oni
select sho.* 
into outfile '##sho##'
from sho
inner join runspecmonth using (monthid)
where yearid = ##context.year##
and linkid = ##context.iterlocation.linkrecordid##;
-- end section oni

-- section shp
select shp.* 
into outfile '##shp##'
from shp
where yearid = ##context.year##
and zoneid = ##context.iterlocation.zonerecordid##;
-- end section shp

-- section population

-- section nonprojectdomain
cache select *
into outfile '##sourcetypeagepopulation##'
from sourcetypeagepopulation
where yearid = ##context.year##;

cache select *
into outfile '##fractionwithinhpmsvtype##'
from fractionwithinhpmsvtype
where yearid = ##context.year##;

cache select *
into outfile '##analysisyearvmt##'
from analysisyearvmt
where yearid = ##context.year##;

cache select *
into outfile '##roadtypedistribution##'
from roadtypedistribution;

cache select zoneid,
	roadtypeid,
	sum(shoallocfactor) as shoallocfactor
into outfile '##zoneroadtype##'
from zoneroadtype
where zoneid=##context.iterlocation.zonerecordid##
group by roadtypeid;
-- end section nonprojectdomain

-- section projectdomain
cache select *
into outfile '##offnetworklink##'
from offnetworklink;

cache select linksourcetypehour.*
into outfile '##linksourcetypehour##'
from linksourcetypehour
inner join link on (link.linkid = linksourcetypehour.linkid)
where zoneid = ##context.iterlocation.zonerecordid##;

cache select sourcetypeagedistribution.*
into outfile '##sourcetypeagedistribution##'
from sourcetypeagedistribution
inner join runspecsourcetype using (sourcetypeid)
where yearid = ##context.year##;
-- end section projectdomain

-- end section population

-- section starts
select starts.* into outfile '##starts##'
from starts
where yearid = ##context.year##
and zoneid = ##context.iterlocation.zonerecordid##;
-- end section starts

-- section createsourcetypefuelfraction
drop table if exists sourcetypefuelfraction;
drop table if exists sourcetypefuelfractiontemp;
drop table if exists sourcetypefuelfractiontotal;

create table if not exists sourcetypefuelfraction (
	sourcetypeid smallint not null,
	modelyearid smallint not null,
	fueltypeid smallint not null,
	fuelfraction double not null,
	primary key (sourcetypeid, modelyearid, fueltypeid),
	key (modelyearid, sourcetypeid, fueltypeid),
	key (modelyearid, fueltypeid, sourcetypeid)
);

create table sourcetypefuelfractiontemp (
	sourcetypemodelyearid int not null,
	fueltypeid smallint not null,
	tempfuelfraction double,
	primary key (sourcetypemodelyearid, fueltypeid),
	key (fueltypeid, sourcetypemodelyearid)
);

create table sourcetypefuelfractiontotal (
	sourcetypemodelyearid int not null,
	temptotal double,
	sourcetypeid smallint null,
	modelyearid smallint null,
	primary key (sourcetypemodelyearid)
);

-- section usesamplevehiclepopulation
insert into sourcetypefuelfractiontemp (sourcetypemodelyearid, fueltypeid, tempfuelfraction)
select sourcetypemodelyearid, fueltypeid, sum(stmyfraction) as tempfuelfraction
from samplevehiclepopulation
group by sourcetypemodelyearid, fueltypeid
order by null;
-- end section usesamplevehiclepopulation

-- section usefuelusagefraction
insert into sourcetypefuelfractiontemp (sourcetypemodelyearid, fueltypeid, tempfuelfraction)
select sourcetypemodelyearid, fuelsupplyfueltypeid as fueltypeid, 
	sum(stmyfraction*usagefraction) as tempfuelfraction
from samplevehiclepopulation svp
inner join fuelusagefraction fuf on (
	fuf.sourcebinfueltypeid = svp.fueltypeid
)
where fuf.countyid = ##context.iterlocation.countyrecordid##
and fuf.fuelyearid = ##context.fuelyearid##
and fuf.modelyeargroupid = 0
group by sourcetypemodelyearid, fuelsupplyfueltypeid
order by null;
-- end section usefuelusagefraction

insert into sourcetypefuelfractiontotal (sourcetypemodelyearid, temptotal)
select sourcetypemodelyearid, sum(stmyfraction) as temptotal
from samplevehiclepopulation
group by sourcetypemodelyearid
order by null;

update sourcetypefuelfractiontotal, sourcetypemodelyear set sourcetypefuelfractiontotal.sourcetypeid=sourcetypemodelyear.sourcetypeid,
	sourcetypefuelfractiontotal.modelyearid=sourcetypemodelyear.modelyearid
where sourcetypemodelyear.sourcetypemodelyearid=sourcetypefuelfractiontotal.sourcetypemodelyearid;

insert into sourcetypefuelfraction (sourcetypeid, modelyearid, fueltypeid, fuelfraction)
select t.sourcetypeid, t.modelyearid, r.fueltypeid,
	case when temptotal > 0 then tempfuelfraction / temptotal
	else 0 end as fuelfraction
from sourcetypefuelfractiontemp r
inner join sourcetypefuelfractiontotal t on (t.sourcetypemodelyearid=r.sourcetypemodelyearid)
inner join runspecsourcefueltype rs on (rs.sourcetypeid=t.sourcetypeid and rs.fueltypeid=r.fueltypeid);

drop table if exists sourcetypefuelfractiontemp;
drop table if exists sourcetypefuelfractiontotal;
-- end section createsourcetypefuelfraction

-- section usesamplevehiclepopulation
cache select *
into outfile '##sourcetypefuelfraction##'
from sourcetypefuelfraction;
-- end section usesamplevehiclepopulation

-- section usefuelusagefraction
cache(countyid=##context.iterlocation.countyrecordid##,fuelyearid=##context.fuelyearid##) select *
into outfile '##sourcetypefuelfraction##'
from sourcetypefuelfraction;
-- end section usefuelusagefraction

-- section withregclassid
cache select *
into outfile '##regclasssourcetypefraction##'
from regclasssourcetypefraction
where modelyearid <= ##context.year##
and modelyearid >= ##context.year## - 30;
-- end section withregclassid

-- end section extract data

-- section local data removal
--truncate xxxxxx;
-- end section local data removal

-- section processing

-- 2, "sourcehours", "source hours"
-- 3, "extidle", "extended idle hours"
-- 4, "sho", "source hours operating"
-- 5, "shp", "source hours parked"
-- 6, "population", "population"
-- 7, "starts", "starts"
-- 13, "hotellingaux", "hotelling diesel aux"
-- 14, "hotellingelectric", "hotelling battery or ac"
-- 15, "hotellingoff", "hotelling all engines off"

-- section sourcehours
-- 2, "sourcehours", "source hours"

-- section withregclassid
-- @algorithm sourcehours = sourcehours[sourcetypeid,hourdayid,monthid,yearid,ageid,linkid]*fuelfraction[sourcetypeid,modelyearid,fueltypeid]*regclassfraction[fueltypeid,modelyearid,sourcetypeid,regclassid]
insert into ##activitytable## (yearid, monthid, dayid, hourid, stateid, countyid,
		zoneid, linkid, sourcetypeid, regclassid, fueltypeid, modelyearid, roadtypeid, scc,
		activitytypeid, activity)
select s.yearid, s.monthid, h.dayid, h.hourid,
		##context.iterlocation.staterecordid## as stateid,
		##context.iterlocation.countyrecordid## as countyid,
		##context.iterlocation.zonerecordid## as zoneid,
		s.linkid, s.sourcetypeid, stf.regclassid,
		stff.fueltypeid as fueltypeid,
		(s.yearid-s.ageid) as modelyearid,
		l.roadtypeid as roadtypeid,
		null as scc,
		2 as activitytypeid,
		(sourcehours*stff.fuelfraction*stf.regclassfraction) as activity
from sourcehours s
inner join hourday h on h.hourdayid=s.hourdayid
inner join sourcetypefuelfraction stff on (stff.sourcetypeid=s.sourcetypeid and stff.modelyearid=(s.yearid-s.ageid))
inner join link l on (l.linkid=s.linkid)
inner join regclasssourcetypefraction stf on (
	stf.sourcetypeid = stff.sourcetypeid
	and stf.fueltypeid = stff.fueltypeid
	and stf.modelyearid = stff.modelyearid
);
-- end section withregclassid

-- section noregclassid
insert into ##activitytable## (yearid, monthid, dayid, hourid, stateid, countyid,
		zoneid, linkid, sourcetypeid, fueltypeid, modelyearid, roadtypeid, scc,
		activitytypeid, activity)
select s.yearid, s.monthid, h.dayid, h.hourid,
		##context.iterlocation.staterecordid## as stateid,
		##context.iterlocation.countyrecordid## as countyid,
		##context.iterlocation.zonerecordid## as zoneid,
		s.linkid, s.sourcetypeid,
		stff.fueltypeid as fueltypeid,
		(s.yearid-s.ageid) as modelyearid,
		l.roadtypeid as roadtypeid,
		null as scc,
		2 as activitytypeid,
		(sourcehours*stff.fuelfraction) as activity
from sourcehours s
inner join hourday h on h.hourdayid=s.hourdayid
inner join sourcetypefuelfraction stff on (stff.sourcetypeid=s.sourcetypeid and stff.modelyearid=(s.yearid-s.ageid))
inner join link l on (l.linkid=s.linkid);
-- end section noregclassid

-- end section sourcehours

-- section extendedidlehours
-- 3, "extidle", "extended idle hours"

-- section withregclassid
-- @algorithm extendedidlehours = extendedidlehours[sourcetypeid,hourdayid,monthid,yearid,ageid,zoneid]*fuelfraction[sourcetypeid,modelyearid,fueltypeid]*regclassfraction[fueltypeid,modelyearid,sourcetypeid,regclassid]
insert into ##activitytable## (yearid, monthid, dayid, hourid, stateid, countyid,
		zoneid, linkid, sourcetypeid, regclassid, fueltypeid, modelyearid, roadtypeid, scc,
		activitytypeid, activity)
select s.yearid, s.monthid, h.dayid, h.hourid,
		##context.iterlocation.staterecordid## as stateid,
		##context.iterlocation.countyrecordid## as countyid,
		zoneid,
		##context.iterlocation.linkrecordid## linkid, s.sourcetypeid, stf.regclassid,
		stff.fueltypeid as fueltypeid,
		(s.yearid-s.ageid) as modelyearid,
		##context.iterlocation.roadtyperecordid## as roadtypeid,
		null as scc,
		3 as activitytypeid,
		(extendedidlehours*stff.fuelfraction*stf.regclassfraction) as activity
from extendedidlehours s
inner join hourday h on h.hourdayid=s.hourdayid
inner join sourcetypefuelfraction stff on (stff.sourcetypeid=s.sourcetypeid and stff.modelyearid=(s.yearid-s.ageid))
inner join regclasssourcetypefraction stf on (
	stf.sourcetypeid = stff.sourcetypeid
	and stf.fueltypeid = stff.fueltypeid
	and stf.modelyearid = stff.modelyearid
);
-- end section withregclassid

-- section noregclassid
insert into ##activitytable## (yearid, monthid, dayid, hourid, stateid, countyid,
		zoneid, linkid, sourcetypeid, fueltypeid, modelyearid, roadtypeid, scc,
		activitytypeid, activity)
select s.yearid, s.monthid, h.dayid, h.hourid,
		##context.iterlocation.staterecordid## as stateid,
		##context.iterlocation.countyrecordid## as countyid,
		zoneid,
		##context.iterlocation.linkrecordid## linkid, s.sourcetypeid,
		stff.fueltypeid as fueltypeid,
		(s.yearid-s.ageid) as modelyearid,
		##context.iterlocation.roadtyperecordid## as roadtypeid,
		null as scc,
		3 as activitytypeid,
		(extendedidlehours*stff.fuelfraction) as activity
from extendedidlehours s
inner join hourday h on h.hourdayid=s.hourdayid
inner join sourcetypefuelfraction stff on (stff.sourcetypeid=s.sourcetypeid and stff.modelyearid=(s.yearid-s.ageid));
-- end section noregclassid

-- end section extendedidlehours

-- section sho
-- 4, "sho", "source hours operating"

-- section withregclassid
-- @algorithm sho = sho[sourcetypeid,hourdayid,monthid,yearid,ageid,linkid]*fuelfraction[sourcetypeid,modelyearid,fueltypeid]*regclassfraction[fueltypeid,modelyearid,sourcetypeid,regclassid]
insert into ##activitytable## (yearid, monthid, dayid, hourid, stateid, countyid,
		zoneid, linkid, sourcetypeid, regclassid, fueltypeid, modelyearid, roadtypeid, scc,
		activitytypeid, activity)
select s.yearid, s.monthid, h.dayid, h.hourid,
		##context.iterlocation.staterecordid## as stateid,
		##context.iterlocation.countyrecordid## as countyid,
		##context.iterlocation.zonerecordid## as zoneid,
		s.linkid, s.sourcetypeid, stf.regclassid,
		stff.fueltypeid as fueltypeid,
		(s.yearid-s.ageid) as modelyearid,
		l.roadtypeid as roadtypeid,
		null as scc,
		4 as activitytypeid,
		(sho*stff.fuelfraction*stf.regclassfraction) as activity
from sho s
inner join hourday h on h.hourdayid=s.hourdayid
inner join sourcetypefuelfraction stff on (stff.sourcetypeid=s.sourcetypeid and stff.modelyearid=(s.yearid-s.ageid))
inner join link l on (l.linkid=s.linkid)
inner join regclasssourcetypefraction stf on (
	stf.sourcetypeid = stff.sourcetypeid
	and stf.fueltypeid = stff.fueltypeid
	and stf.modelyearid = stff.modelyearid
);
-- end section withregclassid

-- section noregclassid
insert into ##activitytable## (yearid, monthid, dayid, hourid, stateid, countyid,
		zoneid, linkid, sourcetypeid, fueltypeid, modelyearid, roadtypeid, scc,
		activitytypeid, activity)
select s.yearid, s.monthid, h.dayid, h.hourid,
		##context.iterlocation.staterecordid## as stateid,
		##context.iterlocation.countyrecordid## as countyid,
		##context.iterlocation.zonerecordid## as zoneid,
		s.linkid, s.sourcetypeid,
		stff.fueltypeid as fueltypeid,
		(s.yearid-s.ageid) as modelyearid,
		l.roadtypeid as roadtypeid,
		null as scc,
		4 as activitytypeid,
		(sho*stff.fuelfraction) as activity
from sho s
inner join hourday h on h.hourdayid=s.hourdayid
inner join sourcetypefuelfraction stff on (stff.sourcetypeid=s.sourcetypeid and stff.modelyearid=(s.yearid-s.ageid))
inner join link l on (l.linkid=s.linkid);
-- end section noregclassid

-- end section sho

-- section shp
-- 5, "shp", "source hours parked"

-- section withregclassid
-- @algorithm shp = shp[sourcetypeid,hourdayid,monthid,yearid,ageid,zoneid]*fuelfraction[sourcetypeid,modelyearid,fueltypeid]*regclassfraction[fueltypeid,modelyearid,sourcetypeid,regclassid]
insert into ##activitytable## (yearid, monthid, dayid, hourid, stateid, countyid,
		zoneid, linkid, sourcetypeid, regclassid, fueltypeid, modelyearid, roadtypeid, scc,
		activitytypeid, activity)
select s.yearid, s.monthid, h.dayid, h.hourid,
		##context.iterlocation.staterecordid## as stateid,
		##context.iterlocation.countyrecordid## as countyid,
		zoneid,
		##context.iterlocation.linkrecordid## linkid, s.sourcetypeid, stf.regclassid,
		stff.fueltypeid as fueltypeid,
		(s.yearid-s.ageid) as modelyearid,
		##context.iterlocation.roadtyperecordid## as roadtypeid,
		null as scc,
		5 as activitytypeid,
		(shp*stff.fuelfraction*stf.regclassfraction) as activity
from shp s
inner join hourday h on h.hourdayid=s.hourdayid
inner join sourcetypefuelfraction stff on (stff.sourcetypeid=s.sourcetypeid and stff.modelyearid=(s.yearid-s.ageid))
inner join regclasssourcetypefraction stf on (
	stf.sourcetypeid = stff.sourcetypeid
	and stf.fueltypeid = stff.fueltypeid
	and stf.modelyearid = stff.modelyearid
);
-- end section withregclassid

-- section noregclassid
insert into ##activitytable## (yearid, monthid, dayid, hourid, stateid, countyid,
		zoneid, linkid, sourcetypeid, fueltypeid, modelyearid, roadtypeid, scc,
		activitytypeid, activity)
select s.yearid, s.monthid, h.dayid, h.hourid,
		##context.iterlocation.staterecordid## as stateid,
		##context.iterlocation.countyrecordid## as countyid,
		zoneid,
		##context.iterlocation.linkrecordid## linkid, s.sourcetypeid,
		stff.fueltypeid as fueltypeid,
		(s.yearid-s.ageid) as modelyearid,
		##context.iterlocation.roadtyperecordid## as roadtypeid,
		null as scc,
		5 as activitytypeid,
		(shp*stff.fuelfraction) as activity
from shp s
inner join hourday h on h.hourdayid=s.hourdayid
inner join sourcetypefuelfraction stff on (stff.sourcetypeid=s.sourcetypeid and stff.modelyearid=(s.yearid-s.ageid));
-- end section noregclassid

-- end section shp

-- section population
-- 6, "population", "population" (zone level)

-- section nonprojectdomain
drop table if exists fractionbysourcetypetemp;

-- @algorithm sutfraction[sourcetypeid] = sum(roadtypevmtfraction * shoallocfactor)/sum(roadtypevmtfraction)
-- @condition non-project domain
create table fractionbysourcetypetemp
select sut.sourcetypeid, sum(rtd.roadtypevmtfraction*zrt.shoallocfactor)/sum(rtd.roadtypevmtfraction) as sutfraction
from sourceusetype sut
inner join roadtypedistribution rtd on (rtd.sourcetypeid=sut.sourcetypeid)
inner join zoneroadtype zrt on (zrt.roadtypeid=rtd.roadtypeid and zrt.zoneid=##context.iterlocation.zonerecordid##)
group by sut.sourcetypeid
order by null;

drop table if exists sourcetypetemppopulation;

-- @algorithm temppopulation = sourcetypeagepopulation[yearid,sourcetypeid,ageid] * sutfraction[sourcetypeid]
-- @condition non-project domain
create table sourcetypetemppopulation
select t.sourcetypeid, stap.ageid, (population*sutfraction) as population, l.linkid
from fractionbysourcetypetemp t
inner join sourcetypeagepopulation stap on (stap.sourcetypeid=t.sourcetypeid)
inner join runspecsourcetype rsst on (rsst.sourcetypeid=stap.sourcetypeid)
inner join link l on (l.roadtypeid=1);

-- section withregclassid
-- @algorithm population = temppopulation*fuelfraction[sourcetypeid,modelyearid,fueltypeid]*regclassfraction[fueltypeid,modelyearid,sourcetypeid,regclassid]
-- @condition non-project domain
insert into ##activitytable## (yearid, monthid, dayid, hourid, stateid, countyid,
		zoneid, linkid, sourcetypeid, regclassid, fueltypeid, modelyearid, roadtypeid, scc,
		activitytypeid, activity)
select ##context.year##, 0 as monthid, 0 as dayid, 0 as hourid,
		##context.iterlocation.staterecordid## as stateid,
		##context.iterlocation.countyrecordid## as countyid,
		##context.iterlocation.zonerecordid## as zoneid,
		s.linkid,
		s.sourcetypeid, stf.regclassid,
		stff.fueltypeid as fueltypeid,
		(##context.year##-s.ageid) as modelyearid,
		1 as roadtypeid,
		null as scc,
		6 as activitytypeid,
		(s.population*stff.fuelfraction*stf.regclassfraction) as activity
from sourcetypetemppopulation s
inner join sourcetypefuelfraction stff on (stff.sourcetypeid=s.sourcetypeid and stff.modelyearid=(##context.year##-s.ageid))
inner join regclasssourcetypefraction stf on (
	stf.sourcetypeid = stff.sourcetypeid
	and stf.fueltypeid = stff.fueltypeid
	and stf.modelyearid = stff.modelyearid
);
-- end section withregclassid

-- section noregclassid
insert into ##activitytable## (yearid, monthid, dayid, hourid, stateid, countyid,
		zoneid, linkid, sourcetypeid, fueltypeid, modelyearid, roadtypeid, scc,
		activitytypeid, activity)
select ##context.year##, 0 as monthid, 0 as dayid, 0 as hourid,
		##context.iterlocation.staterecordid## as stateid,
		##context.iterlocation.countyrecordid## as countyid,
		##context.iterlocation.zonerecordid## as zoneid,
		s.linkid,
		s.sourcetypeid,
		stff.fueltypeid as fueltypeid,
		(##context.year##-s.ageid) as modelyearid,
		1 as roadtypeid,
		null as scc,
		6 as activitytypeid,
		(s.population*stff.fuelfraction) as activity
from sourcetypetemppopulation s
inner join sourcetypefuelfraction stff on (stff.sourcetypeid=s.sourcetypeid and stff.modelyearid=(##context.year##-s.ageid));
-- end section noregclassid

-- end section nonprojectdomain

-- section projectdomain

-- section withregclassid
-- @algorithm population on off-network link = vehiclepopulation*agefraction[yearid,sourcetypeid,ageid]*fuelfraction[sourcetypeid,modelyearid,fueltypeid]*regclassfraction[fueltypeid,modelyearid,sourcetypeid,regclassid]
-- @condition project domain
-- @condition offnetwork link
insert into ##activitytable## (yearid, monthid, dayid, hourid, stateid, countyid,
		zoneid, linkid, sourcetypeid, regclassid, fueltypeid, modelyearid, roadtypeid, scc,
		activitytypeid, activity)
select ##context.year##, 0 as monthid, 0 as dayid, 0 as hourid,
		##context.iterlocation.staterecordid## as stateid,
		##context.iterlocation.countyrecordid## as countyid,
		##context.iterlocation.zonerecordid## as zoneid,
		l.linkid,
		onl.sourcetypeid, stf.regclassid,
		stff.fueltypeid as fueltypeid,
		(##context.year##-stad.ageid) as modelyearid,
		1 as roadtypeid,
		null as scc,
		6 as activitytypeid,
		(onl.vehiclepopulation*stad.agefraction*stff.fuelfraction*stf.regclassfraction) as activity
from link l
inner join offnetworklink onl using (zoneid)
inner join sourcetypeagedistribution stad on (stad.sourcetypeid=onl.sourcetypeid)
inner join sourcetypefuelfraction stff on (stff.sourcetypeid=stad.sourcetypeid and stff.modelyearid=(##context.year##-stad.ageid))
inner join regclasssourcetypefraction stf on (
	stf.sourcetypeid = stff.sourcetypeid
	and stf.fueltypeid = stff.fueltypeid
	and stf.modelyearid = stff.modelyearid
)
and l.roadtypeid = 1
where l.zoneid = ##context.iterlocation.zonerecordid##;

-- @algorithm population on roadways = linkvolume[linkid]*sourcetypehourfraction[linkid,sourcetypeid]*agefraction[yearid,sourcetypeid,ageid]*fuelfraction[sourcetypeid,modelyearid,fueltypeid]*regclassfraction[fueltypeid,modelyearid,sourcetypeid,regclassid]
-- @condition project domain
-- @condition on roadways
insert into ##activitytable## (yearid, monthid, dayid, hourid, stateid, countyid,
		zoneid, linkid, sourcetypeid, regclassid, fueltypeid, modelyearid, roadtypeid, scc,
		activitytypeid, activity)
select ##context.year##, 0 as monthid, 0 as dayid, 0 as hourid,
		##context.iterlocation.staterecordid## as stateid,
		##context.iterlocation.countyrecordid## as countyid,
		##context.iterlocation.zonerecordid## as zoneid,
		l.linkid,
		lsth.sourcetypeid, stf.regclassid,
		stff.fueltypeid as fueltypeid,
		(##context.year##-stad.ageid) as modelyearid,
		roadtypeid,
		null as scc,
		6 as activitytypeid,
		(l.linkvolume*lsth.sourcetypehourfraction*stad.agefraction*stff.fuelfraction*stf.regclassfraction) as activity
from link l
inner join linksourcetypehour lsth on (lsth.linkid=l.linkid)
inner join sourcetypeagedistribution stad on (stad.sourcetypeid=lsth.sourcetypeid)
inner join sourcetypefuelfraction stff on (stff.sourcetypeid=stad.sourcetypeid and stff.modelyearid=(##context.year##-stad.ageid))
inner join regclasssourcetypefraction stf on (
	stf.sourcetypeid = stff.sourcetypeid
	and stf.fueltypeid = stff.fueltypeid
	and stf.modelyearid = stff.modelyearid
)
where l.roadtypeid<>1;
-- end section withregclassid

-- section noregclassid
insert into ##activitytable## (yearid, monthid, dayid, hourid, stateid, countyid,
		zoneid, linkid, sourcetypeid, fueltypeid, modelyearid, roadtypeid, scc,
		activitytypeid, activity)
select ##context.year##, 0 as monthid, 0 as dayid, 0 as hourid,
		##context.iterlocation.staterecordid## as stateid,
		##context.iterlocation.countyrecordid## as countyid,
		##context.iterlocation.zonerecordid## as zoneid,
		l.linkid,
		onl.sourcetypeid,
		stff.fueltypeid as fueltypeid,
		(##context.year##-stad.ageid) as modelyearid,
		1 as roadtypeid,
		null as scc,
		6 as activitytypeid,
		(onl.vehiclepopulation*stad.agefraction*stff.fuelfraction) as activity
from link l
inner join offnetworklink onl using (zoneid)
inner join sourcetypeagedistribution stad on (stad.sourcetypeid=onl.sourcetypeid)
inner join sourcetypefuelfraction stff on (stff.sourcetypeid=stad.sourcetypeid and stff.modelyearid=(##context.year##-stad.ageid))
where l.zoneid = ##context.iterlocation.zonerecordid##
and l.roadtypeid = 1;

insert into ##activitytable## (yearid, monthid, dayid, hourid, stateid, countyid,
		zoneid, linkid, sourcetypeid, fueltypeid, modelyearid, roadtypeid, scc,
		activitytypeid, activity)
select ##context.year##, 0 as monthid, 0 as dayid, 0 as hourid,
		##context.iterlocation.staterecordid## as stateid,
		##context.iterlocation.countyrecordid## as countyid,
		##context.iterlocation.zonerecordid## as zoneid,
		l.linkid,
		lsth.sourcetypeid,
		stff.fueltypeid as fueltypeid,
		(##context.year##-stad.ageid) as modelyearid,
		roadtypeid,
		null as scc,
		6 as activitytypeid,
		(l.linkvolume*lsth.sourcetypehourfraction*stad.agefraction*stff.fuelfraction) as activity
from link l
inner join linksourcetypehour lsth on (lsth.linkid=l.linkid)
inner join sourcetypeagedistribution stad on (stad.sourcetypeid=lsth.sourcetypeid)
inner join sourcetypefuelfraction stff on (stff.sourcetypeid=stad.sourcetypeid and stff.modelyearid=(##context.year##-stad.ageid))
where l.roadtypeid<>1;
-- end section noregclassid

-- end section projectdomain

-- end section population

-- section starts
-- 7, "starts", "starts"

-- section withregclassid
-- @algorithm starts = starts[sourcetypeid,hourdayid,monthid,yearid,ageid,zoneid]*fuelfraction[sourcetypeid,modelyearid,fueltypeid]*regclassfraction[fueltypeid,modelyearid,sourcetypeid,regclassid]
insert into ##activitytable## (yearid, monthid, dayid, hourid, stateid, countyid,
		zoneid, linkid, sourcetypeid, regclassid, fueltypeid, modelyearid, roadtypeid, scc,
		activitytypeid, activity)
select s.yearid, s.monthid, h.dayid, h.hourid,
		##context.iterlocation.staterecordid## as stateid,
		##context.iterlocation.countyrecordid## as countyid,
		##context.iterlocation.zonerecordid## as zoneid,
		##context.iterlocation.linkrecordid## as linkid,
		s.sourcetypeid, stf.regclassid,
		stff.fueltypeid as fueltypeid,
		(s.yearid-s.ageid) as modelyearid,
		##context.iterlocation.roadtyperecordid## as roadtypeid,
		null as scc,
		7 as activitytypeid,
		(starts*stff.fuelfraction*stf.regclassfraction) as activity
from starts s
inner join hourday h on h.hourdayid=s.hourdayid
inner join sourcetypefuelfraction stff on (stff.sourcetypeid=s.sourcetypeid and stff.modelyearid=(s.yearid-s.ageid))
inner join regclasssourcetypefraction stf on (
	stf.sourcetypeid = stff.sourcetypeid
	and stf.fueltypeid = stff.fueltypeid
	and stf.modelyearid = stff.modelyearid
);
-- end section withregclassid

-- section noregclassid
insert into ##activitytable## (yearid, monthid, dayid, hourid, stateid, countyid,
		zoneid, linkid, sourcetypeid, fueltypeid, modelyearid, roadtypeid, scc,
		activitytypeid, activity)
select s.yearid, s.monthid, h.dayid, h.hourid,
		##context.iterlocation.staterecordid## as stateid,
		##context.iterlocation.countyrecordid## as countyid,
		##context.iterlocation.zonerecordid## as zoneid,
		##context.iterlocation.linkrecordid## as linkid,
		s.sourcetypeid,
		stff.fueltypeid as fueltypeid,
		(s.yearid-s.ageid) as modelyearid,
		##context.iterlocation.roadtyperecordid## as roadtypeid,
		null as scc,
		7 as activitytypeid,
		(starts*stff.fuelfraction) as activity
from starts s
inner join hourday h on h.hourdayid=s.hourdayid
inner join sourcetypefuelfraction stff on (stff.sourcetypeid=s.sourcetypeid and stff.modelyearid=(s.yearid-s.ageid));
-- end section noregclassid

-- end section starts

-- section hotellinghours
-- 13, "hotellingaux", "hotelling diesel aux"
-- 14, "hotellingelectric", "hotelling battery or ac"
-- 15, "hotellingoff", "hotelling all engines off"

-- section withregclassid
-- @algorithm hotellingaux hours = hotellinghours[sourcetypeid,hourdayid,monthid,yearid,ageid,zoneid]*opmodefraction[opmodeid=201,modelyearid]*fuelfraction[sourcetypeid,modelyearid,fueltypeid]*regclassfraction[fueltypeid,modelyearid,sourcetypeid,regclassid].
-- hotellingelectric hours = hotellinghours[sourcetypeid,hourdayid,monthid,yearid,ageid,zoneid]*opmodefraction[opmodeid=203,modelyearid]*fuelfraction[sourcetypeid,modelyearid,fueltypeid]*regclassfraction[fueltypeid,modelyearid,sourcetypeid,regclassid].
-- hotellingoff hours = hotellinghours[sourcetypeid,hourdayid,monthid,yearid,ageid,zoneid]*opmodefraction[opmodeid=204,modelyearid]*fuelfraction[sourcetypeid,modelyearid,fueltypeid]*regclassfraction[fueltypeid,modelyearid,sourcetypeid,regclassid].
insert into ##activitytable## (yearid, monthid, dayid, hourid, stateid, countyid,
		zoneid, linkid, sourcetypeid, regclassid, fueltypeid, modelyearid, roadtypeid, scc,
		activitytypeid, activity)
select s.yearid, s.monthid, h.dayid, h.hourid,
		##context.iterlocation.staterecordid## as stateid,
		##context.iterlocation.countyrecordid## as countyid,
		s.zoneid,
		##context.iterlocation.linkrecordid## linkid, s.sourcetypeid, stf.regclassid,
		stff.fueltypeid as fueltypeid,
		(s.yearid-s.ageid) as modelyearid,
		##context.iterlocation.roadtyperecordid## as roadtypeid,
		null as scc,
		case when opmodeid=201 then 13
			when opmodeid=203 then 14
			when opmodeid=204 then 15
			else 8 end as activitytypeid,
		(hotellinghours*opmodefraction*stff.fuelfraction*stf.regclassfraction) as activity
from hotellinghours s
inner join hourday h on h.hourdayid=s.hourdayid
inner join sourcetypefuelfraction stff on (stff.sourcetypeid=s.sourcetypeid and stff.modelyearid=(s.yearid-s.ageid))
inner join regclasssourcetypefraction stf on (
	stf.sourcetypeid = stff.sourcetypeid
	and stf.fueltypeid = stff.fueltypeid
	and stf.modelyearid = stff.modelyearid)
inner join hotellingactivitydistribution ha on (
	ha.beginmodelyearid <= stf.modelyearid
	and ha.endmodelyearid >= stf.modelyearid
	and ha.opmodeid in (201,203,204));
-- end section withregclassid

-- section noregclassid
insert into ##activitytable## (yearid, monthid, dayid, hourid, stateid, countyid,
		zoneid, linkid, sourcetypeid, fueltypeid, modelyearid, roadtypeid, scc,
		activitytypeid, activity)
select s.yearid, s.monthid, h.dayid, h.hourid,
		##context.iterlocation.staterecordid## as stateid,
		##context.iterlocation.countyrecordid## as countyid,
		s.zoneid,
		##context.iterlocation.linkrecordid## linkid, s.sourcetypeid,
		stff.fueltypeid as fueltypeid,
		(s.yearid-s.ageid) as modelyearid,
		##context.iterlocation.roadtyperecordid## as roadtypeid,
		null as scc,
		case when opmodeid=201 then 13
			when opmodeid=203 then 14
			when opmodeid=204 then 15
			else 8 end as activitytypeid,
		(hotellinghours*opmodefraction*stff.fuelfraction) as activity
from hotellinghours s
inner join hourday h on h.hourdayid=s.hourdayid
inner join sourcetypefuelfraction stff on (stff.sourcetypeid=s.sourcetypeid and stff.modelyearid=(s.yearid-s.ageid))
inner join hotellingactivitydistribution ha on (
	ha.beginmodelyearid <= stff.modelyearid
	and ha.endmodelyearid >= stff.modelyearid
	and ha.opmodeid in (201,203,204));
-- end section noregclassid

-- end section hotellinghours

-- section oni
-- 16, "shi", "source hours idling" -- changed to sho (4)

-- section withregclassid
-- @algorithm shi = sho[roadtypeid=1,sourcetypeid,hourdayid,monthid,yearid,ageid,linkid]*fuelfraction[sourcetypeid,modelyearid,fueltypeid]*regclassfraction[fueltypeid,modelyearid,sourcetypeid,regclassid]
insert into ##activitytable## (yearid, monthid, dayid, hourid, stateid, countyid,
		zoneid, linkid, sourcetypeid, regclassid, fueltypeid, modelyearid, roadtypeid, scc,
		activitytypeid, activity)
select s.yearid, s.monthid, h.dayid, h.hourid,
		##context.iterlocation.staterecordid## as stateid,
		##context.iterlocation.countyrecordid## as countyid,
		##context.iterlocation.zonerecordid## as zoneid,
		s.linkid, s.sourcetypeid, stf.regclassid,
		stff.fueltypeid as fueltypeid,
		(s.yearid-s.ageid) as modelyearid,
		l.roadtypeid as roadtypeid,
		null as scc,
		4 as activitytypeid,
		(sho*stff.fuelfraction*stf.regclassfraction) as activity
from sho s
inner join hourday h on h.hourdayid=s.hourdayid
inner join sourcetypefuelfraction stff on (stff.sourcetypeid=s.sourcetypeid and stff.modelyearid=(s.yearid-s.ageid))
inner join link l on (l.linkid=s.linkid)
inner join regclasssourcetypefraction stf on (
	stf.sourcetypeid = stff.sourcetypeid
	and stf.fueltypeid = stff.fueltypeid
	and stf.modelyearid = stff.modelyearid
);
-- end section withregclassid

-- section noregclassid
insert into ##activitytable## (yearid, monthid, dayid, hourid, stateid, countyid,
		zoneid, linkid, sourcetypeid, fueltypeid, modelyearid, roadtypeid, scc,
		activitytypeid, activity)
select s.yearid, s.monthid, h.dayid, h.hourid,
		##context.iterlocation.staterecordid## as stateid,
		##context.iterlocation.countyrecordid## as countyid,
		##context.iterlocation.zonerecordid## as zoneid,
		s.linkid, s.sourcetypeid,
		stff.fueltypeid as fueltypeid,
		(s.yearid-s.ageid) as modelyearid,
		l.roadtypeid as roadtypeid,
		null as scc,
		4 as activitytypeid,
		(sho*stff.fuelfraction) as activity
from sho s
inner join hourday h on h.hourdayid=s.hourdayid
inner join sourcetypefuelfraction stff on (stff.sourcetypeid=s.sourcetypeid and stff.modelyearid=(s.yearid-s.ageid))
inner join link l on (l.linkid=s.linkid);
-- end section noregclassid

-- end section oni

-- end section processing

-- section cleanup
drop table if exists sourcetypefuelfraction;
drop table if exists fractionbysourcetypetemp;
drop table if exists sourcetypetemppopulation;
-- end section cleanup
