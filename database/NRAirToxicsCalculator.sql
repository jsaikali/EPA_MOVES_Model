-- author wesley faler
-- version 2015-04-07

-- @algorithm
-- @owner nonroad air toxics calculator
-- @calculator

-- section create remote tables for extracted data

##create.nratratio##;
truncate nratratio;

##create.nrdioxinemissionrate##;
truncate nrdioxinemissionrate;

##create.nrintegratedspecies##;
truncate nrintegratedspecies;

##create.nrmetalemissionrate##;
truncate nrmetalemissionrate;

##create.nrpahgasratio##;
truncate nrpahgasratio;

##create.nrpahparticleratio##;
truncate nrpahparticleratio;

-- end section create remote tables for extracted data

-- section extract data

-- section usenratratio
cache select pollutantid, processid, engtechid, fuelsubtypeid, nrhpcategory, atratio
into outfile '##nratratio##'
from nratratio
where (pollutantid*100+processid) in (##outputnratratio##);
-- end section usenratratio

-- section usenrdioxinemissionrate
cache select pollutantid, processid, fueltypeid, engtechid, nrhpcategory, meanbaserate
into outfile '##nrdioxinemissionrate##'
from nrdioxinemissionrate
where (pollutantid*100+processid) in (##outputnrdioxinemissionrate##);
-- end section usenrdioxinemissionrate

-- section usenonhaptog
cache select pollutantid
into outfile '##nrintegratedspecies##'
from nrintegratedspecies;
-- end section usenonhaptog

-- section usenrmetalemissionrate
cache select pollutantid, processid, fueltypeid, engtechid, nrhpcategory, meanbaserate
into outfile '##nrmetalemissionrate##'
from nrmetalemissionrate
where (pollutantid*100+processid) in (##outputnrmetalemissionrate##);
-- end section usenrmetalemissionrate

-- section usenrpahgasratio
cache select pollutantid, processid, fueltypeid, engtechid, nrhpcategory, atratio
into outfile '##nrpahgasratio##'
from nrpahgasratio
where (pollutantid*100+processid) in (##outputnrpahgasratio##);
-- end section usenrpahgasratio

-- section usenrpahparticleratio
cache select pollutantid, processid, fueltypeid, engtechid, nrhpcategory, atratio
into outfile '##nrpahparticleratio##'
from nrpahparticleratio
where (pollutantid*100+processid) in (##outputnrpahparticleratio##);
-- end section usenrpahparticleratio

-- end section extract data

-- section processing

-- all processing logic is done in the external calculator.

-- end section processing

-- section cleanup

-- end section cleanup

-- section final cleanup
-- end section final cleanup
