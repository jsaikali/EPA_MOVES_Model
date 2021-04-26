-- author wesley faler
-- version 2015-02-07

-- @algorithm
-- @owner nonroad hc speciation calculator
-- @calculator

-- section create remote tables for extracted data

##create.nrhcspeciation##;
truncate table nrhcspeciation;

##create.nrmethanethcratio##;
truncate table nrmethanethcratio;

-- end section create remote tables for extracted data

-- section extract data

cache select pollutantid,processid,engtechid,fuelsubtypeid,nrhpcategory,speciationconstant
into outfile '##nrhcspeciation##'
from nrhcspeciation
where (pollutantid*100+processid) in (##hcpolprocessids##);

cache select processid,engtechid,fuelsubtypeid,nrhpcategory,ch4thcratio
into outfile '##nrmethanethcratio##'
from nrmethanethcratio
where processid in (##hcprocessids##)
and fuelsubtypeid in (##macro.csv.all.nrfuelsubtypeid##);

-- end section extract data

-- section processing

-- all processing logic is done in the external calculator.

-- end section processing

-- section cleanup
drop table if exists hcetohbin;
drop table if exists nrhcspeciation;
-- end section cleanup

-- section final cleanup
-- end section final cleanup
