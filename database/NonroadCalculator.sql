-- nonroad calculator script.
-- author wesley faler
-- version 2015-04-07

-- @algorithm
-- @owner nonroad calculator
-- @calculator

-- section create remote tables for extracted data

##create.nrequipmenttype##;
truncate table nrequipmenttype;

##create.nrscc##;
truncate table nrscc;

##create.enginetech##;
truncate table enginetech;

##create.nrsourceusetype##;
truncate table nrsourceusetype;

-- end section create remote tables for extracted data

-- section extract data

-- end section extract data

-- section local data removal
-- end section local data removal

-- section processing

nonroad monthtorealdayfactor=##nrmonthtorealdayfactor## ##nrpolprocessids##;

-- end section processing

-- section cleanup
drop table if exists nrequipmenttype;
drop table if exists nrscc;
drop table if exists enginetech;
drop table if exists nrsourceusetype;
-- end section cleanup
