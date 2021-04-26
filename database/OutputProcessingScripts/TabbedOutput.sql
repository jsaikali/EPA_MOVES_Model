-- this mysql script produces tab-delimited output suitable for reading into an
-- excel spreadsheet from the moves maria database output tables.
-- three separate text files are produced.  they are:
--      movesoutputyyyymmddhhmmss.txt
--      movesactivityoutputyyyymmddhhmmss.txt
--      movesrunyyyymmddhhmmss.txt

-- these correspond directly to the similarily named moves output tables.

-- the script does not write out the moveserror table.

--
--
-- general:

flush tables;

set @datetime = concat(  mid(curdate(),1,4),
                         mid(curdate(),6,2),
                         mid(curdate(),9,2),
                         mid(curtime(),1,2),
                         mid(curtime(),4,2),
                         mid(curtime(),7,2) );

-- create 'movesoutputyyyymmddhhmmss.txt':


set @sql_text =
concat(
" select * ",
" into outfile ", "'movesoutput",
    @datetime,
    ".txt'",
    " fields terminated by '\t'",
    " lines terminated by '\r\n'",  
"from(",
    " select 'movesrunid',    'iterationid',       'yearid',             'monthid',           'dayid',
        'hourid',        'stateid',           'countyid',           'zoneid',            'linkid',
        'pollutantid',   'processid',         'sourcetypeid',       'regclassid',        'fueltypeid',
        'fuelsubtypeid',
        'modelyearid',   'roadtypeid',        'scc',                'engtechid',         'sectorid',
        'hpid',          'emissionquant',     'emissionquantmean',  'emissionquantsigma'",
    " union ",
    " select movesrunid,    iterationid,       yearid,             monthid,           dayid,
        hourid,        stateid,           countyid,           zoneid,            linkid,
        pollutantid,   processid,         sourcetypeid,       regclassid,        fueltypeid,
        fuelsubtypeid,
        modelyearid,   roadtypeid,        scc,                engtechid,         sectorid,
        hpid,          emissionquant,     emissionquantmean,  emissionquantsigma ",
    " from movesoutput",
")as t1;");




prepare s1 from @sql_text;
execute s1;
drop prepare s1;


-- create 'movesactivityoutputyyyymmddhhmmss.txt':
set @sql_text =
   concat (
" select * ",
" into outfile ", "'movesactivityoutput",
    @datetime,
    ".txt'",
    " fields terminated by '\t'",
    " lines terminated by '\r\n'",  
"from(",
    " select 'movesrunid',     'iterationid',  'yearid',        'monthid',       'dayid',
            'hourid',         'stateid',      'countyid',      'zoneid',        'linkid',
            'sourcetypeid',   'regclassid',   'fueltypeid',    'fuelsubtypeid', 'modelyearid',
            'roadtypeid',     'scc',          'engtechid',     'sectorid',      'hpid',
            'activitytypeid', 'activity',     'activitymean',  'activitysigma'",
    " union ",
    " select movesrunid,     iterationid,  yearid,        monthid,       dayid,
        hourid,         stateid,      countyid,      zoneid,        linkid,
        sourcetypeid,   regclassid,   fueltypeid,    fuelsubtypeid, modelyearid,
        roadtypeid,     scc,          engtechid,     sectorid,      hpid,
        activitytypeid, activity,     activitymean,  activitysigma",
    " from movesactivityoutput",
")as t1;");


prepare s2 from @sql_text;
execute s2;
drop prepare s2;


-- create 'movesrunyyyymmddhhmmss.txt':

set @sql_text =
concat (
" select * ",
" into outfile ", "'movesrun",
    @datetime,
    ".txt'",
    " fields terminated by '\t'",
    " lines terminated by '\r\n'",  
"from(",
    " select 'movesrunid',          'outputtimeperiod',  'timeunits',            'distanceunits',
        'massunits',           'energyunits',       'runspecfilename',      'runspecdescription',
        'runspecfiledatetime', 'rundatetime',       'scale',                'minutesduration',
        'defaultdatabaseused', 'masterversion',     'mastercomputerid',     'masteridnumber',
        'domain',              'domaincountyid',   'domaincountyname',    'domaindatabaseserver',
        'domaindatabasename',  'expecteddonefiles', 'retrieveddonefiles',   'models'",
    " union ",
    " select movesrunid,          outputtimeperiod,  timeunits,            distanceunits,
        massunits,           energyunits,       runspecfilename,      runspecdescription,
        runspecfiledatetime, rundatetime,       scale,                minutesduration,
        defaultdatabaseused, masterversion,     mastercomputerid,     masteridnumber,
        domain,              domaincountyid,   domaincountyname,    domaindatabaseserver,
        domaindatabasename,  expecteddonefiles, retrieveddonefiles,   models",
    " from movesrun",
")as t1;");


prepare s3 from @sql_text;
execute s3;
drop prepare s3;


flush tables;
