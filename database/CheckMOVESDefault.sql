/*
checks for the movesdefault schema.
*/
select
	stateid,
	statename
from state
limit 1;

select
	countyid,
	countyname,
	stateid
from county
limit 1;
