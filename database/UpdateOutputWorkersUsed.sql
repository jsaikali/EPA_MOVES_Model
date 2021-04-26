-- version 2006-08-05
-- updates pre-task 220 output databases to be task 220-compliant

alter table movesrun
	add column minutesduration		float not null default 0,
	add column defaultdatabaseused	varchar(200) null,
	add column masterversiondate	char(10) null,
	add column mastercomputerid	 	varchar(20) null
;

create table movesworkersused (
	movesrunid			smallint unsigned not null,
	workerversion		char(10) not null,
	workercomputerid	varchar(20) not null,
	workerid			varchar(10),
	
	primary key (movesrunid, workerversion, workercomputerid, workerid),

	bundlecount			integer unsigned not null default 0,
	failedbundlecount	integer unsigned not null default 0
);
