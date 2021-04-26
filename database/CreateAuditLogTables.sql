-- version 2008-05-22
-- create tables for tracking imports and database changes.

create table if not exists auditlog (
	whenhappened datetime not null,
	importername varchar(100) not null,
	briefdescription varchar(100) null,
	fulldescription varchar(4096) null,
	key logbydate (whenhappened),
	key logbyimporter (importername)
);
