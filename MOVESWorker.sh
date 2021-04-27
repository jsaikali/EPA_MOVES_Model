#!/bin/sh
if [-d WorkerFolder/]
then 
rm -r WorkerFolder/*
fi
call setenv.csh
ant runworker
