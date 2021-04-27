#!/bin/sh
if  [ -d sharedwork/ ]
then
rm -r sharedwork/*
fi

if  [-d WorkerFolder/]
then
rm -r WorkerFolder/*
fi

if  [-d moveslog_old.txt]
then
rm moveslog_old.txt
fi

if  [-d moveslog.txt]
then
rm moveslog.txt
fi
call setenv.csh
ant rungui
