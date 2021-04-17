@echo off
SET PATH=jre\bin;ant\bin;%PATH%
REM SET PATH=jre\bin;ant\bin;
SET JAVA_TOOL_OPTIONS=-Dfile.encoding=UTF-8
SET ANT_OPTS=-XX:-UseGCOverheadLimit
