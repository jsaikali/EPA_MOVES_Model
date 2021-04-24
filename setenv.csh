#!/bin/bash

export MOVES_HOME=/home/ec2-user/climateaction/EPA_MOVES_Model

export CLASSPATH=$MOVES_HOME
export CLASSPATH=$CLASSPATH":$MOVES_HOME/libs/jai_codec.jar"
export CLASSPATH=$CLASSPATH":$MOVES_HOME/libs/TestNG/velocity-dep-1.4.jar"
export CLASSPATH=$CLASSPATH":$MOVES_HOME/libs/TestNG/commons-beanutils.jar"
export CLASSPATH=$CLASSPATH":$MOVES_HOME/libs/TestNG/reportng-1.1.5.jar"
export CLASSPATH=$CLASSPATH":$MOVES_HOME/libs/TestNG/testng-6.3.jar"
export CLASSPATH=$CLASSPATH":$MOVES_HOME/libs/TestNG/guice-3.0.jar"
export CLASSPATH=$CLASSPATH":$MOVES_HOME/libs/poi/commons-logging-1.1.jar"
export CLASSPATH=$CLASSPATH":$MOVES_HOME/libs/poi/poi-ooxml-3.9-20121203.jar"
export CLASSPATH=$CLASSPATH":$MOVES_HOME/libs/poi/xmlbeans-2.3.0.jar"
export CLASSPATH=$CLASSPATH":$MOVES_HOME/libs/poi/log4j-1.2.13.jar"
export CLASSPATH=$CLASSPATH":$MOVES_HOME/libs/poi/jsr173_1.0_api.jar"
export CLASSPATH=$CLASSPATH":$MOVES_HOME/libs/poi/commons-codec-1.5.jar"
export CLASSPATH=$CLASSPATH":$MOVES_HOME/libs/poi/poi-3.9-20121203.jar"
export CLASSPATH=$CLASSPATH":$MOVES_HOME/libs/poi/dom4j-1.6.1.jar"
export CLASSPATH=$CLASSPATH":$MOVES_HOME/libs/poi/poi-ooxml-3.5-beta5-20090219.jar"
export CLASSPATH=$CLASSPATH":$MOVES_HOME/libs/poi/poi-ooxml-schemas-3.9-20121203.jar"
export CLASSPATH=$CLASSPATH":$MOVES_HOME/libs/poi/stax-api-1.0.1.jar"
export CLASSPATH=$CLASSPATH":$MOVES_HOME/libs/poi/poi-3.5-beta5-20090219.jar"
export CLASSPATH=$CLASSPATH":$MOVES_HOME/libs/poi/ooxml-schemas-1.0.jar"
export CLASSPATH=$CLASSPATH":$MOVES_HOME/libs/mysql-connector-java-5.1.17-bin.jar"
export CLASSPATH=$CLASSPATH":$MOVES_HOME/libs/xml-apis.jar"
export CLASSPATH=$CLASSPATH":$MOVES_HOME/libs/ant-contrib-1.0b3.jar"
export CLASSPATH=$CLASSPATH":$MOVES_HOME/libs/xercesImpl.jar"
export CLASSPATH=$CLASSPATH":$MOVES_HOME/libs/sax.jar"
export CLASSPATH=$CLASSPATH":$MOVES_HOME/libs/jaxp-api.jar"
export CLASSPATH=$CLASSPATH":$MOVES_HOME/libs/jai_core.jar"
export CLASSPATH=$CLASSPATH":$MOVES_HOME/libs/jakarta-regexp-1.3.jar"
export CLASSPATH=$CLASSPATH":$MOVES_HOME/libs/commons-lang-2.2.jar"
export CLASSPATH=$CLASSPATH":$MOVES_HOME/libs/jlfgr-1_0.jar"
export CLASSPATH=$CLASSPATH":$MOVES_HOME/libs/dom.jar"
export CLASSPATH=$CLASSPATH":$MOVES_HOME/libs/abbot/costello.jar"
export CLASSPATH=$CLASSPATH":$MOVES_HOME/libs/abbot/bsh-2.0b4.jar"
export CLASSPATH=$CLASSPATH":$MOVES_HOME/libs/abbot/jdom-1.0.jar"
export CLASSPATH=$CLASSPATH":$MOVES_HOME/libs/abbot/abbot.jar"
export CLASSPATH=$CLASSPATH":$MOVES_HOME/libs/abbot/gnu-regexp-1.1.0.jar"
export CLASSPATH=$CLASSPATH":$MOVES_HOME/libs/junit-4.5.jar"

export JAVA_HOME=/usr/lib/jvm/java-1.7.0-openjdk-1.7.0.9.x86_64
export ANT_HOME=/home/ec2-user/climateaction/EPA_MOVES_Model/ant
export PATH=$ANT_HOME/bin:$JAVA_HOME/bin:$PATH
export JAVA_TOOL_OPTIONS=-Dfile.encoding=UTF-8
