#!/bin/bash
echo "Script executed from: ${PWD}"
BASEDIR=$(dirname $0)
echo "Script location: ${BASEDIR}"
cd ${BASEDIR}
echo "Current dir ${PWD}"
LATEST_JAR=$(ls -t datasetutils-*.jar | head -1)
echo ${LATEST_JAR}

java -cp .:${LATEST_JAR} com.sforce.dataset.util.UpgradeChecker


LATEST_JAR=$(ls -t datasetutils-*.jar | head -1)
echo ${LATEST_JAR}

java -Xmx1G -jar ${LATEST_JAR} --server true

