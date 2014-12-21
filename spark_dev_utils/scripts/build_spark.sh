#!/bin/bash

# See http://spark.apache.org/docs/latest/building-spark.html

# "Install" with 
# echo 'export PATH=${PATH}:'"${HOME}/git/data42/spark_dev_utils/scripts/" >>  ${HOME}/.bashrc
# source ${HOME}/.bashrc

#################
# Configuration
#################
SPARK_SRC_HOME=${HOME}/git/spark
HADOOP_OPTS='-Phadoop-2.4 -Dhadoop.version=2.4.0'
    # for Hive support
HIVE_OPTS='-Phive -Phive-thriftserver'
YARN_OPTS='-Pyarn'
# SCALA_OPTS='-Dscala-2.11' # Scala 2.10
SCALA_OPTS='' # Scala 2.10

#################
# Main
#################
pushd ${SPARK_SRC_HOME}
export MAVEN_OPTS="-Xmx2g -XX:MaxPermSize=512M -XX:ReservedCodeCacheSize=512m"

GOAL='build'
if [ $# -ge 1 ]
then
    GOAL=$1
fi

case $GOAL in 
    'build')
        echo 'Building Spark ... '
        echo 
        mvn ${YARN_OPTS} ${HADOOP_OPTS} ${SCALA_OPTS} ${HIVE_OPTS} -DskipTests clean package
        ;;

    'tests')
        echo 'Executing tests for Spark ... '
        echo 
        mvn ${YARN_OPTS} ${HADOOP_OPTS} ${SCALA_OPTS} ${HIVE_OPTS} test
        ;;

    *)
        echo 'Usage $0: [build | tests]'
        popd
        exit 1
        ;;
esac

popd
