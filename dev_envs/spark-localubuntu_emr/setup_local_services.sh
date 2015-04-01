#!/bin/bash

EXIT_CODE=0
ZK_START_SCRIPT="/opt/zookeeper/zookeeper*/bin/zkServer.sh"
KAFKA_ROOT="/opt/kafka/kafka*"

function start {
    echo "Starting Zookeeper"
    $ZK_START_SCRIPT start
    echo "Starting Kafka"
    $KAFKA_ROOT/bin/kafka-server-start.sh $KAFKA_ROOT/config/server.properties &> /dev/null &
}

function set_kafka_broker_pid {
    unset kafka_broker_pid
    kafka_broker_pid=$(ps ax | grep -i 'kafka\.Kafka' | grep java | grep -v grep | awk '{print $1}')
}

function stop {
    echo "Stopping Kafka"
    $KAFKA_ROOT/bin/kafka-server-stop.sh
    sleep 1
    set_kafka_broker_pid
    if [ ! -z $kafka_broker_pid ]
    then 
        echo "Kafka is still running, trying with TERM"
        kill -TERM $kafka_broker_pid
    fi 
    sleep 1
    set_kafka_broker_pid
    if [ ! -z $kafka_broker_pid ]
    then 
        echo "Kafka is still running, trying with KILL"
        kill -KILL $kafka_broker_pid
    fi 

    echo "Stopping Zookeeper"
    $ZK_START_SCRIPT stop
}

function print_usage {
    # echo $"Usage: $0 {start|stop|restart|status}"
    echo $"Usage: $0 {start|stop|restart}"
}

################
# Main 
################
if [ $# -ne 1 ]
then 
    print_usage
    exit 1
fi 

case "$1" in
  start)
    start
    ;;
  stop)
    stop
    ;;
  # TODO
  # status)
  #   get_status
  #   ;;
  restart)
    stop
    start
    ;;
  *)
    print_usage
    EXIT_CODE=2
esac

exit $EXIT_CODE