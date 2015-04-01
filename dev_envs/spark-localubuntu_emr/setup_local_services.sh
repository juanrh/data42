#!/bin/bash

EXIT_CODE=0
ZK_START_SCRIPT="/opt/zookeeper/zookeeper*/bin/zkServer.sh"

function start {
    echo "Starting zookeeper"
    $ZK_START_SCRIPT start
}

function stop {
    echo "Stopping zookeeper"
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