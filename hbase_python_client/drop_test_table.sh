#!/bin/bash

TABLE_NAME='test_hbase_py_client'
hbase shell <<END
disable '${TABLE_NAME}'
drop '${TABLE_NAME}'
list
exit
END