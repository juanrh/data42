#!/bin/bash

TABLE_NAME='test_hbase_py_client'

hbase shell <<END
create '${TABLE_NAME}', 'info', 'visits'
put '${TABLE_NAME}', 'john', 'info:age', 42
put '${TABLE_NAME}', 'mary', 'info:age', 26
put '${TABLE_NAME}', 'john', 'visits:amazon.com', 5
put '${TABLE_NAME}', 'john', 'visits:google.es', 2
put '${TABLE_NAME}', 'mary', 'visits:amazon.com', 4
put '${TABLE_NAME}', 'mary', 'visits:facebook.com', 2
list
scan '${TABLE_NAME}'
exit
END