#!/usr/bin/env python

'''
Developed and tested for Python 2.7

Prerequisites: 
    pip2.7 install happybase

Happy Based connects to HBase through HBase thrift server

    # hbase thrift start
'''
import happybase


def hbase_test(): 
    hbase_conn = happybase.Connection("localhost")
    hbase_conn.open()
    print "Tables:", hbase_conn.tables()
    table = hbase_conn.table("test_hbase_py_client")
    print dict(table.row("mary"))
    for key, data in table.scan():
        print key, data
    for key, data in table.scan(row_start = 'ma', row_stop='n'):
        print key
        for k, v in data.iteritems(): 
            print "\t", k, v
    hbase_conn.close()
    
if __name__ == '__main__':
    hbase_test()
