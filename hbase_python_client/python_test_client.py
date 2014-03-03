#!/usr/local/bin/python2.7

'''
Proof of concept for a Python client for HBase based on JPype
Tested for:
    - HBase Version 0.94.6-cdh4.4.0, rUnknown, Tue Sep  3 20:09:51 PDT 2013
    - python2.6 and python2.7, JPype installed with $ sudo pip install JPype1
'''
import jpype
import glob

def iterate_iterable(iterable):
    '''
    Given iterable implementing java.lang.Iterable, return a generator for its values

    TODO: add to module for Java adapter
    '''        
    iterator = iterable.iterator()
    while iterator.hasNext():
        yield iterator.next()

_jvm_lib_path = "/usr/java/jdk1.6.0_32/jre/lib/amd64/server/libjvm.so"
cp_dirs = '/usr/lib/hadoop/client-0.20:/usr/lib/hadoop/lib:/usr/lib/hadoop:/usr/lib/hadoop/client:/usr/lib/hbase/lib/:/usr/lib/hbase/'
cp_jars_str = ":".join(set(jar for cp_dir in cp_dirs.split(':') for jar in glob.iglob(cp_dir + "/*.jar")))

test_table_name = 'test_hbase_py_client'

jpype.startJVM(_jvm_lib_path, "-ea","-Djava.class.path=" + cp_jars_str)
try:
    HTablePoolClass = jpype.JClass("org.apache.hadoop.hbase.client.HTablePool")
    connection_pool = HTablePoolClass()
    test_table = connection_pool.getTable(test_table_name)
    BytesClass = jpype.JClass("org.apache.hadoop.hbase.util.Bytes")
    ScanClass = jpype.JClass("org.apache.hadoop.hbase.client.Scan")
    scan_all = ScanClass()
        # class ResultScanner
    result_scanner = test_table.getScanner(scan_all)
    # for result in result_scanner: TypeError: 'org.apache.hadoop.hbase.client.ClientScanner' object is not iterable
    print '\n'*2, '-'*30
    print 'Scanning table "{table_name}"'.format(table_name=test_table_name) 
    for result in iterate_iterable(result_scanner):
        print "row id:", result.getRow()
        for key_val in iterate_iterable(result.list()):
            print "\t", "family : {family}, qual : {qual}, value : {value}".format(family = key_val.getFamily(), qual = key_val.getQualifier(), value = BytesClass.toString(key_val.getValue()).encode('ascii', 'ignore'))
    print '-'*30, '\n'*2
    test_table.close()
except jpype.JavaException as ex:
    print 'exception', ex.javaClass(), ex.message()
    print 'stacktrace:', ex.stacktrace()

jpype.shutdownJVM()