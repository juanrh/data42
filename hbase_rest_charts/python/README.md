# Dependencies
This program has been developed and tested with Python 2.7. First install the Python dependencies with pip: 

```bash
$ sudo pip2.7 install flask requests
```

For pygal install lxml
  - Site: http://lxml.de/tutorial.html
  - Installation:

Note `yum install python-lxml.x86_64` installs python lxml it for python2.6 omming with the system. For python 2.7 installed from sources in CentOS 6 (http://stackoverflow.com/questions/5178416/pip-install-lxml-error), we first need to manually install some dependencies:

```bash
$ sudo yum install libxslt-python.x86_64 libxslt-devel.x86_64 libxslt-devel.i686 libxml2-devel.x86_64 libxml2-devel.i686 libxml2-python.x86_64
$ sudo pip2.7 install lxml
$ sudo pip2.7 install pygal
```

# Running the program 
- Start HBase REST server, for that HBase itself must be up too. This commands starts the REST server in read-only mode, and serving at port `9998`, this port will be latter used in the URLs for the chart server. 

```bash
$ hbase rest start -ro -p 9998
```

Test that with curl
```bash
$  curl -H "Accept: application/json" http://localhost:9998/test_hbase_py_client/john
{"Row":[{"key":"am9obg==","Cell":[{"column":"aW5mbzphZ2U=","timestamp":1393791170961,"$":"NDI="},{"column":"dmlzaXRzOmFtYXpvbi5jb20=","timestamp":1393791171026,"$":"NQ=="},{"column":"dmlzaXRzOmdvb2dsZS5lcw==","timestamp":1393791171063,"$":"Mg=="}]}]}
```

Now run the chart server, here it will run at port `9999`:

```bash
(py27env)[cloudera@localhost python]$ python hbase_rest_charts.py 9999
```

Now check that everything is working ok by creating a test HBase table with 

```bash
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
```

and then opening http://localhost:9999/hbase/charts/localhost:9998/test_hbase_py_client/width/1500/cols/2/refresh/5/bar/Sites%20Visited/visits/bar/Info/info/keys/* with Firefox. Don't forget the `*` at the end.


# TODO
TODO: this is requesting some online CSS, in offline mode animation is lost and the serve
time is slower, probably due to that. Consider downloading the CSS and adding it to the 
application if possible. Maybe this is due to using DarkSolarizedStyle, consider dropping
the style if this implies this kind of dependencies