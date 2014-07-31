#!/usr/bin/env python 
# -*- coding: UTF-8 -*-

'''
- Following:
  * https://gist.github.com/rduplain/1641344 <-- Flask + Matplotlib
  * http://en.m.wikipedia.org/wiki/Matplotlib <-- more Matplotlib

- Dependencies:
$ sudo yum install libpng-devel.x86_64
$ sudo pip2.7 install flask matplotlib requests

for pygal install lxml
        - Site: http://lxml.de/tutorial.html
        - Installation:

(py27env)[cloudera@localhost bicing-bcn]$ sudo yum install python-lxml.x86_64
<-- that installs it for python2.6 omming with the system

For python 2.7 installed from sources in CentOS 6 (http://stackoverflow.com/questions/5178416/pip-install-lxml-error), we first need to manually install some dependencies:

(py27env)[cloudera@localhost bicing-bcn]$ sudo yum install libxslt-python.x86_64 libxslt-devel.x86_64 libxslt-devel.i686 libxml2-devel.x86_64 libxml2-devel.i686 libxml2-python.x86_64

    (py27env)[cloudera@localhost bicing-bcn]$ sudo pip2.7 install lxml

and then 

$ sudo pip2.7 install pygal

- Start HBase REST server:

$ hbase rest start -ro -p 9998

$  curl -H "Accept: application/json" http://localhost:9998/test_hbase_py_client/john
{"Row":[{"key":"am9obg==","Cell":[{"column":"aW5mbzphZ2U=","timestamp":1393791170961,"$":"NDI="},{"column":"dmlzaXRzOmFtYXpvbi5jb20=","timestamp":1393791171026,"$":"NQ=="},{"column":"dmlzaXRzOmdvb2dsZS5lcw==","timestamp":1393791171063,"$":"Mg=="}]}]}(py27env)[cloudera@localhost hbase_rest_charts]$ 

hbase(main):002:0> scan 'test_hbase_py_client'
ROW                                              COLUMN+CELL                                                                                                                                
 john                                            column=info:age, timestamp=1393791170961, value=42                                                                                         
 john                                            column=visits:amazon.com, timestamp=1393791171026, value=5                                                                                 
 john                                            column=visits:google.es, timestamp=1393791171063, value=2                                                                                  
 mary                                            column=info:age, timestamp=1393791170995, value=26                                                                                         
 mary                                            column=visits:amazon.com, timestamp=1393791171079, value=4                                                                                 
 mary                                            column=visits:facebook.com, timestamp=1393791171098, value=2   

TODO: this is requesting some online CSS, in offline mode animation is lost and the serve
time is slower, probably due to that. Consider downloading the CSS and adding it to the 
application if possible. Maybe this is due to using DarkSolarizedStyle, consider dropping
the style if this implies this kind of dependencies
'''
from flask import Flask, make_response
from flask import render_template

# For matplotlib
from matplotlib.backends.backend_agg import FigureCanvasAgg as FigureCanvas
from matplotlib.figure import Figure
import StringIO

import pygal
from pygal.style import DarkSolarizedStyle

import requests
from base64 import b64decode

from collections import deque

_hbase_base_url='http://localhost:9998/'
values = [1,0,2, 5, 7, 2]

app = Flask(__name__)
if not hasattr(app, 'extensions'):
    app.extensions = {}
app.extensions['values'] = deque(values)

@app.route('/barchart.png')
def plotOne():
    fig = Figure()
    axis = fig.add_subplot(1, 1, 1)
    axis.bar(range(len(values)), values)

    canvas = FigureCanvas(fig)
    output = StringIO.StringIO()
    canvas.print_png(output)
    response = make_response(output.getvalue())
    response.mimetype = 'image/png'
    return response

@app.route('/barchart.svg')
def graph_something():
     bar_chart = pygal.Bar(style=DarkSolarizedStyle)
     # bar_chart.add('Fibonacci', [0, 1, 1, 2, 3, 5, 8, 13, 21, 34, 55])
     bar_chart.add('Values', app.extensions['values'])
     app.extensions['values'].rotate(1)
     return bar_chart.render_response()

@app.route('/barchart.html')
def barchart(refresh_rate=10000):
    ''' 
    By default jinja2 will look for templates at the templates folder 
    in the root of the application.
    By using the template be get autorefresh by using a <meta> header
    '''
    return render_template('chart.html', refresh_rate=refresh_rate, 
                            title="chart", chart_src="/barchart.svg")

'''
>>> headers = {'accept': 'application/json'}
>>> r = requests.get("http://localhost:9998/test_hbase_py_client/john", headers=headers)
>>> r.json()
{u'Row': [{u'Cell': [{u'column': u'aW5mbzphZ2U=', u'timestamp': 1393791170961, u'$': u'NDI='}, {u'column': u'dmlzaXRzOmFtYXpvbi5jb20=', u'timestamp': 1393791171026, u'$': u'NQ=='}, {u'column': u'dmlzaXRzOmdvb2dsZS5lcw==', u'timestamp': 1393791171063, u'$': u'Mg=='}], u'key': u'am9obg=='}]}
>>> r.json()['Row']
[{u'Cell': [{u'column': u'aW5mbzphZ2U=', u'timestamp': 1393791170961, u'$': u'NDI='}, {u'column': u'dmlzaXRzOmFtYXpvbi5jb20=', u'timestamp': 1393791171026, u'$': u'NQ=='}, {u'column': u'dmlzaXRzOmdvb2dsZS5lcw==', u'timestamp': 1393791171063, u'$': u'Mg=='}], u'key': u'am9obg=='}]

>>> from base64 import b64decode
>>> b64decode(r.json()['Row'][0]['key'])
'john'
>>> [(b64decode(col['column']), b64decode(col['$']), long(col['timestamp'])) for col in r.json()['Row'][0]['Cell'] ]
[('info:age', '42', 1393791170961L), ('visits:amazon.com', '5', 1393791171026L), ('visits:google.es', '2', 1393791171063L)]
'''

_get_hBase_row_format = 'http://{server}/{table}/{row_key}/{family}'
_get_hBase_row_headers = {'accept': 'application/json'}
def get_hBase_row(server, table, row_key, family=''):
    '''
    :param server: e.g. 'localhost:9998'
    :return None if there was some error with the request, otherwise
        returns a dictionary like

        {'key': 'john', 'row': [{'qual': 'age', 'value': '42', 'family': 'info', 'timestamp': 1393791170961L}, {'qual': 'amazon.com', 'value': '5', 'family': 'visits', 'timestamp': 1393791171026L}, {'qual': 'google.es', 'value': '2', 'family': 'visits', 'timestamp': 1393791171063L}]}

        where decoding from base64 was already performed

    Example:
       get_hBase_row("localhost:9998", "test_hbase_py_client", "john")
    '''
    # TODO: try - except for the request and extra argument for the timeout of the request
    try:
        r = requests.get(_get_hBase_row_format.format(server=server, table=table, row_key=row_key, family=family), 
                         headers=_get_hBase_row_headers)
    except:
        None
    key = b64decode(r.json()['Row'][0]['key'])
    row = [{'family' : column[:sep_idx], 'qual' : column[sep_idx + 1:], 
      'value' :  b64decode(cell['$']), 'timestamp' : long(cell['timestamp']) }   
           for cell in r.json()['Row'][0]['Cell'] 
           for column in (b64decode(cell['column']), ) 
           for sep_idx in (column.find(':'), ) ]
    return {'key' : key, 'row' : row}

# http://localhost:9999/hbase/svg/barchart/localhost:9998/test_hbase_py_client/john/visits
@app.route('/hbase/svg/barchart/<server>/<table>/<row_key>/<family>')
def svg_barchart_for_hbase_row(server, table, row_key, family):
    '''
    A chart will be build from the values of the cells in that
    row key and column family

    NOTE: assuming all the values are of type float

    TODO: consider PROBLEM of versions
    '''
    # get values from HBase: don't forget conversion to number
    row = get_hBase_row(server, table, row_key, family)
    values = [float(cell['value']) for cell in row['row']]

    # build an SVG chart
    bar_chart = pygal.Bar(style=DarkSolarizedStyle)
    bar_chart.add('Values', values)
    return bar_chart.render_response()

_svg_barchart_for_hbase_row_url_format = '/hbase/svg/barchart/{server}/{table}/{row_key}/{family}'
@app.route('/hbase/charts/barchart/<server>/<table>/<row_key>/<family>/', defaults={'refresh' : 5})
@app.route('/hbase/charts/barchart/<server>/<table>/<row_key>/<family>/<int:refresh>')
def barchart_for_hbase_row(server, table, row_key, family, refresh):
    ''' 
    By default jinja2 will look for templates at the templates folder 
    in the root of the application.
    By using the template be get autorefresh by using a <meta> header
    '''
    return render_template('chart.html', refresh_rate=refresh, title="HBase Barchart",
                            chart_src=_svg_barchart_for_hbase_row_url_format.format(server=server, table=table, row_key=row_key, family=family))

'''
http://stackoverflow.com/questions/18602276/flask-urls-w-variable-parameters

TODO: s2 is the proposed format for a composed chart, using bar for barchart, pie for piechart, etc, 
covering all the chart types in pygal

>>> s
'/hbase/charts/barchart/<server>/<table>/<row_key>/<family>/<int:refresh>'
>>> s2 = '/hbase/charts/<server>/<table>/bar/cols/2/<row_key>/<family>/pie/<row_key>/<family>/refresh/5'

'''

if __name__ == '__main__':
    import sys
    print 'Usage: <port>'
    port = int(sys.argv[1])
    print 'Go to http://localhost:9999/hbase/svg/barchart/localhost:9998/test_hbase_py_client/john/visits'
    print 'Go to http://localhost:9999/hbase/charts/barchart/localhost:9998/test_hbase_py_client/john/visits'
    # print 'Go to http://127.0.0.1:{port}/barchart.html, http://127.0.0.1:{port}/barchart.png, http://127.0.0.1:{port}/barchart.svg'.format(port=port)
    app.run(debug=True, port=port)