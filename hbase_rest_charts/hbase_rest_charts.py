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
from werkzeug.routing import BaseConverter, ValidationError

import pygal
from pygal.style import DarkSolarizedStyle

import requests
from base64 import b64decode
from operator import itemgetter

app = Flask(__name__)

'''
Dictionary from chart type as string to a format string for the URL to get the chart as svg.
Each function registers itself in this dictionary
'''
_supported_chart_types = {}

_get_hBase_row_format = 'http://{server}/{table}/{row_key}/{family}'
_get_hBase_row_headers = {'accept': 'application/json'}
def get_hBase_row(server, table, row_key, family=''):
    '''
    Queries HBase for a the last version of the cells in a table and row key, and optionally for a 
    particular column family. 
    Values are decoded from base64

    :param server: e.g. 'localhost:9998'
    :param table name of the HBase table
    :param row_key key of the HBase row to obtain
    :param family if this value is present only the cells in that family are obtained 

    :return None if there was some error with the request, otherwise
        returns a dictionary like

        {'key': 'john', 'row': [{'qual': 'age', 'value': '42', 'family': 'info', 'timestamp': 1393791170961L}, {'qual': 'amazon.com', 'value': '5', 'family': 'visits', 'timestamp': 1393791171026L}, {'qual': 'google.es', 'value': '2', 'family': 'visits', 'timestamp': 1393791171063L}]}

        where decoding from base64 was already performed

    Examples:
    >>> get_hBase_row("localhost:9998", "test_hbase_py_client", "john")
    {'key': 'john', 'row': [{'qual': 'age', 'value': '42', 'family': 'info', 'timestamp': 1393791170961L}, {'qual': 'amazon.com', 'value': '5', 'family': 'visits', 'timestamp': 1393791171026L}, {'qual': 'google.es', 'value': '2', 'family': 'visits', 'timestamp': 1393791171063L}]}
    >>> get_hBase_row("localhost:9998", "test_hbase_py_client", "john", "visits")
    {'key': 'john', 'row': [{'qual': 'amazon.com', 'value': '5', 'family': 'visits', 'timestamp': 1393791171026L}, {'qual': 'google.es', 'value': '2', 'family': 'visits', 'timestamp': 1393791171063L}]}
    '''
    # TODO: try - except for the request and extra argument for the timeout of the request
    try: 
        hbase_request = requests.get(_get_hBase_row_format.format(server=server, table=table, row_key=row_key, family=family), 
                                                                  headers=_get_hBase_row_headers)
    except:
        return None
    key = b64decode(hbase_request.json()['Row'][0]['key'])
    row = [{'family' : column[:sep_idx], 'qual' : column[sep_idx + 1:], 
      'value' :  b64decode(cell['$']), 'timestamp' : long(cell['timestamp']) }   
           for cell in hbase_request.json()['Row'][0]['Cell'] 
           for column in (b64decode(cell['column']), ) 
           for sep_idx in (column.find(':'), ) ]
    return {'key' : key, 'row' : row}

_supported_chart_types['bar'] = '/hbase/svg/bar/{server}/{table}/{title}/{family}/{row_keys}'
@app.route('/hbase/svg/bar/<server>/<table>/<title>/<family>/<path:row_keys>')
def svg_barchart_for_hbase_rows(server, table, title, family, row_keys):
    '''
    A chart will be build from the values of the cells in that column family
     * The x-axis labels are the column qualifiers found in all cells for the keys
     * For each key there is a bar group with a bar in each point of the x-axis. None 
     is used to fill missing values for a qual, with the usual meaning in pygal (no 
     value will be shown for that bar group at the point)

    NOTE: assuming all the values are of type float
    NOTE: taking last version of each cell
    '''
    # Example URL: http://localhost:9999/hbase/svg/bar/localhost:9998/test_hbase_py_client/Sites%20Visited/visits/john/mary
    # get values from HBase: don't forget conversion to number  
    #  {row : { qual : value) } }
    rows = { row['key'] : { cell['qual'] : float(cell['value']) for cell in row['row'] } 
                for row_key in row_keys.split('/') 
                for row in (get_hBase_row(server, table, row_key, family),)
            }
    # get sorted values for x axis
    x_labels = sorted({ qual for qual_vals in rows.values() for qual in qual_vals.keys() })
    # build an SVG chart
    # TODO: consider specifying styles (e.g. chart = pygal.Bar(style=DarkSolarizedStyle))
    chart = pygal.Bar()
    chart.title = title
    chart.x_labels = x_labels
        # add the values for each key
    for key, qual_vals in rows.iteritems():
        # use get to fill spaces with None
        chart.add(key, [ qual_vals.get(label) for label in x_labels ])
    # return as a Flask response
    return chart.render_response()

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

s_ex = 'http://localhost:9999/hbase/charts/barchart/localhost:9998/test_hbase_py_client/john/visits/'
>>> s2 = '/hbase/charts/<server>/<table>/bar/cols/2/<row_key>/<family>/pie/<row_key>/<family>/refresh/5'
s2b = '/hbase/charts/<server>/<table>/cols/2/bar/<row_key>/<family>/pie/<row_key>/<family>/refresh/<int:refresh>'

s2_ex = '/hbase/charts/localhost:9998/test_hbase_py_client/cols/2/bar/john/visits/pie/mary/visits/refresh/10'

>>> re.match('.*(?=refresh)', "bar/john/visits/pie/mary/visits/refresh/10").group(0)
'bar/john/visits/pie/mary/visits/'
>>> re.match('.*(?=/refresh)', "bar/john/visits/pie/mary/visits/refresh/10").group(0)
'bar/john/visits/pie/mary/visits' <-- suena mejor
'''

# _supported_chart_types['bar'] = '/hbase/svg/bar/{server}/{table}/{title}/{family}/{row_keys}'


class ChartsSpecsConverter(BaseConverter):
    def __init__(self, url_map):
        super(ChartsSpecConverter, self).__init__(url_map)
        self.regex = '(?:.*)'

    def to_python(self, value):
        '''
        For tuples (chart_type, chart_title, family, row_key) 
        e.g. value is 'bar/Sites%20Visited/visits/john/bar/Sites%20Visited/visits/mary'

        Apply validation rules here, e.g. 
          - valid chart types: see variable chart_types
          - each chart spec must be a 4 elements

        Return a list of spec tuples
        '''
        split_value = value.split('/')
        n_splits = len(split_value)
        tuple_size = 4
        if (n_splits % tuple_size) != 0:
            raise ValidationError("Chart specs must be 4 elements tuples of the shape (chart_type, chart_title, family, row_key)")
        specs = [ split_value[spec_idx * tuple_size: (spec_idx + 1) * tuple_size ] 
                    for spec_idx in xrange(0, n_splits / tuple_size) ]
        for spec in specs:
            chart_type = spec[0]
            if chart_type not in _supported_chart_types.keys():
                 raise ValidationError("Unknow chart type {chart_type} for chart specification {chart_spec}".format(chart_type=chart_type, chart_spec=spec))
        return specs

    def to_url(self, specs):
        # Don't forget to eliminate trailing '/'
        return '/'.join(('/'.join(spec) for spec in specs))[:-1]
# Register the converter
app.url_map.converters['charts_specs'] = ChartsSpecsConverter

_charts_table_template='charts_table.html'
@app.route('/hbase/charts/<server>/<table>/cols/<int:num_cols>/refresh/<int:refresh>/<charts_specs:charts>')
def charts_table(server, table, num_cols, refresh, charts_specs, row_keys):
    ''' 
    By default jinja2 will look for templates at the templates folder 
    in the root of the application.

    By using the template be get autorefresh using a <meta> header

    TODO: configurable table_width

    TODO: consider other routing '/hbase/charts2/<server>/<table>/cols/<int:num_cols>/refresh/<int:refresh>/<charts_spec_2:charts>/keys/<path:row_keys>'
        with charts_spec_b for triples (char_type, chart_title,  family) using the same row keys for all the charts, with a BaseConverter using
        self.regex = '(?:.*(?=/keys))'
    '''
    # TODO
    return render_template(_charts_table_template, table_width=1200, refresh_rate=refresh, title="HBase Barchart",
                            chart_src=_svg_barchart_for_hbase_row_url_format.format(server=server, table=table, row_key=row_key, family=family))

if __name__ == '__main__':
    import sys
    print 'Usage: <port>'
    port = int(sys.argv[1])
    print 'Go to http://localhost:9999/hbase/svg/bar/localhost:9998/test_hbase_py_client/Sites%20Visited/visits/john/mary'
    print 'Go to http://localhost:9999/hbase/charts/barchart/localhost:9998/test_hbase_py_client/john/visits'
    # print 'Go to http://127.0.0.1:{port}/barchart.html, http://127.0.0.1:{port}/barchart.png, http://127.0.0.1:{port}/barchart.svg'.format(port=port)
    app.run(debug=True, port=port)