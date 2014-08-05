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

'''
Example URLs supported by HBase

    curl -H "Accept: application/json" http://localhost:9998/test_hbase_py_client/john
    curl -H "Accept: application/json" http://localhost:9998/test_hbase_py_client/john/visits
    curl -H "Accept: application/json" http://localhost:9998/test_hbase_py_client/*
    curl -H "Accept: application/json" http://localhost:9998/test_hbase_py_client/*/visits    
'''
_get_hBase_rows_format = 'http://{server}/{table}/{row_keys}/{family}'
_get_hBase_rows_headers = {'accept': 'application/json'}
def get_hBase_rows(server, table, row_keys, family=''):
    '''
    Queries HBase for a the last version of the cells in a table and row key, and optionally for a 
    particular column family. 
    Values are decoded from base64

    :param server: e.g. 'localhost:9998'
    :param table name of the HBase table
    :param row_keys key of the HBase row to obtain. Suffix globbing is supported as described 
      in http://wiki.apache.org/hadoop/Hbase/Stargate
    :param family if this value is present only the cells in that family are obtained 

    :return None if there was some error with the request, otherwise
        returns a list of dictionaries like

        {'key': 'john', 'row': [{'qual': 'age', 'value': '42', 'family': 'info', 'timestamp': 1393791170961L}, {'qual': 'amazon.com', 'value': '5', 'family': 'visits', 'timestamp': 1393791171026L}, {'qual': 'google.es', 'value': '2', 'family': 'visits', 'timestamp': 1393791171063L}]}

        for each of the keys specified, where decoding from base64 was already performed

    Examples:
    >>> get_hBase_rows("localhost:9998", "test_hbase_py_client", "john")
    [{'key': 'john', 'row': [{'qual': 'age', 'value': '42', 'family': 'info', 'timestamp': 1393791170961L}, {'qual': 'amazon.com', 'value': '5', 'family': 'visits', 'timestamp': 1393791171026L}, {'qual': 'google.es', 'value': '2', 'family': 'visits', 'timestamp': 1393791171063L}]}]
    >>> get_hBase_rows("localhost:9998", "test_hbase_py_client", "john", "visits")
    [{'key': 'john', 'row': [{'qual': 'amazon.com', 'value': '5', 'family': 'visits', 'timestamp': 1393791171026L}, {'qual': 'google.es', 'value': '2', 'family': 'visits', 'timestamp': 1393791171063L}]}]
    '''
    # TODO: try - except for the request and extra argument for the timeout of the request
    try: 
        hbase_request = requests.get(_get_hBase_rows_format.format(server=server, table=table, row_keys=row_keys, family=family), 
                                                                  headers=_get_hBase_rows_headers)
    except:
        return None
    return [{'key' : b64decode(row['key']), 
             'row' : [{'family' : column[:sep_idx], 'qual' : column[sep_idx + 1:], 
                       'value' :  b64decode(cell['$']), 'timestamp' : long(cell['timestamp']) }   
                          for cell in row['Cell'] 
                          for column in (b64decode(cell['column']), ) 
                          for sep_idx in (column.find(':'), ) ]} 
                for row in hbase_request.json()['Row']]
    # FIXME delete
    # key = b64decode(hbase_request.json()['Row'][0]['key'])
    # row = [{'family' : column[:sep_idx], 'qual' : column[sep_idx + 1:], 
    #   'value' :  b64decode(cell['$']), 'timestamp' : long(cell['timestamp']) }   
    #        for cell in hbase_request.json()['Row'][0]['Cell'] 
    #        for column in (b64decode(cell['column']), ) 
    #        for sep_idx in (column.find(':'), ) ]
    # return {'key' : key, 'row' : row}

'''
Wildcards are supported when specifying the row key, even when a family is latter specified

[cloudera@localhost local]$ curl -H "Accept: application/json" http://localhost:9998/test_hbase_py_client/*/visits
{"Row":[{"key":"am9obg==","Cell":[{"column":"dmlzaXRzOmFtYXpvbi5jb20=","timestamp":1393791171026,"$":"NQ=="},{"column":"dmlzaXRzOmdvb2dsZS5lcw==","timestamp":1393791171063,"$":"Mg=="}]},{"key":"bWFyeQ==","Cell":[{"column":"dmlzaXRzOmFtYXpvbi5jb20=","timestamp":1393791171079,"$":"NA=="},{"column":"dmlzaXRzOmZhY2Vib29rLmNvbQ==","timestamp":1393791171098,"$":"Mg=="}]}]}[cloudera@localhost local]$ 

[cloudera@localhost local]$ curl -H "Accept: application/json" http://localhost:9998/test_hbase_py_client/*
{"Row":[{"key":"am9obg==","Cell":[{"column":"aW5mbzphZ2U=","timestamp":1393791170961,"$":"NDI="},{"column":"dmlzaXRzOmFtYXpvbi5jb20=","timestamp":1393791171026,"$":"NQ=="},{"column":"dmlzaXRzOmdvb2dsZS5lcw==","timestamp":1393791171063,"$":"Mg=="}]},{"key":"bWFyeQ==","Cell":[{"column":"aW5mbzphZ2U=","timestamp":1393791170995,"$":"MjY="},{"column":"dmlzaXRzOmFtYXpvbi5jb20=","timestamp":1393791171079,"$":"NA=="},{"column":"dmlzaXRzOmZhY2Vib29rLmNvbQ==","timestamp":1393791171098,"$":"Mg=="}]}]}[cloudera@localhost local]$ 

'''


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

    :param row_keys key of the HBase row to obtain, as a string of row keys separated
       by '/', suffix globbing as described in http://wiki.apache.org/hadoop/Hbase/Stargate
       is supported for each component of the string as obtained by row_keys.split('/') 
    '''
    # Example URL: http://localhost:9999/hbase/svg/bar/localhost:9998/test_hbase_py_client/Sites%20Visited/visits/john/mary
    # get values from HBase: don't forget conversion to number  
    #  {row : { qual : value) } }
    rows = { row['key'] : { cell['qual'] : float(cell['value']) for cell in row['row'] } 
                for row_key in row_keys.split('/') 
                for row in get_hBase_rows(server, table, row_key, family)
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

class ChartsSpecsConverter(BaseConverter):
    def __init__(self, url_map):
        super(ChartsSpecsConverter, self).__init__(url_map)
        self.regex =  '(?:.*(?=/keys))'
        # self.regex =  '(?:.*(?=keys))'

    def to_python(self, value):
        '''
        For tuples (chart_type, chart_title, family) 
        e.g. value is 'bar/Sites%20Visited%20Bar/visits/pie/Sites%20Visited%20Pie/visits'

        Apply validation rules here, e.g. 
          - valid chart types: see variable chart_types
          - each chart spec must be a 3 elements
          - same chart is not specified twice

        Return a list of dictionaries corresponding to spec tuples, 
         with keys 'chart_type', 'title', 'family'
        '''
        split_value = value.split('/')
        n_splits = len(split_value)
        tuple_size = 3
        if (n_splits % tuple_size) != 0:
            raise ValidationError("Chart specs must be 3 elements tuples of the shape (chart_type, chart_title, family)")
        specs = [ split_value[spec_idx * tuple_size : (spec_idx + 1) * tuple_size ] 
                    for spec_idx in xrange(0, n_splits / tuple_size) ]
        titles = {}
        for spec in specs:
            chart_type, title = spec[0], spec[1]
            if chart_type not in _supported_chart_types.keys():
                raise ValidationError("Unknow chart type {chart_type} for chart specification {chart_spec}".format(chart_type=chart_type, chart_spec=spec))
            if title in titles:
                raise ValidationError("Title {title} appears twice in chart specification {chart_spec}".format(title=title, chart_spec=spec))

        return [{'chart_type' : spec[0], 'title' : spec[1], 'family' : spec[2]} for spec in specs ]

    def to_url(self, specs):
        # Don't forget to eliminate trailing '/'
        return '/'.join(('/'.join(spec) for spec in specs))[:-1]
# Register the converter
app.url_map.converters['charts_specs'] = ChartsSpecsConverter

_charts_table_template='charts_table.html'
@app.route('/hbase/charts/<server>/<table>/width/<int:table_width>/cols/<int:num_cols>/refresh/<int:refresh>/<charts_specs:charts>/keys/<path:row_keys>')
def charts_table(server, table, table_width, num_cols, refresh, charts, row_keys):
    ''' 
    By default jinja2 will look for templates at the templates folder 
    in the root of the application.

    By using the template be get autorefresh using a <meta> header

    TODO: configurable title for the whole chart

    TODO: consider other routing '/hbase/charts2/<server>/<table>/cols/<int:num_cols>/refresh/<int:refresh>/<charts_spec_2:charts>'
        with charts_spec_b for tuples (char_type, chart_title, family, row_key) with different rows for the charts and grouping the
        tuples by chart_title. Note that implies richer checks of the URL, as for example all the tuples with the same chart title
        should have the same type => that suggests that maybe a better URL schema would first declare types for the chart titles 
        and then entries as pairs (family, row_key)

    Example URLs: 
     http://localhost:9999/hbase/charts/localhost:9998/test_hbase_py_client/width/1500/cols/2/refresh/500/bar/Sites%20Visited/visits/bar/Info/info/keys/*
     http://localhost:9999/hbase/charts/localhost:9998/test_hbase_py_client/width/1500/cols/2/refresh/500/bar/Sites%20Visited/visits/bar/Info/info/keys/mary/john
     http://localhost:9999/hbase/charts/localhost:9998/test_hbase_py_client/width/850/cols/1/refresh/5/bar/Sites%20Visited/visits/bar/Info/info/keys/*
    '''
    spec_rows = [charts[i*num_cols : (i+1)*num_cols] for i in xrange(0, len(charts)/num_cols +1)]
      # drop last row in case it's empty (when len(charts) % num_cols) == 0)
    spec_rows = spec_rows if spec_rows[-1] != [] else spec_rows[:-1]
    def update_chart_dict(spec):
        spec.update({'server' : server, 'table' : table, 'row_keys' : row_keys})
        return spec
    chart_src_rows = [ [_supported_chart_types[spec['chart_type']].format(**update_chart_dict(spec)) for spec in row] for row in spec_rows ]
    return render_template(_charts_table_template, table_width=table_width, refresh_rate=refresh, 
                           title="HBase Chart", chart_src_rows=chart_src_rows)

# _supported_chart_types['bar'] = '/hbase/svg/bar/{server}/{table}/{title}/{family}/{row_keys}'
# return [{'chart_type' : spec[0], 'title' : spec[1], 'family' : spec[2], 'row_key' : spec[3]} for spec in specs ]

'''
Real TODOs: 
    - TODO: THIS IS THE MOST USEFUL, add an additional routing where row_keys is not specified that performs a full scan of the
    table, this is obviously only useful when the table contains already aggregated data

    - TODO: center charts in its table cell
'''


if __name__ == '__main__':
    import sys
    print 'Usage: <port>'
    port = int(sys.argv[1])
    # FIXME delete
    print 'Sample URLs:'
    print 'http://localhost:9999/hbase/svg/bar/localhost:9998/test_hbase_py_client/Sites%20Visited/visits/john/mary'
    print 'http://localhost:9999/hbase/svg/bar/localhost:9998/test_hbase_py_client/Info/info/*'
    print 'http://localhost:9999/hbase/charts/localhost:9998/test_hbase_py_client/width/1500/cols/2/refresh/500/bar/Sites%20Visited/visits/bar/Info/info/keys/mary/john'
    print 'http://localhost:9999/hbase/charts/localhost:9998/test_hbase_py_client/width/1500/cols/2/refresh/500/bar/Sites%20Visited/visits/bar/Info/info/keys/*'
    print 'http://localhost:9999/hbase/charts/localhost:9998/test_hbase_py_client/width/850/cols/1/refresh/5/bar/Sites%20Visited/visits/bar/Info/info/keys/*'

    # print 'Go to http://127.0.0.1:{port}/barchart.html, http://127.0.0.1:{port}/barchart.png, http://127.0.0.1:{port}/barchart.svg'.format(port=port)
    app.run(debug=True, port=port)