# -*- coding: utf-8 -*-

# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

'''
Implementation of Storm's multilang protocol that can be used to create Spout and Bolts in python. Communication with the parent process is performed using strings corresponding to JSON objects through stdin and stdout, and finishing messages with a "end" line. See readMsg() below for details.
    - As a consequence no log message should be written to stdout, use stderr or a proper logging mechanism instead
    - See https://github.com/nathanmarz/storm/wiki/Multilang-protocol for a description of Storm's multilang protocol. See the book "Getting Started with Storm" -> Chapter 7. Using Non-JVM Languages with Storm for another example in php

Author: Nathan Marz <nathan@nathanmarz.com>
    This is a modification of the code from https://github.com/nathanmarz/storm/blob/master/storm-core/src/multilang/py/storm.py with some additional comments by Juan Rodriguez Hortala <juan.rodriguez.hortala@gmail.com>. Only comments were added, the functionality is the same. 

'''

import sys
import os
import traceback
from collections import deque

try:
    import simplejson as json
except ImportError:
    import json

json_encode = lambda x: json.dumps(x)
json_decode = lambda x: json.loads(x)

#reads lines and reconstructs newlines appropriately
def readMsg():
    '''
    Read messages sent by parent process. According to the multilang protocol messages end with a single line containing "end" and correspond to JSON objects
    '''
    msg = ""
    while True:
        line = sys.stdin.readline()[0:-1]
        if line == "end":
            break
        msg = msg + line + "\n"
    return json_decode(msg[0:-1])

MODE = None
ANCHOR_TUPLE = None

#queue up commands we read while trying to read taskids
pending_commands = deque()

def readTaskIds():
    if pending_taskids:
        return pending_taskids.popleft()
    else:
        msg = readMsg()
        while type(msg) is not list:
            pending_commands.append(msg)
            msg = readMsg()
        return msg

#queue up taskids we read while trying to read commands/tuples
pending_taskids = deque()

def readCommand():
    if pending_commands:
        return pending_commands.popleft()
    else:
        msg = readMsg()
        while type(msg) is list:
            pending_taskids.append(msg)
            msg = readMsg()
        return msg

def readTuple():
    cmd = readCommand()
    return Tuple(cmd["id"], cmd["comp"], cmd["stream"], cmd["task"], cmd["tuple"])

def sendMsgToParent(msg):
    print json_encode(msg)
    print "end"
    sys.stdout.flush()

def sync():
    sendMsgToParent({'command':'sync'})

def sendpid(heartbeatdir):
    '''
    Multilang-protocol handshake: 
        Send the PID of this python process to the parent process, and create an empty file at the path specified by heartbeatdir.
    '''
    pid = os.getpid()
    sendMsgToParent({'pid':pid})
    open(heartbeatdir + "/" + str(pid), "w").close()

def emit(*args, **kwargs):
    '''
    Used to emulate the methods SpoutOutputCollector.emit() and OutputCollector.emit(). 
    Use in Spouts and Bolts to emit a tuple. 

    :param args: I have identified just a single argument:
        - tup: list of values corresponding to the tuple to be emitted. The types of these values should be in sync with those declared in the method declareOutputFields() of the ShellSpout or ShellBolt that is wrapping the python code that calls this function, in the Java program defining the topology
    :param kwargs: some relevant possible values:
        - For Spouts:
            * id: message id for the tuple to emit, for guarteed message processing
        - For Bolts:
            * anchors: optional, list of ids for anchor tuples to use for this emit
        - For Spouts or Bolts:
            * stream: optional, stream to use to emit this tuple. Default stream 1 is used if not declared
    '''
    __emit(*args, **kwargs)
    return readTaskIds()

def emitDirect(task, *args, **kwargs):
    '''
    Used to emulate the methods SpoutOutputCollector.emitDirect() and OutputCollector.emitDirect(). 
    Use in Spouts and Bolts to emit a tupl  directly to the task id specified in a keyword argument directTask, which is required. 
    Appart from that it's equivalent to emit() above
    '''
    kwargs["directTask"] = task
    __emit(*args, **kwargs)

def __emit(*args, **kwargs):
    '''
    Calls emitBolt or emitSpout according to the value of the global variable MODE, that is set accordingly in the Bolt and Spout classes below.
    '''
    global MODE
    if MODE == Bolt:
        emitBolt(*args, **kwargs)
    elif MODE == Spout:
        emitSpout(*args, **kwargs)

def emitBolt(tup, stream=None, anchors=[], directTask=None):
    '''
    Roughly equivalent to the OutputCollector class. 
    '''
    global ANCHOR_TUPLE
    if ANCHOR_TUPLE is not None:
        anchors = [ANCHOR_TUPLE]
    m = {"command": "emit"}
    if stream is not None:
        m["stream"] = stream
    m["anchors"] = map(lambda a: a.id, anchors)
    if directTask is not None:
        # For direct emits
        m["task"] = directTask
    m["tuple"] = tup
    sendMsgToParent(m)

def emitSpout(tup, stream=None, id=None, directTask=None):
    '''
    Roughly equivalent to the SpoutOutputCollector class. 
    '''
    m = {"command": "emit"}
    if id is not None:
        m["id"] = id
    if stream is not None:
        m["stream"] = stream
    if directTask is not None:
        # For direct emits
        m["task"] = directTask
    m["tuple"] = tup
    sendMsgToParent(m)

def ack(tup):
    '''
    Used to emulate the method OutputCollector.ack()
    '''
    sendMsgToParent({"command": "ack", "id": tup.id})

def fail(tup):
    '''
    Used to emulate the method OutputCollector.fail()
    '''
    sendMsgToParent({"command": "fail", "id": tup.id})

def reportError(msg):
    '''
    Used to emulate the methods SpoutOutputCollector.reportError() and OutputCollector.reportError()
    '''
    sendMsgToParent({"command": "error", "msg": msg})

def log(msg):
    '''
    Used to log messages in the parent Storm worker log
    '''
    sendMsgToParent({"command": "log", "msg": msg})

def initComponent():
    '''
    Multilang-protocol handshake: 
        1. read from stdin the setup info from the parent process. This a JSON object with fields 
            - "conf": Storm configuration, as a map from strings to objects http://nathanmarz.github.io/storm/doc/backtype/storm/Config.html
            - "context": JSON representation of the TopologyContext object http://nathanmarz.github.io/storm/doc-0.7.1/?backtype/storm/task/TopologyContext.html on which the component will run
            - "pidDir": path of the directory heartbeatdir where this process should create an empty file named with its PID, so the parent process can kill this process is needed.
        2. Send the PID of this python process to the parent process, and create an empty file at the path specified by heartbeatdir.

    :returns: a list with two element, the first is a dictionary with the storm configuration, and the second is a dictionary with the storm context
    '''
    setupInfo = readMsg()
    sendpid(setupInfo['pidDir'])
    return [setupInfo['conf'], setupInfo['context']]

class Tuple(object):
    '''
    This class abstracts the tuples sent to a bolt. See https://github.com/nathanmarz/storm/wiki/Multilang-protocol#bolts for details

    :ivar id: id of the tuple as string
    :ivar component: id of the component that created this tuple as string
    :ivar stream: id of the stream this tuple was emitted to as string
    :ivar task: id of the task that created this tuple as integer
    :ivar values: list of values this tuple is composed
    '''
    def __init__(self, id, component, stream, task, values):
        self.id = id
        self.component = component
        self.stream = stream
        self.task = task
        self.values = values

    def __repr__(self):
        return '<%s%s>' % (
                self.__class__.__name__,
                ''.join(' %s=%r' % (k, self.__dict__[k]) for k in sorted(self.__dict__.keys())))

class Bolt(object):
    '''
    Roughly equivalent to the IBolt interface http://nathanmarz.github.io/storm/doc-0.7.1/backtype/storm/task/IBolt.html

    To define a bolt in python write a module that defines a subclass of Bolt, and that calls run() for and instance of that class in its __main__ 

    There is no equivalent to IBolt.prepare(), but:
        - The configuration and context obtained during the Multilang-protocol handshake are passed in the call to initialize().
        - Use the functions emit(), emitDirect(), ack(), fail() and reportError() to emulate the corresponding methods for OutputCollector  
        - Use the functiom log() to log messages in the parent Storm worker log
    '''
    def initialize(self, stormconf, context):
        '''
        Similar to IBolt.prepare(). Called when a task for this component is initialized within a worker on the cluster.

        :param stormconf: Storm configuration deserialized from the JSON object sent through stdin by the parent process at protocol handshake. See http://nathanmarz.github.io/storm/doc/backtype/storm/Config.html
        :type stormconf: dict

        :param context: Topology context on for the topology on which this component will run, deserialized from the JSON object sent through stdin by the parent process at protocol handshake. See http://nathanmarz.github.io/storm/doc-0.7.1/?backtype/storm/task/TopologyContext.html. 
        :type context: dict 
        '''
        pass

    def process(self, tuple):
        '''
        Process a new tuple

        :param tuple: Input tuple to process. Use tuple.values to access the list of component values for this tuple
        :type tuple: storm.Tuple
        '''
        pass

    def run(self):
        '''
        To define a bolt in python write a module that defines a subclass of Bolt, and that calls run() for and instance of that class in its __main__ 
        '''
        global MODE
        MODE = Bolt
        conf, context = initComponent()
        try:
            self.initialize(conf, context)
            while True:
                tup = readTuple()
                self.process(tup)
        except Exception, e:
            reportError(traceback.format_exc(e))

class BasicBolt(object):
    '''
    Similar to the Bolt class, but the anchoring and ack of each tuple is handled automatically. The same effect can be obtained within the Bolt class with suitable calls to the functions emit(), emitDirect() and ack() at the method process()
    '''
    def initialize(self, stormconf, context):
        pass

    def process(self, tuple):
        pass

    def run(self):
        global MODE
        MODE = Bolt
        global ANCHOR_TUPLE
        conf, context = initComponent()
        try:
            self.initialize(conf, context)
            while True:
                tup = readTuple()
                ANCHOR_TUPLE = tup
                self.process(tup)
                ack(tup)
        except Exception, e:
            reportError(traceback.format_exc(e))

class Spout(object):
    '''
    Roughly equivalent to the ISpout interface http://nathanmarz.github.io/storm/doc/backtype/storm/spout/ISpout.html

    To define a spout in python write a module that defines a subclass of Spout, and that calls run() for and instance of that class in its __main__ 

    There is no equivalent to ISpout.open(), but:
        - The configuration and context obtained during the Multilang-protocol handshake are passed in the call to initialize().
        - Use the functions emit(), emitDirect() and reportError() to emulate the corresponding methods for SpoutOutputCollector  
        - Use the functiom log() to log messages in the parent Storm worker log
    '''
    def initialize(self, conf, context):
        '''
        Similar to ISpout.open(). Called when a task for this component is initialized within a worker on the cluster.

        :param stormconf: Storm configuration deserialized from the JSON object sent through stdin by the parent process at protocol handshake. See http://nathanmarz.github.io/storm/doc/backtype/storm/Config.html
        :type stormconf: dict

        :param context: Topology context on for the topology on which this component will run, deserialized from the JSON object sent through stdin by the parent process at protocol handshake. See http://nathanmarz.github.io/storm/doc-0.7.1/?backtype/storm/task/TopologyContext.html. 
            As seen in the example below, and in https://groups.google.com/forum/#!topic/storm-user/LEnEB4k_SG4, there is currently a bug by which Storm doesn't send the "taskid" to the spouts on the initial handshake of the multilang protocol
        :type context: dict

        Examples:
            - Example configuration:

            {"topology.tuple.serializer": "backtype.storm.serialization.types.ListDelegateSerializer", "topology.workers": 1, "drpc.worker.threads": 64, "storm.messaging.netty.client_worker_threads": 1, "supervisor.heartbeat.frequency.secs": 5, "topology.executor.send.buffer.size": 1024, "drpc.childopts": "-Xmx768m", "nimbus.thrift.port": 6627, "storm.zookeeper.retry.intervalceiling.millis": 30000, "storm.local.dir": "/tmp/9a198ac9-61e4-406e-95a6-88d5a8d623b9", "topology.receiver.buffer.size": 8, "storm.zookeeper.servers": ["localhost"], "transactional.zookeeper.root": "/transactional", "drpc.request.timeout.secs": 600, "topology.skip.missing.kryo.registrations": true, "worker.heartbeat.frequency.secs": 1, "zmq.hwm": 0, "storm.zookeeper.connection.timeout": 15000, "java.library.path": "/usr/local/lib:/opt/local/lib:/usr/lib", "topology.max.error.report.per.interval": 5, "storm.messaging.netty.server_worker_threads": 1, "storm.id": "test-1-1397736849", "supervisor.worker.start.timeout.secs": 120, "zmq.threads": 1, "topology.acker.executors": null, "storm.local.mode.zmq": false, "topology.max.task.parallelism": null, "storm.zookeeper.port": 2000, "nimbus.childopts": "-Xmx1024m", "worker.childopts": "-Xmx768m", "drpc.queue.size": 128, "storm.zookeeper.retry.times": 5, "nimbus.monitor.freq.secs": 10, "storm.cluster.mode": "local", "dev.zookeeper.path": "/tmp/dev-storm-zookeeper", "drpc.invocations.port": 3773, "topology.tasks": null, "storm.zookeeper.root": "/storm", "logviewer.childopts": "-Xmx128m", "transactional.zookeeper.port": null, "topology.worker.childopts": null, "topology.max.spout.pending": null, "topology.kryo.register": null, "nimbus.cleanup.inbox.freq.secs": 600, "storm.messaging.netty.min_wait_ms": 100, "nimbus.task.timeout.secs": 30, "topology.sleep.spout.wait.strategy.time.ms": 1, "topology.optimize": true, "nimbus.reassign": true, "storm.messaging.transport": "backtype.storm.messaging.zmq", "logviewer.appender.name": "A1", "nimbus.host": "localhost", "ui.port": 8080, "supervisor.slots.ports": [1, 2, 3], "nimbus.file.copy.expiration.secs": 600, "supervisor.monitor.frequency.secs": 3, "ui.childopts": "-Xmx768m", "transactional.zookeeper.servers": null, "zmq.linger.millis": 0, "topology.error.throttle.interval.secs": 10, "topology.worker.shared.thread.pool.size": 4, "topology.executor.receive.buffer.size": 1024, "topology.spout.wait.strategy": "backtype.storm.spout.SleepSpoutWaitStrategy", "task.heartbeat.frequency.secs": 3, "topology.transfer.buffer.size": 1024, "storm.zookeeper.session.timeout": 20000, "topology.stats.sample.rate": 0.05, "topology.fall.back.on.java.serialization": true, "supervisor.childopts": "-Xmx256m", "topology.enable.message.timeouts": true, "storm.messaging.netty.max_wait_ms": 1000, "nimbus.topology.validator": "backtype.storm.nimbus.DefaultTopologyValidator", "nimbus.supervisor.timeout.secs": 60, "topology.disruptor.wait.strategy": "com.lmax.disruptor.BlockingWaitStrategy", "storm.messaging.netty.buffer_size": 5242880, "drpc.port": 3772, "topology.kryo.factory": "backtype.storm.serialization.DefaultKryoFactory", "storm.zookeeper.retry.interval": 1000, "storm.messaging.netty.max_retries": 30, "topology.tick.tuple.freq.secs": 30, "supervisor.enable": true, "nimbus.task.launch.secs": 120, "task.refresh.poll.secs": 10, "topology.message.timeout.secs": 30, "nimbus.inbox.jar.expiration.secs": 3600, "topology.state.synchronization.timeout.secs": 60, "topology.name": "test", "supervisor.worker.timeout.secs": 30, "topology.trident.batch.emit.interval.millis": 50, "topology.builtin.metrics.bucket.size.secs": 60, "storm.thrift.transport": "backtype.storm.security.auth.SimpleTransportPlugin", "logviewer.port": 8000, "topology.kryo.decorators": [], "topology.debug": true}

            - Example context:
                {"task->component": {"11": "__acker", "10": "TrendsSpout", "1": "TrendsSpout", "3": "TrendsSpout", "2": "TrendsSpout", "5": "TrendsSpout", "4": "TrendsSpout", "7": "TrendsSpout", "6": "TrendsSpout", "9": "TrendsSpout", "8": "TrendsSpout"}}
        '''
        pass

    def ack(self, id):
        '''
        Equivalent to ISpout.ack()

        :param id: messageId of a tuple emmitted by this spout which has been fully processed
        :type id: str
        '''
        pass

    def fail(self, id):
        '''
        Equivalent to ISpout.fail()

        :param id: messageId of a tuple emmitted by this spout which has failed to be fully processed
        :type id: str
        '''
        pass

    def nextTuple(self):
        '''
        This method is called when storm request.

        - Use the functions emit(), emitDirect() and reportError() to emulate the corresponding methods for SpoutOutputCollector  
        - Use the functiom log() to log messages in the parent Storm worker log
        '''
        pass

    def run(self):
        '''
        To define a spout in python write a module that defines a subclass of Spout, and that calls run() for and instance of that class in its __main__ 
        '''
        global MODE
        MODE = Spout
        conf, context = initComponent()
        try:
            self.initialize(conf, context)
            while True:
                msg = readCommand()
                if msg["command"] == "next":
                    self.nextTuple()
                if msg["command"] == "ack":
                    self.ack(msg["id"])
                if msg["command"] == "fail":
                    self.fail(msg["id"])
                sync()
        except Exception, e:
            reportError(traceback.format_exc(e))


