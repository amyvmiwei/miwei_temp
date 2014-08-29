import errno
import json
import os
import socket
import sys
import time
from socket import error as socket_error

LISTEN_PORT=15860

descriptors = list()
values = dict()

def metric_callback(name):
    '''Metric callback function.'''
    global values
    collect_metrics()
    return values[name]

def collect_metrics():
    global sock
    global values
    buf = ""
    while True:
        try:
            buf = sock.recv(8192)
            metrics = json.loads(buf)
            for key, value in metrics.iteritems():
                if isinstance(value, unicode):
                    values[key] = str(value)
                else:
                    values[key] = value
        except socket_error as serr:
            if serr.errno != errno.EAGAIN:
                return None
            break

def metric_init(params):
    '''Initialize the random number generator and create the
    metric definition dictionary object for each metric.'''
    global descriptors
    global sock

    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        sock.bind(("127.0.0.1", LISTEN_PORT))
    except socket_error as serr:
        print "error: unable to bind to port", LISTEN_PORT, "-", os.strerror(serr.errno)
        sys.exit(1)
    sock.setblocking(0);

    ##
    ## Specifying all version metrics for all hosts renders better in the UI
    ##
    d = {'name': 'hypertable.hyperspace.version',
         'call_back': metric_callback,
         'value_type': 'string',
         'description': 'Hyperspace version',
         'groups': 'hypertable Hyperspace'}
    values['hypertable.hyperspace.version'] = 'n/a'
    descriptors.append(d);
    
    d = {'name': 'hypertable.master.version',
         'call_back': metric_callback,
         'value_type': 'string',
         'description': 'Master version',
         'groups': 'hypertable Master'}
    values['hypertable.master.version'] = 'n/a'
    descriptors.append(d);
    
    d = {'name': 'hypertable.rangeserver.version',
         'call_back': metric_callback,
         'value_type': 'string',
         'description': 'RangeServer version',
         'groups': 'hypertable RangeServer'}
    values['hypertable.rangeserver.version'] = 'n/a'
    descriptors.append(d);

    d = {'name': 'hypertable.thriftbroker.version',
         'call_back': metric_callback,
         'value_type': 'string',
         'description': 'ThriftBroker version',
         'groups': 'hypertable ThriftBroker'}
    values['hypertable.thriftbroker.version'] = 'n/a'
    descriptors.append(d);


    ##
    ## Hyperspace metrics
    ##
    if 'EnableHyperspace' in params and int(params['EnableHyperspace']) == 1:
        d = {'name': 'hypertable.hyperspace.requests',
             'call_back': metric_callback,
             'time_max': 90,
             'value_type': 'float',
             'units': 'requests/s',
             'slope': 'both',
             'format': '%f',
             'description': 'Request rate',
             'groups': 'hypertable Hyperspace'}
        values['hypertable.hyperspace.requests'] = 0
        descriptors.append(d);
        
        d = {'name': 'hypertable.hyperspace.cpu.sys',
             'call_back': metric_callback,
             'time_max': 90,
             'value_type': 'uint',
             'units': '%',
             'slope': 'both',
             'format': '%u',
             'description': 'Process CPU system time',
             'groups': 'hypertable Hyperspace'}
        values['hypertable.hyperspace.cpu.sys'] = 0
        descriptors.append(d);
        
        d = {'name': 'hypertable.hyperspace.cpu.user',
             'call_back': metric_callback,
             'time_max': 90,
             'value_type': 'uint',
             'units': '%',
             'slope': 'both',
             'format': '%u',
             'description': 'Process CPU user time',
             'groups': 'hypertable Hyperspace'}
        values['hypertable.hyperspace.cpu.user'] = 0
        descriptors.append(d);
        
        d = {'name': 'hypertable.hyperspace.memory.virtual',
             'call_back': metric_callback,
             'time_max': 90,
             'value_type': 'float',
             'units': 'GB',
             'slope': 'both',
             'format': '%f',
             'description': 'Virtual memory',
             'groups': 'hypertable Hyperspace'}
        values['hypertable.hyperspace.memory.virtual'] = 0
        descriptors.append(d);
        
        d = {'name': 'hypertable.hyperspace.memory.resident',
             'call_back': metric_callback,
             'time_max': 90,
             'value_type': 'float',
             'units': 'GB',
             'slope': 'both',
             'format': '%f',
             'description': 'Resident memory',
             'groups': 'hypertable Hyperspace'}
        values['hypertable.hyperspace.memory.resident'] = 0
        descriptors.append(d);
        
        d = {'name': 'hypertable.hyperspace.memory.heap',
             'call_back': metric_callback,
             'time_max': 90,
             'value_type': 'float',
             'units': 'GB',
             'slope': 'both',
             'format': '%f',
             'description': 'Heap memory',
             'groups': 'hypertable Hyperspace'}
        values['hypertable.hyperspace.memory.heap'] = 0
        descriptors.append(d);
        
        d = {'name': 'hypertable.hyperspace.memory.heapSlack',
             'call_back': metric_callback,
             'time_max': 90,
             'value_type': 'float',
             'units': 'GB',
             'slope': 'both',
             'format': '%f',
             'description': 'Heap slack bytes',
             'groups': 'hypertable Hyperspace'}
        values['hypertable.hyperspace.memory.heapSlack'] = 0
        descriptors.append(d);
        
    ##
    ## Master metrics
    ##
    if 'EnableMaster' in params and int(params['EnableMaster']) == 1:
        d = {'name': 'hypertable.master.operations',
             'call_back': metric_callback,
             'time_max': 90,
             'value_type': 'float',
             'units': 'operations/s',
             'slope': 'both',
             'format': '%f',
             'description': 'Operation rate',
             'groups': 'hypertable Master'}
        values['hypertable.master.operations'] = 0
        descriptors.append(d);
        
        d = {'name': 'hypertable.master.cpu.sys',
             'call_back': metric_callback,
             'time_max': 90,
             'value_type': 'uint',
             'units': '%',
             'slope': 'both',
             'format': '%u',
             'description': 'Process CPU system time',
             'groups': 'hypertable Master'}
        values['hypertable.master.cpu.sys'] = 0
        descriptors.append(d);
        
        d = {'name': 'hypertable.master.cpu.user',
             'call_back': metric_callback,
             'time_max': 90,
             'value_type': 'uint',
             'units': '%',
             'slope': 'both',
             'format': '%u',
             'description': 'Process CPU user time',
             'groups': 'hypertable Master'}
        values['hypertable.master.cpu.user'] = 0
        descriptors.append(d);
        
        d = {'name': 'hypertable.master.memory.virtual',
             'call_back': metric_callback,
             'time_max': 90,
             'value_type': 'float',
             'units': 'GB',
             'slope': 'both',
             'format': '%f',
             'description': 'Virtual memory',
             'groups': 'hypertable Master'}
        values['hypertable.master.memory.virtual'] = 0
        descriptors.append(d);
        
        d = {'name': 'hypertable.master.memory.resident',
             'call_back': metric_callback,
             'time_max': 90,
             'value_type': 'float',
             'units': 'GB',
             'slope': 'both',
             'format': '%f',
             'description': 'Resident memory',
             'groups': 'hypertable Master'}
        values['hypertable.master.memory.resident'] = 0
        descriptors.append(d);
        
        d = {'name': 'hypertable.master.memory.heap',
             'call_back': metric_callback,
             'time_max': 90,
             'value_type': 'float',
             'units': 'GB',
             'slope': 'both',
             'format': '%f',
             'description': 'Heap memory',
             'groups': 'hypertable Master'}
        values['hypertable.master.memory.heap'] = 0
        descriptors.append(d);
        
        d = {'name': 'hypertable.master.memory.heapSlack',
             'call_back': metric_callback,
             'time_max': 90,
             'value_type': 'float',
             'units': 'GB',
             'slope': 'both',
             'format': '%f',
             'description': 'Heap slack bytes',
             'groups': 'hypertable Master'}
        values['hypertable.master.memory.heapSlack'] = 0
        descriptors.append(d);

    ##
    ## RangeServer metrics
    ##
    if 'EnableRangeServer' in params and int(params['EnableRangeServer']) == 1:
        d = {'name': 'hypertable.rangeserver.scans',
             'call_back': metric_callback,
             'time_max': 90,
             'value_type': 'float',
             'units': 'scans/s',
             'slope': 'both',
             'format': '%f',
             'description': 'Scans per second',
             'groups': 'hypertable RangeServer'}
        values['hypertable.rangeserver.scans'] = 0
        descriptors.append(d);
        
        d = {'name': 'hypertable.rangeserver.updates',
             'call_back': metric_callback,
             'time_max': 90,
             'value_type': 'float',
             'units': 'updates/s',
             'slope': 'both',
             'format': '%f',
             'description': 'Updates per second',
             'groups': 'hypertable RangeServer'}
        values['hypertable.rangeserver.updates'] = 0
        descriptors.append(d);
        
        d = {'name': 'hypertable.rangeserver.cellsRead',
             'call_back': metric_callback,
             'time_max': 90,
             'value_type': 'float',
             'units': 'cells/s',
             'slope': 'both',
             'format': '%f',
             'description': 'Cells read per second',
             'groups': 'hypertable RangeServer'}
        values['hypertable.rangeserver.cellsRead'] = 0
        descriptors.append(d);
        
        d = {'name': 'hypertable.rangeserver.cellsWritten',
             'call_back': metric_callback,
             'time_max': 90,
             'value_type': 'float',
             'units': 'cells/s',
             'slope': 'both',
             'format': '%f',
             'description': 'Cells written per second',
             'groups': 'hypertable RangeServer'}
        values['hypertable.rangeserver.cellsWritten'] = 0
        descriptors.append(d);
        
        d = {'name': 'hypertable.rangeserver.compactions.major',
             'call_back': metric_callback,
             'time_max': 90,
             'value_type': 'uint',
             'units': 'compactions',
             'slope': 'both',
             'format': '%u',
             'description': 'Major compactions',
             'groups': 'hypertable RangeServer'}
        values['hypertable.rangeserver.compactions.major'] = 0
        descriptors.append(d);
        
        d = {'name': 'hypertable.rangeserver.compactions.minor',
             'call_back': metric_callback,
             'time_max': 90,
             'value_type': 'uint',
             'units': 'compactions',
             'slope': 'both',
             'format': '%u',
             'description': 'Minor compactions',
             'groups': 'hypertable RangeServer'}
        values['hypertable.rangeserver.compactions.minor'] = 0
        descriptors.append(d);
        
        d = {'name': 'hypertable.rangeserver.compactions.merging',
             'call_back': metric_callback,
             'time_max': 90,
             'value_type': 'uint',
             'units': 'compactions',
             'slope': 'both',
             'format': '%u',
             'description': 'Merging compactions',
             'groups': 'hypertable RangeServer'}
        values['hypertable.rangeserver.compactions.merging'] = 0
        descriptors.append(d);
        
        d = {'name': 'hypertable.rangeserver.compactions.gc',
             'call_back': metric_callback,
             'time_max': 90,
             'value_type': 'uint',
             'units': 'compactions',
             'slope': 'both',
             'format': '%u',
             'description': 'GC compactions',
             'groups': 'hypertable RangeServer'}
        values['hypertable.rangeserver.compactions.gc'] = 0
        descriptors.append(d);
        
        d = {'name': 'hypertable.rangeserver.scanners',
             'call_back': metric_callback,
             'time_max': 90,
             'value_type': 'uint',
             'units': 'scanners',
             'slope': 'both',
             'format': '%u',
             'description': 'Outstanding scanner count',
             'groups': 'hypertable RangeServer'}
        values['hypertable.rangeserver.scanners'] = 0
        descriptors.append(d);
        
        d = {'name': 'hypertable.rangeserver.cellstores',
             'call_back': metric_callback,
             'time_max': 90,
             'value_type': 'uint',
             'units': 'cellstores',
             'slope': 'both',
             'format': '%u',
             'description': 'CellStore count',
             'groups': 'hypertable RangeServer'}
        values['hypertable.rangeserver.cellstores'] = 0
        descriptors.append(d);
        
        d = {'name': 'hypertable.rangeserver.ranges',
             'call_back': metric_callback,
             'time_max': 90,
             'value_type': 'uint',
             'units': 'ranges',
             'slope': 'both',
             'format': '%u',
             'description': 'Range count',
             'groups': 'hypertable RangeServer'}
        values['hypertable.rangeserver.ranges'] = 0
        descriptors.append(d);
        
        d = {'name': 'hypertable.rangeserver.memory.virtual',
             'call_back': metric_callback,
             'time_max': 90,
             'value_type': 'float',
             'units': 'GB',
             'slope': 'both',
             'format': '%f',
             'description': 'Virtual memory',
             'groups': 'hypertable RangeServer'}
        values['hypertable.rangeserver.memory.virtual'] = 0
        descriptors.append(d);
        
        d = {'name': 'hypertable.rangeserver.memory.resident',
             'call_back': metric_callback,
             'time_max': 90,
             'value_type': 'float',
             'units': 'GB',
             'slope': 'both',
             'format': '%f',
             'description': 'Resident memory',
             'groups': 'hypertable RangeServer'}
        values['hypertable.rangeserver.memory.resident'] = 0
        descriptors.append(d);
        
        d = {'name': 'hypertable.rangeserver.memory.heap',
             'call_back': metric_callback,
             'time_max': 90,
             'value_type': 'float',
             'units': 'GB',
             'slope': 'both',
             'format': '%f',
             'description': 'Heap memory',
             'groups': 'hypertable RangeServer'}
        values['hypertable.rangeserver.memory.heap'] = 0
        descriptors.append(d);
        
        d = {'name': 'hypertable.rangeserver.memory.heapSlack',
             'call_back': metric_callback,
             'time_max': 90,
             'value_type': 'float',
             'units': 'GB',
             'slope': 'both',
             'format': '%f',
             'description': 'Heap slack bytes',
             'groups': 'hypertable RangeServer'}
        values['hypertable.rangeserver.memory.heapSlack'] = 0
        descriptors.append(d);
        
        d = {'name': 'hypertable.rangeserver.memory.tracked',
             'call_back': metric_callback,
             'time_max': 90,
             'value_type': 'float',
             'units': 'GB',
             'slope': 'both',
             'format': '%f',
             'description': 'Tracked memory',
             'groups': 'hypertable RangeServer'}
        values['hypertable.rangeserver.memory.tracked'] = 0
        descriptors.append(d);
        
        d = {'name': 'hypertable.rangeserver.cpu.sys',
             'call_back': metric_callback,
             'time_max': 90,
             'value_type': 'uint',
             'units': '%',
             'slope': 'both',
             'format': '%u',
             'description': 'Process CPU system time',
             'groups': 'hypertable RangeServer'}
        values['hypertable.rangeserver.cpu.sys'] = 0
        descriptors.append(d);
        
        d = {'name': 'hypertable.rangeserver.cpu.user',
             'call_back': metric_callback,
             'time_max': 90,
             'value_type': 'uint',
             'units': '%',
             'slope': 'both',
             'format': '%u',
             'description': 'Process CPU user time',
             'groups': 'hypertable RangeServer'}
        values['hypertable.rangeserver.cpu.user'] = 0
        descriptors.append(d);
        
        d = {'name': 'hypertable.rangeserver.blockCache.hitRate',
             'call_back': metric_callback,
             'time_max': 90,
             'value_type': 'uint',
             'units': '%',
             'slope': 'both',
             'format': '%u',
             'description': 'Block cache hit rate',
             'groups': 'hypertable RangeServer'}
        values['hypertable.rangeserver.blockCache.hitRate'] = 0
        descriptors.append(d);
        
        d = {'name': 'hypertable.rangeserver.blockCache.memory',
             'call_back': metric_callback,
             'time_max': 90,
             'value_type': 'float',
             'units': 'GB',
             'slope': 'both',
             'format': '%f',
             'description': 'Block cache memory',
             'groups': 'hypertable RangeServer'}
        values['hypertable.rangeserver.blockCache.memory'] = 0
        descriptors.append(d);
        
        d = {'name': 'hypertable.rangeserver.blockCache.fill',
             'call_back': metric_callback,
             'time_max': 90,
             'value_type': 'float',
             'units': 'GB',
             'slope': 'both',
             'format': '%f',
             'description': 'Block cache fill',
             'groups': 'hypertable RangeServer'}
        values['hypertable.rangeserver.blockCache.fill'] = 0
        descriptors.append(d);
        
        d = {'name': 'hypertable.rangeserver.queryCache.hitRate',
             'call_back': metric_callback,
             'time_max': 90,
             'value_type': 'uint',
             'units': '%',
             'slope': 'both',
             'format': '%u',
             'description': 'Query cache hit rate',
             'groups': 'hypertable RangeServer'}
        values['hypertable.rangeserver.queryCache.hitRate'] = 0
        descriptors.append(d);
        
        d = {'name': 'hypertable.rangeserver.queryCache.memory',
             'call_back': metric_callback,
             'time_max': 90,
             'value_type': 'float',
             'units': 'GB',
             'slope': 'both',
             'format': '%f',
             'description': 'Query cache memory',
             'groups': 'hypertable RangeServer'}
        values['hypertable.rangeserver.queryCache.memory'] = 0
        descriptors.append(d);
        
        d = {'name': 'hypertable.rangeserver.queryCache.fill',
             'call_back': metric_callback,
             'time_max': 90,
             'value_type': 'float',
             'units': 'GB',
             'slope': 'both',
             'format': '%f',
             'description': 'Query cache fill',
             'groups': 'hypertable RangeServer'}
        values['hypertable.rangeserver.queryCache.fill'] = 0
        descriptors.append(d);

    ##
    ## ThriftBroker metrics
    ##
    if 'EnableThriftBroker' in params and int(params['EnableThriftBroker']) == 1:
        d = {'name': 'hypertable.thriftbroker.requests',
             'call_back': metric_callback,
             'time_max': 90,
             'value_type': 'float',
             'units': 'requests/s',
             'slope': 'both',
             'format': '%f',
             'description': 'Request rate',
             'groups': 'hypertable ThriftBroker'}
        values['hypertable.thriftbroker.requests'] = 0
        descriptors.append(d);
        
        d = {'name': 'hypertable.thriftbroker.errors',
             'call_back': metric_callback,
             'time_max': 90,
             'value_type': 'float',
             'units': 'errors/s',
             'slope': 'both',
             'format': '%f',
             'description': 'Error rate',
             'groups': 'hypertable ThriftBroker'}
        values['hypertable.thriftbroker.errors'] = 0
        descriptors.append(d);
        
        d = {'name': 'hypertable.thriftbroker.connections',
             'call_back': metric_callback,
             'time_max': 90,
             'value_type': 'uint',
             'units': 'connections',
             'slope': 'both',
             'format': '%u',
             'description': 'Active connection count',
             'groups': 'hypertable ThriftBroker'}
        values['hypertable.thriftbroker.connections'] = 0
        descriptors.append(d);
        
        d = {'name': 'hypertable.thriftbroker.cpu.sys',
             'call_back': metric_callback,
             'time_max': 90,
             'value_type': 'uint',
             'units': '%',
             'slope': 'both',
             'format': '%u',
             'description': 'Process CPU system time',
             'groups': 'hypertable ThriftBroker'}
        values['hypertable.thriftbroker.cpu.sys'] = 0
        descriptors.append(d);
        
        d = {'name': 'hypertable.thriftbroker.cpu.user',
             'call_back': metric_callback,
             'time_max': 90,
             'value_type': 'uint',
             'units': '%',
             'slope': 'both',
             'format': '%u',
             'description': 'Process CPU user time',
             'groups': 'hypertable ThriftBroker'}
        values['hypertable.thriftbroker.cpu.user'] = 0
        descriptors.append(d);
        
        d = {'name': 'hypertable.thriftbroker.memory.virtual',
             'call_back': metric_callback,
             'time_max': 90,
             'value_type': 'float',
             'units': 'GB',
             'slope': 'both',
             'format': '%f',
             'description': 'Virtual memory',
             'groups': 'hypertable ThriftBroker'}
        values['hypertable.thriftbroker.memory.virtual'] = 0
        descriptors.append(d);
        
        d = {'name': 'hypertable.thriftbroker.memory.resident',
             'call_back': metric_callback,
             'time_max': 90,
             'value_type': 'float',
             'units': 'GB',
             'slope': 'both',
             'format': '%f',
             'description': 'Resident memory',
             'groups': 'hypertable ThriftBroker'}
        values['hypertable.thriftbroker.memory.resident'] = 0
        descriptors.append(d);
        
        d = {'name': 'hypertable.thriftbroker.memory.heap',
             'call_back': metric_callback,
             'time_max': 90,
             'value_type': 'float',
             'units': 'GB',
             'slope': 'both',
             'format': '%f',
             'description': 'Heap memory',
             'groups': 'hypertable ThriftBroker'}
        values['hypertable.thriftbroker.memory.heap'] = 0
        descriptors.append(d);
        
        d = {'name': 'hypertable.thriftbroker.memory.heapSlack',
             'call_back': metric_callback,
             'time_max': 90,
             'value_type': 'float',
             'units': 'GB',
             'slope': 'both',
             'format': '%f',
             'description': 'Heap slack bytes',
             'groups': 'hypertable ThriftBroker'}
        values['hypertable.thriftbroker.memory.heapSlack'] = 0
        descriptors.append(d);

    return descriptors

def metric_cleanup():
    '''Clean up the metric module.'''
    global sock
    sock.close()


#This code is for debugging and unit testing
if __name__ == '__main__':
    if len(sys.argv) < 2:
        print "usage: hypertable.py <metric>"
        sys.exit(0)
    params = { 'EnableHyperspace': '1',
               'EnableMaster': '1',
               'EnableRangeServer': '1',
               'EnableThriftBroker': '1' }
    metric_init(params)
    while True:
        scans = metric_callback(sys.argv[1])
        print sys.argv[1], "=", scans
        time.sleep(30)
