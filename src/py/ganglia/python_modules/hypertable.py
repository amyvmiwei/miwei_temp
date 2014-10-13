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
    if name in values:
        rval = values[name]
        if not ".version" in name or ".type" in name:
            del values[name]
        return rval
    else:
        return None

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
    d = {'name': 'ht.hyperspace.version',
         'call_back': metric_callback,
         'value_type': 'string',
         'description': 'Hyperspace version',
         'groups': 'hypertable Hyperspace'}
    values['ht.hyperspace.version'] = 'n/a'
    descriptors.append(d);

    d = {'name': 'ht.fsbroker.version',
         'call_back': metric_callback,
         'value_type': 'string',
         'description': 'FSBroker version',
         'groups': 'hypertable FSBroker'}
    values['ht.fsbroker.version'] = 'n/a'
    descriptors.append(d);

    d = {'name': 'ht.fsbroker.type',
         'call_back': metric_callback,
         'value_type': 'string',
         'description': 'FSBroker type',
         'groups': 'hypertable FSBroker'}
    values['ht.fsbroker.type'] = 'n/a'
    descriptors.append(d);
    
    d = {'name': 'ht.master.version',
         'call_back': metric_callback,
         'value_type': 'string',
         'description': 'Master version',
         'groups': 'hypertable Master'}
    values['ht.master.version'] = 'n/a'
    descriptors.append(d);
    
    d = {'name': 'ht.rangeserver.version',
         'call_back': metric_callback,
         'value_type': 'string',
         'description': 'RangeServer version',
         'groups': 'hypertable RangeServer'}
    values['ht.rangeserver.version'] = 'n/a'
    descriptors.append(d);

    d = {'name': 'ht.thriftbroker.version',
         'call_back': metric_callback,
         'value_type': 'string',
         'description': 'ThriftBroker version',
         'groups': 'hypertable ThriftBroker'}
    values['ht.thriftbroker.version'] = 'n/a'
    descriptors.append(d);


    ##
    ## Hyperspace metrics
    ##
    if 'EnableHyperspace' in params and int(params['EnableHyperspace']) == 1:
        d = {'name': 'ht.hyperspace.requests',
             'call_back': metric_callback,
             'time_max': 90,
             'value_type': 'float',
             'units': 'requests/s',
             'slope': 'both',
             'format': '%f',
             'description': 'Request rate',
             'groups': 'hypertable Hyperspace'}
        descriptors.append(d);
        
        d = {'name': 'ht.hyperspace.cpu.sys',
             'call_back': metric_callback,
             'time_max': 90,
             'value_type': 'uint',
             'units': '%',
             'slope': 'both',
             'format': '%u',
             'description': 'Process CPU system time',
             'groups': 'hypertable Hyperspace'}
        descriptors.append(d);
        
        d = {'name': 'ht.hyperspace.cpu.user',
             'call_back': metric_callback,
             'time_max': 90,
             'value_type': 'uint',
             'units': '%',
             'slope': 'both',
             'format': '%u',
             'description': 'Process CPU user time',
             'groups': 'hypertable Hyperspace'}
        descriptors.append(d);
        
        d = {'name': 'ht.hyperspace.memory.virtual',
             'call_back': metric_callback,
             'time_max': 90,
             'value_type': 'float',
             'units': 'GB',
             'slope': 'both',
             'format': '%f',
             'description': 'Virtual memory',
             'groups': 'hypertable Hyperspace'}
        descriptors.append(d);
        
        d = {'name': 'ht.hyperspace.memory.resident',
             'call_back': metric_callback,
             'time_max': 90,
             'value_type': 'float',
             'units': 'GB',
             'slope': 'both',
             'format': '%f',
             'description': 'Resident memory',
             'groups': 'hypertable Hyperspace'}
        descriptors.append(d);
        
        d = {'name': 'ht.hyperspace.memory.heap',
             'call_back': metric_callback,
             'time_max': 90,
             'value_type': 'float',
             'units': 'GB',
             'slope': 'both',
             'format': '%f',
             'description': 'Heap memory',
             'groups': 'hypertable Hyperspace'}
        descriptors.append(d);
        
        d = {'name': 'ht.hyperspace.memory.heapSlack',
             'call_back': metric_callback,
             'time_max': 90,
             'value_type': 'float',
             'units': 'GB',
             'slope': 'both',
             'format': '%f',
             'description': 'Heap slack bytes',
             'groups': 'hypertable Hyperspace'}
        descriptors.append(d);

    ##
    ## FSBroker metrics
    ##
    if 'EnableFSBroker' in params and int(params['EnableFSBroker']) == 1:
        d = {'name': 'ht.fsbroker.errors',
             'call_back': metric_callback,
             'time_max': 90,
             'value_type': 'uint',
             'units': 'errors',
             'slope': 'both',
             'format': '%u',
             'description': 'Error count',
             'groups': 'hypertable FSBroker'}
        descriptors.append(d);

        d = {'name': 'ht.fsbroker.syncs',
             'call_back': metric_callback,
             'time_max': 90,
             'value_type': 'float',
             'units': 'syncs/s',
             'slope': 'both',
             'format': '%f',
             'description': 'Syncs/s',
             'groups': 'hypertable FSBroker'}
        descriptors.append(d);

        d = {'name': 'ht.fsbroker.syncLatency',
             'call_back': metric_callback,
             'time_max': 90,
             'value_type': 'uint',
             'units': 'ms',
             'slope': 'both',
             'format': '%u',
             'description': 'Sync latency',
             'groups': 'hypertable FSBroker'}
        descriptors.append(d);

        d = {'name': 'ht.fsbroker.readThroughput',
             'call_back': metric_callback,
             'time_max': 90,
             'value_type': 'uint',
             'units': 'MB/s',
             'slope': 'both',
             'format': '%u',
             'description': 'Read throughput',
             'groups': 'hypertable FSBroker'}
        descriptors.append(d);

        d = {'name': 'ht.fsbroker.writeThroughput',
             'call_back': metric_callback,
             'time_max': 90,
             'value_type': 'uint',
             'units': 'MB/s',
             'slope': 'both',
             'format': '%u',
             'description': 'Write throughput',
             'groups': 'hypertable FSBroker'}
        descriptors.append(d);

        if 'FSBroker' in params and params['FSBroker'] == "hadoop":
            d = {'name': 'ht.fsbroker.jvm.gc',
                 'call_back': metric_callback,
                 'time_max': 90,
                 'value_type': 'uint',
                 'units': 'GCs',
                 'slope': 'both',
                 'format': '%u',
                 'description': 'JVM GCs',
                 'groups': 'hypertable FSBroker'}
            descriptors.append(d);
            
            d = {'name': 'ht.fsbroker.jvm.gcTime',
                 'call_back': metric_callback,
                 'time_max': 90,
                 'value_type': 'uint',
                 'units': 'ms',
                 'slope': 'both',
                 'format': '%u',
                 'description': 'JVM GC time',
                 'groups': 'hypertable FSBroker'}
            descriptors.append(d);
            
            d = {'name': 'ht.fsbroker.jvm.heapSize',
                 'call_back': metric_callback,
                 'time_max': 90,
                 'value_type': 'float',
                 'units': 'GB',
                 'slope': 'both',
                 'format': '%f',
                 'description': 'JVM heap size',
                 'groups': 'hypertable FSBroker'}
            descriptors.append(d);
        
        d = {'name': 'ht.fsbroker.cpu.sys',
             'call_back': metric_callback,
             'time_max': 90,
             'value_type': 'uint',
             'units': '%',
             'slope': 'both',
             'format': '%u',
             'description': 'Process CPU system time',
             'groups': 'hypertable FSBroker'}
        descriptors.append(d);
        
        d = {'name': 'ht.fsbroker.cpu.user',
             'call_back': metric_callback,
             'time_max': 90,
             'value_type': 'uint',
             'units': '%',
             'slope': 'both',
             'format': '%u',
             'description': 'Process CPU user time',
             'groups': 'hypertable FSBroker'}
        descriptors.append(d);
        
        d = {'name': 'ht.fsbroker.memory.virtual',
             'call_back': metric_callback,
             'time_max': 90,
             'value_type': 'float',
             'units': 'GB',
             'slope': 'both',
             'format': '%f',
             'description': 'Virtual memory',
             'groups': 'hypertable FSBroker'}
        descriptors.append(d);
        
        d = {'name': 'ht.fsbroker.memory.resident',
             'call_back': metric_callback,
             'time_max': 90,
             'value_type': 'float',
             'units': 'GB',
             'slope': 'both',
             'format': '%f',
             'description': 'Resident memory',
             'groups': 'hypertable FSBroker'}
        descriptors.append(d);

        if 'FSBroker' in params and params['FSBroker'] != "hadoop":
            d = {'name': 'ht.fsbroker.memory.heap',
                 'call_back': metric_callback,
                 'time_max': 90,
                 'value_type': 'float',
                 'units': 'GB',
                 'slope': 'both',
                 'format': '%f',
                 'description': 'Heap memory',
                 'groups': 'hypertable FSBroker'}
            descriptors.append(d);
            
            d = {'name': 'ht.fsbroker.memory.heapSlack',
                 'call_back': metric_callback,
                 'time_max': 90,
                 'value_type': 'float',
                 'units': 'GB',
                 'slope': 'both',
                 'format': '%f',
                 'description': 'Heap slack bytes',
                 'groups': 'hypertable FSBroker'}
            descriptors.append(d);
        
    ##
    ## Master metrics
    ##
    if 'EnableMaster' in params and int(params['EnableMaster']) == 1:
        d = {'name': 'ht.master.operations',
             'call_back': metric_callback,
             'time_max': 90,
             'value_type': 'float',
             'units': 'operations/s',
             'slope': 'both',
             'format': '%f',
             'description': 'Operation rate',
             'groups': 'hypertable Master'}
        descriptors.append(d);
        
        d = {'name': 'ht.master.cpu.sys',
             'call_back': metric_callback,
             'time_max': 90,
             'value_type': 'uint',
             'units': '%',
             'slope': 'both',
             'format': '%u',
             'description': 'Process CPU system time',
             'groups': 'hypertable Master'}
        descriptors.append(d);
        
        d = {'name': 'ht.master.cpu.user',
             'call_back': metric_callback,
             'time_max': 90,
             'value_type': 'uint',
             'units': '%',
             'slope': 'both',
             'format': '%u',
             'description': 'Process CPU user time',
             'groups': 'hypertable Master'}
        descriptors.append(d);
        
        d = {'name': 'ht.master.memory.virtual',
             'call_back': metric_callback,
             'time_max': 90,
             'value_type': 'float',
             'units': 'GB',
             'slope': 'both',
             'format': '%f',
             'description': 'Virtual memory',
             'groups': 'hypertable Master'}
        descriptors.append(d);
        
        d = {'name': 'ht.master.memory.resident',
             'call_back': metric_callback,
             'time_max': 90,
             'value_type': 'float',
             'units': 'GB',
             'slope': 'both',
             'format': '%f',
             'description': 'Resident memory',
             'groups': 'hypertable Master'}
        descriptors.append(d);
        
        d = {'name': 'ht.master.memory.heap',
             'call_back': metric_callback,
             'time_max': 90,
             'value_type': 'float',
             'units': 'GB',
             'slope': 'both',
             'format': '%f',
             'description': 'Heap memory',
             'groups': 'hypertable Master'}
        descriptors.append(d);
        
        d = {'name': 'ht.master.memory.heapSlack',
             'call_back': metric_callback,
             'time_max': 90,
             'value_type': 'float',
             'units': 'GB',
             'slope': 'both',
             'format': '%f',
             'description': 'Heap slack bytes',
             'groups': 'hypertable Master'}
        descriptors.append(d);

    ##
    ## RangeServer metrics
    ##
    if 'EnableRangeServer' in params and int(params['EnableRangeServer']) == 1:
        d = {'name': 'ht.rangeserver.scans',
             'call_back': metric_callback,
             'time_max': 90,
             'value_type': 'float',
             'units': 'scans/s',
             'slope': 'both',
             'format': '%f',
             'description': 'Scans per second',
             'groups': 'hypertable RangeServer'}
        descriptors.append(d);
        
        d = {'name': 'ht.rangeserver.updates',
             'call_back': metric_callback,
             'time_max': 90,
             'value_type': 'float',
             'units': 'updates/s',
             'slope': 'both',
             'format': '%f',
             'description': 'Updates per second',
             'groups': 'hypertable RangeServer'}
        descriptors.append(d);
        
        d = {'name': 'ht.rangeserver.cellsReturned',
             'call_back': metric_callback,
             'time_max': 90,
             'value_type': 'float',
             'units': 'cells/s',
             'slope': 'both',
             'format': '%f',
             'description': 'Cells returned per second',
             'groups': 'hypertable RangeServer'}
        descriptors.append(d);

        d = {'name': 'ht.rangeserver.cellsScanned',
             'call_back': metric_callback,
             'time_max': 90,
             'value_type': 'float',
             'units': 'cells/s',
             'slope': 'both',
             'format': '%f',
             'description': 'Cells scanned per second',
             'groups': 'hypertable RangeServer'}
        descriptors.append(d);

        d = {'name': 'ht.rangeserver.cellsScanYield',
             'call_back': metric_callback,
             'time_max': 90,
             'value_type': 'float',
             'units': '%',
             'slope': 'both',
             'format': '%f',
             'description': 'Cell scan yield',
             'groups': 'hypertable RangeServer'}
        descriptors.append(d);
        
        d = {'name': 'ht.rangeserver.cellsWritten',
             'call_back': metric_callback,
             'time_max': 90,
             'value_type': 'float',
             'units': 'cells/s',
             'slope': 'both',
             'format': '%f',
             'description': 'Cells written per second',
             'groups': 'hypertable RangeServer'}
        descriptors.append(d);

        d = {'name': 'ht.rangeserver.bytesReturned',
             'call_back': metric_callback,
             'time_max': 90,
             'value_type': 'float',
             'units': 'bytes/s',
             'slope': 'both',
             'format': '%f',
             'description': 'Bytes returned per second',
             'groups': 'hypertable RangeServer'}
        descriptors.append(d);

        d = {'name': 'ht.rangeserver.bytesScanned',
             'call_back': metric_callback,
             'time_max': 90,
             'value_type': 'float',
             'units': 'bytes/s',
             'slope': 'both',
             'format': '%f',
             'description': 'Bytes scanned per second',
             'groups': 'hypertable RangeServer'}
        descriptors.append(d);

        d = {'name': 'ht.rangeserver.bytesScanYield',
             'call_back': metric_callback,
             'time_max': 90,
             'value_type': 'float',
             'units': '%',
             'slope': 'both',
             'format': '%f',
             'description': 'Byte scan yield',
             'groups': 'hypertable RangeServer'}
        descriptors.append(d);
        
        d = {'name': 'ht.rangeserver.bytesWritten',
             'call_back': metric_callback,
             'time_max': 90,
             'value_type': 'float',
             'units': 'bytes/s',
             'slope': 'both',
             'format': '%f',
             'description': 'Bytes written per second',
             'groups': 'hypertable RangeServer'}
        descriptors.append(d);

        d = {'name': 'ht.rangeserver.requestBacklog',
             'call_back': metric_callback,
             'time_max': 90,
             'value_type': 'uint',
             'units': 'requests',
             'slope': 'both',
             'format': '%u',
             'description': 'Request backlog',
             'groups': 'hypertable RangeServer'}
        descriptors.append(d);
        
        d = {'name': 'ht.rangeserver.compactions.major',
             'call_back': metric_callback,
             'time_max': 90,
             'value_type': 'uint',
             'units': 'compactions',
             'slope': 'both',
             'format': '%u',
             'description': 'Major compactions',
             'groups': 'hypertable RangeServer'}
        descriptors.append(d);
        
        d = {'name': 'ht.rangeserver.compactions.minor',
             'call_back': metric_callback,
             'time_max': 90,
             'value_type': 'uint',
             'units': 'compactions',
             'slope': 'both',
             'format': '%u',
             'description': 'Minor compactions',
             'groups': 'hypertable RangeServer'}
        descriptors.append(d);
        
        d = {'name': 'ht.rangeserver.compactions.merging',
             'call_back': metric_callback,
             'time_max': 90,
             'value_type': 'uint',
             'units': 'compactions',
             'slope': 'both',
             'format': '%u',
             'description': 'Merging compactions',
             'groups': 'hypertable RangeServer'}
        descriptors.append(d);
        
        d = {'name': 'ht.rangeserver.compactions.gc',
             'call_back': metric_callback,
             'time_max': 90,
             'value_type': 'uint',
             'units': 'compactions',
             'slope': 'both',
             'format': '%u',
             'description': 'GC compactions',
             'groups': 'hypertable RangeServer'}
        descriptors.append(d);
        
        d = {'name': 'ht.rangeserver.scanners',
             'call_back': metric_callback,
             'time_max': 90,
             'value_type': 'uint',
             'units': 'scanners',
             'slope': 'both',
             'format': '%u',
             'description': 'Outstanding scanner count',
             'groups': 'hypertable RangeServer'}
        descriptors.append(d);
        
        d = {'name': 'ht.rangeserver.cellstores',
             'call_back': metric_callback,
             'time_max': 90,
             'value_type': 'uint',
             'units': 'cellstores',
             'slope': 'both',
             'format': '%u',
             'description': 'CellStore count',
             'groups': 'hypertable RangeServer'}
        descriptors.append(d);
        
        d = {'name': 'ht.rangeserver.ranges',
             'call_back': metric_callback,
             'time_max': 90,
             'value_type': 'uint',
             'units': 'ranges',
             'slope': 'both',
             'format': '%u',
             'description': 'Range count',
             'groups': 'hypertable RangeServer'}
        descriptors.append(d);
        
        d = {'name': 'ht.rangeserver.memory.virtual',
             'call_back': metric_callback,
             'time_max': 90,
             'value_type': 'float',
             'units': 'GB',
             'slope': 'both',
             'format': '%f',
             'description': 'Virtual memory',
             'groups': 'hypertable RangeServer'}
        descriptors.append(d);
        
        d = {'name': 'ht.rangeserver.memory.resident',
             'call_back': metric_callback,
             'time_max': 90,
             'value_type': 'float',
             'units': 'GB',
             'slope': 'both',
             'format': '%f',
             'description': 'Resident memory',
             'groups': 'hypertable RangeServer'}
        descriptors.append(d);
        
        d = {'name': 'ht.rangeserver.memory.heap',
             'call_back': metric_callback,
             'time_max': 90,
             'value_type': 'float',
             'units': 'GB',
             'slope': 'both',
             'format': '%f',
             'description': 'Heap memory',
             'groups': 'hypertable RangeServer'}
        descriptors.append(d);
        
        d = {'name': 'ht.rangeserver.memory.heapSlack',
             'call_back': metric_callback,
             'time_max': 90,
             'value_type': 'float',
             'units': 'GB',
             'slope': 'both',
             'format': '%f',
             'description': 'Heap slack bytes',
             'groups': 'hypertable RangeServer'}
        descriptors.append(d);
        
        d = {'name': 'ht.rangeserver.memory.tracked',
             'call_back': metric_callback,
             'time_max': 90,
             'value_type': 'float',
             'units': 'GB',
             'slope': 'both',
             'format': '%f',
             'description': 'Tracked memory',
             'groups': 'hypertable RangeServer'}
        descriptors.append(d);
        
        d = {'name': 'ht.rangeserver.cpu.sys',
             'call_back': metric_callback,
             'time_max': 90,
             'value_type': 'uint',
             'units': '%',
             'slope': 'both',
             'format': '%u',
             'description': 'Process CPU system time',
             'groups': 'hypertable RangeServer'}
        descriptors.append(d);
        
        d = {'name': 'ht.rangeserver.cpu.user',
             'call_back': metric_callback,
             'time_max': 90,
             'value_type': 'uint',
             'units': '%',
             'slope': 'both',
             'format': '%u',
             'description': 'Process CPU user time',
             'groups': 'hypertable RangeServer'}
        descriptors.append(d);
        
        d = {'name': 'ht.rangeserver.blockCache.hitRate',
             'call_back': metric_callback,
             'time_max': 90,
             'value_type': 'uint',
             'units': '%',
             'slope': 'both',
             'format': '%u',
             'description': 'Block cache hit rate',
             'groups': 'hypertable RangeServer'}
        descriptors.append(d);
        
        d = {'name': 'ht.rangeserver.blockCache.memory',
             'call_back': metric_callback,
             'time_max': 90,
             'value_type': 'float',
             'units': 'GB',
             'slope': 'both',
             'format': '%f',
             'description': 'Block cache memory',
             'groups': 'hypertable RangeServer'}
        descriptors.append(d);
        
        d = {'name': 'ht.rangeserver.blockCache.fill',
             'call_back': metric_callback,
             'time_max': 90,
             'value_type': 'float',
             'units': 'GB',
             'slope': 'both',
             'format': '%f',
             'description': 'Block cache fill',
             'groups': 'hypertable RangeServer'}
        descriptors.append(d);
        
        d = {'name': 'ht.rangeserver.queryCache.hitRate',
             'call_back': metric_callback,
             'time_max': 90,
             'value_type': 'uint',
             'units': '%',
             'slope': 'both',
             'format': '%u',
             'description': 'Query cache hit rate',
             'groups': 'hypertable RangeServer'}
        descriptors.append(d);
        
        d = {'name': 'ht.rangeserver.queryCache.memory',
             'call_back': metric_callback,
             'time_max': 90,
             'value_type': 'float',
             'units': 'GB',
             'slope': 'both',
             'format': '%f',
             'description': 'Query cache memory',
             'groups': 'hypertable RangeServer'}
        descriptors.append(d);
        
        d = {'name': 'ht.rangeserver.queryCache.fill',
             'call_back': metric_callback,
             'time_max': 90,
             'value_type': 'float',
             'units': 'GB',
             'slope': 'both',
             'format': '%f',
             'description': 'Query cache fill',
             'groups': 'hypertable RangeServer'}
        descriptors.append(d);

    ##
    ## ThriftBroker metrics
    ##
    if 'EnableThriftBroker' in params and int(params['EnableThriftBroker']) == 1:
        d = {'name': 'ht.thriftbroker.requests',
             'call_back': metric_callback,
             'time_max': 90,
             'value_type': 'float',
             'units': 'requests/s',
             'slope': 'both',
             'format': '%f',
             'description': 'Request rate',
             'groups': 'hypertable ThriftBroker'}
        descriptors.append(d);
        
        d = {'name': 'ht.thriftbroker.errors',
             'call_back': metric_callback,
             'time_max': 90,
             'value_type': 'float',
             'units': 'errors/s',
             'slope': 'both',
             'format': '%f',
             'description': 'Error rate',
             'groups': 'hypertable ThriftBroker'}
        descriptors.append(d);
        
        d = {'name': 'ht.thriftbroker.connections',
             'call_back': metric_callback,
             'time_max': 90,
             'value_type': 'uint',
             'units': 'connections',
             'slope': 'both',
             'format': '%u',
             'description': 'Active connection count',
             'groups': 'hypertable ThriftBroker'}
        descriptors.append(d);
        
        d = {'name': 'ht.thriftbroker.cpu.sys',
             'call_back': metric_callback,
             'time_max': 90,
             'value_type': 'uint',
             'units': '%',
             'slope': 'both',
             'format': '%u',
             'description': 'Process CPU system time',
             'groups': 'hypertable ThriftBroker'}
        descriptors.append(d);
        
        d = {'name': 'ht.thriftbroker.cpu.user',
             'call_back': metric_callback,
             'time_max': 90,
             'value_type': 'uint',
             'units': '%',
             'slope': 'both',
             'format': '%u',
             'description': 'Process CPU user time',
             'groups': 'hypertable ThriftBroker'}
        descriptors.append(d);
        
        d = {'name': 'ht.thriftbroker.memory.virtual',
             'call_back': metric_callback,
             'time_max': 90,
             'value_type': 'float',
             'units': 'GB',
             'slope': 'both',
             'format': '%f',
             'description': 'Virtual memory',
             'groups': 'hypertable ThriftBroker'}
        descriptors.append(d);
        
        d = {'name': 'ht.thriftbroker.memory.resident',
             'call_back': metric_callback,
             'time_max': 90,
             'value_type': 'float',
             'units': 'GB',
             'slope': 'both',
             'format': '%f',
             'description': 'Resident memory',
             'groups': 'hypertable ThriftBroker'}
        descriptors.append(d);
        
        d = {'name': 'ht.thriftbroker.memory.heap',
             'call_back': metric_callback,
             'time_max': 90,
             'value_type': 'float',
             'units': 'GB',
             'slope': 'both',
             'format': '%f',
             'description': 'Heap memory',
             'groups': 'hypertable ThriftBroker'}
        descriptors.append(d);
        
        d = {'name': 'ht.thriftbroker.memory.heapSlack',
             'call_back': metric_callback,
             'time_max': 90,
             'value_type': 'float',
             'units': 'GB',
             'slope': 'both',
             'format': '%f',
             'description': 'Heap slack bytes',
             'groups': 'hypertable ThriftBroker'}
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
    params = { 'FSBroker': 'hadoop',
               'EnableFSBroker': '1',
               'EnableHyperspace': '1',
               'EnableMaster': '1',
               'EnableRangeServer': '1',
               'EnableThriftBroker': '1' }
    metric_init(params)
    while True:
        value = metric_callback(sys.argv[1])
        print sys.argv[1], "=", value
        time.sleep(30)
