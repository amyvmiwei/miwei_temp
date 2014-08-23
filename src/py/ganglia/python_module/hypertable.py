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
            buf = sock.recv(1024)
            metrics = json.loads(buf)
            for key, value in metrics.iteritems():
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

    ## RangeServer metrics

    d = {'name': 'hypertable.rangeserver.scans',
        'call_back': metric_callback,
        'time_max': 90,
        'value_type': 'float',
        'units': 'per second',
        'slope': 'both',
        'format': '%f',
        'description': 'Scans per second',
        'groups': 'hypertable,rangeserver'}
    values['hypertable.rangeserver.scans'] = 0
    descriptors.append(d);

    d = {'name': 'hypertable.rangeserver.updates',
        'call_back': metric_callback,
        'time_max': 90,
        'value_type': 'float',
        'units': 'per second',
        'slope': 'both',
        'format': '%f',
        'description': 'Updates per second',
        'groups': 'hypertable,rangeserver'}
    values['hypertable.rangeserver.updates'] = 0
    descriptors.append(d);

    d = {'name': 'hypertable.rangeserver.cellsRead',
        'call_back': metric_callback,
        'time_max': 90,
        'value_type': 'float',
        'units': 'per second',
        'slope': 'both',
        'format': '%f',
        'description': 'Cells read per second',
        'groups': 'hypertable,rangeserver'}
    values['hypertable.rangeserver.cellsRead'] = 0
    descriptors.append(d);

    d = {'name': 'hypertable.rangeserver.cellsWritten',
        'call_back': metric_callback,
        'time_max': 90,
        'value_type': 'float',
        'units': 'per second',
        'slope': 'both',
        'format': '%f',
        'description': 'Cells written per second',
        'groups': 'hypertable,rangeserver'}
    values['hypertable.rangeserver.cellsWritten'] = 0
    descriptors.append(d);

    d = {'name': 'hypertable.rangeserver.scanners',
        'call_back': metric_callback,
        'time_max': 90,
        'value_type': 'uint',
        'units': 'outstanding scanners',
        'slope': 'both',
        'format': '%u',
        'description': 'Outstanding scanners',
        'groups': 'hypertable,rangeserver'}
    values['hypertable.rangeserver.scanners'] = 0
    descriptors.append(d);

    d = {'name': 'hypertable.rangeserver.cellstores',
        'call_back': metric_callback,
        'time_max': 90,
        'value_type': 'uint',
        'units': 'count',
        'slope': 'both',
        'format': '%u',
        'description': 'CellStore count',
        'groups': 'hypertable,rangeserver'}
    values['hypertable.rangeserver.cellstores'] = 0
    descriptors.append(d);

    d = {'name': 'hypertable.rangeserver.ranges',
        'call_back': metric_callback,
        'time_max': 90,
        'value_type': 'uint',
        'units': 'count',
        'slope': 'both',
        'format': '%u',
        'description': 'Range count',
        'groups': 'hypertable,rangeserver'}
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
        'groups': 'hypertable,rangeserver'}
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
        'groups': 'hypertable,rangeserver'}
    values['hypertable.rangeserver.memory.resident'] = 0
    descriptors.append(d);

    d = {'name': 'hypertable.rangeserver.memory.pageFaults',
        'call_back': metric_callback,
        'time_max': 90,
        'value_type': 'uint',
        'units': 'page faults',
        'slope': 'both',
        'format': '%u',
        'description': 'Page faults (major)',
        'groups': 'hypertable,rangeserver'}
    values['hypertable.rangeserver.memory.pageFaults'] = 0
    descriptors.append(d);

    d = {'name': 'hypertable.rangeserver.memory.heap',
        'call_back': metric_callback,
        'time_max': 90,
        'value_type': 'float',
        'units': 'GB',
        'slope': 'both',
        'format': '%f',
        'description': 'Heap memory',
        'groups': 'hypertable,rangeserver'}
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
        'groups': 'hypertable,rangeserver'}
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
        'groups': 'hypertable,rangeserver'}
    values['hypertable.rangeserver.memory.tracked'] = 0
    descriptors.append(d);

    d = {'name': 'hypertable.rangeserver.cpu.user',
        'call_back': metric_callback,
        'time_max': 90,
        'value_type': 'uint',
        'units': 'milliseconds',
        'slope': 'both',
        'format': '%u',
        'description': 'Process CPU user time',
        'groups': 'hypertable,rangeserver'}
    values['hypertable.rangeserver.cpu.user'] = 0
    descriptors.append(d);

    d = {'name': 'hypertable.rangeserver.cpu.sys',
        'call_back': metric_callback,
        'time_max': 90,
        'value_type': 'uint',
        'units': 'milliseconds',
        'slope': 'both',
        'format': '%u',
        'description': 'Process CPU system time',
        'groups': 'hypertable,rangeserver'}
    values['hypertable.rangeserver.cpu.sys'] = 0
    descriptors.append(d);

    d = {'name': 'hypertable.rangeserver.blockCache.hitRate',
        'call_back': metric_callback,
        'time_max': 90,
        'value_type': 'float',
        'units': '%',
        'slope': 'both',
        'format': '%f',
        'description': 'Block cache hit rate',
        'groups': 'hypertable,rangeserver'}
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
        'groups': 'hypertable,rangeserver'}
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
        'groups': 'hypertable,rangeserver'}
    values['hypertable.rangeserver.blockCache.fill'] = 0
    descriptors.append(d);

    d = {'name': 'hypertable.rangeserver.queryCache.hitRate',
        'call_back': metric_callback,
        'time_max': 90,
        'value_type': 'float',
        'units': 'percentage',
        'slope': 'both',
        'format': '%f',
        'description': 'Query cache hit rate',
        'groups': 'hypertable,rangeserver'}
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
        'groups': 'hypertable,rangeserver'}
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
        'groups': 'hypertable,rangeserver'}
    values['hypertable.rangeserver.queryCache.fill'] = 0
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
    params = { }
    metric_init(params)
    while True:
        scans = metric_callback(sys.argv[1])
        print sys.argv[1], "=", scans
        time.sleep(30)
