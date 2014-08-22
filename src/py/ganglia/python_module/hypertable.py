import errno
import json
import socket
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
    sock.bind(("127.0.0.1", LISTEN_PORT))
    sock.setblocking(0);

    d1 = {'name': 'hypertable.rangeserver.Scans',
        'call_back': metric_callback,
        'time_max': 90,
        'value_type': 'int',
        'units': 'scans/s',
        'slope': 'both',
        'format': '%d',
        'description': 'Scans per second',
        'groups': 'hypertable,rangeserver'}
    values['hypertable.rangeserver.Scans'] = 0

    descriptors = [d1]
    return descriptors

def metric_cleanup():
    '''Clean up the metric module.'''
    global sock
    sock.close()


#This code is for debugging and unit testing
if __name__ == '__main__':
    params = {'RandomMax': '500',
        'ConstantValue': '322'}
    metric_init(params)
    for d in descriptors:
        v = d['call_back'](d['name'])
        print 'value for %s is %u' % (d['name'],  v)
    while True:
        time.sleep(30)
        scans = metric_callback('hypertable.rangeserver.Scans')
        print 'value for \'hypertable.rangeserver.Scans\' is %d' % scans
