from smashbox.utilities import *
import pickle
import struct
import socket
import sys
from multiprocessing import Queue

class StateMonitor:
    # {'subtest_id':subtest_id , 'parameters': params, 'errors': [], 'success': [], 'operations':[], 'qos_metrics': []}

    def __init__(self):
        self.testname = None
        self.worker_results = Queue()
        self.test_results = dict()

    def initialize(self,args, config):
        """
        Initialize the test state with initial information
        """
        self.testname = (str(args.test_target).split("test_"))[-1].split(".")[0]
        subtest_id = config.__dict__["loop"]

        params = []
        for c in config.__dict__:
            if c.startswith(self.testname + "_"):
                params.append((c.replace(self.testname + "_", ""), config[c]))
                print c, config[c]

        self.test_results = {'subtest_id':subtest_id,'parameters':params,'errors': [], 'success': [], 'operations':[], 'qos_metrics': []}


    def join_worker_results(self):
        """
        Join partial worker tests results information
        """
        self.test_results['errors'].append(self.worker_results.get())
        self.test_results['success'].append(self.worker_results.get())
        self.test_results['operations'].append(self.worker_results.get())
        self.test_results['qos_metrics'].append(self.worker_results.get())

    def testcase_stop(self):
        """
        Update the dictionary with the last results and publish
        """
        self.publish_json()


    def publish_json(self):

        print "+ test_name: " + self.testname
        print "+ timestamp: " + datetime.datetime.now().strftime("%y%m%d-%H%M%S")
        print "+ oc_client version: " +  str(ocsync_version())
        print "+ eos_version: beryl_aquamarine"
        print "+ platform: " +  platform.system() + platform.release()

        print "-----------------------------------------------------------------"

        print("+ subtest_id: " + str(self.test_results['subtest_id']))
        for entry in self.test_results['parameters']:
            print "     + " + str(entry[0])  + " :" + str(entry[1])

        print "     + success: " + str(self.test_results['success'])
        print "     + errors: " + str(self.test_results['errors'])
        print "     + operations: " + str(self.test_results['operations'])
        print "     + qos_metrics: " + str(self.test_results['qos_metrics'])


# simple monitoring to grafana (disabled if not set in config)

def push_to_monitoring(tuples,timestamp=None):

    if not timestamp:
        timestamp = time.time()
    logger.info("publishing logs to grafana %s" % timestamp)
    send_metric(tuples)


#--------------------------------------------------------------------------------
# Send metrics to Grafana
#   Report tests results and statistics to the Grafana monitoring dashboard
#--------------------------------------------------------------------------------
def send_metric(tuples):

    monitoring_host=config.get('monitoring_host',"filer-carbon.cern.ch")
    monitoring_port=config.get('monitoring_port',2003)

    payload = pickle.dumps(tuples, protocol=2)
    header = struct.pack('!L', len(payload))
    message = header + payload
    logger.info("message %s" % message)
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    print 'Socket created'

    # Bind socket to local host and port
    try:
        s.connect((monitoring_host, monitoring_port))
    except socket.error as msg:
        logger.info('Connect failed. Error Code : ' + str(msg[0]) + ' Message : ' + msg[1])
        sys.exit()

    s.sendall(message)
    logger.info("publishing logs to grafana")
    s.close()
