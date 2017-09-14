from smashbox.utilities import *
import pickle
import struct
import socket
import sys
import requests
import json

#--keep-going -a --loop=40 --quiet C:\Users\ydelahoz\workspace\smashbox\lib\
class StateMonitor:

    def __init__(self,manager):
        self.testname = None
        self.worker_results = manager.Queue()
        self.test_results = dict()


    def initialize(self,args, config):
        """
        Initialize the test state with initial information
        """
        self.testname = (str(args.test_target).split("test_"))[-1].split(".")[0]
        self.subtest_id = config.__dict__["subtest_id"]
        self.runid = config.__dict__["runid"]
        self.nsubtests =  config.__dict__["nsubtests"]
        self.smashdir = config.smashdir

        # extract test parameters
        self.parameters = []
        param = {}
        for c in config.__dict__:
            if c.startswith(self.testname + "_"):
                param[str((c.replace(self.testname + "_", "")))] = config[c]
                self.parameters.append(param)
                print c, config[c]

        # initialize json to be sent for monitoring
        self.test_results = {"activity": "smashbox-regression", 'test_name': self.testname, 'hostname': socket.gethostname(),
                             'oc_client_version': str(str(ocsync_version())[1:-1].replace(",",".")),'eos_version': "beryl_aquamarine",'platform': platform.system() + platform.release(),
                             'subtest_id':self.subtest_id,'parameters':self.parameters,'parameters_text':str(self.parameters),'errors': [],'errors_text': "",'success': [],
                             'total_errors':0,'total_success':0, 'qos_metrics': [],'passed': 0,'failed': 0 }


    def join_worker_results(self):
        """
        Join partial worker tests results information. The partial results are stored in queue (FIFO order)
        """
        partial_results = self.worker_results.get()
        if(partial_results[0]): self.test_results['errors'].append(partial_results[0])
        if(partial_results[1]): self.test_results['success'].append(partial_results[1])

        if(len(partial_results[3])!=0): # normally only one worker has the qos_metrics
            self.test_results['qos_metrics'].append(partial_results[3])

        self.test_results['total_errors']+=len(self.test_results['errors'])
        self.test_results['total_success']+=len(self.test_results['success'])


    def test_finish(self):
        """"
        Check if the test has passed and publish results
        """
        if(self.test_results['total_errors']>=1): # A subtest is considered failed with one or more errors
            self.test_results['passed'] = 0
            self.test_results['failed'] = 1
        else:
            self.test_results['failed'] = 0
            self.test_results['passed'] = 1

        json_results = self.get_json_results()

        if config.__dict__["kmonit"]==True:
            self.send_and_check(json_results)

        if config.__dict__["gmonit"]==True and self.testname=="nplusone" and len(self.test_results['qos_metrics'][0][0])>0:
            tuples = ([])
            qos_metrics = self.test_results['qos_metrics'][0][0]
            tuples.append(("cernbox.cboxsls.nplusone." + self.test_results['platform'] + ".nfiles", (self.runid, qos_metrics['nfiles'])))
            tuples.append(("cernbox.cboxsls.nplusone." + self.test_results['platform'] + ".transfer_rate", (self.runid,  qos_metrics['transfer_rate'])))
            tuples.append(("cernbox.cboxsls.nplusone." + self.test_results['platform'] + ".worker0.synced_files", qos_metrics['synced_files']))
            self.push_to_monitoring(json_results)


    def get_json_results(self):
        """
        Saved results in a dictionary to be able to convert them in a json format
        """
        if (self.test_results['errors']): self.test_results['errors_text'] = str(self.test_results['errors'])
        json_result = [{'producer':"cernbox", 'type':"ops", 'hostname': socket.gethostname(), 'timestamp':int(round(self.runid* 1000)), "data":self.test_results}]
        return json_result

    # --------------------------------------------------------------------------------
    # Send metrics to kibana-monit central service
    #   Report tests results and statistics to the kibana monitoring dashboard
    # --------------------------------------------------------------------------------

    def send(self,document):
        return requests.post('http://monit-metrics:10012/', data=json.dumps(document),
                             headers={"Content-Type": "application/json; charset=UTF-8"})

    def send_and_check(self,document, should_fail=False):
        response = self.send(document)
        assert (
        (response.status_code in [200]) != should_fail), 'With document: {0}. Status code: {1}. Message: {2}'.format(
            document, response.status_code, response.text)


    # --------------------------------------------------------------------------------
    # Send metrics to Grafana
    #   Report tests results and statistics to the Grafana monitoring dashboard
    # --------------------------------------------------------------------------------

    # simple monitoring to grafana (disabled if not set in config)
    def push_to_monitoring(self,tuples,timestamp=None):

        if not timestamp:
            timestamp = time.time()
        logger.info("publishing logs to grafana %s" % timestamp)
        self.send_metric(tuples)

    def send_metric(self,tuples):

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





