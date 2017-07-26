from smashbox.utilities import *
import pickle
import struct
import socket
import sys
import json
import urllib2

#--keep-going -a --loop=40 --quiet C:\Users\ydelahoz\workspace\smashbox\lib\
class HeadRequest(urllib2.Request):
      def get_method(self):
        return "HEAD"

class StateMonitor:
    # Test information  -->  {'test_name': string, 'timestamp': string, 'hostname': string, 'oc_client version': string, 'eos_version': string, 'platform': string, 'passed': int, 'failed': int, 'subtests': [self.test_results]}
    # Subtests information --> {'subtest_id':subtest_id , 'parameters': params, 'errors': [], 'success': [], 'operations':[], 'qos_metrics': [], 'total_errors':0, 'total_success':0}

    def __init__(self,manager):
        self.testname = None
        self.worker_results = manager.Queue()
        self.test_results = dict()


    def initialize(self,args, config):
        """
        Initialize the subtest state with initial information
        """
        self.testname = (str(args.test_target).split("test_"))[-1].split(".")[0]
        self.subtest_id = config.__dict__["subtest_id"]
        self.runid = config.__dict__["runid"]
        self.nsubtests =  config.__dict__["nsubtests"]
        self.smashdir = config.smashdir

        self.parameters = []
        param = {}
        for c in config.__dict__:
            if c.startswith(self.testname + "_"):
                param[str((c.replace(self.testname + "_", "")))] = config[c]
                self.parameters.append(param)
                print c, config[c]

        self.test_results = {'subtest_id':self.subtest_id,'parameters':[],'parameters_text':"",'errors': [],'errors_text': "",'success': [], 'operations':[], 'qos_metrics': [], 'total_errors':0,'total_success':0}


    def join_worker_results(self):
        """
        Join partial worker tests results information. The partial results are stored in queue (FIFO order)
        """
        partial_results = self.worker_results.get()
        if(partial_results[0]): self.test_results['errors'].append(partial_results[0])
        if(partial_results[1]): self.test_results['success'].append(partial_results[1])
        self.test_results['operations'].append(partial_results[2])

        if(len(partial_results[3])!=0): # normally only one worker has the qos_metrics
            self.test_results['qos_metrics'].append(partial_results[3])

        self.test_results['total_errors']+=len(self.test_results['errors'])
        self.test_results['total_success']+=len(self.test_results['success'])

        if (len( self.parameters) > self.subtest_id and len(self.test_results['parameters'])==0): # add subtests parameters
            self.test_results['parameters'].append(self.parameters[self.subtest_id])
            self.test_results['parameters_text'] = str(self.parameters[self.subtest_id])



    def subtestcase_finish(self):
        """
        Update the dictionary with the last results and publish at the end
        """
        json_results = self.get_json_results()

        if(self.subtest_id==0): # first iteration
            if (self.subtest_id>=self.nsubtests-1): # no subtests
                self.test_finish(json_results)
            self.save_json(json_results)
        else:
            data = self.get_json()
            data['subtests'].append(self.test_results)

            if(self.subtest_id>=self.nsubtests-1):
                self.test_finish(data)
            else:
                self.save_json(data)

    def save_json(self,data):
        """
        Save json results in a file to preserve the state between main program calls 
        """
        data = json.dumps(data)
        time = str(self.runid).split(" ")
        dir = self.smashdir + "/" + "json_results" + "/" + str(self.testname) + "/" + time[0]

        if not os.path.exists(dir):
            os.makedirs(dir)

        filename = dir + "/" +  time[1].replace(":","-") + ".txt"

        try:
            fd = open(filename, 'w')
            fd.write(data)
            fd.close()
        except:
            print 'ERROR writing json results', filename
            pass

    def get_json(self):
        """
        Restore json results previously saved in the file 
        """
        time = str(self.runid).split(" ")
        dir = self.smashdir + "/" + "json_results" + "/" + str(self.testname) + "/" + time[0]

        filename = dir + "/" +  time[1].replace(":","-") + ".txt"

        returndata = {}
        try:
            fd = open(filename, 'r')
            text = fd.read()
            fd.close()
            returndata = json.loads(text)
        except:
            print 'ERROR loading json results:', filename
        return returndata

    def test_finish(self,json_results):
        """
        Compute total passed subtests and publish results
        """
        # compute passed subtests
        failed = 0
        for subtest in json_results['subtests']:
            if(subtest['total_errors']>=1): # A subtest is considered failed with one or more errors
                failed += 1

        json_results['failed']= failed
        json_results['passed']=len(json_results['subtests']) - failed

        print self.publish_results(json.dumps(json_results))

    def get_json_results(self):
        """
        Saved results in a dictionary to be able to convert them in a json format
        """
        if (self.test_results['errors']): self.test_results['errors_text'] = str(self.test_results['errors'])
        json_result = {'test_name': self.testname, 'timestamp': self.runid.replace(" ","T")+"Z", 'hostname': socket.gethostname(),
                                  'oc_client version': str(str(ocsync_version())[1:-1].replace(",",".")), 'eos_version': "beryl_aquamarine",
                                  'platform': platform.system() + platform.release(), 'subtests': [self.test_results]}
        return json_result


    def publish_results(self,data):
        """
        Publish results in a kibana dashboard
        """
        url_parameters = {'cluster': "http://smashbox-monitoring:9200/smashbox-tests",
                          'index': self.testname,
                          'index_period':self.runid.replace(" ","T")+"Z", }
        url = "%(cluster)s/%(index)s/%(index)s-%(index_period)s" % url_parameters
        url = url
        headers = {'content-type': 'application/json'}
        try:
            url = urllib2.Request(url, headers=headers, data=data)
            req = urllib2.urlopen(url)
            response = req.read()
        except Exception as e:
            response = "Error:  {}".format(str(e))
        return response





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
