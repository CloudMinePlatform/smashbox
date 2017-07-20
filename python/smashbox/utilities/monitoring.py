from smashbox.utilities import *
import pickle
import struct
import socket
import sys
import json
import urllib2


class HeadRequest(urllib2.Request):
      def get_method(self):
        return "HEAD"

class StateMonitor:
    # {'subtest_id':subtest_id , 'parameters': params, 'errors': [], 'success': [], 'operations':[], 'qos_metrics': []}

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

        params = []
        for c in config.__dict__:
            if c.startswith(self.testname + "_"):
                params.append(str((c.replace(self.testname + "_", ""), config[c])))
                print c, config[c]

        self.test_results = {'subtest_id':self.subtest_id,'parameters':params,'errors': [], 'success': [], 'operations':[], 'qos_metrics': []}


    def join_worker_results(self):
        """
        Join partial worker tests results information. The partial results are stored in queue (FIFO order)
        """
        partial_results = self.worker_results.get()
        self.test_results['errors'].append(partial_results[0])
        self.test_results['success'].append(partial_results[1])
        self.test_results['operations'].append(partial_results[2])
        self.test_results['qos_metrics'].append(partial_results[3])


    def subtestcase_finish(self):
        """
        Update the dictionary with the last results and publish
        """
        json_results = self.get_json_results()
        if(self.subtest_id==0): # first iteration
            json_result = json.dumps(json_results)
            if (self.subtest_id>=self.nsubtests-1): # no subtests
                self.test_finish(json_result)
            self.save_json(json_result)
        else:
            data = self.get_json()
            data['subtests'].append(self.test_results)
            json_results = json.dumps(data)
            if(self.subtest_id>=self.nsubtests-1):
                self.test_finish(json_results)
            else:
                self.save_json(json_results)

    def save_json(self,data):
        dir = self.smashdir + "/" + "json_results" + "/" + str(self.testname)

        if not os.path.exists(dir):
            os.makedirs(dir)

        filename =  dir   + "/" +  str(self.runid) + ".txt"
        try:
            fd = open(filename, 'w')
            fd.write(data)
            fd.close()
        except:
            print 'ERROR writing json results', filename
            pass

    def get_json(self):
        dir = self.smashdir + "/" + "json_results" + "/" + str(self.testname)
        filename = dir + "/" + str(self.runid) + ".txt"

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
        print self.publish_results(json_results)

    def get_json_results(self):
        json_result = {'test_name': self.testname, 'timestamp': self.runid, 'hostname': socket.gethostname(),
                                  'oc_client version': str(ocsync_version()), 'eos_version': "beryl_aquamarine",
                                  'platform': platform.system() + platform.release(), 'subtests': [self.test_results]}
        return json_result


    def publish_results(self,data):
        """
        Update the dictionary with the last results and publish
        """
        url_parameters = {'cluster': "http://smashbox-monitoring:9200/smashbox-tests",
                          'index': self.testname,
                          'index_period':self.runid, }
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

    #
    # def publish_results(self, data):
    #     url_parameters = {'cluster': "http://smashbox-monitoring:9200",
    #                       'index': self.testname,
    #                       'index_period':self.runid, }
    #     type = "/message"
    #     url = "%(cluster)s/%(index)s/%(index)s-%(index_period)s" % url_parameters
    #
    #     if(self.exists_test(url + type)):# check if there is an entry with the test run id
    #         print("The index already exitsts in the database")
    #         data = {'script': {'lang': "painless", 'inline': "ctx._source.subtests.add(params.subtest)", 'params' : {'subtest' : "sadas" }}}
    #         print self.push_to_monitoring(json.dumps(data), url + type + "/" + "_update" )
    #     else: # create a new index for this runid
    #         print self.push_to_monitoring(data, url + type)
    #
    # def exists_test(self, url):
    #     try:
    #         response = urllib2.urlopen(HeadRequest(url)).geturl()
    #     except Exception as e:
    #         response = "Error:  {}".format(str(e))
    #     return response == url








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
