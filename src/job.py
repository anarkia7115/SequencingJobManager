#!/usr/bin/env python

from BaseHTTPServer import BaseHTTPRequestHandler
from multiprocessing import Process
import cgi
import json
import config

class JobManager():

    def __init__(self, dataJson):
        self.currentStepID = 0
        self.clusterID = 1

        # init argument generator
        from args import ArgsGenerator
        self.ag = ArgsGenerator(dataJson)

        self.processID =  self.ag.getProcessID()
        self.accession =  self.ag.getAccession()

        # create RequestSender
        from status import RequestSender
        self.rs = RequestSender(self.processID, self.accession)

        return

    def run(self):
        steps = ['distribution', 'align', 'variation', 'pkgResult', 'qa']

        # start steps
        from step import StepManager

        sm = StepManager(self.processID, steps, self.ag, self.rs)
        self.withError = sm.wait()
        self.cleanUp()

    """ 
    get step information with a json containing(used):
        1. accession
        2. sample paths
        3. result path
    """
    def cleanUp(self):
        print "cleaning up Job Manager"

        # create request signal
        returnJson = dict()
        returnJson['clusterId'] = self.clusterID

        if(self.withError):
            returnJson['result'] = False
        else:
            returnJson['result'] = True

        # send request signal
        self.rs.send(returnJson, "/gcbi/ch/inner/cluster/clusterEnd")

        return

class RequestMonitor(BaseHTTPRequestHandler):
    # monitor requests
    def __init__(self):

        return

    def loop(self):
        from BaseHTTPServer import HTTPServer

        server = HTTPServer((config.host['local_server_host'],
                             config.host['local_server_port']), PostHandler)
        print 'Starting server, use <Ctrl-C> to stop'
        print "server: {0} port: {1}".format(
                                    config.host['local_server_host'], 
                                    config.host['local_server_port'])
        server.serve_forever()
        return

class PostHandler(BaseHTTPRequestHandler):
    # monitor requests

    def do_POST(self):

        # get data string
        self.data_string=self.rfile.read(int(self.headers['Content-Length']))
        print "in post"

        # send request
        self.send_response(200)
        self.end_headers()

        # string to json
        self.data_json = json.loads(self.data_string)

        # start new job
        self.startNewJob()
        return

    def startNewJob(self):

        print "----json start----"
        print self.data_json
        print "----json end----"
        jm = JobManager(self.data_json)

        # run background
        p = Process(target=jm.run, args=())
        p.start()
        return 

def main():
    dataString = """ { "processId":"0", "resultPath": "/online/GCBI/result", "sampleList": [ { "accession": "GCS1001", "fastqFileList": [ { "mateFile1": { "filename": "line2_R1.fastq.gz", "key": "/Users/Yvonne/Downloads/ch-gcbi/GCS1001/line2_R1.fastq.gz", "protocol": "file" }, "mateFile2": { "filename": "line2_R2.fastq.gz", "key": "/Users/Yvonne/Downloads/ch-gcbi/GCS1001/line2_R2.fastq.gz", "protocol": "file" } }, { "mateFile1": { "filename": "line1_R1.fastq.gz", "key": "/Users/Yvonne/Downloads/ch-gcbi/GCS1001/line1_R1.fastq.gz", "protocol": "file" }, "mateFile2": { "filename": "line1_R2.fastq.gz", "key": "/Users/Yvonne/Downloads/ch-gcbi/GCS1001/line1_R2.fastq.gz", "protocol": "file" } } ], "genomeVersion": "hg38", "species": "hsa" } ], "sampleType": "WSG" } """
    dataJson = json.loads(dataString)
    jm = JobManager(0, dataJson)
    jm.run()
    #print dataJson

	

if __name__ == "__main__":
    #main()
    rm = RequestMonitor()
    rm.loop()
