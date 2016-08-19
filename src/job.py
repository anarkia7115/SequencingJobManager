#!/usr/bin/env python

from BaseHTTPServer import BaseHTTPRequestHandler
from multiprocessing import Process
import cgi
import json

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
        steps = ['distribution', 'alignment', 'variation', 'packaging', 'qc']

        # start steps
        from step import StepManager

        sm = StepManager(self.processID, steps, self.ag, self.rs)
        sm.wait()
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
        returnJson['result'] = True

        # send request signal
        self.rs.send(returnJson, "/gcbi/ch/inner/cluster/clusterEnd")

        return

class RequestMonitor(BaseHTTPRequestHandler):
    # monitor requests
    def __init__(self, server=None, port=None):
        if server is None:
            self.localServer = "192.168.2.156"
        else:
            self.localServer = server

        if port is None:
            self.localPort = 8080
        else:
            self.localPort = port

        self.currentJobID = 0

        return

    def loop(self):
        from BaseHTTPServer import HTTPServer

        server = HTTPServer((self.localServer, self.localPort), PostHandler)
        print 'Starting server, use <Ctrl-C> to stop'
        print "server: {} port: {}".format(self.localServer, self.localPort)
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
