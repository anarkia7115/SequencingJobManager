#!/usr/bin/env python
import json
import urllib2
import requests
import config
from datetime import datetime
import subprocess
import sys
import yarn_api_client as yarn

class StatusChecker():

    def __init__(self, finishSignal):
        self.finishSignal = finishSignal
        self.rm = yarn.ResourceManager(config.host['rmhost'])
        self.success = False
        return

    def check(self):
        signal = self.finishSignal

        if(type(signal) is str):
            # if is string
            if (signal.startswith('hdfs')):
                status = self.checkHdfsFile(signal)
            elif(signal.startswith('job_')):
                status = self.checkHadoopJob(signal)
            elif(signal == "testTrue"):
                self.success = True
                status = True
            else:
                print >> sys.stderr, "unknown signal type: {0}".format(signal)
                self.success = False 
                status = True

        elif(type(signal) is subprocess.Popen):
            # if is process handle
            status = self.checkProcess(signal)
        else:
            # unknown type
            status = self.checkLocalFile(signal)

        return status

    def checkProcess(self, proc):
        if (proc.poll() is None):
            return False
        else:
            if (proc.returncode == 0):
                stdout = proc.stdout.read()
                print("[stdout] {0}".format(stdout))
                print >> sys.stderr, "---self.success set to true---"
                self.success = True
            else:
                stdout = proc.stdout.read()
                print("[stdout] {0}".format(stdout))
                print >> sys.stderr, "[ERROR] process return non-zero code: {0}".format(proc.returncode)
                self.success = False
            return True

    #TODO
    def checkHdfsFile(self, filePath):
        print "hdfsFile: {0} exists!".format(filePath)
        return True

    #TODO
    def checkLocalFile(self, filePath):
        #print "localFile: {0} exists!".format(filePath)
        print >> sys.stderr, "check local file signal not implemented"
        print >> sys.stderr, "signal info: {0}".format(filePath)
        sys.exit(-1)

    def isSuccess(self):
        return self.success

    def checkHadoopJob(self, jobID):

        # if jobID not parsable, finish check loop and make job fail
        try:
            app = self.rm.cluster_application(jobID)
        except yarn.base.APIError:
            self.success = False
            return True

        state = app.data['app']['state']
        if (state == "FINISHED"):
            finalStatus = app.data['app']['finalStatus']
            if (finalStatus == "SUCCEEDED"):
                self.success = True
            else:
                print >> sys.stderr, "[ERROR] hadoop finished non-successful status: {0}".format(finalStatus)
                self.success = False
            return True
        else:
            print("waiting for hadoop job: {0} to finish".format(jobID))
            return False
        #print "{0} finished!".format(jobID)
        return True

class RequestSender():
    def __init__(self, processID, accession):

        self.accession = accession
        self.processID = processID
        return
    
    def send(self, returnJson, path):
        returnJson['processId'] = self.processID
        returnJson['accession'] = self.accession

        self.post(returnJson, path)
        return

    def post(self, dictJson, path):

        url = "http://{host}{path}".format(host=config.host['request'], path=path)

        r = requests.post(url, json=dictJson)
        print "[RequestSender] {0} posted to {1}".format(dictJson, url)
        return


class SignalSender():
    def __init__(self):
        return
