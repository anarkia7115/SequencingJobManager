#!/usr/bin/env python
import json
import urllib2
import requests
import config
from datetime import datetime
from extractor import PidExtractor
import subprocess
import sys

class StatusChecker():

    def __init__(self, finishSignal):
        self.finishSignal = finishSignal
        return

    def check(self):
        signal = self.finishSignal

        # init extractor
        pe = PidExtractor()

        if(type(signal) is str):
            # if is string
            if (signal.startswith('hdfs')):
                status = self.checkHdfsFile(signal)
            elif(signal.startswith('job_')):
                status = self.checkHadoopJob(signal)
            else:
                print >> sys.stderr, "unknown signal type"
                sys.exit(-1)
        elif(type(signal) is subprocess.Popen):
            # if is process handle
            status = self.checkProcess(signal)
        else:
            # unknown type
            status = self.checkLocalFile(signal)

        return status

    #TODO check return code
    def checkProcess(self, proc):
        if (proc.poll() is None):
            return False
        else:
            return True

    #TODO
    def checkHdfsFile(self, filePath):
        print "hdfsFile: {0} exists!".format(filePath)
        return True

    #TODO
    def checkLocalFile(self, filePath):
        #print "localFile: {0} exists!".format(filePath)
        print >> sys.stderr, "check local file signal not implemented"
        print >> sys.stderr, "signal info: {}".format(filePath)
        sys.exit(-1)

    #TODO
    def checkHadoopJob(self, jobID):
        print "{0} finished!".format(jobID)
        return True

class RequestSender():
    def __init__(self, processID, accession):

        self.accession = accession
        self.processID = processID
        return
    
    def send(self, returnJson, path):
        returnJson['processId'] = self.processID
        returnJson['accession'] = self.accession
        returnJson['timeType'] = datetime.strftime(datetime.now(), '%Y-%m-%d %H:%M:%S')

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
