#!/usr/bin/env python
import json
import urllib2
import requests
from datetime import datetime

class StatusChecker():

    def __init__(self, finishSignal):
        self.finishSignal = finishSignal
        return

    def check(self):
        signal = self.finishSignal

        if (signal.startswith('hdfs')):
            status = self.checkHdfsFile(signal)
        elif (signal.startswith('job_')):
            status = self.checkHadoopJob(signal)
        else:
            status = self.checkLocalFile(signal)

        return status
    #TODO
    def checkHdfsFile(self, filePath):
        print "hdfsFile: {} exists!".format(filePath)
        return True

    #TODO
    def checkLocalFile(self, filePath):
        print "localFile: {} exists!".format(filePath)
        return True

    #TODO
    def checkHadoopJob(self, jobID):
        print "{} finished!".format(jobID)
        return True

class RequestSender():
    def __init__(self, processID, accession):
        self.requestHost = "192.168.2.156"
        self.requestPort = 8081

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

        url = "http://{host}:{port}{path}".format(host=self.requestHost,
                                                  port=self.requestPort,
                                                  path=path)

        r = requests.post(url, json=dictJson)
        print "[RequestSender] {} posted to {}".format(dictJson, url)
        return


class SignalSender():
    def __init__(self):
        return
