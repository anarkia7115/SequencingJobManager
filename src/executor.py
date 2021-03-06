#!/usr/bin/env python

from extractor import HadoopExtractor 
import sys
import subprocess

class CommandLineExecutor():
    def __init__(self, args):
        self.args = args
        return

    def run(self):
        # init process runner
        try:
            proc = subprocess.Popen(self.args, stdout=subprocess.PIPE)
            # test
            #return "testTrue"
            return proc
        except OSError, e:
            print >>sys.stderr, "Execution failed:", e

        return "cl_error"

class HadoopAppExecutor():
    def __init__(self, args):
        self.args = args
        return

    def run(self):
        # init process runner
        proc = subprocess.Popen(self.args, stderr = subprocess.PIPE)

        # init extractor
        he = HadoopExtractor()

        with proc.stderr:

            # extract and return jobID from stderr
            errLines = []
            while proc.poll() is None:
                errLine = proc.stderr.readline()
                errLines.append(errLine)
                errLine = errLine.strip()
                if (he.parseJobID(errLine)):

                    return he.getJobID()

            # if job id not found
            print >> sys.stderr, "---err msg---\n{0}---err msg---".format("\n".join(errLines))
            print >> sys.stderr, "job id not found. hadoop job init failed"
            #sys.exit(-1)

        return "job_error"
