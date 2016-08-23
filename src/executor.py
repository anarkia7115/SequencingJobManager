#!/usr/bin/env python

from extractor import HadoopExtractor 
import sys

class CommandLineExecutor():
    def __init__(self, args):
        self.args = args
        return

    def run(self):
        # init process runner
        proc = subprocess.Popen(self.args, stderr = subprocess.PIPE)

        return proc


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
            while proc.poll() is None:
                errLine = proc.stderr.readline()
                if (he.parseJobID(errLine)):

                    return he.getJobID()

            # if job id not found
            print >> sys.stderr, "job id not found. hadoop job init failed"
            sys.exit(-1)

        return "job_error"
