#!/usr/bin/env python
# -*- coding: iso-8859-15 -*-

class HadoopGrabber():
    def getJobID(self, txt):
        print(txt.rstrip())
        return False

if __name__ == "__main__":
    hg = HadoopGrabber()
    f = open("../data/hadoop_job_header")
    for l in f:
        if (hg.getJobID(l)):
            break
