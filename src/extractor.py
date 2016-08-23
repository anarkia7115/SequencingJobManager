#!/usr/bin/env python
# -*- coding: iso-8859-15 -*-

import re

class HadoopExtractor():
    def getJobID(self):
        return self.jobID

    def parseJobID(self, txt):
        # init re.search
        pattern = "(.*)(job_[0-9]+_[0-9]+)(.*)"
        jobIDsearch = re.search(pattern, txt)

        if jobIDsearch:
            # if find jobID pattern, break out from loop
            self.jobID = jobIDsearch.group(2)
            return True
        else: 
            # else continue
            self.jobID = "job_error"
            return False

if __name__ == "__main__":
    hg = HadoopExtractor()
    f = open("../data/hadoop_job_header")
    for l in f:
        if (hg.getJobID(l)):
            break
