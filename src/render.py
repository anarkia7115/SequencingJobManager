#!/usr/bin/env python
# -*- coding: iso-8859-15 -*-

import time
import os
class FastqPair(object):
    def __init__(self, manifestFile):
        #open(manifestFile)
        try:
            self.manifestFile = manifestFile
        except IOError as e:
            print "File not found: {0}".format(manifestFile)
            raise
        except:
            print "Unexpected error: {0}".format(sys.exc_info()[0])
            raise
        self.fastqPair = []
        self.m1List = []
        self.m2List = []

    def parseManifest(self):
        with open(self.manifestFile) as f:
            for l in f:
                pairLine = l.rstrip().split()
                m1 = pairLine[0]
                m2 = pairLine[1]
                self.m1List.append(m1)
                self.m2List.append(m2)

    def genJsonFrame(self, sampleName, processId=None):
        dateString = time.strftime("%Y-%m-%d")
        fj = dict()
        if processId is None:
            fj["processId"] = "{0}_{1}_{2}".format(
                    sampleName, dateString, os.getpid())
        else:
            fj["processId"] = processId

        fj["resultPath"] = "/online/GCBI/result"

        # generate fastqFileList
        fastqFileList = []
        for m1, m2 in zip(self.m1List, self.m2List):
            m1FileName = os.path.basename(m1)
            m1Key = m1
            m1Protocal = "file"

            m2FileName = os.path.basename(m2)
            m2Key = m2
            m2Protocal = "file"

            m1Info = dict()
            m1Info['filename'] = m1FileName
            m1Info['key'] = m1Key
            m1Info['protocol'] = m1Protocal

            m2Info = dict()
            m2Info['filename'] = m2FileName
            m2Info['key'] = m2Key
            m2Info['protocol'] = m2Protocal

            fastqFileList.append({"mateFile1" : m1Info, "mateFile2" : m2Info})

        sampleList = dict()
        sampleList["fastqFile"] = fastqFileList
        sampleList["accession"] = str(os.getpid())
        sampleList["genomeVersion"] = "hg38"
        sampleList["species"] = "hsa"

        fj["sampleList"] = [sampleList]
        fj["sampleType"] = "WSG"

        self.fastqDataFrame = fj

    def getFastqDataFrame(self, sampleName, processId=None):
        self.parseManifest()
        self.genJsonFrame(sampleName, processId)
        return self.fastqDataFrame


if __name__ == "__main__":
    manifestFile = "./demo1.manifest"
    fp = FastqPair(manifestFile)
