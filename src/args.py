#!/usr/bin/env python
# -*- coding: iso-8859-15 -*-

import os
import sys
import config

class ArgsGenerator():

    def __init__(self, dataJson):
        self.argsDict = self.getStepInfo(dataJson)
        self.accession  =  self.argsDict['accession']
        self.processID  =  self.argsDict['processID']
        self.resultPath =  self.argsDict['resultPath']
        self.inputDir   =  self.findCommonPrefix(self.argsDict['sampleList'])

    def getInputDir(self):
        return self.inputDir

    def getAccession(self):
        return self.accession

    def getProcessID(self):
        return self.processID

    def getResultPath(self):
        return self.resultPath

    def generateArgs(self, step):

        if step == 'distribution':
            # get local sample list
            sampleList = self.argsDict['sampleList']

            # generate sample list manifest
            manifestFileName = config.local_config['tmp_manifest'].format(self.accession)
            manifest = open(manifestFileName, 'w')
            for l in sampleList:
                manifest.write(l)
                manifest.write('\n')

            manifest.close()

            # init args
            jarFile = config.jar['upload']
            output = config.hdfs_out['upload'].format(self.processID) 
            threadNum = 30

            # generate args
            args = ['java', '-jar', jarFile, '-1', manifestFileName, 
                    '-O', output, '-t', threadNum]

            return [str(i) for i in args ]

        elif step == 'align':
            # init args
            inputFile = config.hdfs_in['align'].format(self.processID)
            outputFile = config.hdfs_out['align'].format(self.processID)

            nonUseVcf = config.hdfs_config['empty_vcf']
            binFile = config.hdfs_config['bin']
            ref = config.hdfs_config['ref']
            jarFile = config.jar['align']
            tmpFile = config.local_config['tmp_folder']
            vcores = 30
            nodes = 6
            mem = 110

            # generate args
            args = ['hadoop', 'jar', jarFile, 'be.ugent.intec.halvade.Halvade', 
                    '-libjars', os.environ['LIBJARS'], 
                    '-I', inputFile, '-R', ref, '-O', outputFile, 
                    '-B', binFile, '-D', nonUseVcf,
                    '-RT', 'bcftools', '-mem', mem, '-report_all', 
                    '-nodes', nodes, '-vcores', vcores,
                    '-tmp', tmpFile, '-aln', '1', '-smt']

            return [str(i) for i in args ]
            
        elif step == 'variation':
            # init args
            inputFile = config.hdfs_in['snv'].format(self.processID)
            outputFile = config.hdfs_out['snv'].format(self.processID)

            nonUseVcf = config.hdfs_config['empty_vcf']
            binFile = config.hdfs_config['bin']
            ref = config.hdfs_config['ref']
            jarFile = config.jar['snv']
            vcores = 30
            nodes = 6
            mem = 110

            # generate args
            args = ['hadoop', 'jar', jarFile, 'be.ugent.intec.halvade.Halvade', 
                    '-libjars', os.environ['LIBJARS'], 
                    '-I', inputFile, '-R', ref, '-O', outputFile, 
                    '-B', binFile, '-D', nonUseVcf,
                    '-RT', 'bcftools', '-mem', mem, '-report_all', 
                    '-nodes', nodes, '-vcores', vcores,
                    '-aln', '1', '-smt']

            return [str(i) for i in args ]

        #TODO
        elif step == 'pkgResult':
            return []

        #TODO
        elif step == 'qa':
            args = [ config.bin['qa'], 
                     self.inputDir,
                     config.local_config['local_qa'].format(self.processID),
                     self.processID]

            return [str(i) for i in args ]
        else:
            print >> sys.stderr, "[Error] unknown step: {0}".format(step)
            sys.exit(-1)

    def getStepInfo(self, dataJson):
        argsDict = dict()
        # accession
        argsDict['accession'] = dataJson['sampleList'][0]['accession']
        # processID
        argsDict['processID'] = dataJson['processId']
        # result paths
        argsDict['resultPath'] = dataJson['resultPath']

        # sample paths
        sampleList = []
        sl = dataJson['sampleList'][0]
        for p in sl['fastqFile']:
            # parse keys
            k1 = p['mateFile1']['key']
            k2 = p['mateFile2']['key']
            # pair to string
            samplePair = '\t'.join([k1, k2])
            # append to list
            sampleList.append(samplePair)

        argsDict['sampleList'] = sampleList

        return argsDict

    def findCommonPrefix(self, pathList):
        prefix = os.path.dirname(pathList[0].split()[0])
        # all the gzs should be kept in one folder
        for subPathPair in pathList:
            for subPath in subPathPair.split():
                if os.path.dirname(subPath) != prefix:
                    print os.path.dirname(subPath)
                    print >> sys.stderr, "fastq gzs should be in the same folder!"
                    sys.exit(-1)

        return prefix

if __name__ == "__main__":
    dataString = """ { "processId":"0", "resultPath": "/online/GCBI/result", "sampleList": [ { "accession": "GCS1001", "fastqFile": [ { "mateFile1": { "filename": "line2_R1.fastq.gz", "key": "/Users/Yvonne/Downloads/ch-gcbi/GCS1001/line2_R1.fastq.gz", "protocol": "file" }, "mateFile2": { "filename": "line2_R2.fastq.gz", "key": "/Users/Yvonne/Downloads/ch-gcbi/GCS1001/line2_R2.fastq.gz", "protocol": "file" } }, { "mateFile1": { "filename": "line1_R1.fastq.gz", "key": "/Users/Yvonne/Downloads/ch-gcbi/GCS1001/line1_R1.fastq.gz", "protocol": "file" }, "mateFile2": { "filename": "line1_R2.fastq.gz", "key": "/Users/Yvonne/Downloads/ch-gcbi/GCS1001/line1_R2.fastq.gz", "protocol": "file" } } ], "genomeVersion": "hg38", "species": "hsa" } ], "sampleType": "WSG" } """
    import json
    dataJson = json.loads(dataString)
    ag = ArgsGenerator(dataJson)
    print ag.getInputDir()

