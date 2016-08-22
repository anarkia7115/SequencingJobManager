#!/usr/bin/env python
# -*- coding: iso-8859-15 -*-

import sys
import config

class ArgsGenerator():

    def __init__(self, dataJson):
        self.argsDict = self.getStepInfo(dataJson)
        self.accession =  self.argsDict['accession']
        self.processID =  self.argsDict['processID']
        self.resultPath = self.argsDict['resultPath']

    def getAccession(self):
        return self.accession

    def getProcessID(self):
        return self.processID

    def getResultPath(self):
        return self.resultPath

    def generateArgs(self, step):

        if step == 'distribution':
            # get args
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
            args = """java -jar {0}
            -1 {1}
            -O {2}
            -t {3}""".format(jarFile, manifestFileName, output, threadNum)
            return args

        elif step == 'alignment':
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
            args = """hadoop jar {jf} be.ugent.intec.halvade.Halvade 
            -libjars $LIBJARS 
            -nodes {nodes}
            -vcores {vc} 
            -RT bcftools
            -report_all
            -I {infile}
            -tmp {tmp}
            -mem {m}
            -R {ref}
            -O {out}            
            -aln 1
            -B {b}
            -D {d}
            -smt
            """.format(jf=jarFile, vc=vcores, b=binFile, d=nonUseVcf, m=mem,
                     out=outputFile, infile=inputFile, ref=ref, nodes=nodes,
                     tmp=tmpFile)
            return args
            
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
            args = """hadoop jar {jf} be.ugent.intec.halvade.Halvade 
            -libjars $LIBJARS 
            -nodes {nodes}
            -vcores {vc} 
            -RT bcftools
            -report_all
            -I {infile}
            -mem {m}
            -R {ref}
            -O {out}            
            -aln 1
            -B {b}
            -D {d}
            -smt
            """.format(jf=jarFile, vc=vcores, b=binFile, d=nonUseVcf, m=mem,
                     out=outputFile, infile=inputFile, ref=ref, nodes=nodes)
            return args

        elif step == 'packaging':
            args = "packaging arg string"
            return args
        elif step == 'qc':
            args = "qc arg string"
            return args
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

