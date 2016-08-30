#!/usr/bin/env python
import time
import sys
import os
import shutil
import subprocess
import config
from executor import CommandLineExecutor, HadoopAppExecutor

def callStep(stepName, args):
    if stepName == 'distribution':
        return DistributionStep(*args)
    elif stepName == 'align':
        return AlignStep(*args)
    elif stepName == 'variation':
        return VariationStep(*args)
    elif stepName == 'qa':
        return QaStep(*args)
    elif stepName == 'pkgResult':
        return PkgResultStep(*args)
    else:
        print >> sys.stderr, "step: {0} not found".format(stepName)
        sys.exit(-1)


class StepManager():

    def __init__(self, jobID, steps, ag, rs):
        self.jobID = jobID
        self.resultPath = ag.getResultPath()
        self.rs = rs
        self.stepWithError = False

        # create Step
        self.steps = []
        for s in steps:
            self.steps.append(callStep(s, [s, jobID, ag, rs]))

        self.finishedSteps = set()
        return

    def addFinishedSteps(self, fs):
        # union 2 finished steps
        self.finishedSteps |= set(fs)

    def wait(self):
        # loop until steps reduce to 0
        while(len(self.steps) > 0):

            if (self.stepWithError):
                break

            # refresh step status
            for i in range(0, len(self.steps)):
                s = self.steps[i]

                # if step finished
                if s.isFinished():
                    self.steps.pop(i)
                    self.finishedSteps.add(s.getStepName())
                    cleanStatus = s.cleanUp()
                    if (s.isSuccess() and cleanStatus):
                        s.sendRequest(resultSucc=True, isStart=False)
                    else:
                        print "step {0} finished with error".format(s.getStepName())
                        self.stepWithError = True
                        self.sendRequest(resultSucc=False, isStart=False)
                        break
                    time.sleep(1)
                    break
                # if step meets all prerequisites but not running
                elif s.isReady(self.finishedSteps):
                    print "starting {0}...".format(s.getStepName())
                    initStatus = s.stepInit()
                    if(initStatus):
                        s.start()
                        print "{0} is started".format(s.getStepName())
                    else:
                        print "step {0} inited with error".format(s.getStepName())
                        self.stepWithError = True
                        break
                    time.sleep(1)
                    break
            # self end 
            time.sleep(1)
        # while end

        # TODO: do something to result path

        self.cleanUp()
        return self.stepWithError

    def cleanUp(self):
        print "cleaning up Step Manager"

        # create request signal
        returnJson = dict()
        returnJson['resultPath'] = self.resultPath

        # send request signal
        self.rs.send(returnJson, '/nosec/cluster/sampleAnalyzeResult')

        return

"""
    Step Interface
"""
class StepModel(object):

    def __init__(self, stepName, jobID, argsGenerator, requestSender):

        self.jobID          = jobID
        self.step           = stepName
        self.requestSender  = requestSender
        self.args           = argsGenerator.generateArgs(self.step)

        self.isRunning = False
        self.setPrerequisites()

        self.sendRequest(resultSucc=False, isStart=True)

        return

    def start(self):
        self.isRunning = True
        return

    def cleanUp(self):
        raise NotImplementedError("Please Implement this method")
        return

    def setPrerequisites(self):
        raise NotImplementedError("Please Implement this method")
        return

    def stepInit(self):
        raise NotImplementedError("Please Implement this method")
        return

    def sendRequest(self, resultSucc, isStart):
        returnJson = dict()
        returnJson['step'] = self.step

        returnJson['result'] = resultSucc
        if(isStart):
            returnJson['timeType'] = 'startTime'
        else:
            returnJson['timeType'] = 'endTime'

        self.requestSender.send(returnJson, '/nosec/cluster/updateAnalyzeStep')

    def getStepName(self):
        return self.step

    def isReady(self, finishedSteps):

        if self.prerequisites.issubset(finishedSteps) and not self.isRunning:
            return True
        else:
            return False

    def isFinished(self):
        try:
            status = self.statusChecker.check()
        except AttributeError:
            "{0} not started".format(self.step)
            return False

        return status

    def isSuccess(self):
        return self.statusChecker.isSuccess()

"""
    Distribution
"""
class DistributionStep(StepModel):

    def setPrerequisites(self):
        self.prerequisites = set()
        return

    def stepInit(self):
        return True

    def start(self):
        super.start()
        xqtr = CommandLineExecutor(self.args)
        processHandle = xqtr.run()
        self.statusChecker = StatusChecker(processHandle)

        return processHandle

    def cleanUp(self):
        return True

"""
    Align
"""
class AlignStep(StepModel):

    def setPrerequisites(self):
        self.prerequisites = set()
        self.prerequisites.add("distribution")

    def stepInit(self):
        return True

    def start(self):
        super.start()
        xqtr = CommandLineExecutor(self.args)
        processHandle = xqtr.run()
        self.statusChecker = StatusChecker(processHandle)

        return processHandle

    def cleanUp(self):
        return True

"""
    Variation
"""
class VariationStep(StepModel):

    def setPrerequisites(self):
        self.prerequisites = set()
        self.prerequisites.add("align")

    def stepInit(self):
        return True

    def start(self):
        super.start()
        xqtr = CommandLineExecutor(self.args)
        processHandle = xqtr.run()
        self.statusChecker = StatusChecker(processHandle)

        return processHandle

    def cleanUp(self):
        return True

"""
    Qa
"""
class QaStep(StepModel):

    def setPrerequisites(self):
        self.prerequisites = set()
        self.prerequisites.add("distribution")
        return

    def stepInit(self):

        hdfsFastq = config.hdfs_base['upload'].format(self.jobID)
        localFastq = config.local_config['local_fastq'].format(self.jobID)

        # check folder exists
        if (os.path.exists(localFastq)):
            shutil.rmtree(localFastq)
            print("{0} is removed".format(localFastq))

        # download from hdfs to local
        import hdfs

        print("{0} to {1} in qc init...".format(hdfsFastq, localFastq))
        try:
            client = hdfs.InsecureClient(
                url="http://{0}:50070".format(config.host['hdfshost']))
            client.download(hdfs_path=hdfsFastq, local_path=localFastq)
        except hdfs.HdfsError:
            print >> sys.stderr, "qc init failed during hdfs downloading"
            return False

        print("download finished")

        # decompressing
        decCmd = ['gunzip', '-r', localFastq]

        print("decompressing...")
        decRc = subprocess.call(decCmd)

        if not (decRc == 0):
            print >> sys.stderr, "qc init failed during decompressing"
            return False

        print("decompress finished")

        return True

    def start(self):
        super.start()
        xqtr = CommandLineExecutor(self.args)
        processHandle = xqtr.run()
        self.statusChecker = StatusChecker(processHandle)

        return processHandle

    def cleanUp(self):

        localQa = config.local_config['local_qa'].format(self.jobID)
        hdfsQa = config.hdfs_base['qa'].format(self.jobID)

        import hdfs

        # upload to hdfs
        try:
            client = hdfs.InsecureClient(
                url="http://{0}:50070".format(config.host['hdfshost']))

            client.upload(local_path=localQa, hdfs_path=hdfsQa)
        except hdfs.HdfsError:
            print >> sys.stderr, "qc clean failed"
            return False

        return True

"""
    PkgResult
"""
class PkgResultStep(StepModel):

    def setPrerequisites(self):
        self.prerequisites = set()
        self.prerequisites.add("variation")
        self.prerequisites.add("qa")
        return

    """
        Get merged vcf, qa files to local
        Run separ_snp_indel
        Add headers to *.snp, *.indel
        Run vcf4convert
    """
    def stepInit(self):

        vcfPath = os.path.join(config.hdfs_out['snv'], "merge/HalvadeCombined.vcf")
        qaPath = os.path.join(config.hdfs_out['qa'], "*")

        localPkg = config.local_config['local_pkgResult'].format(self.jobID)

        localVcf = os.path.join(localPkg, "HalvadeCombined.vcf")

        localSnp = localVcf + ".snp"
        localIndel = localVcf + ".indel"
        localSnpOut = os.path.join(localPkg, "sample_basic_snp-snp.vcf") 
        localIndelOut = os.path.join(localPkg, "sample_basic_indel-indel.vcf") 
        localIndelOut2 = os.path.join(localPkg, "sample_basic2_indel-indel.vcf") 

        localVcfHeader = config.local_config['local_vcf_header'] 
        separBin = config.bin['separ_snp_indel']
        indelBin = config.bin['vcf4convert']

        # get to local
        downloadCmd = ['hdfs', '-get', '-r', vcfPath, qaPath, localPkg]
        rc = subprocess.call(downloadCmd)
        if not (rc == 0):
            print >> sys.stderr, "pkg init failed during download hdfs files"
            return False

        # separate vcf
        separCmd = [separBin, '-f', localVcf]
        rc = subprocess.call(separCmd)
        if not (rc == 0):
            print >> sys.stderr, "pkg init failed during separating vcf"
            return False

        # add Headers
        with open(localSnpOut, 'w') as fso:
            with open(localIndelOut, 'w') as fdo: 
                with open(localSnp) as fsi: 
                    with open(localIndel) as fdi: 
                        with open(localVcfHeader) as fheader:
                            for hl in fheader:
                                fso.write(hl)
                                fdo.write(hl)

                            for l in fsi:
                                fso.write(l)

                            for l in fdi:
                                fdo.write(l)

        # run indel
        indelCmd = [indelBin, localIndelOut, localIndelOut2]
        rc = subprocess.call(indelCmd)
        if not (rc == 0):
            print >> sys.stderr, "pkg init failed during indel"
            return False

        return True

    """
        Do Nothing
    """
    def start(self):
        super.start()
        return None

    def isFinished(self):
        if (self.isRunning):
            return True
        else:
            "{0} not started".format(self.step)
            return False

    def isSuccess(self):
        return True

    """
        Remove unused files
        Zip files
        Move to result path
    """
    def cleanUp(self):

        localPkg = config.local_config['local_pkgResult'].format(self.jobID)
        localResult = config.local_config['local_result'].format(self.jobID) 
        localZip = os.path.join(localPkg, 'result.zip')

        localVcf = os.path.join(localPkg, "HalvadeCombined.vcf")

        localSnp = localVcf + ".snp"
        localIndel = localVcf + ".indel"

        # remove unused files
        os.remove(localVcf)
        os.remove(localSnp)
        os.remove(localIndel)

        # zip files
        zipCmd = ['zip', '-mj', localZip, os.path.join(localPkg, '*')]
        rc = subprocess.call(zipCmd)
        if not (rc == 0):
            print >> sys.stderr, "pkg clean failed during zip"
            return False

        # move to target
        os.mkdir(localResult)
        shutil.move(localZip, localResult)

        return True
