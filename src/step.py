#!/usr/bin/env python
import time
import sys
import os
import config

class StepManager():

    def __init__(self, jobID, steps, ag, rs):
        self.jobID = jobID
        self.resultPath = ag.getResultPath()
        self.rs = rs
        self.stepWithError = False

        # create Step
        self.steps = []
        for s in steps:
            self.steps.append(Step(s, jobID, ag, rs))

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
                print "".format(s.getStatus())
                s.refresh(self.finishedSteps)

                # if step finished
                if s.isFinished():
                    self.steps.pop(i)
                    self.finishedSteps.add(s.getStepName())
                    if (s.isFinalSuccess()):
                        pass
                    else:
                        self.stepWithError = True
                    time.sleep(1)
                    break
                # if step meets all prerequisites but not running
                elif s.isReady():
                    s.start(s.getArgs())
                    print "{0} is started".format(s.getStepName())
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

class Step():

    def __init__(self, stepName, jobID, argsGen, rs):

        self.status = "pending"

        self.step = stepName
        self.jobID = jobID
        self.args = argsGen.generateArgs(self.step)
        print("self.args: {0}".format(self.args))
        self.rs = rs

        self.prerequisites = set()
        si = StepInit()
        sc = StepClean()

        # init step specified infos
        if self.step == "distribution":
            self.execType = "cl"
            # self.prerequisites is empty
            #self.finishSignal = config.hdfs_config['signal'].format(self.step, self.jobID)
        elif self.step == "align":
            self.execType = "ha"
            self.prerequisites.add("distribution")
            #self.finishSignal = "job_alignment"
        elif self.step == "variation":
            self.execType = "ha"
            self.prerequisites.add("align")
            #self.finishSignal = "job_variation"
        elif self.step == "pkgResult":
            self.execType = "cl"
            self.prerequisites.add("variation")
            self.prerequisites.add("qa")
            self.stepInit = si.pkgResultInit
            self.stepInitArgs = []
            #self.finishSignal = config.hdfs_config['signal'].format(self.step, self.jobID)
        elif self.step == "qa":
            self.execType = "cl"
            self.stepInit = si.qaInit
            self.stepClean = sc.qaClean
            self.stepInitArgs = [
                 config.hdfs_base['upload'].format(self.jobID), 
                 config.local_config['local_fastq'].format(self.jobID) ]
            self.stepCleanArgs = [
                 config.local_config['local_qa'].format(self.jobID),
                 config.hdfs_base['qa'].format(self.jobID) ]
            self.prerequisites.add("distribution")
            #self.finishSignal = config.hdfs_config['signal'].format(self.step, self.jobID)

        else:
            print >> sys.stderr, "unknown step name: {0}".format(self.step)
            sys.exit(-1)

        return

    def start(self, args):
        # run init function
        #self.stepInit(self.stepInitArgs)
        try:
            inited = self.stepInit(self.stepInitArgs)
            if(not inited): 
                print >> sys.stderr, "{0} init failed".format(self.step)
                self.status = "finished"
                return

        except AttributeError as e:
            print e.strerror
            print "skip init for {0}".format(self.step)

        # run xqtr
        from executor import CommandLineExecutor, HadoopAppExecutor

        if self.execType == 'cl':
            xqtr = CommandLineExecutor(args)
        elif self.execType == 'ha':
            xqtr = HadoopAppExecutor(args)
        else:
            print >> sys.stderr, "unknown execType: {0}".format(self.execType)
            sys.exit(-1)

        self.status = "running"
        finishSignal = xqtr.run()

        # send start signal
        # create return json
        returnJson = dict()
        returnJson['step'] = self.step

        returnJson['result'] = False
        returnJson['timeType'] = 'startTime'

        self.rs.send(returnJson, '/nosec/cluster/updateAnalyzeStep')

        # init status checker
        from status import StatusChecker
        self.sc = StatusChecker(finishSignal)

        return

    def refresh(self, finishedSteps):
        if self.status == "pending":
            # see if meetPrerequisites ?
            if self.meetPrerequisites(finishedSteps):
                self.status = "ready"

        elif self.status == "running":
            if self.foundFinishSignal():
                self.cleanUp()
                self.status = "finished"
        return

    def isFinalSuccess(self):
        return self.sc.isFinalSuccess()

    def cleanUp(self):
        # 1. send signal
        # 2. send request

        try:
            cleaned = self.stepClean(self.stepCleanArgs)
            if(not cleaned):
                print >> sys.stderr, "{0} clean failed".format(self.step)
                self.status = "finished"
                return

        except AttributeError:
            print "skip clean for {0}".format(self.step)

        # check finalStatus
        isSuccess = self.isFinalSuccess() 

        # create return json
        returnJson = dict()
        returnJson['step'] = self.step

        if (isSuccess):
            returnJson['result'] = True
        else:
            returnJson['result'] = False

        returnJson['timeType'] = 'endTime'
        self.rs.send(returnJson, '/nosec/cluster/updateAnalyzeStep')

        # send request

        return

    def isFinished(self):
        if self.status == "finished":
            return True
        else:
            return False

    def isReady(self):
        if self.status == "ready":
            return True
        else:
            return False

    def foundFinishSignal(self):
        status = self.sc.check()
        return status

    def getStepName(self):
        return self.step

    def getArgs(self):
        return self.args

    def meetPrerequisites(self, finishedSet):

        if self.prerequisites.issubset(finishedSet):
            return True
        else:
            return False

    def getStatus(self):
        return self.status

class StepInit():

    def qaInit(self, args):

        hdfsFastq = args[0]
        localFastq = args[1]

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

        # decompressing
        decCmd = ['gunzip', os.path.join(localFastq, '*')]

        decRc = subprocess.call(decCmd)

        if not (decRc == 0):
            print >> sys.stderr, "qc init failed during decompressing"
            return False

        return True

    def pkgResultInit(self, args):

        cmds1 = ['hdfs', 'mkdir', '-p', config.hdfs_in['pkgResult']]
        cmds2 = ['hdfs', 'cp', '-r', 
                config.hdfs_out['upload'], config.hdfs_out['align'],
                config.hdfs_out['snv'], config.hdfs_out['qa'],
                config.hdfs_in['pkgResult']]

        rc1 = subprocess.call(cmds1)
        rc2 = subprocess.call(cmds2)

        if not (rc1 == 0 and rc2 == 0):
            print >> sys.stderr, "pkg init failed"
            return False
        else:
            return True

class StepClean():

    def qaClean(self, args):

        localQa = args[0]
        hdfsQa = args[1]

        import hdfs

        try:
            client = hdfs.InsecureClient(
                url="http://{0}:50070".format(config.host['hdfshost']))

            client.upload(local_path=localQa, hdfs_path=hdfsQa)
        except hdfs.HdfsError:
            print >> sys.stderr, "qc clean failed"
            return False
        else:
            return True

