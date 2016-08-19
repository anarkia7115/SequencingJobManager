#!/usr/bin/env python
import time
import sys

class StepManager():

    def __init__(self, jobID, steps, ag, rs):
        self.jobID = jobID
        self.resultPath = ag.getResultPath()
        self.rs = rs

        # create Step
        self.steps = []
        for s in steps:
            self.steps.append(Step(s, jobID, ag, rs))

        self.finishedSteps = set()
        return

    def wait(self):
        # loop until steps reduce to 0
        while(len(self.steps) > 0):

            # refresh step status
            for i in range(0, len(self.steps)):
                s = self.steps[i]
                print "".format(s.getStatus())
                s.refresh(self.finishedSteps)

                # if step finished
                if s.isFinished():
                    self.steps.pop(i)
                    self.finishedSteps.add(s.getStepName())
                    time.sleep(1)
                    break
                # if step meets all prerequisites but not running
                elif s.isReady():
                    s.start(s.getArgs())
                    print "{} is started".format(s.getStepName())
                    time.sleep(1)
                    break
            # self end 
            time.sleep(1)
        # while end

        # TODO: do something to result path

        self.cleanUp()
        return 

    def cleanUp(self):
        print "cleaning up Step Manager"

        # create request signal
        returnJson = dict()
        returnJson['resultPath'] = self.resultPath

        # send request signal
        self.rs.send(returnJson, '/gcbi/ch/inner/cluster/sampleAnalyzeResult')

        return

class Step():

    def __init__(self, stepName, jobID, argsGen, rs):

        self.status = "pending"
        self.hdfsHost = "node19"

        self.step = stepName
        self.jobID = jobID
        self.args = argsGen.generateArgs(self.step)
        self.rs = rs

        self.prerequisites = set()

        # init step specified infos
        if self.step == "distribution":
            self.execType = "cl"
            # self.prerequisites is empty
            self.finishSignal = "hdfs://{}:9000/signal/dist_{}".format(self.hdfsHost,
                                                             self.jobID)
        elif self.step == "alignment":
            self.execType = "ha"
            self.prerequisites.add("distribution")
            self.finishSignal = "job_alignment"
        elif self.step == "variation":
            self.execType = "ha"
            self.prerequisites.add("alignment")
            self.finishSignal = "job_variation"
        elif self.step == "packaging":
            self.execType = "cl"
            self.prerequisites.add("variation")
            self.prerequisites.add("qc")
            self.finishSignal = "hdfs://{}:9000/signal/pkg_{}".format(self.hdfsHost,
                                                             self.jobID)
        elif self.step == "qc":
            self.execType = "cl"
            self.prerequisites.add("distribution")
            self.finishSignal = "hdfs://{}:9000/signal/qc_{}".format(self.hdfsHost,
                                                             self.jobID)
        else:
            print >> sys.stderr, "unknown step name: {}".format(self.step)
            sys.exit(-1)

        # init status checker
        from status import StatusChecker
        self.sc = StatusChecker(self.finishSignal)

        return

    def start(self, args):
        from executor import CommandLineExecutor, HadoopAppExecutor

        if self.execType == 'cl':
            xqtr = CommandLineExecutor(args)
        elif self.execType == 'ha':
            xqtr = HadoopAppExecutor(args)
        else:
            print >> sys.stderr, "unknown execType: {}".format(self.execType)
            sys.exit(-1)

        self.status = "running"
        xqtr.run()

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

    def cleanUp(self):
        # 1. send signal
        # 2. send request

        # create return json
        returnJson = dict()
        returnJson['step'] = self.step
        returnJson['result'] = True

        self.rs.send(returnJson, '/gcbi/ch/inner/cluster/updateAnalyzeStep')

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

