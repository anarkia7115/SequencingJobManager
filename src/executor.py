#!/usr/bin/env python

class CommonExecutor():
    def __init__(self, execType, args):
        if execType == 'cl':
            return CommandLineExecutor(args)
        elif execType == 'ha':
            return HadoopAppExecutor(args)

class CommandLineExecutor():
    def __init__(self, args):
        self.args = args
        return

    def run(self):
        print "running: {0}".format(self.args)
        return


class HadoopAppExecutor():
    def __init__(self, args):
        self.args = args
        return

    def run(self):
        print "running: {0}".format(self.args)
        return
