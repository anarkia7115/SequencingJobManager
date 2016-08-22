#!/usr/bin/env python

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
