#!/usr/bin/env python
# -*- coding: iso-8859-15 -*-

from subprocess import Popen, PIPE
def main():
    example = "/home/shawn/workspace/hadoop-2.7.2/share/hadoop/mapreduce/hadoop-mapreduce-examples-2.7.2.jar"
    cmd = ['hadoop', 'jar', example, "wordcount", "wiki", "output"]

    p = Popen(cmd, stdout=PIPE, bufsize=1)
    with p.stdout:
        for line in iter(p.stdout.readline, b''):
            print line,

    #p.wait()

if __name__ == "__main__":
    main()
