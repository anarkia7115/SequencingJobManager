#!/usr/bin/env python
# -*- coding: iso-8859-15 -*-

from subprocess import Popen, PIPE
from hdfs import InsecureClient
import sys
sys.path.insert(0, "../src")
from extractor import HadoopExtractor

def main():
    example = "/home/shawn/workspace/hadoop-2.7.2/share/hadoop/mapreduce/hadoop-mapreduce-examples-2.7.2.jar"
    cmd = ['hadoop', 'jar', example, "pi", "100", "10000"]

    # delete output folder
    #ic = InsecureClient(url="http://localhost:50070", user="shawn")
    #ic.delete(hdfs_path="output", recursive=True)

    #tempStdout = tempfile.TemporaryFile('w+t')
    p = Popen(cmd, stderr=PIPE, bufsize=1)

    # init extractor
    he = HadoopExtractor()

    with p.stderr:
        while p.poll() is None:
            errLine = p.stderr.readline()
            if (he.getJobID(errLine)):
                break

if __name__ == "__main__":
    main()
