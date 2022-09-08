#!/usr/bin/env python
# Description:
#   An example to test the DBI
# 
# Author: Tao Lin <lintao AT ihep.ac.cn>
#

import Sniper

import argparse

if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument("--evtmax", type=int, default=1)
    parser.add_argument("--user")
    parser.add_argument("--password")
    parser.add_argument("--mycnf")
    parser.add_argument("--mycnfgrp")
    parser.add_argument("--sqlite")

    args = parser.parse_args()
    
    task = Sniper.Task("task")
    task.setEvtMax(args.evtmax)
    task.setLogLevel(3)

    Sniper.loadDll("libdbi.so")
    testalg = task.createAlg("DummyDBTestAlg")
    if args.user:
        testalg.property("User").set(args.user)
    if args.password:
        testalg.property("Pass").set(args.password)
    if args.mycnf:
        testalg.property("mycnf").set(args.mycnf)
    if args.mycnfgrp:
        testalg.property("mycnfgrp").set(args.mycnfgrp)

    if args.sqlite:
        testalg.property("sqlitefn").set(args.sqlite)

    task.show()
    task.run()
