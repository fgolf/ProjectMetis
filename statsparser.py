import json
import os
import sys
from pprint import pprint

with open("summary.json","r") as fhin:
    data = json.load(fhin)
summary = data["summary"]
counts = data["counts"]

for dsname in summary.keys():
    print

    sample = summary[dsname]
    cms4nevts = 0
    dbsnevts = counts[dsname]["dbs"]
    for iout in sample.keys():
        job = sample[iout]

        is_done  = job["output_exists"] and not job["is_on_condor"]

        if is_done:
            cms4nevts += job["output"][1]
            continue

        condor_jobs = job["condor_jobs"]
        retries = max(0, len(condor_jobs)-1)
        inputs = job["inputs"]
        innames, innevents = zip(*inputs)
        nevents = sum(innevents)
        print "[{0}] Job {1} is not done. Retried {2} times.".format(dsname, iout, retries)
        print "   --> {0} inputs with a total of {1} events".format(len(inputs),nevents)
        if retries >= 1:
            print "   --> Previous condor logs:"
            for ijob in range(len(condor_jobs)-1):
                outlog = condor_jobs[ijob]["logfile_out"]
                errlog = condor_jobs[ijob]["logfile_err"]
                print "       - {0}".format(errlog)

    if dbsnevts != cms4nevts:
        print "Dataset {0} is missing {1} events (DBS: {2}, CMS4: {3})".format(
                dsname, dbsnevts-cms4nevts, dbsnevts, cms4nevts
                )

