#!/bin/bash

OUTPUTDIR=$1
OUTPUTNAME=$2
INPUTFILENAMES=$3
IFILE=$4
PSET=$5
CMSSWVERSION=$6
SCRAMARCH=$7
NEVTS=$8
EXPECTEDNEVTS=$9
PSETARGS="${@:10}" # since args can have spaces, we take 10th-->last argument as one

# Make sure OUTPUTNAME doesn't have .root since we add it manually
OUTPUTNAME=$(echo $OUTPUTNAME | sed 's/\.root//')

echo -e "\n--- begin header output ---\n" #                     <----- section division
echo "OUTPUTDIR: $OUTPUTDIR"
echo "OUTPUTNAME: $OUTPUTNAME"
echo "INPUTFILENAMES: $INPUTFILENAMES"
echo "IFILE: $IFILE"
echo "PSET: $PSET"
echo "CMSSWVERSION: $CMSSWVERSION"
echo "NEVTS: $NEVTS"
echo "EXPECTEDNEVTS: $EXPECTEDNEVTS"
echo "PSETARGS: $PSETARGS"

echo "hostname: $(hostname)"
echo "uname -a: $(uname -a)"
echo "time: $(date +%s)"
echo "args: $@"

echo -e "\n--- end header output ---\n" #                       <----- section division

source /cvmfs/cms.cern.ch/cmsset_default.sh

export SCRAM_ARCH=${SCRAMARCH}

eval `scramv1 project CMSSW $CMSSWVERSION`
cd $CMSSWVERSION
eval `scramv1 runtime -sh`
mv ../$PSET pset.py
mv ../package.tar.gz package.tar.gz
tar xzf package.tar.gz
scram b

# logging every 45 seconds gives ~100kb log file/3 hours
dstat -cdngytlmrs --float --nocolor -T --output dsout.csv 45 >& /dev/null &

echo "process.maxEvents.input = cms.untracked.int32(${NEVTS})" >> pset.py
echo "process.out.fileName = cms.untracked.string(\"${OUTPUTNAME}.root\")" >> pset.py
echo "process.source.fileNames = cms.untracked.vstring([" >> pset.py
for INPUTFILENAME in $(echo "$INPUTFILENAMES" | sed -n 1'p' | tr ',' '\n'); do
    # INPUTFILENAME=$(echo $INPUTFILENAME | sed 's|/hadoop/cms||')
    # INPUTFILENAME="root://xrootd.unl.edu/${INPUTFILENAME}"
    echo "\"${INPUTFILENAME}\"," >> pset.py
done
echo "])" >> pset.py

echo "before running: ls -lrth"
ls -lrth 

echo -e "\n--- begin running ---\n" #                           <----- section division

cmsRun pset.py ${PSETARGS}

# Rigorous sweeproot which checks ALL branches for ALL events.
# If GetEntry() returns -1, then there was an I/O problem, so we will delete it
python << EOL
import ROOT as r
import os

foundBad = False
try:
    f1 = r.TFile("${OUTPUTNAME}.root")
    t = f1.Get("Events")
    nevts = t.GetEntries()
    expectednevts = ${EXPECTEDNEVTS}
    print "[RSR] ntuple has %i events and expected %i" % (t.GetEntries(), expectednevts)
    if int(t.GetEntries()) != int(expectednevts):
        print "[RSR] nevents mismatch"
        foundBad = True
    for i in range(0,t.GetEntries(),1):
        if t.GetEntry(i) < 0:
            foundBad = True
            print "[RSR] found bad event %i" % i
            break
except: foundBad = True

if foundBad:
    print "[RSR] removing output file because it does not deserve to live"
    os.system("rm ${OUTPUTNAME}.root")
else: print "[RSR] passed the rigorous sweeproot"
EOL

echo -e "\n--- end running ---\n" #                             <----- section division

echo "after running: ls -lrth"
ls -lrth

echo -e "\n--- begin copying output ---\n" #                    <----- section division
echo "Sending output file $OUTPUTNAME.root"
gfal-copy -p -f -t 4200 --verbose file://`pwd`/${OUTPUTNAME}.root gsiftp://gftp.t2.ucsd.edu${OUTPUTDIR}/${OUTPUTNAME}_${IFILE}.root --checksum ADLER32
echo -e "\n--- end copying output ---\n" #                      <----- section division

echo -e "\n--- begin dstat output ---\n" #                      <----- section division
cat dsout.csv
echo -e "\n--- end dstat output ---\n" #                        <----- section division
kill %1 # kill dstat

# cd ../
# echo "cleaning up"
# rm -rf CMSSW*

