#!/bin/bash

OUTPUTDIR=$1
OUTPUTNAME=$2
INPUTFILENAMES=$3
IFILE=$4
CMSSWVERSION=$5
SCRAMARCH=$6


export SCRAM_ARCH=${SCRAMARCH}

function getjobad {
    grep -i "^$1" "$_CONDOR_JOB_AD" | cut -d= -f2- | xargs echo
}
function setup_chirp {
    if [ -e /usr/libexec/condor/condor_chirp ]; then
        export PATH="$PATH:/usr/libexec/condor"
        echo "[chirp] Found condor_chirp in /usr/libexec/condor"
    else
        echo "[chirp] No condor_chirp :("
    fi
}
function chirp {
    # Note, $1 (the classad name) must start with Chirp
    condor_chirp set_job_attr_delayed $1 $2
    ret=$?
    echo "[chirp] Chirped $1 => $2 with exit code $ret"
}
function stageout {
    COPY_SRC=$1
    COPY_DEST=$2
    retries=0
    COPY_STATUS=1
    until [ $retries -ge 3 ]
    do
        echo "Stageout attempt $((retries+1)): env -i X509_USER_PROXY=${X509_USER_PROXY} gfal-copy -p -f -t 7200 --verbose --checksum ADLER32 ${COPY_SRC} ${COPY_DEST}"
        env -i X509_USER_PROXY=${X509_USER_PROXY} gfal-copy -p -f -t 7200 --verbose --checksum ADLER32 ${COPY_SRC} ${COPY_DEST}
        COPY_STATUS=$?
        if [ $COPY_STATUS -ne 0 ]; then
            echo "Failed stageout attempt $((retries+1))"
        else
            echo "Successful stageout with $retries retries"
            break
        fi
        retries=$[$retries+1]
        echo "Sleeping for 30m"
        sleep 30m
    done
    if [ $COPY_STATUS -ne 0 ]; then
        echo "Removing output file because gfal-copy crashed with code $COPY_STATUS"
        env -i X509_USER_PROXY=${X509_USER_PROXY} gfal-rm --verbose ${COPY_DEST}
        REMOVE_STATUS=$?
        if [ $REMOVE_STATUS -ne 0 ]; then
            echo "Uhh, gfal-copy crashed and then the gfal-rm also crashed with code $REMOVE_STATUS"
            echo "You probably have a corrupt file sitting on hadoop now."
            exit 1
        fi
    fi
}

function setup_env {
    if [ -r "$OSGVO_CMSSW_Path"/cmsset_default.sh ]; then
        echo "sourcing environment: source $OSGVO_CMSSW_Path/cmsset_default.sh"
        source "$OSGVO_CMSSW_Path"/cmsset_default.sh
    elif [ -r "$OSG_APP"/cmssoft/cms/cmsset_default.sh ]; then
        echo "sourcing environment: source $OSG_APP/cmssoft/cms/cmsset_default.sh"
        source "$OSG_APP"/cmssoft/cms/cmsset_default.sh
    elif [ -r /cvmfs/cms.cern.ch/cmsset_default.sh ]; then
        echo "sourcing environment: source /cvmfs/cms.cern.ch/cmsset_default.sh"
        source /cvmfs/cms.cern.ch/cmsset_default.sh
    else
        echo "ERROR! Couldn't find $OSGVO_CMSSW_Path/cmsset_default.sh or /cvmfs/cms.cern.ch/cmsset_default.sh or $OSG_APP/cmssoft/cms/cmsset_default.sh"
        exit 1
    fi
}

# Make sure OUTPUTNAME doesn't have .root since we add it manually later
OUTPUTNAME=$(echo $OUTPUTNAME | sed 's/\.root//')

echo "OUTPUTDIR: $OUTPUTDIR"
echo "OUTPUTNAME: $OUTPUTNAME"
echo "INPUTFILENAMES: $INPUTFILENAMES"
echo "IFILE: $IFILE"
echo "CMSSWVERSION: $CMSSWVERSION"
echo "SCRAMARCH: $SCRAMARCH"
# echo  CLASSAD: $(cat "$_CONDOR_JOB_AD")

echo "GLIDEIN_CMSSite: $GLIDEIN_CMSSite"
echo "hostname: $(hostname)"
echo "uname -a: $(uname -a)"
echo "time: $(date +%s)"
echo "args: $@"
echo "tag: $(getjobad tag)"
echo "taskname: $(getjobad taskname)"

setup_chirp
setup_env

eval `scramv1 project CMSSW $CMSSWVERSION`
cd $CMSSWVERSION
eval `scramv1 runtime -sh`
mv ../package.tar.gz package.tar.gz
tar xf package.tar.gz

echo "before running: ls -lrth"
ls -lrth

echo -e "\n--- begin running ---\n" #                           <----- section division

chirp ChirpMetisStatus "startedrunning"

export XRD_LOGLEVEL=Debug
export XRD_LOGFILE=xrd.log

python looper.py $INPUTFILENAMES -o ${OUTPUTNAME}.root
RET=$?

chirp ChirpMetisStatus "finishedrunning"

echo "after running: ls -lrth"
ls -lrth

echo -e "\n--- begin xrootd log ---\n"
cat "$XRD_LOGFILE"
echo -e "\n--- end xrootd log ---\n"

if [[ $RET != 0 ]]; then
    echo "Removing output file because looper crashed with exit code $RET"
    rm ${OUTPUTNAME}.root
    exit 1
fi

echo "time before copy: $(date +%s)"
chirp ChirpMetisStatus "startedcopy"

# # Old
# COPY_SRC="file://`pwd`/${OUTPUTNAME}.root"
# COPY_DEST="gsiftp://gftp.t2.ucsd.edu${OUTPUTDIR}/${OUTPUTNAME}_${IFILE}.root"
# stageout $COPY_SRC $COPY_DEST

# New
COPY_SRC="file://`pwd`/${OUTPUTNAME}.root"
OUTPUTDIRSTORE=$(echo $OUTPUTDIR | sed "s#^/ceph/cms/store#/store#")
COPY_DEST="davs://redirector.t2.ucsd.edu:1095${OUTPUTDIRSTORE}/${OUTPUTNAME}_${IFILE}.root"
stageout $COPY_SRC $COPY_DEST

echo "time at end: $(date +%s)"


chirp ChirpMetisStatus "done"
