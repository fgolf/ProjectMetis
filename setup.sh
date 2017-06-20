#!/bin/bash

source /code/osgcode/cmssoft/cms/cmsset_default.sh
source /cvmfs/cms.cern.ch/crab3/crab.sh
source /code/osgcode/cmssoft/cms/cmsset_default.sh

SCRAM_ARCH="slc6_amd64_gcc530"
CMS3TAG="CMS3_V08-00-16"
CMSSW_VER="CMSSW_8_0_21"

export METIS_BASE=`pwd`

# echo "[setup] Using $CMS3TAG and $CMSSW_VER"

# # if the cmssw dir doesn't exist or the current tag hasn't been extracted, and if we're not making babies
# if [ ! -d $CMSSW_VER ] || [ ! -e $CMSSW_VER/lib_${CMS3TAG}.tar.gz ]; then
#     if [ ! -e /nfs-7/userdata/libCMS3/lib_${CMS3TAG}.tar.gz ]
#     then
#         echo "[setup] Making tar on-the-fly"
#         source $METIS_BASE/scripts/make_libCMS3.sh ${CMS3TAG} $CMSSW_VER
#         cp lib_${CMS3TAG}.tar.gz /nfs-7/userdata/libCMS3/lib_${CMS3TAG}.tar.gz
#         cd $CMSSW_BASE
#     else
#         scramv1 p -n ${CMSSW_VER} CMSSW $CMSSW_VER
#         cd $CMSSW_VER
#         cmsenv
#         cp /nfs-7/userdata/libCMS3/lib_${CMS3TAG}.tar.gz . 
#         echo "[setup] Extracting tar"
#         tar -xzf lib_${CMS3TAG}.tar.gz
#         scram b -j 10
#     fi
# else
#     echo "[setup] $CMSSW_VER already exists, only going to set environment then"
#     cd ${CMSSW_VER}
#     cmsenv
# fi

source /cvmfs/cms.cern.ch/crab3/crab.sh

cd $METIS_BASE

# CRAB screws up our PYTHONPATH. Go figure.
export PYTHONPATH=$(pwd):$PYTHONPATH
