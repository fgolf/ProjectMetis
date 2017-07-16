#!/bin/bash
source /code/osgcode/cmssoft/cms/cmsset_default.sh
cd /cvmfs/cms.cern.ch/slc6_amd64_gcc493/cms/cmssw/CMSSW_8_0_20/; cmsenv; cd -
source /cvmfs/cms.cern.ch/crab3/crab.sh

export METIS_BASE=`pwd`
cd $METIS_BASE

# CRAB screws up our PYTHONPATH. Go figure.
export PYTHONPATH=$(pwd):$PYTHONPATH
