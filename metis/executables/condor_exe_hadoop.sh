PACKAGE=package.tar.gz
OUTPUTDIR=$1
OUTPUTFILENAME=$2
INPUTFILENAMES=$3
INDEX=$4
CMSSW_VER=$5

ARGS=$7

echo "[wrapper] OUTPUTDIR	= " ${OUTPUTDIR}
echo "[wrapper] OUTPUTFILENAME	= " ${OUTPUTFILENAME}
echo "[wrapper] INPUTFILENAMES	= " ${INPUTFILENAMES}
echo "[wrapper] INDEX		= " ${INDEX}

echo "[wrapper] printing env"
printenv
echo

echo "[wrapper] hostname  = " $(hostname)
echo "[wrapper] date      = " $(date)
echo "[wrapper] linux timestamp = " $(date +%s)

######################
# Set up environment #
######################

export SCRAM_ARCH=slc7_amd64_gcc700
source /cvmfs/cms.cern.ch/cmsset_default.sh

# Untar
tar -xvf package.tar.gz

# Build
echo "using CMSSW version " ${CMSSW_VER}
cd "$CMSSW_VER/src"
echo "[wrapper] in directory: " ${PWD}
echo "[wrapper] ls : " 
ls
echo "[wrapper] attempting to build"
eval $(scramv1 runtime -sh)
scramv1 b ProjectRename
scram b -j3
eval $(scramv1 runtime -sh)

cmssw_cfg="template.py"

if [[ $ARGS == *"2016_MC"* ]]
then

    cmssw_cfg="HIG-RunIISummer20UL16NanoAODv9-00678_1_cfg.py"

elif [[ $ARGS == *"2016_APV_MC"* ]]
then

    cmssw_cfg="HIG-RunIISummer20UL16NanoAODAPVv9-00193_1_cfg.py"

elif [[  $ARGS == *"2017_MC"*  ]]
then 

    cmssw_cfg="HIG-RunIISummer20UL17NanoAODv9-00797_1_cfg.py"

elif [[  $ARGS == *"2018_MC"*  ]]
then 

    cmssw_cfg="HIG-RunIISummer20UL18NanoAODv9-00892_1_cfg.py"

elif [[  $ARGS == *"2016_Data"*  ]]
then 
    cmssw_cfg="data_2016.py"

elif [[  $ARGS == *"2017_Data"*  ]]
then 
    cmssw_cfg="data_2017.py"

elif [[  $ARGS == *"2018_Data"*  ]]
then 
    cmssw_cfg="data_2018.py"

else
    echo "Don't know which cmssw cfg to use, check ARGS!!"
fi

# update input file
echo "process.source = cms.Source(\"PoolSource\",
fileNames=cms.untracked.vstring(\"${INPUTFILENAMES}\".replace('/hadoop', 'file:/hadoop').split(\",\"))
)

process.maxEvents = cms.untracked.PSet( input = cms.untracked.int32( -1 ) )
" >> $cmssw_cfg 

# Create tag file
echo "[wrapper $(date +\"%Y%m%d %k:%M:%S\")] running: cmsRun "${cmssw_cfg}
cmsRun ${cmssw_cfg}

if [ "$?" != "0" ]; then
    echo "Removing output file because cmsRun crashed with exit code $?"
    rm -f ./*.root
fi

echo "[wrapper] output root files are currently: "
ls -lh *.root

substr="/hadoop/cms"
new_OUTPUTDIR=${OUTPUTDIR#$substr}
# Copy output
env -i X509_USER_PROXY=${X509_USER_PROXY} gfal-copy -p -f -t 4200 --verbose file://$(pwd)/nanoaod.root davs://redirector.t2.ucsd.edu:1094/${new_OUTPUTDIR}/${OUTPUTFILENAME}_${INDEX}.root --checksum ADLER32
