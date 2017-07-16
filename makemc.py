import Utils
from CMSSWTask import CMSSWTask
from Sample import DirectorySample
from Path import Path

lhe = CMSSWTask(
        sample = DirectorySample(
            location="/hadoop/cms/store/user/namin/lhe_Apr1/",
            globber="*seed6*.lhe",
            dataset="/stop-stop/procv1/LHE",
            ),
        events_per_output = 20,
        total_nevents = 100,
        pset = "mcmaking/pset_gensim.py",
        cmssw_version = "CMSSW_7_1_20_patch3",
        split_within_files = True,
        )

raw = CMSSWTask(
        sample = DirectorySample(
            location = lhe.get_outputdir(),
            dataset = lhe.get_sample().get_datasetname().replace("LHE","RAW"),
            ),
        open_dataset = True,
        files_per_output = 1,
        pset = "mcmaking/pset_raw.py",
        cmssw_version = "CMSSW_8_0_21",
        )

aod = CMSSWTask(
        sample = DirectorySample(
            location = raw.get_outputdir(),
            dataset = raw.get_sample().get_datasetname().replace("RAW","AOD"),
            ),
        open_dataset = True,
        files_per_output = 1,
        pset = "mcmaking/pset_aod.py",
        cmssw_version = "CMSSW_8_0_21",
        )

miniaod = CMSSWTask(
        sample = DirectorySample(
            location = aod.get_outputdir(),
            dataset = aod.get_sample().get_datasetname().replace("AOD","MINIAOD"),
            ),
        open_dataset = True,
        flush = True,
        files_per_output = 2,
        pset = "mcmaking/pset_miniaod.py",
        cmssw_version = "CMSSW_8_0_21",
        )

p = Path([lhe,raw,aod,miniaod])
p.process()

