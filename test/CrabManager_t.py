import unittest
import os

from Constants import Constants
from Utils import do_cmd

class CrabManagerTest(unittest.TestCase):

    @unittest.skipIf("uaf-" not in os.uname()[1], "CRAB only testable on UAF")
    def test_config_parameters(self):

        from CrabManager import CrabManager

        basepath = "/tmp/{0}/metis/crab_test/".format(os.getenv("USER"))
        do_cmd("mkdir -p {0}".format(basepath))
        do_cmd("touch {0}/pset.py".format(basepath))
        dataset = "/TTZToLL_M-1to10_TuneCUETP8M1_13TeV-madgraphMLM-pythia8/RunIISummer16MiniAODv2-80X_mcRun2_asymptotic_2016_TrancheIV_v6-v1/MINIAODSIM"
        dataset_user = "/TTZToLL_M-1to10_TuneCUETP8M1_13TeV-madgraphMLM-pythia8/RunIISummer16MiniAODv2-80X_mcRun2_asymptotic_2016_TrancheIV_v6-v1/USER"
        request_name =  "test_metis_ttzlowmass"
        pset_location = "{0}/pset.py".format(basepath)

        cm1 = CrabManager(
                dataset=dataset,
                request_name=request_name,
                pset_location=pset_location,
                )
        cfg = cm1.get_crab_config()
        self.assertEqual(cfg.JobType.pluginName, "Analysis")
        self.assertEqual(cfg.JobType.psetName, pset_location)
        self.assertEqual(cfg.Data.inputDataset, dataset)
        self.assertEqual(cfg.Data.splitting, "FileBased")
        self.assertEqual(cfg.Data.inputDBS, "global")

        cm2 = CrabManager(
                dataset=dataset_user,
                request_name=request_name,
                pset_location=pset_location,
                plugin_name = "MyPlugin",
                )
        cfg = cm2.get_crab_config()
        self.assertEqual(cfg.Data.inputDataset, dataset_user)
        self.assertEqual(cfg.JobType.pluginName, "MyPlugin")
        self.assertEqual(cfg.Data.inputDBS, "phys03")


if __name__ == "__main__":
    unittest.main()
