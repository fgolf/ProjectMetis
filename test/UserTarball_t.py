import unittest
import os

import metis.UserTarball as UserTarball
import metis.Utils as Utils

class UserTarballTest(unittest.TestCase):
    def test_make_tar(self):
        basedir = "/tmp/{0}/metis/tar_test/".format(os.getenv("USER"))
        tarname = "{0}/test.tar.gz".format(basedir)
        textname = "{0}/test.txt".format(basedir)

        Utils.do_cmd("mkdir -p {0}".format(basedir))

        ut = UserTarball.UserTarball(name=tarname)
        Utils.do_cmd("echo check > {0}".format(textname))
        ut.tarfile.add(textname)
        ut.close()

        self.assertEqual(Utils.do_cmd("tar xzOf {0}".format(tarname)).strip(), "check")

if __name__ == "__main__":
    unittest.main()
