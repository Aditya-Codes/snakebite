import unittest2
import os
import time
from snakebite.minicluster import MiniCluster
from snakebite.client import Client
from snakebite.platformutils import get_current_username


class MiniClusterTestBase(unittest2.TestCase):

    cluster = None
    username = get_current_username()
    trash_location = "/user/%s/.Trash/Current" % username

    @classmethod
    def setupClass(cls):
        if not cls.cluster:
            # Prevent running tests if a hadoop cluster is reachable. This guard
            # is in place because the MiniCluster java class can break things on
            # a production cluster. The MiniCluster python class is used, but doesn't
            # start an actual cluster. We only use convenience methods to call java
            # hadoop.

            c = MiniCluster(None, start_cluster=False)
            result = c.ls("/")
            if result:
                raise Exception("An active Hadoop cluster is found! Not running tests!")

            testfiles_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "testfiles")
            cls.cluster = MiniCluster(testfiles_path)
            cls.cluster.put("/test1", "/test1")
            cls.cluster.put("/test1", "/test2")
            cls.cluster.put("/test3", "/test3") #1024 bytes
            cls.cluster.put("/test1", "/test4")

            cls.cluster.mkdir('/empty_dir_1')
            cls.cluster.mkdir('/empty_dir_2')
            cls.cluster.mkdir('/empty_dir_3')
            cls.cluster.mkdir('/empty_dir_4')

            cls.cluster.mkdir('/empty_glob_dir')

            cls.cluster.mkdir("/zipped")
            cls.cluster.put("/zipped/test1.gz", "/zipped")
            cls.cluster.put("/zipped/test1.bz2", "/zipped")

            cls.cluster.put("/zerofile", "/")

            cls.cluster.mkdir("/dir1")
            cls.cluster.put("/zerofile", "/dir1")
            cls.cluster.mkdir("/dir2")
            cls.cluster.mkdir("/dir2/dir3")
            cls.cluster.put("/test1", "/dir2/dir3")
            cls.cluster.put("/test3", "/dir2/dir3")

            cls.cluster.mkdir("/foo/bar/baz", ['-p'])
            cls.cluster.put("/zerofile", "/foo/bar/baz/qux")
            cls.cluster.mkdir("/bar/baz/foo", ['-p'])
            cls.cluster.put("/zerofile", "/bar/baz/foo/qux")
            cls.cluster.mkdir("/bar/foo/baz", ['-p'])
            cls.cluster.put("/zerofile", "/bar/foo/baz/qux")
            cls.cluster.put("/log", "/")

            cls.cluster.mkdir("/sticky_dir")

    @classmethod
    def tearDownClass(cls):
        if cls.cluster:
            cls.cluster.terminate()

    def setUp(self):
        version = os.environ.get("HADOOP_PROTOCOL_VER", 9)
        self.cluster = self.__class__.cluster
        self.client = Client(self.cluster.host, self.cluster.port, int(version))

    def assertNotExists(self, location_under_test):
        self.assertFalse(self.client.test(location_under_test, exists=True))

    def assertExists(self, location_under_test):
        self.assertTrue(self.client.test(location_under_test, exists=True))

    def assertTrashExists(self):
        list(self.client.ls([self.trash_location]))

    def assertInTrash(self, location_under_test):
        self.assertTrashExists()
        trash_location = "%s%s" % (self.trash_location, location_under_test)
        self.assertTrue(self.client.test(trash_location, exists=True))

    def assertNotInTrash(self, location_under_test):
        self.assertTrashExists()
        trash_location = "%s%s" % (self.trash_location, location_under_test)
        self.assertFalse(self.client.test(trash_location, exists=True))


class MiniClusterSpecificPortTest(unittest2.TestCase):
    def test_explicit_port(self):
        c = MiniCluster(None, nnport=50050)
        self.assertEqual(50050, c.port)
        c.terminate()

if __name__ == '__main__':
    try:
        MiniClusterTestBase.setupClass()
        while True:
            time.sleep(5)
    finally:
        MiniClusterTestBase.cluster.terminate()
