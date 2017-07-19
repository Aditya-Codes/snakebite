# -*- coding: utf-8 -*-
# Copyright (c) 2013 Spotify AB
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not
# use this file except in compliance with the License. You may obtain a copy of
# the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations under
# the License.
from snakebite.errors import FileNotFoundException
from snakebite.errors import InvalidInputException
from snakebite.errors import DirectoryException
from snakebite.platformutils import get_current_username
from minicluster_testbase import MiniClusterTestBase

import os
import re


class RmdirTest(MiniClusterTestBase):
    def test_delete_file_throws_exception(self):
        with self.assertRaises(DirectoryException):
            list(self.client.rmdir(['/zerofile']))

    def test_unknown_file(self):
        with self.assertRaises(FileNotFoundException):
            list(self.client.rmdir(['/doesnotexist']))

    def test_invalid_input(self):
        with self.assertRaises(InvalidInputException):
            list(self.client.rmdir('/stringpath'))

    def test_removing_non_empty_directory(self):
        with self.assertRaises(DirectoryException):
            list(self.client.rmdir(['/dir1']))

    def test_delete_multi(self):
        before_state = set([node['path'] for node in self.client.ls(['/'])])
        list(self.client.rmdir(['/empty_dir_1', '/empty_dir_2']))
        after_state = set([node['path'] for node in self.client.ls(['/'])])
        self.assertEqual(len(after_state), len(before_state) - 2)
        self.assertFalse('/empty_dir_1' in after_state or '/empty_dir_2' in after_state)

    def test_glob(self):
        list(self.client.rmdir(['/empty_glob_*']))
        client_output = self.client.ls(['/'])
        paths = [node['path'] for node in client_output]
        self.assertFalse('/empty_glob_dir' in paths)


class RmdirWithTrashTest(MiniClusterTestBase):
    def setUp(self):
        super(RmdirWithTrashTest, self).setUp()
        self.client.use_trash = True

    def test_delete_file(self):
        with self.assertRaises(DirectoryException):
            list(self.client.rmdir(['/zerofile']))

    def test_delete_multi(self):
        locations_under_test = ['/empty_dir_1', '/empty_dir_2']
        list(self.client.rmdir(locations_under_test))
        for location_under_test in locations_under_test:
            self.assertNotExists(location_under_test)
            self.assertInTrash(location_under_test)

    def test_removing_non_empty_directory(self):
        with self.assertRaises(DirectoryException):
            list(self.client.rmdir(['/dir1']))

    def test_glob(self):
        list(self.client.rmdir(['/empty_glob_*']))
        self.assertNotExists('/empty_glob_dir')
        self.assertInTrash('/empty_glob_dir')

    def test_path_in_trash(self):
        location_under_test = '/empty_dir_3'
        list(self.client.rmdir([location_under_test]))
        self.assertInTrash(location_under_test)
        list(self.client.rmdir(["%s%s" % (self.trash_location, location_under_test)]))
        self.assertNotInTrash(location_under_test)

    def test_delete_trash_parent_when_not_empty(self):
        list(self.client.rmdir(['/empty_dir_4']))
        try_path = [os.path.dirname(os.path.dirname(self.trash_location))]

        with self.assertRaises(DirectoryException):
            list(self.client.rmdir(try_path))

