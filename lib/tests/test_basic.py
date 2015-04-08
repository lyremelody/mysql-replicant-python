# Copyright (c) 2010, Mats Kindahl, Charles Bell, and Lars Thalmann
# All rights reserved.
#
# Use of this source code is goverened by a BSD licence that can be
# found in the LICENCE file.

import sys, os.path
rootpath = os.path.split(os.path.dirname(os.path.abspath(__file__)))[0]
sys.path.append(rootpath) 

import mysql.replicant
import unittest

class TestPosition(unittest.TestCase):
    "Test case for binlog positions class."

    def __init__(self, methodName, options={}):
        super(TestPosition, self).__init__(methodName)

    def _checkPos(self, p):
        """Check that a position is valid and can be converted to
        string and back.

        """

        from mysql.replicant.server import Position
        self.assertEqual(p, eval(repr(p)))
        
    def testSimple(self):
        from mysql.replicant.server import Position
        positions = [Position('master-bin.00001', 4711),
                     Position('master-bin.00001', 9393),
                     Position('master-bin.00002', 102)]
 
        for position in positions:
            self._checkPos(position)

        # Check that comparison works as expected.
        for i, i_pos in enumerate(positions):
            for j, j_pos in enumerate(positions):
                if i < j:
                    self.assertTrue(i_pos < j_pos)
                elif i == j:
                    self.assertEqual(i_pos, j_pos)
                else:
                    self.assertTrue(i_pos > j_pos)

def suite(options={}):
    suite = unittest.TestSuite()
    for test in unittest.defaultTestLoader.getTestCaseNames(TestPosition):
        suite.addTest(TestPosition(test, options))
    return suite

if __name__ == '__main__':
    unittest.main(defaultTest='suite')
