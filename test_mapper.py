# -*- coding: utf-8 -*-
#
# Copyright (c) 2009 Benoit Chesneau <benoitc@e-engura.com> 
#
# Permission to use, copy, modify, and distribute this software for any
# purpose with or without fee is hereby granted, provided that the above
# copyright notice and this permission notice appear in all copies.
#
# THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES
# WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
# MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR
# ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
# WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
# ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
# OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
#
__author__ = 'jannis.schnitzer@itisme.org (Jannis Andrija Schnitzer)'

from datetime import datetime
import unittest

from httplib2 import Http
from restclient.transport import HTTPLib2Transport
from couchdbkit.resource import ResourceNotFound

from mapper import Mapper
from couchdbkit.client import Server
from couchdbkit.schema import Document

class Greeting(object):
    def __init__(self, author, content):
        self.author  = author
        self.content = content
    def printout(self):
        print self.author+':', self.content
    def __eq__(self, other):
        return self.author == other.author and self.content == other.content

url = 'http://127.0.0.1:5984'
user = None
password = None
db_name = 'couchdbkit_test'

# alternatively
url = 'http://localhost:5984'
#user = 'username'
#password = 'topsecret'
#db_name = 'some_other_db'

class MapperTestCase(unittest.TestCase):
    def setUp(self):
        self.server = Server(url)
        self.server.create_db(db_name)
        self.db = Mapper(db_name, server=self.server)
        # TODO: create greeting/all view
        design_doc = {
            '_id': '_design/greeting',
            'language': 'javascript',
            'views': {
                'all': {
                    "map": """
function (doc)
{
    if (doc.doc_type == "Greeting")
        emit(doc._id, doc);
}
"""
                }
            }
        }
        self.db.save_doc(design_doc)
    
    def tearDown(self):
        self.server.delete_db(db_name)        
    
    def testMapping(self):
        # introduce our class to the mapper
        self.db.add(Greeting)
        self.assert_(issubclass(self.db.classes['Greeting'], (Greeting, Document)))
        # TODO: more stuff
        g = Greeting('Jannis', 'welcome to the mapping world')
        self.db['g'] = g
        mapped_g = self.db['g']
        self.assert_(mapped_g._id == 'g')
        self.assert_(isinstance(mapped_g, Greeting))
        self.assert_(g == mapped_g)
        greetings = [
            Greeting('Jannis', 'this is the first test greeting'),
            Greeting('Jannis again', 'this is the second test greeting')
        ]
        greetings[0]._id = 'first test greeting'
        greetings[1]._id = 'second test greeting'
        self.db.bulk_save(greetings)
        self.assert_([self.db['first test greeting'],
            self.db['second test greeting']] == greetings)
        h = Greeting('Xjs', 'Hi!')
        h2 = self.db.save_doc(h)
        h2_id = h2._id
        self.assertEquals(h, self.db[h2_id])
        self.db.delete_doc(h2)
        self.assertRaises(ResourceNotFound, self.db.__getitem__, h2_id)
        self.db.copy_doc(mapped_g, 'another_g')
        self.assertEquals(g, self.db['another_g'])
    
    def testViewResults(self):
        def frobnicate(greeting):
            """Helper function that returns a greeting which is ``frobnicated''
            (this simulates an action being performed).
            
            Used as test for the wrapper func - because Mapper.view overwrites
            the mapper func passed to Database.view with a custom one in order
            to give back mapped objects but still retain the ability to pass
            a custom wrapper function."""
            greeting.frobnicated = True
            return greeting
            
        # view generated in setUp
        all_greetings = self.db.view('greeting/all', wrapper=frobnicate)
        for greeting in all_greetings:
            # Test if we get a Greeting class back, not a Document or even dict
            self.assert_(isinstance(greeting, Greeting))
            # Test if custom wrapper function worked though Mapper mapped the
            # document (i. e. set a wrapper function itself)
            self.assert_(greeting.frobnicated)
    
if __name__ == '__main__':
    unittest.main()

