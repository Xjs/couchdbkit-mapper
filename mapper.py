# -*- coding: utf-8 -*-
#
# Copyright (c) 2009 Jannis Andrija Schnitzer <jannis.schnitzer@itisme.org>
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

from couchdbkit.client import Database
from couchdbkit.schema import SchemaProperties, Document

from couchdbkit.schema.base import check_reserved_words
from couchdbkit.schema import ReservedWordError

# TODO: works only for new-style classes, need a way to find determine
# old-style classes
def coerce_doc(document, _TemporaryClass=None):
    # The name was choosen in order not to conflict with the BIF `coerce'
    """Coerce the given document into something (a new-style class or object)
    that can be handled by CouchDBKit.
    
    Returns a 2-tuple of the new coerced class and the new document. The
    latter is None if `document' was a class itself."""    
    # `document' is either a class or an object of a class
    # In the latter case, we need to create a new Object of a subclass
    # of Document.
    # We assume if `document' is a class if it has the __base__ 
    # attribute.
    
    is_document = hasattr(document, '_db') # stolen from session.py
    if is_document:
        return document.__class__, document
    
    document_is_class = False
    
    if _TemporaryClass is None:
        document_is_class = hasattr(document, '__base__')
        if document_is_class:
            old_class = document
        else:
            old_class = document.__class__
        doc_type = old_class.__name__
        
        _TemporaryClass = SchemaProperties.__new__(SchemaProperties, doc_type, (Document, old_class), {})
        
    if not document_is_class:
        try:
            d = document.__dict__
        except AttributeError:
            d = document
        document = object.__new__(_TemporaryClass)
        Document.__init__(document, _d=d)
        return (_TemporaryClass, document)
    else:
        return (_TemporaryClass, None)

def dict_from_doc(doc):
    try:
        return doc.to_json()
    except AttributeError:
        return doc

def bulk_inner(obj, docs, _raw_json=False):
    """Return an iterator over the dictionaries of the given docs, needed
    for the bulk actions"""
    for doc in docs:
        if _raw_json or isinstance(doc, dict):
            yield doc
        else:
            yield obj.add(doc).to_json()
            
def map(database):
    return Mapper(database.server, database.dbname)

def inherit_documentation(method):
    "Add the parent class's docstring to the overwritten method"
    # TODO: this is very unflexible :-(
    method.__doc__ = getattr(Database, method.__name__).__doc__
    return method

class Mapper(Database):
    """Provide high-level mapping of CouchDB documents to Python objects.
    
    Usage:
        Call the mapper object with a class you'd like to map, or an object
        that should be made into a Document-based object.
        
        Otherwise, this behaves like a Session object, except that everything
        is mapped to Document-based (and your submitted) objects (i. e. view
        results, got and set documents, etc.).
    """
    
    # No WeakValueDictionary here, because sometimes you'd tell the mapper
    # about classes to map without further references to them
    classes = dict()
        
    def __init__(self, *args, **parameters):
        super(Mapper, self).__init__(*args, **parameters)
    
    # obsolete
    #def __call__(self, document):
    #    return self.add(document)
        
    def add(self, document):
        class_name = document.__class__.__name__
        if class_name not in self.classes:
            cls = None
        else:
            cls = self.classes[class_name]
        if not isinstance(document, Document):
            cls, document = coerce_doc(document, cls) # cls might be new now
            class_name = cls.__name__
        # sad that I have to check this twice, but I have no other chance
        if class_name not in self.classes:
            self.classes[class_name] = cls
        if document is None:
            document = cls
        document.set_db(self)
        return document
    
    def make_object(self, document):
        """Create a Document-based object out of ``document'' (which can be a
        dict or an object itself), using the previously mapped classes"""
        def is_reserved_word(word):
            try:
                check_reserved_words(word)
                # I think this is obsolete now
                #return word == 'rev'
                return False
            except ReservedWordError:
                return True
        
        if hasattr(document, '__dict__'):
            class_name = document.__class__.__name__
            document = document.__dict__
        else:
            try:
                class_name = document['doc_type']
            except KeyError:
                class_name = None

        filtered_dictionary = (
            (key, value) for (key, value) in document.iteritems() \
            if not is_reserved_word(key)
        )

        try:
            cls = self.classes[class_name]
        except KeyError:
            cls = Document

        assert(issubclass(cls, Document))
        _, result = coerce_doc(
            dict(filtered_dictionary), cls)
        
        try:
            result['_id'] = document['_id']
            result._doc['_rev'] = document['_rev']
            # when benoit has finished his changes:
            # result['_rev'] = document['_rev']
        except KeyError:
            pass
        except TypeError:
            # it is probably a design document, or otherwise has an invalid ID
            return document
        return result

# somehow useless:        
#    def contain(self, *docs):
#        def contain_inner(docs):
#            for doc in docs:
#                yield self.add(doc)
#        return super(Mapper, self).contain(*contain_inner(docs))
    
    def wrapper_maker(self, original_wrapper=None):
        def mapper_wrapper(obj):
            value = obj['value']
            value['_id'] = obj['id'] # for make_object
            if '_rev' not in value: # we might be the _all_docs view
                try:
                    value['_rev'] = obj['value']['rev']
                    del(value['rev'])
                except KeyError:
                    pass
            new_value = self.make_object(value)
            obj['value'] = new_value
            if callable(original_wrapper):
                return original_wrapper(obj)
            else:
                return obj['value']
        return mapper_wrapper

    def view_wrapper(self, view_name, temporary_view=False, obj=None, wrapper=None, **parameters):
        if obj is not None:
            if not hasattr(obj, 'wrap') or not callable(obj.wrap):
                raise AttributeError(''.join(["no 'wrap' method found in obj ", str(obj), "), or not callable"]))
            else:
                wrapper = obj.wrap
        # clarity of code!
        original_wrapper = wrapper
        
        if temporary_view:
            view_func = super(Mapper, self).temp_view
        else:
            view_func = super(Mapper, self).view
        return view_func(view_name, obj=None, wrapper=self.wrapper_maker(original_wrapper), **parameters)
    
    @inherit_documentation
    def view(self, view_name, **parameters):
        return self.view_wrapper(view_name, temporary_view=False, **parameters)
    
    @inherit_documentation
    def temp_view(self, design, **parameters):
        return self.view_wrapper(design, temporary_view=True, **parameters)
        
    @inherit_documentation
    def get(self, docid, rev=None, wrapper=None, _raw_json=False):
        def get_wrapper(obj):
            if not _raw_json:
                obj = self.make_object(obj)
            if wrapper is not None:
                if not callable(wrapper):
                    raise TypeError("wrapper isn't a callable")
                return wrapper(obj)
            else:
                return obj
        return super(Mapper, self).get(docid, rev=rev, wrapper=get_wrapper, _raw_json=_raw_json)
    
    # what we inherit from Database
    # copied and pasted, sad but true. But we need this special behavior
    @inherit_documentation
    def doc_revisions(self, docid, with_doc=True, _raw_json=False):
        result = super(Mapper, self).doc_revisions(docid=docid, with_doc=with_doc, _raw_json=_raw_json)
        if not with_doc or _raw_json:
            return result
        else:
            return self.make_object(result)
    
    @inherit_documentation
    def save_doc(self, doc, _raw_json=False, **params):
        if hasattr(doc, '__dict__'): # Is not a dictionary itself
            doc = self.add(doc)
            doc.save()
            return doc
        else:
            return super(Mapper, self).save_doc(doc, _raw_json=_raw_json, **params)
    
    @inherit_documentation
    def __setitem__(self, doc_id, doc):
        doc = self.add(doc)
        return super(Mapper, self).__setitem__(doc_id, doc)
    
    @inherit_documentation
    def bulk_save(self, docs, _raw_json=False, **parameters):
        return super(Mapper, self).bulk_save(bulk_inner(self, docs, _raw_json), **parameters)
    
    @inherit_documentation
    def bulk_delete(self, docs, **parameters):
        return super(Mapper, self).bulk_delete(bulk_inner(self, docs, _raw_json), **parameters)
    
    @inherit_documentation
    def delete_doc(self, doc, **parameters):
        return super(Mapper, self).delete_doc(dict_from_doc(doc), **parameters)
    
    @inherit_documentation
    def copy_doc(self, doc, dest=None, **parameters):
        return super(Mapper, self).copy_doc(dict_from_doc(doc), dest=dest, **parameters)
    
    @inherit_documentation
    def documents(self, wrapper=None, **parameters):
        return self.view('_all_docs', wrapper=wrapper, **parameters)
        
    iterdocuments = documents
    
