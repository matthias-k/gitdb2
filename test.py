# -*- coding: utf-8 -*-


import unittest
import errno
import os
import shutil
import datetime
import codecs

import sqlalchemy as sa
from sqlalchemy import Column, Integer, String, DateTime, Boolean, ForeignKey, ForeignKeyConstraint
from sqlalchemy.orm import relationship, backref, attributes
import sqlalchemy.ext.declarative
from sqlalchemy.ext.orderinglist import ordering_list

from sqlalchemy.ext.associationproxy import association_proxy

from base import *
import data_types


#@sa.event.listens_for(sa.engine.Engine, "connect")
#def set_sqlite_pragma(dbapi_connection, connection_record):
    #cursor = dbapi_connection.cursor()
    #cursor.execute("PRAGMA foreign_keys=ON")
    #cursor.close()

#dbengine = 'sqlite'
#enginepath = '{engine}:///{databasename}'.format(engine=dbengine, databasename = 'test.db')

#engine = sqlalchemy.create_engine(enginepath, echo=True)
#Base = sqlalchemy.ext.declarative.declarative_base()

#class Test1(Base):
	#__tablename__ = 'test1'
	#id = Column(Integer, primary_key = True)
	#col2 = Column('komischer_name', String)
	#test2s = association_proxy('rels', 'test2')

#class Test2(Base):
	#__tablename__ = 'test2'
	#id = Column(Integer, primary_key = True)
	#col2 = Column(String)
	#test1s = association_proxy('rels', 'test1')

#class Rel(Base):
	#__tablename__ = 'rels'
	#test1_id = Column(Integer, ForeignKey('test1.id'), primary_key=True)
	#test2_id = Column(Integer, ForeignKey('test2.id'), primary_key=True)
	#test1 = relationship(Test1, backref=backref('rels', cascade="all, delete-orphan"), passive_updates=False)
	#test2 = relationship(Test2, backref=backref('rels', cascade="all, delete-orphan"), passive_updates=False)
	#def __init__(self, test2=None, test1=None):
		#self.test2=test2
		#self.test1=test1

class BaseSessionTest(unittest.TestCase):
	test_dir = 'unittest_repo'
	def setUp(self):
		self.Base = sqlalchemy.ext.declarative.declarative_base()
		os.makedirs(self.test_dir)
		sp.check_output(['git', 'init'], cwd=self.test_dir)
	def check_repository(self, repo_data):
		def check_directory(directory, data):
			files = os.listdir(directory)
			files = [f for f in files if not f in ['.git', 'test.db', 'dbcommit']]
			files = set(files)
			needed_files = set(data.keys())
			self.assertEqual(needed_files, files)
			for f in data:
				if isinstance(data[f], dict):
					check_directory(os.path.join(directory, f), data[f])
				elif data[f] is not None:
					self.assertEqual(codecs.open(os.path.join(directory, f), encoding='utf-8').read(), data[f])
		check_directory(self.test_dir, repo_data)
	def initSession(self):
		try:
			os.remove('test.db')
		except OSError as e:
			if not e.errno == errno.ENOENT:
				raise
		dbengine = 'sqlite'
		enginepath = '{engine}:///{databasename}'.format(engine=dbengine, databasename = '{0}/test.db'.format(self.test_dir))
		engine = sqlalchemy.create_engine(enginepath, echo=False)
		self.Base.metadata.create_all(engine)
		Session = sqlalchemy.orm.sessionmaker(bind=engine)
		self.session = Session()
		self.GitDBSession = GitDBSession(self.session, self.test_dir, Base=self.Base)
	def newSession(self):
		self.GitDBSession.close()
		self.session.close()
		del self.GitDBSession
		del self.session
		dbengine = 'sqlite'
		enginepath = '{engine}:///{databasename}'.format(engine=dbengine, databasename = '{0}/test.db'.format(self.test_dir))
		engine = sqlalchemy.create_engine(enginepath, echo=False)
		Session = sqlalchemy.orm.sessionmaker(bind=engine)
		self.session = Session()
		self.GitDBSession = GitDBSession(self.session, self.test_dir, Base=self.Base)
	def tearDown(self):
		self.GitDBSession.close()
		try:
			shutil.rmtree(self.test_dir)
			os.remove
		except OSError as e:
			if not e.errno == errno.ENOENT:
				raise
	def test_init_session(self):
		class Test(self.Base):
			__tablename__ = 'test'
			id = Column(Integer, primary_key = True)
			foo = Column(String)
		self.initSession()
		self.check_repository({})
	def test_add_object(self):
		class Test(self.Base):
			__tablename__ = 'test'
			id = Column(Integer, primary_key = True)
			foo = Column(String)
		self.initSession()
		test = Test()
		test.foo = 'probe'
		self.session.add(test)
		self.session.commit()
		self.check_repository({'test': {'1.txt': "id: 1\nfoo: probe\n"}})
	def test_change_object(self):
		class Test(self.Base):
			__tablename__ = 'test'
			id = Column(Integer, primary_key = True)
			foo = Column(String)
		self.initSession()
		test = Test()
		test.foo = 'probe'
		self.session.add(test)
		self.session.commit()
		self.check_repository({'test': {'1.txt': "id: 1\nfoo: probe\n"}})
		test.foo = 'probe2'
		self.session.commit()
		self.check_repository({'test': {'1.txt': "id: 1\nfoo: probe2\n"}})
		self.newSession()
		test1 = self.session.query(Test).get(1)
		self.assertEqual(test1.foo, 'probe2')
	def test_None(self):
		class Test(self.Base):
			__tablename__ = 'test'
			id = Column(Integer, primary_key = True)
			foo = Column(String)
		self.initSession()
		test = Test()
		test.foo = None
		self.session.add(test)
		self.session.commit()
		self.check_repository({'test': {'1.txt': u"id: 1\n"}})
	def test_remove_object(self):
		class Test(self.Base):
			__tablename__ = 'test'
			id = Column(Integer, primary_key = True)
			foo = Column(String)
		self.initSession()
		test = Test()
		test.foo = 'probe'
		self.session.add(test)
		self.session.commit()
		self.check_repository({'test': {'1.txt': "id: 1\nfoo: probe\n"}})
		self.session.delete(test)
		self.session.commit()
		self.check_repository({})
	def test_remove_objects_bulk(self):
		class Test(self.Base):
			__tablename__ = 'test'
			id = Column(Integer, primary_key = True)
			foo = Column(String)
		self.initSession()
		test = Test()
		test.foo = 'probe'
		self.session.add(test)
		self.session.commit()
		self.check_repository({'test': {'1.txt': "id: 1\nfoo: probe\n"}})
		self.newSession()
		with self.assertRaises(NotImplementedError):
			self.session.query(Test).delete()
		#self.session.commit()
		#self.check_repository({})
	def test_remove_objects_single(self):
		class Test(self.Base):
			__tablename__ = 'test'
			id = Column(Integer, primary_key = True)
			foo = Column(String)
		self.initSession()
		test = Test()
		test.foo = 'probe'
		self.session.add(test)
		self.session.commit()
		self.check_repository({'test': {'1.txt': "id: 1\nfoo: probe\n"}})
		self.newSession()
		self.session.delete(self.session.query(Test).get(1))
		self.session.commit()
		self.check_repository({})
	def test_multiple_primary_keys(self):
		class Test(self.Base):
			__tablename__ = 'test'
			id1 = Column(Integer, primary_key = True)
			id2 = Column(Integer, primary_key = True)
		self.initSession()
		test = Test()
		test.id1 = 10
		test.id2 = 20
		self.session.add(test)
		self.session.commit()
		self.check_repository({'test': {'10,20.txt': "id1: 10\nid2: 20\n"}})
	def test_change_primary_key(self):
		class Test(self.Base):
			__tablename__ = 'test'
			id = Column(Integer, primary_key = True)
			foo = Column(String)
		self.initSession()
		test = Test()
		test.id = 1
		test.foo = 'probe'
		self.session.add(test)
		self.session.commit()
		self.check_repository({'test': {'1.txt': "id: 1\nfoo: probe\n"}})
		test.id = 2
		self.session.commit()
		self.check_repository({'test': {'2.txt': "id: 2\nfoo: probe\n"}})
	def test_relationship(self):
		class Parent(self.Base):
			__tablename__ = 'parents'
			id = Column(Integer, primary_key = True)
		class Child(self.Base):
			__tablename__ = 'children'
			id = Column(Integer, primary_key = True)
			parent_id = Column(Integer, ForeignKey(Parent.id))
			parent = relationship(Parent, backref='children')
		self.initSession()
		parent = Parent()
		parent.children.append(Child())
		self.session.add(parent)
		self.session.commit()
		self.check_repository({'parents': {'1.txt': "id: 1\n"},
			'children': {'1.txt': "id: 1\nparent_id: 1\n"}})

	def test_association_proxy(self):
		class Test1(self.Base):
			__tablename__ = 'test1'
			id = Column(Integer, primary_key = True)
			test2s = association_proxy('rels', 'test2')
		class Test2(self.Base):
			__tablename__ = 'test2'
			id = Column(Integer, primary_key = True)
		class Rel(self.Base):
			__tablename__ = 'rel'
			test1_id = Column(Integer, ForeignKey(Test1.id), primary_key=True)
			test2_id = Column(Integer, ForeignKey(Test2.id), primary_key=True)
			test1    = relationship(Test1, backref='rels', passive_updates=False)
			test2    = relationship(Test2, backref='rels', passive_updates=False)
			def __init__(self, test2=None, test1=None):
				self.test1=test1
				self.test2=test2
		self.initSession()
		test1 = Test1()
		test1.test2s.append(Test2())
		self.session.add(test1)
		self.session.commit()
		self.check_repository({'test1': {'1.txt': "id: 1\n"},
			'test2': {'1.txt': "id: 1\n"},
			'rel': {'1,1.txt': "test1_id: 1\ntest2_id: 1\n"}})
		test1.id = 4
		self.session.commit()
		self.check_repository({'test1': {'4.txt': "id: 4\n"},
			'test2': {'1.txt': "id: 1\n"},
			'rel': {'4,1.txt': "test1_id: 4\ntest2_id: 1\n"}})

	def test_association_proxy_remove(self):
		class Test1(self.Base):
			__tablename__ = 'test1'
			id = Column(Integer, primary_key = True)
			test2s = association_proxy('rels', 'test2')
		class Test2(self.Base):
			__tablename__ = 'test2'
			id = Column(Integer, primary_key = True)
		class Rel(self.Base):
			__tablename__ = 'rel'
			test1_id = Column(Integer, ForeignKey(Test1.id), primary_key=True)
			test2_id = Column(Integer, ForeignKey(Test2.id), primary_key=True)
			test1    = relationship(Test1, backref=backref('rels', cascade="all, delete-orphan"), passive_updates=False)
			test2    = relationship(Test2, backref=backref('rels', cascade="all, delete-orphan"), passive_updates=False)
			def __init__(self, test2=None, test1=None):
				self.test1=test1
				self.test2=test2
		self.initSession()
		test1 = Test1()
		test1.test2s.append(Test2())
		self.session.add(test1)
		self.session.commit()
		self.check_repository({'test1': {'1.txt': "id: 1\n"},
			'test2': {'1.txt': "id: 1\n"},
			'rel': {'1,1.txt': "test1_id: 1\ntest2_id: 1\n"}})
		test2 = test1.test2s[0]
		test1.test2s.remove(test2)
		self.session.commit()
		self.check_repository({'test1': {'1.txt': "id: 1\n"},
			'test2': {'1.txt': "id: 1\n"}})
			#'rel': {'4,1.txt': "test1_id: 4\ntest2_id: 1\n"}})

	def test_types_string(self):
		class Test(self.Base):
			__tablename__ = 'test'
			id = Column(Integer, primary_key = True)
			foo = Column(String)
		self.initSession()
		test = Test()
		test.foo = 'probe'
		self.session.add(test)
		self.session.commit()
		self.check_repository({'test': {'1.txt': "id: 1\nfoo: probe\n"}})
	def test_types_integer(self):
		class Test(self.Base):
			__tablename__ = 'test'
			id = Column(Integer, primary_key = True)
			foo = Column(Integer)
		self.initSession()
		test = Test()
		test.foo = 12345
		self.session.add(test)
		self.session.commit()
		self.check_repository({'test': {'1.txt': "id: 1\nfoo: 12345\n"}})
	def test_types_datetime(self):
		class Test(self.Base):
			__tablename__ = 'test'
			id = Column(Integer, primary_key = True)
			foo = Column(DateTime)
		self.initSession()
		test = Test()
		test.foo = datetime.datetime(year=2013, month=9, day=30, hour=12, minute=30, second=12)
		self.session.add(test)
		self.session.commit()
		self.check_repository({'test': {'1.txt': "id: 1\nfoo: 2013-09-30_12-30-12\n"}})
	def test_types_integer(self):
		class Test(self.Base):
			__tablename__ = 'test'
			id = Column(Integer, primary_key = True)
			foo = Column(Boolean)
		self.initSession()
		test = Test()
		test.foo = True
		self.session.add(test)
		self.session.commit()
		self.check_repository({'test': {'1.txt': "id: 1\nfoo: True\n"}})
	def test_types_content(self):
		class Test(self.Base):
			__tablename__ = 'test'
			id = Column(Integer, primary_key = True)
			foo = Column(String)
			__content__ = 'foo'
		self.initSession()
		test = Test()
		test.foo = 'probe\nstring'
		self.session.add(test)
		self.session.commit()
		self.check_repository({'test': {'1.txt': "id: 1\n\nprobe\nstring"}})

class GitDBRepoTest(unittest.TestCase):
	test_dir = 'unittest_repo'
	def setUp(self):
		self.Base = sqlalchemy.ext.declarative.declarative_base()
		os.makedirs(self.test_dir)
		sp.check_output(['git', 'init'], cwd=self.test_dir)
	def check_repository(self, repo_data):
		def check_directory(directory, data):
			files = os.listdir(directory)
			files = [f for f in files if not f in ['.git', 'database.db', 'dbcommit']]
			files = set(files)
			needed_files = set(data.keys())
			self.assertEqual(needed_files, files)
			for f in data:
				if isinstance(data[f], dict):
					check_directory(os.path.join(directory, f), data[f])
				elif data[f] is not None:
					self.assertEqual(open(os.path.join(directory, f)).read(), data[f])
		check_directory(self.test_dir, repo_data)
	def initRepo(self):
		self.repo = GitDBRepo.init(self.Base, self.test_dir)
		self.session = self.repo.session
	def restartRepo(self, reloadDatabase=False):
		self.repo.close()
		if reloadDatabase:
			os.remove(os.path.join(self.test_dir, 'database.db'))
		self.repo = GitDBRepo(self.Base, self.test_dir)
		self.session = self.repo.session
	def tearDown(self):
		self.repo.close()
		try:
			shutil.rmtree(self.test_dir)
			os.remove
		except OSError as e:
			if not e.errno == errno.ENOENT:
				raise
	def test_init_repo(self):
		class Test(self.Base):
			__tablename__ = 'test'
			id = Column(Integer, primary_key = True)
			foo = Column(String)
		self.initRepo()
		self.check_repository({})
	def test_add_object(self):
		class Test(self.Base):
			__tablename__ = 'test'
			id = Column(Integer, primary_key = True)
			foo = Column(String)
		self.initRepo()
		test = Test()
		test.foo = 'probe'
		self.session.add(test)
		self.session.commit()
		self.check_repository({'test': {'1.txt': "id: 1\nfoo: probe\n"}})
		self.restartRepo(reloadDatabase=False)
		test = self.session.query(Test).one()
		self.assertEqual(test.foo, 'probe')
		self.restartRepo(reloadDatabase=True)
		test = self.session.query(Test).one()
		self.assertEqual(test.foo, 'probe')
	def test_None(self):
		class Test(self.Base):
			__tablename__ = 'test'
			id = Column(Integer, primary_key = True)
			foo = Column(String)
		self.initRepo()
		test = Test()
		test.foo = None
		self.session.add(test)
		self.session.commit()
		self.check_repository({'test': {'1.txt': "id: 1\n"}})
		self.restartRepo(reloadDatabase=False)
		test = self.session.query(Test).one()
		self.assertEqual(test.foo, None)
		self.restartRepo(reloadDatabase=True)
		test = self.session.query(Test).one()
		self.assertEqual(test.foo, None)
	def test_Colname_not_Attribute_name(self):
		class Test(self.Base):
			__tablename__ = 'test'
			id = Column(Integer, primary_key = True)
			foo = Column('_foo', String)
		self.initRepo()
		test = Test()
		test.foo = 'blub'
		self.session.add(test)
		self.session.commit()
		self.check_repository({'test': {'1.txt': "_foo: blub\nid: 1\n"}})
		self.restartRepo(reloadDatabase=False)
		test = self.session.query(Test).one()
		self.assertFalse(hasattr(test, '_foo'))
		self.assertEqual(test.foo, "blub")
		self.restartRepo(reloadDatabase=True)
		test = self.session.query(Test).one()
		self.assertFalse(hasattr(test, '_foo'))
		self.assertEqual(test.foo, "blub")
		
class TypeTests(unittest.TestCase):
	def test_bool(self):
		self.assertEqual(data_types.TypeManager.type_dict[sa.Boolean].to_string(True), 'True')
		self.assertEqual(data_types.TypeManager.type_dict[sa.Boolean].to_string(False), 'False')
		self.assertEqual(data_types.TypeManager.type_dict[sa.Boolean].to_string(None), 'None')

		self.assertEqual(data_types.TypeManager.type_dict[sa.Boolean].from_string('True'), True)
		self.assertEqual(data_types.TypeManager.type_dict[sa.Boolean].from_string('False'), False)
		self.assertEqual(data_types.TypeManager.type_dict[sa.Boolean].from_string('None'), None)

	def test_string(self):
		self.assertEqual(data_types.TypeManager.type_dict[sa.String].to_string('foo\nbar'), 'foo\\nbar')
		self.assertEqual(data_types.TypeManager.type_dict[sa.String].from_string('foo\\nbar'), 'foo\nbar')


if __name__ == '__main__':
	import sys
	#if len(sys.argv)>1:
	#	unittest.defaultTestLoader.testMethodPrefix = sys.argv[1]
	unittest.main(verbosity=2)
   
if False:
	if True:#session is None:
		import os
		if os.path.exists('test.db'):
			os.remove('test.db')
		import shutil
		shutil.rmtree('test')
		os.makedirs('test')
		sp.check_call(['git', 'init'], cwd='test')
		Base.metadata.create_all(engine)
		Session = sqlalchemy.orm.sessionmaker(bind=engine)
		session = Session()
		gitdbSession = GitDBSession(session, 'test')


	#event.listen(Session, "after_flush", my_after_flush)
	#event.listen(Session, "after_flush_postexec", my_after_flush)
	#event.listen(Session, "after_commit", my_after_commit)
	
	#def some_listener(mapper, connection, target):
	#	print "Instance %s being inserted" % target

	#def some_listener_delete(mapper, connection, target):
	#	print "Instance %s being deleted" % target
	#	deleted.append(target)

	# attach to all mappers
	#event.listen(mapper, 'after_delete', some_listener_delete)
	
	#def after_bulk_delete(session, query, query_context, result):
	#	print "AFTER_BULK"
	#	affected_table = query_context.statement.froms[0]
	#	print affected_table
	#	affected_rows = query_context.statement.execute().fetchall() 
	#	print affected_rows

	#sqlalchemy.event.listen(session, "after_bulk_delete", after_bulk_delete)
	
	test1 = Test1()
	test1.col2 = 'bla'
	test1.test2s.append(Test2())
	session.add(test1)
	session.commit()
	#change attribute
	test1.col2 = 'blub'
	session.commit()
	
	#change primary key
	test1.id = 4
	session.commit()
