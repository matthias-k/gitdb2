import sqlalchemy as sa
from sqlalchemy import Column, Integer, String, DateTime, Boolean, ForeignKey, ForeignKeyConstraint
from sqlalchemy.orm import relationship, backref, attributes
import sqlalchemy.ext.declarative
from sqlalchemy.ext.orderinglist import ordering_list

from sqlalchemy.ext.associationproxy import association_proxy

from base import *

dbengine = 'sqlite'
enginepath = '{engine}:///{databasename}'.format(engine=dbengine, databasename = 'test.db')

engine = sqlalchemy.create_engine(enginepath, echo=True)
Base = sqlalchemy.ext.declarative.declarative_base()

class Test1(Base):
	__tablename__ = 'test1'
	id = Column(Integer, primary_key = True)
	col2 = Column('komischer_name', String)
	test2s = association_proxy('rels', 'test2')

class Test2(Base):
	__tablename__ = 'test2'
	id = Column(Integer, primary_key = True)
	col2 = Column(String)
	test1s = association_proxy('rels', 'test1')

class Rel(Base):
	__tablename__ = 'rels'
	test1_id = Column(Integer, ForeignKey('test1.id'), primary_key=True)
	test2_id = Column(Integer, ForeignKey('test2.id'), primary_key=True)
	test1 = relationship(Test1, backref=backref('rels', cascade="all, delete-orphan"), passive_updates=False)
	test2 = relationship(Test2, backref=backref('rels', cascade="all, delete-orphan"), passive_updates=False)
	def __init__(self, test2=None, test1=None):
		self.test2=test2
		self.test1=test1

if __name__ == '__main__':
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
	def gitify_filename(filename):
		return filename.replace('test/', '')
	def getFilename(obj, old=True):
		old_primary_keys = []
		primary_keys = []
		for name in obj.__mapper__.columns.keys():
			col = obj.__mapper__.columns[name]
			if col.primary_key:
				history = attributes.get_history(obj, name)
				#print history
				if not history.deleted or history.deleted[0] == None:
					old_primary_keys.append(str(getattr(obj, name)))
				else:
					old_primary_keys.append(str(history.deleted[0]))
				primary_keys.append(str(getattr(obj, name)))
		oldprimarykey = ','.join(old_primary_keys)
		primarykey = ','.join(primary_keys)
		filename = 'test/{0}/{1}.txt'.format(obj.__tablename__, primarykey)
		oldfilename = 'test/{0}/{1}.txt'.format(obj.__tablename__, oldprimarykey)
		if old:
			return filename, oldfilename
		else:
			return filename
	def writeObject(obj):
		filename, oldfilename = getFilename(obj, old=True)
		if oldfilename!=filename:
			print "Primarykey changed from {0} to {1}!".format(oldfilename, filename)
			sp.check_call(['git', 'mv', gitify_filename(oldfilename), gitify_filename(filename)], cwd='test')
		try:
			os.makedirs(os.path.dirname(filename))
		except:
			pass
		with open(filename, 'w') as outfile:
			for name in obj.__mapper__.columns.keys():
				col_name = obj.__mapper__.columns[name].name
				line = '{0}: {1}\n'.format(col_name, getattr(obj, name))
				print line
				outfile.write(line)
		print filename
		sp.check_call(['git', 'add', gitify_filename(filename)], cwd='test')
	def deleteObject(obj):
		print "DELETE"
		filename, oldfilename = getFilename(obj, old=True)
		sp.check_call(['git', 'rm', gitify_filename(oldfilename)], cwd='test')
	def my_after_commit(session):
		global deleted
		deleted = []
		print "after commit!"
		print "New", session.new
		print "Dirty", session.dirty
		print "Deleted", session.deleted
		
		sp.check_call(['git', 'commit', '-m', 'somemessage'], cwd='test')
	def my_after_flush(session, flush_context):
		global deleted
		print "After Flush!"
		print "New", session.new
		print "Dirty", session.dirty
		print "Deleted", session.deleted
		for obj in session.deleted:
			deleteObject(obj)
		for obj in session.dirty:
			if obj in deleted:
				deleteObject(obj)
		for obj in session.dirty:
			if obj not in deleted:
				writeObject(obj)
		for obj in session.new:
			writeObject(obj)

	def my_after_flush_postexp(session, flush_context):
		global deleted
		print "After Flush!"
		print "New", session.new
		print "Dirty", session.dirty
		print "Deleted", session.deleted
		for obj in session.deleted:
			deleteObject(obj)
		for obj in session.dirty:
			if obj in deleted:
				deleteObject(obj)
		for obj in session.dirty:
			if obj not in deleted:
				writeObject(obj)
		for obj in session.new:
			writeObject(obj)

		

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
