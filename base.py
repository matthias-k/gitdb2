from sqlalchemy.orm import attributes
from sqlalchemy import event


import subprocess as sp
import os
import errno

"""
   In sqlite, one should use "passive_updates=False" for relationships,
   as sqlite does not cascade primary_key-updates. Also, if the database
   system does the updates, GitDB probably does not recognizes it (untested).
   
   Also, many2many relationships via a secondary table do not work
   in GitDB, as GitDB has no access to the secondary table and thus cannot store it.
   Use association_proxies instead. Do not forget to set 'cascade="all, delete-orphan"'
   in order for association_proxy.remove() to work.
"""

def makedirs(dirname):
	"""Creates the directories for dirname via os.makedirs, but does not raise
	   an exception if the directory already exists"""
	try:
		os.makedirs(dirname)
	except OSError as e:
		if e.errno != errno.EEXIST:
			raise

class GitDBSession(object):
	def __init__(self, session, path, Base=None):
		self.session = session
		self.new = set()
		self.deleted = set()
		self.dirty = set()
		self.path = path
		self.Base = Base
		event.listen(session, "after_commit", self.after_commit)
		if self.Base:
			for klazz in self.Base.__subclasses__():
				event.listen(klazz.__mapper__, 'after_delete', self.after_delete)
				event.listen(klazz.__mapper__, 'after_insert', self.after_insert)
				event.listen(klazz.__mapper__, 'after_update', self.after_update)
	def getFilename(self, obj, old=True):
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
		filename = '{0}/{1}.txt'.format(obj.__tablename__, primarykey)
		oldfilename = '{0}/{1}.txt'.format(obj.__tablename__, oldprimarykey)
		if old:
			return filename, oldfilename
		else:
			return filename
	def writeObject(self, obj):
		filename, oldfilename = self.getFilename(obj, old=True)
		if oldfilename!=filename:
			print "Primarykey changed from {0} to {1}!".format(oldfilename, filename)
			self.gitCall(['mv', oldfilename, filename])
		real_filename = os.path.join(self.path, filename)
		makedirs(os.path.dirname(real_filename))
		with open(real_filename, 'w') as outfile:
			for name in obj.__mapper__.columns.keys():
				col_name = obj.__mapper__.columns[name].name
				line = '{0}: {1}\n'.format(col_name, getattr(obj, name))
				print line
				outfile.write(line)
		print filename
		self.gitCall(['add', filename])
	def deleteObject(self, obj):
		print "DELETE"
		filename, oldfilename = self.getFilename(obj, old=True)
		self.gitCall(['rm', oldfilename])
	def after_commit(self, session):
		actions = self.gitCall(['status', '--porcelain'])
		if actions:
			self.gitCall(['commit', '-m', actions])
	def after_delete(self, mapper, connection, target):
		print "Instance %s being deleted" % target
		self.deleteObject(target)
	def after_insert(self, mapper, connection, target):
		print "Instance %s being inserted" % target
		self.writeObject(target)
	def after_update(self, mapper, connection, target):
		print "Instance %s being updated" % target
		self.writeObject(target)
	def gitCall(self, args):
		return sp.check_output(['git']+args, cwd = self.path)

