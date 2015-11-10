from Queue import Queue, Empty

from threading import Thread, Event
import signal

import os
import codecs
import subprocess as sp
import errno

def makedirs(dirname):
	"""Creates the directories for dirname via os.makedirs, but does not raise
	   an exception if the directory already exists and passes if dirname=""."""
	if not dirname:
		return
	try:
		os.makedirs(dirname)
	except OSError as e:
		if e.errno != errno.EEXIST:
			raise


class SynchronousGitHandler(object):
	def __init__(self, path):
		self.path = path
		self.written_files = []
		self.deleted_files = []
		print("Started git handler in ", self.path)

	def write_file(self, filename, content):
		real_filename = os.path.join(self.path, filename)
		makedirs(os.path.dirname(real_filename))
		do_write = True
		if os.path.isfile(real_filename):
			with codecs.open(real_filename, encoding='utf-8') as infile:
				if infile.read() == content:
					do_write = False
		if do_write:
			with codecs.open(real_filename, 'w', encoding='utf-8') as outfile:
				outfile.write(content)
			self.written_files.append(filename)

	def remove_file(self, filename):
		self.deleted_files.append(filename)
		self.gitCall(['rm', filename])

	def move_file(self, old_filename, new_filename):
		self.gitCall(['mv', old_filename, new_filename])

	def commit(self):
		if self.written_files or self.deleted_files:
			if self.written_files:
				self.gitCall(['add']+self.written_files)
				self.written_files = []
			self.deleted_files = []
			actions = self.gitCall(['status', '--porcelain'])
			actions = actions.replace('?? database.db\n', '')
			actions = actions.replace('?? dbcommit\n', '')
			if actions:
				self.gitCall(['commit', '-m', actions])
				self.saveCurrentCommit()

	def reset(self):
		if self.written_files:
			self.gitCall(['add']+self.written_files)
			self.written_files = []
		if os.path.isfile(os.path.join(self.path, 'dbcommit')):
			self.gitCall(['reset', '--hard', 'HEAD'])

	def getCurrentCommit(self):
		out = self.gitCall(['rev-parse', 'HEAD'])
		return out.split('\n',1)[0].strip()

	def saveCurrentCommit(self):
		with open(os.path.join(self.path, 'dbcommit'), 'w') as dbcommit_file:
			dbcommit_file.write(self.getCurrentCommit()+'\n')

	def gitCall(self, args):
		return sp.check_output(['git']+args, cwd = self.path)

class GitHandlerThread(Thread):
	def __init__(self, path, queue):
		self.path = path
		self.handler = SynchronousGitHandler(path)
		self.queue = queue
		#self.stopEvent
		super(GitHandlerThread, self).__init__()

	def run(self):
		while True:
			item = self.queue.get()
			#try:
			#	item = self.queue.get(timeout=0.1)
			#except Empty:
			#	continue
			cmd = item[0]
			if cmd == 'write_file':
				self.handler.write_file(item[1], item[2])
			elif cmd == 'remove_file':
				self.handler.remove_file(item[1])
			elif cmd == 'move_file':
				self.handler.move_file(item[1], item[2])
			elif cmd == 'commit':
				self.handler.commit()
			elif cmd == 'reset':
				self.handler.reset()
			else:
				raise ValueError('Unkown command:', item)
			self.queue.task_done()


class GitHandler(object):
	def __init__(self, path, async=True):
		self.path = path
		self.async = async
		if async:
			self.q = Queue()
			self.t = GitHandlerThread(self.path, self.q)
			self.t.daemon = True
			self.t.start()
			# TODO: start queue
		else:
			self.handler = SynchronousGitHandler(path)

	def write_file(self, filename, content):
		if self.async:
			self.q.put(('write_file', filename, content))
		else:
			self.handler.write_file(filename, content)

	def remove_file(self, filename):
		if self.async:
			self.q.put(('remove_file', filename))
		else:
			self.handler.remove_file(filename)

	def move_file(self, old_filename, new_filename):
		if self.async:
			self.q.put(('move_file', old_filename, new_filename))
		else:
			self.handler.move_file(old_filename, new_filename)

	def commit(self):
		if self.async:
			self.q.put(('commit',))
		else:
			self.handler.commit()

	def reset(self):
		if self.async:
			self.q.put(('reset',))
		else:
			self.handler.reset()

	def join(self):
		if self.async:
			while not self.q.empty():
				sleep(0.1)
			self.q.join()
