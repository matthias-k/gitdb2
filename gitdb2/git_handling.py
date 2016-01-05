from __future__ import print_function, division, absolute_import, unicode_literals

from six.moves.queue import Queue, Empty
from six import text_type, binary_type

from threading import Thread, Event
import signal

import os
import codecs
import subprocess as sp
import errno
from time import sleep
import warnings

from boltons.fileutils import mkdir_p

try:
    from pygit2 import Repository, GIT_FILEMODE_BLOB, GIT_FILEMODE_TREE, GIT_CHECKOUT_FORCE, Signature, Oid
    from pygit2 import hash as git_hash

    empty_tree_id = Oid(hex='4b825dc642cb6eb9a060e54bf8d69288fbee4904')
except ImportError:
    print("Could not import pygit2, trying without it")

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


def full_split(filename):
    head, tail = os.path.split(filename)
    parts = [tail]
    while head:
        head, tail = os.path.split(head)
        parts.insert(0, tail)
    return parts


def insert_into_tree(repo, tree, filename, oid, attr):
    """
    insert oid as filename into tree, possibly including subdirectories.
    Will return id of new tree.
    """
    parts = full_split(filename)
    assert len(parts) > 0
    if len(parts) > 1:
        #Create or get tree
        sub_directory = parts[0]
        sub_filename = os.path.join(*parts[1:])
        if tree is not None and sub_directory in tree:
            sub_tree = repo[tree[sub_directory].id]
        else:
            sub_tree = None

        oid = insert_blob_into_tree(repo, sub_tree, oid, sub_filename)
        mode = GIT_FILEMODE_TREE
        filename = sub_directory
    else:
        # insert in this tree
        mode = attr

    # do the actual insert
    if tree is None:
        tree_builder = repo.TreeBuilder()
    else:
        tree_builder = repo.TreeBuilder(tree)
    tree_builder.insert(filename, oid, mode)
    new_tree_id = tree_builder.write()
    return new_tree_id


def insert_blob_into_tree(repo, tree, blob_id, filename):
    """
    insert blob as filename into tree, possibly including subdirectories.
    Will return id of new tree.
    """
    return insert_into_tree(repo, tree, filename, blob_id, GIT_FILEMODE_BLOB)


def remove_file_from_tree(repo, tree, filename):
    """
    remove filename from tree, recursivly removing empty subdirectories.
    Will return id of new tree.
    """
    if tree is None:
        tree_builder = repo.TreeBuilder()
    else:
        tree_builder = repo.TreeBuilder(tree)

    parts = full_split(filename)
    assert len(parts) > 0
    if len(parts) > 1:
        sub_directory = parts[0]
        sub_filename = os.path.join(*parts[1:])
        sub_tree_entry = tree_builder.get(sub_directory)
        if not sub_tree_entry:
            return tree.id
        sub_tree = repo[sub_tree_entry.id]
        new_sub_tree_id = remove_file_from_tree(repo, sub_tree, sub_filename)
        new_sub_tree = repo[new_sub_tree_id]

        if new_sub_tree_id == empty_tree_id:
            filename = sub_directory
        else:
            tree_builder.insert(sub_directory, new_sub_tree_id, GIT_FILEMODE_TREE)
            filename = None

    # remove from this tree
    if filename and tree_builder.get(filename):
        tree_builder.remove(filename)
    new_tree_id = tree_builder.write()
    new_tree = repo[new_tree_id]
    return new_tree_id


def move_file_in_tree(repo, tree, old_filename, new_filename):
    print("{} -> {}".format(old_filename, new_filename))
    tree_entry = get_tree_entry(repo, tree, old_filename)
    if not tree_entry:
        raise ValueError('filename not in tree: {}'.format(old_filename))
    oid = tree_entry.id
    filemode = tree_entry.filemode

    new_tree_id = remove_file_from_tree(repo, tree, old_filename)
    new_tree = repo[new_tree_id]
    new_tree_id = insert_into_tree(repo, new_tree, new_filename, oid, filemode)
    #new_tree = repo[new_tree_id]

    return new_tree_id


def get_tree_entry(repo, tree, filename):
    """Recurse through tree. If filename is in tree, returns tree entry.
    Otherwise returns None"""
    parts = full_split(filename)
    if len(parts) == 1:
        name = parts[0]
        if name in tree:
            return tree[name]
        else:
            return None
    else:
        sub_directory = parts[0]
        sub_filename = os.path.join(*parts[1:])
        if sub_directory not in tree:
            return None
        sub_tree = repo[tree[sub_directory].id]
        return get_tree_entry(repo, sub_tree, sub_filename)


class LibGit2GitHandler(object):
    def __init__(self, path, repo_path=None, update_working_copy=True):
        self.path = path
        if repo_path is None:
            repo_path = self.path #os.path.join(self.path, 'repository')
        self.repo_path = repo_path
        self.update_working_copy = update_working_copy
        self.repo = Repository(self.repo_path)
        self.working_tree = self.get_last_tree()
        self.messages = []
        print("Started libgit2 git handler in ", self.path)

    def get_last_tree(self):
        if self.repo.head_is_unborn:
            tree_id = self.repo.TreeBuilder().write()
            return self.repo[tree_id]
        commit = self.repo[self.getCurrentCommit()]
        return commit.tree

    def insert_into_working_tree(self, blob_id, filename):
        tree_id = insert_blob_into_tree(self.repo, self.working_tree, blob_id, filename)
        self.working_tree = self.repo[tree_id]

    def remove_from_working_tree(self, filename):
        tree_id = remove_file_from_tree(self.repo, self.working_tree, filename)
        self.working_tree = self.repo[tree_id]

    def write_file(self, filename, content):
        # TODO: combine writing many files
        assert isinstance(content, text_type)
        data = content.encode('utf-8')
        existing_entry = get_tree_entry(self.repo, self.working_tree, filename)
        if existing_entry:
            type = 'M'
            if existing_entry.id == git_hash(data):
                return
        else:
            type = 'A'
        blob_id = self.repo.create_blob(data)
        self.insert_into_working_tree(blob_id, filename)

        if not self.repo.is_bare and self.update_working_copy:
            real_filename = os.path.join(self.path, filename)
            mkdir_p(os.path.dirname(real_filename))
            with codecs.open(real_filename, 'w', encoding='utf-8') as outfile:
                outfile.write(content)

        self.messages.append('    {}  {}'.format(type, filename))

    def remove_file(self, filename):
        existing_entry = get_tree_entry(self.repo, self.working_tree, filename)
        if existing_entry:
            self.remove_from_working_tree(filename)

            if not self.repo.is_bare and self.update_working_copy:
                real_filename = os.path.join(self.path, filename)
                os.remove(real_filename)

            self.messages.append('    D  {}'.format(filename))

    def move_file(self, old_filename, new_filename):
        tree_id = move_file_in_tree(self.repo, self.working_tree, old_filename, new_filename)
        self.working_tree = self.repo[tree_id]

        if not self.repo.is_bare and self.update_working_copy:
            real_old_filename = os.path.join(self.path, old_filename)
            real_new_filename = os.path.join(self.path, new_filename)
            mkdir_p(os.path.dirname(real_new_filename))
            os.rename(real_old_filename, real_new_filename)

        self.messages.append('    R  {} -> {}'.format(old_filename, new_filename))

    def commit(self):
        if self.repo.head_is_unborn:
            parents = []
        else:
            commit = self.repo[self.getCurrentCommit()]
            if commit.tree.id == self.working_tree.id:
                return
            parents = [commit.id]

        config = self.repo.config
        author = Signature(config['user.name'], config['user.email'])
        committer = Signature(config['user.name'], config['user.email'])
        tree_id = self.working_tree.id
        message = '\n'.join(self.messages)
        self.repo.create_commit('refs/heads/master',
                                author, committer, message,
                                tree_id,
                                parents)
        self.saveCurrentCommit()
        self.messages = []
        #if not self.repo.is_bare:
        #    self.repo.checkout_head(strategy=GIT_CHECKOUT_FORCE)

    def reset(self):
        self.working_tree = self.get_last_tree()
        self.messages = []

    def getCurrentCommit(self):
        return self.repo.head.target

    def saveCurrentCommit(self):
        with open(os.path.join(self.path, 'dbcommit'), 'w') as dbcommit_file:
            dbcommit_file.write(self.getCurrentCommit().hex+'\n')


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
            print("TYPE", type(actions))
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
        return sp.check_output(['git']+args, cwd = self.path).decode('utf-8', 'ignore')

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
            #    item = self.queue.get(timeout=0.1)
            #except Empty:
            #    continue
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
            self.handler = LibGit2GitHandler(path)
            #self.handler = SynchronousGitHandler(path)

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
