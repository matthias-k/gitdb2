from __future__ import print_function, absolute_import, division, unicode_literals

from sqlalchemy.orm import attributes, sessionmaker
from sqlalchemy import event
import sqlalchemy as sa


import subprocess as sp
import os
import errno
import glob
import codecs
from time import sleep

from .data_types import TypeManager
from .git_handling import GitHandler
from pygit2 import Repository

import logging
logger = logging.getLogger(__name__)
#logger.setLevel(logging.ERROR)
logger.setLevel(logging.INFO)

"""
   In sqlite, one should use "passive_updates=False" for relationships,
   as sqlite does not cascade primary_key-updates. Also, if the database
   system does the updates, GitDB probably does not recognizes it (untested).

   Also, many2many relationships via a secondary table do not work
   in GitDB, as GitDB has no access to the secondary table and thus cannot store it.
   Use association_proxies instead. Do not forget to set 'cascade="all, delete-orphan"'
   in order for association_proxy.remove() to work.

   Bulk updates and bulk deletes are not supported at the moment (i.e., Query.update(),
   Query.delete(). This is because GitDB cannot get the precise rows updated or deleted.
   GitDB will raise an NotImplementedError if bulk updates or bulk deletes occur in its
   session. In a later version, this might be overcome by explicitly checking all objects
   of the respective table and e.g., delete all files without a table row.

   newlines in string columns are escaped to r'\n' when saved to file.
"""

#class MyFormatter(logging.Formatter):
  #""" from http://web.archive.org/web/20100107060919/http://tony.czechit.net/2009/02/unicode-support-for-pythons-logging-library/"""
  #def __init__(self, fmt=None, datefmt=None, encoding='windows-1250'):
    #logging.Formatter.__init__(self, fmt, datefmt)
    #self.encoding = encoding

  #def formatException(self, ei):
    #r = logging.Formatter.formatException(self, ei)
    #if type(r) in [types.StringType]:
      #r = r.decode('windows-1250', 'replace') # Convert to unicode
    #return r

  #def format(self, record):
    #t = logging.Formatter.format(self, record)
    #if type(t) in [types.UnicodeType]:
      #t = t.encode(self.encoding, 'replace')
    #return t

#console = logging.StreamHandler()
#console.setLevel(logging.DEBUG)
#formatter = MyFormatter(
  #fmt='%(name)s: %(message)s',
  #encoding='cp852')
#console.setFormatter(formatter)
#logger.addHandler(console)


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

class GitDBSession(object):
    def __init__(self, session, path, Base=None, async=True):
        #self.logger = logger.getChild('session')
        self.session = session
        self.new = set()
        self.deleted = set()
        self.dirty = set()
        self.path = path
        self.Base = Base
        self.active=True
        self.async = async
        self.git_handler = GitHandler(self.path, async=self.async)
        event.listen(session, "after_commit", self.after_commit)
        event.listen(session, "after_rollback", self.after_rollback)
        event.listen(session, "after_bulk_delete", self.after_bulk_delete)
        event.listen(session, "after_bulk_update", self.after_bulk_update)

        if self.Base:
            def register_class(klazz):
                if hasattr(klazz, '__mapper__'):
                    event.listen(klazz.__mapper__, 'after_delete', self.after_delete)
                    event.listen(klazz.__mapper__, 'after_insert', self.after_insert)
                    event.listen(klazz.__mapper__, 'after_update', self.after_update)
                for sub_klazz in klazz.__subclasses__():
                    register_class(sub_klazz)
            register_class(self.Base)
    def close(self):
        self.git_handler.join()
        self.active=False

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
            #self.logger.debug("Primarykey changed from {0} to {1}!".format(oldfilename, filename))
            self.git_handler.move_file(oldfilename, filename)
        real_filename = os.path.join(self.path, filename)

        #print real_filename

        output = ''

        #print "Starting outfile"
        if hasattr(obj, '__content__'):
            content_name = obj.__content__
        else:
            content_name = None
        for name in obj.__mapper__.columns.keys():
            if name == content_name:
                continue
            col_name = obj.__mapper__.columns[name].name
            value = getattr(obj, name)
            if value is None:
                continue
            for t in TypeManager.type_dict:
                if isinstance(obj.__mapper__.columns[name].type, t):
                    value_str = TypeManager.type_dict[t].to_string(value)
                    break
            #print "line", col_name
            #print value_str
            line = u'{0}: {1}\n'.format(col_name, value_str)
            #print line
            #self.logger.debug('%r' % line)
            output += line
            #print 'lab'
        if content_name:
            value = getattr(obj, content_name)
            output += '\n'
            output += value

        self.git_handler.write_file(filename, output)

    def deleteObject(self, obj):
        #self.logger.debug("DELETE")
        filename, oldfilename = self.getFilename(obj, old=True)

        self.git_handler.remove_file(oldfilename)


    def after_commit(self, session):
        if not self.active: return
        print("pre After commit!!!")
        self.git_handler.commit()

        print("post After commit!!!")
    def after_rollback(self, session):
        if not self.active: return
        self.git_handler.reset()

    def after_delete(self, mapper, connection, target):
        if not self.active: return
        #self.logger.debug("Instance %s being deleted" % target)
        self.deleteObject(target)
    def after_bulk_delete(self, session, query, query_context, result):
        if not self.active: return
        raise NotImplementedError('GitDB cannot yet handle bulk deletes!')
        affected_table = query_context.statement.froms[0]
        print(affected_table)
        #affected_rows = query_context.statement.execute().fetchall()
        affected_rows = session.execute(query_context.statement).fetchall()
        print(affected_rows)
        for res in  result.fetchall():
            print(res)
    def after_bulk_update(self, session, query, query_context, result):
        if not self.active: return
        raise NotImplementedError('GitDB cannot yet handle bulk updates!')
    def after_insert(self, mapper, connection, target):
        if not self.active: return
        #self.logger.debug("Instance %s being inserted" % target)
        self.writeObject(target)
    def after_update(self, mapper, connection, target):
        if not self.active: return
        #self.logger.debug("Instance %s being updated in %s" % (target, self))
        self.writeObject(target)

def construct_from_string(klazz, data):
    new_object = klazz()
    parts = data.split('\n\n', 1)
    if len(parts)>1:
        meta, content = parts
    else:
        meta, content = parts[0], None
    data = {}
    name_dict = {klazz.__mapper__.columns[attr_name].name: attr_name for attr_name in klazz.__mapper__.columns.keys()}
    for line in meta.split('\n'):
        if line:
            key, value=line.split(': ',1)
            if key in name_dict:
                col = klazz.__mapper__.columns[name_dict[key]]
                for t in TypeManager.type_dict:
                    if isinstance(col.type, t):
                        real_value = TypeManager.type_dict[t].from_string(value)
                        break
                setattr(new_object, name_dict[key], real_value)
    if content != None:
        setattr(new_object, new_object.__content__, content)
    return new_object

def construct_insert_values_from_string(klazz, data):
    values = {}
    parts = data.split('\n\n', 1)
    if len(parts)>1:
        meta, content = parts
    else:
        meta, content = parts[0], None
    table_cols = klazz.__table__.columns.keys()
    #name_dict = {klazz.__mapper__.columns[attr_name].name: attr_name for attr_name in klazz.__mapper__.columns.keys()}
    for line in meta.split('\n'):
        if line:
            key, value=line.split(': ',1)
            if key in table_cols:
                col = klazz.__table__.columns[key]# klazz.__mapper__.columns[name_dict[key]]
                for t in TypeManager.type_dict:
                    if isinstance(col.type, t):
                        real_value = TypeManager.type_dict[t].from_string(value)
                        break
                else:
                    raise TypeError(col.type)
                values[key] = real_value
                #setattr(new_object, name_dict[key], real_value)
    if content != None and hasattr(klazz, '__content__'):
        values[klazz.__content__] = content
    for key in table_cols:
        if key not in values:
            values[key] = None
        #setattr(new_object, new_object.__content__, content)
    return values

class GitDBRepo(object):
    def __init__(self, Base, path, dbname='database.db', async=True):
        self.Base = Base
        self.path = path
        self.repo = Repository(self.path)
        self.dbname = dbname
        self.async = async
        databasepath = os.path.join(self.path, self.dbname)
        if not os.path.exists(databasepath):
            self.startDatabase(refresh=True)
        else:
            try:
                with open(os.path.join(self.path, 'dbcommit')) as dbcommit_file:
                    content = dbcommit_file.read()
                    commit = content.split('\n')[0].strip()
            except IOError as e:
                if e.errno == errno.ENOENT:
                    commit = None
                else:
                    raise e
            if commit != self.getCurrentCommit():
                self.startDatabase(refresh=True)
            else:
                self.startDatabase(refresh=False)
    def startDatabase(self, refresh=False):
        databasename = os.path.join(self.path, self.dbname)
        if refresh:
            if os.path.exists(databasename):
                os.remove(databasename)
        makedirs(os.path.dirname(self.path))
        dbengine = 'sqlite'
        enginepath = '{engine}:///{databasename}'.format(engine=dbengine, databasename = databasename)

        self.engine = sa.create_engine(enginepath)
        if refresh:
            self.Base.metadata.create_all(self.engine)
        Session = sa.orm.sessionmaker(bind=self.engine)
        self.session = Session()
        if refresh:
            logging.info("Refreshing database")
            self.setup()
            self.saveCurrentCommit()
        else:
            logging.info("Reusing database")
        self.gitDBSession = GitDBSession(self.session, self.path,
                                         Base=self.Base,
                                         async=self.async)
    def setup(self):
        def read_class(klazz):
            insert_entries = []
            if hasattr(klazz, '__mapper__'):
                root_tree = self.repo[self.repo.head.target].tree
                if klazz.__tablename__ in root_tree:
                    sub_tree = self.repo[root_tree[klazz.__tablename__].id]
                    for tree_entry in sub_tree:
                        logging.debug("Reading blob %s/%s" % (klazz.__tablename__, tree_entry.name))
                        text = self.repo[tree_entry.id].data.decode('utf-8')
                        insert_entries.append(construct_insert_values_from_string(klazz, text))

                    if insert_entries:
                        self.session.execute(klazz.__table__.insert(), insert_entries)
            for sub_klazz in klazz.__subclasses__():
                read_class(sub_klazz)

        if self.repo.head_is_unborn:
            return
        read_class(self.Base)
        self.session.commit()
    def getCurrentCommit(self):
        out = self.gitCall(['rev-parse', 'HEAD'])
        return out.split('\n',1)[0].strip()
    def saveCurrentCommit(self):
        out, err = sp.Popen(['git', 'rev-parse', 'HEAD'], stdout = sp.PIPE, stderr=sp.PIPE, cwd = self.path).communicate()
        out = out.decode('utf-8', 'ignore')
        err = err.decode('utf-8', 'ignore')
        if err:
            if not 'unknown revision or path not in the working tree' in err:
                raise sp.CalledProcessError(err)
        else:
            with open(os.path.join(self.path, 'dbcommit'), 'w') as dbcommit_file:
                dbcommit_file.write(out.split('\n',1)[0]+'\n')
    def gitCall(self, args):
        return sp.check_output(['git']+args, cwd = self.path).decode('utf-8', 'ignore')
    def close(self):
        self.gitDBSession.close()
        self.session.close()
    @classmethod
    def init(cls, Base, path):
        makedirs(path)
        sp.check_output(['git', 'init'], cwd=path)
        return cls(Base, path)
