gitdb2
======

This is a library that extends sqlalchemy databases to store their content in a directory of
files that is managed by git. This makes synchronizing databases much easier.

Example
-------


    import sqlalchemy as sa
    from sqlalchemy import Column, Integer, String, DateTime, Boolean, ForeignKey, ForeignKeyConstraint
    from sqlalchemy.orm import relationship, backref, attributes
    import sqlalchemy.ext.declarative
    from gitdb2 import *

    Base = sqlalchemy.ext.declarative.declarative_base()


    class Test(self.Base):
        __tablename__ = 'test'
        id = Column(Integer, primary_key = True)
        foo = Column(String)
    gitDBRepo = GitDBRepo.init(Base, 'test')
    session = gitDBRepo.session
    test = Test()
    test.foo = 'probe'
    session.add(test)
    session.commit()

    
More complex example
--------------------

    import sqlalchemy as sa
    from sqlalchemy import Column, Integer, String, DateTime, Boolean, ForeignKey, ForeignKeyConstraint
    from sqlalchemy.orm import relationship, backref, attributes
    import sqlalchemy.ext.declarative
    from gitdb2 import *

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

    gitDBRepo = GitDBRepo.init(Base, 'test')
    session = gitDBRepo.session
    test1 = Test1()
    test1.col2 = 'bla'
    test1.test2s.append(Test2())
    session.add(test1)
    #The commit will save the object to an file and commit it in git.
    session.commit()
    #change attribute
    test1.col2 = 'blub'
    session.commit()
    
    #changing primary keys does work as well, the files will be moved.
    test1.id = 4
    session.commit()