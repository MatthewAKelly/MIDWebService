#!/usr/bin/python

# create user 
# db is created automatically
# CREATE USER 'mid'@'localhost' IDENTIFIED BY <INSERT PASSWORD HERE>;
# create database `mid`;
# GRANT ALL PRIVILEGES ON `mid`.*  TO 'mid'@'localhost';
# FLUSH PRIVILEGES;

from sqlalchemy import *

DB_PASSWORD = <INSERT PASSWORD HERE>

#database_file = "mid-hits.db"
#engine = create_engine('sqlite:///'+database_file)
#engine = create_engine('sqlite:///'+database_file)
engine = create_engine("mysql://mid:%s@localhost/MID2"%DB_PASSWORD)
connection = engine.connect()


from sqlalchemy import Table, Column, Integer, String, MetaData, ForeignKey, Sequence, Float, Boolean, DateTime
metadata = MetaData()
docs = Table('docs', metadata,
   Column('key', String(100), primary_key=True),
   Column('gskey', String(100)),
   Column('svm_score', Float()),
   Column('collection', String(100)),
   Column('headline', Text(convert_unicode=True)),
   Column('date', String(250)),
   Column('source', String(80,convert_unicode=True)),
   Column('dateline', String(60)),
   Column('byline', String(100)),
   Column('language', String(60)),
   Column('subject', String(100)),
   Column('organization', String(100)),
   Column('geographic', String(100)),
   Column('loaddate', String(100)),
   Column('pubtype', String(100)),
   Column('country1', String(60)),
   Column('country2', String(60)),
   Column('country3', String(60)),
   Column('country4', String(60)),
   Column('country5', String(60)),
   Column('nelloc1', String(60)),
   Column('nelloc2', String(60)),
   Column('nelloc3', String(60)),
   Column('nelloc4', String(60)),
   Column('nelloc5', String(60)),
   Column('mid_attr1', String(60)),
   Column('mid_attr2', String(60)),
   Column('mid_attr3', String(60)),
   Column('mid_attr4', String(60)),   
   Column('r1Action',      String(60)),
   Column('r2Action',      String(60)),
   Column('r3Action',      String(60)),
   Column('r4Action',      String(60)),
   Column('r1Initiator',      String(60)),
   Column('r2Initiator',      String(60)),
   Column('r3Initiator',      String(60)),
   Column('r4Initiator',      String(60)),
   Column('r1Targets',      String(60)),
   Column('r2Targets',      String(60)),
   Column('r3Targets',      String(60)),
   Column('r4Targets',      String(60)),
   Column('r1Date',      DateTime, nullable=True), 
   Column('r2Date',      DateTime, nullable=True), 
   Column('r3Date',      DateTime, nullable=True), 
   Column('r4Date',      DateTime, nullable=True), 
   Column('gsAction',     String(60)),
   Column('gsInitiator',  String(60)),
   Column('gsTargets',    String(60)),
   Column('gsActionNum',    Integer,   nullable=True),
   Column('gsInitiatorISO', String(4), nullable=True),
   Column('gsTargetISO',    String(4), nullable=True),
   Column('gsDate',       DateTime, nullable=True),   
   Column('correct',      Integer),
   Column('text',         Text(convert_unicode=True)),
)

assignments = Table('assignments', metadata,
   Column('id', Integer, Sequence('hits_id_seq'), primary_key=True),
   Column('amt_worker_id', String(50), nullable=True, index=True),
   Column('doc', ForeignKey('docs.key')),
   Column('token', String(20), nullable=True),   
   Column('started', DateTime, nullable=True),
   Column('reserved', DateTime, nullable=True),  # bundle reservation
   Column('completed', DateTime, nullable=True),
   Column('amt_token', String(10), nullable=True),   
   Column('bundle_id', Integer, index=True),
   Column('big_bundle_id', Integer,index=True),
   Column('initiator', String(100), nullable=True),
   Column('targets', String(100), nullable=True),
   Column('geogLocation', String(100), nullable=True),
   Column('geogState', String(100), nullable=True),
   Column('initiator', String(100), nullable=True),
   Column('date', DateTime, nullable=True),
   Column('initiatorApology', String(20), nullable=True),
   Column('targetProtest', String(20), nullable=True),
   Column('lineNumber', String(100), nullable=True),
   Column('targetSupport', String(20), nullable=True),
   Column('action', String(100), nullable=True),
   Column('initiatorDenial', String(20), nullable=True),
   Column('covertAction', Integer, nullable=True),
   Column('addActionLines', String(100), nullable=True),
   Column('initiatorFatalities', String(100), nullable=True),
   Column('targetFatalities', String(100), nullable=True),
   Column('groups', String(100), nullable=True),
   Column('initiatorRole', String(100), nullable=True),
   Column('targetRole', String(100), nullable=True),   
   Column('condition', ForeignKey('conditions.name')),
   Column('previous', ForeignKey('assignments.id')),
   Column('locked', Integer, nullable=True),
)

workers = Table('workers', metadata,
   Column('id', String(50), primary_key=True),
   Column('test_score', Integer),
   Column('condition', ForeignKey('conditions.name')),
   Column('level', Integer),
   Column('assignmentCounter', Integer, nullable=True),
   Column('testCounter', Integer, nullable=True),
   Column('cond2', Integer, nullable=True),
)

conditions = Table('conditions', metadata,
   Column('name', String(50), primary_key=True),
   Column('assigned', Integer),
   Column('workers', Integer),
)

