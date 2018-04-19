import sys
import os
os.environ['PYTHON_EGG_CACHE'] = '/var/cache/.python-eggs' # a writable directory 
import re
import random
import string
from datetime import *

from sqlalchemy import *
from sqlalchemy.exc import OperationalError, IntegrityError
from sqlalchemy.orm import scoped_session
from sqlalchemy.orm import sessionmaker

from models import *


# WSGI stuff
from wsgiref.simple_server import make_server
from wsgiref.util import application_uri

MAX_ANNOTATIONS_PER_DOC = 50

LINK_PREFIX = "/mid-feed"

PASSWORD = <INSERT PASSWORD HERE>

START_LEVEL = 2 # initial level of new workers

SKIP_RESERVATIONS = False  # DISABLE BEFORE RUNNING BIG DATASET!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!


log_target = sys.stderr
def log(s):
    print >>log_target, "feed.py: ", s

log("feed.py started.")     
# request for...

from cgi import parse_qs, escape

def clean(s):
    #s = s[0:20] # Some countries are longer than 20 characters so we really don't want to do this.
    s = re.sub(r'[;\n\r \s\'"]', '', s)
    return s

from sqlalchemy import MetaData
from sqlalchemy.orm import scoped_session, sessionmaker


# TODO
# Multithreading still does not work.

# will produce errors if threads=15 in the wsgi.conf (apache config).

# in theory, SQLalchemy should provide thread-local sessions.
# unclear why that is the case.

# to do: move the functions below into a class so it operates on its own request.
# sort of done for the get_feed request (amtreq instead of self).


Session = scoped_session(sessionmaker())

class AMTAssignRequest:

    def __init__ (self):

        self.engine = create_engine("mysql://mdi:%s@localhost/MID2"%DB_PASSWORD)
        Session.configure(bind=self.engine)
        self.session = Session()
        self.connection = self.engine.connect()

    def terminate (self):
        self.connection.close()
        Session.remove()
    
# output
def get_assignment_with_id (aid, worker=None):
    workers_assignments = connection.execute(assignments.select(assignments.c.id==aid))
    
    a = workers_assignments.first()
    return a
    #if (not worker) or (a and a.amt_worker_id == worker):
    #    return a
    #return None

def mark_assignment_completed (amtreq, aid, worker, token):

    r = amtreq.connection.execute(assignments.select().where(and_(assignments.c.id==aid, assignments.c.token==token)))
    if r.returns_rows:
        a = r.fetchone()
        if a and a.amt_worker_id == worker:
            # OK
            # if a.completed != None:
            #     print "warning -  assignment %s for worker %s is already marked completed."%(aid,worker)

            
            key_token = make_token(5)
            
            q = assignments.update(synchronize_session=False).where(assignments.c.id==aid).values({assignments.c.completed:func.now(),assignments.c.amt_token:key_token})                                                                                                   
            amtreq.connection.execute(q)

            # unlock next in chain
            q2 = assignments.update(synchronize_session=False).where(assignments.c.previous==aid).values({assignments.c.locked:0})
            amtreq.connection.execute(q2)

            # check worker phase
            # first retrieve worker
            w = connection.execute(workers.select(workers.c.id==worker))

            if w:
                a = w.first()
                if a:
                    # check if the worker has a phase
                    if a.condition:
                        # if in the working phase, increment assignment counter
                        if a.condition == 'working':
                            if a.assignmentCounter:
                                newCount = a.assignmentCounter + 1
                            else:
                                newCount = 1
                            
                            # check if we should switch back to testing phase
                            # we test after the third story, and then every 8 stories after that
                            if ((newCount - 3) % 8) == 0:
                                newCond = 'testing'
                            elif a.level == 1:
                                newCond = 'testing'
                            else:
                                newCond = 'working'
                            
                            newVal = {}
                            newVal['condition'] = newCond
                            newVal['assignmentCounter'] = newCount                         
                            qq2  = workers.update(synchronize_session=False).where(workers.c.id==worker).values(newVal)
                            connection.execute(qq2)
                        elif a.condition == 'testing':
                            # check if we should switch to working phase or dismiss the worker for poor performance
                            newVal = {}
                            if a.testCounter:
                                newVal['testCounter'] = a.testCounter + 1
                                if a.level:
                                    if (a.level > 1) and (newVal['testCounter'] > 1):
                                        newVal['condition'] = 'working'
                                    elif (a.level == 0) and (newVal['testCounter'] > 1):
                                        newVal['condition'] = 'dismissed'
                            else:
                                newVal['testCounter'] = 1
                                newVal['assignmentCounter'] = 0
                            qq2  = workers.update(synchronize_session=False).where(workers.c.id==worker).values(newVal)
                            connection.execute(qq2)

            return key_token
        
    return False

def make_token (len):
    return ''.join(random.choice(string.ascii_uppercase + string.digits) for x in range(len))

def get_assignment (amtreq, worker, aid=None):
    "issues separate queries - let DB do any optimization"
    # any started, but not completed assignments?
    workers_assignments = amtreq.connection.execute(assignments.select(and_(assignments.c.amt_worker_id==worker, assignments.c.started!=None, assignments.c.completed==None)))

    for a in workers_assignments:
        # print "found existing assignment."
        return a

    # get the latest completed assignment
    workers_assignments = amtreq.connection.execute(select([assignments, func.max(assignments.c.completed)], and_(assignments.c.amt_worker_id==worker)))

    latest_completed = None
    if workers_assignments.rowcount:
        latest_completed = workers_assignments.fetchone()

    # retrieve the worker's experimental condition from the database
    condition = connection.execute(workers.select(workers.c.id==worker)).first().condition
    
    # take the first best assignment - we issue an update statement
    # here to take the not-yet started assignment do not just query -
    # otherwise we'd have to lock the database

    def take_assn(select):
        global MAX_ANNOTATIONS_PER_DOC
        # could be done  in a simple SQL statement... 
        # can't figure out how to do it with SQLalchemy
        # str = "update assignment set amt_worker_id='david' where id in (select min(id) from assignment)"
                
        try:
            amtreq.connection.execute("LOCK TABLES assignments WRITE")
            trans = connection.begin()

        except OperationalError:
            log("OperationalError while locking table.")
            pass
        
        try:

            r1 = amtreq.connection.execute(select)
            if r1.returns_rows:
                for i in range(0,MAX_ANNOTATIONS_PER_DOC):
                    row = r1.fetchone()
                    if not row:
                        return None
                    # have we started to do this document within a different task already?
                    r2 = amtreq.connection.execute(assignments.select().where(and_(assignments.c.id!=row.id, assignments.c.amt_worker_id==worker, assignments.c.doc==row.doc, assignments.c.started!=None)))
                    if not (r2.returns_rows and r2.fetchone()):
                        #  good - worker has not done this task before
                        if row:

                            key_token = make_token(10)
                            
                            # grab this particular assignment
                            amtreq.connection.execute(assignments.update(synchronize_session=False).where(assignments.c.id==row.id).values({assignments.c.started:func.now(), assignments.c.amt_worker_id:worker, assignments.c.token:key_token, assignments.c.reserved:func.now()}))
                            num_assn = 1
                            if not SKIP_RESERVATIONS:
                                # grab the whole bundle at this time (all assignments in this bundle that don't have a name on them) 
                                amtreq.connection.execute(assignments.update(synchronize_session=False).where(and_(assignments.c.bundle_id==row.bundle_id,assignments.c.amt_worker_id==None)).values({assignments.c.amt_worker_id:worker, assignments.c.reserved:func.now()}))
                                                            
                            # get updated assignment
                            row = amtreq.connection.execute(assignments.select().where(assignments.c.id==row.id)).fetchone()
                        return row
        except:
            trans.rollback()

        finally:
            trans.commit()
                            
            try:
                amtreq.connection.execute("UNLOCK TABLES")
            except OperationalError:
                pass
        
    # all assignments with my name on them in my condition
    q = assignments.select().where(and_(assignments.c.amt_worker_id==worker, assignments.c.started==None, assignments.c.completed==None, assignments.c.condition==condition))
    a = take_assn(q)
    if a: 
        return a
        
    # all incomplete assignments in the worker's experimental condition and isn't locked, ordered randomly
    q = assignments.select().order_by(func.rand()).where(and_(assignments.c.started==None, assignments.c.completed==None, assignments.c.condition==condition, assignments.c.locked!=1))
    #q = assignments.select().order_by(func.rand()).where(and_(assignments.c.amt_worker_id==None, assignments.c.started==None, assignments.c.completed==None, assignments.c.condition==condition, assignments.c.locked!=1))

    # prefer same bundle
    if latest_completed:
        a = take_assn(q.where(assignments.c.bundle_id==latest_completed.bundle_id))
        if a:
            return a
        a = take_assn(q.where(assignments.c.big_bundle_id==latest_completed.big_bundle_id))
        if a:
            return a

    # if this is the first call, or we just got finished with a big bundle,
    # let's try to use a bundle that is different from the one assigned last (to spread out workers)
    # (to do)
    # here, we'll just take anything
    a = take_assn(q)
    if a:
        return a

    # all incomplete assignments that aren't locked and are not assigned an experimental condition, ordered randomly
    #q = assignments.select().order_by(func.rand()).where(and_(assignments.c.amt_worker_id==None, assignments.c.started==None, assignments.c.completed==None, assignments.c.condition==None, assignments.c.locked!=1))
    q = assignments.select().order_by(func.rand()).where(and_(assignments.c.started==None, assignments.c.completed==None, assignments.c.condition==None, assignments.c.locked!=1))

    # prefer same bundle
    if latest_completed:
        a = take_assn(q.where(assignments.c.bundle_id==latest_completed.bundle_id))
        if a:
            return a
        a = take_assn(q.where(assignments.c.big_bundle_id==latest_completed.big_bundle_id))
        if a:
            return a

    # if this is the first call, or we just got finished with a big bundle,
    # let's try to use a bundle that is different from the one assigned last (to spread out workers)
    # (to do)
    # here, we'll just take anything
    a = take_assn(q)
    if a:
        return a

    # print "nothing found - no assignment taken"
    return None

def make_error_rss(s, id=None):
    # Create the feed
    feed = Feed()

    # Set the feed/channel level properties
    feed.feed["title"] = "STATUS - MID-coding on Mechanical Turk"
    feed.feed["link"] = "http://www.psu.edu"
    feed.feed["author"] = "The Pennsylvania State University"
    feed.feed["description"] = u"%s"%s
    item = {}
    item["title"] = u"%s"%s
    if id:
        item["mid:id"] = u"%s"%id
    item["source"] = u""
    feed.items.append(item)
    return feed.format_rss2_string()

def doc_for_doc(a):
    
    if a and 'doc' in a:
        doc = a.doc
    else:
        doc = a        
    if isinstance(doc, basestring):
        ds = connection.execute(docs.select().where(docs.c.key==doc)).first()
    else:
        ds = doc
    s = '<html><style type="text/css">ol.code > li { white-space: pre-wrap; }</style><body>'
    s = s + '<h1>%s</h1><p>%s</p><pre><ol class="code">'%(ds.headline,ds.date)
    lines = ds.text.split('\n')
    for line in lines:
        s = s + '<li>%s</li>'%(line)
    s = s + '</ol></pre></body>'
    return str(s)
        

from feedformatter import Feed, _rss2_item_mappings
import time
def feed_for_docs(alist, appuri=""):  # takes a list of assignments, document IDs, or documents
    global LINK_PREFIX
    # must give assignments
    # Create the feed
    feed = Feed()

    # Set the feed/channel level properties
    feed.feed["title"] = "MID-coding on Mechanical Turk"
    feed.feed["link"] = "http://www.psu.edu"
    feed.feed["author"] = "The Pennsylvania State University"
    feed.feed["description"] = "An experiment - document retrieval"

    print "feed for docs alist=", alist
    for a in alist:
        aid = None
        atoken = None
        if 'doc' in a:
            doc = a.doc
            aid = a.id
            atoken = a.token
        else:
            doc = a
            
        if isinstance(doc, basestring):
            ds = connection.execute(docs.select().where(docs.c.key==doc))
        else:
            ds = [doc]

        if not ds:
            # print "ERROR - could not cross-reference document ", doc
            # print "- this is missing from docs table."
            return make_error_rss("ERROR - could not cross-reference document %s"%doc)
        for d in ds:
            # Create an item
#             item = {}
#             item["source"] = u""+d.source.rstrip()
#             item["mid:file"] = d.filename.rstrip()
#             item["mid:meta_date"] = d.meta_date.rstrip()
#             item["mid:meta_class"] = u"%s"%d.meta_class
#             item["mid:host_level"] = u"%s"%d.host_level
#             i = 0
#             for e in d.meta_entities.split(","):
#                 item["mid:meta_entity_%s"%i] = e
#                 i+=1
# 
            item = {}
            for (key, value) in d.items():
                if not isinstance(value, float):
                    if value is None:
                        item["mid:"+key] = 'none'
                    elif isinstance(value, ( int, long ) ):
                        item["mid:"+key] = str(value)
                    elif isinstance(value, datetime):
                        item["mid:"+key] = value.strftime('%B %-d, %Y')
                    else:
                        item["mid:"+key] = value
            item["title"] = u""+d.headline.rstrip()#.decode("utf8")
            #date = d.gskey.split('--')[0]
            #item["date"] = int(datetime.datetime.strptime(date, '%Y%m%d').strftime('%s'))
            item["link"] = u""+appuri+"?aid=%s&request=doc"%aid
            item["mid:token"] = u"%s"%atoken
            item["mid:aid"] = u"%s"%aid
            if not LINK_PREFIX:
                item["mid:text"] = u""+d.text #.decode("utf8")
            feed.items.append(item)
    # print len(feed.items)
    return feed.format_rss2_string()


from sqlalchemy import inspect
def object_as_dict(obj):
    dct = {}
    for c in inspect(obj).mapper.column_attrs:
        dct[c.key] = getattr(obj, c.key)
    return dct

def get_feed_request(amtreq, env):

    dqs = parse_qs(env['QUERY_STRING'])

    def getenv (key):
        val = dqs.get(key, [''])[0]
        val = escape(clean(val))
        
        return val
    
    worker= getenv('worker')
    assn_id = getenv('aid')
    request = getenv('request')
    password = getenv('password')

    # Request a document
    # mid-feed?request=doc&assn_id=2135

    # Request worker info
    # mid-feed?request=worker&worker=johnny78&password=<INSERT PASSWORD HERE>
    # (replies with RSS, STATUS feed, TITLE tag of first item gives 0 (not qualified), 1 (qualified), N/A (unknown)

    # Set worker info
    # mid-feed?request=setworker&worker=johnny78&qualified=1&password=<INSERT PASSWORD HERE>
    # use 1 for qualified, 0 for disqualified.
    # reply as in "request worker info"

    # Get next or current assignment with document
    # mid-feed?worker=johnny78&password=<INSERT PASSWORD HERE>
    # Gives RSS feed with one item (the document)

    # Mark assignment as completed
    # mid-feed?worker=johnny78&aid=234&token=ASDJHLWJASDHN&completed=1&password=<INSERT PASSWORD HERE>
    # token, aid and worker must match.
    # replies with RSS STATUS feed, TITLE tag of first item is short key code to be used as AMT confirmation code.
    
    global PASSWORD
    if request == "doc" or password == PASSWORD:
        if request == "doc":
            return 'text/html',get_doc(worker, assn_id)
        elif request == "worker":
            return 'application/rss+xml',get_worker_info(worker)
        elif request == "setworker":
            qualities = {}
            qualities['test_score']        = getenv('test_score')
            qualities['level']             = getenv('level')

            # remove blank fields
            cleanQualities = {}
            for field in qualities:
                if qualities[field] != '':
                    cleanQualities[field] = qualities[field]
            log(cleanQualities)

            return 'application/rss+xml',set_worker_info(worker, cleanQualities)
        elif request == "gettest":
            return 'application/rss+xml',get_test_score(worker)
        elif request == "settest":
            test_score = getenv('score')
            return 'application/rss+xml',set_test_score(worker, test_score)
        elif request == "getresponse":
            return 'application/rss+xml',get_worker_response(assn_id)
        elif request == "setresponse":
            assn_token = getenv('token')
        
            response = {}
            response['initiator']           = getenv('initiator')
            response['targets']             = getenv('targets')
            response['geogLocation']        = getenv('geogLocation')
            response['geogState']           = getenv('geogState')
            response['date']                = getenv('date')
            response['initiatorApology']    = getenv('initiatorApology')
            response['targetProtest']       = getenv('targetProtest')
            response['lineNumber']          = getenv('lineNumber')
            response['targetSupport']       = getenv('targetSupport')
            response['action']              = getenv('action')
            response['initiatorDenial']     = getenv('initiatorDenial')
            response['covertAction']        = getenv('covertAction')
            response['addActionLines']      = getenv('addActionLines')
            response['initiatorFatalities'] = getenv('initiatorFatalities')
            response['targetFatalities']    = getenv('targetFatalities')
            response['groups']              = getenv('groups')
            response['initiatorRole']       = getenv('initiatorRole')
            response['targetRole']          = getenv('targetRole')
            
            # remove blank fields
            cleanResponse = {}
            for field in response:
                if response[field] != '':
                    cleanResponse[field] = response[field]
            log(cleanResponse)
            return 'application/rss+xml',set_worker_response(assn_id, assn_token, cleanResponse)
        elif request == "status":
            qualified = getenv('qualified')
            completed = getenv('completed')
            table = getenv('table')
            return 'text/html',get_status(worker=worker, qualified=qualified, completed=completed, table=table)
        else:    
            completed = getenv('completed')
            assn_token = getenv('token')
            return 'application/rss+xml',get_feed(amtreq, worker, assn_id, assn_token, completed, application_uri(env))
    else:
        return 'application/rss+xml', make_error_rss("Must give correct password for this request.")
        

def get_doc(worker, assn_id):

    log("getting doc for worker %s"%(worker))
    if  assn_id:
        a = get_assignment_with_id(assn_id, worker)
        if a:
            return doc_for_doc(a)
        else:
            return "no document with id %s"%assn_id
        
    return make_error_rss("No worker / aid parameters given to web service")

def get_test_score(worker):
    if worker:
        w = connection.execute(workers.select(workers.c.id==worker))

        if w:
            
            a = w.first()
            if a:
                return make_error_rss(a.test_score, worker)
                
        return make_error_rss("N/A", worker)

    else:
        return make_error_rss("No worker parameter given to web service")

def set_test_score(worker, test_score):
    if worker and test_score:
        w = connection.execute(workers.select(workers.c.id==worker)).first()
        if not w:
            qq = workers.insert().values(id=worker)
            connection.execute(qq)

        qq2 = workers.update(synchronize_session=False).where(workers.c.id==worker).values(test_score=test_score)
        connection.execute(qq2)
        return get_worker_info(worker)
    else:
        return make_error_rss("No worker parameter given to web service")

def get_worker_info(worker):
    if worker:
        w = connection.execute(workers.select(workers.c.id==worker))

        if w:   
            a = w.first()
            if a:
                # check if the worker has an experimental condition
                if a.condition:
                    return make_item_rss("Worker info", a)
                # if no experimental condition, then assign one:
                else:
                    cond = pick_worker_condition()
                    qq2  = workers.update(synchronize_session=False).where(workers.c.id==worker).values(condition=cond)
                    connection.execute(qq2)
                    return get_worker_info(worker)
        
        # else: add worker to worker table
        cond  = pick_worker_condition()
        cond2 = random.randint(1,3)
        qq   = workers.insert().values(id=worker, condition=cond, level=START_LEVEL, cond2=cond2)
        connection.execute(qq)
        return get_worker_info(worker)

    else:
        return make_error_rss("No worker parameter given to web service")

# # Assign a worker to a random-ish experimental condition
# def pick_worker_condition():
#     # get a random condition
#     allConditions = connection.execute(conditions.select().order_by(func.rand()))
# 
#     for row in allConditions:
#         # select all assignments from the selected condition that haven't been assigned and aren't locked
#         q = assignments.select().where(and_(assignments.c.amt_worker_id==None, assignments.c.started==None, assignments.c.completed==None, assignments.c.condition==row.name, assignments.c.locked!=1))
#         freeAssignments = connection.execute(q)
#         if freeAssignments.first() != None:
#             selectedCondition = row.name
#             break
#     
#     # update that condition with an additional worker
#     qq  = conditions.update().where(conditions.c.name==selectedCondition).values(workers=(row.workers+1))
#     connection.execute(qq)
#     # return name of condition
#     # print 'pick_worker_condition: ',row.name, minAssigned
#     return selectedCondition

def pick_worker_condition():
    return "testing"

def make_item_rss(title, row):
    # Create the feed
    feed = Feed()

    # Set the feed/channel level properties
    feed.feed["title"] = "STATUS - MID-coding on Mechanical Turk"
    feed.feed["link"] = "http://www.psu.edu"
    feed.feed["author"] = "The Pennsylvania State University"
    feed.feed["description"] = "An experiment - row retrieved"
    item = {}
    for (key, value) in row.items():
        item["mid:"+str(key)] = str(value)
    item["title"] = u"%s"%title
    feed.items.append(item)
    return feed.format_rss2_string()


from feedformatter import Feed, _rss2_item_mappings
import time
def get_worker_response(assn_id):
    if assn_id:
        # Create the feed
        feed = Feed()

        # Set the feed/channel level properties
        feed.feed["title"] = "MID-coding on Mechanical Turk"
        feed.feed["link"] = "http://www.psu.edu"
        feed.feed["author"] = "The Pennsylvania State University"
        feed.feed["description"] = "An experiment - saved response"
        existing = connection.execute(assignments.select(assignments.c.id==assn_id)).first()
        if existing:
            item = {}
            for (key, value) in existing.items():
                item["mid:"+str(key)] = str(value)
            item["title"]       = str(assn_id)
            item["description"] = 'saved response' 
            feed.items.append(item)
            return feed.format_rss2_string()
        else:
            return make_error_rss("No assignment with requested ID")
    else:
        return make_error_rss("Assignment ID not given to web service")

def set_worker_response(assn_id, token, response):
    if assn_id:
        existing = connection.execute(assignments.select(and_(assignments.c.id==assn_id, assignments.c.token==token)))

        if existing:
            a = existing.first()
            if a.action:
                alreadyAnnotated = True
            else:
                alreadyAnnotated = False
            
            ins    = assignments.update().where(assignments.c.id==assn_id).values(response)
            result = engine.execute(ins)
            # check the correctness of the response
            if (a.condition == 'testing') and not(alreadyAnnotated):
                check_correctness(assn_id, response)
        else:
            return make_error_rss("No assignment with requested ID or token. Maybe you took to long?")
        return get_worker_response(assn_id)
    else:
        return make_error_rss("Assignment ID not given to web service")

# check the correctness of the response
def check_correctness(assn_id, response):
    # retrieve assignment
    list_of_assn = connection.execute(assignments.select(assignments.c.id==assn_id))
    if list_of_assn:
        a = list_of_assn.first()
        if a:
            worker_id = a.amt_worker_id
            doc_id    = a.doc
            # retrieve document
            list_of_doc = connection.execute(docs.select(docs.c.key==doc_id))
            if list_of_doc:
                d = list_of_doc.first()
                if d:
                    # evaluate correctness, assume wrong by default
                    correctness = 0
                    wrongness   = 1
                    
                    # parse action string
                    if '(' in response['action']:
                        # convert string to int (e.g., 'Clash (17)' to 17)
                        junk,mid_level      = response['action'].split('(') 
                        mid_level,junk      = mid_level.split(')')
                        mid_level           = int(mid_level)
                    else:
                        # convert '17' to 17
                        mid_level = int(response['action'])   
                    if mid_level == d.gsActionNum:
                        # if there's no MID, then initiator and target don't matter
                        if d.gsActionNum == 0:
                            correctness = 1
                            wrongness   = 0
                        else:
                            workerIni = "".join(response['initiator'].lower().split())
                            goldIni   = "".join(d.gsInitiator.lower().split())
                            if workerIni == goldIni:
                                workerTar = "".join(response['targets'].lower().split())
                                goldTar   = "".join(d.gsTargets.lower().split())
                                if workerTar == goldTar:
                                    correctness = 1
                                    wrongness   = 0
                
                    # retrieve worker
                    list_of_work = connection.execute(workers.select(workers.c.id==worker_id))
                    if list_of_work:
                        w = list_of_work.first()
                        if w:
                            # increase / decrease test_score
                            newVal = {}
                            if w.test_score:
                                newVal['test_score'] = w.test_score + correctness
                            else:
                                newVal['test_score'] = 0 + correctness
                            # keep test_score within bounds
                            #if newVal['test_score'] > 3:
                            #    newVal['test_score'] = 3
                            #elif newVal['test_score'] < 0:
                            #    newVal['test_score'] = 0
                            # increase / decrease level
                            if w.level:
                                # if this is the first test, set level to START_LEVEL
                                if w.testCounter < 1:
                                    newVal['level'] = START_LEVEL
                                # if this is the second test,
                                # and both tests are wrong,
                                # then remove worker from task
                                # otherwise: level is test_score+1 (i.e., 2 or 3)
                                elif w.testCounter < 2:
                                    if newVal['test_score'] == 0:
                                        newVal['level'] = 0
                                    else:
                                        newVal['level'] = newVal['test_score'] + 1
                                # if this is the third test,
                                # set level to test_score + 1
                                # unless only 1 out of 3 is correct
                                # in which case set level to 1.
                                # (i.e., we have 1, 3, or 4 as possible scores)
                                elif w.testCounter < 3:
                                    if newVal['test_score'] == 1:
                                        newVal['level'] = 1
                                    else:
                                        newVal['level'] = newVal['test_score'] + 1
                                else:
                                    if correctness == 1:
                                        newVal['level'] = w.level + 1
                                    else:
                                        newVal['level'] = w.level - 1
                            else:
                                newVal['level'] = START_LEVEL
                            # keep test_score within bounds
                            if newVal['level'] > 4:
                                newVal['level'] = 4
                            elif newVal['level'] < 0:
                                newVal['level'] = 0
                            qq2  = workers.update(synchronize_session=False).where(workers.c.id==worker_id).values(newVal)
                            connection.execute(qq2)

def set_worker_info(worker, qualities):
    if worker and qualities:
    
        try:
            qualities['id'] = worker
            if 'level' not in qualities:
                qualities['level'] = START_LEVEL
            if 'cond2' not in qualities:
                qualities['cond2'] = random.randint(1,3)
            qq = workers.insert().values(qualities)
            connection.execute(qq)  # this may fail if worker already has a row.
        except IntegrityError:
            pass

        qq2 = workers.update(synchronize_session=False).where(workers.c.id==worker).values(qualities)
        connection.execute(qq2)
        return get_worker_info(worker)
    else:
        return make_error_rss("No worker parameter given to web service")

# increases the number of workers assigned to
# an experimental condition by num_assn
# num_assn defaults to one
def increment_condition(condition, num_assn=1):
    # get the number of workers 
    old_assn  = connection.execute(conditions.select(conditions.c.name==condition)).first().assigned
    # sum the old number of workers with the additional workers from num_assn
    new_total = old_assn + num_assn
    # update condition with the new total
    qq = conditions.update(synchronize_session=False).where(conditions.c.name==condition).values(assigned=new_total)
    # execute update
    connection.execute(qq)

def get_feed(amtreq, worker=None, assn_id=None, assn_token=None, completed=False, appuri=""):

    # Does this worker have an incomplete assignment?

    ###

    log("getting feed for worker %s"%(worker))

    if worker:
        if completed:
            tok = mark_assignment_completed(amtreq, assn_id, worker, assn_token)
            if tok:
                return make_error_rss(tok)
            else:
                return make_error_rss("Failed - aid/token missing or mismatched?  Expired?")
        else:
            print "assn_id", assn_id
            if assn_id:
                a = get_assignment_with_id(assn_id, worker)
            else:
                print "getting regular assignment ", worker
                a = get_assignment(amtreq, worker)
                print "got ", a
                if a:
                    increment_condition(a.condition)
            if a:
                #log("feeding document %s"%a.doc)
                return feed_for_docs([a], appuri)
            else:
                return make_error_rss("No assignment available for worker %s"%worker)

    return make_error_rss("No worker / aid parameters given to web service")

from models import DB_PASSWORD
#import subprocess
from subprocess import Popen,PIPE
def get_status(table=None, worker=None, completed=False, qualified=False):
    global DB_PASSWORD


    if table == None or table == '':
        table = "assignment"
        
    where = "WHERE TRUE "

    if table == "assignment":
        if worker:
            where += "AND amt_worker_id='%s' "%worker
        if completed == "1":
            where += "AND NOT ISNULL(completed) "
        if completed == "0":
            where += "AND ISNULL(completed) "
    elif table == "worker":
        if qualified == "1":
            where += "AND NOT ISNULL(qualified) "
        if qualified == "0":
            where += "AND ISNULL(completed) "

    sql ="select * from %s %s;"%(table, where)

    cmd = ["mysql","-u","mdi","--password=%s"%DB_PASSWORD,"-H", "-e",sql, "mdi"]

    # Python 2.7
    # out = subprocess.check_output(cmd)
    out = Popen(cmd, stdout=PIPE).communicate()[0]
    
    out = out.replace("BORDER=1","")
    return "<HTML><style>table {border:0 white; font-size:10pt; font-family:sans-serif;} tr:nth-child(odd)		{ background-color:#ddd; } tr:nth-child(even)		{ background-color:#fff; }</style><BODY>"+out + "</BODY></HTML>"
    
    



# This is our application object. It could have any name,
# except when using mod_wsgi where it must be "application"
def application( # It accepts two arguments:
    # environ points to a dictionary containing CGI like environment variables

    # which is filled by the server for each received request from the client
    environ,
    # start_response is a callback function supplied by the server
    # which will be used to send the HTTP status and headers to the server
      start_response):

   global log_target
   if 'wsgi.errors' in environ:
       log_target = environ['wsgi.errors']

   log("Wsgi app started.")
    
   # build the response body possibly using the environ dictionary
   response_body = 'The request method was %s' % environ['REQUEST_METHOD']

   amtreq = AMTAssignRequest()

   try:
       log("received request.")
       response_type,response_body = get_feed_request(amtreq, environ)

       # HTTP response code and message
       status = '200 OK'

       # These are HTTP headers expected by the client.
       # They must be wrapped as a list of tupled pairs:
       # [(Header name, Header value)].
       response_headers = [('Content-Type', response_type),
                           ('Content-Length', str(len(response_body)))]
       log("responding.")
       # Send them to the server using the supplied function
       start_response(status, response_headers)

   except BaseException as e:
       log(e)


   finally:
       amtreq.terminate()
     
   # Return the response body.
   # Notice it is wrapped in a list although it could be any iterable.
   return [response_body]


from random import choice
def test_app(worker=None, complete=None):

    print "testing..."

    amtreq = AMTAssignRequest()
    
    # test the set and get response methods
    print get_worker_response(1)   
    response = {'initiator':'The Empire', 'action':'Fire when ready', 'geogLocation':'Yavin IV'}
    print set_worker_response(1,response) 
    print get_worker_response(1)   
        
    e = {}
    if worker:
        workers = [worker]
    else:
        workers = ['d','e','f','g','h','i','j','k','l','m','n','o','p','q','r','s','t']
    todo = dict([(x,None) for x in workers])
    done = dict([(x,[]) for x in workers])
    docmts = {}

    # test the set test score method (and get worker info method)  
    for wid in workers:
        print set_test_score(wid,6)          

    def getdoc (d):
        if d in docmts:
            return docmts[d]
        else:
            getdoc.doccount += 1
            docmts[d] = getdoc.doccount
            return getdoc.doccount
    getdoc.doccount = 0

    
    # test feed making
    alldocs = connection.execute(docs.select())
    dlist = []
    for d in alldocs:
        dlist += [d.key]
        x = feed_for_docs([d])
    docmts = dict(zip(dlist, range(len(dlist))))
    print "feed OK."
    
    reps = 1000
    if worker:
        reps = 1
        if complete:
            todo[worker] = complete
    
    for k in range(0,reps):
        w = choice(workers)
        if todo[w]:
            id,doc,t = todo[w]
            if not mark_assignment_completed(amtreq,id,w,t):
                print "error - could not mark assignment %s by worker %s as completed."%(todo[w],w)
                return
            done[w] += [getdoc(doc)]
            todo[w] = None
        else:
            a = get_assignment(amtreq, w)
            if not a:
                print "error - could not get assignment for worker %s"%w
                return
            else:
                increment_condition(a.condition)
                if getdoc(a.doc) in done[w]:
                    print "error - document %s assigned twice to worker %s"(a.doc, w)
                    return
                # print "got assignment ", a.id, a.doc
            todo[w] = (a.id, a.doc, a.token)
            feed_text = get_feed(amtreq, worker=w) # more db access
            if not feed_text:
                print "error - no feed. worker: %s"%w
                
    print done

import sys

args =  [None, None, None]
if len(sys.argv)>1:
    args = sys.argv[1:] + args

if args[0]=="-test":
    test_app(worker=args[1], complete=args[2])
elif args[0]=="-serve":
    # Instantiate the WSGI server.
    # It will receive the request, pass it to the application
    # and send the application's response to the client
    httpd = make_server(
       'localhost', # The host name.
       8051, # A port number where to wait for the request.
       application # Our application object name, in this case a function.
       )

    # Wait for a single request, serve it and quit.
    httpd.handle_request()

# regular wsgi does not need to do anything alse beyond d