#!/usr/bin/python

import sys
import re

# 1. No additional information
# 2. 1 correct answer
# 3. 1 wrong answer
# 4. 1 correct, 1 wrong
# 5. 2 wrong
# 6. 1 correct, 2 wrong
# 7. 3 wrong
# 8. 1 correct, 3 wrong
# 9. 4 wrong

conditionNames      = ["1", "2", "3", "4", "5", "6", "7", "8", "9"]
annotations_per_doc = 5  # redundancy
bundle_sizes        = (3,6)  # WARNING: SKIP_RESERVATIONS in feed.py
test_file_name      = "/data/mid/experiment_docIDs3.tsv"

sample_subset = None  # None for all

use_test_set = True # False to use all documents

import random

if len(sys.argv)>1:
    in_file = sys.argv[1]
from sqlalchemy import *

from models import *


# db structure
# Docs table (ID, filename, title, timestamp, source, svm(float), text)
# Hits table  (primary key: ID,  char: AMT_HitID, foreignkey: doc)


assignments.drop(engine, checkfirst=True)
conditions.drop(engine, checkfirst=True)
#workers.drop(engine, checkfirst=True)
metadata.create_all(engine)   # create if it doesn't exist yet

test_file = open(test_file_name, 'r')
test_gskeys = []
while test_file:
    row = test_file.readline()
    if row:
        key,other = row.split('\t',1)
        if key != "Document.ID":
            test_gskeys.append(key)
    else:
        break

###
# bundles

bundle_counts = None
current_bundles = None
def reset_bundle():
    global bundle_counts, bundle_sizes, current_bundles
    
    bundle_counts = tuple([0 for _ in bundle_sizes])
    current_bundles = tuple([0 for _ in bundle_sizes])    
def new_bundles():
    
    global bundle_counts, bundle_sizes, current_bundles
    bundle_counts = tuple([0 for _ in bundle_sizes])
    current_bundles = tuple([x+1 for x in current_bundles])
    
def inc_bundle():
    global bundle_counts, bundle_sizes, current_bundles
    
    new_bc = []
    new_cur_b = []
    for cur_b, bc, bz in zip(current_bundles, bundle_counts, bundle_sizes):
        bc += 1
        if bc==bz:
            bc = 0
            cur_b += 1
        new_bc += [bc]
        new_cur_b += [cur_b]
    bundle_counts = tuple(new_bc)
    current_bundles = tuple(new_cur_b)
    return current_bundles

###

docquery = connection.execute(docs.select())  # [docs.id]

alldocs = docquery.fetchall()

if sample_subset: # select a random sample from all docs
    alldocs = random.sample(alldocs, sample_subset)
elif use_test_set: # select a specific sample from all docs
    testdocs = []  # where docs have the specified gskey in test_gskeys
    for docid in alldocs:
        if docid.gskey in test_gskeys:
            testdocs.append(docid)
    alldocs = testdocs
    # check for missing documents
    for gskey in test_gskeys:
        gskey_found = False
        for docid in alldocs:
            if (docid.gskey == gskey):
                gskey_found = True
        if not(gskey_found):
            print 'missing gskey %s'%gskey
        
# number of docs times annno

anno_rem = range(annotations_per_doc * len(alldocs))

reset_bundle()

# population conditions table with experimental conditions
for thisCondition in conditionNames:
    ins    = conditions.insert().values(name=thisCondition,assigned=0, workers=0)
    result = engine.execute(ins)

count=0 # number of assignments
for a in range(annotations_per_doc):
    for docid in alldocs:
        bundle,big_bundle = inc_bundle()
        ins = assignments.insert().values(doc=docid.key, bundle_id=bundle, big_bundle_id=big_bundle, locked=0)
        result = engine.execute(ins)
        count += 1
    new_bundles()

print "%s experimental conditions.  %s assignments made.  %s assignments per document per condition. %s unique documents."%(len(conditionNames),count,annotations_per_doc,len(alldocs))
