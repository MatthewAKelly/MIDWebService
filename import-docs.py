#!/usr/bin/python

import sys
import re
import json
from operator import itemgetter
from datetime import datetime, date, timedelta
import calendar
from random import *
import csv

in_file     = "/data/mid/2011_gs_supplemented.txt"
#in_file     = "/data/mid/2011_sorted_pos4.txt"
#gs_file     = "/data/mid/last9months2011.xlsx"
gs_file     = "/data/mid/gs_master_plus_corrected.xlsx"
action_file = "/data/mid/actions.csv"
nation_file = "/data/mid/nations.csv"
iso_file    = "/data/mid/ISO2.csv"

#in_file = "2011_sorted_pos4.txt"
fileencoding = "windows-1252"

mid_action_names = [
    'No militarized action',
    'Threat to use force',
    'Threat to use force',
    'Show of force',
    'Alert',
    'Mobilization',
    'Border fortification',
    'Border violation',
    'Blockade',
    'Occupation of territory',
    'Seizure of material or personnel',
    'Attack',
    'Clash',
]

mid_action_types = [
    'NO_MILITARIZED_ACTION',
    'THREAT_TO_USE_FORCE',
    'THREAT_TO_DECLARE_WAR',
    'SHOW_OF_FORCE',
    'ALERT',
    'MOBILIZATION',
    'FORTIFY_BORDER',
    'BORDER_VIOLATION',
    'BLOCKADE',
    'OCCUPATION_OF_TERRITORY',
    'SEIZURE',
    'ATTACK',
    'CLASH',
]

mid_action_gold = [
    'no militarized action (0)',
    'threat to use force (1)',
    'threat to declare war (4)',
    'show of force (7)',
    'alert (8)',
    'mobilization (10)',
    'fortify border (11)',
    'border violation (12)',
    'blockade (13)',
    'occupation of territory (14)',
    'seizure (15)',
    'attack (16)',
    'clash (17)',
]

# combine the lists to create dictionaries
mid_dict  = dict(zip(mid_action_types, mid_action_names))
gold_dict = dict(zip(mid_action_gold, mid_action_names))

# minimum threshold of classification confidence
# to classify as MID
MID_Threshold = 0.5

# read ISO codes for each nation
isoDict = {}
nationDict = {}
with open(iso_file) as csvfile:
    isoreader = csv.reader(csvfile,delimiter=',')
    for row in isoreader:
        key   = row[0].lower().strip() 
        isoDict[key]    = row[1]
        nationDict[key] = row[2]
        
# read action similarities from csv
actDict = {}
with open(action_file) as csvfile:
    actreader = csv.reader(csvfile,delimiter=',')
    for row in actreader:
        key = row[0].lower()
        actDict[key] = []
        for item in row[1:]:
            actDict[key] = actDict[key] + [item.lower()]

# read nation similarities from csv
natDict = {}
with open(nation_file) as csvfile:
    natreader = csv.reader(csvfile,delimiter=',')
    for row in natreader:
        try:
            rowNme,rowCOW = row[0].split('(')
        except:
            rowNme = row[0]
        key = rowNme.strip()
        #key    = rowTrm.upper()
        natDict[key] = []
        for item in row[1:]:
            try:
                rowNme,rowCOW = item.split('(')
            except:
                rowNme = item
            rowTrm = rowNme.strip()
            #rowUpr = rowTrm.upper()
            natDict[key] = natDict[key] + [rowTrm]

# read gold standard classification from Excel

import xlrd

with xlrd.open_workbook(gs_file) as book:

    # 0 corresponds for 1st worksheet, usually named 'Book1'
    sheet = book.sheet_by_index(0)

    # gets col A values, the gold standard keys
    A = [ A for A in sheet.col_values(0) ]

    # gets col B values, the golden standard classifications
    B = [ B for B in sheet.col_values(1) ]

    # get initiator
    C = [ C for C in sheet.col_values(2) ]
    
    # get target
    D = [ D for D in sheet.col_values(3) ]

    # get start month
    E = [ E for E in sheet.col_values(4) ]

    # get start day
    F = [ F for F in sheet.col_values(5) ]

    # get start year
    G = [ G for G in sheet.col_values(6) ]

    # FIX DICTIONARIES: Use the highest MID classification

    # strip whitespace from the start/end of A and B elements
    # convert B elements to lowercase
    gold_keys = []
    for row in A[1:]:
        rowStr = row.encode('utf-8')
        rowTrm = rowStr.strip()
        rowUni = rowTrm.decode('utf-8')
        gold_keys.append(rowUni)

    gold_actions = []
    for row in B[1:]:
        rowStr = row.encode('utf-8')
        rowTrm = rowStr.strip()
        rowLwr = rowTrm.lower()
        gold_actions.append(rowLwr)

    gold_initiators = []
    for row in C[1:]:
        rowStr = row.encode('utf-8')
        try:
            rowNme,rowCOW = rowStr.split('(')
        except:
            rowNme = rowStr
        rowTrm = rowNme.strip()
        #rowUpr = rowTrm.upper()
        if len(rowTrm) < 1:
            rowTrm = 'None'
        gold_initiators.append(rowTrm)

    gold_targets = []
    for row in D[1:]:
        rowStr = row.encode('utf-8')
        try:
            rowNme,rowCOW = rowStr.split('(')
        except:
            rowNme = rowStr
        rowTrm = rowNme.strip()
        #rowUpr = rowTrm.upper()
        if len(rowTrm) < 1:
            rowTrm = 'None'
        gold_targets.append(rowTrm) 

    gold_months = []
    for row in E[1:]:
        try:
            gold_months.append(int(row))
        except:
            gold_months.append(1) # default to January

    gold_days = []
    for row in F[1:]:
        try:
            gold_days.append(int(row))
        except:
            gold_days.append(1) # default to first of month

    gold_years = []
    for row in G[1:]:
        try:
            gold_years.append(int(row))
        except:
            gold_years.append(None)

    # gs_mids is a dictionary of document keys and their integer MID levels
    gs_mids = {} 
    gs_actions = {}
    gs_initiators = {}
    gs_targets = {}
    gs_stdates = {}
    
    # loop through the spreadsheet columns, selecting the highest MID for each doc
    for i in range(0,len(gold_actions)):
        thisAction = gold_actions[i]
        if (gold_actions[i] is not None) and (gold_actions[i] is not ""):
            # convert string to int (e.g., 'Clash (17)' to 17)
            junk,mid_level      = thisAction.split('(') 
            mid_level,junk      = mid_level.split(')')
            mid_level           = int(mid_level)
            # use key to check if this document already has an MID level
            if gold_keys[i] in gs_mids:
                if mid_level > gold_keys[i]:
                    update_dict = True
                else:
                    update_dict = False
            else:
                update_dict = True
            # if the key is undefined or the new mid_level is higher
            # then update the dictionary
            if update_dict:
                gs_mids[gold_keys[i]]       = mid_level
                gs_actions[gold_keys[i]]    = gold_actions[i]
                gs_initiators[gold_keys[i]] = gold_initiators[i]
                gs_targets[gold_keys[i]]    = gold_targets[i]
                if gold_years[i] is not None: 
                    gs_stdates[gold_keys[i]] = date(gold_years[i],gold_months[i],gold_days[i])
                else:
                    gs_stdates[gold_keys[i]] = None
                
if len(sys.argv)>1:
    in_file = sys.argv[1]

# db structure
# Docs table (ID, filename, title, timestamp, source, svm(float), text)
# Hits table  (primary key: ID,  char: AMT_HitID, foreignkey: doc)

from models import *

assignments.drop(engine, checkfirst=True)
docs.drop(engine, checkfirst=True)

metadata.create_all(engine)   # create if it doesn't exist yet


def nextline (file):
    raw = file.readline()
    if not raw:
        raise EOFError()
    if raw.find('-------------------------------------------------')>=0:
        raise ValueError()
    
    return raw.decode(fileencoding)



file = open(in_file, 'r')
readdocs = []
count = 0
while file:
    meta = {}
    try:
        while True:
            # read meta-data
            dataline  = nextline(file).rstrip()
            if '>>>>>>>' in dataline:
                break
            elif ':' in dataline:
                # split at the first colon
                tag,value = dataline.split(':',1)
                tag = tag.lower()
                tag = tag.strip()
                tag = tag.replace(' ','_')
                # the Countries meta-data is represented as a list of
                # ('Country', freq) pairs, where 'Country' is the name
                # of the country and freq is the frequency it occurs in
                # the document. We need to parse this and select the 5
                # most frequent countries for inclusion in the database 
                if tag == 'countries':
                    valueList = value.strip().split(')')
                    countryList = []
                    for countryFreq in valueList:
                        countryTuple = tuple(countryFreq.strip()[1:].split(','))
                        if countryTuple[0] != '':
                            country = countryTuple[0]
                            freq    = int(countryTuple[1])
                            countryList += [[country,freq]]
                    # sort countries in descending order by frequency
                    countryList.sort(key=itemgetter(1),reverse=True)
                    # get the 5 most frequent countries
                    # or all countries if there are less than five
                    for index in range(1, min(5, len(countryList)) + 1):
                        (country, freq) = countryList[index - 1]
                        meta['country'+str(index)] = country.strip("'")
                else:
                    # add tag and value to the dictionary
                    meta[tag] = value.strip()
        # get text of document        
        text = u""
        while True:
            t = nextline(file)

            if t.find('<<<<<<<<<<<<<<<')>=0:
                break

            t = t.replace('\r\n','\n').replace('\r','\n').rstrip()

            text += t + u"\n"

        meta['text'] = text 

        # get NER and NELL-CAT from pipeline data
        # open pipeline data file (HTML version)

        # store the frequency of each NELL-CAT / NER location in document
        #try:
        locFreq = {}
        counted = [False]*len(text)
        with open('/data/mid/docs/' + meta['key'].strip()) as nell_file:    
            for line in nell_file:
                if line.startswith('Type: ner'):
                    if line.find('Value: LOCATION') > -1:
                        if not(counted[spanStart]):
                            counted[spanStart] = True
                            location = text[spanStart:spanEnd]
                            if location in locFreq:
                                locFreq[location] = locFreq[location] + 1
                            else:
                                locFreq[location] = 1
                elif line.startswith('Type: mid-attr'):
                    # parse out list of MID actions
                    # and the pipeline's confidence in each
                    # from the HTML output of the pipeline
                    preamble,values = line.split('Value: ',1)
                    valueList       = values.split(',')
                    valueDict       = {}
                    for value in valueList:
                        key,num  = value.split(':',1)
                        if num.find('<') > -1:
                            num,html = num.split('<',1)
                        valueDict[key] = float(num)
    
                    # filter out actions other than those on
                    # our list of actions we care about
                    actionDict   = {}
                    for action in mid_action_types:
                        # if there is a defined strength for this action
                        if action in valueDict:
                            actionDict[action] = valueDict[action]
                        else:
                            actionDict[action] = 0
                    
                    # Set 'NO_MILITARIZED_ACTION' to at least threshold
                    if actionDict['NO_MILITARIZED_ACTION'] < MID_Threshold:
                        actionDict['NO_MILITARIZED_ACTION'] = MID_Threshold

                elif line.startswith('Type: nell-cat'):
                    isLoc = False
                    if line.find('Value: location') > -1:
                        isLoc = True
                    if line.find('Value: country') > -1:
                        isLoc = True
                    if line.find('Value: geo') > -1:
                        isLoc = True
                    if isLoc:
                        if not(counted[spanStart]):
                            counted[spanStart] = True
                            location = text[spanStart:spanEnd]
                            if location in locFreq:
                                locFreq[location] = locFreq[location] + 1
                            else:
                                locFreq[location] = 1

                elif line.startswith('<li><div class="annotation"'):
                    spanList = line.split(' ')
                    for span in spanList:
                        if span.startswith('spanStart='):
                            itemList  = span.split('"')
                            spanStart = int(itemList[1])
                        elif span.startswith('spanEnd='):
                            itemList  = span.split('"')
                            spanEnd   = int(itemList[1])
        
        # add mid action classification to document data
        # pick the top 4 most confident classifications
        # if the 4th classification is below threshold,
        #   suggest "No militarized action" instead
        bestAction = sorted(actionDict, key=actionDict.get, reverse=True)
        for index in range(1,5):
            meta['mid_attr'+str(index)] = mid_dict[bestAction[index - 1]]

        # pick the top 5 most frequent locations
        bestLoc = sorted(locFreq, key=locFreq.get, reverse=True)
        for index in range(1,min(5, len(bestLoc)) + 1):
            meta['nelloc'+str(index)] = bestLoc[index - 1]

        # add gold standard MID classification to database
        gold_key    = meta['gskey']
        if gold_key in gs_actions:
            gold_action = gs_actions[gold_key]
            if gold_action in gold_dict:
                gold_db          = gold_dict[gold_action]
                meta['gsAction'] = gold_db
            else:
                meta['gsAction'] = 'Unknown'
            if (gs_mids[gold_key] > 6) or (gs_mids[gold_key] == 0):
                meta['gsActionNum']  = gs_mids[gold_key]
            else: # don't discriminate between actions 1 thru 6
                meta['gsActionNum']  = 6
            # convert nations into Qualtrics format
            ini = gs_initiators[gold_key].lower().strip()
            tar = gs_targets[gold_key].lower().strip()

            if ini in nationDict:
                meta['gsInitiator']  = nationDict[ini]
            else:
                meta['gsInitiator']  = gs_initiators[gold_key]
        
            if tar in nationDict:
                meta['gsTargets']    = nationDict[tar]
            else:
                meta['gsTargets']    = gs_targets[gold_key]
            meta['gsDate']       = gs_stdates[gold_key]

            # get ISO codes for gold standard nations
            if ini in isoDict:
                meta['gsInitiatorISO'] = isoDict[ini]
            else:
                print "ISO for " + ini + " not found."
            if tar in isoDict:
                meta['gsTargetISO'] = isoDict[tar]
            else:
                print "ISO for " + tar + " not found."
    
            # make several incorrect suggestions for the action
            similar_actions = actDict[gold_action]
            # suggest that there's no MID
            # with proportion of non-MID documents in gold standard
            suggest_no_mid  = (gold_action != mid_action_gold[0]) and (randint(0,1) <= 0.2108)
            if suggest_no_mid: 
                # randomly add "no MID action" as a suggestion
                similar_actions = [mid_action_gold[0]] + similar_actions
            # assign actions    
            meta['r1Action'] = gold_dict[similar_actions[0]]
            meta['r2Action'] = gold_dict[similar_actions[1]]
            meta['r3Action'] = gold_dict[similar_actions[2]]
            meta['r4Action'] = gold_dict[similar_actions[3]]
    
            # now let's make incorrect suggestions for the date
            dateList = meta['date'].split(' ')
            dateStr  = ' '.join(dateList[0:3])
            try:
                pubDate = datetime.strptime(dateStr,'%B %d, %Y')
            except:
                try:
                    pubDate = datetime.strptime(dateStr,'%B %d, %Y,')
                except:
                    pubDate = datetime.strptime(dateStr,'%B %d %Y')
            # define a range of dates: 10 days before and 1 day after publication
            pick_a_day = range(-10,1)
            if meta['gsDate'] is not None:
                pub_gold_diff = meta['gsDate'].day - pubDate.day
                if pub_gold_diff in pick_a_day:
                    pick_a_day.pop(pick_a_day.index(pub_gold_diff)) # cannot pick gold/correct
            # now select random dates within that range
            suggested_dates = []
            if suggest_no_mid and (meta['gsDate'] is not None):
                suggested_dates = [None]
            while len(suggested_dates) < 4:
                j = randint(0,len(pick_a_day) - 1)
                suggested_dates = suggested_dates + [pubDate + timedelta(days=pick_a_day[j])]
                pick_a_day.pop(j)
            # assign dates
            meta['r1Date'] = suggested_dates[0]
            meta['r2Date'] = suggested_dates[1]
            meta['r3Date'] = suggested_dates[2]
            meta['r4Date'] = suggested_dates[3]
    
            # suggest some incorrect initiators
            suggested_nations = []
            if suggest_no_mid:
                suggested_nations = suggested_nations + ['None']
            if ('country1' in meta) and (meta['country1'] != gs_initiators[gold_key]):
                suggested_nations = suggested_nations + [meta['country1']]
            if ('country2' in meta) and (meta['country2'] != gs_initiators[gold_key]):
                suggested_nations = suggested_nations + [meta['country2']]
            if ('country3' in meta) and (meta['country3'] != gs_initiators[gold_key]):
                suggested_nations = suggested_nations + [meta['country3']]
            if ('country4' in meta) and (meta['country4'] != gs_initiators[gold_key]):
                suggested_nations = suggested_nations + [meta['country4']]
            if len(suggested_nations) < 4:
                if (gs_initiators[gold_key] is not None) and (gs_initiators[gold_key] != '') and (gs_initiators[gold_key] is not 'None'):
                    similar_nations = natDict[gs_initiators[gold_key]]
                elif (len(suggested_nations) > 0) and (suggested_nations[0] in natDict):
                    similar_nations = natDict[suggested_nations[0]]
                else: # get some random countries
                    similar_nations = natDict['NATO']
                    shuffle(similar_nations)
                for nation in similar_nations:
                    if not(nation in suggested_nations):
                        suggested_nations = suggested_nations + [nation]
                        if len(suggested_nations) > 4:
                            break
            # assign suggestions
            meta['r1Initiator'] = suggested_nations[0]
            meta['r2Initiator'] = suggested_nations[1]
            meta['r3Initiator'] = suggested_nations[2]
            meta['r4Initiator'] = suggested_nations[3]

            # suggest some incorrect targets
            suggested_nations = []
            if suggest_no_mid:
                suggested_nations = suggested_nations + ['None']
            if ('country1' in meta) and (meta['country1'] !=  gs_targets[gold_key]):
                suggested_nations = suggested_nations + [meta['country1']]
            if ('country2' in meta) and (meta['country2'] !=  gs_targets[gold_key]):
                suggested_nations = suggested_nations + [meta['country2']]
            if ('country3' in meta) and (meta['country3'] !=  gs_targets[gold_key]):
                suggested_nations = suggested_nations + [meta['country3']]
            if ('country4' in meta) and (meta['country4'] !=  gs_targets[gold_key]):
                suggested_nations = suggested_nations + [meta['country4']]
            if len(suggested_nations) < 4:
                if (gs_targets[gold_key] is not None) and (gs_targets[gold_key] != '')  and (gs_targets[gold_key] is not 'None'):
                    similar_nations = natDict[gs_targets[gold_key]]
                elif (len(suggested_nations) > 0) and (suggested_nations[0] in natDict):
                    similar_nations = natDict[suggested_nations[0]]
                else: # get some random countries
                    similar_nations = natDict['NATO']
                    shuffle(similar_nations)
                for nation in similar_nations:
                    if not(nation in suggested_nations):
                        suggested_nations = suggested_nations + [nation]
                        if len(suggested_nations) > 4:
                            break
            # assign suggestions
            meta['r1Targets'] = suggested_nations[0]
            meta['r2Targets'] = suggested_nations[1]
            meta['r3Targets'] = suggested_nations[2]
            meta['r4Targets'] = suggested_nations[3]
    
            # check to see if gold standard classification
            # is correctly guessed by NELL
            if meta['gsAction'] == meta['mid_attr1']:
                meta['correct'] = 1
            elif meta['gsAction'] == meta['mid_attr2']:
                meta['correct'] = 1
            elif meta['gsAction'] == meta['mid_attr3']:
                meta['correct'] = 1
            elif meta['gsAction'] == meta['mid_attr4']:
                meta['correct'] = 1
            else:
                meta['correct'] = 0
            #except:
            #    print "Could not find pipeline data for document "+meta['key']

            # add data from this document to the document list
            if (meta['gsAction'].lower() == 'unknown') or (meta['gsAction'] == ''):
                pass # if the action is unknown, do not include document
            elif meta['gsActionNum'] == 0: 
                readdocs += [meta] # if the action is 0 / no MID, include
            elif (meta['gsInitiator'] == '') or (meta['gsInitiator'].lower() == 'none'):
                pass # if the action is not zero, and no initiator, do not include
            elif (meta['gsTargets'] == '') or (meta['gsTargets'].lower() == 'none'):
                pass # if the action is not zero, and no target, do not include
            else: # otherwise, include
                readdocs += [meta]

        try:
            nextline(file)

        except ValueError:
            pass

    except EOFError:
        break
    except ValueError:
        print meta
        print "document could not be read - encountered ----- separator.  skipping doc."


# shuffle readdocs
from random import shuffle
shuffle(readdocs)
        
for data in readdocs:
    # sqlalchemy.sql.expression.insert(table, values=None, inline=False, **kwargs)
    from sqlalchemy.exc import IntegrityError
    try:
        # chk = docs.select(docs.c.filename==fname)
        # result = engine.execute(chk.exists())
        existing = connection.execute(docs.select(docs.c.key==data['key']))

        if existing.first():
            ins = docs.update().where(docs.c.key==data['key']).values(data)
        else:
            ins = docs.insert().values(data)

        result = engine.execute(ins)
    except IntegrityError as x:
        print x


    count = count+1

    if count%100 == 0:
        print count


print "%s imported."%count
      
    


# 20100913--0161-Sep13_2010_LN_NP1.txt-files.list
# Australian troops to encounter more violence in Afghanistan: Defense Force   AFG-AUL
# September 13, 2010 Monday 1:25 AM EST
# News source: (c) Xinhua General News Service
# SVM score: 1.757

# >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
# Australian troops in Afghanistan  can expect to encounter increased violence as
# they push deeper  into Taliban sanctuaries, Australian Defense Force warned on
# Monday.
# Air Chief Marshal Angus Houston told reporters during a  briefing in Canberra on
# Monday that troops of the Afghan National  Army 4th Brigade, accompanied by
# their Australian mentors, were  heading deeper into the Baluchi and Chora
# Valleys and Deh Rawood  area of Oruzgan province in Afghanistan.
# "These are partnered patrols and it is dangerous work," said  Houston, adding
# the fight was becoming more intense.
# "We can expect violence levels to increase as we contest the  insurgency in
# greater numbers and across a wider area in the  south. "
# Afghanistan is now in the midst of its fighting season with 10  Australian
# soldiers killed so far this year.
# The most recent fatality was Lance Corporal Jared MacKinney who  was killed
# while accompanying Afghan troops in the Tangi Valley  near Deh Rawood.
# The past few months had been tough for Australian troops,  Houston said.
# "But it is important that we maintain our resolve, push forward  with the
# strategy and keep the pressure on the Taliban," he said.
# He said last Tuesday that Australian troops had achieved  significant success in
# training the Afghan security forces and  pressuring insurgents.

# <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<
