# MIDWebService
MIDWebService

MID Web Service. Written by Matthew Kelly and David Reitter. Uses SQL Alchemy Library for Python.

feed.py: Interprets URL requests from the server, interfaces with the SQL database, and passes HTML back.

models.py: Defines the structure of the SQL database (tables, columns, and data types).

import-docs.py: Imports news stories, gold standard classifications, and machine learning inferences, into the SQL database.

assign-hits.py: Creates assignments/"hits" to be completed by workers. Variations of this code exist for different experiments.

(tree exp) creates hits to study tree-structure survey, wiki-style suggestions from other users, and the "supplemented" condition where machine learning suggestions are made by Bill McDowell's MID analysis pipeline (code for pipeline not included)

(info exp) explores the effect of offering workers between 0 and 4 multiple choice answers that may or may not include the correct answer

(probe exp) explores the effect of interleaving test probes in amongst the assignments workers are asked to complete
