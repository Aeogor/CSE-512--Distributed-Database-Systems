#!/usr/bin/python2.7
#
# Assignment2 Interface
#

# Srinivas Lingutla
# 1217124532
# CSE 512 - DDS 

import psycopg2
import os
import sys

# Donot close the connection inside this file i.e. do not perform openconnection.close()
def RangeQuery(ratingMinValue, ratingMaxValue, openconnection, outputPath):
    #Implement RangeQuery Here.
    #pass #Remove this once you are done with implementation
    rangeList = []
    cur = openconnection.cursor()
    cur.execute("""Select PartitionNum from RangeRatingsMetadata
                    Where MinRating >= """ + str(ratingMinValue) + 
                    " OR MaxRating <= " + str(ratingMaxValue) + ";")
    k = cur.fetchall()
    for r in k:
        cur.execute("Select UserID, MovieID, Rating from RangeRatingsPart" + str(r[0]) + " where rating >= " + str(ratingMinValue) + " and rating <= " + str(ratingMaxValue) + ";")
        for l in cur.fetchall():
            rangeList.append("RangeRatingsPart" + str(r[0]) + "," + str(l[0]) + "," + str(l[1]) + "," + str(l[2]) + "\n")
        
    cur.execute("Select PartitionNum from RoundRobinRatingsMetadata")
    k = cur.fetchone()[0]
    for r in range(k):
        cur.execute("Select UserID, MovieID, Rating from RoundRobinRatingsPart" + str(r) + " where rating >= " + str(ratingMinValue) + " and rating <= " + str(ratingMaxValue) + ";")
        for l in cur.fetchall():
            rangeList.append("RoundRobinRatingsPart" + str(r) + "," + str(l[0]) + "," + str(l[1]) + "," + str(l[2]) + "\n")
    f = open(outputPath, 'a')
    for l in rangeList:
        f.write(l)


def PointQuery(ratingValue, openconnection, outputPath):
    #Implement PointQuery Here.
    #pass # Remove this once you are done with implementation

    rangeList = []
    cur = openconnection.cursor()
    cur.execute("""Select PartitionNum from RangeRatingsMetadata
                    Where MinRating <= """ + str(ratingValue) + 
                    " OR MaxRating >= " + str(ratingValue) + ";")
    k = cur.fetchall()
    for r in k:
        cur.execute("Select UserID, MovieID, Rating from RangeRatingsPart" + str(r[0]) + " where rating = " + str(ratingValue) + ";")
        for l in cur.fetchall():
            rangeList.append("RangeRatingsPart" + str(r[0]) + "," + str(l[0]) + "," + str(l[1]) + "," + str(l[2]) + "\n")

    cur.execute("Select PartitionNum from RoundRobinRatingsMetadata")
    k = cur.fetchone()[0]
    for r in range(k):
        cur.execute("Select UserID, MovieID, Rating from RoundRobinRatingsPart" + str(r) + " where rating = " + str(ratingValue) + ";")
        for l in cur.fetchall():
            rangeList.append("RoundRobinRatingsPart" + str(r) + "," + str(l[0]) + "," + str(l[1]) + "," + str(l[2]) + "\n")

    f = open(outputPath, 'a')
    for l in rangeList:
        f.write(l)
