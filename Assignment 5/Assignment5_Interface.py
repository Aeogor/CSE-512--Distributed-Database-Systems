#!/usr/bin/python2.7
#
# Assignment5 Interface
# Name: 
#

from pymongo import MongoClient
import os
import sys
import json
from math import sin, cos, atan2, radians, sqrt

def FindBusinessBasedOnCity(cityToSearch, saveLocation1, collection):
	f = open(saveLocation1, 'w')
	for i in collection.find({"city" : cityToSearch}):
		f.write(i["name"] + "$" + i["full_address"] + "$" + i["city"] + "$" + i["state"] + "\n")
	f.close()


def FindBusinessBasedOnLocation(categoriesToSearch, myLocation, maxDistance, saveLocation2, collection):
	f = open(saveLocation2, 'w')
	for i in collection.find({"categories" : {"$all" : categoriesToSearch}}):
		r = 3959.0
		p1 = radians(float(myLocation[0]))
		p2 = radians(float(i["latitude"]))
		d1 = radians(float(i["latitude"])-float(myLocation[0]))
		d2 = radians(float(i["longitude"]) - float(myLocation[1]))
		a = sin(d1/2) * sin(d1/2) + cos(p1) * cos(p2) * sin(d2/2) * sin(d2/2)
		c = 2 * atan2(sqrt(a),sqrt(1-a))
		d = r*c
		if d <= maxDistance:
			f.write(i["name"] + "\n")	
	f.close()

