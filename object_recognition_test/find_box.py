#!/usr/bin/python

import os
import sys
import math

def main():

	f = open(sys.argv[1].strip())
	points = []
	for line in f:
		points.append(line.strip().split(" "))

	#naive way - take average of the points
	sumx = 0
	sumy = 0
	for p in points:
		sumx += int(p[0])
		sumy += int(p[1])

	print math.floor(sumx/len(points)), math.floor(sumy/len(points))

if __name__=="__main__":

	main()
