#!/usr/bin/python

import sys
import os

def main():

	coordsfile = sys.argv[1].strip()
	imagefile = sys.argv[2].strip()
  
	f = open(coordsfile)
	line = f.readline().strip().split(" ")
	offsetx = int(line[1])
	offsety = int(line[2])

	line = f.readline().strip().split(" ")
	x = int(line[1])-offsetx
	y = int(line[2])-offsety

	os.system("convert \""+imagefile+"\" -crop "+str(x)+"x"+str(y)+"+"+str(offsetx)+"+"+str(offsety)+" \""+imagefile+"\"")
  
  #convert out.png -crop 591x340+2614+1329 crop.png

if __name__=="__main__":
  main()
