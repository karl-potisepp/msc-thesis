#!/usr/bin/python

import sys
import os

def main():

  outname = "crop.png"
  if len(sys.argv) > 1:
    outname = sys.argv[1].strip()
  
  f = open("coords")
  line = f.readline().strip().split(" ")
  offsetx = int(line[1])
  offsety = int(line[2])

  line = f.readline().strip().split(" ")
  x = int(line[1])-offsetx
  y = int(line[2])-offsety
  os.system("convert out.png -crop "+str(x)+"x"+str(y)+"+"+str(offsetx)+"+"+str(offsety)+" "+outname)
  
  #convert out.png -crop 591x340+2614+1329 crop.png

if __name__=="__main__":
  main()
