#!/bin/bash

#start Pandore

#bash /home/karl/Downloads/pandore/pandore

if [ $# != 2 ]; then
	echo "Usage: ./pandore_script.sh OBJECT_FILE TARGET_FILE"
	exit 0
fi

OBJFILE=${1}
TARGETFILE=${2}

#TODO python script to read coords from find_obj for ppixelvalue

#ppng2pan ${OBJFILE} object.pan
echo "convert to .pan"
pjpeg2pan "${TARGETFILE}" target.pan

#normalize
echo "normalize"
pnormalization 0 255 target.pan target.pan
echo "thresholding"
pbinarization 156 255 target.pan labels.pan
echo "labeling"
plabeling 8 labels.pan labels.pan
echo "erosion"
perosion 1 10 labels.pan labels.pan
#echo "dilatation"
#pdilatation 1 10 labels.pan labels.pan
echo "bounding box"
pboundingbox labels.pan labels.pan

#extract location
echo "extract point of interest"
ppixelvalue 3076 1387 0 labels.pan tmp.col
VAL=`pstatus`
plabelselection $VAL labels.pan out.pan
ppan2txt out.pan out.txt
pmask target.pan out.pan out.pan

echo "write output"
#prg2im out.pan out.pan
#pnormalization 0 255 out.pan out.pan
pim2uc out.pan out.pan
ppan2png out.pan out.png

head -n 1 out.txt > coords
tail -n 1 out.txt >> coords

#python script to read coords and call convert
python extract_box.py out.png

rm *pan
rm tmp.col
rm out.txt
rm coords

