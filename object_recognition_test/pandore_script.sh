#!/bin/bash

#start Pandore
export PANDOREHOME=/home/karl/kool/msc_thesis/archaeology/pandore
export PATH=$PANDOREHOME/bin:$PATH
export LD_LIBRARY_PATH=$PANDOREHOME/lib:$LD_LIBRARY_PATH

if [ $# != 2 ]; then
	echo "Usage: ./pandore_script.sh OBJECT_FILE TARGET_FILE"
	exit 0
fi

OBJFILE=${1}
TARGETFILE=${2}
OUTNAME=${TARGETFILE%.*}

echo "running object recognition"
./find_obj "${OBJFILE}" "${TARGETFILE}" >  "${OUTNAME}_coords"

if [ ! -s "${OUTNAME}_coords" ]; then
	echo "No match (find_obj)!"
	rm "${OUTNAME}_coords"
	exit 255
fi

#python script to that reads coords from find_obj for ppixelvalue
LOC=`python find_box.py "${OUTNAME}_coords"`

#ppng2pan ${OBJFILE} object.pan
echo "convert to .pan"
pjpeg2pan "${TARGETFILE}" "${OUTNAME}.pan"

#normalize
echo "normalize"
pnormalization 0 255 "${OUTNAME}.pan" "${OUTNAME}.pan"
echo "thresholding"
pbinarization 156 255 "${OUTNAME}.pan" "${OUTNAME}_labels.pan"
echo "labeling"
plabeling 8 "${OUTNAME}_labels.pan" "${OUTNAME}_labels.pan"
echo "erosion"
perosion 1 10 "${OUTNAME}_labels.pan" "${OUTNAME}_labels.pan"
echo "dilatation"
pdilatation 1 10 "${OUTNAME}_labels.pan" "${OUTNAME}_labels.pan"
echo "bounding box"
pboundingbox "${OUTNAME}_labels.pan" "${OUTNAME}_labels.pan"


#extract location
echo "extract point of interest"
ppixelvalue $LOC 0 "${OUTNAME}_labels.pan" "${OUTNAME}_tmp.col"
#get region ID
VAL=`pstatus`
plabelselection $VAL "${OUTNAME}_labels.pan" "${OUTNAME}_out.pan"
ppan2txt "${OUTNAME}_out.pan" "${OUTNAME}_out.txt"
pmask "${OUTNAME}.pan" "${OUTNAME}_out.pan" "${OUTNAME}_out.pan"

echo "write output"
pim2uc "${OUTNAME}_out.pan" "${OUTNAME}_out.pan"
ppan2png "${OUTNAME}_out.pan" "${OUTNAME}_out.png"

head -n 1 "${OUTNAME}_out.txt" > "${OUTNAME}_coords"
tail -n 1 "${OUTNAME}_out.txt" >> "${OUTNAME}_coords"

if [ ! -s "${OUTNAME}_coords" ]; then
	echo "No match (POI extract)!"
	rm "${OUTNAME}.pan"
	rm "${OUTNAME}_labels.pan"
	rm "${OUTNAME}_out.pan"
	rm "${OUTNAME}_tmp.col"
	rm "${OUTNAME}_out.txt"
	rm "${OUTNAME}_coords"
	exit 254
fi

#python script to read coords and call convert
python extract_box.py "${OUTNAME}_coords" "${OUTNAME}_out.png"

echo "OCR"
tesseract "${OUTNAME}_out.png" "${OUTNAME}" -psm 3

rm "${OUTNAME}.pan"
rm "${OUTNAME}_labels.pan"
rm "${OUTNAME}_out.pan"
rm "${OUTNAME}_out.png"
rm "${OUTNAME}_tmp.col"
rm "${OUTNAME}_out.txt"
rm "${OUTNAME}_coords"

exit 0

