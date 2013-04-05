#!/bin/bash

mkdir out
cd out
cp ../bibliography.bib .
cp ../ut_thesis.sty .
cp ../ut_thesis_utf8.sty .
latex ../thesis.tex
bibtex thesis.aux
latex ../thesis.tex
latex ../thesis.tex
dvips thesis.dvi
ps2pdf thesis.ps
mv thesis.pdf ../thesis.pdf
cd ..
rm -r out
