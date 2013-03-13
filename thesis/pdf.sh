#!/bin/bash

mkdir out
cd out
latex ../thesis.tex
cp ../bibliography.bib .
bibtex thesis.aux
latex ../thesis.tex
latex ../thesis.tex
dvips thesis.dvi
ps2pdf thesis.ps
mv thesis.pdf ../thesis.pdf
cd ..
rm -r out
