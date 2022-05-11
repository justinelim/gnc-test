#!/usr/bin/env bash
./run_synthea -c synthea.properties -p 10 -s 0 -cs 0 -r 20220511

find ./output -name "*.csv" -size +50000000c -print | while read file; do
  fn=$(basename -s .csv ${file})
  dn=$(dirname ${file})
  split --additional-suffix=.csv --elide-empty-files --line-bytes=50000000 --numeric-suffixes --suffix-length=2 $file ${dn}/${fn}. && rm $file
done