#!/bin/sh

cd ~
git clone https://github.com/priyankamittal-09/spark-search-analysis.git
cd spark-search-analysis/
/sbt/bin/sbt test
