#!/bin/bash

if (( $# != 2 )); then
  echo "Illegal number of arguments."
  echo "Must provide CMS3 tag then CMSSW tag"
  return 1
else
  THE_CMS3_TAG=$1
  CMSSW_RELEASE=$2
fi

DIR=$PWD

cd $CMSSW_RELEASE
echo "Making the tarball..."
stuff1=`find src/ -name "data"`
stuff2=`find src/ -name "interface"`
stuff3=`find src/ -name "python"`
tar -chzvf lib_$THE_CMS3_TAG.tar.gz *.db lib/ python/ $stuff1 $stuff2 $stuff3 src/CMS3/NtupleMaker/test/*_cfg.py

mv lib_$THE_CMS3_TAG.tar.gz $DIR/lib_$THE_CMS3_TAG.tar.gz
cd $DIR
echo "Your tarball is lib_$THE_CMS3_TAG.tar.gz"
