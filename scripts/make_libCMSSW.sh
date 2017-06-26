#!/bin/bash

DIR=$PWD
libname=lib_CMSSW.tar.gz
[ ! -z "$CMSSW_BASE" ] || {
    echo ">>> [!] Did you do 'cmsenv'? "'$CMSSW_BASE'" is not set!"
} && {
    cd $CMSSW_BASE
    echo ">>> Making the tarball..."
    src=`find src/ -name "src"`
    data=`find src/ -name "data"`
    interface=`find src/ -name "interface"`
    python=`find src/ -name "python"`
    echo tar -chzf --exclude-vcs $libname lib/ python/ src/ $src $data $interface $python
    tar -chzf $libname --exclude-vcs lib/ python/ src/ $src $data $interface $python
    mv $libname $DIR/$libname
    cd $DIR
    echo ">>> Your tarball is $libname"
}
