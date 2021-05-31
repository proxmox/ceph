#!/usr/bin/env bash
#
# Copyright (C) 2016 <contact@redhat.com>
#
# Author: Loic Dachary <loic@dachary.org>
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU Library Public License as published by
# the Free Software Foundation; either version 2, or (at your option)
# any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Library Public License for more details.
#

SCRIPTNAME="$(basename $0)"
if [ `uname` == FreeBSD ]; then
    GETOPT="/usr/local/bin/getopt"
else
    GETOPT=getopt
fi

function usage {
    echo
    echo "$SCRIPTNAME - automate setup of Python virtual environment"
    echo "    (for use in building Ceph)"
    echo
    echo "Usage:"
    echo "    $SCRIPTNAME [--python=PYTHON_BINARY] TARGET_DIRECTORY"
    echo
    echo "    TARGET_DIRECTORY will be created if it doesn't exist,"
    echo "        and completely destroyed and re-created if it does!"
    echo
    exit 1
}

TEMP=$($GETOPT --options "h" --long "help,python:" --name "$SCRIPTNAME" -- "$@")
test $? != 0 && usage
eval set -- "$TEMP"

PYTHON_OPTION=""
while true ; do
    case "$1" in
        -h|--help) usage ;;  # does not return
        --python) PYTHON_OPTION="--python=$2" ; shift ; shift ;;
        --) shift ; break ;;
        *) echo "Internal error" ; exit 1 ;;
    esac
done

DIR=$1
if [ -z "$DIR" ] ; then
    echo "$SCRIPTNAME: need a directory path, but none was provided"
    usage
fi
rm -fr $DIR
mkdir -p $DIR
virtualenv $PYTHON_OPTION $DIR
. $DIR/bin/activate

if pip --help | grep -q disable-pip-version-check; then
    DISABLE_PIP_VERSION_CHECK=--disable-pip-version-check
else
    DISABLE_PIP_VERSION_CHECK=
fi

if pip --help | grep -q use-feature; then
    USE_FEATURE=--use-feature=2020-resolver
else
    USE_FEATURE=
fi

# older versions of pip will not install wrap_console scripts
# when using wheel packages
pip $DISABLE_PIP_VERSION_CHECK --log $DIR/log.txt install \
    $USE_FEATURE \
    --upgrade 'pip >= 6.1'

if pip --help | grep -q disable-pip-version-check; then
    DISABLE_PIP_VERSION_CHECK=--disable-pip-version-check
else
    DISABLE_PIP_VERSION_CHECK=
fi

if test -d wheelhouse ; then
    export NO_INDEX=--no-index
fi

pip $DISABLE_PIP_VERSION_CHECK --log $DIR/log.txt install \
    $USE_FEATURE \
    $NO_INDEX \
    --find-links=file://$(pwd)/wheelhouse 'tox >=2.9.1'

require_files=$(ls *requirements*.txt 2>/dev/null) || true
constraint_files=$(ls *constraints*.txt 2>/dev/null) || true
require=$(echo -n "$require_files" | sed -e 's/^/-r /')
constraint=$(echo -n "$constraint_files" | sed -e 's/^/-c /')
md5=wheelhouse/md5
if test "$require"; then
    if ! test -f $md5 || ! md5sum -c wheelhouse/md5 > /dev/null; then
        NO_INDEX=''
    fi
    pip --exists-action i $DISABLE_PIP_VERSION_CHECK --log $DIR/log.txt install \
        $USE_FEATURE \
        $NO_INDEX \
        --find-links=file://$(pwd)/wheelhouse $require $constraint
fi
