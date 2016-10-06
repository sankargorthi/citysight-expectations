#!/usr/bin/env bash

SOURCE=devdenver
TARGETS=devdenver,stagingdenver,denver
DATE=`date +%Y-%m-%d`
Rscript /opt/citysight-expectations/GenerateMarkExpectations.R $DATE $SOURCE $TARGETS
