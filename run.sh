#!/bin/bash
spark-submit --master yarn --deploy-mode client --jars `pwd`/target/sparkhacks-0.0.1-SNAPSHOT.jar dirhash.py "$@" 2>/dev/null | sed '/^spark/d'

# Note: sed is used to filter a single warning, that spark prints to stdout instead stderr. I think this is a bug in spark1.4.1, and thus the sed-part can be removed once SDIL moves to a newer version of spark
# see: https://github.com/SmartDataInnovationLab/dirhash/issues/13