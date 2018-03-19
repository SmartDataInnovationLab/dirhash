#!/bin/bash
spark-submit --master yarn --deploy-mode client --jars `pwd`/target/sparkhacks-0.0.1-SNAPSHOT.jar dirhash.py "$@"
