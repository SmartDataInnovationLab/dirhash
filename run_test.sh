#!/bin/bash
DIR="${BASH_SOURCE%/*}"
spark-submit --master yarn --deploy-mode client --jars $DIR/target/sparkhacks-0.0.1-SNAPSHOT.jar dirhash_test.py "$@"
