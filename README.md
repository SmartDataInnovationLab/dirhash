# Dirhash

Create and verify hash values for contents of entire directories in parallel with PySpark.

This is tool is design to run on the sdil-platform.

<<<<<<< HEAD
Original Authors: Gunnar Hartung / Niklas Baumstark
=======
Original Authors: Gunnar Hartung (gunnar.hartung@kit.edu) / Niklas Baumstark (niklas.baumstark@gmail.com)
>>>>>>> ca78f4ba5637ba7e1f138786f9946599741ebbc3

Current Authors: Björn Jürgens (juergens@teco.edu) / Dr. Till Riedel <till.riedel@kit.edu>

## Getting started

First build the Scala component:

    $ cd /path/to/dirhash
    $ mvn package

You may want to install the pysha3 and/or pyblake2 module from PyPI:

    $ pip install pysha3 pyblake2

Run:

    $ ./run.sh hdfs://localhost/tmp/input-text 2>/dev/null

## dev notes

### test locally

    docker build -t dirh .
    docker run -it -v (realpath .)/test/data/:/hash_this dirh

expectd output `v1:sha256:128M:aa669dcefba57e01bd7ff0526a0001d2118f06adc8106d265b5743b0ee90084f`

debugging dockerfile

    docker build -t dirh .
    docker run -it  -v (realpath .)/test/data/:/hash_this dirh bash

    # test spark:
    cd $SPARK_HOME
    ./bin/run-example SparkPi 10

    # test dirhash:
    spark-submit --jars `pwd`/target/sparkhacks-0.0.1-SNAPSHOT.jar dirhash.py /hash_this/

    # run as python:
    $SPARK_HOME/bin/pyspark --jars `pwd`/target/sparkhacks-0.0.1-SNAPSHOT.jar
    # import dirhash
    # hash_value = dirhash.hash_directory('/hash_this', 'sha256', '128M', SparkContext.getOrCreate() )

    # todo: run as python
    # export PYTHONPATH=$SPARK_HOME/python/:$PYTHONPATH
    # python
    # import dirhash
    # hash_value = dirhash.hash_directory('/dirhash', 'sha256', '128M' )
