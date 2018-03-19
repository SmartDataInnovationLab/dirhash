# Dirhash

Create and verify hash values for contents of entire directories in parallel with PySpark.

Authors: Gunnar Hartung (gunnar.hartung@kit.edu) / Niklas Baumstark (niklas.baumstark@gmail.com)

## Installing

First build the Scala component:
    
    $ cd /path/to/dirhash
    $ mvn package

You may want to install the pysha3 and/or pyblake2 module from PyPI:
    
    $ pip install pysha3 pyblake2

Run:

    $ ./run.sh hdfs://localhost/tmp/input-text 2>/dev/null
    
