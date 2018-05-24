#!/usr/bin/python

"""create and verify hash values for contents of entire directores

This module provides some functions to create a hash value that uniquely
(up to collisions in a hash function)
determines the contents of a directory,
including the contents of all sub-directories.

The hash value will be computed in parallel, using pyspark.
Large files will be split into blocks of a given size,
such that even single files can be hashed in parallel.

Example use:
>>> import dirhash
>>> hash_value = dirhash.hash_directory(
>>>     '/path/to/dir', 'sha256', '128M' [, your_spark_context]
>>> )

This will return a string of the form
"v1-sha256-128M-hash_value_in_hexadecimal".

.. note:: The directory to be hashed must be available to PySpark.
Thus, it should be located e.g. on a Hadoop file system,
and should be available at this path on all worker nodes in the
cluster.

.. note::
A single directory may have very different hash values, depending
on the settings (block size and hash function) used for hashing.
Therefore, this module does not only offer functions that return the
raw hash value (in hexadecimal), but also offers an interface working
with (hash function, blocksize, raw hash value)-triples.

If you have a hash value and you would like to verify that a
given directory has this hash value use:
>>> dirhash.verify_directory_hash('/path/to/dir',
        hash_value [, your_spark_context])
The hash value will be parsed, the contents of the given directory
will be hashed with the settings extracted from the string, and
the hash values will be compared.
The result is a class:`HashComparisonResult` object, which,
when cast to boolean, has the value `True` if the hash values match
and `False` otherwise.

The two functions of hashing a directory and verifying a hash are also
accessible via a more direct interface: func:`hash_directory_raw` and
func:`verify_raw_directory_hash`.
These return only the actual hash value, and accept the block size as
a plain python integer.

This module makes use of some cryptographic hash functions,
such as the SHA2-family (SHA224, SHA256, SHA384, SHA512).
The list of hash functions supported on your platform can be obtained by
calling :func:`supported_hash_algorithms()`.
Note that this list differs from :hashlib.algorithms_available:,
because hashlib contains some algorithms that are
known not to provide adequate collision resistance.

In Python 3.6 and later, this module also supports the SHA3-family
(except for the SHAKE-algorithms) and BLAKE2.
If you want to use these hash functions in earlier Python versions,
please install the `pysha3` module and/or the `pyblake2` module from PyPI.
If these are installed, this module will use them automagically.

When the blocksize is given as a string, it should be specified
as in the example '32M'.
In general, the blocksize must contain an integer between 1 and 1023
(both inclusive) and an optional suffix.
The suffix may be 'k' or 'K' (meaning the blocksize is measured
in kilobytes, i.e. multiples of 1024 bytes), 'M' (megabytes,
1024*1024 bytes) or 'G' (gigabytes, 1024*1024*1024 bytes).

This module uses a Python logger object obtained using
>>> logging.getLogger("dirhash")
You can configure logging in this module using the `logging.config` module.

This module can be run in a stand-alone fashion. To do so, run
>>> python dirhash.py /path/to/dir
When doing so, the hash value will be printed to stdout.
You may optionally specify the hash function to be used as well as the
block size via the -a/--hash-algorithm and -b/--block-size switches.
See `python dirhash.py --help` for details.

To check a hash value, use
>>> python dirhash.py /path/to/dir --verify hash_string
This will print some information to stdout and exit with exit code 0
if the hash values match.
It will exit with a non-zero exit code if there has been a problem while
computing the hash value or if the hash values mismatch.

"""


#imports
import subprocess
import argparse
import hashlib
import re
import sys
import binascii
from pyspark import SparkContext
import logging

import os
import shutil

# get logger instance
_logger = logging.getLogger("dirhash")

# If this script is invoked stand-alone, configure the logger manually.
# Otherwise, whoever is using this module can configure the logger using the
# logging.config module.
if __name__ == "__main__":
    _handler = logging.StreamHandler()
    _formatter = logging.Formatter("[%(asctime)s]: %(levelname)s: %(message)s")
    _handler.setFormatter(_formatter)
    _logger.addHandler(_handler)
    del _handler, _formatter

# SHA3 and Blake2 are only built-in starting from Python version 3.6.
# In Python versions before 3.6, try importing SHA3 and blake2.
# See: https://pypi.python.org/pypi/pysha3
# See: https://pypi.python.org/pypi/pyblake2
if sys.version_info < (3,6):

    try:
        # Importing the sha3 module patches the hashlib module,
        # so that no further changes are necessary.
        import sha3 as _sha3
        _sha3_present = True
        _logger.info("Imported SHA-3 module")
    except Exception as e:
        _sha3_present = False
        _logger.info(
            "Failed to import SHA-3 module with exception %s", e,
            extra={'exception': e}
        )

    try:
        from pyblake2 import blake2b as _blake2b, blake2s as _blake2s
        _blake2_present = True
        _logger.info("Imported blake2 module")
    except Exception as e:
        _blake2_present = False
        _logger.info(
            "Failed to import pyblake2 module with exception %s", e,
            extra={'exception': e}
        )


# CONFIG
_DEFAULT_BLOCK_SIZE = '128M'
_MAX_BLOCK_SIZE_INT = 1024
# If supporting _MAX_BLOCK_SIZE_INT > 1024, we might have to normalize the block size.
# E.g., the block size 2048k and 2M had the same meaning. This should be normalized.

# we use a a whitelist to exclude known weak algorithms such as md5, SHA, or SHA-1.
_HASH_FUNC_WHITELIST = {
    # SHA-2 family
    'sha224', 'SHA224',
    'sha256', 'SHA256',
    'sha384', 'SHA384',
    'sha512', 'SHA512',
    # blake family
    'blake2b',
    'blake2s',
    # sha 3 family
    'sha3_224',
    'sha3_256',
    'sha3_384',
    'sha3_512',
}


_supported_hash_algorithms = hashlib.algorithms_available & _HASH_FUNC_WHITELIST
if sys.version_info < (3,6):
    if _blake2_present:
        _supported_hash_algorithms |= {"blake2b", "blake2s"}
    if _sha3_present:
        _supported_hash_algorithms |= {
            "sha3_224", "sha3_256", "sha3_384", "sha3_512"
        }
_supported_hash_algorithms = frozenset(_supported_hash_algorithms)
_logger.debug("Supported hash algorithms: %s" % _supported_hash_algorithms)

def supported_hash_algorithms():
    """Returns a set of all hash algorithms supported on the current platform.
    
    More specifically, the returned set contains the names of all
    supported hash algorithms.
    These can be passed to func:`get_hash_func()` in order to obtain an
    instance of the respective hash function."""

    return _supported_hash_algorithms

def get_hash_func(name):
    """Returns an instance of the hash function with the given name.
    
    :param name: A name of the desired hash function. Must be one of the
		names returned by func:`supported_hash_algorithms()`.
    :returns: an instance of the hash function.
		The returned instance can be used as if returned by `hashlib.new()`.
    :raises ValueError: if the name does not refer to a supported hash function."""

    if name in hashlib.algorithms_available & supported_hash_algorithms():
        return hashlib.new(name)
    elif _sha3_present and name == "sha3_224":
        return hashlib.sha3_224()
    elif _sha3_present and name == "sha3_256":
        return hashlib.sha3_256()
    elif _sha3_present and name == "sha3_384":
        return hashlib.sha3_384()
    elif _sha3_present and name == "sha3_512":
        return hashlib.sha3_512()
    elif _blake2_present and name == "blake2s":
        return _blake2s()
    elif _blake2_present and name == "blake2b":
        return _blake2b()
    else:
        raise ValueError("unsupported hash function: \"%s\"" % name)

_blocksize_regex_as_str = '(\d+)([kKMG]?)'
_blocksize_regex = re.compile("\A" + _blocksize_regex_as_str + "\Z")

def _parse_blocksize(blocksize):
    match = _blocksize_regex.match(blocksize)
    i = int(match.group(1))
    if i <= 0 or i >= _MAX_BLOCK_SIZE_INT:
        raise ValueError(
            "Integer in block size %s out of range. " \
            "(Must be between %s and %s.)" % (blocksize, 0, _MAX_BLOCK_SIZE_INT)
        )
    suffix = match.group(2)
    factor = _suffix_factor(suffix)
    return i * factor

def _suffix_factor(suffix):
    if suffix == '':
        return 1
    elif suffix == 'k' or suffix == 'K':
        return 1024
    elif suffix == 'M':
        return 1024 * 1024
    elif suffix == 'G':
        return 1024 * 1024 * 1024
    else:
        raise ValueError("Unknown block size suffix: \"%s\"" % suffix)

def _build_hash_string(algo, blocksize, digest):
    assert(algo in _supported_hash_algorithms)
    assert(_parse_blocksize(blocksize)) # this will throw an error if the blocksize is not well-formatted
    return "-".join(("v1", algo.lower(), blocksize, digest))


_hexadecimal_regex_as_str = "[0-9a-fA-F]+"
_hexadecimal_regex = re.compile("\A" + _hexadecimal_regex_as_str + "\Z")

def _parse_hash_string(hash_string):
    version, hash_function, blocksize, hash_value = hash_string.split("-")
    if version != "v1":
        raise ValueError("unknown hash value version: \"%s\"" % version)

    # raises an exception if the hash function is unknown
    get_hash_func(hash_function)

    # raises an exception if the blocksize is not well-formatted
    blocksize = _parse_blocksize(blocksize)

    # check that the hash value is hexadecimal
    if not _hexadecimal_regex.match(hash_value):
        raise ValueError("malformed hash value: \"%s\"" % hash_value)

    return version, hash_function, blocksize, hash_value


def _file_chunks(sc, fpath, recordlength=1024):
    """returns a RDD consisting of all chunks of the file at fpath."""

    rdd = sc.newAPIHadoopFile(fpath,
        'niklasb.sparkhacks.FixedLengthBinaryInputFormat',
        'org.apache.hadoop.io.LongWritable',
        'org.apache.hadoop.io.BytesWritable',
        conf={'org.apache.spark.input.FixedLengthBinaryInputFormat.recordLength': str(recordlength)})
    rdd.setName(fpath)
    return rdd

def _hash_chunk(path, num, content, hash_func):
    """called to compute the hash of a single chunk of data.
    
    :param content: the content of chunk, i.e. a sequence of characters and/or bytes.
    :param path: the path of the file that content is from.
    :param num: specifies the block index of content in the file. (I.e., 0, 1, 2, 3, ...)
    :param hash_func: the name of the hash function to be used for hashing.
    """

    h = get_hash_func(hash_func)
    h.update(path)
    h.update('\0')
    h.update(str(num))
    h.update('\0')
    h.update(content)
    return (path, num, h.digest())



def hash_directory_raw(dir, algo, blocksize, sparkcontext=None):

    """Computes the hash value of the directory `dir`, using the given hash function `algo` and the given `blocksize`.
    
    This function behaves very much like func:`hash_directory`, but returns _only_ the hash value as a hexadecimal string,
    and accepts the blocksize as a plain integer.
    
    :param dir: The path to the directory that should be hashed.
    :param algo: The name of the hash algorithm to be used. Must be one of the names returned by func:`supported_hash_algorithms()`.
    :param blocksize: Files larger than this will be split into blocks of this size. Must be given as a plain integer.
    :param sparkcontext: If given, the hashing jobs will be executed within this `SparkContext`.
        Otherwise, a new SparkContext will be created.
    :returns: the string representation of the hash value of the given directory, excluding the hash function's name and the blocksize.
    :raises ValueError: if algo does not refer to a supported hash function.
    """

    dir = dir.rstrip("/")

    # make sure we have a Spark Context
    if sparkcontext is None:
        _logger.debug("creating a new Spark context")
        sc = SparkContext(appName="DirHash")
        try:
            return hash_directory_raw(dir, algo, blocksize, sc)
        finally:
            sc.stop()
    else:
        _logger.debug("using spark context passed as an argument")
        sc = sparkcontext

    _logger.info("Hashing directory %s with algorithm %s and blocksize %d", dir, algo, blocksize)

    # FIXME: there must be cleaner way to recursively list files with hadoop in PySpark.
    directory_listing = subprocess.check_output(
        ['hadoop', 'fs', '-ls', '-R', dir],
        stderr=subprocess.PIPE
    )
    _logger.debug("directory listing as returned by hadoop: %s", directory_listing)
    directory_listing = directory_listing.strip().splitlines()

    # FIXME: Is s (for setuid, sticky, ...) valid on HDFS?
    regex = (
        "\A" # start of string \
        "(-|d)" # initial character: flag specifying if this is a directory or a regular file \
        "[rwxs-]{9}" # access rights \
        "\s+" # space \
        "(-|\d+)" # number of replicas \
        "\s+" # space \
        "(\w|-)+" # owner name (userId) \
        "\s+" #space \
        "(\w|-)+" # group name (groupId) \
        "\s+" # space \
        "\d+" # file size in bytes \
        "\s+" # space \
        "\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}" # modification date and time \ 
        # the time format seems to be fixed and not localized \
        "\s+" # space \
        "(.*)" # filename \
        "\Z" #end of string
    )
    regex = re.compile(regex)
    target_relative_filepaths = list()
    hadoop_relative_filepaths = list()
    all_entries = list()
    for entry in directory_listing:
        # parse entry
        match = regex.match(entry.strip())
        file_path = match.group(5)
        is_dir = (match.group(1) == "d")
        # print(match, match.group(0), match.group(1), match.group(2), match.group(3), match.group(4), match.group(5))
        assert(file_path.startswith(dir))


        target_relative_filepath = file_path[len(dir) + 1:]
        if not is_dir:
            hadoop_relative_filepaths.append(file_path)
            target_relative_filepaths.append(target_relative_filepath)
            all_entries.append(target_relative_filepath)
        else:
            all_entries.append(target_relative_filepath + '/')

    # some logging output
    _logger.info("All entries: %s", all_entries)
    _logger.info("Hashing files: %s", target_relative_filepaths)

    class _Mapper:
        def __init__(self, filename):
            self.filename = filename
        def __call__(self, t):
            num, content = t
            return (self.filename, num, content)

    rdds = [
        _file_chunks(sc, dir + '/' + f, blocksize).map(_Mapper(f)) \
        for f in target_relative_filepaths
    ]

    all_chunks = sc.parallelize([])
    for rdd in rdds:
        all_chunks = all_chunks.union(rdd)

    def _hash(t):
        f, num, content = t
        return _hash_chunk(f, num, content, algo)

    hashed_chunks = all_chunks.map(_hash)
    x = hashed_chunks.sortBy(lambda t: (t[0], t[1])).collect()

    _logger.debug("Chunk Hashes:\n%s", str(x))

    # make sure that all_entries is sorted
    all_entries.sort()
    _logger.debug("Done sorting...")

    # build the result hash
    res = get_hash_func(algo)

    _logger.debug("Creating final hash value.")
    _logger.debug("All entries: " + str(all_entries))
    # first hash the number of all entries, then a zero byte
    res.update(str(len(all_entries)) + "\0")
    # then, hash the list of all entries, separated by zero bytes
    res.update("\0".join(all_entries))

    # then, another zero bytes as a separator
    res.update("\0")
    _logger.debug("starting to add digests to the final hash value:")

    # finally, the hash values of all chunks
    for path, num, content in x:
        _logger.debug("digest for chunk %s of file %s: %s", num, path, binascii.hexlify(content))
        res.update(content)

    # this is the result:
    hash_value = res.hexdigest()

    _logger.info("final hash value: %s", hash_value)
    return hash_value

def hash_directory(dir, algo, blocksize, sparkcontext=None):
    """Computes the hash value of the directory `dir`, using the given hash function `algo` and the given `blocksize`.
    
    :param dir: The path to the directory that should be hashed.
    :param algo: The name of the hash algorithm to be used. Must be one of the names returned by func:`supported_hash_algorithms()`.
    :param blocksize: Files larger than this will be split into blocks of this size. Must be given in the string representation.
    :param sparkcontext: If given, the hashing jobs will be executed within this `SparkContext`.
        Otherwise, a new `SparkContext` will be created.
    :returns: the string representation of the hash value of the given directory, including the hash function's name and the blocksize.
    :raises ValueError: if `algo` does not refer to a supported hash function or the `blocksize` is ill-formatted.
    """

    raw_hash = hash_directory_raw(dir, algo, _parse_blocksize(blocksize), sparkcontext)
    return _build_hash_string(algo, blocksize, raw_hash)


class HashComparisonResult:

    """result of a hash comparison as `(boolean, actual_hash_value)`.
    
    This class represents the result of a comparison of hash values.
    It basically implements a tuple of a boolean value named `match`
    (set to `True` if the two hash values are equal
    and `False` otherwise) and the actual hash value of the contents
    of a directory found on disk.
    
    More than that, this class uses some simple Python magic to make
    sure that when an object is interpreted as a boolean value,
    the `match` value is returned.
    
    Thus, you can say:
    >>> result = dirhash.verify_directory_hash(
            '/path/to/dir', hash_value)
    >>> if result:
    >>>        print("The hash values match!")
    >>> else:
    >>>        print("The hash values don't match :(")
    >>>        print("actual hash value: %s".format(
    >>>            r.actual_hash_value()
    >>>        ))
    """

    def __init__(self, match, actual_hash_value):
        self._match = match
        self._actual_hash_value = actual_hash_value

    def match(self):
        return self._match

    def actual_hash_value(self):
        return self._actual_hash_value

    # in Python 3.0 and later, the magic method for conversion to a
    # boolean value is named __bool__.
    # in Python 2, __nonzero__ is used.
    if sys.version_info < (3, 0):
        def __nonzero__(self):
            return self.match()
    else:
        def __bool__(self):
            return self.match()

    def __eq__(self, other):
        if isinstance(other, HashComparisonResult):
            return self.match() == other.match() and \
                self.actual_hash_value() == other.actual_hash_value()
        else:
            return NotImplemented

    def __repr__(self):
        return "HashComparisonResult(%s, %s)" % \
            (self.match(), self.actual_hash_value())



def verify_raw_directory_hash(dir, algo, blocksize, digest, sparkcontext=None):

    """verifies that the given directory `dir` has the given hash value `digest`.
    
    :param dir: the path to a directory whose hash value shall be computed and checked against `digest`.
    :param algo: the hash algorithm to use for hashing. Must be one of the names returned by func:`supported_hash_algorithms()`.
    :param blocksize: Files larger than this will be split into blocks of this size. Must be a plain integer.
    :param digest: the expected hash value of the directory `dir`.
    :param sparkcontext: If given, the hashing jobs will be executed within this `SparkContext`. Otherwise,
        a new `SparkContext` will be created.
    :returns: a class:`HashComparisonResult` object (indicating if the actual hash value of `dir` matches `digest` and containing the actual hash value of `dir`).
    :raises ValueError: if algo does not refer to a supported hash function."""

    h = hash_directory_raw(dir, algo, blocksize, sparkcontext)
    return HashComparisonResult(h == digest, h)


def verify_directory_hash(dir, hash_string, sparkcontext=None):

    """verifies that the given directory has the given hash value.
    
    :param dir: the path to a directory whose hash value shall be computed and checked against `hash_string`.
    :param hash_string: must contain the name of the hash function to be used, the block size, and the actual digest (in hexadecimal)
        in the format returned by func:`hash_directory`.
    :param sparkcontext: the `SparkContext` to use during hashing. If set to `None`, a new `SparkContext` will be created.
    :returns: a class:`HashComparisonResult` object (indicating if the actual hash value of `dir` matches the hash value contained in `hash_string`)
    and containing the actual hash value of `dir`).
    :raises ValueError: if `hash_string` can not be parsed.
    """

    v, algo, blocksize, hash_value = _parse_hash_string(hash_string)

    h = hash_directory_raw(dir, algo, blocksize, sparkcontext)

    return HashComparisonResult(h == hash_value, h)


def move_folder_to_hashed_archive(hashed_repo, path_to_folder, hash_str, set_readonly = True):
    new_path = os.path.join(hashed_repo, hash_str)
    # in python3 we could do:
    # shutil.copytree(path_to_folder, new_path, copy_function=os.link)
    # until then we have to do this:
    try:
        # Note: in python 2.7 CalledProcessError has no stderr, so we need to do the "trick" with redirecting stderr
        if not os.path.isdir(new_path):
            # subprocess.check_output(['cp', '-rlu', path_to_folder, new_path],stderr=subprocess.STDOUT)
            shutil.move(path_to_folder, new_path)
        else:
            _logger.info("folder alread exists: "+ new_path + "\t so instead of moving the source, we just delete it.")
            shutil.rmtree(path_to_folder)
        if set_readonly:
            subprocess.check_output(['chmod', '-R', 'a-w', new_path],stderr=subprocess.STDOUT)
        return new_path
    except subprocess.CalledProcessError as e:
        message = """Error while executing command %s
output: %s
""" % (e.cmd, e.output)
        _logger.error(message)



def _main(argv):
    """Used when running this module stand-alone.
    
    :param argv: the complete command line arguments (e.g. `sys.argv`).
    """

    _logger.debug("main called with argv == %s" % argv)

    p = argparse.ArgumentParser()
    p.add_argument('--check', '-c', '--verify', '-v',
        help='Verify that the given directory has the given hash.',
        dest='hash_str'
    )

    p.add_argument('--check-name', '-cn', '--verify-name', '-vn',
        help='Verify that the name of the given directory matches it\'s content.',
        dest='hash_name',
        action='store_true')

    p.add_argument('--block-size', '-b',
        default=_DEFAULT_BLOCK_SIZE,
        help='Block size. Ignored if --verify (or -v, -c or --check) is given.',
        dest="blocksize"
    )

    p.add_argument(
        '--hash-algorithm', '--algorithm', '-a',
        default='sha256',
        choices=supported_hash_algorithms(),
        dest='hash_algo',
        help='The Hash function used to process individual blocks/files.' \
            ' Ignored if --verify (or -v, -c or --check) is given.'
    )

    p.add_argument('--move-to-archive',
        help='hashes the folder and then moves the folder the hashed-repo',
        dest='archive_path'
    )

    p.add_argument('--softlink', '--sl', '-s',
        help='after move the folder to the hashed repo, a softlink will be created pointing '\
             'to the hashed repo. Must be used together with --add-folder-to-repo',
        dest='softlink'
    )

    p.add_argument('dir')

    args = p.parse_args(argv[1:])
    _logger.debug("Parsed arguments: %s", args)

    if args.hash_str or args.hash_name:
        if args.hash_str and args.hash_name:
            raise ValueError("parameter --check and --check-name can not be used together ")

        if args.hash_str:
            expected_hash = args.hash_str
        else:
            expected_hash = os.path.basename(os.path.normpath(args.dir))


        match = verify_directory_hash(args.dir, expected_hash)
        _, _, _, expected_hash_value = _parse_hash_string(expected_hash)

        if match:
            print(
                "The hash values match:\n%9s %s\n%9s %s\n" % \
                (
                    "Expected:", expected_hash_value,
                    "Actual:", match.actual_hash_value()
                )
            )
        else:
            print(
                "Hash value mismatch:\n%9s %s\n%9s %s\n" % \
                (
                    "Expected:", expected_hash_value,
                    "Actual:", match.actual_hash_value()
                )
            )
            exit(1) # return error to caller
    elif args.archive_path:
        if args.softlink:
            if os.path.exists(args.softlink) and os.path.normpath(args.dir) != os.path.normpath(args.softlink):
                print("softlink-target alread exists")
                exit(1)

        new_path = move_folder_to_hashed_archive(args.archive_path,args.dir, hash_directory(args.dir, args.hash_algo, args.blocksize))
        print(new_path)
        if args.softlink:
            if not os.path.exists(args.softlink):
                os.makedirs(args.softlink)

            try:
                # os.symlink doesn't work. I don't know why.
                subprocess.check_output(['ln', '-s', new_path, args.softlink],stderr=subprocess.STDOUT)

            except subprocess.CalledProcessError as e:
                message = """Error while executing command %s
output: %s
""" % (e.cmd, e.output)
                _logger.error(message)
    elif args.softlink:
        print("option --softlink must only be used with option --move--to-archive")
    else:
        h = hash_directory(args.dir, args.hash_algo, args.blocksize)
        print(h)


__all__ = [
    hash_directory, verify_directory_hash,
    hash_directory_raw, verify_raw_directory_hash,
    supported_hash_algorithms, get_hash_func
]

if __name__ == "__main__":
    _main(sys.argv)

