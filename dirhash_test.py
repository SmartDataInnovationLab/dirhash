#!/usr/bin/python

import logging.config

# configure logging for the dirhash module, _before_ that module is loaded
if __name__ == "__main__":
    logging_config = {
        'version': 1,
        'formatters': {
            'f': {
                'format': "[%(asctime)s]: %(levelname)s: %(message)s"
            }
        },
        'filters': dict(),
        'handlers': {
            'h': {
                'class': 'logging.StreamHandler',
                'level': 'DEBUG',
                'formatter': 'f'
            }
        },
        'loggers': {
            'dirhash': {
                'level': 'DEBUG',
                'handlers': ['h']
            }
        },
        'root': dict()
    }
    logging.config.dictConfig(logging_config)
    del logging_config

# more imports
import unittest
import tempfile
import dirhash
import shutil
import os
import logging.config
from pyspark import SparkContext, SparkConf



class DirHashTests(unittest.TestCase):
    
    # TEST DATA
    LOREM_IPSUM_PATH = "dir/subdir1/loremipsum.txt"
    LOREM_IPSUM_TEXT = "Lorem ipsum dolor sit amet..."
    LOREM_IPSUM_CHUNK_SHA224 = "G\xf6C\x13;\xc4\x85\xcc\xd3_\x80bH~\xf5\xde" + \
        "\xa8&\xc7\xceGa\x17'\x87\xcc\x0em"
    LOREM_IPSUM_CHUNK_SHA256 = '1\xcf\x1c7\xb0\xad4\xb0\xf38\xdf\xd6~(\xf8N' + \
        'l%\x0f\xf8dI\xd0\xca\x04\xe4Y\xbf]\x8e\xce\xf2'
    
    HELLO_WORLD_HTML_PATH = "dir/subdir1/hello_world.html"
    HELLO_WORLD_HTML_TEXT = "<html><body>Hello, World!</body></html>"
    HELLO_WORLD_CHUNK_SHA256 = (
        "E\x805^\xbe\x17n\xaf\x91\x04`J)\xec\xf9J)\xd0\xfc\x03q\x95\xcbq\x88\xdbM9^\x08>\xab"
    )
    
    PASSWORDS_PATH = "dir/subdir2/my_passwords.txt"
    PASSWORDS_TEXT = (
        "123456\n" \
        "password\n" \
        "qwerty\n" \
        "admin\n" \
        "1968\n"
    )
    PASSWORDS_CHUNK_SHA256 = (
        'Rl\x93\xbf\x90u!.\xde\x97\x16-h\xa4v\x97\xb4\x12\xa1R\xe7\x80KS\xcb\x03jm\x1b6\x160'
    )
    PASSWORDS_CHUNK_SHA384 = (
        '\x0c\x9a\xd0L\x85S\x04n\xac\xbcb`\xc3-\xaav\xe9\xf8\x8d\x0f3\xf7|\xf3\xae\xbd' \
        '\x03\xe2\x04\xe5\xe1h\xd50\x87K\x129\xf7\xd9\x9b\xfcdx\x9f\xc1"N'
    )
    
    ABC_PATH = "dir/subdir3/abc.txt"
    ABC_TEXT = "abc"
    ABC_CHUNK_SHA256 = (
        '\xb4\xf5g\xd6\xc8\x9c\xd9\x99\x8b\xf0\x82\x92\xba\x1f\x04\x19\x0b"\x13#mV\x91\xb2\xa2Jj\xdc\xef\x1d\xc6c'
    )
    ABC_CHUNK_SHA512 = (
        "^{\xfa\xf0\xfamnF5{\x0cL\x19\xe8]\xcf\x17\xd0\xac\x91\x0f\xc8)\xc4\x80\xd0DW\xf0'" \
        "\x95\xfa#\xae\tma\xac\xfb\t\xd5\x11\x0e\xa250\xf0\xdb\xd5\xb4\xa5\xd8\x19\x07\x1a\x00\xb4.3u $\t\xea"
    )
    
    
    EMPTY_FILE_PATH = "dir/empty_file.txt"
    EMPTY_FILE_CHUNK_SHA224 = (
        '\x9b"qI\xfd\xfc\xf5\x94\x98\x04\x96\xa2\x03\xb9F\xf8[G\xc2\x0cOq-\xd5Y\xfc\xe4G'
    )
    EMPTY_FILE_CHUNK_SHA256 = (
        'Y\xd4\xae{\xc1]h\xb0!\xc0\xc9U|5h\xb7i\xe3ml\xc9\xa5e\x82\xccL\x1b\x7f\x1d\x9a\x1b\xac'
    )
    
    MANY_ZEROS_PATH = "32M Zeros.bin"
    MANY_ZEROS_CHUNK_SHA256 = (
        'g\xee%>\xb4\xf7\xdb6\x87\xec\xd8\xfb\x8e\x8f\xd6q+' \
        '\x82\x8f\x1b\x8ft&\x91\x07\x03C\xb1\xc5\xbdc\x0b'
    )

    
    @classmethod
    def setUpClass(self):
        """creates some test data in a temporary folder.
        
        This function is called automatically before the tests are run."""
        
        # Create a new temporary folder
        self.dir = tempfile.mkdtemp(self.__name__, dir=os.getcwd()) # this folder should be on a hadoop file system
        os.mkdir(self.dir)
        
        
        dirhash._logger.info("writing test data to folder \"%s\"", self.dir)
        
        # create some sub-folder
        os.makedirs(self.dir + "/dir/subdir1")
        os.makedirs(self.dir + "/dir/subdir2")
        os.makedirs(self.dir + "/dir/subdir3")
        os.makedirs(self.dir + "/dir/emptysubdir")
        dirhash._logger.info("created subdirectories")
        
        # Create a lorem ipsum file
        with open(self.dir + "/" + self.LOREM_IPSUM_PATH, "w") as f:
            f.write(self.LOREM_IPSUM_TEXT)
        
        # Create an HTML file
        with open(self.dir + "/" + self.HELLO_WORLD_HTML_PATH, "w") as f:
            f.write(self.HELLO_WORLD_HTML_TEXT)
        
        # Create a file holding typical passwords
        with open(self.dir + "/" + self.PASSWORDS_PATH, "w") as f:
            f.write(self.PASSWORDS_TEXT)
        
        # just a few characters
        with open(self.dir + "/" + self.ABC_PATH, "w") as f:
            f.write(self.ABC_TEXT)
        
        # an empty file
        with open(self.dir + "/" + self.EMPTY_FILE_PATH, "w") as f:
            f.write("")
        
        # create a file of 32 * 1024 * 1024 zero bytes
        with open(self.dir + "/" + self.MANY_ZEROS_PATH, "wb") as f:
            f.write(b"\0" * (32 * 1024 * 1024))
        
        # create a spark context for execution, that will run all jobs locally
        self._context = SparkContext(appName="dirhash_test")
        self._context.addPyFile('dirhash.py')
        
    @classmethod
    def tearDownClass(cls):
        """deletes the test files created by :setUpClass."""
        # recursively delete the temporary test files
        shutil.rmtree(cls.dir)
        
        # get rid of the spark context
        cls._context.stop()
        del cls._context
        
        
    def test_hash_block(self):
        
        def compare_hashes(path, chunkno, content, algo, expected_hash):
            actual = dirhash._hash_chunk(path, chunkno, content, algo)
            expected = (path, chunkno, expected_hash)
            self.assertEqual(actual, expected)
        
        compare_hashes(self.LOREM_IPSUM_PATH, 0, self.LOREM_IPSUM_TEXT, 'sha224', self.LOREM_IPSUM_CHUNK_SHA224)
        compare_hashes(self.LOREM_IPSUM_PATH, 0, self.LOREM_IPSUM_TEXT, 'sha256', self.LOREM_IPSUM_CHUNK_SHA256)
        
        compare_hashes(self.HELLO_WORLD_HTML_PATH, 0, self.HELLO_WORLD_HTML_TEXT, 'sha256', self.HELLO_WORLD_CHUNK_SHA256)
        
        compare_hashes(self.PASSWORDS_PATH, 0, self.PASSWORDS_TEXT, 'sha256', self.PASSWORDS_CHUNK_SHA256)
        compare_hashes(self.PASSWORDS_PATH, 0, self.PASSWORDS_TEXT, 'sha384', self.PASSWORDS_CHUNK_SHA384)
        
        compare_hashes(self.ABC_PATH, 0, self.ABC_TEXT, 'sha256', self.ABC_CHUNK_SHA256)
        compare_hashes(self.ABC_PATH, 0, self.ABC_TEXT, 'sha512', self.ABC_CHUNK_SHA512)
        
        compare_hashes(self.EMPTY_FILE_PATH, 0, "", 'sha224', self.EMPTY_FILE_CHUNK_SHA224)
        compare_hashes(self.EMPTY_FILE_PATH, 0, "", 'sha256', self.EMPTY_FILE_CHUNK_SHA256)
        
        compare_hashes(self.MANY_ZEROS_PATH, 0, "\0" * (32 * 1024 * 1024), 'sha256', self.MANY_ZEROS_CHUNK_SHA256)
        
    
    def test_chunking(self):
        
        sc = self._context
        
        rdd = dirhash._file_chunks(sc, self.dir + "/" + self.ABC_PATH, 1024)
        rdd = rdd.collect()
        self.assertEqual(rdd, [(0, bytearray(self.ABC_TEXT))])
        
        rdd = dirhash._file_chunks(sc, self.dir + "/" + self.ABC_PATH, 1)
        actual = rdd.collect()
        expected = [(0, bytearray("a")), (1, bytearray("b")), (2, bytearray("c"))]
        self.assertEqual(actual, expected)
        
        rdd = dirhash._file_chunks(sc, self.dir + "/" + self.ABC_PATH, 2)
        actual = rdd.collect()
        expected = [(0, bytearray("ab")), (1, bytearray("c"))]
        self.assertEqual(actual, expected)
        
        rdd = dirhash._file_chunks(sc, self.dir + "/" + self.EMPTY_FILE_PATH, 1024)
        actual = rdd.collect()
        expected = []
        self.assertEqual(actual, expected)
        
        rdd = dirhash._file_chunks(sc, self.dir + "/" + self.MANY_ZEROS_PATH, 32 * 1024 * 1024)
        actual = rdd.collect()
        expected = [(0, bytearray("\0" * (32 * 1024 * 1024)))]
        self.assertEqual(actual, expected)
        
        rdd = dirhash._file_chunks(sc, self.dir + "/" + self.MANY_ZEROS_PATH, 16 * 1024 * 1024)
        actual = rdd.collect()
        expected = [(0, bytearray("\0" * (16 * 1024 * 1024))), (1, bytearray("\0" * (16 * 1024 * 1024)))]
        self.assertEqual(actual, expected)
        
        rdd = dirhash._file_chunks(sc, self.dir + "/" + self.MANY_ZEROS_PATH, 32 * 1024)
        actual = rdd.collect()
        arr = bytearray("\0" * (32 * 1024))
        expected = list(enumerate([arr] * 1024))
        self.assertEqual(actual, expected)
    
    def test_dirhash(self):
        
        # if this test fails, make sure that test_chunking and test_hash_block 
        # work properly. Only if these tests pass and this one does not,
        # work on this test case.
        
        complete_directory_listing = [
            "32M Zeros.bin",
            "dir/",
            "dir/emptyfile.txt",
            "dir/emptysubdir/",
            "dir/subdir1/",
            "dir/subdir1/hello_world.html",
            "dir/subdir1/loremipsum.txt",
            "dir/subdir2/",
            "dir/subdir2/my_passwords.txt",
            "dir/subdir3/",
            "dir/subdir3/abc.txt",
        ]
        
        h = dirhash.get_hash_func('sha256')
        h.update("11\0") # length of the directory listing
        h.update("32M Zeros.bin\0")
        h.update("dir/\0")
        h.update("dir/empty_file.txt\0")
        h.update("dir/emptysubdir/\0")
        h.update("dir/subdir1/\0")
        h.update("dir/subdir1/hello_world.html\0")
        h.update("dir/subdir1/loremipsum.txt\0")
        h.update("dir/subdir2/\0")
        h.update("dir/subdir2/my_passwords.txt\0")
        h.update("dir/subdir3/\0")
        h.update("dir/subdir3/abc.txt\0")
        
        h.update(self.MANY_ZEROS_CHUNK_SHA256)
        h.update(self.HELLO_WORLD_CHUNK_SHA256)
        h.update(self.LOREM_IPSUM_CHUNK_SHA256)
        h.update(self.PASSWORDS_CHUNK_SHA256)
        h.update(self.ABC_CHUNK_SHA256)
        
        correct_hash_value = h.hexdigest()
        correct_hash_str = 'v1:sha256:32M:' + correct_hash_value
        
        # test _build_hash_str
        self.assertEqual(dirhash._build_hash_string('SHA256', '32M', correct_hash_value), correct_hash_str)
        
        actual = dirhash.hash_directory_raw(self.dir, 'sha256', 32 * 1024 * 1024, self._context)
        self.assertEqual(actual, correct_hash_value)
        
        # test that everything works equally when the directory ends with a "/".
        # In the past, there has been a bug, due to which things did _not_ work.
        actual = dirhash.hash_directory_raw(self.dir + "/", 'sha256', 32 * 1024 * 1024, self._context)
        self.assertEqual(actual, correct_hash_value)
        self.assertTrue(actual)
        
        # test verify_raw_directory_hash
        expected = dirhash.HashComparisonResult(True, correct_hash_value)
        print("DEUBG: " + str(self._context))
        actual = dirhash.verify_raw_directory_hash(self.dir, 'sha256', 32 * 1024 * 1024, correct_hash_value, self._context)
        self.assertEqual(actual, expected)
        self.assertTrue(actual)
        
        # test hash_directory
        actual = dirhash.hash_directory(self.dir, 'sha256', '32M', self._context)
        self.assertEqual(actual, correct_hash_str)
        
        #test verify_directory_hash
        expected = dirhash.HashComparisonResult(True, correct_hash_value)
        actual = dirhash.verify_directory_hash(self.dir, correct_hash_str, self._context)
        self.assertEqual(actual, expected)
        self.assertTrue(actual)
        
    #FIXME: maybe test _parse_block_size, _get_suffix_factor and _build_hash_str


class HashComparisonResultTest(unittest.TestCase):
		
	f1 = dirhash.HashComparisonResult(True, "foo")
	f2 = dirhash.HashComparisonResult(True, "bar")
	f3 = dirhash.HashComparisonResult(False, "foo")
	f4 = dirhash.HashComparisonResult(False, "bar")
	f5 = dirhash.HashComparisonResult(True, "bar")
	f6 = dirhash.HashComparisonResult(False, "foo")
		
	def test_true_false_interpretation(self):
		self.assertTrue (self.f1)
		self.assertTrue (self.f2)
		self.assertFalse(self.f3)
		self.assertFalse(self.f4)
		self.assertTrue (self.f5)
		self.assertFalse(self.f6)
	
	
	def test_equality_with_other_objects(self):
		self.assertEqual   (self.f1, self.f1)
		self.assertNotEqual(self.f1, self.f2)
		self.assertNotEqual(self.f1, self.f3)
		self.assertNotEqual(self.f1, self.f4)
		self.assertNotEqual(self.f1, self.f5)
		self.assertNotEqual(self.f1, self.f6)
		
		self.assertNotEqual(self.f2, self.f1)
		self.assertEqual   (self.f2, self.f2)
		self.assertNotEqual(self.f2, self.f3)
		self.assertNotEqual(self.f2, self.f4)
		self.assertEqual   (self.f2, self.f5)
		self.assertNotEqual(self.f2, self.f6)
		
		self.assertNotEqual(self.f3, self.f1)
		self.assertNotEqual(self.f3, self.f2)
		self.assertEqual   (self.f3, self.f3)
		self.assertNotEqual(self.f3, self.f4)
		self.assertNotEqual(self.f3, self.f5)
		self.assertEqual   (self.f3, self.f6)
		
		self.assertNotEqual(self.f4, self.f1)
		self.assertNotEqual(self.f4, self.f2)
		self.assertNotEqual(self.f4, self.f3)
		self.assertEqual   (self.f4, self.f4)
		self.assertNotEqual(self.f4, self.f5)
		self.assertNotEqual(self.f4, self.f6)
		
		self.assertNotEqual(self.f5, self.f1)
		self.assertEqual   (self.f5, self.f2)
		self.assertNotEqual(self.f5, self.f3)
		self.assertNotEqual(self.f5, self.f4)
		self.assertEqual   (self.f5, self.f5)
		self.assertNotEqual(self.f5, self.f6)
		
		self.assertNotEqual(self.f6, self.f1)
		self.assertNotEqual(self.f6, self.f2)
		self.assertEqual   (self.f6, self.f3)
		self.assertNotEqual(self.f6, self.f4)
		self.assertNotEqual(self.f6, self.f5)
		self.assertEqual   (self.f6, self.f6)
		
	def test_equality_with_other_types(self):
		
		self.assertNotEqual(1, self.f1)
		self.assertNotEqual(self.f1, 1)
		self.assertNotEqual(self.f2, "bar")
		self.assertNotEqual("bar", self.f2)
		self.assertNotEqual(self.f1, True)
		self.assertNotEqual(self.f1, False)
		
		
class HashFunctionsTest(unittest.TestCase):
    #FIXME: maybe use subtests...
    
    # NOTE: if you want to test the behaviour of this module with/without sha3 and blake2,
    # I recommend using virtualenvs. Create a virtualenv with those modules installed,
    # create one without, and run the tests in both.
    
    # test that everything returned by supported_hash_algorithms()
    # can be used as an input to get_hash_func().
    # Also checks that certain strings can not be passed.
    def test_get_hash_func(self):
        for name in dirhash.supported_hash_algorithms():
            h = dirhash.get_hash_func(name)
        
        self.assertRaises(ValueError, dirhash.get_hash_func, 'md5')
        self.assertRaises(ValueError, dirhash.get_hash_func, 'MD5')
        self.assertRaises(ValueError, dirhash.get_hash_func, 'sha1')
        self.assertRaises(ValueError, dirhash.get_hash_func, 'SHA1')
        self.assertRaises(ValueError, dirhash.get_hash_func, 'sha')
        self.assertRaises(ValueError, dirhash.get_hash_func, 'SHA')
        self.assertRaises(ValueError, dirhash.get_hash_func, 'abcdef')
    
    # The tests for the hash functions are mainly there to make
    # sure that if something is contained in supported_hash_algorithms(),
    # then get_hash_func actually returns a usable hash object.
    def test_sha224(self):
        for name in ['sha224', 'SHA224']:
            
            # empty string
            h = dirhash.get_hash_func(name)
            self.assertEqual(h.hexdigest(), 'd14a028c2a3a2bc9476102bb288234c415a2b01f828ea62ac5b3e42f')
            
            # a-z
            h = dirhash.get_hash_func(name)
            h.update(b"abcdefghijklmnopqrstuvwxyz")
            self.assertEqual(h.hexdigest(), '45a5f72c39c5cff2522eb3429799e49e5f44b356ef926bcf390dccc2')
            
    def test_sha256(self):
        for name in ['sha256', 'SHA256']:
            
            # empty string
            h = dirhash.get_hash_func(name)
            self.assertEqual(h.hexdigest(), 'e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855')
            
            # a-z
            h = dirhash.get_hash_func(name)
            h.update(b"abcdefghijklmnopqrstuvwxyz")
            self.assertEqual(h.hexdigest(), '71c480df93d6ae2f1efad1447c66c9525e316218cf51fc8d9ed832f2daf18b73')
     
    def test_sha384(self):
        for name in ['sha384', 'SHA384']:
            
            # empty string
            h = dirhash.get_hash_func(name)
            self.assertEqual(
                h.hexdigest(),
                '38b060a751ac96384cd9327eb1b1e36a21fdb71114be07434c0cc7bf63f6e1da274edebfe76f65fbd51ad2f14898b95b'
            )
            
            # a-z
            h = dirhash.get_hash_func(name)
            h.update(b"abcdefghijklmnopqrstuvwxyz")
            self.assertEqual(
                h.hexdigest(),
                'feb67349df3db6f5924815d6c3dc133f091809213731fe5c7b5f4999e463479ff2877f5f2936fa63bb43784b12f3ebb4'
            )
    
    def test_sha512(self):
        for name in ['sha512', 'SHA512']:
            
            # empty string
            h = dirhash.get_hash_func(name)
            self.assertEqual(
                h.hexdigest(),
                'cf83e1357eefb8bdf1542850d66d8007d620e4050b5715dc83f4a921d36ce9ce' \
                '47d0d13c5d85f2b0ff8318d2877eec2f63b931bd47417a81a538327af927da3e'
            )
            
            # a-z
            h = dirhash.get_hash_func(name)
            h.update(b"abcdefghijklmnopqrstuvwxyz")
            self.assertEqual(
                h.hexdigest(),
                '4dbff86cc2ca1bae1e16468a05cb9881c97f1753bce3619034898faa1aabe429' \
                '955a1bf8ec483d7421fe3c1646613a59ed5441fb0f321389f77f48a879c7b1f1'
            )
    
    @unittest.skipIf(
        'sha3_224' not in dirhash.supported_hash_algorithms(),
        "sha3_224 not supported"
    )
    def test_sha3_224(self):
        for name in ['sha3_224']:
            
            # empty string
            h = dirhash.get_hash_func(name)
            self.assertEqual(
                h.hexdigest(),
                '6b4e03423667dbb73b6e15454f0eb1abd4597f9a1b078e3f5b5a6bc7'
            )
            
            # a-z
            h = dirhash.get_hash_func(name)
            h.update("abcdefghijklmnopqrstuvwxyz")
            self.assertEqual(
                h.hexdigest(),
                '5cdeca81e123f87cad96b9cba999f16f6d41549608d4e0f4681b8239'
            )
    
    @unittest.skipIf(
        'sha3_256' not in dirhash.supported_hash_algorithms(),
        "sha3_256 not supported"
    )
    def test_sha3_256(self):
        for name in ['sha3_256']:
            
            # empty string
            h = dirhash.get_hash_func(name)
            self.assertEqual(
                h.hexdigest(),
                'a7ffc6f8bf1ed76651c14756a061d662f580ff4de43b49fa82d80a4b80f8434a'
            )
            
            # a-z
            h = dirhash.get_hash_func(name)
            h.update("abcdefghijklmnopqrstuvwxyz")
            self.assertEqual(
                h.hexdigest(),
                '7cab2dc765e21b241dbc1c255ce620b29f527c6d5e7f5f843e56288f0d707521'
            )
    
    @unittest.skipIf(
        'sha3_384' not in dirhash.supported_hash_algorithms(),
        "sha3_384 not supported"
    )
    def test_sha3_384(self):
        for name in ['sha3_384']:
            
            # empty string
            h = dirhash.get_hash_func(name)
            self.assertEqual(
                h.hexdigest(),
                '0c63a75b845e4f7d01107d852e4c2485c51a50aaaa94fc61995e71bbee983a2ac3713831264adb47fb6bd1e058d5f004'
            )
            
            # a-z
            h = dirhash.get_hash_func(name)
            h.update("abcdefghijklmnopqrstuvwxyz")
            self.assertEqual(
                h.hexdigest(),
                'fed399d2217aaf4c717ad0c5102c15589e1c990cc2b9a5029056a7f7485888d6ab65db2370077a5cadb53fc9280d278f'
            )
    
    @unittest.skipIf(
        'sha3_512' not in dirhash.supported_hash_algorithms(),
        "sha3_512 not supported"
    )
    def test_sha3_512(self):
        for name in ['sha3_512']:
            
            # empty string
            h = dirhash.get_hash_func(name)
            self.assertEqual(
                h.hexdigest(),
                'a69f73cca23a9ac5c8b567dc185a756e97c982164fe25859e0d1dcc1475c80a6' \
                '15b2123af1f5f94c11e3e9402c3ac558f500199d95b6d3e301758586281dcd26'
            )
            
            # a-z
            h = dirhash.get_hash_func(name)
            h.update("abcdefghijklmnopqrstuvwxyz")
            self.assertEqual(
                h.hexdigest(),
                'af328d17fa28753a3c9f5cb72e376b90440b96f0289e5703b729324a975ab384' \
                'eda565fc92aaded143669900d761861687acdc0a5ffa358bd0571aaad80aca68'
            )
    
    @unittest.skipIf(
        'blake2b' not in dirhash.supported_hash_algorithms(),
        "blake2b not supported"
    )
    def test_blake2b(self):
        if 'blake2b' not in dirhash.supported_hash_algorithms():
            return
        
        # else
        for name in ['blake2b']:
            
            # empty string
            h = dirhash.get_hash_func(name)
            self.assertEqual(
                h.hexdigest(),
                '786a02f742015903c6c6fd852552d272912f4740e15847618a86e217f71f5419' \
                'd25e1031afee585313896444934eb04b903a685b1448b755d56f701afe9be2ce'
            )
            
            # a-z
            h = dirhash.get_hash_func(name)
            h.update("abcdefghijklmnopqrstuvwxyz")
            self.assertEqual(
                h.hexdigest(),
                'c68ede143e416eb7b4aaae0d8e48e55dd529eafed10b1df1a61416953a2b0a56' \
                '66c761e7d412e6709e31ffe221b7a7a73908cb95a4d120b8b090a87d1fbedb4c'
            )
    
    @unittest.skipIf(
        'blake2s' not in dirhash.supported_hash_algorithms(),
        "blake2s not supported"
    )
    def test_blake2s(self):
        if 'blake2s' not in dirhash.supported_hash_algorithms():
            return
        
        # else
        for name in ['blake2s']:
            
            # empty string
            h = dirhash.get_hash_func(name)
            self.assertEqual(
                h.hexdigest(),
                '69217a3079908094e11121d042354a7c1f55b6482ca1a51e1b250dfd1ed0eef9'
            )
            
            # a-z
            h = dirhash.get_hash_func(name)
            h.update("abcdefghijklmnopqrstuvwxyz")
            self.assertEqual(
                h.hexdigest(),
                'bdf88eb1f86a0cdf0e840ba88fa118508369df186c7355b4b16cf79fa2710a12'
            )
    
    
if __name__ == '__main__':
    unittest.main()
