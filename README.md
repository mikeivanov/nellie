Nellie
------

*Away from the circus*

Nellie is intended to be a suite of interconnected APIs and
commandline tools for accessing and manipulating data
in Hadoop clusters from outside of those clusters.

## Usage

    $ nellie ls /tmp
    755 mapred supergroup 0   2013-10-22 12:55:28 mapred/
    644 hdfs   supergroup 510 2013-11-16 15:03:28 rain.txt

    $ echo "012345" > data.dat
    $ nellie put data.dat /tmp
    $ nellie ls /tmp
    644 hdfs   supergroup 6   2013-11-15 00:18:08 data.dat
    755 mapred supergroup 0   2013-10-22 12:55:28 mapred/
    644 hdfs   supergroup 510 2013-11-16 15:03:28 rain.txt

    $ nellie fstat /tmp/data.dat
    Path:        /tmp/data.dat
    Type:        FILE
    Length:      6
    Permission:  644
    Owner:       hdfs
    Group:       supergroup
    Mod. time:   2013-11-15 00:18:08
    Access time: 2013-11-15 00:18:08
    Block size:  134217728
    Replication: 3

    $ time nellie get /tmp/data.dat
    012345
    real	0m0.234s
    user	0m0.131s
    sys	0m0.052s

## Features

   * It's FAST
   * Runnable outside of a cluster
   * Minimal runtime dependencies
   * Portability

## Promise

   * API first

## Status

Currently, Nellie is more like a proof of concept than a real thing.

## Building

### 1. Install [SBCL](http://sbcl.org)

* MacOS (using [Homebrew](http://brew.sh/)):

        brew install sbcl

* Ubuntu:

        sudo yum install sbcl

* RHEL/CentOS:

Follow [these instrucions](http://www.mikeivanov.com/post/66510551125/installing-sbcl-1-1-on-rhel-centos-systems).

### 2. Install [Quicklisp](http://www.quicklisp.org/beta/)

    curl -O http://beta.quicklisp.org/quicklisp.lisp
    sbcl --load quicklisp.lisp \
         --eval '(quicklisp-quickstart:install)' \
         --eval '(ql:add-to-init-file)' \
         --eval '(quit)' 

### 3. Install [Buildapp](http://www.xach.com/lisp/buildapp/)

    curl -O http://www.xach.com/lisp/buildapp.tgz
    tar xfz buildapp.tgz
    cd buildapp-1.5
    sudo make install

IMPORTANT: Even though it's possible, don't install buildapp using
quicklisp -- the version coming from QL is outdated and will not work.

### 4. Build Nellie

    cd nellie
    ./build.sh

This will create a file called `nellie` in the `dist/` directory.

The file is a fully self-contained executable without any external
dependencies. The file is fairly large (~80G), so it might make
sense to compress it before sending over network, etc.

Despite its size, this binary loads reasonably fast:

    $ time dist/nellie
    ....
    real	0m0.024s
    user	0m0.004s
    sys	    0m0.019s

This is so because when a binary is executed, the OS uses memory
mapping instead of loading the whole file into memory. In case of
SBCL, there is nothing like .jar or .pyc loading phase of any kind as
this file is actually a memory dump of a running SBCL virtual machine.

## TODO

   * Tests
   * Documentation
   * Code cleanup
   * A better build script
   * More use cases covered
   * Other Hadoop components (HCat, etc..)
   * Performance improvements

## License

This software is covered by the [MIT](./LICENSE) license.
