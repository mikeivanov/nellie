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

## TODO

   * Tests
   * Documentation
   * Code cleanup
   * A build script
   * More use cases covered
   * Other Hadoop components (HCat, etc..)
   * Performance improvements

## License

This software is covered by the [MIT](./LICENSE) license.
