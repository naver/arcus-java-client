## arcus-java-client: Arcus Java Client [![Build Status](https://travis-ci.org/naver/arcus-java-client.svg?branch=develop)](https://travis-ci.org/naver/arcus-java-client)

This is a fork of [spymemcached][spymemcached] with the following modifications
to support Arcus memcached cloud.

- Collection data types
	- List: A doubly-linked list.
	- Set: An unordered set of unique data.
	- B+Tree: A B+Tree structure similar to sorted map.
- ZooKeeper based clustering

[spymemcached]: https://code.google.com/p/spymemcached/ "spymemcached"

## Getting Started

The Maven artifact for arcus java client is in the central Maven repository.
To use it, add the following dependency to your pom.xml.

```xml
<dependencies>
	<dependency>
		<groupId>com.navercorp.arcus</groupId>
		<artifactId>arcus-java-client</artifactId>
		<version>1.11.2</version>
	</dependency>
</dependencies>
```

- [Getting Started Guide (in Korean)][getting-started-guide]

[getting-started-guide]: docs/arcus-java-client-getting-started.md "guide"

## Building

To build your own library, simply run the following maven command:

```
$ mvn clean install

# Test cases may not run properly if you do not already have memcached
# and ZooKeeper installed on the local machine.  To skip tests, use skipTests.

$ mvn clean install -DskipTests=true
```

## Running Test Cases

Before running test cases, make sure to set up a local ZooKeeper and run
an Arcus memcached instance.  Several Arcus specific test cases assume that
there is an Arcus instance running, along with ZooKeeper.

First, make a simple ZooKeeper configuration file.  By default, tests assume
ZooKeeper is running at localhost:2181.
```
$ cat test-zk.conf
# The number of milliseconds of each tick
tickTime=2000
# The number of ticks that the initial 
# synchronization phase can take
initLimit=10
# The number of ticks that can pass between 
# sending a request and getting an acknowledgement
syncLimit=5
# the directory where the snapshot is stored.
dataDir=/home1/openarcus/zookeeper_data
# the port at which the clients will connect
clientPort=2181
maxClientCnxns=200
```

Second, create znodes for one memcached instance running at localhost:11211.
ZooKeeper comes with a command line tool.  The following script uses it to
set up the directory structure.
```
$ cat setup-test-zk.bash

ZK_CLI="./zookeeper/bin/zkCli.sh"
ZK_ADDR="-server localhost:2181"

$ZK_CLI $ZK_ADDR create /arcus 0
$ZK_CLI $ZK_ADDR create /arcus/cache_list 0
$ZK_CLI $ZK_ADDR create /arcus/cache_list/test 0
$ZK_CLI $ZK_ADDR create /arcus/client_list 0
$ZK_CLI $ZK_ADDR create /arcus/client_list/test 0
$ZK_CLI $ZK_ADDR create /arcus/cache_server_mapping 0
$ZK_CLI $ZK_ADDR create /arcus/cache_server_log 0
$ZK_CLI $ZK_ADDR create /arcus/cache_server_mapping/127.0.0.1:11211 0
$ZK_CLI $ZK_ADDR create /arcus/cache_server_mapping/127.0.0.1:11211/test 0
$ZK_CLI $ZK_ADDR create /arcus/cache_server_mapping/127.0.0.1:11212 0
$ZK_CLI $ZK_ADDR create /arcus/cache_server_mapping/127.0.0.1:11212/test 0
```

Now start the ZooKeeper instance using the configuration above.
```
$ ZOOCFGDIR=$PWD ./zookeeper/bin/zkServer.sh start test-zk.conf
```

And, start the memcached instance.
```
$ /home1/openarcus/bin/memcached -E /home1/openarcus/lib/default_engine.so -p 11211 -z localhost:2181
```

Finally, run test cases.
```
$ mvn test
[...]
Results :

Tests run: 722, Failures: 0, Errors: 0, Skipped: 8

[INFO] ------------------------------------------------------------------------
[INFO] BUILD SUCCESS
[INFO] ------------------------------------------------------------------------
[INFO] Total time: 3:17.308s
[INFO] Finished at: Thu Mar 06 13:42:58 KST 2014
[INFO] Final Memory: 9M/722M
[INFO] ------------------------------------------------------------------------
```

## API Documentation

Please refer to [Arcus Java Client User Guide](docs/arcus-java-client-user-guide.md)
for the detailed usage of Arcus java client.

## Issues

If you find a bug, please report it via the GitHub issues page.

https://github.com/naver/arcus-java-client/issues

## Arcus Contributors

In addition to those who had contributed to the original libmemcached, the
following people at NAVER have contributed to arcus-java-client.

Chisu Yu (netspider) <chisu.yu@navercorp.com>; <puseori9th@gmail.com>  
Hoonmin Kim (harebox) <hoonmin.kim@navercorp.com>; <harebox@gmail.com>  
YeaSol Kim (ngleader) <sol.k@navercorp.com>; <ngleader@gmail.com>  
SeongHwa Ahn <getconnected@sk.com>; <ash@nhn.com>  
HyongYoub Kim <hyongyoub.kim@navercorp.com>  

## License

Licensed under the Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0

## Patents

Arcus has patents on b+tree smget operation.
Refer to PATENTS file in this directory to get the patent information.

Under the Apache License 2.0, a perpetual, worldwide, non-exclusive,
no-charge, royalty-free, irrevocable patent license is granted to any user for any usage.
You can see the specifics on the grant of patent license in LICENSE file in this directory.
