# Hekate.io

Java library for cluster communications and computing.

[![Build Status](https://travis-ci.org/hekate-io/hekate.svg?branch=master)](https://travis-ci.org/hekate-io/hekate)
[![codecov](https://codecov.io/gh/hekate-io/hekate/branch/master/graph/badge.svg)](https://codecov.io/gh/hekate-io/hekate)
[![Javadocs](http://javadoc.io/badge/io.hekate/hekate-all.svg)](http://javadoc.io/doc/io.hekate/hekate-all)

## License
Open source [Apache License v2.0](http://www.apache.org/licenses/)  

## Features

- **Cluster**
    - Gossip-based decentralized cluster membership
    - Pluggable bootstrap (aka seed nodes discovery)
        - Multicast
        - JDBC
        - Shared File System
        - Clouds (based on [Apache JClouds](http://jclouds.apache.org))
            - Amazon EC2/S3
            - Azure Blob Storage
            - Google Cloud Storage
            - etc
        - [ZooKeeper](https://zookeeper.apache.org) (_planned_) 
        - [Etcd](https://github.com/coreos/etcd) (_planned_)
    - Cluster event listeners    
    - User-defined properties and roles of cluster nodes
    - Cluster views and node filtering API
    
- **Messaging**
    - Asynchronous ([Netty](http://netty.io)-based)
    - Cluster-aware load balancing and routing
    - SSL/TLS encryption of socket communications (optional)
    - Custom failover policies
    - Pluggable serialization
        - [Kryo](https://github.com/EsotericSoftware/kryo)
        - [FST](https://github.com/RuedigerMoeller/fast-serialization)
        - JDK serialization
        - Manual serialization

- **Distributed Closures and Tasks Execution**
    - Execute Runnable/Callable tasks on the cluster
    - Split/Aggregate for parallel processing of large tasks
    - Pluggable load balancing and routing
    
- **Cluster Leader Election (aka cluster singleton)**
    - Decentralized leader election
    - Followers are aware of the current leader
    - Leader can dynamically yield leadership

- **Distributed Locks**
    - Synchronous and asynchronous reentrant locks
    - Decentralized lock management
    - Configurable lock groups (aka Lock Regions)

- **Distributed Metrics**
    - Custom user-defined metrics (Counters and Probes)
    - Cluster-wide (nodes can see metrics of other nodes)
    - Recording
        - [StatsD](https://github.com/etsy/statsd)
        - [InfluxDB](https://www.influxdata.com/time-series-platform/influxdb/)
        - [CloudWatch](https://aws.amazon.com/cloudwatch/) (_planned_)
                
- **Spring Framework/Boot Support (optional)**
    - Spring-compliant beans
    - [Spring XML Schema](http://docs.spring.io/spring/docs/current/spring-framework-reference/html/xsd-configuration.html) 
      to simplify configuration
    - [Spring Boot](https://projects.spring.io/spring-boot) auto-configurations and helper annotations

- **Synchronous and Asynchronous RPC (_planned_)**

- **Raft-based Replicated State Machines (_planned_)**


## Documentation

For now, the most detailed documentation is provided as part of [javadocs](http://javadoc.io/doc/io.hekate/hekate-all/). 
Complete reference guide is coming soon.

## Maven artifacts

 * For projects based on **Spring Boot**:
```
<dependency>
    <groupId>io.hekate</groupId>
    <artifactId>hekate-spring-boot</artifactId>
    <version>1.0.1</version>
</dependency>
```

 * For projects based on **Spring Framework**:
```
<dependency>
    <groupId>io.hekate</groupId>
    <artifactId>hekate-spring</artifactId>
    <version>1.0.1</version>
</dependency>
```

 * For standalone applications:
```
<dependency>
    <groupId>io.hekate</groupId>
    <artifactId>hekate-core</artifactId>
    <version>1.0.1</version>
</dependency>
```

 * Other artifacts:
    - [hekate-codec-kryo](hekate-codec-kryo/README.md) - Integration with [Kryo](https://github.com/EsotericSoftware/kryo) for data 
      serialization.
    - [hekate-codec-fst](hekate-codec-fst/README.md) - Integration with [FST](http://ruedigermoeller.github.io/fast-serialization/) for data 
      serialization.
    - [hekate-jclouds-core](hekate-jclouds-core/README.md) - Base integration with [Apache JClouds](http://jclouds.apache.org) for cloud 
      environments.
    - [hekate-jclouds-aws](hekate-jclouds-aws/README.md) - Integration with [Amazon EC2](https://aws.amazon.com) cloud environment.
    - [hekate-metrics-influxdb](hekate-metrics-influxdb/README.md) - Metrics publishing to [InfluxDB](https://www.influxdata.com) 
      time-series data storage.
    - [hekate-metrics-statsd](hekate-metrics-statsd/README.md) - Metrics publishing to [StatsD](https://github.com/etsy/statsd) statistics 
      aggregation daemon. 

## How to build

### Software requirements:

 - Latest stable [Oracle JDK 8](http://www.oracle.com/technetwork/java/) or [Open JDK 8](http://openjdk.java.net/)
 - Latest stable [Apache Maven](http://maven.apache.org/) (3.3+)
 - Latest stable [Docker](https://www.docker.com) (required for tests only)


### Building project (no tests):

 - cd to the project's root folder
 - run `./mvnw clean package -DskipTests=true`
 
### Building project (with tests):
 
  - cd to the project's root folder
  - make a copy of `test.properties` file with name `my_test.properties`
  - edit `my_test.properties` according to your environment
  - run `docker-compose up -d` to prepare Docker-based testing infrastructure
  - run `./mvnw clean package`
  
## Release notes

### v.1.0.1 (01-Jul-2017)

 - Fixed invalid links in javadocs.
 - Fixed invalid URL in `<scm>` section of pom.xml files  
 - Upgraded to Spring Boot 1.5.4.RELEASE
 - Upgraded to AWS Java SDK 1.11.158
 - Upgraded to Spring Framework 4.3.9.RELEASE

### v.1.0.0 (30-Jun-2017)

 - Initial version.  
