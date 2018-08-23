# Hekate.io

Java Library for Cluster Discovery and Communications

[![Build Status](https://travis-ci.org/hekate-io/hekate.svg?branch=master)](https://travis-ci.org/hekate-io/hekate)
[![codecov](https://codecov.io/gh/hekate-io/hekate/branch/master/graph/badge.svg)](https://codecov.io/gh/hekate-io/hekate)
[![Maven Central](https://maven-badges.herokuapp.com/maven-central/io.hekate/hekate-all/badge.svg)](https://search.maven.org/search?q=io.hekate)
[![Javadocs](http://javadoc.io/badge/io.hekate/hekate-all.svg)](http://javadoc.io/doc/io.hekate/hekate-all)

## License
Open source [Apache License v2.0](http://www.apache.org/licenses/)  

## Features

- **Cluster**
    - Gossip-based Decentralized Cluster Membership
    - Pluggable Bootstrapping (aka Seed Node Discovery)
        - Multicast
        - Shared Database (JDBC-based)
        - Shared File System
        - Clouds (based on [Apache JClouds](https://jclouds.apache.org))
            - Amazon EC2 and S3
            - Google Cloud Storage and Compute Engine
            - Azure Blob Storage
            - etc
        - [Kubernetes](https://kubernetes.io)    
        - [ZooKeeper](https://zookeeper.apache.org)
        - [Etcd](https://github.com/coreos/etcd) (_planned_)
    - Cluster Event Listeners    
    - User-defined Properties and Roles of Cluster Nodes
    - Cluster Views and Node Filtering API
    
- **Messaging**
    - Synchronous and Asynchronous APIs (backed by [Netty](https://netty.io))
    - Cluster-aware Load Balancing and Routing
    - SSL/TLS Encryption of Network Communications (optional)
    - Back Pressure Policies
    - Pluggable Serialization
        - [Kryo](https://github.com/EsotericSoftware/kryo)
        - [FST](https://github.com/RuedigerMoeller/fast-serialization)
        - JDK Serialization
        - Manual Serialization
        
- **Remote Procedure Calls (RPC)**
    - Type-safe Invocation of Remote Java objects
    - Automatic Discovery and Load Balancing
    - Synchronous and Asynchronous APIs
    - Multi-node Broadcasting and Aggregation of Results
    - Pluggable Failover and Retry Policies
    - ...and everything from the "Messaging" section above:)
    
- **Cluster Leader Election (aka Cluster Singleton)**
    - Decentralized Leader Election
    - Followers are Aware of the Current Leader
    - Leader can Dynamically Yield Leadership

- **Distributed Locks**
    - Synchronous and Asynchronous Reentrant Locks
    - Decentralized Lock Management
    - Configurable Lock Groups (aka Lock Regions)
                
- **Spring Boot/Framework Support (optional)**
    - [Spring Boot](https://spring.io/projects/spring-boot) Auto-configurations and @Annotations
    - Spring-compliant Beans
    - [Spring XML Schema](https://docs.spring.io/spring/docs/4.3.x/spring-framework-reference/html/xsd-configuration.html) 
      to Simplify Configuration

- **Metrics**
    - Internal metrics recording and publishing via [Micrometer.io](https://micrometer.io/): 

- **Raft-based Replicated State Machines (_planned_)**


## Documentation

For now, the most detailed documentation is provided as part of [javadocs](http://javadoc.io/doc/io.hekate/hekate-all/). 
Complete reference guide is coming soon.

## Code Examples

Please see [hekate-io/hekate-examples](https://github.com/hekate-io/hekate-examples) project.

## Maven artifacts

 * For projects based on **Spring Boot**:
```
<dependency>
    <groupId>io.hekate</groupId>
    <artifactId>hekate-spring-boot</artifactId>
    <version>3.0.0-SNAPSHOT</version>
</dependency>
```

 * For projects based on **Spring Framework**:
```
<dependency>
    <groupId>io.hekate</groupId>
    <artifactId>hekate-spring</artifactId>
    <version>3.0.0-SNAPSHOT</version>
</dependency>
```

 * For standalone applications:
```
<dependency>
    <groupId>io.hekate</groupId>
    <artifactId>hekate-core</artifactId>
    <version>3.0.0-SNAPSHOT</version>
</dependency>
```

 * Other artifacts:
    - **Cluster Bootstrapping** (seed node discovery)
        - [hekate-jclouds-core](hekate-jclouds-core/) - Integration with the [Apache JClouds](https://jclouds.apache.org) 
          for cloud environments.
        - [hekate-jclouds-aws](hekate-jclouds-aws/) - Extended integration with the [Amazon EC2](https://aws.amazon.com) cloud.
        - [hekate-kubernetes](hekate-kubernetes/) - Integration with the [Kubernetes](https://kubernetes.io) 
        - [hekate-zookeeper](hekate-zookeeper/) - Integration with the [Apache ZooKeeper](https://zookeeper.apache.org) 
    - **Serialization Codecs**
        - [hekate-codec-kryo](hekate-codec-kryo/README.md) - Integration with [Kryo](https://github.com/EsotericSoftware/kryo) for data 
          serialization.
        - [hekate-codec-fst](hekate-codec-fst/README.md) - Integration with [FST](https://github.com/RuedigerMoeller/fast-serialization) for 
          data serialization.
    - **Metrics**
        - [hekate-metrics-influxdb](hekate-metrics-influxdb/) - Metrics publishing to the [InfluxDB](https://www.influxdata.com) 
          (time-series data storage).
        - [hekate-metrics-statsd](hekate-metrics-statsd/) - Metrics publishing to the [StatsD](https://github.com/etsy/statsd) 
          (statistics aggregation daemon). 

## How to build

### Software requirements:

 - Latest stable [Oracle JDK 8](http://www.oracle.com/technetwork/java/) or [Open JDK 8](http://openjdk.java.net/)
 - Latest stable [Docker](https://www.docker.com) (required for tests only)


### Building (no tests):

 - `cd` to the project's root folder
 - run `./mvnw clean package -DskipTests=true`
 
### Building (with tests):
 
  - cd to the project's root folder
  - make a copy of `test.properties` file with name `my_test.properties`
  - edit `my_test.properties` according to your environment
  - run `docker-compose up -d` to prepare Docker-based testing infrastructure
  - run `./mvnw clean package`
  
## Release History

 - v.2.6.0 (22-Aug-2018) - [[release notes](https://github.com/hekate-io/hekate/releases/tag/v.2.6.0)]

 - v.2.5.0 (4-Aug-2018) - [[release notes](https://github.com/hekate-io/hekate/releases/tag/v.2.5.0)]

 - v.2.4.1 (15-Jul-2018) - [[release notes](https://github.com/hekate-io/hekate/releases/tag/v.2.4.1)]

 - v.2.4.0 (15-Jul-2018) - [[release notes](https://github.com/hekate-io/hekate/releases/tag/v.2.4.0)]

 - v.2.3.1 (18-May-2018) - [[release notes](https://github.com/hekate-io/hekate/releases/tag/v.2.3.1)]

 - v.2.3.0 (4-May-2018) - [[release notes](https://github.com/hekate-io/hekate/releases/tag/v.2.3.0)]

 - v.2.2.2 (21-Apr-2018) - [[release notes](https://github.com/hekate-io/hekate/releases/tag/v.2.2.2)]

 - v.2.2.1 (12-Apr-2018) - [[release notes](https://github.com/hekate-io/hekate/releases/tag/v.2.2.1)]

 - v.2.2.0 (11-Apr-2018) - [[release notes](https://github.com/hekate-io/hekate/releases/tag/v.2.2.0)]

 - v.2.1.0 (22-Feb-2018) - [[release notes](https://github.com/hekate-io/hekate/releases/tag/v.2.1.0)]

 - v.2.0.0 (2-Jan-2018) - [[release notes](https://github.com/hekate-io/hekate/releases/tag/v.2.0.0)]

 - v.1.0.2 (23-Sep-2017) - [[release notes](https://github.com/hekate-io/hekate/releases/tag/v.1.0.2)]

 - v.1.0.1 (01-Jul-2017) - [[release notes](https://github.com/hekate-io/hekate/releases/tag/v.1.0.1)]

 - v.1.0.0 (30-Jun-2017) - [[release notes](https://github.com/hekate-io/hekate/releases/tag/v.1.0.0)]
