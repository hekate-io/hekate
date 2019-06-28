# Hekate.io

Java Library for Distributed Services

[![Build Status](https://travis-ci.org/hekate-io/hekate.svg?branch=master)](https://travis-ci.org/hekate-io/hekate)
[![codecov](https://codecov.io/gh/hekate-io/hekate/branch/master/graph/badge.svg)](https://codecov.io/gh/hekate-io/hekate)
[![Maven Central](https://maven-badges.herokuapp.com/maven-central/io.hekate/hekate-all/badge.svg)](https://search.maven.org/search?q=io.hekate)
[![Javadocs](http://javadoc.io/badge/io.hekate/hekate-all.svg)](http://javadoc.io/doc/io.hekate/hekate-all)

## License
Open source [Apache License v2.0](http://www.apache.org/licenses/)  

## Features

- **Distributed Service Discovery**
    - Decentralized Service Discovery (based on performance-optimized [Gossip](https://en.wikipedia.org/wiki/Gossip_protocol) protocol)
    - Easy integration into existing infrastructure
        - Clouds (based on [Apache JClouds](https://jclouds.apache.org))
            - Amazon EC2 and S3
            - Google Cloud Storage and Compute Engine
            - Azure Blob Storage
            - etc
        - [Kubernetes](https://kubernetes.io)    
        - [ZooKeeper](https://zookeeper.apache.org)
        - [Etcd](https://github.com/coreos/etcd)
        - [Etcd](https://github.com/hashicorp/consul)
        - IP Multicast
        - Shared Database (JDBC-based)
        - Shared File System
    - User-defined Service Properties and Roles
    - Cluster Event Listeners    
    - Service Topology Views and Filtering APIs
    - Health Monitoring and Split-brain Detection
    
- **Messaging**
    - Synchronous and Asynchronous Messaging (backed by [Netty](https://netty.io))
    - Cluster-aware Load Balancing and Routing
    - SSL/TLS Encryption of Network Communications (optional)
    - Back Pressure Policies
    - Error Retry Policies
    - Pluggable Serialization
        - [Kryo](https://github.com/EsotericSoftware/kryo)
        - [FST](https://github.com/RuedigerMoeller/fast-serialization)
        - [Protocol Buffers](https://developers.google.com/protocol-buffers/) (_work in progress_)
        - JDK Serialization
        - Manual Serialization
        
- **Remote Procedure Calls (RPC)**
    - Type-safe Invocation of Remote Java Services
    - Automatic Discovery and Load Balancing
    - Synchronous and Asynchronous APIs
    - Multi-node Broadcasting and Aggregation of Results
    - Back Pressure Policies
    - Error Retry Policies
    - ...and everything from the "Messaging" section above:)
    
- **Cluster-wide Singleton Service (aka Leader Election )**
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
    - Internal Metrics Recording and Publishing via [Micrometer.io](https://micrometer.io/) 

- **Raft-based Replicated State Machines (_planned_)**


## Documentation

For now, the most detailed documentation is provided as part of [javadocs](http://javadoc.io/doc/io.hekate/hekate-all/). 
Complete reference guide is coming soon.

## Code Examples

Quickstart for Standalone Java Application
```java
public class MyApplication{
    public static void main(String[] args) throws Exception {
        Hekate hekate = new HekateBootstrap()
            .withClusterName("my-cluster")
            .withNodeName("my-node")
            .join();
        
        System.out.println("Cluster topology: " + hekate.cluster().topology());
    }
}

```

Quickstart for Spring Boot Application
```java
@EnableHekate // <-- Enable Hekate integration.
@SpringBootApplication
public class MyApplication {
    @Autowired
    private Hekate hekate;

    public static void main(String[] args) {
        SpringApplication.run(MyApplication.class, args);
    }
    
    @PostConstruct
    public void start(){
        System.out.println("Cluster topology: " + hekate.cluster().topology());
    }
}
```

Please see the **[hekate-io/hekate-examples](https://github.com/hekate-io/hekate-examples)** project for more examples.


## Maven artifacts

 * For projects based on **Spring Boot**:
```xml
<dependency>
    <groupId>io.hekate</groupId>
    <artifactId>hekate-spring-boot</artifactId>
    <version>3.3.0</version>
</dependency>
```

 * For projects based on **Spring Framework**:
```xml
<dependency>
    <groupId>io.hekate</groupId>
    <artifactId>hekate-spring</artifactId>
    <version>3.3.0</version>
</dependency>
```

 * For standalone applications:
```xml
<dependency>
    <groupId>io.hekate</groupId>
    <artifactId>hekate-core</artifactId>
    <version>3.3.0</version>
</dependency>
```

 * Other artifacts:
    - **Cluster Bootstrapping** (seed node discovery)
        - [hekate-jclouds-core](hekate-jclouds-core/) - Integration with [Apache JClouds](https://jclouds.apache.org) 
          for cloud environments.
        - [hekate-jclouds-aws](hekate-jclouds-aws/) - Extended integration with [Amazon EC2](https://aws.amazon.com) cloud.
        - [hekate-kubernetes](hekate-kubernetes/) - Integration with [Kubernetes](https://kubernetes.io) 
        - [hekate-zookeeper](hekate-zookeeper/) - Integration with [Apache ZooKeeper](https://zookeeper.apache.org) 
        - [hekate-etcd](hekate-etcd/) - Integration with [Etcd](https://github.com/etcd-io/etcd) 
        - [hekate-consul](hekate-consul/) - Integration with [Consul](https://github.com/hashicorp/consul) 
    - **Serialization Codecs**
        - [hekate-codec-kryo](hekate-codec-kryo/README.md) - Integration with [Kryo](https://github.com/EsotericSoftware/kryo) for data 
          serialization.
        - [hekate-codec-fst](hekate-codec-fst/README.md) - Integration with [FST](https://github.com/RuedigerMoeller/fast-serialization) for 
          data serialization.

## How to build

### Software requirements:

 - Latest stable [Java SDK](https://adoptopenjdk.net/) (8+)
 - Latest stable [Docker](https://www.docker.com) (required for tests only)


### Build (no tests):

 - `cd` to the project's root folder
 - run `./mvnw clean package -DskipTests=true`
 
### Build (with tests):
 
  - cd to the project's root folder
  - make a copy of `test.properties` file with name `my_test.properties`
  - edit `my_test.properties` according to your environment
  - run `docker-compose up -d` to prepare Docker-based testing infrastructure
  - run `./mvnw clean package`
  
## Release History

 - v.3.3.0 (19-Apr-2019) - [[release notes](https://github.com/hekate-io/hekate/releases/tag/v.3.3.0)]
 
 - v.3.2.0 (5-Apr-2019) - [[release notes](https://github.com/hekate-io/hekate/releases/tag/v.3.2.0)]
 
 - v.3.1.0 (28-Mar-2019) - [[release notes](https://github.com/hekate-io/hekate/releases/tag/v.3.1.0)]
 
 - v.3.0.0 (19-Mar-2019) - [[release notes](https://github.com/hekate-io/hekate/releases/tag/v.3.0.0)]
 
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
