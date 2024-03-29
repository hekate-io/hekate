# Hekate.io

Java Library for Distributed Services

[![build](https://github.com/hekate-io/hekate/workflows/Build/badge.svg?branch=master)](https://github.com/hekate-io/hekate/actions)
[![codecov](https://codecov.io/gh/hekate-io/hekate/branch/master/graph/badge.svg)](https://codecov.io/gh/hekate-io/hekate)
[![Maven Central](https://maven-badges.herokuapp.com/maven-central/io.hekate/hekate-all/badge.svg)](https://search.maven.org/search?q=io.hekate)
[![Javadocs](http://javadoc.io/badge/io.hekate/hekate-all.svg)](http://javadoc.io/doc/io.hekate/hekate-all)

## License
Open source [Apache License v2.0](http://www.apache.org/licenses/)  

## Features

- **Distributed Service Discovery**
    - Decentralized Service Discovery (based on performance-optimized [Gossip](https://en.wikipedia.org/wiki/Gossip_protocol) protocol)
    - Easy integration into existing infrastructure
        - Clouds (based on Apache JClouds)
            - Amazon EC2 and S3
            - Google Cloud Storage and Compute Engine
            - Azure Blob Storage
            - etc
        - Kubernetes    
        - ZooKeeper
        - Etcd
        - Consul
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
        - Google Protocol Buffers (_work in progress_)
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
    - Spring Boot Auto-configurations and @Annotations
    - Spring-compliant Beans
    - Spring XML Schema

- **Metrics**
    - Internal Metrics Recording and Publishing via [Micrometer.io](https://micrometer.io/) 

- **Raft-based Replicated State Machines (_work in progress_)**


## Documentation

- [Javadocs](http://javadoc.io/doc/io.hekate/hekate-all/) (Follow ~~the White Rabbit~~ `[« start here]` marks)
- Reference Guide (coming soon...) 
- [Spring Boot Properties Reference](http://javadoc.io/doc/io.hekate/hekate-all/latest/spring-boot.properties.txt)

## Code Examples

Quickstart for **Standalone** Java Application
```java
public class MyApplication{
    public static void main(String[] args) throws Exception {
        Hekate hekate = new HekateBootstrap()
            .withNamespace("my-cluster")
            .withNodeName("my-node")
            .join();
        
        System.out.println("Cluster topology: " + hekate.cluster().topology());
    }
}

```

Quickstart for **Spring Boot** Application
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

__More Examples__: 

Please see the **[hekate-io/hekate-examples](https://github.com/hekate-io/hekate-examples)** project for more examples.


## Maven artifacts

 * For projects based on **Spring Boot**:
```xml
<dependency>
    <groupId>io.hekate</groupId>
    <artifactId>hekate-spring-boot</artifactId>
    <version>4.1.3</version>
</dependency>
```

 * For projects based on **Spring Framework**:
```xml
<dependency>
    <groupId>io.hekate</groupId>
    <artifactId>hekate-spring</artifactId>
    <version>4.1.3</version>
</dependency>
```

 * For standalone applications:
```xml
<dependency>
    <groupId>io.hekate</groupId>
    <artifactId>hekate-core</artifactId>
    <version>4.1.3</version>
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

## How to build

### Software requirements:

 - Latest stable [Java SDK](https://adoptopenjdk.net/) (8, 11, 17)
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

 - v.4.1.3 (25-Nov-2022) - [[release notes](https://github.com/hekate-io/hekate/releases/tag/v.4.1.3)]

 - v.4.1.2 (12-May-2022) - [[release notes](https://github.com/hekate-io/hekate/releases/tag/v.4.1.2)]

 - v.4.1.1 (11-May-2022) - [[release notes](https://github.com/hekate-io/hekate/releases/tag/v.4.1.1)]

 - v.4.1.0 (3-Apr-2022) - [[release notes](https://github.com/hekate-io/hekate/releases/tag/v.4.1.0)]

 - v.4.0.0 (20-Mar-2021) - [[release notes](https://github.com/hekate-io/hekate/releases/tag/v.4.0.0)]

 - v.3.11.0 (24-Oct-2020) - [[release notes](https://github.com/hekate-io/hekate/releases/tag/v.3.11.0)]
 
 - v.3.10.0 (4-Jul-2020) - [[release notes](https://github.com/hekate-io/hekate/releases/tag/v.3.10.0)]

 - v.3.9.0 (8-May-2020) - [[release notes](https://github.com/hekate-io/hekate/releases/tag/v.3.9.0)]

 - v.3.8.0 (29-Feb-2020) - [[release notes](https://github.com/hekate-io/hekate/releases/tag/v.3.8.0)]
 
 - v.3.7.0 (3-Feb-2020) - [[release notes](https://github.com/hekate-io/hekate/releases/tag/v.3.7.0)]

 - v.3.6.0 (13-Oct-2019) - [[release notes](https://github.com/hekate-io/hekate/releases/tag/v.3.6.0)]
 
 - v.3.5.0 (14-Aug-2019) - [[release notes](https://github.com/hekate-io/hekate/releases/tag/v.3.5.0)]
 
 - v.3.4.1 (4-Jul-2019) - [[release notes](https://github.com/hekate-io/hekate/releases/tag/v.3.4.1)]

 - v.3.4.0 (29-Jun-2019) - [[release notes](https://github.com/hekate-io/hekate/releases/tag/v.3.4.0)]
 
 - v.3.3.0 (19-Apr-2019) - [[release notes](https://github.com/hekate-io/hekate/releases/tag/v.3.3.0)]
 
 - v.3.2.0 (5-Apr-2019) - [[release notes](https://github.com/hekate-io/hekate/releases/tag/v.3.2.0)]
 
 - v.3.1.0 (28-Mar-2019) - [[release notes](https://github.com/hekate-io/hekate/releases/tag/v.3.1.0)]
 
 - v.3.0.0 (19-Mar-2019) - [[release notes](https://github.com/hekate-io/hekate/releases/tag/v.3.0.0)]
 
 - v.2.6.0 (22-Aug-2018) - [[release notes](https://github.com/hekate-io/hekate/releases/tag/v.2.6.0)]

 - v.2.5.0 (4-Aug-2018) - [[release notes](https://github.com/hekate-io/hekate/releases/tag/v.2.5.0)]

 - v.2.4.1 (15-Jul-2018) - [[release notes](https://github.com/hekate-io/hekate/releases/tag/v.2.4.1)]

 - v.2.4.0 (15-Jul-2018) - [[release notes](https://github.com/hekate-io/hekate/releases/tag/v.2.4.0)]

 - [...and so on](https://github.com/hekate-io/hekate/releases)
