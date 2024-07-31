# 처음 사용자용 가이드

이 문서는 ARCUS를 처음 접하는 자바 개발자를 위해 작성되었습니다.
[Apache Maven][maven]의 개념과 기본 사용법을 알고 있다고 가정하고 있으며,
자세한 설명을 하기 보다는 Copy&Paste를 통해 ARCUS를 사용해볼 수 있는 내용으로 되어 있습니다.

[maven]: http://maven.apache.org/ "Apache Maven"

## ARCUS

ARCUS는 오픈소스 key-value 캐시 서버인 memcached를 기반으로 부분적으로 fault-tolerant한 메모리 기반의 캐시 클라우드 입니다.
* memcached : 구글, 페이스북 등에서 대규모로 사용하고 있는 메모리 캐시 서버입니다.
* 캐시 : 자주 사용되는 데이터를 비교적 고속의 저장소에 넣어둠으로써, 느린 저장소로의 요청을 줄이고 보다 빠른 응답성을 기대할 수 있게 하는 서비스입니다.
* 메모리 기반 : ARCUS는 데이터를 메모리에만 저장합니다. 따라서 모든 데이터는 휘발성이며 언제든지 삭제될 수 있습니다.
* 클라우드 : 각 서비스는 필요에 따라 전용 캐시 클러스터를 구성할 수 있으며 동적으로 캐시 서버를 추가하거나 삭제할 수 있습니다. (단, 일부 데이터는 유실됩니다)
* fault-tolerant : ARCUS는 일부 또는 전체 캐시 서버의 이상 상태를 감지하여 적절한 조치를 취합니다.

또한 ARCUS는 key-value 형태의 데이터뿐만 아니라 List, Set, Map, B+Tree 등의 자료구조를 저장할 수 있는 기능을 제공합니다.

## 미리 알아두기

- 키(key)
    - ARCUS의 key는 prefix와 subkey로 구성되며, prefix와 subkey는 콜론(:)으로 구분됩니다. (예) *users:user_12345*
    - ARCUS는 prefix를 기준으로 별도의 통계를 수집합니다. prefix 개수의 제한은 없으나 통계 수집을 하는 경우에는 너무 많지 않는 수준(5~10개)으로 생성하시는 것을 권합니다.
    - 키는 prefix, subkey를 포함하여 4000자를 넘을 수 없습니다. 따라서 응용에서 키 길이를 제한하셔야 합니다.
- 값(value)
    - 하나의 키에 대한 값은 바이트 스트림 형태로 최대 1MB 까지 저장될 수 있습니다.
    - 자바 객체를 저장하는 경우, 해당 객체는 반드시 Serializable 인터페이스를 구현해야 합니다.
* ARCUS 접속 정보
    - ARCUS admin: ZooKeeper 서버 주소로서 캐시 서버들의 IP와 PORT 정보를 조회하고 변경이 있을 때 클라이언트에게 알려주는 역할을 합니다.
    - ARCUS service code: 사용자 또는 서비스에게 할당된 캐시 서버들을 구분짓는 코드값입니다.

## Hello, ARCUS!

기본적인 key-value 캐시 요청을 수행해보도록 하겠습니다. 아커스 서버가 구성되어 있다고 가정합니다.
우선 다음과 같이 비어 있는 자바 프로젝트를 생성합니다.

```bash
$ mvn archetype:generate -DgroupId=com.navercorp.arcus -DartifactId=arcus-quick-start -DinteractiveMode=false
$ cd arcus-quick-start
$ mvn eclipse:eclipse // 이클립스 IDE를 사용하는 경우 실행하여 이클립스 프로젝트를 생성하여 활용합니다.
```

### pom.xml

프로젝트가 생성되면 pom.xml에서 ARCUS 클라이언트를 참조하도록 변경합니다.

```xml
<project xmlns="http://maven.apache.org/POM/4.0.0" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
    xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 http://maven.apache.org/xsd/maven-4.0.0.xsd">
    <modelVersion>4.0.0</modelVersion>

    <groupId>com.navercorp.arcus</groupId>
    <artifactId>arcus-quick-start</artifactId>
    <version>1.0-SNAPSHOT</version>
    <packaging>jar</packaging>

    <name>arcus-quick-start</name>
    <url>http://maven.apache.org</url>

    <properties>
        <project.build.sourceEncoding>UTF-8</project.build.sourceEncoding>
    </properties>

    <dependencies>
        <!-- 편의상 JUnit 버전을 4.x로 변경합니다. -->
        <dependency>
            <groupId>junit</groupId>
            <artifactId>junit</artifactId>
            <version>4.4</version>
            <scope>test</scope>
        </dependency>

        <!-- ARCUS 클라이언트 의존성을 추가합니다. -->
        <dependency>
            <groupId>com.navercorp.arcus</groupId>
            <artifactId>arcus-java-client</artifactId>
            <version>1.14.0</version>
        </dependency>
        
        <!-- 로거 의존성을 추가합니다. -->
        <dependency>
            <groupId>org.apache.logging.log4j</groupId>
            <artifactId>log4j-core</artifactId>
            <version>2.8.2</version>
        </dependency>
        <dependency>
            <groupId>org.apache.logging.log4j</groupId>
            <artifactId>log4j-api</artifactId>
            <version>2.8.2</version>
        </dependency>
        <dependency>
            <groupId>org.apache.logging.log4j</groupId>
            <artifactId>log4j-slf4j-impl</artifactId>
            <version>2.8.2</version>
        </dependency>
    </dependencies>
</project>
```

### HelloArcus.java

이제 ARCUS와 통신하는 클래스를 생성해봅시다.
시나리오는 다음과 같습니다.
- HelloArcus.sayHello(): Arcus 캐시 서버에 "Hello, Arcus!" 값을 저장합니다.
- HelloArcus.listenHello(): Arcus 캐시 서버에 저장된 "Hello, Arcus!" 값을 읽어옵니다.

```java
// HelloArcusTest.java
package com.navercorp.arcus;

import org.junit.Assert;

import org.junit.Before;
import org.junit.Test;

public class HelloArcusTest {

    HelloArcus helloArcus = new HelloArcus("127.0.0.1:2181", "test");
    
    @Before
    public void sayHello() {
        helloArcus.sayHello();
    }
    
    @Test
    public void listenHello() {
        Assert.assertEquals("Hello, Arcus!", helloArcus.listenHello());
    }
    
}
```

```java
// HelloArcus.java
package com.navercorp.arcus;

import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import net.spy.memcached.ArcusClient;
import net.spy.memcached.ConnectionFactoryBuilder;

public class HelloArcus {

    private String arcusAdmin;
    private String serviceCode;
    private ArcusClient arcusClient;

    public HelloArcus(String arcusAdmin, String serviceCode) {
        this.arcusAdmin = arcusAdmin;
        this.serviceCode = serviceCode;
        
        // log4j logger를 사용하도록 설정합니다.
        // 코드에 직접 추가하지 않고 아래의 JVM 환경변수를 사용해도 됩니다.
        //   -Dnet.spy.log.LoggerImpl=net.spy.memcached.compat.log.Log4JLogger
        System.setProperty("net.spy.log.LoggerImpl", "net.spy.memcached.compat.log.Log4JLogger");

        // Arcus 클라이언트 객체를 생성합니다.
        // - arcusAdmin : Arcus 캐시 서버들의 그룹을 관리하는 admin 서버(ZooKeeper)의 주소입니다.
        // - serviceCode : 사용자에게 할당된 Arcus 캐시 서버들의 집합에 대한 코드값입니다. 
        // - connectionFactoryBuilder : 클라이언트 생성 옵션을 지정할 수 있습니다.
        //
        // 정리하면 arcusAdmin과 serviceCode의 조합을 통해 유일한 캐시 서버들의 집합을 얻어 연결할 수 있는 것입니다.
        this.arcusClient = ArcusClient.createArcusClient(arcusAdmin, serviceCode, new ConnectionFactoryBuilder());
    }

    public boolean sayHello() {
        Future<Boolean> future = null;
        boolean setSuccess = false;

        // Arcus의 "test:hello" 키에 "Hello, Arcus!"라는 값을 저장합니다.
        // 그리고 Arcus의 거의 모든 API는 Future를 리턴하도록 되어 있으므로
        // 비동기 처리에 특화된 서버가 아니라면 반드시 명시적으로 future.get()을 수행하여
        // 반환되는 응답을 기다려야 합니다.
        future = this.arcusClient.set("test:hello", 600, "Hello, Arcus!");
        
        try {
            setSuccess = future.get(700L, TimeUnit.MILLISECONDS);
        } catch (Exception e) {
            if (future != null) future.cancel(true);
            e.printStackTrace();
        }
        
        return setSuccess;
    }
    
    public String listenHello() {
        Future<Object> future = null;
        String result = "Not OK.";
        
        // Arcus의 "test:hello" 키의 값을 조회합니다.
        // Arcus에서는 가능한 모든 명령에 명시적으로 timeout 값을 지정하도록 가이드 하고 있으며
        // 사용자는 set을 제외한 모든 요청에 async로 시작하는 API를 사용하셔야 합니다.
        future = this.arcusClient.asyncGet("test:hello");
        
        try {
            result = (String)future.get(700L, TimeUnit.MILLISECONDS);
        } catch (Exception e) {
            if (future != null) future.cancel(true);
            e.printStackTrace();
        }
        
        return result;
    }

}
```

### src/test/resources/log4j2.xml
```xml
<?xml version="1.0" encoding="UTF-8"?>
<Configuration>
    <Appenders>
        <Console name="console" target="SYSTEM_OUT">
            <PatternLayout pattern="%d{yyyy-MM-dd HH:mm:ss} [%-5p](%-35c{1}:%-3L) %m%n" />
        </Console>
    </Appenders>
    <Loggers>
        <Root level="WARN">
            <AppenderRef ref="console" />
        </Root>
        <Logger name="net.spy.memcached.StatisticsHandler" level="INFO" additivity="false">
            <AppenderRef ref="console" />
        </Logger>
        <Logger name="net.spy.memcached.ArcusClient" level="INFO" additivity="false">
            <AppenderRef ref="console" />
        </Logger>
        <Logger name="net.spy.memcached.protocol.ascii.BTreeGetBulkOperationImpl" level="DEBUG" additivity="false">
            <AppenderRef ref="console" />
        </Logger>
        <Logger name="net.spy.memcached.protocol.ascii.CollectionInsertOperationImpl" level="DEBUG" additivity="false">
            <AppenderRef ref="console" />
        </Logger>
        <Logger name="net.spy.memcached.protocol.ascii.CollectionPipedInsertOperationImpl" level="DEBUG" additivity="false">
            <AppenderRef ref="console" />
        </Logger>
        <Logger name="net.spy.memcached.protocol.ascii.CollectionGetOperationImpl" level="DEBUG" additivity="false">
            <AppenderRef ref="console" />
        </Logger>
        <Logger name="net.spy.memcached.protocol.ascii.BTreeSortMergeGetOperationImpl" level="DEBUG" additivity="false">
            <AppenderRef ref="console" />
        </Logger>
        <Logger name="net.spy.memcached.protocol.ascii.CollectionDeleteOperationImpl" level="DEBUG" additivity="false">
            <AppenderRef ref="console" />
        </Logger>
        <Logger name="net.spy.memcached.protocol.ascii.CollectionUpdateOperationImpl" level="DEBUG" additivity="false">
            <AppenderRef ref="console" />
        </Logger>
        <Logger name="net.spy.memcached.protocol.ascii.CollectionPipedExistOperationImpl" level="DEBUG" additivity="false">
            <AppenderRef ref="console" />
        </Logger>
        <Logger name="net.spy.memcached.protocol.ascii.SetAttrOperationImpl" level="DEBUG" additivity="false">
            <AppenderRef ref="console" />
        </Logger>
        <Logger name="net.spy.memcached.protocol.ascii.StoreOperationImpl" level="DEBUG" additivity="false">
            <AppenderRef ref="console" />
        </Logger>
        <Logger name="net.spy.memcached.protocol.ascii.CollectionCountOperationImpl" level="DEBUG" additivity="false">
            <AppenderRef ref="console" />
        </Logger>
    </Loggers>
</Configuration>
```

### 테스트

위 예제는 127.0.0.1:2181 에 ZooKeeper 가 작동하고 있고 memcached 서버가 구동하고 있다고 가정합니다.
아직 준비가 안 되어 있다면, 다음 페이지 Running Test Cases 를 따라 준비합니다.

https://github.com/naver/arcus-java-client/blob/master/README.md

테스트가 통과하는지 확인해봅니다.

```bash
$ mvn test
...
-------------------------------------------------------
 T E S T S
-------------------------------------------------------
...
Results :

Tests run: 1, Failures: 0, Errors: 0, Skipped: 0

[INFO] ------------------------------------------------------------------------
[INFO] BUILD SUCCESS
[INFO] ------------------------------------------------------------------------
[INFO] Total time: 1.885s
[INFO] Finished at: Mon Dec 17 14:13:22 KST 2012
[INFO] Final Memory: 4M/81M
[INFO] ------------------------------------------------------------------------
```
