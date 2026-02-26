# 2. ARCUS Java Client 사용법

- [ARCUS Client 기본 사용법](02-arcus-java-client.md#arcus-client-basic-usage)
- [ARCUS Client 생성, 소멸, 관리](02-arcus-java-client.md#arcus-client-lifecycle)
- [ARCUS Client 설정](02-arcus-java-client.md#arcus-client-settings)


<a id="arcus-client-basic-usage"></a>
## ARCUS Client 기본 사용법

예제를 통해 ARCUS java client 기본 사용법을 알아본다.
아래 예제는 ARCUS cache에 key가 “sample:testKey”이고 value가 “testValue”인 cache item을 저장한다.

```java
package com.navercorp.arcus.example; 

import java.util.concurrent.ExecutionException; 
import java.util.concurrent.Future; 
import java.util.concurrent.TimeUnit; 
import java.util.concurrent.TimeoutException; 
import net.spy.memcached.ArcusClient; 
import net.spy.memcached.ConnectionFactoryBuilder; 

public class HelloArcus { 

    private static final String ARCUS_ADMIN = "10.0.0.1:2181,10.0.0.2:2181,10.0.0.3:2181"; 
    private static final String SERVICE_CODE = "test"; 
    private final ArcusClient arcusClient; 

    public static void main(String[] args) { 
        HelloArcus hello = new HelloArcus(); 
        System.out.printf("hello.setTest() result=%b", hello.setTest()); 
        hello.closeArcusConnection(); 
    } 

    public HelloArcus() { 
        arcusClient = ArcusClient.createArcusClient(ARCUS_ADMIN, SERVICE_CODE, 
                new ConnectionFactoryBuilder()); // (1) 
    } 

    public boolean setTest() { 
        Future<Boolean> future = null; 
        try { 
            future = arcusClient.set("sample:testKey", 10, "testValue"); // (2) 
        } catch (IllegalStateException e) { 
            // client operation queue 문제로 요청이 등록되지 않았을 때 예외처리. 
        } 

        if (future == null) return false; 

        try { 
            return future.get(500L, TimeUnit.MILLISECONDS); // (3) 
        } catch (TimeoutException te) { // (4) 
            future.cancel(true); 
        } catch (ExecutionException re) { // (5) 
            future.cancel(true); 
        } catch (InterruptedException ie) { // (6) 
            future.cancel(true); 
        } 

        return false; 
    } 

    public void closeArcusConnection() { 
        arcusClient.shutdown(); // (7) 
    } 
} 
```

(1) ArcusClient 클래스의 객체(client 객체)를 생성한다. Client 객체는 매 요청마다 생성하지 않고
    미리 하나를 만들어 재활용하도록 한다.
    ARCUS에 접속할 때, 각종 설정을 변경하기 위해서 ConnectionFactoryBuilder를 사용하였다.

- **잘못된 SERVICE_CODE를 지정했다면 NotExistsServiceCodeException이 발생한다.**
- **SERVICE_CODE는 올바르지만 접속 가능한 cache 서버(또는 노드)가 없다면, 모든 요청은 Exception을 발생시킨다.**
  **Cache 서버가 구동되어 접속이 가능해지면, 자동으로 해당 cache 서버로 연결하여 정상 서비스하게 된다.**

(2) Client 객체의 `set` 메소드를 호출하고 그 결과를 Boolean 값을 갖는 Future 클래스의 객체(future 객체)로 받는다.

- 저장할 값으로 ""(길이가 0인 문자열)을 넣으면, ""이 그대로 저장된다. (해당 key가 삭제되지 않는다.)
- 저장할 값으로 null을 지정할 수 없다. (key 삭제 의도이면, `delete` 메소드를 사용해야 한다.)
- 저장할 값은 serializable해야 한다. (사용자 정의 클래스의 경우 Serializable 인터페이스를 구현해야 한다.)

(3) Future 객체의 값을 받아서 result에 담는다. (만약 set 작업이 실패하였을 경우에는 false가 반환된다.)

- Cache 서버에 key가 존재하지 않는다면, 다시 말해 cache miss이라면, null이 반환된다.
- 길이가 0인 문자열(“”)은 cache miss가 아니고 “”가 저장되어 있는 것이다.

(4) 지정한 시간에 결과가 넘어 오지 않거나 JVM의 과부하로 operation queue에서 처리되지 않을 경우
    TimeoutException이 발생한다.

- 예를 들어, timeout 시간을 500ms로 지정했는데 GC time이 600ms걸렸다면
  ARCUS cache 서버와 통신에 문제가 없음에도 불구하고 100ms를 초과했기 때문에 TimeoutException이 발생하게 된다.
- TimeoutException이 연속해서 n(디폴트는 10)회 이상 발생하면 클라이언트는 서버와의 연결을 끊고 재접속한다.
  여기에서 n번의 값은 ConnectionFactoryBuilder를 생성할 때 지정할 수 있다. 
- 또한, 모든 Exception이 발생한 상황에서는 future.cancel(true)를 반드시 호출해 주어야 한다.

(5) ArcusClient의 operation queue에 대기하고 있던 작업이 취소되었을 때, ExecutionException이 발생한다.
    ExecutionException은 서버 또는 네트워크 장애 시 발생한다.

(6) 다른 쓰레드에서 해당 쓰레드의 작업을 Interrupt했을 때 InterruptedException이 발생한다. (발생할 여지가 거의 없다.)

(7) 프로세스를 종료하기 전에 더 이상 ArcusClient를 사용하지 않으므로,
    반드시 `shutdown` 메소드를 호출하여 서버와의 연결을 끊어야 한다.

- Tomcat과 같은 WAS에서는 Tomcat이 shutdown이 될 때 `shutdown` 메소드가 호출되게 하면 된다.
- Spring container에서 관리되는 경우 bean 설정의 destroy-method에서 `shutdown` 메소드가 호출되도록 설정해야 한다.


<a id="arcus-client-lifecycle"></a>
## ARCUS Client 생성, 소멸, 관리

### ARCUS Client 생성

하나의 ARCUS Client 객체는 ARCUS cache cloud에 있는 모든 cache server(or cache node)와 연결을 하나씩 생성하며,
요청되는 각 cache item의 key에 대해 그 key가 mapping되는 cache server와의 연결을 이용하여 request를 보내고
response를 받는다.

ARCUS Client 객체를 생성하는 방법으로 두 가지가 있다.

- 단일 ARCUS Client 생성
- ARCUS Client Pool 생성

먼저, 단일 ARCUS Client 객체를 생성하기 위한 메소드는 아래와 같다.

```java
ArcusClient.createArcusClient(String arcusAdminAddress, String serviceCode, ConnectionFactoryBuilder cfb)
```

- arcusAdminAddress: 접근할 cache cloud를 관리하는 ARCUS zookeeper ensemble 주소
  - IP:port 리스트인 "ip1:port,ip2:port,ip3:port" 형태로 지정하거나
  - "FQDN:port" 형태로 지정할 수 있다. (zookeeper IP list에 대한 domain name을 DNS에 등록한 경우)
- serviceCode: 접속할 cache cloud의 식별자
- cfb: ARCUS client의 동작 설정을 위한 ConnectionFactoryBuilder 객체


ARCUS_ADMIN 서버에서 관리되는 SERVICE_CODE에 해당하는 cache cloud로 연결하는 하나의 ArcusClient 객체를
생성하는 예는 아래와 같다.

```java
ConnectionFactoryBuilder cfb = new ConnectionFactoryBuilder();
ArcusClient client = ArcusClient.createArcusClient(ARCUS_ADMIN, SERVICE_CODE, cfb);
```

하나의 ARCUS Client만으로는 응용의 requests를 처리하는 용량 즉, throughput에 한계가 있다.
예를 들어, 하나의 연결을 통해 하나의 request가 처리되는 시간이 1ms라 가정하면,
그 연결을 통해 최대 1000 requests/second 밖에 처리할 수 없다.
따라서, 많은 요청 처리량이 필요한 응용인 경우는 다수의 ARCUS client 객체를 생성하여야 한다.
이를 위해 ARCUS client pool 객체를 생성할 수 있으며, 아래의 메소드를 사용해 생성한다.

```java
ArcusClient.createArcusClientPool(String arcusAdminAddress, String serviceCode, ConnectionFactoryBuilder cfb, int poolSize);
```

메소드의 인자로 단일 ARCUS client 객체를 생성할 시의 인자들 외에
pool에 들어갈 arcus client 객체 수를 지정하는 poolSize 인자가 있다.
pool size가 너무 작으면 응용 요청들을 제시간에 처리할 수 없는 문제가 생기고,
너무 크면 arcus cache server로 불필요하게 많은 연결을 맺게 한다.
적절한 pool size는 "응용 서버의 peak arcus request 요청량"을 "하나의 arcus client의 처리량"으로 나누면 
얻을 수 있다. 여기서, 하나의 arcus client가 처리할 수 있는 처리량은
응용 서버가 요청하는 arcus request 유형과 응용 서버와 cache server 간의 네트웍 상태 등에 영향받을 수 있으므로,
실제 테스트를 통해 확인해 보고 pool size를 결정하길 권한다.

특정 SERVICE_CODE에 해당하는 cache cloud로 연결되는 ARCUS client 4 개를 가지는 pool을 생성하는 예는
다음과 같다.

```java
int poolSize = 4;
ConnectionFactoryBuilder cfb = new ConnectionFactoryBuilder();
ArcusClientPool pool = ArcusClient.createArcusClientPool(ARCUS_ADMIN, SERVICE_CODE, cfb, poolSize);
```

ARCUS client 객체를 정상적으로 생성하면, 아래의 로그와 같이 cache cloud와 정상 연결됨을 볼 수 있다.

```
WARN net.spy.memcached.CacheManager: All arcus connections are established.
```

ARCUS cache cloud로 정상 연결되지 않으면, 다음과 같은 로그가 보인다.
예를 들어 5대의 Cache server에 접속을 해야 하는데 이들 중 일부 서버에 접속하지 못했다면 아래 로그가 남게 된다.
접속 실패한 cache server에 대해서는 ARCUS client가 1초에 한 번씩 자동으로 재연결을 시도한다.

```
WARN net.spy.memcached.CacheManager: Some arcus connections are not established.
```

### ARCUS Client 소멸

ArcusClient 또는 ArcusClientPool를 사용하고 난 다음에는
반드시 shutdown() 메소드를 호출하여 client와 admin, cache server간의 연결을 해제시켜주어야 한다.

```java
client.shutdown();
pool.shutdown();
```

#### ARCUS Client 생명주기 관리

Arcus에 대한 매 요청마다 arcus client 객체를 생성하고 소멸시키는 것은 적절하지 못하다.
응용 서버의 구동 시에 arcus client 객체를 생성하고, 종료 시에 arcus client 객체를 소멸하면 된다.

일반적으로, 응용에서는 ArcusClient wrapper를 만들어 사용할 것을 권장한다.
이렇게 하면 ArcusClient의 생명주기를 관리하기 수월해진다.
Service code별 ArcusClient instance를 가지는 factory를 singleton으로 만들어두고
WAS가 초기화 될 때 Arcus server 와 연결을 맺도록 하자.
WAS가 shutdown될 때 ArcusClient도 함께 shutdown되도록 설정하면 가장 이상적이다.


#### Cache Server List 관리

ARCUS는 cache server list를 자동으로 관리한다. 
Cache server들 중에 일부 서버가 사용 불가능한 상태가 되면
ARCUS admin이 자동으로 상황을 인지하고 해당 서버를 cache server list에서 제거하며,
변경된 cache server list가 있음을 각 arcus client에 알림으로써
각 arcus client가 최신의 cache server list를 유지하게 한다.
반대로 사용 가능한 cache server가 추가되었을 때에도 마찬가지로,
ARCUS admin의 도움으로 ARCUS client는 최신의 cache server list를 유지하고,
cache key와 cache server와의 mapping을 갱신하게 한다.
따라서, ARCUS client를 사용할 때 cache server 대수의 변화에 대한 방어 로직은 신경 쓰지 않아도 된다.


<a id="arcus-client-settings"></a>
## ARCUS Client 설정

### Key-Value에서 데이터 압축 설정

ARCUS client는 key-value item의 데이터 압축 및 해제 기능을 가지고 있다.
즉, 일정 크기 이상의 데이터이면 그 데이터를 압축하여 cache server에 보내어 저장하고,
cache server로 부터 가져온 데이터가 압축 데이터이면, 해제하여 응용에 전달한다.

ARCUS client는 저정할 값의 크기가 16KB 이상일 경우에 압축하여 cache server에 저장하도록 되어 있다.
이러한 데이터 압축 임계값은 ConnectionFactoryBuilder의 setTranscoder메소드를 통해 설정할 수 있다.

다음은 4KB 이상의 데이터는 모두 압축하도록 설정하는 예제이다.

```java
ConnectionFactoryBuilder cfb = new ConnectionFactoryBuilder();

SerializingTranscoder trans = new SerializingTranscoder();
trans.setCharset(“UTF-8”);
trans.setCompressionThreshold(4096);

cfb.setTranscoder(trans);

ArcusClient client = ArcusClient.createArcusClient(SERVICE_CODE, cfb);
```

### Logger 설정

ARCUS client 사용 시에 default(DefaultLogger), log4j(Log4JLogger), slf4j(SLF4JLogger), jdk(SunLogger) 등 4가지 종류의 Logger를 사용할 수 있다.
사용할 logger를 지정하지 않으면 ArcusClient는 DefaultLogger를 기본으로 사용하며,
DefaultLogger는 INFO level 이상의 로그를 stderr (System.err) 로 출력한다. (변경 불가)

log4j를 사용하여 ArcusClient 로그를 관리하려면, 아래 옵션을 WAS나 자바 프로세스 옵션에 추가하여 JVM 구동시 System property를 지정한다. 

```
-Dnet.spy.log.LoggerImpl=net.spy.memcached.compat.log.Log4JLogger
```

또는, 소스 코드에서 ArcusClient / ArcusClientPool을 사용하기 전에 직접 System property를 설정하여 사용할 수 있다. (programmatic configuration)

```java
System.setProperty("net.spy.log.LoggerImpl", "net.spy.memcached.compat.log.Log4JLogger");
...
ConnectionFactoryBuilder cfb = new ConnectionFactoryBuilder();
ArcusClient client = ArcusClient.createArcusClient(SERVICE_CODE, cfb);
```

ARCUS Java client에서는 Log를 기록할 때 Class의 이름(```clazz.getName()```)을 기준으로 Logger를 구분하여 사용하며,
class의 이름과 정확히 일치하는 로거가 없다면 logger tree 상의 상위 logger 를 사용한다.

아래의 예제는 ```root``` logger 의 level을 ```WARN```으로 설정하여 WARN level 이상의 로그는 항상 기록하고, ```net.spy.memcached.protocol.ascii.CollectionUpdateOperationImpl``` class의 로그만 DEBUG level 이상의 로그를 기록하도록 한 예제이다.
```xml
<Root level="WARN">
    <AppenderRef ref="console" />
</Root>
<Logger name="net.spy.memcached.protocol.ascii.CollectionUpdateOperationImpl" additivity="false" level="DEBUG">
    <AppenderRef ref="console" />
</Logger>
```
Application을 디버깅해야 할 때 ARCUS client에서 Arcus server로 전송하는 ascii protocol 문자열이 궁금할 때가 있다. ARCUS Java Client에서 ARCUS server로 전송하는 protocol을 로그로 살펴보려면 아래와 같이 logger를 설정하면 된다.
예제에 나열된 logger를 모두 설정하면 요청(get, set 등..)별로 모든 로그가 남게 되니 필요한 요청에 해당하는 logger만 설정하면 편리하다.
Ascii Protocol에 대한 자세한 내용은 [ARCUS 서버 명령 프로토콜](https://github.com/naver/arcus-memcached/blob/master/docs/ascii-protocol/README.md) 문서를 참고하기 바란다.
```xml
<!-- collection update -->
<Logger name="net.spy.memcached.protocol.ascii.CollectionUpdateOperationImpl" level="DEBUG" additivity="false">
    <AppenderRef ref="console" />
</Logger>

<!-- collection piped exist -->
<Logger name="net.spy.memcached.protocol.ascii.CollectionPipedExistOperationImpl" level="DEBUG" additivity="false">
    <AppenderRef ref="console" />
</Logger>

<!-- set attributes -->
<Logger name="net.spy.memcached.protocol.ascii.SetAttrOperationImpl" level="DEBUG" additivity="false">
    <AppenderRef ref="console" />
</Logger>

<!-- collection insert -->
<Logger name="net.spy.memcached.protocol.ascii.CollectionInsertOperationImpl" level="DEBUG" additivity="false">
    <AppenderRef ref="console" />
</Logger>

<!-- collection get -->
<Logger name="net.spy.memcached.protocol.ascii.CollectionGetOperationImpl" level="DEBUG" additivity="false">
    <AppenderRef ref="console" />
</Logger>

<!-- collection update -->
<Logger name="net.spy.memcached.protocol.ascii.CollectionUpdateOperationImpl" level="DEBUG" additivity="false">
    <AppenderRef ref="console" />
</Logger>

<!-- collection count -->
<Logger name="net.spy.memcached.protocol.ascii.CollectionCountOperationImpl" level="DEBUG" additivity="false">
    <AppenderRef ref="console" />
</Logger>
```

기타 log4j의 자세한 설정 방법은 [log4j 설정 방법](http://logging.apache.org/log4j/2.x/manual/configuration.html)을 확인하기 바란다. 

### Log4JLogger 사용시 유의사항

log4j 1.2 이하 버전에서 보안 취약점이 존재하여, ARCUS client의 1.11.5 버전부터 Log4JLogger를 사용하려면 log4j2 라이브러리가 요구된다. 이를 위해 응용 의존성에 아래와 같이 log4j2 라이브러리를 추가한다.

```xml
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
```

만약 아래와 같은 예외가 발생되면, log4j2 라이브러리가 클래스패스에 존재하지 않은 것이다. log4j2 라이브러리가 응용 의존성에 제대로 추가가 됐는지 확인하도록 한다.

```
Warning:  net.spy.memcached.compat.log.Log4JLogger not found while initializing net.spy.compat.log.LoggerFactory
java.lang.NoClassDefFoundError: org/apache/logging/log4j/spi/ExtendedLogger
    at java.base/java.lang.Class.forName0(Native Method)
    at java.base/java.lang.Class.forName(Class.java:315)
    at net.spy.memcached.compat.log.LoggerFactory.getConstructor(LoggerFactory.java:134)
    at net.spy.memcached.compat.log.LoggerFactory.getNewInstance(LoggerFactory.java:119)
    at net.spy.memcached.compat.log.LoggerFactory.internalGetLogger(LoggerFactory.java:100)
    at net.spy.memcached.compat.log.LoggerFactory.getLogger(LoggerFactory.java:89)
    at net.spy.memcached.ArcusClient.<clinit>(ArcusClient.java:183)
    at Main.main(Main.java:10)
```

### SLF4JLogger 사용시 유의 사항

slf4j를 사용하는 경우, ARCUS client의 SLF4JLogger 클래스를 사용할 것이다. 이 클래스를 사용하려면 slf4j를 구현한 로깅 라이브러리가 응용 의존성에 추가되어야 한다. 만약 추가하지 않을 경우 아래의 예외 메시지가 발생한다.

```
SLF4J: Failed to load class "org.slf4j.impl.StaticLoggerBinder".
SLF4J: Defaulting to no-operation (NOP) logger implementation
SLF4J: See http://www.slf4j.org/codes.html#StaticLoggerBinder for further details.
```

log4j, logback과 같은 대표적인 자바의 로그 라이브러리들은 slf4j api를 구현한 구현 라이브러리를 제공하고 있다. 해당 라이브러리를 사용할 경우 아래와 같이 응용 의존성에 추가하도록 한다. 자세한 내용은 [slf4j](http://www.slf4j.org/manual.html#swapping) 문서를 참고한다. 

```xml
<!-- slf4j + log4j 사용시 -->
<dependency>
    <groupId>com.navercorp.arcus</groupId>
    <artifactId>arcus-java-client</artifactId>
    <version>${arcus-java-client.version}</version>
</dependency>
<dependency>
    <groupId>org.apache.logging.log4j</groupId>
    <artifactId>log4j-core</artifactId>
    <version>${log4j.version}</version>
</dependency>
<dependency>
    <groupId>org.apache.logging.log4j</groupId>
    <artifactId>log4j-api</artifactId>
    <version>${log4j.version}</version>
</dependency>
<dependency>
    <groupId>org.apache.logging.log4j</groupId>
    <artifactId>log4j-slf4j-impl</artifactId>
    <version>${log4j.version}</version>
</dependency>
```

```xml
<!-- slf4j + logback 사용시 -->
<dependency>
    <groupId>com.navercorp.arcus</groupId>
    <artifactId>arcus-java-client</artifactId>
    <version>${arcus-java-client.version}</version>
</dependency>
<dependency>
    <groupId>ch.qos.logback</groupId>
    <artifactId>logback-classic</artifactId>
    <version>${logback.version}</version>
</dependency>
<dependency>
    <groupId>ch.qos.logback</groupId>
    <artifactId>logback-core</artifactId>
    <version>${logback.version}</version>
</dependency>
```

또한 2개 이상의 slf4j의 구현 라이브러리(log4j-slf4j-impl, logback-classic, ...)들이 같은 클래스패스에 존재할 경우, SLF4J에서 [multiple binding error](http://www.slf4j.org/codes.html#multiple_bindings)가 발생하므로 반드시 exclusion 키워드를 이용해 slf4j 구현 라이브러리가 하나만 존재하도록 하여야 한다.

```
SLF4J: Class path contains multiple SLF4J bindings.
SLF4J: See http://www.slf4j.org/codes.html#multiple_bindings for an explanation.
```

### ConnectionFactoryBuilder 설정

`ConnectionFactoryBuilder`는 ArcusClient의 동작 방식을 커스터마이징하기 위한 빌더 클래스다.

> 더 자세한 내용은 [ConnectionFactoryBuilder 문서](13-connection-factory.md)를 참고한다.

| Method                                          | Description                                           | Default                      | Unit  | Note                          |
|:------------------------------------------------|:------------------------------------------------------|:-----------------------------|:------|:------------------------------|
| `setOpQueueFactory(OperationQueueFactory)`      | Input Queue의 팩토리 객체 설정                                | `ArrayOperationQueueFactory` |       | 크기: 16,384                    |
| `setWriteOpQueueFactory(OperationQueueFactory)` | Write Queue의 팩토리 객체 설정                                | `ArrayOperationQueueFactory` |       | 크기: 16,384                    |
| `setReadOpQueueFactory(OperationQueueFactory)`  | Read Queue의 팩토리 객체 설정                                 | `ArrayOperationQueueFactory` |       | 크기: 16,384                    |
| `setOpQueueMaxBlockTime(long)`                  | Input Queue가 가득 찬 상태일 때 유저 스레드가 대기하는 최대 시간            | 10,000                       | ms    |                               |
| `setTranscoder(Transcoder<Object>)`             | Key-Value 타입의 캐시 데이터와 Java 객체 간 변환을 위한 Transcoder     | SerializingTranscoder        |       |                               |
| `setCollectionTranscoder(Transcoder<Object>)`   | Collection 타입의 캐시 데이터와 Java 객체 간 변환을 위한 Transcoder    | SerializingTranscoder        |       |                               |
| `setOpTimeout(long)`                            | `Future.get()` 호출 시 캐시 서버로부터 응답을 대기하는 최대 시간           | 700                          | ms    |                               |
| `setShouldOptimize(boolean)`                    | Operation Queue 내 연속된 GET Operation을 조합하여 하나의 요청으로 처리 | false                        |       | 사용 비권장                        |
| `setReadBufferSize(int)`                        | 캐시 서버와 소켓 통신 시 사용되는 전역 ByteBuffer 크기                  | 16,384                       | bytes | 읽기/쓰기 버퍼 모두 이 값을 기준으로 생성됨     |
| `setDaemon(boolean)`                            | Memcached I/O 스레드를 데몬 스레드로 설정                         | true                         |       | 기본값(true) 유지 권장               |
| `setUseNagleAlgorithm(boolean)`                 | Nagle 알고리즘 사용 여부 (false = TCP NoDelay 활성화)            | false                        |       |                               |
| `setMaxReconnectDelay(long)`                    | 캐시 서버 연결 문제 발생 시 재연결 전 대기 시간                          | 1                            | s     |                               |
| `setTimeoutExceptionThreshold(int)`             | 재연결 조건 판단 시 연속 Timeout 발생 횟수 임계값                      | 10                           | count | 최솟값: 2                        |
| `setTimeoutDurationThreshold(int)`              | 재연결 조건 판단 시 첫 Timeout부터 현재까지의 지속 시간 임계값               | 1,600                        | ms    | 0: 비활성화, 제한 범위: 1,000~5,000   |
| `setDelimiter(byte)`                            | 캐시 서버 `-D <char>` 옵션으로 지정된 Prefix/Subkey 구분자          | `:`                          |       | 서버 옵션과 반드시 일치해야 함             |
| `setKeepAlive(boolean)`                         | 캐시 서버와의 TCP KeepAlive 옵션 설정                           | false                        |       |                               |
| `setDnsCacheTtlCheck(boolean)`                  | 구동 시 DNS 캐시 TTL 검증 활성화 여부                             | true                         |       | ZooKeeper를 도메인 주소로 관리하는 경우 유효 |
| `enableShardKey(boolean)`                       | 키의 `{`, `}` 로 감싸진 부분만을 해싱 대상으로 사용                     | false                        |       |                               |
| `setAuthDescriptor(AuthDescriptor)`             | 캐시 서버 연결 시 SASL 인증을 위한 AuthDescriptor                 | null                         |       | 현재 `scramSha256` 만 지원         |
| `setMaxFrontCacheElements(int)`                 | Front Cache에 저장할 수 있는 최대 아이템 수                        | 0                            | items | 0: 비활성화                       |
| `setFrontCacheExpireTime(int)`                  | Front Cache 아이템 만료 시간                                 | 5                            | s     |                               |
| `setReadPriority(ReadPriority)`                 | Replication 환경에서 읽기 요청의 우선순위 설정                       | MASTER                       |       |                               |
| `setAPIReadPriority(APIType, ReadPriority)`     | Replication 환경에서 API 타입별 읽기 우선순위 설정                   | -                            |       | `setReadPriority`보다 우선 적용됨    |
| `setFailureMode(FailureMode)`                   | 노드 장애 발생 시 동작 방식 설정                                   | Cancel                       |       | `Cancel`만 정상 동작               |
