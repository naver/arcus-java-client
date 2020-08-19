### Java Client Log Messages

Java client에서 남기는 로그들과 그것들이 의미하는 바는 아래와 같다. 로그는 서비스에서 설정한 위치에 남게 된다. 로그 레벨 변경과 logger 설정은 [arcus client 설정](02-arcus-java-client.md#arcus-client-설정)을 참고하기 바란다.


#### ARCUS client가 정상적으로 초기화 되었을 때

ARCUS client가 정상적으로 초기화 되면 다음과 같은 로그를 남긴다.

```java
INFO net.spy.memcached.CacheManager: CacheManager started. ([mailto:dev@dev.arcuscloud.nhncorp.com:17288 dev@dev.arcuscloud.nhncorp.com:17288])

WARN net.spy.memcached.CacheMonitor: Cache list has been changed : From=null, To=[127.0.0.1:11211-hostname, xxx.xxx.xxx.xxx:xxxx-hostname] : [serviceCode=dev]

INFO net.spy.memcached.MemcachedConnection: new memcached node added {QA sa=/127.0.0.1:11211, #Rops=0, #Wops=0, #iq=0, topRop=null, topWop=null, toWrite=0, interested=0} to connect queue

INFO net.spy.memcached.MemcachedConnection: new memcached node added {QA sa=/127.0.0.1:11211, #Rops=0, #Wops=0, #iq=0, topRop=null, topWop=null, toWrite=0, interested=0} to connect queue

INFO net.spy.memcached.MemcachedConnection: Connection state changed for sun.nio.ch.SelectionKeyImpl@388ee016

INFO net.spy.memcached.MemcachedConnection: Connection state changed for sun.nio.ch.SelectionKeyImpl@2e5bbd6

2011-09-21 10:44:54.055 WARN net.spy.memcached.CacheManager: All arcus connections are established.
```

#### ARCUS admin address가 잘못 되었을 때

ARCUS admin address를 잘못 지정했거나 server가 응답이 없을 때 아래와 같은 로그를 남긴다.

```java
FATAL net.spy.memcached.CacheManager: Unexpected exception. contact to arcus administrator

INFO net.spy.memcached.CacheManager: Close ZooKeeper client.
```

#### ARCUS admin과 연결이 단절 되었을 때

ARCUS admin과의 네트웍 문제로 연결이 단절되면 아래 로그를 남기고 ARCUS admin으로 접속을 재시도한다.

```java
WARN net.spy.memcached.CacheMonitor: Disconnected from the ARCUS admin. Trying to reconnect : [serviceCode=dev]
```

#### ARCUS admin 세션이 만료되었을 때

세션이 만료되는 경우는 admin과의 연결문제로 heart beat이 제대로 되지 않아 만료된 경우이다.

```java
WARN net.spy.memcached.CacheMonitor: Session expired. Trying to reconnect to the Arcus admin : [serviceCode=dev]
```

세션이 만료되면 ARCUS client는 reconnect를 시도한다.

```java
INFO net.spy.memcached.CacheMonitor: Shutting down the CacheMonitor : [serviceCode=dev]

WARN net.spy.memcached.CacheManager: Unexpected disconnection from Arcus admin. Trying to reconnect to Arcus admin.

INFO net.spy.memcached.CacheManager: Close ZooKeeper client.
```

Reconnect에 성공하면 아래와 같은 로그가 남는다.

```java
WARN net.spy.memcached.CacheMonitor: Reconnected to the Arcus admin : [serviceCode=dev]

ERROR net.spy.memcached.CacheMonitor: Cache list has been changed : From=null, To=[10.0.0.1:11211-arcus01.companyname.com, 10.0.0.2:11211-arcus02.companyname.com] : [serviceCode=dev]
```

#### Object serialization 문제

저장하는 값이 serializable하지 않거나 null일 경우 아래와 같은 오류 메시지를 남긴다.

Null을 저장하려 하는 경우

```java
Can’t serialize null
```

저장하려는 값이 serializable하지 않은 경우

```java
java.lang.IllegalArgumentException: Non-serializable object, cause=원인
```

이 때 `원인`에 serialize 실패 원인이 되는 클래스이름이 보여진다.

#### out of memory storing object

Expire time을 -1로 지정하여 아이템을 저장할 때 “out of memory storing object”에러가 발생할 수 있다. 이유는 ARCUS서버를 실행할 때 백분율로 지정한 sticky item 저장 영역이 가득 찼기 때문이다. Sticky item으로 사용 가능한 비율을 지정하지 않았을 때에도 동일한 오류메시지가 나타난다.

```java
[ERROR](StoreOperationImpl :? ) Error: SERVER_ERROR out of memory storing object
[INFO ](MemcachedConnection :? ) Reconnection due to exception handling a memcached operation on {QA sa=/ 127.0.0.1:11211, #Rops=2, #Wops=0, #iq=0, topRop=net.spy.memcached.protocol.ascii.StoreOperationImpl@250d593e, topWop=null, toWrite=0, interested=1}. This may be due to an authentication failure.
OperationException: SERVER: SERVER_ERROR out of memory storing object
at net.spy.memcached.protocol.BaseOperationImpl.handleError(BaseOperationImpl.java:127)
at net.spy.memcached.protocol.ascii.OperationImpl.readFromBuffer(OperationImpl.java:131)
at net.spy.memcached.MemcachedConnection.handleReads(MemcachedConnection.java:457)
at net.spy.memcached.MemcachedConnection.handleIO(MemcachedConnection.java:389)
at net.spy.memcached.MemcachedConnection.handleIO(MemcachedConnection.java:182)
at net.spy.memcached.MemcachedClient.run(MemcachedClient.java:1630)
```

#### Operation Timeout

timeout시간 내에 사용자가 요청한 결과 값을 반환해 줄 수 없으면 아래 로그를 남기고 TimeoutException을 던진다.

```java
net.spy.memcached.internal.CheckedOperationTimeoutException: Timed out waiting for operation. > 300 - failing node: /127.0.0.1:11211 [WRITING] [#iq=13 #Wops=7 #Rops=10 #CT=13]
```

로그의 의미는 다음과 같다.

| 메세지 | 설명 |
| ------ | ---- |
| Timed out waiting for operation. > 300 MILLISECONDS| Timeout 값이 300ms로 지정되어있고 요청의 결과를 받기까지300ms이상 걸려서 timeout되었다.|
| Failing node: /127.0.0.1:11211 | 해당 요청은 127.0.0.1:11211 에서 수행한다. |
| [WRITING]	| 해당 요청은 socket write를 위해 대기 중이다. |
| [READING]	| 해당 요청은 서버로 전달되었고 결과가 돌아오기를 기다리거나, 결과 값을 읽어 들이는 중이다. |
| #iq	| 해당 ARCUS node Input queue에서 대기중인 요청 수 |
| #Wops	| Writing 상태에 있는 요청 수 |
| #Rops	| Reading 상태에 있는 요청 수 |
| #CT	| Continuous timeout, 해당 arcus node에서 연속적으로 발생한 timeout 횟수, 이 값이 connection factory builder로 지정되는 timeout threshold값을 넘어서면 클라이언트는 해당 서버와의 연결을 끊고 다시 연결한다. |

즉, 해석해 보자면 ARCUS node 127.0.0.1:11211에 대해 요청을 보냈는데 300ms이내에 결과값을 돌려줄 수 없어 실패했다는 의미이다.
가장 많이 발생할 수 있는 메시지로 원인 또한 매우 다양하다. 원인을 나열해 보자면 다음과 같다.

- JVM Full GC time값이 operation timeout값 보다 클 때.

  WAS의 full GC 시간을 측정하여 timeout 값을 그보다 크게 설정한다. Operation timeout으로 매우 작은 값이 설정되지 않았는지 살펴본다. (대부분의 원인이 이것이다.)
 
- Client와 ARCUS server간의 네트워크 문제


  서비스와 ARCUS server간의 네트워크 (switch, AS, DS등)에 문제는 없었는지 확인한다.

  서비스와 연결되는 외부 서버와의 연결에도 문제가 있었는지 살펴본다. 만약 외부와 연결되는 서버가 없다면 ARCUS admin서버와 연결에 문제는 없었는지 로그를 살펴본다. 네트워크 관련 문제가 있었더라도 ARCUS timeout만 발생할 수도 있다. 예를 들어 ARCUS timeout이 1초이고, DB와의 timeout이 2초라고 할 때. network단절이 1.5초간 있었다 하면 ARCUS timeout메시지만 남게 될 것이다. 네트워크 단절이 3초간 발생했다고 하면 ARCUS |- timeout이 먼저 발생하고 뒤따라서 DB timeout이 발생하는 것처럼 보일 것이다. 대부분 이와 같은 문제가 발생하면 WAS의 thread가 증가하는 현상을 보인다. Request process thread가 timeout내에 응답을 주지 못하게 되면 추가로 들어오는 요청을 받기 위한 thread가 추가로 생성되기 때문이다.

- ARCUS server

  ARCUS 서버의 하드웨어 및hubble 모니터 결과를 살펴보고 원인을 찾는다.
  
  Timeout이 발생한 ARCUS host가 한 개인지 여러 개인지 살펴본다.

- 한계

  Client 한 개로 처리하는데 한계이다. pool사용을 고려해본다.  


