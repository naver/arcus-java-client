# 13. ConnectionFactoryBuilder 설정

`ConnectionFactoryBuilder` 는 ArcusClient의 동작 방식을 커스터마이징하기 위한 빌더 클래스다.

기본값으로도 동작하지만, 네트워크 환경이나 응용의 요구사항에 맞게 다양한 옵션을 조정할 수 있다.


```java
ConnectionFactoryBuilder cfb = new ConnectionFactoryBuilder()
        /* 다양한 옵션 설정 */
        .setOpQueueFactory(...)
        .setOpTimeout(...)
        .build();

ArcusClientPool pool = ArcusClient.createArcusClientPool(hostPorts, serviceCode, cfb);
```

**Queue**

| Method                                          | Description                                | Default                      | Unit | Note       |
|:------------------------------------------------|:-------------------------------------------|:-----------------------------|:-----|:-----------|
| `setOpQueueFactory(OperationQueueFactory)`      | Input Queue의 팩토리 객체 설정                     | `ArrayOperationQueueFactory` |      | 크기: 16,384 |
| `setWriteOpQueueFactory(OperationQueueFactory)` | Write Queue의 팩토리 객체 설정                     | `ArrayOperationQueueFactory` |      | 크기: 16,384 |
| `setReadOpQueueFactory(OperationQueueFactory)`  | Read Queue의 팩토리 객체 설정                      | `ArrayOperationQueueFactory` |      | 크기: 16,384 |
| `setOpQueueMaxBlockTime(long)`                  | Input Queue가 가득 찬 상태일 때 유저 스레드가 대기하는 최대 시간 | 10,000                       | ms   |            |

**타임아웃 및 재연결**

| Method                              | Description                              | Default | Unit  | Note                        |
|:------------------------------------|:-----------------------------------------|:--------|:------|:----------------------------|
| `setOpTimeout(long)`                | `Future.get()` 호출 시 캐시 서버 응답을 대기하는 최대 시간 | 700     | ms    |                             |
| `setMaxReconnectDelay(long)`        | 캐시 서버 연결 문제 발생 시 재연결 전 대기 시간             | 1       | s     |                             |
| `setTimeoutExceptionThreshold(int)` | 재연결 조건 판단 시 연속 Timeout 발생 횟수 임계값         | 10      | count | 최솟값: 2                      |
| `setTimeoutDurationThreshold(int)`  | 재연결 조건 판단 시 첫 Timeout부터 현재까지의 지속 시간 임계값  | 1,600   | ms    | 0: 비활성화, 제한 범위: 1,000~5,000 |
| `setKeepAlive(boolean)`             | 캐시 서버와의 TCP KeepAlive 설정                 | false   |       |                             |

**직렬화**

| Method                                        | Description                                        | Default               | Unit | Note |
|:----------------------------------------------|:---------------------------------------------------|:----------------------|:-----|:-----|
| `setTranscoder(Transcoder<Object>)`           | Key-Value 타입 캐시 데이터와 Java 객체 간 변환에 사용할 Transcoder  | SerializingTranscoder |      |      |
| `setCollectionTranscoder(Transcoder<Object>)` | Collection 타입 캐시 데이터와 Java 객체 간 변환에 사용할 Transcoder | SerializingTranscoder |      |      |

**네트워크 및 연결**

| Method                              | Description                                  | Default | Unit  | Note                          |
|:------------------------------------|:---------------------------------------------|:--------|:------|:------------------------------|
| `setReadBufferSize(int)`            | 캐시 서버와 소켓 통신 시 사용되는 전역 ByteBuffer 크기         | 16,384  | bytes | 읽기/쓰기 버퍼 모두 이 값을 기준으로 생성됨     |
| `setDaemon(boolean)`                | Memcached I/O 스레드를 데몬 스레드로 설정                | true    |       | 기본값(true) 유지 권장               |
| `setUseNagleAlgorithm(boolean)`     | Nagle 알고리즘 사용 여부 (TCP NoDelay 옵션)            | false   |       |                               |
| `setDelimiter(byte)`                | 캐시 서버 `-D <char>` 옵션으로 지정된 Prefix/Subkey 구분자 | `:`     |       | 서버 옵션과 반드시 일치해야 함             |
| `setDnsCacheTtlCheck(boolean)`      | 구동 시 DNS 캐시 TTL 검증 활성화 여부                    | true    |       | ZooKeeper를 도메인 주소로 관리하는 경우 유효 |
| `enableShardKey(boolean)`           | 키의 `{`, `}` 로 감싸진 부분만을 해싱 대상으로 사용            | false   |       |                               |
| `setAuthDescriptor(AuthDescriptor)` | 캐시 서버 연결 시 SASL 인증을 위한 AuthDescriptor        | null    |       | 현재 `scramSha256` 만 지원         |

**최적화**

| Method                       | Description                                                | Default | Unit | Note   |
|:-----------------------------|:-----------------------------------------------------------|:--------|:-----|:-------|
| `setShouldOptimize(boolean)` | Operation Queue 내 연속된 GET 요청을 최대 100개 단위로 조합하여 하나의 요청으로 처리 | false   |      | 사용 비권장 |

**Front Cache**

| Method                          | Description                    | Default | Unit  | Note    |
|:--------------------------------|:-------------------------------|:--------|:------|:--------|
| `setMaxFrontCacheElements(int)` | Front Cache에 저장할 수 있는 최대 아이템 수 | 0       | items | 0: 비활성화 |
| `setFrontCacheExpireTime(int)`  | Front Cache 아이템 만료 시간          | 5       | s     |         |

**Replication (Enterprise Only)**

| Method                                      | Description                         | Default | Unit | Note                       |
|:--------------------------------------------|:------------------------------------|:--------|:-----|:---------------------------|
| `setReadPriority(ReadPriority)`             | Replication 환경에서 읽기 요청의 우선순위 설정     | MASTER  |      |                            |
| `setAPIReadPriority(APIType, ReadPriority)` | Replication 환경에서 API 타입별 읽기 우선순위 설정 | 없음      |      | `setReadPriority`보다 우선 적용됨 |

## Queue

### setOpQueueFactory / setWriteOpQueueFactory / setReadOpQueueFactory

사용자의 요청은 Operation 객체로 변환되어 아래 순서로 Queue를 거쳐 처리된다.

```
사용자 요청 → Input Queue → Write Queue → 캐시 서버 → Read Queue
```

- **Input Queue**: 사용자 요청이 최초로 담기는 Queue
- **Write Queue**: 캐시 서버로 전송하기 위해 대기 중인 Operation Queue
- **Read Queue**: 캐시 서버로부터 응답을 받기 위해 대기 중인 Operation Queue

각 Queue의 팩토리 객체를 지정하지 않으면 크기가 16,384인 `ArrayBlockingQueue`가 기본적으로 사용된다.

Queue 팩토리는 두 가지 구현을 제공한다.

| 구현체                           | 내부 Queue              | 크기 지정 |
|:------------------------------|:----------------------|:------|
| `ArrayOperationQueueFactory`  | `ArrayBlockingQueue`  | 가능    |
| `LinkedOperationQueueFactory` | `LinkedBlockingQueue` | 불가능   |

### setOpQueueMaxBlockTime

Input Queue가 가득 찬 상태일 때, 사용자 스레드가 Operation을 Queue에 등록하기 위해 대기하는 최대 시간을 지정한다.

## 타임아웃 및 재연결

### setOpTimeout

`Future.get()` 호출 시 캐시 서버로부터 응답을 대기하는 최대 시간을 지정한다.

기본값을 사용하면 아래 두 호출은 동일하게 동작한다.

```java
future.get();
future.get(700,TimeUnit.MILLISECONDS);
```

### setTimeoutExceptionThreshold / setTimeoutDurationThreshold

요청 처리 중 네트워크 연결에 문제가 발생했을 때 reconnect가 적절히 수행되도록 임계값을 지정한다. 

다음 두 조건을 **모두** 만족하면 reconnect를 시도한다.

- 첫 Timeout 발생 시점부터 현재까지 timeout이 `TimeoutExceptionThreshold`회 이상 **연속으로** 발생 중이다.
- 첫 Timeout 발생 시점부터 현재까지의 **경과 시간**이 `TimeoutDurationThreshold`를 초과했다.

**예시**

| TimeoutExceptionThreshold | TimeoutDurationThreshold | reconnect 조건                          |
|:--------------------------|:-------------------------|:--------------------------------------|
| 10                        | 1,600ms                  | 1.6초 동안 10회 이상 timeout이 연속 발생 중일 때    |
| 10                        | 0 (비활성화)                 | timeout이 10회 이상 연속 발생 시 무조건 reconnect |

- `TimeoutDurationThreshold`를 0으로 설정하면 Burst 트래픽으로 인한 일시적인 timeout에도 reconnect가 발생할 수 있다.
- 따라서, burst 트래픽 요청이 자주 발생하는 환경에서는 0으로 설정하는 것을 권장하지 않는다.

### setMaxReconnectDelay

캐시 서버와의 연결에서 문제가 발생하여 일정 시간 후 reconnect를 시도하는 경우, 재연결 전 대기 시간을 지정한다.

### setKeepAlive

캐시 서버와의 연결을 TCP KeepAlive 옵션을 통해 유지할지 여부를 설정한다.

- 장시간 트래픽이 없는 TCP 연결을 강제 종료하는 환경에서 주기적인 트래픽을 발생시켜 강제 종료를 방지하는 용도로 사용할 수 있다.

## 직렬화

> 직렬화에 대한 자세한 내용은 [Transcoder 문서](14-transcoder.md)를 참고한다.

### setTranscoder

Key-Value 타입의 캐시 데이터와 Java 객체 간 변환에 사용할 Transcoder를 지정한다.

기본적으로 `SerializingTranscoder`를 사용하며, 압축 시 GZip 방식을 사용한다.

```java
SerializingTranscoder transcoder = SerializingTranscoder.forKV()
        .maxSize(CachedData.MAX_SIZE)
        .classLoader(this.getClass().getClassLoader())
        .build();

transcoder.setCharset("EUC-KR");
transcoder.setCompressionThreshold(4096);

ConnectionFactoryBuilder cfb = new ConnectionFactoryBuilder();
cfb.setTranscoder(transcoder);
```

`SerializingTranscoder` 외에도 `JsonSerializingTranscoder`, `GenericJsonSerializingTranscoder`를 사용할 수 있다.

### setCollectionTranscoder

Collection 타입의 캐시 데이터와 Java 객체 간 변환에 사용할 Transcoder를 지정한다.

기본적으로 `SerializingTranscoder`를 사용한다.

```java
SerializingTranscoder transcoder = SerializingTranscoder.forCollection()
        .maxSize(16384)
        .classLoader(this.getClass().getClassLoader())
        .build();

transcoder.setCharset("EUC-KR");
transcoder.setCompressionThreshold(4096);

ConnectionFactoryBuilder cfb = new ConnectionFactoryBuilder();
cfb.setCollectionTranscoder(transcoder);
```

Collection 아이템에 `String`, `Integer` 등 서로 다른 타입의 값을 저장하는 경우,`forceJDKSerializationForCollection()`을 활성화하여 모든 값에 Java 직렬화를 강제 적용할 수 있다.

```java
SerializingTranscoder transcoder = SerializingTranscoder.forCollection()
        .forceJDKSerializationForCollection()
        .build();
```

## 네트워크 및 연결

### setReadBufferSize

캐시 서버와 소켓 통신 시 사용되는 전역(Read/Write) ByteBuffer 크기를 지정한다.

- 이름과 달리 **읽기/쓰기 버퍼 모두 이 값을 기준으로 생성** 된다.
- ByteBuffer 크기를 초과하는 데이터가 수신되면, ByteBuffer 크기만큼 처리한 후 버퍼를 비우고 다시 사용하는 방식으로 동작한다.

### setDaemon

Memcached I/O 스레드를 데몬 스레드로 설정할지 여부를 지정한다.

- 기본값은 true로 애플리케이션 종료 시 I/O 스레드가 함께 종료된다. 
- 기본값을 유지하며, Graceful Shutdown이 필요한 경우 `shutdown(long timeout, TimeUnit unit)` 을 명시적으로 호출하는 것을 권장한다.

> `setDaemon(false)` 설정 후 명시적으로 `shutdown()`이 호출되지 않은 상태에서 프로세스 종료가 시도되면, 정상적으로 종료되지 않을 수 있다.

### useNagleAlgorithm

Nagle 알고리즘을 통해 **TCP NoDelay** 옵션을 활성화할지 여부를 설정한다.

- 기본값은 false로 TCP NoDelay 옵션을 활성화한다.
- 처리량보다 응답 속도가 중요한 ARCUS 환경에서는 기본값을 유지하는 것을 권장한다.
- 만약, true로 설정 시 네트워크 효율은 개선될 수 있으나, 캐시 응답 지연이 발생할 수 있다.

### setDelimiter

Prefix와 Subkey를 구분하는 Delimiter 문자를 지정한다.

- 만약, 캐시 서버에 `-D <char>` 옵션으로 구분자를 직접 지정한 경우 클라이언트에서도 동일한 구분자를 지정해주어야 한다.
- 서버에 별도 옵션을 지정하지 않았다면 설정할 필요가 없다. (기본값 `:` 사용)

### setDnsCacheTtlCheck

클라이언트 구동 시 DNS 캐시 TTL 검증 활성화 여부를 설정한다.

- ZooKeeper Ensemble을 도메인 주소로 관리하는 경우, DNS에 매핑된 IP 정보가 변경될 때 정상적으로 반영되도록 DNS 캐시 TTL 값이 0~300초 내에 존재하는지 검증한다.

### enableShardKey

키의 `{`, `}` 로 감싸진 일부분만을 해싱 대상으로 사용하는 기능을 활성화한다.

- 만약 따로 지정하지 않았다면 기존 방식대로 전체 키를 해싱한다.

```java
// "user" 부분만 해싱 대상으로 사용
String key = "{user}:profile:1";
```

### setAuthDescriptor

캐시 서버 연결 시 SASL 인증을 시도하며 인증에 실패하는 경우 해당 연결을 종료하고 1초 뒤 재시도한다.

- 현재 `scramSha256` 방식만 지원된다.

```java
ConnectionFactoryBuilder cfb = new ConnectionFactoryBuilder();
cfb.setAuthDescriptor(AuthDescriptor.scramSha256("username", "password"));
```

## 최적화

### setShouldOptimize

Operation Queue 내 연속된 GET 요청을 최대 100개 단위로 조합하여 하나의 요청으로 처리하는 최적화 로직 사용 여부를 설정한다.

> 이 기능은 기존 spymemcached 호환을 위해 존재하며, cancel, replication 등의 시나리오에서 검증이 충분히 이루어지지 않았다.  
> 따라서, 비활성화(false)를 권장한다.

## Front Cache

> Front Cache에 대한 자세한 내용은 [Front Cache 문서](11-front-cache.md)를 참고한다.

ARCUS Remote Cache의 네트워크 지연을 줄이고 GC 부담을 낮추기 위해 로컬 메모리에 데이터를 캐싱하는 기능이다.

- 내부적으로 Ehcache3를 사용한다.
- `setMaxFrontCacheElements()`에 0보다 큰 값을 설정하면 활성화된다.

```java
ConnectionFactoryBuilder cfb = new ConnectionFactoryBuilder();
cfb.setMaxFrontCacheElements(10000); // 필수
cfb.setFrontCacheExpireTime(5);      // 선택, 기본값 5초
```


## Replication (Enterprise Only)

### setReadPriority

Replication 환경에서 읽기 요청의 우선순위를 설정한다.

| 값                     | 동작                                               |
|:----------------------|:-------------------------------------------------|
| `ReadPriority.MASTER` | 읽기 요청을 Master 노드로 전송 (기본값)                       |
| `ReadPriority.SLAVE`  | 읽기 요청을 Slave 노드로 전송, Slave 부재 시 Master로 fallback |
| `ReadPriority.RR`     | Master/Slave 노드에 Round Robin 방식으로 읽기 요청을 분산      |

**Slave 부재 시 동작**

- `ReadPriority.SLAVE`로 설정된 상태에서 모든 Slave가 다운된 경우, 자동으로 Master로 fallback되어 읽기 요청을 처리한다.
- Slave가 복구되면 즉시 Slave로 읽기가 전환된다.

**Switchover 시 동작**

- Switchover가 발생하면 클라이언트는 ZooKeeper 이벤트를 통해 역할 변경을 감지하고, 새로운 Master/Slave 구성에 맞게 읽기 및 쓰기 요청을 자동으로 재분배한다.
- Switchover 발생 시점에 처리 중이던 Operation은 기존 노드의 Queue(inputQ, writeQ, readQ)에서 새로운 Master 노드의 writeQ로 이전(moveOperations)되어 유실 없이 처리된다.

### setAPIReadPriority

Replication 환경에서 API 타입별로 읽기 우선순위를 개별 설정한다.

`setReadPriority()`보다 우선 적용된다.

```java
ConnectionFactoryBuilder cfb = new ConnectionFactoryBuilder();
cfb.setReadPriority(ReadPriority.SLAVE);
cfb.setAPIReadPriority(APIType.GET, ReadPriority.MASTER); // GET만 Master로 읽기
```

## 주의사항

### setFailureMode

노드 장애 발생 시 동작 방식을 설정한다.

`FailureMode`는 아래 세 가지 값을 제공하지만, ARCUS 환경에서는 `Cancel`만 정상적으로 동작한다.

| 값              | 동작                          | ARCUS 지원 여부 |
|:---------------|:----------------------------|:------------|
| `Redistribute` | 장애 노드의 요청을 다른 노드로 재분배       | X           |
| `Retry`        | 객체 생성은 가능하나 실제 재시도가 동작하지 않음 | X           |
| `Cancel`       | 장애 노드로 향하는 모든 요청을 자동으로 취소   | O           |

### setInitialObservers

`setInitialObservers()` 는 노드 연결 상태를 모니터링하기 위한 Observer를 등록하는 메서드이다.

> `setInitialObservers()`는 내부 초기화 전용 메서드로, 연결 상태 모니터링이 필요한 경우 `ArcusClient` 초기화 완료 후 `addObserver()`를 사용한다.

```java
ArcusClient arcusClient = ArcusClient.createArcusClient(zkAddress, serviceCode, cfb);

arcusClient.addObserver(new ConnectionObserver() {
    @Override
    public void connectionEstablished(MemcachedNode node, int reconnectCount) {
        log.info("Connected to {}, reconnectCount={}", node, reconnectCount);
    }

    @Override
    public void connectionLost(MemcachedNode node) {
        log.warn("Connection lost: {}", node);
    }
});
```