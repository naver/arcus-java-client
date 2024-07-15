# Java Client 사용시 주의사항

## Counter 사용

ARCUS에서 counter를 사용할 경우 최초에 set 또는 add command를 이용하여 counter key를 등록해야 한다.
일단 다음과 같이 저장해 보자.

```java
client.set("my_counter", 10000, 100); // 사실은 절대 이렇게 사용하면 안된다!
```

분명히 잘 저장되었다는 결과를 받을 것이다. 이번에는 incr counter를 이용해서 값을 증가시켜 보자.

```java
Future<Long> f = client.asyncIncr("my_counter", 1);
Object o = f.get(1000, TimeUnit.MILLISECONDS);
```

그런데, 이번에는 “cannot increment or decrement non-numeric value” 라는 에러 메시지를 받을 것이다.
Cache 서버에서 직접 counter 정보를 조회해 보면 “d”가 저장되어 있을 것이다.
왜 이런 일이 벌어졌을까? 이유는 다음과 같이 2가지다.

* Cache server에 저장할 때 data type을 보내지 않았으니, 데이터를 저장할 때 byte 값을 그대로 저장할 것이다.
  참고로 100의 비트 값은 “01100100”이니까 ASCII code 값으로는 “d”다.
  그러면 counter에서는 이 값을 int값으로 변경하면 되지 않을까 생각하겠지만 여기에는 또 다른 이유가 있다.

* Cache server는 multi-platform을 지원해야 하므로 byte-ordering(endianness)에 상관없이 데이터를 저장할 수 있어야 한다.
  좀 더 자세하게 말하면 cache server의 counter는 Integer 값만 받을 수 있게 되어 있는데,
  platform에 상관없이 동작하기 위한 방법은 오직 String으로 값을 입력할 수밖에 없는 것이다.
  만약 cache server가 primitive값을 지원한다면, 즉 byte-ordering이 서로 다른 client를 사용한다면
  counter가 우리가 원하는 대로 동작하지 않을 것이다.
  따라서 앞서 언급했듯이 “d”값을 integer값으로 변경할 수 없었던 것이다.


**ARCUS는 counter 로 사용할 데이터의 최초 값은 반드시 String으로 지정하도록 강제하고 있다.**
**따라서, 반드시 최초 값은 다음과 같이 String 값으로 설정해야 한다.**


```java
client.set("my_counter", 10000, "100"); // 반드시 이렇게 사용해야 한다!
```

참고로, 정말 운이 좋은 개발자는 integer 값으로 등록해도 에러가 나지 않을 수 있다.
예를 들어 52로 초기값을 저장한다고 하면, 이번에는 counter를 사용해도 에러가 나지 않을 것이다.
왜냐하면 52의 ASCII code값이 4이기 때문이다.

그리고, ARCUS client에서 incr/decr command의 입력 값을 primitive 또는 numeric wrapper 값으로 받을 수 있도록 되어 있으나, 내부적으로는 String으로 변환 후 cache server로 전달하도록 되어 있다.


## Operation Queue Block Timeout 설정

setOpQueueMaxBlockTime(long t) 함수를 이용하여 queue에 operation을 등록할 때 timeout을 설정한 경우,
모든 operation은 IllegalStateException을 다음 코드와 같이 catch해 주어야 한다.

```java
Future<Boolean> future = null;
try {
    future = client.set(key, 60 * 60 * 24, value);
} catch (IllegalStateException ise) { // operation queue가 full 상태여서 timeout 내에 Operation을 등록하지 못한 경우
    System.out.println("illegal state exception");
}

if (future != null) {
    try {
        boolean result = future.get(1000, TimeUnit.MILLISECONDS);
    } catch (TimeoutException e) {
        f.cancel(true);
        System.out.println("timeout exception");
    } catch (ExecutionException e) {
        f.cancel(true);
        System.out.println("execution exception");
    } catch (InterruptedException e) {
        f.cancel(true);
        System.out.println("interrupted exception");
    }
}
```

Operation queue block timeout 옵션을 설정하지 않으면, 작업 요청을 하는 client 함수들은 모두 blocking을 당할 수가 있다.
물론, 실제로 future.get 수행시 정상 처리가 되든 exception이 나서 취소가 되든 operation queue에 등록된 작업은
지속적으로 빠져나가 operation queue에 작업을 등록할 공간은 느리지만 생길 것이다.
그러나, request 요청 속도가 operation queue에 등록된 작업의 처리 속도보다 매우 빠르다면,
해당 request를 요청하는 것보다 차라리 바로 실패로 처리하고,
back-end 데이터 저장소로 요청을 보내는 것이 더 좋은 선택이 될 수 있다.


## Expiretime 설정

Expiretime은 초 단위로 지정된 시간만큼 미래의 시간인 Unix Time으로 변경되어 저장된다.
그러나, '''expire time이 30일을 초과하면 1970년 기준의 Unix time으로 변경된다.''' 
예를 들어, expiretime을 1000 * 60 * 60과 같은 식으로 등록을 하게 되면 대략 40일 정도가 되는데,
이는 1970년 기준의 unix time으로 인식되어 아주 옛날 시간이 되어 버리고 즉각 expire하게 된다.
 '''따라서 client에서는 분명히 저장했다고 생각하여 retrieval command(get, gets)를 수행했을 경우에
 cache data가 전혀 나오지 않는 현상이 발생할 수 있는 것이다.'''


### Operation timeout 설정

ARCUS Client의 모든 비동기방식의 메서드를 호출할 때 timeout을 지정할 수 있다.
**이러한 timeout 값을 반드시 지정하여 사용할 것을 권장한다.**

```java
Future<Boolean> setResult = client.set("sample:testKey", 10, "testValue");
boolean result = setResult.get(300L, TimeUnit.MILLISECONDS);
```

위 예제는 ARCUS cache server에 “testValue”를 저장할 때 timeout값을 300ms로 지정한 코드이다.

첫째, 이 코드가 실행되는 시점에서 full GC(garbage collection)가 발생했고, 
full GC time이 500ms였다면 이 요청은 timeout이 되게 된다.
**따라서, timeout값은 JVM full GC time을 고려하여 설정해야 한다.**
**대부분 몇십 개의 timeout이 발생하는 문제는 full GC time이 길어서 발생하는 문제이다.**

둘째, burst traffic, small packet buffer size 등의 이유로 cache client와 cache server 사이에
packet retransmission이 발생할 수 있다.
Linux 환경에서 최소 retransmission timeout은 200ms이며,
그 다음의 retransmission timeout은 400ms, 800ms, ... 형태로 두 배씩 길어지게 된다.
Packet retransmission은 제법 흔하게 발생하고 있으므로,
이러한 packet retransmission에 대해 견딜 수 있을 정도로 timeout을 설정하길 권장한다.
따라서, 300ms, 700ms 정도가 권장되는 timeout 값이다.

## 캐릭터 인코딩

ARCUS Client가 지원하는 캐릭터 인코딩은 UTF-8이다. UTF-8 이외의 UTF-16과 같은 다른 인코딩 타입을 지원하지 않아,
만약 시스템에서 UTF-16과 같은 캐릭터 인코딩을 사용하고 있다면 반드시 UTF-8 타입으로 변경해주어야 한다. 
리눅스 기준으로 시스템의 기본 캐릭터 인코딩 확인 및 변경 방법은 아래와 같다. 
리눅스 배포판, 버전마다 다를 수 있으므로 참고로만 활용한다.

### 시스템 캐릭터 인코딩 확인 (Linux 기준)
```bash
$ echo $LANG
en_US.UTF-8
```

```bash
$ locale
LANG=en_US.UTF-8
LC_CTYPE=en_US.UTF-8
LC_NUMERIC="en_US.UTF-8"
LC_TIME="en_US.UTF-8"
LC_COLLATE="en_US.UTF-8"
LC_MONETARY="en_US.UTF-8"
LC_MESSAGES="en_US.UTF-8"
LC_PAPER="en_US.UTF-8"
LC_NAME="en_US.UTF-8"
LC_ADDRESS="en_US.UTF-8"
LC_TELEPHONE="en_US.UTF-8"
LC_MEASUREMENT="en_US.UTF-8"
LC_IDENTIFICATION="en_US.UTF-8"
LC_ALL=
```

### 시스템 캐릭터 인코딩 변경 (Linux 기준)
 
```bash
$ localectl set-locale LANG=en_US.UTF-8
```


### JVM 인코딩 변경

시스템의 캐릭터 인코딩을 변경하기가 어렵다면, JVM에서 변경해주어도 괜찮다. JVM의 Property(-D) 옵션 중 `file.encoding` 옵션을 사용하여 변경할 수 있다.

```
-Dfile.encoding=UTF-8
```

## JVM의 DNS Cache 만료 시간 설정

ArcusClient 객체 생성 시 ZooKeeper 주소를 도메인 네임으로 설정한 경우, 도메인 네임이 가리키는 IP 주소를 변경하면 ZooKeeper 서버에 연결할 때마다 DNS 조회로 IP 주소를 얻어 연결하므로 응용 프로세스의 재구동 없이도 새로운 IP 주소에 연결할 수 있다.

JVM에는 OS의 DNS Cache와는 별도로 자체적인 DNS Cache 만료 시간을 설정할 수 있다.
이 값은 아무런 설정을 하지 않았을 때 기본 30초이지만, 보안 관련 요구 사항으로 `SecurityManager`가 설정된 상태이면 기본 -1(무한대)로 설정된다.
이 값이 무한대이면 ZooKeeper 서버 이전 등으로 IP 주소가 변경되었어도 ArcusClient 객체는 DNS 조회에서 캐싱된 예전 IP 주소를 얻게 되므로 새로운 ZooKeeper 서버에 연결할 수 없다.

이에 따라 arcus-java-client 1.14.0 버전에 현재 Java 프로세스의 DNS Cache 만료 시간을 검증하는 기능이 추가되었다.
이 기능은 JVM의 DNS Cache 만료 시간이 0초(캐싱하지 않음) 이상, 300초 이하인지 확인하고 범위를 벗어나면 Exception을 발생시켜 ArcusClient 객체의 생성을 불가능하게 한다.
단, 응용의 성격에 따라 JVM의 DNS Cache 만료 시간이 300초를 초과해야 할 수 있으므로 설정을 통해 검증 기능을 끌 수 있다.

### JVM의 DNS Cache 만료 시간 검증 기능을 끄는 방법

JVM의 DNS Cache 만료 시간을 검증하는 기능은 기본적으로 켜져 있으며, 이를 끄고 싶다면 아래와 같이 설정한다.

```java
ConnectionFactoryBuilder cfb = new ConnectionFactoryBuilder();
cfb.setDnsCacheTtlCheck(false);

ArcusClient.createArcusClient(ZOOKEEPER_ADDRESS, SERVICE_CODE, cfb);
```

### JVM의 DNS Cache 만료 시간 설정 방법

JVM의 DNS Cache 만료 시간을 설정하는 방법은 다음과 같다.
아래의 예시들은 만료 시간을 30초로 설정하는 경우이다.

#### Java 프로세스 실행 시 `-Dnetworkaddress.cache.ttl` 인자를 주어 실행

```bash
java -jar ... -Dnetworkaddress.cache.ttl=30
```

#### 소스 코드 상에서 만료 시간 설정

```java
Security.setProperty("networkaddress.cache.ttl", "30");
```

```java
System.setProperty("sun.net.inetaddr.ttl", "30");
```

#### JDK 설정 (Linux 기준)

- JDK 8+ : `$JAVA_HOME/jre/lib/security/java.security`
- JDK 11+ : `$JAVA_HOME/conf/security/java.security`

```vim
#
# The Java-level namelookup cache policy for successful lookups:
#
# any negative value: caching forever
# any positive value: the number of seconds to cache an address for
# zero: do not cache
#
# default value is forever (FOREVER). For security reasons, this
# caching is made forever when a security manager is set. When a security
# manager is not set, the default behavior in this implementation
# is to cache for 30 seconds.
#
# NOTE: setting this to anything other than the default value can have
#       serious security implications. Do not set it unless
#       you are sure you are not exposed to DNS spoofing attack.
#
networkaddress.cache.ttl=30
```

### 주의 사항

JVM의 DNS Cache 만료 시간을 별도로 설정하지 않더라도, 아래의 코드를 통해 `SecurityManager`를 설정하게 되면 해당 만료 시간이 -1(무한대)로 설정된다.
만약 JVM의 DNS Cache 만료 시간을 별도로 설정하지 않았음에도 JVM의 DNS Cache 만료 시간을 검증하는 기능 내에서 Exception이 발생한다면 응용에 아래의 코드가 있는지 확인한다. 

```java
System.setSecurityManager(...)
```
