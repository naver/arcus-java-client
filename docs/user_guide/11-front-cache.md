# 11. Front Cache 기능

## 개요
ARCUS는 Remote Cache 시스템이므로 요청을 보낼 때 장비의 사양, 네트워크 리소스에 따라 지연 시간이 느려지거나 처리가 불가능한 상태가 발생할 수 있다.
그리고, 요청에 대한 응답을 받을 때마다 데이터를 새로운 객체로 변환시키므로 JVM의 Garbage Collector에 부담이 가해질 수 있다.
따라서 실제 데이터가 거의 변경되지 않고, 변경이 있더라도 아주 짧은 시간 내에는 이전 데이터를 보여줘도 상관없는 경우라면 Front Cache를 사용할 것을 권장한다.

ARCUS Java Client에서는 Transparent하게 Front Cache를 활성화할 수 있는 API를 제공하여, 
Remote Cache에서 Hit이 되었을 때 Front Cache에 데이터를 기록하거나
Front Cache로부터 먼저 조회 후 Remote Cache를 조회하는 Front cache 로직을 직접 작성할 필요가 없다.

Arcus Java Client는 내부적으로 Ehcache3 라이브러리를 사용해 데이터를 캐싱한다. 따라서 Front Cache를 사용할 때 아래 그림과 같은 형태로
데이터를 조회할 수 있게 된다. 데이터를 저장할 때에는 ARCUS와 Ehcache에 모두 저장된다.

![img.png](./images/java_client_ehcache.png)

## 주의 사항
- Transparent Front Cache는 현재 Key-Value get/set에 대해서만 적용 가능하다.
- Front cache는 remote ARCUS와 sync를 맞추지 않기 때문에 주로 read-only data를 caching하는데 적합하다.
  그리고 front caching expire time도 remote cache entry update주기에 따라 sync가 맞지 않는 기간을 잘 파악하여
  설정해야 한다.
- Front Cache에 저장된 데이터는 ArcusClient의 flush 메서드를 통해 flush 되지 않는다.
- 만약 하나의 애플리케이션에서 ARCUS의 front cache를 사용해야 하는 부분과 사용하지 않아야 하는 부분이 나뉜다면,
  각 용도에 맞는 ARCUS client 객체를 별도로 생성해 사용해야 한다.

## 사용법
Front cache를 적용하려면 ConnectionFactoryBuilder 객체를 생성하여 아래 메서드들을 호출한 후,
ArcusClient 객체 생성을 위한 정적 팩토리 메서드 인자로 넘겨주어야 한다.

- `setMaxFrontCacheElements(int to)` (Required)

  Front Cache에서 사용할 최대 아이템 개수를 지정한다.
  기본값은 0이며 Front Cache를 사용하지 않는다는 뜻이다.
  따라서 Front Cache를 사용하기 위해서는 반드시 양의 정수값을 지정해야 한다.
  만약 최대 Item 수를 초과하면 LRU 알고리즘을 통해 가장 사용되지 않는 Item을 제거하고 새로운 Item을 등록하게 된다.

- `setFrontCacheExpireTime(int to)` (Optional, default 5)

  Front Cache item의 expire time이다.
  Front cache는 item별 expire time을 설정하지 않고, 등록된 모든 item에 동일한 expire time이 적용된다.
  기본값은 5이며 단위는 second이다.
  설정하지 않는다면 기본값을 그대로 사용한다면 등록된 지 5초가 지나면 자동으로 사라지게 된다.

아래는 Front cache를 사용하기 위한 예시 코드이다.
setMaxFrontCacheElements을 0보다 큰 값으로 설정하면 Front Cache가 활성화된다.
setFrontCacheExpireTime은 필수 설정은 아니지만, 사용 용도에 맞도록 명시적인 값을 설정해 주는 것을 권장한다.

```java
ConnectionFactoryBuilder cfb = new ConnectionFactoryBuilder();

/* Required to use transparent front cache */
cfb.setMaxFrontCacheElements(10000);

/* Optional settings */
cfb.setFrontCacheExpireTime(5);

ArcusClient client = new ArcusClient(SERVICE_CODE, cfb);
```
