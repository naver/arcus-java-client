## Item Attributes

Item attributes는 각 cache item의 메타데이터를 의미한다.
Item attributes의 기본 설명은 [Arcus cache server의 item attributes 부분](https://github.com/naver/arcus-memcached/blob/master/doc/arcus-item-attribute.md)을 참고 하길 바란다.

Item attributes를 변경하거나 조회하는 함수들을 설명한다.

- [Attribute 변경](08-attribute-API.md#attribute-%EB%B3%80%EA%B2%BD)
- [Attribute 조회](08-attribute-API.md#attribute-%EC%A1%B0%ED%9A%8C)


### Attribute 변경

주어진 key의 attributes를 변경하는 함수이다.

```java
CollectionFuture <Boolean> asyncSetAttr(String key, Attributes attr)
```

어떤 collection에 모든 element를 삽입하기 전 까지는 다른 여러 thread에서 
그 collection의 element를 조회하여서는 안된다.
이를 위해, READABLE 속성을 false로 설정해 해당 collection을 읽을 수 없는 상태로 만들어두고
모든 element를 삽입한 후에 다시 readable 속성을 true하는 예제이다.

```java
// Unreadable list를 생성한다.
CollectionAttributes attribute = new CollectionAttributes();
attribute.setReadable(false);

CollectionFuture<Boolean> createFuture = mc.asyncLopCreate(KEY, ElementValueType.STRING, attribute);

try {
    createFuture.get(300L, TimeUnit.MILLISECONDS);
} catch (Exception e) {
    createFuture.cancel(true);
    // throw an exception or logging.
}

// 여기에서 List를 갱신한다. 이 상태에서는 collection을 읽을 수 없다. 쓰기, 수정, 삭제만 가능하다.

// List를 Readable상태로 만든다.
CollectionAttributes attrs = new CollectionAttributes();
attrs.setReadable(true);

CollectionFuture<Boolean> setAttrFuture = mc.asyncSetAttr(KEY, attrs);
try {
    setAttrFuture.get(300L, TimeUnit.MILLISECONDS);
} catch (Exception e) {
    setAttrFuture.cancel(true);
    // throw an exception or logging.
}

// 이제 collection을 읽을 수 있다.
```


두번째 예제는 expiretime 속성을 변경한다.

```java
String key = "Sample:Object";

CollectionFuture<Boolean> future = null;

try {
    Attributes attrs = new Attributes(); // (1)
    attrs.setExpireTime(1);
    future = client.asyncSetAttr(key, attrs); // (2)
} catch (IllegalStateException e) {
    // handle exception
}

if (future == null)
    return;

try {
    Boolean result = future.get(1000L, TimeUnit.MILLISECONDS); // (3)
    System.out.println(result);
} catch (TimeoutException e) {
    future.cancel(true);
} catch (InterruptedException e) {
    future.cancel(true);
} catch (ExecutionException e) {
    future.cancel(true);
}
```

1. Attribute객체를 생성하여 expire time을 1초로 지정했다. 지정한 시간에 결과가 넘어 오지 않거나
   JVM의 과부하로 operation queue에서 처리되지 않을 경우 TimeoutException이 발생한다
2. asyncSetAttr메소드를 사용해 key의 attribute를 변경한다.
   이렇게 하면 key의 expire time이 attribute에 지정된 1초 이후로 재설정 된다.
   결론적으로 key의 expire time은 asyncSetAttr메소드를 통해 Attribute가 적용된 1초 후로 설정되는 것이다.
3. Attribute가 key에 정상적으로 반영되면 true를 반환한다.


### Attribute 조회

주어진 key의 attributes를 조회하는 함수이다.

```java
CollectionFuture <CollectionAttributes> asyncGetAttr(String key)
```

Colleciton에 저장된 element 개수를 조회하는 예제이다.

```java
String key = "Sample:List";
CollectionFuture<CollectionAttributes> future = null;

try {
    future = client.asyncGetAttr(key); // (1)
} catch (IllegalStateException e) {
    // handle exception
}

if (future == null) {
    return;
}

try {
    CollectionAttributes result = future.get(1000L, TimeUnit.MILLISECONDS); // (2)

    if (result == null) { // (3)
        System.out.println("Key가 없습니다.");
        return;
    }

    long totalItemCountOfBTree = result.getCount(); // (4)
    System.out.println("Item count=" + totalItemCountOfBTree);
} catch (InterruptedException e) {
    future.cancel(true);
} catch (TimeoutException e) {
    future.cancel(true);
} catch (ExecutionException e) {
    future.cancel(true);
}
```

1. Key의 Attribute를 조회한다.
2. timeout은 1초로 지정했다. 지정한 시간에 삭제 결과가 넘어 오지 않거나 JVM의 과부하로 operation queue에서 처리되지 않을 경우 TimeoutException이 발생한다.
3. Key가 없으면 null이 반환된다.
4. 조회된 Attribute객체에서 count값을 조회한다. 이 값이 key에 저장된 Collection이 가지고 있는 엘리먼트의 총 개수이다.

