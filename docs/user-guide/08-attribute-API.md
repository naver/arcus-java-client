# 8. Item Attribute API

Item attributes는 각 cache item의 메타데이터를 의미한다.
Item attributes의 기본 설명은 [ARCUS cache server의 item attributes 부분](https://github.com/naver/arcus-memcached/blob/master/docs/ascii-protocol/ch03-item-attributes.md)을 참고 하길 바란다.

Item attributes를 활용하여 item을 생성하거나, Item attributes를 조회 또는 변경하는 함수들을 설명한다.

- [Attribute 생성](08-attribute-API.md#attribute-create)
- [Attribute 조회](08-attribute-API.md#attribute-get)
- [Attribute 변경](08-attribute-API.md#attribute-update)

<a id="attribute-create"></a>
## Attribute 생성

CollectionAttributes
- collection item 속성 구현체는 `CollectionAttributes`이며, 각 collection 유형의 item을 생성할 때 속성을 설정하기 위한 용도이다.   
- 기본 생성자 `new CollectionAttributes()`를 생성할 때 default value가 설정된다.
- 속성 설정을 위해 `setAttribute()`, `set()` API를 사용할 때, 설정하지 않은 속성들은 default value로 설정된다.

CollectionAttributes 객체를 사용하여 collection 생성 시에 속성을 지정하는 예는 아래와 같다.

 ```java
CollectionAttributes collectionAttributes = new CollectionAttributes();

//setAttribute(유형=값)으로 설정하며, 유형은 알파벳 소문자를 입력해야 한다.
collectionAttributes.setAttribute("expiretime=10");         // item의 만료는 10초 뒤에 이루어진다.
collectionAttributes.setAttribute("maxcount=1000");         // item의 최대 element 수는 1000개이다.
collectionAttributes.setAttribute("overflowaction=error");  // maxcount를 초과하여 element를 추가할 경우 element를 추가하지 않고, overflow 오류를 반환한다.

// 아래와 같이 set() API를 이용해 동일하게 각 속성을 설정할 수도 있다.
// set API를 이용시 compile time에 속성 값 검증이 가능하므로 set API를 이용한 속성 설정을 추천한다.
collectionAttributes.setExpireTime(10);
collectionAttributes.setMaxCount(1000);
collectionAttributes.setOverflowAction(CollectionOverflowAction.error);

//생성된 CollectionAttributes를 활용하여 Collection Item 생성
String key = "Sample:List"

try {
    client.asyncLopCreate(key, ElementValueType.OTHERS, collectionAttributes);
} catch (IllegalStateException e) {
    // handle exception
    return;
}
    
```
<a id="attribute-get"></a>
## Attribute 조회

주어진 key의 attributes를 조회하는 함수이다.

```java
CollectionFuture <CollectionAttributes> asyncGetAttr(String key)
```

Collection에 저장된 attributes를 조회하는 예는 아래와 같다.

```java
String key = "Sample:List";
CollectionFuture<CollectionAttributes> future = null;

try {
    future = client.asyncGetAttr(key); // (1)
} catch (IllegalStateException e) {
    // handle exception
    return;
}

try {
    CollectionAttributes result = future.get(1000L, TimeUnit.MILLISECONDS); // (2)

    if (result == null) { // (3)
        System.out.println("Key가 없습니다.");
        return;
    }
} catch (InterruptedException e) {
    future.cancel(true);
} catch (TimeoutException e) {
    future.cancel(true);
} catch (ExecutionException e) {
    future.cancel(true);
}

System.out.println("type: " + result.getType());
System.out.println("flags: " + result.getFlags());
System.out.println("expiretime: " + result.getExpireTime());
System.out.println("count: " + result.getCount());
System.out.println("maxcount: " + result.getMaxCount());
System.out.println("readable: " + result.getReadable());
System.out.println("overflowaction: " + result.getOverflowAction());
```
attributes 조회 결과는 아래와 같다. 
attribute 생성 부분에서 직접 설정해주지 않은 값들은 기본값으로 조회되었다.
```text
type: list
flags: 1
expiretime: 10
count: 0
maxcount: 1000
readable: true
overflowaction: error
```

1. Key의 Attribute를 조회한다.
2. timeout은 1초로 지정했다. 지정한 시간에 삭제 결과가 넘어오지 않거나 JVM의 과부하로 operation queue에서 처리되지 않을 땐 `TimeoutException`이 발생한다.
3. Key가 없으면 null이 반환된다.

<a id="attribute-update"></a>
## Attribute 변경

주어진 key의 attributes를 변경하는 함수이다.

```java
CollectionFuture <Boolean> asyncSetAttr(String key, Attributes attr)
```

어떤 collection에 모든 element를 삽입하기 전까지는 다른 여러 thread에서 
그 collection의 element를 조회하여서는 안 된다.
이를 위해, `readable` 속성을 false로 설정해 해당 collection을 읽을 수 없는 상태로 만들어두고
모든 element를 삽입한 후에 다시 `readable` 속성을 true로 설정하는 예는 아래와 같다.

```java
// Unreadable list를 생성한다.
CollectionAttributes attribute = new CollectionAttributes();
attribute.setReadable(false);

CollectionFuture<Boolean> createFuture = null

try {
    createFutre = client.asyncLopCreate(KEY, ElementValueType.STRING, attribute);
} catch (IllegalStateException e) {
    // handle exception
    return;    
}
        

try {
    createFuture.get(1000L, TimeUnit.MILLISECONDS);
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
    setAttrFuture.get(1000L, TimeUnit.MILLISECONDS);
} catch (Exception e) {
    setAttrFuture.cancel(true);
    // throw an exception or logging.
}

// 이제 collection을 읽을 수 있다.
```


두 번째 예제는 `expiretime` 속성을 변경하는 예제이다. 모든 item의 공통 속성인 `expireTime`을 설정할 시에는 `Attributes`를 사용하여 변경할 수 있다.
```java
String key = "Sample:Object";

CollectionFuture<Boolean> future = null;

try {
    Attributes attrs = new Attributes(); // (1)
    attrs.setExpireTime(1);
    future = client.asyncSetAttr(key, attrs); // (2)
} catch (IllegalStateException e) {
    // handle exception
    return;    
}


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

1. `Attributes` 객체를 생성하여 `expiretime`을 1초로 지정했다. 
2. `asyncSetAttr()` API를 사용해 key의 attribute를 변경한다.
   이렇게 하면 해당 key를 가진 item의 `expiretime`이 1초로 재설정 되며 1초 후에 item은 소멸된다.
3. Attribute가 key에 정상적으로 반영되면 true를 반환한다. 지정한 시간에 결과가 반환되지 않거나
   JVM의 과부하로 operation queue에서 처리되지 않을 때는 `TimeoutException`이 발생한다.






