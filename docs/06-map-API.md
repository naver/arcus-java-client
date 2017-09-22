## Map Item

Map item은 하나의 key에 대해 hash 구조 기반으로 mkey & value 쌍을 data 집합으로 가진다.
Map을 Java의 Map 자료형을 저장하는 용도로 사용하길 권장한다.

**제약 조건**
- 저장 가능한 최대 element 개수 : 디폴트 4,000개 (attribute 설정으로 최대 50,000개 확장 가능)
- 각 element에서 value 최대 크기 : 4KB
- mkey의 입력, Java map type에서 key는 string type만 가능하다. mkey 최대 길이는 250 바이트 이고, 하나의 map에 중복된 mkey는 허용하지 않는다.

Map item에 대해 수행가능한 기본 연산은 다음과 같다.

- [Map Item 생성](06-map-API.md#map-item-생성)
- [Map Element 삽입](06-map-API.md#map-element-삽입)
- [Map Element 변경](06-map-API.md#map-element-변경)
- [Map Element 삭제](06-map-API.md#map-element-삭제)
- [Map Element 조회](06-map-API.md#map-element-조회)

여러 map element들에 대해 한번에 일괄 수행하는 연산은 다음과 같다.

- [Map Element 일괄 삽입](06-map-API.md#map-element-일괄-삽입)
- [Map Element 일괄 변경](06-map-API.md#map-element-일괄-변경)

### Map Item 생성

새로운 empty map item을 생성한다.

```java
CollectionFuture<Boolean> asyncMopCreate(String key, ElementValueType valueType, CollectionAttributes attributes)
```

- key: 생성할 map item의 key 
- valueType: map에 저장할 value의 유형을 지정한다. 아래의 유형이 있다.
  - ElementValueType.STRING
  - ElementValueType.LONG
  - ElementValueType.INTEGER
  - ElementValueType.BOOLEAN
  - ElementValueType.DATE
  - ElementValueType.BYTE
  - ElementValueType.FLOAT
  - ElementValueType.DOUBLE
  - ElementValueType.BYTEARRAY
  - ElementValueType.OTHERS : for example, user defined class
- attributes: map item의 속성들을 지정한다.

수행 결과는 future 객체를 통해 얻는다.

future.get() | future.operationStatus().getResponse() | 설명 
------------ | -------------------------------------- | -------
True         | CollectionResponse.CREATED             | 생성 성공
False        | CollectionResponse.EXISTS              | 동일 key가 이미 존재함


Map item을 생성하는 예제는 아래와 같다.

```java
String key = "Sample:EmptyMap";
CollectionFuture<Boolean> future = null;
CollectionAttributes attribute = new CollectionAttributes(); // (1)
attribute.setExpireTime(60); // (1)

try {
    future = client.asyncMopCreate(key, ElementValueType.STRING, attribute); // (2)
} catch (IllegalStateException e) {
    // handle exception
}

if (future == null)
    return;

try {
    Boolean result = future.get(1000L, TimeUnit.MILLISECONDS); // (3)
    System.out.println(result);
    System.out.println(future.getOperationStatus().getResponse()); // (4)
} catch (TimeoutException e) {
    future.cancel(true);
} catch (InterruptedException e) {
    future.cancel(true);
} catch (ExecutionException e) {
    future.cancel(true);
}
```

1. Map의 expire time을 60초로 지정하였다.
   CollectionAttributes의 자세한 사용방법은 [Manual:Attribute_사용](08-attribute-API.md) 장에서 자세히 다룬다.
2. Empty map을 생성할 때에는 map에 어떤 타입의 element를 저장할 것인지를 미리 지정해 두어야 한다.
   이렇게 해야 하는 이유는 Java client에서 value를 encoding/decoding하는 메커니즘 때문이다.
   위 예제는 String 타입을 저장할 수 있는 empty map을 생성한다.
   만약에 empty map을 생성할 때 지정한 element type과 일치하지 않는 값을 map에 저장한다면
   저장은 성공하겠지만 조회할 때 엉뚱한 값이 조회된다.
3. timeout은 1초로 지정했다. 생성에 성공하면 future는 true를 반환한다.
   지정한 시간에 생성 결과가 넘어 오지 않거나 JVM의 과부하로 operation queue에서 처리되지 않을 경우
   TimeoutException이 발생한다.
4. 생성 결과에 대한 상세 정보는 future.getOperationStatus().getResponse()를 통해 조회 할 수 있다.


### Map Element 삽입

Map에 하나의 element를 삽입한다.

```java
CollectionFuture<Boolean> asyncMopInsert(String key, String mkey, Object value, CollectionAttributes attributesForCreate)
```

- key: 삽입 대상 map의 key
- mkey: 삽입할 element의 mkey
- value: 삽입할 element의 value
- attributesForCreate: 대상 map이 없을 시, 동작을 지정한다.
  - null: element 삽입하지 않는다. 
  - attributes: 주어진 attributes를 가진 empty map item 생성 후에 element 삽입한다.

수행 결과는 future 객체를 통해 얻는다.

future.get() | future.operationStatus().getResponse() | 설명 
------------ | -------------------------------------- | ---------
True         | CollectionResponse.STORED              | Map collection이 존재하여 element만 삽입함
True         | CollectionResponse.CREATED_STORED      | Map collection 생성하고 element를 삽입함
False        | CollectionResponse.NOT_FOUND           | Key miss (주어진 key에 해당하는 item이 없음)
False        | CollectionResponse.TYPE_MISMATCH       | 해당 item이 map이 아님
False        | CollectionResponse.ELEMENT_EXISTS      | 주어진 mkey를 가진 element가 이미 존재함
False        | CollectionResponse.OVERFLOWED          | 최대 저장가능한 개수만큼 element들이 존재함

Map element를 삽입하는 예제는 아래와 같다.

```java
String key = "Prefix:MapKey";
String mkey = "mkey";
String value = "This is a value.";

CollectionAttributes attributesForCreate = new CollectionAttributes();
CollectionFuture<Boolean> future = null;

try {
    future = client.asyncMopInsert(key, mkey, value, attributesForCreate); // (1)
} catch (IllegalStateException e) {
    // handle exception
}

if (future == null)
    return;

try {
    Boolean result = future.get(1000L, TimeUnit.MILLISECONDS); // (2)
    System.out.println(result);
    System.out.println(future.getOperationStatus().getResponse()); // (3)
} catch (TimeoutException e) {
    future.cancel(true);
} catch (InterruptedException e) {
    future.cancel(true);
} catch (ExecutionException e) {
    future.cancel(true);
}
```

1. attributesForCreate값이 null이 아니면 map이 존재하지 않을 때
   attributesForCreate속성을 가진 map을 새로 생성한 다음 element를 저장한다.
   만약 key가 존재하지 않는 상황에서 attributesForCreate값이 null이면 insert에 실패한다.
   - 위 예제는 디폴트 CollectionAttributes를 사용하며, 기본 expire time은 0으로 만료되지 않음을 뜻한다.
2. timeout은 1초로 지정했다. Insert가 성공하면 future는 true를 반환한다.
   지정한 시간에 insert 결과가 넘어 오지 않거나 JVM의 과부하로 operation queue에서 처리되지 않을 경우
   TimeoutException이 발생한다.
3. Insert결과에 대한 자세한 결과 코드를 확인하려면 future.getOperationStatus().getResponse()를 사용한다.

### Map Element 변경

Map에서 하나의 element를 변경하는 함수이다. Element의 value를 변경한다.

```java
CollectionFuture<Boolean> asyncMopUpdate(String key, String mkey, Object value)
```

- key: 변경 대상 map의 key
- mkey: 변경 대상 element의 mkey
- value: element의 new value

수행 결과는 future 객체를 통해 얻는다.

future.get() | future.operationStatus().getResponse() | 설명 
------------ | -------------------------------------- | ---------
True         | CollectionResponse.UPDATED             | Element가 변경됨
False        | CollectionResponse.NOT_FOUND           | Key miss (주어진 key에 해당하는 item이 없음)
False        | CollectionResponse.NOT_FOUND_ELEMENT   | 주어진 mkey를 가진 element가 없음
False        | CollectionResponse.TYPE_MISMATCH       | 해당 item이 map이 아님

특정 element의 value를 변경한다.

```java
CollectionFuture<Boolean> future = mc.asyncMopUpdate(key, mkey, value);
```

Element 수정에 대한 자세한 수행 결과는 future.getOperationStatus().getResponse()를 통해 조회할 수 있다.


### Map Element 삭제

Map에서 element를 삭제하는 함수들은 두 가지가 있다.

첫째, 해당 Map의 모든 element를 삭제한다.

```java
CollectionFuture<Boolean>
asyncMopDelete(String key, boolean dropIfEmpty)
```

둘째, Map에서 주어진 mkey의 element를 삭제한다.

```java
CollectionFuture<Boolean>
asyncMopDelete(String key, String mkey, boolean dropIfEmpty)
```

- key: 삭제 대상 map의 key
- dropIfEmpty: element 삭제로 empty map이 되면, 그 map 자체를 삭제할 지를 지정


수행 결과는 future 객체를 통해 얻는다.

future.get() | future.operationStatus().getResponse() | 설명 
------------ | -------------------------------------- | ---------
True         | CollectionResponse.DELETED             | Element만 삭제함
True         | CollectionResponse.DELETED_DROPPED     | Element 삭제하고 Map 자체도 삭제함
False        | CollectionResponse.NOT_FOUND           | Key miss (주어진 key에 해당하는 item이 없음)
False        | CollectionResponse.NOT_FOUND_ELEMENT   | 주어진 mkey를 가진 element가 없음
False        | CollectionResponse.TYPE_MISMATCH       | 해당 item이 map이 아님


다음은 map에서 mkey가 mkey1인 element를 삭제하는 예제이다.

```java
String key = "Prefix:MapKey";
String mkey1 = "mkey1";
boolean dropIfEmpty = true;
CollectionFuture<Boolean> future = null;

try {
    future = client.asyncMopDelete(key, mkey1, dropIfEmpty); // (1)
} catch (IllegalStateException e) {
    // handle exception
}

if (future == null)
    return;

try {
    boolean result = future.get(1000L, TimeUnit.MILLISECONDS); // (2)
    System.out.println(result);
    CollectionResponse response = future.getOperationStatus().getResponse(); // (3)
    System.out.println(response);
} catch (InterruptedException e) {
    future.cancel(true);
} catch (TimeoutException e) {
    future.cancel(true);
} catch (ExecutionException e) {
    future.cancel(true);
}
```

1. Map에서 mkey1에 해당하는 element 를 삭제한다.
   dropIfEmpty값이 true이면 element를 삭제하고 map이 비어있게 되었을 때 map도 함께 삭제한다.
2. delete timeout은 1초로 지정했다. 지정한 시간에 삭제 결과가 넘어 오지 않거나 JVM의 과부하로
   operation queue에서 처리되지 않을 경우 TimeoutException이 발생한다
3. 삭제 결과에 대한 상세 정보는 future.getOperationStatus().getResponse()를 통해 조회 할 수 있다.

## Map Element 조회

Map element를 조회하는 함수는 세 유형이 있다.

첫째, 해당 Map의 모든 element를 조회한다.

```java
CollectionFuture<Map<String, Object>>
asyncMopGet(String key, boolean withDelete, boolean dropIfEmpty)
```

둘째, 해당 Map에서 주어진 mkey 하나의 element를 조회한다.

```java
CollectionFuture<Map<String, Object>>
asyncMopGet(String key, String mkey, boolean withDelete, boolean dropIfEmpty)
```

셋째, Map에서 주어진 mkeyList의 element를 조회한다.

```java
CollectionFuture<Map<String, Object>>
asyncMopGet(String key, List<String> mkeyList, boolean withDelete, boolean dropIfEmpty)
```

- key: map item의 key
- mkey: 조회할 element의 mkey
- mkeyList: 조회할 element의 mkeyLists
- withDelete: element 조회와 함께 그 element를 삭제할 것인지를 지정
- dropIfEmpty: element 삭제로 empty map이 되면, 그 map 자체도 삭제할 것인지를 지정

수행 결과는 future 객체를 통해 얻는다.

future.get() | future.operationStatus().getResponse() | 설명 
------------ | -------------------------------------- | -------
not null     | CollectionResponse.END                 | Element만 조회
not null     | CollectionResponse.DELETED             | Element를 조회하고 삭제한 상태
not null     | CollectionResponse.DELETED_DROPPED     | Element를 조회하고 삭제한 다음 map을 drop한 상태
null         | CollectionResponse.NOT_FOUND           | Key miss (주어진 key에 해당하는 item이 없음)
null         | CollectionResponse.NOT_FOUND_ELEMENT   | 조회된 element가 없음, 조회 범위에 map 영역 없음
null         | CollectionResponse.TYPE_MISMATCH       | 해당 key가 map이 아님
null         | CollectionResponse.UNREADABLE          | 해당 key를 읽을 수 없는 상태임. (unreadable item상태)


Map element를 조회하는 예제는 아래와 같다.

```java
String key = "Prefix:MapKey";
List<String> mkeyList = new ArrayList<String>();
mkeyList.add("mkey1");
mkeyList.add("mkey2");
boolean withDelete = false;
boolean dropIfEmpty = false;
CollectionFuture<Map<String, Object>> future = null;

try {
    future = client.asyncMopGet(key, mkeyList, withDelete, dropIfEmpty); // (1)
} catch (IllegalStateException e) {
    // handle exception
}

if (future == null)
    return;

try {
    Map<String, Object> result = future.get(1000L, TimeUnit.MILLISECONDS); // (2)
    System.out.println(result);

    CollectionResponse response = future.getOperationStatus().getResponse(); // (3)
    if (response.equals(CollectionResponse.NOT_FOUND)) {
        System.out.println("Key가 없습니다.(Key에 저장된 Map이 없음.");
    } else if (response.equals(CollectionResponse.NOT_FOUND_ELEMENT)) {
        System.out.println("Key에 map은 존재하지만 저장된 값 중 조건에 맞는 것이 없음.");
    }

} catch (InterruptedException e) {
    future.cancel(true);
} catch (TimeoutException e) {
    future.cancel(true);
} catch (ExecutionException e) {
    future.cancel(true);
}
```

1. map에서 mkey1, mkey2를 한번에 조회하기 위해 List에 add하고 mkeyList를 조회했다.
2. timeout은 1초로 지정했다. 지정한 시간에 조회 결과가 넘어 오지 않거나
   JVM의 과부하로 operation queue에서 처리되지 않을 경우 TimeoutException이 발생한다.
   반환되는 Map 인터페이스의 구현체는 HashMap이며, 그 결과는 다음 중의 하나이다.
   - key 존재하지 않음 : null 반환
   - key 존재하지만 조회 조건을 만족하는 elements 없음: empty map 반환
   - key 존재하고 조회 조건을 만족하는 elements 있음: non-empty map 반환
3. 조회 결과에 대한 상세 정보는 future.getOperationStatus().getResponse()으로 확인한다.


## Map Element 일괄 삽입

Map에 여러 element를 한번에 삽입하는 함수는 두 유형이 있다.

첫째, 하나의 key가 가리키는 Map에 다수의 element를 삽입하는 함수이다.

```java
CollectionFuture<Map<Integer, CollectionOperationStatus>>
asyncMopPipedInsertBulk(String key, Map<String, Object> elements, CollectionAttributes attributesForCreate)
```

- key: 삽입 대상 map의 key 
- elements: 삽입할 element들
  - Map\<String, Object\> 유형
- attributesForCreate: 대상 map이 없을 시, 동작을 지정한다.
  - null: element 삽입하지 않는다. 
  - attributes: 주어진 attributes를 가진 empty map item 생성 후에 element 삽입한다.


둘째, 여러 key들이 가리키는 map들에 각각 하나의 element를 삽입하는 함수이다.

```java
Future<Map<String, CollectionOperationStatus>>
asyncMopInsertBulk(List<String> keyList, String mkey, Object value, CollectionAttributes attributesForCreate)
```

- keyList: 삽입 대상 map들의 key list
- mkey: 삽입할 element의 mkey
- value: 삽입할 element의 value
- attributesForCreate: 대상 map이 없을 시, 동작을 지정한다.
  - null: element 삽입하지 않는다. 
  - attributes: 주어진 attributes를 가진 empty map item 생성 후에 element 삽입한다.


하나의 map에 여러 개의 elements을 bulk insert하고 각각의 element에 대해 insert 결과를 확인하는 코드이다.

```java
String key = "Sample:MapBulk";
Map<String, Object> elements = new HashMap<String, Object>();

elements.put("mkey1", "value1");
elements.put("mkey2", "value2");
elements.put("mkey3", "value3");

boolean createKeyIfNotExists = true;

if (elements.size() > mc.getMaxPipedItemCount()) { // (1)
    System.out.println("insert 할 아이템 개수는 mc.getMaxPipedItemCount개를 초과할 수 없다.");
    return;
}

CollectionFuture<Map<Integer, CollectionOperationStatus>> future = null;

try {
    future = mc.asyncMopPipedInsertBulk(key, elements, new CollectionAttributes()); // (2)

} catch (IllegalStateException e) {
    // handle exception
}

if (future == null)
    return;

try {
    Map<Integer, CollectionOperationStatus> result = future.get(1000L, TimeUnit.MILLISECONDS); // (3)

    if (!result.isEmpty()) { // (4)
        System.out.println("일부 item이 insert 실패 하였음.");
        
        for (Map.Entry<Integer, CollectionOperationStatus> entry : result.entrySet()) {
            System.out.print("실패한 아이템=" + elements.get(entry.getKey()));
            System.out.println(", 실패원인=" + entry.getValue().getResponse());
        }
    } else {
        System.out.println("모두 insert 성공함.");
    }
} catch (TimeoutException e) {
    future.cancel(true);
} catch (InterruptedException e) {
    future.cancel(true);
} catch (ExecutionException e) {
    future.cancel(true);
}
```

1. 한꺼번에 insert할 아이템은 client.getMaxPipedItemCount()개를 초과할 수 없다. (기본값은 500개 이다.)
   만약 개수를 초과하면 IllegalArguementException이 발생한다.
2. Key에 저장된 map에 bulkData를 한꺼번에 insert하고 그 결과를 담은 future객체를 반환한다.
   이 future로부터 각 아이템의 insert성공 실패 여부를 조회할 수 있다.
   여기에서는 attributesForCreate값을 지정하여 bulk insert하기 전에 key가 없으면 생성하고 element를 insert되도록 하였다.
3. delete timeout은 1초로 지정했다. 지정한 시간에 모든 아이템의 insert 결과가 넘어 오지 않거나
   JVM의 과부하로 operation queue에서 처리되지 않을 경우 TimeoutException이 발생한다.
4. 모든 아이템이 insert에 성공하면 empty map이 반환된다.
   - 반환된 Map의 Key= insert한 값(bulkData)를 iteration했을 때의 index값.
   - 반환된 Map의 Value= insert실패사유
5. 일부 실패한 아이템의 실패 원인을 조회하려면 insert할 때 사용된 값(bulkData)의 iteration 순서에 따라
   결과 Map을 조회하면 된다.
6. Future로부터 얻은 Map의 Key가 입력된 값(bulkData)의 mapKey이기 때문에 위와 같은 방법으로 실패 원인을 조회하면 된다.


### Map Element 일괄 변경

Map에서 주어진 elements에 해당하는 모든 element의 value를 일괄 변경한다.

```java
CollectionFuture<Map<Integer, CollectionOperationStatus>>
asyncMopPipedUpdateBulk(String key, Map<String, Object>> elements)
```
- key: 변경 대상 map의 key
- elements: 변경 대상 map에 대해 mkey, new value를 가진다.
