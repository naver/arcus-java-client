## List Item

List collection은 하나의 key에 대해 여러 value들을 double linked list 구조로 유지한다.


**제약 조건**
- 저장 가능한 최대 element 개수 : 디폴트 4,000개 (attribute 설정으로 최대 50,000개 확장 가능)
- 각 element에서 value 최대 크기 : 4KB
- List의 앞, 뒤에서 element를 삽입/삭제하기를 권한다. 임의의 index 위치에서 element 삽입/삭제가 가능하지만,
  임의의 index 위치를 신속히 찾아가기 위한 자료구조가 현재 없는 상태라서 비용이 많이 든다.

List item에 대해 수행가능한 기본 연산들은 아래와 같다.

- [List Item 생성](04-list-API.md#list-item-%EC%83%9D%EC%84%B1) (List Item 삭제는 key-value item 삭제 함수로 수행한다)
- [List Element 삽입](04-list-API.md#list-element-%EC%82%BD%EC%9E%85)
- [List Element 삭제](04-list-API.md#list-element-%EC%82%AD%EC%A0%9C) 
- [List Element 조회](04-list-API.md#list-element-%EC%A1%B0%ED%9A%8C)

여러 list element들에 대해 한번에 일괄 수행하는 연산은 다음과 같다.

- [List Element 일괄 삽입](04-list-API.md#list-element-%EC%9D%BC%EA%B4%84-%EC%82%BD%EC%9E%85)


### List Item 생성

새로운 empty list item을 생성한다.

```java
CollectionFuture<Boolean> asyncLopCreate(String key, ElementValueType valueType, CollectionAttributes attributes)
```

- key: 생성할 list의 key
- valueType: list에 저장할 value의 유형을 지정한다. 아래의 유형이 있다.
  - ElementValueType.STRING - String
  - ElementValueType.LONG - Long
  - ElementValueType.INTEGER - Integer
  - ElementValueType.BOOLEAN - Boolean
  - ElementValueType.DATE - Date
  - ElementValueType.BYTE - Byte
  - ElementValueType.FLOAT - Float
  - ElementValueType.DOUBLE - Double
  - ElementValueType.BYTEARRAY - Byte array
  - ElementValueType.OTHERS - 위에 나열한 타입을 제외한 모든 타입
- attributes: list item의 속성들을 지정한다.

수행 결과는 future 객체를 통해 얻는다.

future.get() | future.operationStatus().getResponse() | 설명 
------------ | -------------------------------------- | -------
True         | CollectionResponse.CREATED             | 생성 성공
False        | CollectionResponse.EXISTS              | 동일 key 가 이미 존재함


List item을 생성하는 예제는 아래와 같다.

```java
String key = "Sample:EmptyList";

CollectionFuture<Boolean> future = null;
CollectionAttributes attribute = new CollectionAttributes();

try {
    future = client.asyncLopCreate(key, ElementValueType.OTHERS, attribute); // (1)
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

1. Empty list를 생성할 때에는 list안에 어떤 타입의 element를 저장할 것인지를 미리 지정해 두어야 한다.
   예제에서는 ElementValueType을 통해 지정할 수 있는 타입을 제외한 나머지들을 저장할 수 있는 empty list를 생성한다.
2. timeout은 1초로 지정했다. 생성에 성공하면 future는 true를 반환한다.
   지정한 시간에 생성 결과가 넘어 오지 않거나 JVM의 과부하로 operation queue에서 처리되지 않을 경우
   TimeoutException이 발생한다.
3. 생성 결과에 대한 상세 정보는 future.getOperationStatus().getResponse()를 통해 조회할 수 있다.  
   

### List Element 삽입

List에 하나의 element를 삽입하는 함수이다.

```java
CollectionFuture<Boolean> asyncLopInsert(String key, int index, Object value, CollectionAttributes attributesForCreate)
```

List에 새로운 element를 삽입한다.

- key: 삽입 대상 list의 key
- index: 삽입 위치로 0-based index로 지정
  - 0, 1, 2, ... : list의 앞에서 시작하여 각 element 위치를 나타냄
  - -1, -2, -3, ... : list의 뒤에서 시작하여 각 element 위치를 나타냄 
- value: 삽입할 element의 value
- attributesForCreate: 대상 list가 존재하지 않을 시의 동작을 지정한다.
  - null: element 삽입하지 않는다. 
  - attributes: 주어진 attributes를 가진 empty list item 생성 후에 element 삽입한다.


수행 결과는 future 객체를 통해 얻는다.

future.get() | future.operationStatus().getResponse() | 설명 
------------ | -------------------------------------- | -------
True         | CollectionResponse.STORED              | List collection이 존재하여 element 만 삽입됨
True         | CollectionResponse.CREATED_STORED      | List collection이 create되고 element가 삽입됨
False        | CollectionResponse.NOT_FOUND           | Key miss (주어진 key에 해당하는 item이 없음)
Fasle        | CollectionResponse.TYPE_MISMATCH       | 해당 key가 list가 아님
False        | CollectionResponse.OVERFLOWED          | Overflow 상태임
False        | CollectionResponse.OUT_OF_RANGE        | 삽입 위치가 list의 element index 범위를 넘어섬
             

List element를 삽입하는 예제는 아래와 같다.

```java
String key = "Sample:List";
int index = -1;
String value = "This is a value.";
CollectionAttributes attributesForCreate = new CollectionAttributes();
CollectionFuture<Boolean> future = null;

try {
    future = client.asyncLopInsert(key, index, value, attributesForCreate); // (1)
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

1. attributesForCreate값이 null이 아니면 key가 존재하지 않을 때
   attributesForCreate 속성을 가지는 list를 새로 생성한 다음 element를 삽입한다.
   만약 attributesForCreate값이 null이고 key가 존재하지 않는다면 element는 insert되지 않는다.
   - 이 예제는 특별한 설정을 하지 않은 CollectionAttributes를 사용하며 기본 expire time은 0으로 만료되지 않음을 뜻한다.
   - 참고로 이미 key가 존재하는 상태에서 value를 저장한다 하더라도 key에 설정된 expire time은 변하지 않는다.
     다시 말해 value가 추가되어도 expire time은 변경되거나 연장되지 않는다.
2. timeout은 1초로 지정했다. 삽입에 성공하면 future는 true를 반환한다.
   지정한 시간에 삽입 결과가 넘어 오지 않거나 JVM의 과부하로 operation queue에서 처리되지 않을 경우
   TimeoutException이 발생한다.
3. 삽입 결과에 대한 상세 정보는 future.getOperationStatus().getResponse()를 통해 조회할 수 있다.


## List Element 삭제

List에서 index 위치에 있는 하나의 element 또는 index range에 포함되는 다수 element를 삭제하는 함수는 아래와 같다.

```java
CollectionFuture<Boolean> asyncLopDelete(String key, int index, Boolean dropIfEmpty)
CollectionFuture<Boolean> asyncLopDelete(String key, int from, int to, boolean dropIfEmpty)
```

- key: 삭제 대상 list의 key
- index or index range(from, to): 삭제 위치로 0-based index로 지정
  - 0, 1, 2, ... : list의 앞에서 시작하여 각 element 위치를 나타냄
  - -1, -2, -3, ... : list의 뒤에서 시작하여 각 element 위치를 나타냄
- dropIfEmpty: element 삭제로 인해 empty list가 될 경우, 그 list도 삭제할 것인지를 지정

수행 결과는 future 객체를 통해 얻는다.

future.get() | future.operationStatus().getResponse() | 설명 
------------ | -------------------------------------- | -------
True         | CollectionResponse.DELETED             | List에서 element만 삭제됨
True         | CollectionResponse.DELETED_DROPPED     | List에서 element 삭제 후, empty list가 되어서 그 list도 삭제함
False        | CollectionResponse.NOT_FOUND           | Key miss (주어진 key에 해당하는 item이 없음)
False        | CollectionResponse.TYPE_MISMATCH       | 해당 key가 list가 아님
False        | CollectionResponse.NOT_FOUND_ELEMENT   | List 는 있지만 조건에 맞는 element가 없음


List에서 index가 0부터 10까지의 element를 삭제하는 예제이다.

```java
String key = "Sample:List";
int from = 0;
int to = 10;
boolean dropIfEmpty = false;
CollectionFuture<Boolean> future = null;

try {
    future = client.asyncLopDelete(key, from, to, dropIfEmpty); // (1)
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

1. List에서 index가 from부터 to사이에 있는 element를 삭제한다.
   dropIfEmpty값이 true이면 element가 삭제된 다음 List가 비어있게 되면 List도 함께 삭제한다.
2. timeout은 1초로 지정했다.
   지정한 시간에 삭제 결과가 넘어 오지 않거나 JVM의 과부하로 operation queue에서 처리되지 않을 경우,
   TimeoutException이 발생한다.
3. 정상적으로 삭제되면 true를 반환한다.
   삭제 결과에 따른 반환 값은 future.operationStatus().getResponse()로 확인한다.

## List Element 조회

List 에서 하나의 index 또는 index range에 해당하는 element를 조회한다.
만약, 해당 element가 존재하지 않으면 null이 반환된다.

```java
CollectionFuture<List<Object>> asyncLopGet(String key, int index, boolean withDelete, boolean dropIfEmpty)
CollectionFuture<List<Object>> asyncLopGet(String key, int from, int to, boolean withDelete, boolean dropIfEmpty)
```

- key: 조회 대상 list의 key
- index or index range(from, to): 조회 위치로 0-based index로 지정
  - 0, 1, 2, ... : list의 앞에서 시작하여 각 element 위치를 나타냄
  - -1, -2, -3, ... : list의 뒤에서 시작하여 각 element 위치를 나타냄
- withDelete: element 조회와 함께 그 element를 삭제할 것인지를 지정
- dropIfEmpty: element 삭제로 인해 empty list가 될 경우, 그 list도 삭제할 것인지를 지정

수행 결과는 future 객체를 통해 얻는다.

future.get() | future.operationStatus().getResponse() | 설명 
------------ | -------------------------------------- | -------
not null     | CollectionResponse.END                 | Element를 조회만 한 상태
not null     | CollectionResponse.DELETED             | Element를 조회하고 삭제한 상태
not null     | CollectionResponse.DELETED_DROPPED     | Element를 조회하고 삭제한 다음 list를 drop(delete)한 상태
null         | CollectionResponse.NOT_FOUND           | Key miss (주어진 key에 해당하는 item이 없음)
null         | CollectionResponse.TYPE_MISMATCH       | 해당 key가 list가 아님
null         | CollectionResponse.UNREADABLE          | 해당 key를 읽을 수 없는 상태임. (unreadable item상태)
null         | CollectionResponse.OUT_OF_RANGE        | Index가 list의 element index범위를 벗어남

List element를 조회하는 예제는 아래와 같다.

```java
String key = "Sample:List";
int from = 0;
int to = 5;
boolean withDelete = false;
boolean dropIfEmpty = false;
CollectionFuture<List<Object>> future = null;

try {
    future = client.asyncLopGet(key, from, to, withDelete, dropIfEmpty); // (1)
} catch (IllegalStateException e) {
    // handle exception
}

if (future == null)
    return;

try {
    List<Object> result = future.get(1000L, TimeUnit.MILLISECONDS); // (2)
    System.out.println(result);
    CollectionResponse response = future.getOperationStatus().getResponse();  // (3)
    System.out.println(response);

    if (response.equals(CollectionResponse.NOT_FOUND)) {
        System.out.println("Key가 없습니다.(Key에 저장된 List가 없습니다.");
    } else if (response.equals(CollectionResponse.NOT_FOUND_ELEMENT)) {
        System.out.println("Key는 존재하지만 List에 저장된 값 중 조건에 맞는 것이 없습니다.");
    }

} catch (InterruptedException e) {
    future.cancel(true);
} catch (TimeoutException e) {
    future.cancel(true);
} catch (ExecutionException e) {
    future.cancel(true);
}
```

1. index가 0부터 5사이에 있는 element들을 조회한다.
   - withDelete값이 true이면 조회와 동시에 list collection에서 element를 삭제한다.
   - dropIfEmpty값이 true이면 element를 삭제한 다음 list collection이 비어있게 되면 list도 함께 삭제한다.
2. timeout은 1초로 지정했다. 
   지정한 시간에 조회 결과가 넘어 오지 않거나 JVM의 과부하로 operation queue에서 처리되지 않을 경우,
   TimeoutException이 발생한다.
   future.get()의 반환 결과는 다음과 같다.
   - key가 없는 경우, null을 반환한다.
   - key는 있지만 조건(index)에 맞는 element가 없는 경우, empty list를 반환
   - key가 존재하고 일부 element만 조건을 만족하는 경우, 조건에 맞는 element만 반환
3. 조회 결과에 대한 상세 정보는 future.getOperationStatus().getResponse()를 통해 조회 할 수 있다.


### List Element 일괄 삽입

두 유형의 bulk 삽입 기능을 제공한다.

첫째, 하나의 key가 가리키는 list에 다수의 element를 삽입하는 함수이다.

```java
CollectionFuture <Map<Integer, CollectionOperationStatus>>
asyncLopPipedInsertBulk(String key, int index, List<Object> valueList, CollectionAttributes attributesForCreate)
```
- key: 삽입 대상 list의 key 
- index: 삽입 위치로 0-based index로 지정
  - -1이면 list의 제일 뒤에
  - 0이면 list의 제일 앞에 삽입한다.
- valueList: 삽입할 element들의 value list 
- attributesForCreate: 대상 list가 존재하지 않을 시의 동작을 지정한다.
  - null: element 삽입하지 않는다. 
  - attributes: 주어진 attributes를 가진 empty list item 생성 후에 element 삽입한다.

둘째, 여러 key들이 가리키는 list들에 각각 동일한 하나의 element를 삽입하는 함수이다.

```java
Future <Map<String, CollectionOperationStatus>>
asyncLopInsertBulk(List<String> keyList, int index, Object value, CollectionAttributes attributesForCreate)
```

keyList로 지정된 모든 key에 대해 하나의 element를 삽입니다.

- key: 삽입 대상 list들의 key list
- index: 삽입 위치로 0-based index로 지정
  - -1이면 list의 제일 뒤에
  - 0이면 list의 제일 앞에 삽입한다.
- value: 삽입할 element의 value
- attributesForCreate: 대상 list가 존재하지 않을 시의 동작을 지정한다.
  - null: element 삽입하지 않는다. 
  - attributes: 주어진 attributes를 가진 empty list item 생성 후에 element 삽입한다.


아래는 하나의 List에 여러 element를 bulk insert하고 각각의 item에 대해 insert 결과를 확인하는 코드이다.

```java
String key = "Sample:ListBulk";
List<Object> bulkData = getBulkData();
int index = -1; // List의 제일 뒤에 insert한다.
CollectionAttributes attributesForCreate = new CollectionAttributes();
if (bulkData.size() > client.getMaxPipedItemCount()) { // (1)
    System.out.println("insert 할 아이템 개수는 client.getMaxPipedItemCount개를 초과할 수 없다.");
    return;
}

CollectionFuture<Map<Integer, CollectionOperationStatus>> future = null;

try {
    future = client.asyncLopPipedInsertBulk(key, index, bulkData, attributesForCreate); // (2)
} catch (Exception e) {
    // handle exception
}

if (future == null)
    return;

try {
    Map<Integer, CollectionOperationStatus> result = future.get(1000L, TimeUnit.MILLISECONDS); // (3)
    if (!result.isEmpty()) { // (4)
        System.out.println("일부 item이 insert 실패 하였음.");
        for (Entry<Integer, CollectionOperationStatus> entry : result.entrySet()) { // (5)
            System.out.print("실패한 아이템=" + bulkData.get(entry.getKey()));
            System.out.println(", 실패원인=" + entry.getValue().getResponse()); // (6)
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
2. Key에 저장된 List에 bulkData를 한꺼번에 insert하고 그 결과를 담은 future객체를 반환한다.
   이 future로부터 각 아이템의 insert성공 실패 여부를 조회할 수 있다.
   여기에서는 attributesForCreate값을 지정하여 bulk insert하기 전에 key가 없으면 생성하고 element를 insert되도록 하였다.
3. delete timeout은 1초로 지정했다.
   지정한 시간에 모든 아이템의 insert 결과가 넘어 오지 않거나 JVM의 과부하로 operation queue에서 처리되지 않을 경우
   TimeoutException이 발생한다.
4. 모든 아이템이 insert에 성공하면 empty map이 반환된다.
   - 반환된 Map의 Key= insert한 값(bulkData)를 iteration했을 때의 index값.
   - 반환된 Map의 Value= insert 실패사유
5. 일부 실패한 아이템의 실패 원인을 조회하려면 insert할 때 사용된 값(bulkData)의 iteration 순서에 따라 결과
   Map을 조회하면 된다.
6. Future로부터 얻은 Map의 Key가 입력된 값(bulkData)의 index이기 때문에 위와 같은 방법으로 실패 원인을 조회하면 된다.

