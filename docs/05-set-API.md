## Set Item

Set item은 하나의 key에 대해 unique value의 집합을 저장한다.
주로 membership checking에 유용하게 사용할 수 있다.

**제약 조건**
- 저장 가능한 최대 element 개수 : 디폴트 4,000개 (attribute 설정으로 최대 50,000개 확장 가능)
- 각 element에서 value 최대 크기 : 16KB
- Element 값의 중복을 허용하지 않는다.

Set item에 수행가능한 기본 연산들은 다음과 같다.

- [Set Item 생성](05-set-API.md#set-item-%EC%83%9D%EC%84%B1) (Set item 삭제는 key-value item 삭제 함수로 수행한다) 
- [Set Element 삽입](05-set-API.md#set-element-%EC%82%BD%EC%9E%85)
- [Set Element 삭제](05-set-API.md#set-element-%EC%82%AD%EC%A0%9C)
- [Set Element 존재유무 확인](05-set-API.md#set-element-%EC%A1%B4%EC%9E%AC%EC%9C%A0%EB%AC%B4-%ED%99%95%EC%9D%B8)
- [Set Element 조회](05-set-API.md#set-element-%EC%A1%B0%ED%9A%8C)

여러 set element들에 대해 한번에 일괄 수행하는 연산은 다음과 같다.

- [Set Element 일괄 삽입](05-set-API.md#set-element-%EC%9D%BC%EA%B4%84-%EC%82%BD%EC%9E%85)
- [Set Element 일괄 존재유무 확인](05-set-API.md#set-element-%EC%9D%BC%EA%B4%84-%EC%A1%B4%EC%9E%AC%EC%97%AC%EB%B6%80-%ED%99%95%EC%9D%B8)


### Set Item 생성

새로운 empty set item을 생성한다.

```java
CollectionFuture<Boolean> asyncSopCreate(String key, ElementValueType valueType, CollectionAttributes attributes)
```

- key: 생성할 set의 key
- valueType: set에 저장할 value의 유형을 지정한다. 아래의 유형이 있다.
  - ElementValueType.STRING
  - ElementValueType.LONG
  - ElementValueType.INTEGER
  - ElementValueType.BOOLEAN
  - ElementValueType.DATE
  - ElementValueType.BYTE
  - ElementValueType.FLOAT
  - ElementValueType.DOUBLE
  - ElementValueType.BYTEARRAY
  - ElementValueType.OTHERS
- attributes: set item의 속성들을 지정한다.


수행 결과는 future 객체를 통해 얻는다.

future.get() | future.operationStatus().getResponse() | 설명 
------------ | -------------------------------------- | -------
True         | CollectionResponse.CREATED             | 생성 성공
False        | CollectionResponse.EXISTS              | 동일 key 가 이미 존재함


Set item을 생성하는 예제는 아래와 같다.

```java
String key = "Sample:EmptySet";
CollectionFuture<Boolean> future = null;
CollectionAttributes attribute = new CollectionAttributes();
try {
	future = client.asyncSopCreate(key, ElementValueType.OTHERS,
			attribute); // (1)
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

1. Empty set을 생성할 때에는 set에 어떤 타입의 element를 저장할 것인지를 미리 지정해 주어야 한다.
   예제에서는 ElementValueType을 통해 지정할 수 있는 타입을 제외한 나머지들을 저장할 수 있는 empty set을 생성한다.
2. timeout은 1초로 지정했다. 생성에 성공하면 future는 true를 반환한다.
   지정한 시간에 생성 결과가 넘어 오지 않거나 JVM의 과부하로 operation queue에서 처리되지 않을 경우
   TimeoutException이 발생한다.
3. 생성 결과에 대한 상세 정보는 future.getOperationStatus().getResponse()를 통해 조회할 수 있다.  


### Set Element 삽입

Set에 하나의 element를 삽입하는 함수이다.

```java
CollectionFuture<Boolean> asyncSopInsert(String key, Object value, CollectionAttributes attributesForCreate)
```

- key: 삽입 대상 set의 key
- value: 삽입할 element의 value
- attributesForCreate: 대상 set이 존재하지 않을 시의 동작을 지정한다.
  - null: element 삽입하지 않는다. 
  - attributes: 주어진 attributes를 가진 empty set item 생성 후에 element 삽입한다.

수행 결과는 future 객체를 통해 얻는다.

future.get() | future.operationStatus().getResponse() | 설명 
------------ | -------------------------------------- | -------
True         | CollectionResponse.STORED              | Set collection이 존재하여 element 만 삽입됨
True         | CollectionResponse.CREATED_STORED      | Set collection이 create되고 element가 삽입됨
False        | CollectionResponse.NOT_FOUND           | Key miss (주어진 key에 해당하는 item이 없음)
False        | CollectionResponse.TYPE_MISMATCH       | 해당 item이 set이 아님
False        | CollectionResponse.OVERFLOWED          | Overflow 상태임
False        | CollectionResponse.ELEMENT_EXISTS      | 동일한 값을 가진 element가 set에 존재함
             

Set에 하나의 element를 삽입하는 예제는 아래와 같다.

```java
String key = "Sample:Set";
String value = "This is a value.";
CollectionAttributes attributesForCreate = new CollectionAttributes();
CollectionFuture<Boolean> future = null;

try {
    future = client.asyncSopInsert(key, value, attributesForCreate); // (1)
} catch (IllegalStateException e) {
    // handle exception
}

if (future == null)
    return;

try {
    Boolean result = future.get(1000L, TimeUnit.MILLISECONDS); // (2)
    System.out.println(result); // (3)
    System.out.println(future.getOperationStatus().getResponse()); // (4)
} catch (TimeoutException e) {
    future.cancel(true);
} catch (InterruptedException e) {
    future.cancel(true);
} catch (ExecutionException e) {
    future.cancel(true);
}
```

1. attributesForCreate값이 null이 아니면 key가 존재하지 않을 때
   attributesForCreate 속성을 가진 set을 새로 생성한 다음 element를 삽입한다.
   만약 attributesForCreate값이 null이고 key가 존재하지 않는다면 element는 insert되지 않는다.
   - 특별한 설정을 하지 않은 CollectionAttributes를 사용하며 기본 expire time은 0으로 만료되지 않음을 뜻한다.
   - 참고로 이미 key가 존재하는 상태에서 value를 저장한다 하더라도 key에 설정된 expire time은 변하지 않는다.
     다시 말해 value가 추가되어도 expire time은 변경되거나 연장되지 않는다.
2. timeout은 1초로 지정했다. 삽입에 성공하면 future는 true를 반환한다.
   지정한 시간에 삽입 결과가 넘어 오지 않거나 JVM의 과부하로 operation queue에서 처리되지 않을 경우
   TimeoutException이 발생한다.
3. Set은 값의 중복을 허용하지 않는다. 중복 값의 유무에 따라 반환 값은 다음과 같이 달라진다.
4. 삽입 결과에 대한 상세 정보는 future.getOperationStatus().getResponse()를 통해 조회할 수 있다.


### Set Element 삭제

Set에서 주어진 value를 가진 element를 삭제하는 함수이다.

```java
CollectionFuture<Boolean> asyncSopDelete(String key, Object value, boolean dropIfEmpty)
```

- key: 삭제 대상 set의 key
- value: 삭제할 element 값
- dropIfEmpty: element 삭제로 인해 empty set이 될 경우, 그 set도 삭제할 것인지를 지정

수행 결과는 future 객체를 통해 얻는다.

future.get() | future.operationStatus().getResponse() | 설명 
------------ | -------------------------------------- | -------
True         | CollectionResponse.DELETED             | Set에서 element만 삭제됨
True         | CollectionResponse.DELETED_DROPPED     | Set에서 element 삭제 후, empty set이 되어서 그 set도 삭제함
False        | CollectionResponse.NOT_FOUND           | Key miss (주어진 key에 해당하는 item이 없음)
False        | CollectionResponse.TYPE_MISMATCH       | 해당 key가 set이 아님
False        | CollectionResponse.NOT_FOUND_ELEMENT   | Key는 있지만 주어진 value를 가진 element가 없음

Set에서 하나의 element를 삭제하는 예제이다.

```java
String key = "Sample:Set";
String value = "This is a value.";
boolean dropIfEmpty = false;
CollectionFuture<Boolean> future = null;

try {
    future = client.asyncSopDelete(key, value, dropIfEmpty); // (1)
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

1. dropIfEmpty값이 true이면 element를 삭제한 후 set이 비어있게 될 때 key도 함께 삭제한다.
2. timeout은 1초로 지정했다. 지정한 시간에 삭제 결과가 넘어 오지 않거나
   JVM의 과부하로 operation queue에서 처리되지 않을 경우 TimeoutException이 발생한다.
3. 정상적으로 삭제되면 true를 반환한다. 자세한 삭제 결과는 future.operationStatus().getResponse() 로 확인 할 수 있다.


### Set Element 존재유무 확인

Set에서 주어진 value를 가진 element의 존재유무를 확인한다.

```java
CollectionFuture<Boolean> asyncSopExist(String key, Object value)
```

- key: 조회 대상 set의 key
- value: 존재유무를 확인할 value


수행 결과는 future 객체를 통해 얻는다.

future.get() | future.operationStatus().getResponse() | 설명 
------------ | -------------------------------------- | -------
True         | CollectionResponse.EXIST               | Element가 존재함
True         | CollectionResponse.NOT_EXIST           | Element가 존재하지 않음
False        | CollectionResponse.NOT_FOUND           | Key miss (주어진 key에 해당하는 item이 없음)
False        | CollectionResponse.TYPE_MISMATCH       | 해당 key가 set이 아님
False        | CollectionResponse.UNREADABLE          | 해당 key를 읽을 수 없는 상태임. (unreadable item상태)


Set element의 존재여부를 확인하는 예제는 아래와 같다.

```java
String key = "Sample:Set";
String value = "This is a value.";
CollectionFuture<Boolean> future = null;

try {
    future = client.asyncSopExist(key, value); // (1)
} catch (IllegalStateException e) {
     // handle exception
}

if (future == null)
    return;

try {
    Boolean result = future.get(1000L, TimeUnit.MILLISECONDS); // (2)
    System.out.println(result); // (2)

    CollectionResponse response = future.getOperationStatus().getResponse(); // (3)
    System.out.println(response);

    if (response.equals(CollectionResponse.NOT_FOUND)) {
        System.out.println("Key가 없습니다.(Key에 저장된 Set이 없습니다.");
    } else if (response.equals(CollectionResponse.NOT_EXIST)) {
        System.out.println("Key는 존재하지만 Set에 요청한 값이 없습니다.");
    }
} catch (InterruptedException e) {
    future.cancel(true);
} catch (TimeoutException e) {
    future.cancel(true);
} catch (ExecutionException e) {
    future.cancel(true);
}
```

1. Key에 저장된 set에 value가 존재하는지 확인한다.
2. timeout은 1초로 지정했다. 값이 존재하면 future는 true를 반환한다.
   지정한 시간에 결과가 넘어 오지 않거나 JVM의 과부하로 operation queue에서 처리되지 않을 경우
   TimeoutException이 발생한다.
3. 조회 결과에 관한 자세한 내용은 future.getOperationStatus().getResponse()로 확인이 가능하다.


### Set Element 조회

Set element를 조회하는 함수이다. 이 함수는 임의의 count 개 element를 조회한다.

```java
CollectionFuture<Set<Object>> asyncSopGet(String key, int count, boolean withDelete, boolean dropIfEmpty)
```

- key: 조회 대상 set의 key
- count: 조회할 element 개수
- withDelete: element 조회와 함께 그 element를 삭제할 것인지를 지정
- dropIfEmpty: element 삭제로 인해 empty set이 될 경우, 그 set도 삭제할 것인지를 지정

수행 결과는 future 객체를 통해 얻는다.

future.get() | future.operationStatus().getResponse() | 설명 
------------ | -------------------------------------- | -------
not null     | CollectionResponse.END                 | Element를 조회만 한 상태
not null     | CollectionResponse.DELETED             | Element를 조회하고 삭제한 상태
not null     | CollectionResponse.DELETED_DROPPED     | Element를 조회하고 삭제한 다음 set을 drop(delete)한 상태
null         | CollectionResponse.NOT_FOUND           | Key miss (주어진 key에 해당하는 item이 없음)
null         | CollectionResponse.NOT_FOUND_ELEMENT   | Element가 존재하지 않은 상태임, set이 비어있음
null         | CollectionResponse.TYPE_MISMATCH       | 해당 key가 set이 아님
null         | CollectionResponse.UNREADABLE          | 해당 key를 읽을 수 없는 상태임. (unreadable item상태)

Set element를 조회하는 예제는 아래와 같다.

```java
String key = "Sample:Set";
int count = 10;
boolean withDelete = false;
boolean dropIfEmpty = false;

CollectionFuture<Set<Object>> future = null;

try {
    future = client.asyncSopGet(key, count, withDelete, dropIfEmpty); // (1)
} catch (IllegalStateException e) {
    // handle exception
}

if (future == null)
    return;

try {
    Set<Object> result = future.get(1000L, TimeUnit.MILLISECONDS); // (2)
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

1. Set collection에서 count개의 element를 조회한다.
   withDelete값이 true이면 조회한 다음 element를 set collection에서 삭제한다.
   dropIfEmpty값이 true이면 조회와 동시에 element가 삭제된 후 set이 비어있게 되면 key를 삭제한다.
2. timeout은 1초로 지정했다. 조회에 성공하면 future는 조회 결과를 반환한다.
   지정한 시간에 결과가 넘어 오지 않거나 JVM의 과부하로 operation queue에서 처리되지 않을 경우
   TimeoutException이 발생한다.
3. 조회 결과에 관한 자세한 내용은 future.operationStatus().getResponse() 로 확인이 가능하다.


### Set Element 일괄 삽입

Set에 여러 element를 한번에 삽입하는 함수는 두 가지가 있다.

첫째, 하나의 key가 가리키는 set에 다수의 element를 삽입하는 함수이다.

```java
CollectionFuture <Map<Integer, CollectionOperationStatus>>
asyncSopPipedInsertBulk(String key, List<Object> valueList, CollectionAttributes attributesForCreate)
```
- key: 삽입 대상 set의 key 
- valueList: 삽입할 element들의 value list 
- attributesForCreate: 대상 set이 존재하지 않을 시의 동작을 지정한다.
  - null: element 삽입하지 않는다. 
  - attributes: 주어진 attributes를 가진 empty set item 생성 후에 element 삽입한다.

둘째, 여러 key들이 가리키는 set들에 각각 하나의 element를 삽입하는 함수이다.

```java
Future<Map<String, CollectionOperationStatus>>
asyncSopInsertBulk(List<String> keyList, Object value, CollectionAttributes attributesForCreate)
```

- key: 삽입 대상 set들의 key list
- value: 삽입할 element의 value
- attributesForCreate: 대상 set이 존재하지 않을 시의 동작을 지정한다.
  - null: element 삽입하지 않는다. 
  - attributes: 주어진 attributes를 가진 empty set item 생성 후에 element 삽입한다.


하나의 Set에 여러 element를 bulk insert하고 각각의 item에 대해 insert 결과를 확인하는 코드이다.

```java
String key = "Sample:SetBulk";
List<Object> bulkData = getBulkData();
CollectionAttributes attributesForCreate = new CollectionAttributes();
if (bulkData.size() > client.getMaxPipedItemCount()) { // (1)
    System.out.println("insert 할 아이템 개수는 client.getMaxPipedItemCount개를 초과할 수 없다.");
    return;
}

CollectionFuture<Map<Integer, CollectionOperationStatus>> future = null;

try {
    future = client.asyncSopPipedInsertBulk(key, bulkData, attributesForCreate); // (2)
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
2. Key에 저장된 Set에 bulkData를 한꺼번에 insert하고 그 결과를 담은 future객체를 반환한다.
   이 future로부터 각 아이템의 insert성공 실패 여부를 조회할 수 있다.
   여기에서는 attributesForCreate 값을 지정하여 bulk insert하기 전에 key가 없으면 생성하고 element를 insert되도록 하였다.
3. timeout은 1초로 지정했다.
   지정한 시간에 모든 아이템의 insert 결과가 넘어 오지 않거나 JVM의 과부하로 operation queue에서 처리되지 않을 경우
   TimeoutException이 발생한다.
4. 모든 아이템이 insert에 성공하면 empty map이 반환된다.
   - 반환된 Map의 Key= insert한 값(bulkData)를 iteration했을 때의 index 값.
   - 반환된 Map의 Value= insert 실패사유
5. 일부 실패한 아이템의 실패 원인을 조회하려면 insert할 때 사용된 값(bulkData)의 iteration 순서에 따라 결과
   Map을 조회하면 된다.
6. Future로부터 얻은 Map의 Key가 입력된 값(bulkData)의 index이기 때문에 위와 같은 방법으로 실패 원인을 조회하면 된다.


### Set Element 일괄 존재여부 확인

Set에서 여러 element의 존재여부를 한번에 확인하는 함수이다.

```java
CollectionFuture<Map<Object, Boolean>> asyncSopPipedExistBulk(String key, List<Object> values)
```

- key: 조회 대상 set의 key
- values: 존재유무를 확인할 value list


수행 결과는 future 객체를 통해 얻는다.

future.get() | future.operationStatus().getResponse() | 설명 
------------ | -------------------------------------- | -------
not null     | CollectionResponse.EXIST               | Element가 존재함
not null     | CollectionResponse.NOT_EXIST           | Element가 존재하지 않음
null         | CollectionResponse.NOT_FOUND           | Key miss (주어진 key에 해당하는 item이 없음)
null         | CollectionResponse.TYPE_MISMATCH       | 해당 key가 set이 아님
null         | CollectionResponse.UNREADABLE          | 해당 key를 읽을 수 없는 상태임. (unreadable item상태)

결과로 반환된 result(Map\<Object, Boolean\>) 객체에서 다음과 같은 정보를 확인할 수 있다

result 객체의 Method | 자료형    | 설명
-------------------|---------|----------
getKey()           | Object  | value
getValue()         | Boolean | value의 존재 유무


아래 코드는 set안에 VALUE1부터 VALUE4의 존재유무를 판단하는 코드이다.
결과는 Map\<Object, Boolean\> 형태이며 결과값의 key는 존재 유무를 판단하기 위한 값이다.
그리고 그 값이 존재하면 map의 value는 true가 반환된다.

```java
String key = "Sample:Set";
List<Object> valueList = new ArrayList<Object>();
valueList.add("value1");
valueList.add("value2");
valueList.add("value3");
valueList.add("value4");
CollectionFuture<Boolean> future = null;

try {
    future = client.asyncSopPipedExistBulk(key, valueList); // (1)
} catch (IllegalStateException e) {
    // handle exception
}

if (future == null)
    return;

try {
    Map<Object, Boolean> result = future.get(1000L, TimeUnit.MILLISECONDS); // (2)
    for(Entry<Object, Boolean> entry : result.entrySet()) {
        System.out.println("Object=" + entry.getKey() + ", exists=" + entry.getValue());
    }

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

1. Value list에 포함된 값들이 set에 각각 존재하는지 조회한다.
2. 결과는 Map\<Object, Boolean\>형태로 반환된다.
   - Map entry의 key는 존재유무를 판단하는 값이며
   - Map entry의 value는 존재유무를 나타내는 boolean값이다. (값이 존재하면 true)
3. Bulk exists결과에 대한 자세한 응답코드는 future.getOperationStatus().getResponse()를 통해 조회할 수 있다.
