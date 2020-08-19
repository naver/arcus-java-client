# B+tree Item

B+tree item은 하나의 key에 대해 b+tree 구조 기반으로 b+tree key(bkey)로 정렬된 data의 집합을 가진다.

**제약 조건**
- 저장 가능한 최대 element 개수 : 디폴트 4,000개 (attribute 설정으로 최대 50,000개 확장 가능)
- 각 element에서 value 최대 크기 : 16KB
- 하나의 b+tree 내에서 모든 element는 동일한 bkey 유형을 가져야 한다.
  즉, long bkey 유형과 byte array bkey 유형이 혼재할 수 없다.

B+tree item 구조와 기본 특징은 [ARCUS Server Ascii Protocol 문서의 내용](https://github.com/naver/arcus-memcached/blob/master/doc/arcus-collection-concept.md)을
먼저 참고하기 바란다.

B+tree item 연산의 설명에 앞서, b+tree 조회 및 변경에 사용하는 객체들을 설명한다.

- [Bkey(B+Tree Key)와 EFlag(Element Flag)](07-btree-API.md#bkeybtree-key%EC%99%80-eflagelement-flag)
- [Element Flag Filter 객체](07-btree-API.md#element-flag-filter-%EA%B0%9D%EC%B2%B4)
- [Element Flag Update 객체](07-btree-API.md#element-flag-update-%EA%B0%9D%EC%B2%B4)


B+tree item에 대해 수행가능한 기본 연산은 다음과 같다.

- [B+Tree Item 생성](07-btree-API.md#btree-item-%EC%83%9D%EC%84%B1) (B+tree item 삭제는 key-value item 삭제 함수로 수행한다)
- [B+Tree Element 삽입](07-btree-API.md#btree-element-%EC%82%BD%EC%9E%85)
- [B+Tree Element Upsert](07-btree-API.md#btree-element-upsert)
- [B+Tree Element 변경](07-btree-API.md#btree-element-%EB%B3%80%EA%B2%BD)
- [B+Tree Element 삭제](07-btree-API.md#btree-element-%EC%82%AD%EC%A0%9C)
- [B+Tree Element 값의 증감](07-btree-API.md#btree-element-%EA%B0%92%EC%9D%98-%EC%A6%9D%EA%B0%90)
- [B+Tree Element 개수 계산](07-btree-API.md#btree-element-%EA%B0%9C%EC%88%98-%EA%B3%84%EC%82%B0)
- [B+Tree Element 조회](07-btree-API.md#btree-element-%EC%A1%B0%ED%9A%8C)

여러 b+tree element들에 대해 한번에 일괄 수행하는 연산은 다음과 같다.

- [B+Tree Element 일괄 삽입](07-btree-API.md#btree-element-%EC%9D%BC%EA%B4%84-%EC%82%BD%EC%9E%85)
- [B+Tree Element 일괄 변경](07-btree-API.md#btree-element-%EC%9D%BC%EA%B4%84-%EB%B3%80%EA%B2%BD)
- [B+Tree Element 일괄 조회](07-btree-API.md#btree-element-%EC%9D%BC%EA%B4%84-%EC%A1%B0%ED%9A%8C)

여러 b+tree element들에 대해 sort-merge 조회 연산은 다음과 같다.

- [B+Tree Element Sort-Merge 조회](07-btree-API.md#btree-element-sort-merge-%EC%A1%B0%ED%9A%8C)

B+tree position 관련 연산들은 다음과 같다.

- [B+Tree Position 조회](07-btree-API.md#btree-position-%EC%A1%B0%ED%9A%8C)
- [B+Tree Position 기반의 Element 조회](07-btree-API.md#btree-position-%EA%B8%B0%EB%B0%98%EC%9D%98-element-%EC%A1%B0%ED%9A%8C)
- [B+Tree Position과 Element 동시 조회](07-btree-API.md#btree-position%EA%B3%BC-element-%EB%8F%99%EC%8B%9C-%EC%A1%B0%ED%9A%8C)


## BKey(B+Tree Key)와 EFlag(Element Flag)

B+tree item에서 사용가능한 bkey 데이터 타입은 아래 두 가지이다.

- long 타입
- byte[1~31] 타입 : byte array크기가 1부터 31까지 어느 것을 사용해도 된다.

byte array 타입의 bkey를 만드는 예는 아래와 같다.
만약, byte array의 크기가 31을 초과하면 IllegalArgumentException이 발생한다.

```
// Bkey로 0x00000001을 사용한다.
byte[] bkey = new byte[] { 0, 0, 0, 1 }
```

eflag는 현재 b+tree element에만 존재하는 필드이다.
eflag 데이터 타입은 byte[1~31] 타입만 가능하며, bkey의 byte array 사용 방식과 동일하다.


## Element Flag Filter 객체


Element를 조회, 수정, 삭제 시에 eflag(element flag)에 대한 filter 조건을 사용할 수 있다.
eflag filter 조건은 아래와 같이 표현한다.

- 기본적으로, eflag의 전체/부분 값을 **compare 값**과 **compare 연산**을 수행한다.
  - **compare 값**은 eflag 값에 대해 compare 연산을 취한 operand로 eflag filter에 명시된다.
- 선택적으로, compare 연산의 수행 전에 eflag의 전체/부분 값에 대해 **bitwise 값**으로 **bitwise 연산**을 먼저 취할 수 있다.
  - **bitwise 값**은 eflag 값에 대해 bitwise 연산을 취한 operand로 eflag filter에 명시된다.
  - **현재, bitwise 값의 길이는 compare 값의 길이와 동일해야 하는 제약이 있다.** 

eflag filter 조건에서 compare/bitwise 연산이 수행될 eflag의 전체/부분 값은 아래와 같이 선택한다.

- eflag 전체 값에서 compare 연산의 대상 값은 **compare offset**과 **compare length**로 지정한다.
  - **compare offset**은 디폴트로 0을 가지며, eflag filter에서 수정이 가능하다.
  - **compare length**는 eflag filter에 명시된 **compare 값**의 길이로 자동 설정된다.
- eflag 전체 값에서 bitwise 연산의 대상 값은 **bitwise offset**과 **bitwise length**로 지정한다.
  - **bitwise offset**은 따로 지정하지 않고, **compare offset**을 그대로 사용한다.
  - **bitwise length**는 eflag filter에 명시된 **bitwise 값**의 길이로 자동 설정된다.

제공하는 compare 연산자는 다음과 같다.

compare 연산자                                | 설명
--------------------------------------------- | ----
ElementFlagFilter.CompOperands.Equal	      | 일치
ElementFlagFilter.CompOperands.NotEqual	      | 일치하지 않음
ElementFlagFilter.CompOperands.LessThan	      | 작은 것
ElementFlagFilter.CompOperands.LessOrEqual    | 작거나 같은 것
ElementFlagFilter.CompOperands.GreaterThan    | 큰 것
ElementFlagFilter.CompOperands.GreaterOrEqual | 크거나 같은 것

제공하는 bitwise 연산자는 다음과 같다.

compare 연산자                                | 설명
--------------------------------------------- | ----
ElementFlagFilter.BitwiseOperands.AND	      | AND 연산
ElementFlagFilter.BitwiseOperands.OR	      | OR 연산
ElementFlagFilter.BitwiseOperands.XOR	      | XOR 연산

### ElementFlagFilter 메소드

ElementFlagFilter 클래스의 생성자 함수는 아래와 같다.
compare 연산자와 compare 값만을 지정하여 ElementFlagFilter 객체를 생성한다.
compare offset는 디폴트로 0을 값으로 가지면, bitwise 연산의 설정은 없는 상태가 된다.

```java
ElementFlagFilter(CompOperands compOperand, byte[] compValue)
```

ElementFlagFilter 객체의 compare offset을 변경하거나 
bitwise 연산 설정을 하고자 한다면, 아래 메소드를 사용할 수 있다.

```java
ElementFlagFilter setCompareOffset(int offset)
ElementFlagFilter setBitOperand(BitWiseOperands bitOp, byte[] bitCompValue)
```

### ElementFlagFilter 사용 예제

첫째 예는 b+tree에 저장된 전체 element들에서 eflag 값이 0x0102와 일치하는 element의 개수를 조회한다.

```java
ElementFlagFilter filter = new ElementFlagFilter(CompOperands.Equal, new byte[] { 1, 2 }); // (1) 
CollectionFuture<Integer> future = mc.asyncBopGetItemCount(KEY, MIN_BKEY, MAX_BKEY, filter); 
Integer count = future.get(); 
```

1. Eflag가 0x0102와 일치 여부를 판단하는 filter를 생성한다.
   compare offset은 0이 되고, compare length는 2가 된다.


둘째 예는 b+tree의 모든 element들에서 eflag의 두 번째 바이트를 0x01과 AND 연산한 결과가 0x01인
element의 개수를 조회하는 예제이다.

```java
ElementFlagFilter filter = new ElementFlagFilter(CompOperands.Equal, new byte[] { 1 }); // (1) 
filter.setBitOperand(BitWiseOperands.AND, new byte[] { 1 }); // (2) 
filter.setCompareOffset(1); // (3) 

CollectionFuture<Integer> future = mc.asyncBopGetItemCount(KEY, MIN_BKEY, MAX_BKEY, filter); 
Integer count = future.get(); 
```

1. Eflag가 0x01와 일치 여부를 판단하는 filter를 생성한다.
2. Eflag에 대한 compare 연산 수행 전에 0x01과 bitwise AND 연산하기 위해 설정한다.
3. Eflag의 두 번째 바이트부터 filtering하기 위해 설정한다.

참고로, 조회 조건에 filter를 사용하지 않으려면 filter 값으로 ElementFlagFilter.DO_NOT_FILTER를 지정하면 된다.

셋째 예는 b+tree에서 eflag가 설정되지 않은 모든 element를 조회하는 예제이다.
Eflag가 지정되지 않은 element를 조회하려면 eflag의 첫 번째 한 바이트를 0x00과 bitwise AND연산을 취하고
그 결과가 0x00이 아닌 element를 찾으면 된다.
eflag를 가진 element라면 첫 번재 한 바이트를 0x00과 bitwise AND 연산을 취했을 때 무조건 0x00이 되기 때문에
bitwise연산 결과가 0x00이 아닌 것을 조회하면 eflag를 갖지 않은 element를 조회할 수 있게 된다.

```java
ElementFlagFilter filter = new ElementFlagFilter(CompOperands.NotEqual, new byte[] { 0 });  // (1)
filter.setBitOperand(BitWiseOperands.AND, new byte[] { 0 }); // (2)
Map<Long, Object> map = mc.asyncBopGet(KEY, BKEY, BKEY + 100, 0, 100, false, false, filter).get(); // (3)
```
1. Eflag가 0x00이 아닌 것들을 조회하는 filter를 생성한다.
2. Filter가 비교할 eflag는 element에 설정된 eflag와 0x00를 bitwise AND연산한 것을 사용한다.
3. 1과 2에서 생성한 filter를 사용하면 eflag가 설정되지 않은 element를 조회할 수 있다.


### ElementMultiFlagsFilter

Eflag에 대해 아래와 같은 IN 연산과 NOT IN 연산을 수행하기 위한 filter이다.

- IN 연산 : eflag 값이 여러 compare values 중 하나와 동일한 지를 검사
- NOT IN 연산 : eflags 값이 여러 compare values와 모두 다른 지를 검사

ElementMultiFlagsFilter는 여러 compare value를 지정할 수 있고, 아래 두 개의 compare 연산자를 지원한다.
- ElementFlagFilter.CompOperands.Equal
- ElementFlagFilter.CompOperands.NotEqual

ElementMultiFlagsFilter 사용 예로,
아래는 b+tree의 전체 element들 중에 eflag의 값이 0x0102 또는 0x0104에 일치하는 element의 개수를 조회한다.
즉, IN 연산의 filtering을 수행한다.

```java
mentMultiFlagsFilter filter = new ElementMultiFlagsFilter(CompOperands.Equal); // (1) 
filter.addCompValue(new byte[] { 1, 2 }); // (2)
filter.addCompValue(new byte[] { 1, 4 }); // (3)
CollectionFuture<Integer> future = mc.asyncBopGetItemCount(KEY, MIN_BKEY, MAX_BKEY, filter); 
Integer count = future.get(); 
```

1. filter를 생성한다.
2. 일치 여부 판단을 위한 값 0x0102 등록 
3. 일치 여부 판단을 위한 값 0x0104 등록

ElementMultiFlagsFilter로 최대 100개 compare value를 지정할 수 있으며,
asyncBopGet, asyncBopCount, asyncBopDelete, asyncBopSortMergeGet 에서만 사용이 가능하다.

### Element Flag Update 객체

Eflag의 전체 또는 부분 값을 변경할 수 있다.
이를 위한 ElementFlagUpdate 생성자 함수는 아래와 같다.

- ElementFlagUpdate(byte[] elementFlag)
  - Eflag의 전체 값을 새로운 elementFlag로 교체한다.
- ElementFlagUpdate(int elementFlagOffset, BitWiseOperands bitOp, byte[] elementFlag)
  - Eflag의 부분 값을 bitwise 연산을 취한 결과로 교체한다.
  - Eflag에서 bitwise 연산을 취할 부분 값의 offset과 length는
    각각 elementFlagOffset과 elementFlag 값의 길이로 결정된다.

Eflag 전체 값을 교체하는 예는 아래와 같다.

```java
byte[] flag = new byte[] { 1, 0, 1, 0 };
ElementFlagUpdate eflagUpdate = new ElementFlagUpdate(flag);

CollectionFuture<Boolean> future = mc.asyncBopUpdate(KEY, BKEY, eflagUpdate, null);
```

Eflag 부분 값을 교체하는 예는 아래와 같다.

```java
int eFlagOffset = 1;
BitWiseOperands bitOp = BitWiseOperands.AND;
byte[] flag = new byte[] { 1 };

ElementFlagUpdate eflagUpdate = new ElementFlagUpdate(eFlagOffset, bitOp, flag);
CollectionFuture<Boolean> future = mc.asyncBopUpdate(KEY, BKEY, eflagUpdate, null);
```


## B+Tree Item 생성

새로운 empty b+tree item을 생성한다.

```java
CollectionFuture<Boolean> asyncBopCreate(String key, ElementValueType valueType, CollectionAttributes attributes)
```

- key: 생성할 b+tree item의 key 
- valueType: b+tree에 저장할 value의 유형을 지정한다. 아래의 유형이 있다.
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
- attributes: b+tree item의 속성들을 지정한다.

수행 결과는 future 객체를 통해 얻는다.

future.get() | future.operationStatus().getResponse() | 설명 
------------ | -------------------------------------- | -------
True         | CollectionResponse.CREATED             | 생성 성공
False        | CollectionResponse.EXISTS              | 동일 key가 이미 존재함


B+tree item을 생성하는 예제는 아래와 같다.

```java
String key = "Sample:EmptyBTree";
CollectionFuture<Boolean> future = null;
CollectionAttributes attribute = new CollectionAttributes(); // (1)
attribute.setExpireTime(60); // (1)

try {
    future = client.asyncBopCreate(key, ElementValueType.STRING, attribute); // (2)
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

1. B+tree의 expire time을 60초로 지정하였다.
   CollectionAttributes의 자세한 사용방법은 [[Manual:Java_Client/Attribute_사용|Attribute_사용]] 장에서 자세히 다룬다.
2. Empty b+tree를 생성할 때에는 b+tree에 어떤 타입의 element를 저장할 것인지를 미리 지정해 두어야 한다.
   이렇게 해야 하는 이유는 Java client에서 value를 encoding/decoding하는 메커니즘 때문이다.
   위 예제는 String 타입을 저장할 수 있는 empty b+tree를 생성한다.
   만약에 empty b+tree를 생성할 때 지정한 element type과 일치하지 않는 값을 b+tree에 저장한다면
   저장은 성공하겠지만 조회할 때 엉뚱한 값이 조회된다.
3. timeout은 1초로 지정했다. 생성에 성공하면 future는 true를 반환한다.
   지정한 시간에 생성 결과가 넘어 오지 않거나 JVM의 과부하로 operation queue에서 처리되지 않을 경우
   TimeoutException이 발생한다.
4. 생성 결과에 대한 상세 정보는 future.getOperationStatus().getResponse()를 통해 조회 할 수 있다.


## B+Tree Element 삽입

B+Tree에 하나의 element를 삽입한다.
전자는 long 타입의 bkey를, 후자는 최대 31 크기의 byte array 타입의 bkey를 사용한다.

```java
CollectionFuture<Boolean>
asyncBopInsert(String key, long bkey, byte[] eFlag, Object value, CollectionAttributes attributesForCreate)
CollectionFuture<Boolean>
asyncBopInsert(String key, byte[] bkey, byte[] eFlag, Object value, CollectionAttributes attributesForCreate)
```

- key: 삽입 대상 b+tree의 key
- bkey: 삽입할 element의 bkey(b+tree key)
- eflag: 삽입할 element의 eflag(element flag), that is optional. 
- value: 삽입할 element의 value
- attributesForCreate: 대상 b+tree가 없을 시, 동작을 지정한다.
  - null: element 삽입하지 않는다. 
  - attributes: 주어진 attributes를 가진 empty b+tree item 생성 후에 element 삽입한다.

수행 결과는 future 객체를 통해 얻는다.

future.get() | future.operationStatus().getResponse() | 설명 
------------ | -------------------------------------- | ---------
True         | CollectionResponse.STORED              | Element만 삽입함
True         | CollectionResponse.CREATED_STORED      | B+tree collection 생성하고 element를 삽입함
False        | CollectionResponse.NOT_FOUND           | Key miss (주어진 key에 해당하는 item이 없음)
False        | CollectionResponse.TYPE_MISMATCH       | 해당 item이 b+tree가 아님
False        | CollectionResponse.BKEY_MISMATCH       | 주어진 bkey 유형이 기존 bkey 유형과 다름
False        | CollectionResponse.ELEMENT_EXISTS      | 주어진 bkey를 가진 element가 이미 존재함
False        | CollectionResponse.OVERFLOWED          | 최대 저장가능한 개수만큼 element들이 존재함
False        | CollectionResponse.OUT_OF_RANGE        | 주어진 bkey가 b+tree trimmed 영역에 해당됨

B+tree element를 삽입하는 예제는 아래와 같다.

```java
String key = "Prefix:BTreeKey";
long bkey = 1L;
String value = "This is a value.";
byte[] eflag = new byte[] { 1, 1, 1, 1 };

CollectionAttributes attributesForCreate = new CollectionAttributes();
CollectionFuture<Boolean> future = null;

try {
    future = client.asyncBopInsert(key, bkey, eflag, value, attributesForCreate); // (1)
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

1. attributesForCreate값이 null이 아니면 b+tree가 존재하지 않을 때
   attributesForCreate속성을 가진 b+tree를 새로 생성한 다음 element를 저장한다.
   만약 key가 존재하지 않는 상황에서 attributesForCreate값이 null이면 insert에 실패한다.
   - 위 예제는 디폴트 CollectionAttributes를 사용하며, 기본 expire time은 0으로 만료되지 않음을 뜻한다.
2. timeout은 1초로 지정했다. Insert가 성공하면 future는 true를 반환한다.
   지정한 시간에 insert 결과가 넘어 오지 않거나 JVM의 과부하로 operation queue에서 처리되지 않을 경우
   TimeoutException이 발생한다.
3. Insert결과에 대한 자세한 결과 코드를 확인하려면 future.getOperationStatus().getResponse()를 사용한다.


아커스에서 B+tree는 가질 수 있는 최대 엘리먼트 개수가 제한되어 있다. 이 제한 범위 안에서 사용자가 직접 B+tree 크기를 지정할 수도 있는데(maxcount), 이러한 제약조건 때문에 가득 찬 B+tree에 새로운 엘리먼트를 입력하면 설정에 따라 기존의 엘리먼트가 삭제될 수 있다.
이렇게 암묵적으로 삭제되는 엘리먼트를 입력(insert, upsert)시점에 획득할 수 있는 기능을 제공한다.

```java
BTreeStoreAndGetFuture<Boolean, Object>
asyncBopInsertAndGetTrimmed(String key, long bkey, byte[] eFlag, Object value, CollectionAttributes attributesForCreate)

BTreeStoreAndGetFuture<Boolean, Object>
asyncBopInsertAndGetTrimmed(String key, byte[] bkey, byte[] eFlag, Object value, CollectionAttributes attributesForCreate)
```

B+tree에 bkey에 해당하는 엘리먼트를 insert 하거나 upsert 할 때 삭제(trim) 되는 엘리먼트가 있을 경우 그 값을 조회한다.


- key: b+tree item의 key
- bkey: 삽입할 element의 bkey(b+tree key)
  - bkey는 element의 key로 long또는 byte[1~31] 유형을 사용할 수 있다.
  - 0이상의 값으로만 지정할 수 있고. key가 존재하는 상태에서 bkey와 value가 저장된다 하더라도
     key에 설정된 expire time은 변하지 않는다. 
- eflag: 삽입할 element의 eflag(element flag)
- value: 삽입할 element의 value
- attributesForCreate: 대상 b+tree가 없을 시, 동작을 지정한다.
  - null: element 삽입하지 않는다. 
  - attributes: 주어진 attributes를 가진 empty b+tree item 생성 후에 element 삽입한다.

수행 결과는 future 객체를 통해 얻는다.

future.get() | future.operationStatus().getResponse() | 설명 
------------ | -------------------------------------- | ---------
True         | CollectionResponse.STORED              | Element만 삽입함
True         | CollectionResponse.CREATED_STORED      | B+tree collection 생성하고 element를 삽입함
True         | CollectionResponse.REPLACED            | Element가 교체됨
True         | CollectionResponse.TRIMMED             | element가 삽입되고, 삽입으로 trimmed element가 조회됨
False        | CollectionResponse.NOT_FOUND           | Key miss (주어진 key에 해당하는 item이 없음)
False        | CollectionResponse.TYPE_MISMATCH       | 해당 item이 b+tree가 아님
False        | CollectionResponse.BKEY_MISMATCH       | 주어진 bkey 유형이 기존 bkey 유형과 다름
False        | CollectionResponse.ELEMENT_EXISTS      | 주어진 bkey를 가진 element가 이미 존재함
False        | CollectionResponse.OVERFLOWED          | 최대 저장가능한 개수만큼 element들이 존재함
False        | CollectionResponse.OUT_OF_RANGE        | 주어진 bkey가 b+tree trimmed 영역에 해당됨

future.getElement()객체를 통해 삭제(trim) 되는 엘리먼트의 정보를 확인할 수 있다

future.getElement() 객체의 Method | 자료형    | 설명
--------------------------------|---------|------
getValue()                      | Object  | element의 값
getByteArrayBkey()              | byte[]  | element bkey 값(byte[])
getLongBkey()                   | long    | element bkey 값(long)
isByteArrayBkey()               | boolean | element bkey byte array 여부
getFlag()                       | byte[]  | element flag값(byte[])

B+tree에 element 삽입하면서 암묵적으로 trim되는 element를 조회하는 예제는 아래와 같다.

```java
private String key = "BopStoreAndGetTest";
private long[] longBkeys = { 10L, 11L, 12L, 13L, 14L, 15L, 16L, 17L, 18L,

public void testInsertAndGetTrimmedLongBKey() throws Exception {
	// insert test data
	CollectionAttributes attrs = new CollectionAttributes();
	attrs.setMaxCount(10);
	attrs.setOverflowAction(CollectionOverflowAction.smallest_trim);
	for (long each : longBkeys) {
		mc.asyncBopInsert(key, each, null, "val", attrs).get();
	}

	// cause an overflow
	assertTrue(mc.asyncBopInsert(key, 1000, null, "val", null).get());
	
	// expecting that bkey 10 was trimmed out and the first bkey is 11 
	Map<Integer, Element<Object>> posMap = mc.asyncBopGetByPosition(key, BTreeOrder.ASC, 0).get();
	assertNotNull(posMap);
	assertNotNull(posMap.get(0)); // the first element
	assertEquals(11L, posMap.get(0).getLongBkey());

	// then cause an overflow again and get a trimmed object
	// it would be a bkey(11)
	BTreeStoreAndGetFuture<Boolean, Object> f = mc.asyncBopInsertAndGetTrimmed(key, 2000, null, "val", null);
	boolean succeeded = f.get();
	Element<Object> element = f.getElement();
	assertTrue(succeeded);
	assertNotNull(element);
	assertEquals(11L, element.getLongBkey());
	System.out.println("The insertion was succeeded and an element " + f.getElement() + " was trimmed out");
	
	// finally check the first bkey which is expected to be 12 
	posMap = mc.asyncBopGetByPosition(key, BTreeOrder.ASC, 0).get();
	assertNotNull(posMap);
	assertNotNull(posMap.get(0)); // the first element
	assertEquals(12L, posMap.get(0).getLongBkey());
}
```

## B+Tree Element Upsert

B+Tree에 하나의 element를 upsert하는 함수들이다.
Upsert 연산은 해당 element가 없으면 insert하고, 있으면 update하는 연산이다.
전자는 long bkey를, 후자는 최대 31 크기의 byte array bkey를 사용한다.

```java
CollectionFuture<Boolean>
asyncBopUpsert(String key, long bkey, byte[] eFlag, Object value, CollectionAttributes attributesForCreate)
CollectionFuture<Boolean>
asyncBopUpsert(String key, byte[] bkey, byte[] eFlag, Object value, CollectionAttributes attributesForCreate)
```

- key: upsert 대상 b+tree의 key
- bkey: upsert할 element의 bkey(b+tree key)
- eflag: upsert할 element의 eflag(element flag), that is optional
- value: upsert할 element의 value
- attributesForCreate: 대상 b+tree가 없을 시, 동작을 지정한다.
  - null: upsert 작업을 수행하지 않는다.
  - attributes: 주어진 attributes를 가진 empty b+tree item 생성 후에 element 삽입한다.

수행 결과는 future 객체를 통해 얻는다.

future.get() | future.operationStatus().getResponse() | 설명 
------------ | -------------------------------------- | ---------
True         | CollectionResponse.STORED              | Element만 삽입함
True         | CollectionResponse.CREATED_STORED      | B+tree collection 생성하고 element를 삽입함
True         | CollectionResponse.REPLACED            | Element가 교체됨
False        | CollectionResponse.NOT_FOUND           | Key miss (주어진 key에 해당하는 item이 없음)
False        | CollectionResponse.TYPE_MISMATCH       | 해당 item이 b+tree가 아님
False        | CollectionResponse.BKEY_MISMATCH       | 주어진 bkey 유형이 기존 bkey 유형과 다름
False        | CollectionResponse.OVERFLOWED          | 최대 저장가능한 개수만큼 element들이 존재함
False        | CollectionResponse.OUT_OF_RANGE        | 주어진 bkey가 b+tree trimmed 영역에 해당됨

B+tree element를 upsert하는 예제는 아래와 같다.

```java
String key = "Prefix:BTreeKey";
long bkey = 1L;
String value = "This is a value.";
byte[] eflag = new byte[] { 1, 1, 1, 1 };

CollectionAttributes attributesForCreate = new CollectionAttributes();
CollectionFuture<Boolean> future = null;


try {
    future = client.asyncBopUpsert(key, bkey, eflag, value, attributesForCreate); // (1)
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

1. B+tree에 element를 update또는 insert한다.
   attributesForCreate값이 null이 아니면 b+tree가 존재하지 않을 때
   attributesForCreate속성을 가진 b+tree를 새로 생성한 다음 element를 저장한다.
   만약 key가 존재하지 않는 상황에서 attributesForCreate값이 null이면 insert에 실패한다.
   Key가 새로 생성될 때 지정하는 attributesForCreate에 expire time을 지정하지 않으면 기본값인 0으로 설정되며
   이는 b+tree가 만료되지 않음을 뜻한다.
2. timeout은 1초로 지정했다. Upsert가 성공하면 future는 true를 반환한다.
   지정한 시간에 upsert 결과가 넘어 오지 않거나 JVM의 과부하로 operation queue에서 처리되지 않을 경우
   TimeoutException이 발생한다.
3. Upsert 결과에 대한 자세한 결과 코드를 확인하려면 future.getOperationStatus().getResponse()를 사용한다.


## B+Tree Element 변경

B+Tree에서 하나의 element를 변경하는 함수이다. Element의 eflag 그리고/또는 value를 변경한다.
전자는 long bkey를, 후자는 최대 31 크기의 byte array bkey를 사용한다.

```java
CollectionFuture<Boolean> asyncBopUpdate(String key, long bkey, ElementFlagUpdate eFlagUpdate, Object value)
CollectionFuture<Boolean> asyncBopUpdate(String key, byte[] bkey, ElementFlagUpdate eFlagUpdate, Object value)
```

- key: 변경 대상 b+tree의 key
- bkey: 변경 대상 element의 bkey(b+tree key)
- eFlagUpate: element의 eflag 변경할 내용
  - eflag를 변경하지 않으려면 null을 지정한다.
  - eflag를 삭제하려면 ElementFlagUpdate.RESET_FLAG를 지정한다.
- value: element의 new value
  - value를 변경하지 않으려면 null을 지정한다.

수행 결과는 future 객체를 통해 얻는다.

future.get() | future.operationStatus().getResponse() | 설명 
------------ | -------------------------------------- | ---------
True         | CollectionResponse.UPDATED             | Element가 변경됨
False        | CollectionResponse.NOT_FOUND           | Key miss (주어진 key에 해당하는 item이 없음)
False        | CollectionResponse.NOT_FOUND_ELEMENT   | 주어진 bkey를 가진 element가 없음
False        | CollectionResponse.TYPE_MISMATCH       | 해당 item이 b+tree가 아님
False        | CollectionResponse.BKEY_MISMATCH       | 주어진 bkey 유형이 기존 bkey 유형과 다름
False        | CollectionResponse.EFLAG_MISMATCH      | 주어진 eFlagUpate가 해당 element의 eflag 데이터와 불일치

특정 element의 eflag는 그대로 두고 value만 변경한다.

```java
CollectionFuture<Boolean> future = mc.asyncBopUpdate(KEY, BKEY, null, value);
```

특정 element의 eflag는 0x01000100로 변경하고 value는 그대로 둔다.

```java
byte[] flag = new byte[] { 1, 0, 1, 0 };
ElementFlagUpdate eflagUpdate = new ElementFlagUpdate(flag);
CollectionFuture<Boolean> future = mc.asyncBopUpdate(KEY, BKEY, eflagUpdate, null);
```

특정 element의 eflag를 삭제하고 value는 그대로 둔다.

```java
CollectionFuture<Boolean> future = mc.asyncBopUpdate(KEY, BKEY, ElementFlagUpdate.RESET_FLAG, null);
```

특정 element의 elfag에서 특정 byte를 bitwise 연산하여 변경하는 예제이다.
아래 예제는 eflag의 두번째 byte를 0x01과 bitwise AND 연산한 결과로 변경한다.
만약, 해당 element에 eflag가 존재하지 않거나 offset이 가리키는 byte가 존재하지 않는다면, 변경 연산은 실패한다.

```java
int eFlagOffset = 1;
BitWiseOperands bitOp = BitWiseOperands.AND;
byte[] flag = new byte[] { 1 };

ElementFlagUpdate eflagUpdate = new ElementFlagUpdate(eFlagOffset, bitOp, flag);
CollectionFuture<Boolean> future = mc.asyncBopUpdate(KEY, BKEY, eflagUpdate, null);
```

Element 수정에 대한 자세한 수행 결과는 future.getOperationStatus().getResponse()를 통해 조회할 수 있다.


## B+Tree Element 삭제

B+tree에서 element를 삭제하는 함수들은 두 유형이 있다.

첫째, B+tree에서 주어진 bkey를 가지면서 eFlagFilter 조건을 만족하는 element를 삭제한다.

```java
CollectionFuture<Boolean>
asyncBopDelete(String key, long bkey, ElementFlagFilter eFlagFilter, boolean dropIfEmpty)
CollectionFuture<Boolean>
asyncBopDelete(String key, byte[] bkey, ElementFlagFilter eFlagFilter, boolean dropIfEmpty)
```

둘째, B+tree에서 from부터 to까지의 bkey를 가진 elements를 탐색하면서 eFlagFilter 조건을 만족하는 
elements를 삭제한다. count가 0이면 bkey range에서 eFlagFilter 조건을 만족하는 모든 element를 삭제하고
0보다 크면, count 개의 elements만 삭제한다.

```java
CollectionFuture<Boolean>
asyncBopDelete(String key, long from, long to, ElementFlagFilter eFlagFilter, int count, boolean dropIfEmpty)
CollectionFuture<Boolean>
asyncBopDelete(String key, byte[] from, byte[] to, ElementFlagFilter eFlagFilter, int count, boolean dropIfEmpty)
```

- key: 삭제 대상 b+tree의 key
- bkey 또는 \<from, to\>: 삭제할 element의 bkey(b+tree key) 또는 bkey range 
- eFlagFilter: eflag에 대한 filter 조건
- count: 삭제할 element 개수를 지정, 0이면 조건 만족하는 모든 element 삭제
- dropIfEmpty: element 삭제로 empty b+tree가 되면, 그 b+tree 자체를 삭제할 지를 지정


수행 결과는 future 객체를 통해 얻는다.

future.get() | future.operationStatus().getResponse() | 설명 
------------ | -------------------------------------- | ---------
True         | CollectionResponse.DELETED             | Element만 삭제함
True         | CollectionResponse.DELETED_DROPPED     | Element 삭제하고 B+tree 자체도 삭제함
False        | CollectionResponse.NOT_FOUND           | Key miss (주어진 key에 해당하는 item이 없음)
False        | CollectionResponse.NOT_FOUND_ELEMENT   | 주어진 bkey를 가진 element가 없음
False        | CollectionResponse.TYPE_MISMATCH       | 해당 item이 b+tree가 아님
False        | CollectionResponse.BKEY_MISMATCH       | 주어진 bkey 유형이 기존 bkey 유형과 다름


다음은 b+tree에서 bkey가 1인 element를 삭제하는 예제이다.

```java
String key = "Prefix:BTreeKey";
long bkey = 1L;
boolean dropIfEmpty = true;
CollectionFuture<Boolean> future = null;

try {
    future = client.asyncBopDelete(key, bkey, ElementFlagFilter.DO_NOT_FILTER, dropIfEmpty); // (1)
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

1. B+tree에서 bkey에 해당하는 element를 삭제한다.
   dropIfEmpty값이 true이면 element를 삭제하고 b+tree가 비어있게 되었을 때 b+tree도 함께 삭제한다.
   예제에서 filter조건은 “filter하지 않음”으로 지정하였다.
2. delete timeout은 1초로 지정했다. 지정한 시간에 삭제 결과가 넘어 오지 않거나 JVM의 과부하로
   operation queue에서 처리되지 않을 경우 TimeoutException이 발생한다
3. 삭제 결과에 대한 상세 정보는 future.getOperationStatus().getResponse()를 통해 조회 할 수 있다.


## B+tree Element 값의 증감

B+tree element의 값을 증가/감소 시키는 함수는 아래와 같다. 
Element의 값은 String 형의 숫자이어야 한다.

```java
CollectionFuture<Long> asyncBopIncr(String key, long bkey, int by)
CollectionFuture<Long> asyncBopDecr(String key, long bkey, int by)

CollectionFuture<Long> asyncBopIncr(String key, Byte[] bkey, int by)
CollectionFuture<Long> asyncBopDecr(String key, Byte[] bkey, int by)

CollectionFuture<Long> asyncBopIncr(String key, long subkey, int by, long initial, byte[] eFlag);
CollectionFuture<Long> asyncBopDecr(String key, long subkey, int by, long initial, byte[] eFlag);

CollectionFuture<Long> asyncBopIncr(String key, byte[] subkey, int by, long initial, byte[] eFlag);
CollectionFuture<Long> asyncBopDecr(String key, byte[] subkey, int by, long initial, byte[] eFlag);
```

- key: b+tree item의 key
- bkey: 대상 element의 bkey
- by: 증감시킬 값 (1 이상의 값이어야 한다. 만약 감소시킬 값이 element 값보다 크면, 감소 후의 결과값은 0으로 저장된다)

아래 값들은 대상 element가 존재하지 않을 때 새롭게 삽입되는 값들이다. (optional)
- initial: 삽입할 element의 value (0 이상의 값[64bit unsigned integer]이어야 한다)
- eFlag: 삽입할 element의 eflag(element flag)

수행 결과는 future 객체를 통해 얻는다.

future.get() | future.operationStatus().getResponse() | 설명
------------ | -------------------------------------- | ---------
element 값    | CollectionResponse.END                 | 증감 정상 수행
null         | CollectionResponse.NOT_FOUND           | Key miss (주어진 key에 해당하는 item이 없음)
null         | CollectionResponse.NOT_FOUND_ELEMENT   | 주어진 bkey를 가진 element가 없음
null         | CollectionResponse.TYPE_MISMATCH       | 해당 item이 b+tree가 아님
null         | CollectionResponse.BKEY_MISMATCH       | 주어진 bkey 유형이 기존 bkey 유형과 다름
null         | CollectionResponse.UNREADABLE          | 해당 key를 읽을 수 없는 상태임. (unreadable item상태)
null         | CollectionResponse.OVERFLOWED          | 최대 저장가능한 개수만큼 element들이 존재함
null         | CollectionResponse.OUT_OF_RANGE        | 조회된 element가 없음, 조회 범위에 b+tree trim 영역 있음

B+tree element 값을 증가시키는 예제는 다음과 같다.

```java
String key = "Prefix:BTree";
long bkey = 0L;
CollectionFuture<Long> future = null;

try {
    future = mc.asyncBopIncr(key, bkey, (int) 2); // (1)
} catch (IllegalStateException e) {
    // handle exception
}

if (future == null)
    return;

try {
    Long result = future.get(1000L, TimeUnit.MILLISECONDS); // (2)
    System.out.println(future.getOperationStatus().getResponse()); // (3)
} catch (InterruptedException e) {
    future.cancel(true);
} catch (TimeoutException e) {
    future.cancel(true);
} catch (ExecutionException e) {
    future.cancel(true);
}
```

1. 이 예제는 b+tree에 저장된 element의 값을 2 만큼 increment 한다. 
2. timeout은 1초로 지정했다. 지정한 시간에 조회 결과가 넘어 오지 않거나
   JVM의 과부하로 operation queue에서 처리되지 않을 경우 TimeoutException이 발생한다.
3. Element increment 후 조회에 대한 자세한 결과는 future.getOperationStatus().getResponse()를 통해 조회할 수 있다.


## B+Tree Element 개수 계산

B+tree에서 from부터 to까지의 bkey를 가진 element들 중 eFlagFilter조건을 만족하는 element 개수를 조회한다.


```java
CollectionFuture<Integer>
asyncBopGetItemCount(String key, long from, long to, ElementFlagFilter eFlagFilter)
CollectionFuture<Integer>
asyncBopGetItemCount(String key, byte[] from, byte[] to, ElementFlagFilter eFlagFilter)
```

- key: b+tree item의 key
- \<from, to\>: element 조회 범위를 나타내는 bkey range 
- eFlagFilter: eflag에 대한 filter 조건


수행 결과는 future 객체를 통해 얻는다.

future.get() | future.operationStatus().getResponse() | 설명
-------------| -------------------------------------- | -------
element 개수  | CollectionResponse.END                 | Element count를 성공적으로 조회
null         | CollectionResponse.NOT_FOUND           | Key miss (주어진 key에 해당하는 item이 없음)
null         | CollectionResponse.TYPE_MISMATCH       | 해당 key가 set이 아님
null         | CollectionResponse.BKEY_MISMATCH       | 주어진 bkey 유형이 기존 bkey 유형과 다름
null         | CollectionResponse.UNREADABLE          | 해당 key를 읽을 수 없는 상태임. (unreadable item상태)


B+tree element 개수를 확인하는 예제는 아래와 같다.


```java
String key = "Prefix:BTree";
long bkeyFrom = 0L;
long bkeyTo = 100L;

CollectionFuture<Integer> future = null;

try {
    future = mc.asyncBopGetItemCount(key, bkeyFrom, bkeyTo, ElementFlagFilter.DO_NOT_FILTER); // (1)
} catch (IllegalStateException e) {
    // handle exception
}

if (future == null)
    return;

try {
    Integer result = future.get(1000L, TimeUnit.MILLISECONDS); // (2)
    System.out.println(future.getOperationStatus().getResponse()); // (3)
} catch (InterruptedException e) {
    future.cancel(true);
} catch (TimeoutException e) {
    future.cancel(true);
} catch (ExecutionException e) {
    future.cancel(true);
}
```

1. 이 예제는 b+ tree에 저장된 element중 bkey가 bkeyFrom부터 bkeyTo까지 인 element의 개수를 조회한다.
2. timeout은 1초로 지정했다. 지정한 시간에 조회 결과가 넘어 오지 않거나
   JVM의 과부하로 operation queue에서 처리되지 않을 경우 TimeoutException이 발생한다.
3. element개수 조회에 대한 자세한 결과는 future.getOperationStatus().getResponse()를 통해 조회할 수 있다.


## B+Tree Element 조회

B+tree element를 조회하는 함수는 두 유형이 있다.

첫째, B+tree에서 주어진 bkey를 가지고 eFlagFilter조건을 만족하는 element를 조회한다.

```java
CollectionFuture<Map<Long, Element<Object>>>
asyncBopGet(String key, long bkey, ElementFlagFilter eFlagFilter, boolean withDelete, Boolean dropIfEmpty)
CollectionFuture<Map<ByteArrayBKey, Element<Object>>>
asyncBopGet(String key, byte[] bkey, ElementFlagFilter eFlagFilter, boolean withDelete, Boolean dropIfEmpty)
```

둘째, B+tree에서 from부터 to까지의 bkey를 가진 elements를 탐색하면서 eFlagFilter조건을 만족하는 elements 중
offset 번째 element부터 count개의 element를 조회한다.

```java
CollectionFuture<Map<Long, Element<Object>>>
asyncBopGet(String key, long from, long to, ElementFlagFilter eFlagFilter, int offset, int count, boolean withDelete, boolean dropIfEmpty)
CollectionFuture<Map<ByteArrayBKey, Element<Object>>>
asyncBopGet(String key, byte[] from, byte[] to, ElementFlagFilter eFlagFilter, int offset, int count, boolean withDelete, Boolean dropIfEmpty)
```

- key: b+tree item의 key
- bkey 또는 \<from, to\>: element 조회 대상이 되는 bkey 또는 조회 범위를 나타내는 bkey range 
- eFlagFilter: eflag에 대한 filter 조건
- withDelete: element 조회와 함께 그 element를 삭제할 것인지를 지정
- dropIfEmpty: element 삭제로 empty b+tree가 되면, 그 b+tree 자체도 삭제할 것인지를 지정

수행 결과는 future 객체를 통해 얻는다.

future.get() | future.operationStatus().getResponse() | 설명 
------------ | -------------------------------------- | -------
not null     | CollectionResponse.END                 | Element만 조회, 조회 범위에 b+tree trim 영역 없음
not null     | CollectionResponse.TRIMMED             | Element만 조회, 조회 범위에 b+tree trim 영역 있음
not null     | CollectionResponse.DELETED             | Element를 조회하고 삭제한 상태
not null     | CollectionResponse.DELETED_DROPPED     | Element를 조회하고 삭제한 다음 b+tree를 drop한 상태
null         | CollectionResponse.NOT_FOUND           | Key miss (주어진 key에 해당하는 item이 없음)
null         | CollectionResponse.NOT_FOUND_ELEMENT   | 조회된 element가 없음, 조회 범위에 b+tree 영역 없음
null         | CollectionResponse.OUT_OF_RANGE        | 조회된 element가 없음, 조회 범위에 b+tree trim 영역 있음
null         | CollectionResponse.TYPE_MISMATCH       | 해당 key가 b+tree가 아님
null         | CollectionResponse.BKEY_MISMATCH       | 주어진 bkey 유형이 기존 bkey 유형과 다름
null         | CollectionResponse.UNREADABLE          | 해당 key를 읽을 수 없는 상태임. (unreadable item상태)


결과로 반환된 result(Map\<Long, Element\<Object\>\>) 객체에서 다음과 같은 정보를 확인할 수 있다

result 객체의 Method            | 자료형    | 설명
------------------------------|---------|-------------
getKey()                      | Long    | btree내의 position
getValue().getValue()         | Object  | element의 값
getValue().getByteArrayBkey() | byte[]  | element bkey 값(byte[])
getValue().getLongBkey()      | long    | element bkey 값long)
getValue().isByteArrayBkey()  | boolean | element bkey 값 byte array 여부
getValue().getFlag()          | byte[]  | element flag 값(byte[])

B+tree element를 조회하는 예제는 아래와 같다.

```java
String key = "Prefix:BTreeKey";
long from = 1L;
long to = 6L;
int offset = 2;
int count = 3;
boolean withDelete = false;
boolean dropIfEmpty = false;
ElementFlagFilter filter = new ElementFlagFilter(CompOperands.Equal, new byte[] { 1, 1 });
CollectionFuture<Map<Long, Element<Object>>> future = null;

try {
    future = client.asyncBopGet(key, from, to, filter, offset, count, withDelete, dropIfEmpty); // (1)
} catch (IllegalStateException e) {
    // handle exception
}

if (future == null)
    return;

try {
    Map<Long, Element<Object>> result = future.get(1000L, TimeUnit.MILLISECONDS); // (2)
    System.out.println(result);

    CollectionResponse response = future.getOperationStatus().getResponse(); // (3)
    if (response.equals(CollectionResponse.NOT_FOUND)) {
        System.out.println("Key가 없습니다.(Key에 저장된 B+ tree가 없음.");
    } else if (response.equals(CollectionResponse.NOT_FOUND_ELEMENT)) {
        System.out.println("Key에 B+ tree는 존재하지만 저장된 값 중 조건에 맞는 것이 없음.");
    }

} catch (InterruptedException e) {
    future.cancel(true);
} catch (TimeoutException e) {
    future.cancel(true);
} catch (ExecutionException e) {
    future.cancel(true);
}
```

1. bkey가 1부터 6사이에 있고 filter조건을 만족하는 element들 중에서 3번째 위치하는 값부터 3개를 조회한다.
   이 예제에서 eflag filter 조건은 eflag 값이 0x0101과 같은지를 검사한다.
2. timeout은 1초로 지정했다. 지정한 시간에 조회 결과가 넘어 오지 않거나
   JVM의 과부하로 operation queue에서 처리되지 않을 경우 TimeoutException이 발생한다.
   반환되는 Map 인터페이스의 구현체는 TreeMap이며, 그 결과는 다음 중의 하나이다.
   - key 존재하지 않음 : null 반환
   - key 존재하지만 조회 조건을 만족하는 elements 없음: empty map 반환
   - key 존재하고 조회 조건을 만족하는 elements 있음: non-empty map 반환
3. 조회 결과에 대한 상세 정보는 future.getOperationStatus().getResponse()으로 확인한다.


## B+Tree Element 일괄 삽입

B+tree에 여러 element를 한번에 삽입하는 함수는 두 유형이 있다.

첫째, 하나의 key가 가리키는 b+tree에 다수의 element를 삽입하는 함수이다.

```java
CollectionFuture<Map<Integer, CollectionOperationStatus>>
asyncBopPipedInsertBulk(String key, List<Element<Object>> elements, CollectionAttributes attributesForCreate)
CollectionFuture<Map<Integer, CollectionOperationStatus>>
asyncBopPipedInsertBulk(String key, Map<Long, Object> elements, CollectionAttributes attributesForCreate)
```

- key: 삽입 대상 b+tree의 key 
- elements: 삽입할 element들
  - List\<Element\<Object\>\> 유형
  - Map\<Long, Object\> 유형
- attributesForCreate: 대상 b+tree가 없을 시, 동작을 지정한다.
  - null: element 삽입하지 않는다. 
  - attributes: 주어진 attributes를 가진 empty b+tree item 생성 후에 element 삽입한다.


둘째, 여러 key들이 가리키는 b+tree들에 각각 하나의 element를 삽입하는 함수이다. 

```java
Future<Map<String, CollectionOperationStatus>>
asyncBopInsertBulk(List<String> keyList, long bkey, byte[] eFlag, Object value, CollectionAttributes attributesForCreate)
Future<Map<String, CollectionOperationStatus>>
asyncBopInsertBulk(List<String> keyList, byte[] bkey, byte[] eFlag, Object value, CollectionAttributes attributesForCreate)
```

- keyList: 삽입 대상 b+tree들의 key list
- bkey: 삽입할 element의 bkey(b+tree key)
- eflag: 삽입할 element의 eflag(element flag)
- value: 삽입할 element의 value
- attributesForCreate: 대상 b+tree가 없을 시, 동작을 지정한다.
  - null: element 삽입하지 않는다. 
  - attributes: 주어진 attributes를 가진 empty b+tree item 생성 후에 element 삽입한다.


하나의 b+tree에 여러 개의 elements을 bulk insert하고 각각의 element에 대해 insert 결과를 확인하는 코드이다.

```java
String key = "Sample:BTreeBulk";
List<Element<Object>> elements = new ArrayList<Element<Object>>();

elements.add(new Element<Object>(1L, "VALUE1", new byte[] { 1, 1 }));
elements.add(new Element<Object>(2L, "VALUE2", new byte[] { 1, 1 }));
elements.add(new Element<Object>(3L, "VALUE3", new byte[] { 1, 1 }));

boolean createKeyIfNotExists = true;

if (elements.size() > mc.getMaxPipedItemCount()) { // (1)
    System.out.println("insert 할 아이템 개수는 mc.getMaxPipedItemCount개를 초과할 수 없다.");
    return;
}

CollectionFuture<Map<Integer, CollectionOperationStatus>> future = null;

try {
    future = mc.asyncBopPipedInsertBulk(key, elements, new CollectionAttributes()); // (2)

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
2. Key에 저장된 b+ tree에 bulkData를 한꺼번에 insert하고 그 결과를 담은 future객체를 반환한다.
   이 future로부터 각 아이템의 insert성공 실패 여부를 조회할 수 있다.
   여기에서는 attributesForCreate값을 지정하여 bulk insert하기 전에 key가 없으면 생성하고 element를 insert되도록 하였다.
3. delete timeout은 1초로 지정했다. 지정한 시간에 모든 아이템의 insert 결과가 넘어 오지 않거나
   JVM의 과부하로 operation queue에서 처리되지 않을 경우 TimeoutException이 발생한다.
4. 모든 아이템이 insert에 성공하면 empty map이 반환된다.
   - 반환된 Map의 Key= insert한 값(bulkData)를 iteration했을 때의 index값.
   - 반환된 Map의 Value= insert실패사유
5. 일부 실패한 아이템의 실패 원인을 조회하려면 insert할 때 사용된 값(bulkData)의 iteration 순서에 따라
   결과 Map을 조회하면 된다.
6. Future로부터 얻은 Map의 Key가 입력된 값(bulkData)의 index이기 때문에 위와 같은 방법으로 실패 원인을 조회하면 된다.


## B+Tree Element 일괄 변경

B+tree에서 주어진 elements에 해당하는 모든 element의 value 그리고/또는 element flag를 일괄 변경한다.

```java
CollectionFuture<Map<Integer, CollectionOperationStatus>>
asyncBopPipedUpdateBulk(String key, List<Element<Object>> elements)
```
- key: 변경 대상 b+tree의 key
- elements: 변경 대상 elements에 대해 bkey와 eFlagUpate, new value를 가진다.


## B+Tree Element 일괄 조회

다수의 b+tree들 각각에 대해 from부터 to까지의 bkey를 가진 elements를 탐색하면서,
eFlagFilter 조건을 만족하는 elements 중 offset 위치부터 count 개수만큼 조회한다.

```java
CollectionFuture<Map<String, BTreeGetResult<Long, Object>>>
asyncBopGetBulk(List<String> keyList, long from, long to, ElementFlagFilter eFlagFilter, int offset, int count)
CollectionFuture<Map<String, BTreeGetResult<ByteArrayBKey, Object>>>
asyncBopGetBulk(List<String> keyList, byte[] from, byte[] to, ElementFlagFilter eFlagFilter, int offset, int count)
```

- keyList: b+tree items의 key list
- bkey 또는 \<from, to\>: element 조회 대상이 되는 bkey 또는 조회 범위를 나타내는 bkey range 
- eFlagFilter: eflag에 대한 filter 조건
  - eflag filter 조건을 지정하지 않으려면, ElementFlagFilter.DO_NOT_FILTER를 입력한다.
- offset, count: bkey range와 eflag filter 조건을 만족하는 elements에서 실제 조회할 element의 offset과 count 지정


수행 결과는 future 객체를 통해 Map\<Stirng, BTreeGetResult\<Bkey, Object\>\>을 얻으며,
이러한 Map은 개별 b+tree item의 key와 그 b+tree에서 조회한 결과를 담고 있는 BTreeGetResult 객체이다.
BTreeGetResult 객체를 통해 개별 조회 결과를 아래와 같이 조회할 수 있다.

BTreeGetResult.getElements() |  BtreeGetResult.getCollectionResponse() | 설명 
---------------------------- | --------------------------------------- | -------
not null                     | CollectionResponse.OK                   | Element 조회, 조회 범위에 b+tree trim 영역 없음
not null                     | CollectionResponse.TRIMMED              | Element 조회, 조회 범위에 b+tree trim 영역 있음
null                         | CollectionResponse.NOT_FOUND            | Key miss (주어진 key에 해당하는 item이 없음)
null                         | CollectionResponse.NOT_FOUND_ELEMENT    | 조회된 element 없음, 조회 범위에 b+tree trim 영역 없음
null                         | CollectionResponse.OUT_OF_RANGE         | 조회된 element 없음, 조회 범위에 b+tree trim 영역 있음
null                         | CollectionResponse.TYPE_MISMATCH        | 해당 key가 b+tree가 아님
null                         | CollectionResponse.BKEY_MISMATCH        | 주어진 bkey 유형이 기존 bkey 유형과 다름
null                         | CollectionResponse.UNREADABLE           | 해당 key를 읽을 수 없는 상태임. (unreadable item상태)

BTreeGetResult.getElements()로 조회한 BTreeElement 객체로부터 개별 element의 bkey, eflag, value를 조회할 수 있다.

BTreeElement 객체의 Method    | 자료형	           | 설명
--------------------------- | ---------------- | ----
getKey()                    | long 또는 byte[]  | element의 bkey
getEFlag()                  | byte[]           | element flag
getValue()                  | Object           | element의 값

B+tree element 일괄 조회하는 예제는 아래와 같다.

```java
final List<String> keyList = new ArrayList<String>() {
    {
        add("Prefix:BTree1");
        add("Prefix:BTree2");
        add("Prefix:BTree3");
    }
};

ElementFlagFilter filter = ElementFlagFilter.DO_NOT_FILTER;
long bkeyFrom = 0L;
long bkeyTo = 100L;
int queryCount = 10;
int offset = 0;

CollectionGetBulkFuture<Map<String, BTreeGetResult<Long, Object>>> future = null;
Map<String, BTreeGetResult<Long, Object>> results = null;

try {
    future = mc.asyncBopGetBulk(keyList, from, to, filter, offset, count); // (1)
    results = future.get(1000L, TimeUnit.MILLISECONDS);
} catch (InterruptedException e) {
    future.cancel(true);
} catch (TimeoutException e) {
    future.cancel(true);
} catch (ExecutionException e) {
    future.cancel(true);
}

if (results == null) return;

for(Entry<String, BTreeGetResult<Long, Object>> entry : results.entrySet()) { // (2)
    System.out.println("key=" + entry.getKey());
    System.out.println("result code=" + entry.getValue().getCollectionResponse().getMessage()); // (3)

    if (entry.getValue().getElements() != null) { // (4)
        for(Entry<Long, BTreeElement<Long, Object>> el : entry.getValue().getElements().entrySet()) {
            System.out.println("bkey=" + el.getKey());
            System.out.println("eflag=" + Arrays.toString(el.getValue().getEflag());
            System.out.println("value=" + el.getValue().getValue());
        }
    }
}
```

1. keyList로 주어진 b+tree에 저장된 element 중 bkey가 from부터 to사이에 있고 eflag filter를 만족하는 elements 중
  offset 번째부터 count 개 element를 조회한다.
2. 조회 결과는 Map으로 반환되는데 Key는 b+tree의 key가 되고 Map의 value는 각 key에 저장된 element들이다.
3. 조회 결과 map에서 key별로 조회한 BTreeGetResult객체는 조회 결과코드와 해당 b+tree에서 조회된 element를 가진다.
   조회 결과코드에 따라 BTreeGetResult.getElements()의 결과는 null일 수 있다.
4. BTreeGetResult.getElements()로 조회한 BTreeElement객체로부터 element의 bkey, eflag, value를 조회할 수 있다.


## B+Tree Element Sort-Merge 조회

다수의 B+tree들에 대해 element 조회를 sort-merge 방식으로 수행하는 기능이다.
물리적으로 여러 b+tree들로 구성되지만, 이들이 논리적으로 하나의 거대한 b+tree라 가정하고, 
이러한 b+tree에 대해 element 조회를 수행하는 기능이다.

smget 동작은 조회 범위와 어떤 b+tree의 trim 영역과의 겹침에 대한 처리로,
아래 두 가지 동작 모드가 있다.

1) 기존 Sort-Merge 조회 (1.8.X 이하 버전에서 동작하던 방식)
   - smget 조회 조건을 만족하는 첫번째 element가 trim된 b+tree가 하나라도 존재하면 OUT_OF_RANGE 응답을 보낸다.
     이 경우, 응용은 모든 key에 대해 백엔드 저장소인 DB에서 elements 조회한 후에
     응용에서 sort-merge 작업을 수행하여야 한다.
   - OUT_OF_RANGE가 없는 상황에서 smget을 수행하면서
     조회 조건을 만족하는 두번째 이후의 element가 trim된 b+tree를 만나게 되면,
     그 지점까지 조회한 elements를 최종 elements 결과로 하고
     smget 수행 상태는 TRIMMED로 하여 응답을 보낸다.
     이 경우, 응용은 모든 key에 대해 백엔드 저장소인 DB에서 trim 영역의 elements를 조회하여
     smget 결과에 반영하여야 한다.

2) 신규 Sort-Merge 조회 (1.9.0 이후 버전에서 추가된 방식)
   - 기존의 OUT_OF_RANGE에 해당하는 b+tree를 missed keys로 분류하고
     나머지 b+tree들에 대해 smget을 계속 수행한다.
     따라서, 응용에서는 missed keys에 한해서만
     백엔드 저장소인 DB에서 elements를 조회하여 최종 smget 결과에 반영할 수 있다.
   - smget 조회 조건을 만족하는 두번째 이후의 element가 trim된 b+tree가 존재하더라도,
     그 지점에서 smget을 중지하는 것이 아니라, 그러한 b+tree를 trimmed keys로 분류하고
     원하는 개수의 elements를 찾을 때까지 smget을 계속 진행한다.
     따라서, 응용에서는 trimmed keys에 한하여
     백엔드 저장소인 DB에서 trim된 elements를 조회하여 최종 smget 결과에 반영할 수 있다.
   - bkey에 대한 unique 조회 기능을 지원한다.
     중복 bkey를 허용하여 조회하는 duplcate 조회 외에
     중복 bkey를 제거하고 unique bkey만을 조회하는 unique 조회를 지원한다.
   - 조회 조건에 offset 기능을 제거한다.

기존 smget 연산을 사용하더라도, offset 값은 항상 0으로 사용하길 권고한다.
양수의 offset을 사용하는 smget에서 missed keys가 존재하고
missed keys에 대한 DB 조회가 offset으로 skip된 element를 가지는 경우,
응용에서 정확한 offset 처리가 불가능해지기 때문이다.
이전의 조회 결과에 이어서 추가로 조회하고자 하는 경우,
이전에 조회된 bkey 값을 바탕으로 bkey range를 재조정하여 사용할 수 있다.

여러 b+tree들에 대해 sort-merge get을 수행하는 함수는 아래와 같다.
여러 b+tree들로 부터 from부터 to까지의 bkey를 가지고 있으면서 eflag filter조건을 만족하는 element를 찾아
sort merge하면서, count개의 element를 조회한다. 

```java
SMGetFuture<List<SMGetElement<Object>>>
asyncBopSortMergeGet(List<String> keyList, long from, long to, ElementFlagFilter eFlagFilter, int count, SMGetMode smgetMode)
SMGetFuture<List<SMGetElement<Object>>>
asyncBopSortMergeGet(List<String> keyList, byte[] from, byte[] to, ElementFlagFilter eFlagFilter, int count, SMGetMode smgetMode)
```

- keyList: b+tree items의 key list
- \<from, to\>: 조회 범위를 나타내는 bkey range 
- eFlagFilter: eflag에 대한 filter 조건
  - eflag filter 조건을 지정하지 않으려면, ElementFlagFilter.DO_NOT_FILTER를 입력한다.
- count: bkey range와 eflag filter 조건을 만족하는 elements에서 실제 조회할 element의 count 지정
  - **제약 조건으로 1000이하이어야 한다.**
  - 이는 sort-merge get 연산이 부담이 너무 크지 않은 연산으로 제한하기 위한 것이다.
- smgetMode: smget에 대해서 mode를 지정하는 flag
  - unique 조회 또는 duplicate 조회를 지정한다.

수행 결과는 future 객체를 통해 얻는다.

future.get() | future.operationStatus().getResponse() | 설명 
------------ | -------------------------------------- | -------
not null     | CollectionResponse.END                 | Element 조회, No duplicate bkey
not null     | CollectionResponse.DUPLICATED          | Element 조회, Duplicate bkey 존재
null         | CollectionResponse.TYPE_MISMATCH       | 해당 key가 b+tree가 아님
null         | CollectionResponse.BKEY_MISMATCH       | 주어진 bkey 유형이 기존 bkey 유형과 다름
null         | CollectionResponse.ATTR_MISMATCH       | sort-merge get에 참여한 b+tree의 속성이 서로 다름, arcus-memcached 1.11.3 이후 속성 제약이 사라짐


B+tree element sort-merge 조회하는 예제는 아래와 같다.

```java
List<String> keyList = new ArrayList<String>() {{
    add("Prefix:KeyA");
    add("Prefix:KeyB");
    add("Prefix:KeyC");
}};


long bkeyFrom = 0L; // (1)
long bkeyTo = 100L;
int queryCount = 10;

SMGetMode smgetMode = SMGetMode.DUPLICATE;
SMGetFuture<List<SMGetElement<Object>>> future = null;

try {
    future = mc.asyncBopSortMergeGet(keyList, bkeyFrom, bkeyTo, ElementFlagFilter.DO_NOT_FILTER, queryCount, smgetMode); // (2)
} catch (IllegalStateException e) {
    // handle exception
}

if (future == null)
    return;

try {
    List<SMGetElement<Object>> result = future.get(1000L, TimeUnit.MILLISECONDS); // (3)
    for (SMGetElement<Object> element : result) { // (4)
        System.out.println(element.getKey());
        System.out.println(element.getBkey());
        System.out.println(element.getValue());
    }

    for (Map.Entry<String, CollectionOperationStatus> m : future.getMissedKeys().entrySet()) {  // (5)
        System.out.print("Missed key : " + m.getKey());
        System.out.println(", response : " + m.getValue().getResponse());
    }
    
    for (SMGetTrimKey e : future.getTrimmedKeys()) { // (6)
        System.out.println("Trimmed key : " + e.getKey() + ", bkey : " + e.getBkey());
    }
} catch (InterruptedException e) {
    future.cancel(true);
} catch (TimeoutException e) {
    future.cancel(true);
} catch (ExecutionException e) {
    future.cancel(true);
}
```

1. 예제는 “KeyA”, “KeyB”, “KeyC”에 저장된 element들 중 bkey가 0부터 100까지 해당하는 element들 10개를 조회한다.
   - 주의할 점은 key로 주어진 b+tree의 attribute설정은 모두 같아야 한다. 그렇지 않으면 오류가 발생한다.
2. ElementFlagFilter는 bkey에 지정된 eflag가 elementFlagFIlter로 지정된 조건을 만족하는 element들만 조회하는 조건이다    
   예제에서는 eflag filter를 사용하지 않음으로 조회하였다.
3. timeout은 1초로 지정했다. 지정한 시간에 조회 결과가 넘어 오지 않거나
   JVM의 과부하로 operation queue에서 처리되지 않을 경우 TimeoutException이 발생한다.
4. 조회된 값은 List\<SMGetElement\>형태로 반환된다. 이로부터 조회된 element를 조회할 수 있다.
   조회 결과에 동일한 bkey가 존재하면 key를 기준으로 정렬되어 반환된다.
5. 조회할 때 지정한 key들 중에 smget에 참여하지 key들은 future.getMissedKeys()를 통해 Map 형태로 실패 원인과 함께 조회할 수 있다.
   - 실패원인은 cache miss(NOT_FOUND), unreadable 상태(UNREADABLE), bkey 범위 조회를 만족하는 처음 bkey가 trim된 상태(OUT_OF_RANGE) 중 하나이다.
   - 응용은 이들 키들에 대해서는 back-end storage인 DB에서 동일 조회 조건으로 elements를 검색하여 sort-merge 결과에 반영하여야 한다.
6. bkey 조회 범위의 처음 bkey가 존재하지만 bkey 범위의 끝에 다다르기 전에 trim이 발생한 key와 trim 직전에 cache에 있는 마지막 bkey를 조회할 수 있다.
   - 응용은 이들 키들에 대해 trim 직전 마지막 bkey 이후에 trim된 bkey들을 back-end storage인 DB에서 조회하여 sort-merge 결과에 반영하여야 한다.
7. Sort merge get의 최종 수 결과는 future.getOperationStatus().getResponse()를 통해 조회할 수 있다.


## B+Tree Position 조회

B+tree의 검색 조건으로 각 엘리먼트의 위치(position) 정보를 사용할 수 있다. 여기서 위치란 B+tree 안에서 bkey를 통해 일렬로 정렬되어 있는 각 엘리먼트의 인덱스를 뜻하며, 0부터 count-1 까지 순서대로 매겨진다. 순서에 대한 기준으로 오름차순(ASC)과 내림차순(DESC)이 지원된다.


B+tree에서 주어진 bkey에 해당하는 element가 주어진 order에 따라 어떤 위치(position)에 있는지 조회하는 함수는 아래와 같다.

```java
CollectionFuture<Integer> asyncBopFindPosition(String key, long bkey, BTreeOrder order)
CollectionFuture<Integer> asyncBopFindPosition(String key, byte[] bkey, BTreeOrder order)
```

- key: b+tree item의 key
- bkey: 조회할 element의 bkey(b+tree bkey)
- order: 위치(position) 기준을 정의한다. (오름차순 : BTreeOrder.ASC, 내림차순 : BTreeOrder.DESC)

수행 결과는 future 객체를 통해 얻는다.

future.get()     | future.operationStatus().getResponse() | 설명
---------------- | -------------------------------------- | ---------
element position | CollectionResponse.OK                  | Element 위치를 성공적으로 조회
null             | CollectionResponse.NOT_FOUND           | Key miss (주어진 key에 해당하는 item이 없음)
null             | CollectionResponse.NOT_FOUND_ELEMENT   | Element miss
null             | CollectionResponse.TYPE_MISMATCH       | 해당 item이 b+tree가 아님
null             | CollectionResponse.BKEY_MISMATCH       | 주어진 bkey 유형이 기존 bkey 유형과 다름
null             | CollectionResponse.UNREADABLE          | 해당 key가 unreadable상태임

B+tree position 조회 예제는 아래와 같다.

```java
String key = "BopFindPositionTest";
long[] longBkeys = { 10L, 11L, 12L, 13L, 14L, 15L, 16L, 17L, 18L, 19L };

public void testLongBKeyAsc() throws Exception {
	// insert
	CollectionAttributes attrs = new CollectionAttributes();
	for (long each : longBkeys) {
		arcusClient.asyncBopInsert(key, each, null, "val", attrs).get();
	}
	
	// bop position
	for (int i=0; i<longBkeys.length; i++) {
		CollectionFuture<Integer> f = arcusClient.asyncBopFindPosition(key, longBkeys[i], BTreeOrder.ASC);
		Integer position = f.get();
		assertNotNull(position);
		assertEquals(CollectionResponse.OK, f.getOperationStatus().getResponse());
		assertEquals(i, position.intValue());
	}
}

public void testLongBKeyDesc() throws Exception {
	// insert
	CollectionAttributes attrs = new CollectionAttributes();
	for (long each : longBkeys) {
		arcusClient.asyncBopInsert(key, each, null, "val", attrs).get();
	}
	
	// bop position
	for (int i=0; i<longBkeys.length; i++) {
		CollectionFuture<Integer> f = arcusClient.asyncBopFindPosition(key, longBkeys[i], BTreeOrder.DESC);
		Integer position = f.get();
		assertNotNull(position);
		assertEquals(CollectionResponse.OK, f.getOperationStatus().getResponse());
		assertEquals("invalid position", longBkeys.length-i-1, position.intValue());
	}
}
```

## B+Tree Position 기반의 Element 조회

B+tree에서 하나의 position 또는 position range에 해당하는 elements를 조회하는 함수이다.

```java
CollectionFuture<Map<Integer, Element<Object>>>
asyncBopGetByPosition(String key, BTreeOrder order, int pos)
CollectionFuture<Map<Integer, Element<Object>>>
asyncBopGetByPosition(String key, BTreeOrder order, int from, int to)
```

- key: b+tree item의 key
- order: 위치(position) 기준을 정의한다. (오름차순 : BTreeOrder.ASC, 내림차순 : BTreeOrder.DESC)
- pos or \<from, to\>: 위치(position)를 하나만 지정하거나 범위로 지정할 수 있다.


수행 결과는 future 객체를 통해 얻는다.

future.get() | future.operationStatus().getResponse() | 설명 
------------ | -------------------------------------- | ---------
not null     | CollectionResponse.END                 | Element를 성공적으로 조회
null         | CollectionResponse.NOT_FOUND           | Key miss (주어진 key에 해당하는 item이 없음)
null         | CollectionResponse.NOT_FOUND_ELEMENT   | Element miss
null         | CollectionResponse.TYPE_MISMATCH       | 해당 item이 b+tree가 아님
null         | CollectionResponse.UNREADABLE          | 해당 key가 unreadable상태임


B+tree에서 position 기반의 element 조회 예제이다.

```java
String key = "BopGetByPositionTest";
long[] longBkeys = { 10L, 11L, 12L, 13L, 14L, 15L, 16L, 17L, 18L, 19L };

public void testLongBKeyMultiple() throws Exception {
	// 10개의 테스트 데이터를 insert 한다.
	CollectionAttributes attrs = new CollectionAttributes();
	for (long each : longBkeys) {
		arcusClient.asyncBopInsert(key, each, null, "val", attrs).get();
	}
	
	// 테스트 : 5 부터 8 위치의 엘리먼트를 조회한다.
	int posFrom = 5;
	int posTo = 8;
	CollectionFuture<Map<Integer, Element<Object>>> f = arcusClient
			.asyncBopGetByPosition(key, BTreeOrder.ASC, posFrom, posTo);
	Map<Integer, Element<Object>> result = f.get(1000,
			TimeUnit.MILLISECONDS);

	assertEquals(4, result.size());
	assertEquals(CollectionResponse.END, f.getOperationStatus().getResponse());

	int count = 0;
	for (Entry<Integer, Element<Object>> each : result.entrySet()) {
		int currPos = posFrom + count++;
		assertEquals("invalid index", currPos, each.getKey().intValue());
		assertEquals("invalid bkey", longBkeys[currPos], each.getValue().getLongBkey());
		assertEquals("invalid value", "val", each.getValue().getValue());
	}
}
```

## B+Tree Position과 Element 동시 조회

B+tree의 검색 조건으로 특정 엘리먼트의 위치(position) 를 기준으로 주변(앞/뒤 position) 엘리먼트들을 조회 할 수 있다.  여기서 위치란 B+tree안에서 bkey를 통해 일렬로 정렬되어 있는 각 엘리먼트의 인덱스를 뜻하며, 0부터 count-1까지 순서대로 매겨진다. 순서에 대한 기준으로 오름차순(ASC)과 내림차순(DESC)이 지원된다.

B+tree에서 bkey에 해당하는 position을 기준으로 count만큼의 element를 주어진 order에 따라서 조회하는 함수는 아래와 같다.
```java
CollectionFuture<Map<Integer, Element<Object>>>
asyncBopFindPositionWithGet(String key, long longBKey, BTreeOrder order, int count)
CollectionFuture<Map<Integer, Element<Object>>>
asyncBopFindPositionWithGet(String key, byte[] byteArrayBKey, BTreeOrder order, int count)
```

- key: b+tree item의 key
- longBKey: 조회할 element의 bkey(b+tree bkey)
- order: longBKey에 해당하는 element의 위치(position) 기준으로 결과를 담을 순서를 정의한다. (오름차순: BTreeOrder.ASC, 내림차순: BTreeOrder.DESC)
- count: longBKey에 해당하는 element의 위치(position) 기준으로 조회할 주변(앞/뒤 position) element 개수를 지정

수행 결과는 future 객체를 통해 얻는다.

future.get() | future.operationStatus().getResponse() | 설명
------------ | -------------------------------------- | ---------
not null     | CollectionResponse.END                 | Element를 성공적으로 조회
null         | CollectionResponse.NOT_FOUND           | Key miss (주어진 key에 해당하는 item이 없음)
null         | CollectionResponse.NOT_FOUND_ELEMENT   | Element miss
null         | CollectionResponse.TYPE_MISMATCH       | 해당 item이 b+tree가 아님
null         | CollectionResponse.BKEY_MISMATCH       | 주어진 bkey 유형이 기존 bkey 유형과 다름
null         | CollectionResponse.UNREADABLE          | 해당 key가 unreadable상태임

결과로 반환된 result(Map\<Integer, Element\<Object\>\>) 객체에서 다음과 같은 정보를 확인할 수 있다

result 객체의 Method                | 자료형              | 설명
----------------------------------|-------------------|---------------
getKey()                          | integer           | btree내의 position
getValue().getValue()             | Object            | element의 값
getValue().getByteArrayBkey()     | byte[]            | element bkey 값(byte[])
getValue().getLongBkey()          | long              | element bkey 값(long)
getValue().isByteArrayBkey()      | boolean           | element bkey 값 byte array 여부
getValue().getFlag()              | byte[]            | element flag 값(byte[])

B+tree에서 position과 element 동시 조회 예제이다.
```java
String key = "BopFindPositionWithGetTest";

public void testLongBKey() throws Exception {
        long longBkey, resultBkey;
        int  totCount = 100;
        int  pwgCount = 10;
        int  rstCount;
        int  position, i;

        // totCount개의 테스트 데이터를 insert한다.
        CollectionAttributes attrs = new CollectionAttributes();
        for (i = 0; i < totCount; i++) {
                longBkey = (long)i;
                arcusClient.asyncBopInsert(key, longBkey, null, "val", attrs).get();
        }

        for (i = 0; i < totCount; i++) {
                // longBkey를 bkey로 가지는 element의 position을 기준으로 주변(앞/뒤 position) element들을 pwgCount만큼 조회
                longBkey = (long)i;
                CollectionFuture<Map<Integer, Element<Object>>> f = arcusClient
                                .asyncBopFindPositionWithGet(key, longBkey, BTreeOrder.ASC, pwgCount);
                Map<Integer, Element<Object>> result = f.get(1000, TimeUnit.MILLISECONDS);

                if (i >= pwgCount && i < (totCount-pwgCount)) {
                        rstCount = pwgCount + 1 + pwgCount;
                } else {
                        if (i < pwgCount)
                        rstCount = i + 1 + pwgCount;
                        else
                        rstCount = pwgCount + 1 + ((totCount-1)-i);
                }
                assertEquals(rstCount, result.size());
                assertEquals(CollectionResponse.END, f.getOperationStatus().getResponse());

                if (i < pwgCount) {
                        position = 0;
                } else {
                        position = i - pwgCount;
                }
                resultBkey = position;
                for (Entry<Integer, Element<Object>> each : result.entrySet()) {
                        assertEquals("invalid position", position, each.getKey().intValue());
                        assertEquals("invalid bkey", resultBkey, each.getValue().getLongBkey());
                        assertEquals("invalid value", "val", each.getValue().getValue());
                        position++; resultBkey++;
                }
        }
}
```
