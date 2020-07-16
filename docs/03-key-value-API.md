## Key-Value Item

Key-value item은 하나의 key에 대해 하나의 value만을 저장하는 item이다.

**제약조건**
- Key의 최대 크기는 250 character이다.
- Value는 최대 1Mb까지 저장할 수 있다.

Key-value item에 대해 수행가능한 연산들은 아래와 같다.

- [Key-Value Item 저장](03-key-value-API.md#key-value-item-%EC%A0%80%EC%9E%A5)
- [Key-Value Item 조회](03-key-value-API.md#key-value-item-%EC%A1%B0%ED%9A%8C)
- [Key-Value Item 값의 증감](03-key-value-API.md#key-value-item-%EA%B0%92%EC%9D%98-%EC%A6%9D%EA%B0%90)
- [Key-Value Item 삭제](03-key-value-API.md#key-value-item-%EC%82%AD%EC%A0%9C)


### Key-Value Item 저장

key-value item을 저장하는 API로 set, add, replace를 제공한다.

```java
Future<Boolean> set(String key, int exp, Object obj)
Future<Boolean> add(String key, int exp, Object obj)
Future<Boolean> replace(String key, int exp, Object obj)
```

- \<key, obj\>의 key-value item을 저장한다.
- Cache에 해당 key의 존재 여부에 따라 각 API 동작은 다음과 같다.
  - set은 \<key, obj\> item을 무조건 저장한다. 해당 key가 존재하면 교체하여 저장하다.
  - add는 해당 key가 없을 경우만, \<key, obj\> item을 저장한다.
  - replace는 해당 key가 있을 경우만, \<key, obj\> item을 교체하여 저장한다.
- 저장된 key-value item은 exp 초 이후에 삭제된다.

key-vlaue item에 주어진 value를 추가하는 API로 prepend, append를 제공한다.

```java
Future<Boolean> prepend(long cas, String key, Object val)
Future<Boolean> append(long cas, String key, Object val)
```

- key-value item에서 value 추가 위치는 API에 따라 다르다.
  - prepend는 item의 value 부분에서 가장 앞쪽에 추가한다.
  - append는 item의 value 부분에서 가장 뒤쪽에 추가한다.
- 첫째 인자인 cas는 현재 이용되지 않으므로 임의의 값을 주면 된다.
  초기에 CAS(compare-and-set) 연산으로 수행하기 위한 용도로 필요했던 인자이다.

한번의 API 호출로 다수의 key-value items을 set하는 bulk API를 제공한다.

```java
Future<Map<String, OperationStatus>> asyncStoreBulk(StoreType type, List<String> key, int exp, Object obj)
Future<Map<String, OperationStatus>> asyncStoreBulk(StoreType type, Map<String, Object> map, int exp)
```

- 다수의 key-value item을 한번에 저장한다.
  - 전자 API는 key list의 모든 key에 대해 동일한 obj로 저장 연산을 한번에 수행한다.  
  - 후자 API는 map에 있는 모든 \<key, obj\>에 대해 저장 연산을 한번에 수행한다.
- 저장된 key-value item들은 모두 exp 초 이후에 삭제된다.
- StoreType은 연산의 저장 유형을 지정한다. 아래의 유형이 있다.
  - StoreType.set
  - StoreType.add
  - StoreType.replace

expiration은 key가 현재 시간부터 expire 될 때까지의 시간(초 단위)을 입력한다.
시간이 30일을 초과하는 경우 expire 될 unix time을 입력한다.
그 외에 expire 되지 않도록 하기 위해 아래 값을 지정할 수 있다.

- 0: key가 expire 되지 않도록 설정한다. 하지만 Arcus cache server의 메모리가 부족한 경우 LRU에 의해 언제든지 삭제될 수 있다.
- -1: key를 sticky item으로 만든다. Sticky item은 expire 되지 않으며 LRU에 의해 삭제되지도 않는다.

저장에 실패한 키와 실패 원인은 future 객체를 통해 Map 형태로 조회할 수 있다.

future.get(key).getStatusCode() | 설명
--------------------------------| ---------
StatusCode.ERR_NOT_FOUND        | Key miss (주어진 key에 해당하는 item이 없음)
StatusCode.ERR_EXISTS           | 동일 key가 이미 존재함


### Key-Value Item 조회

하나의 key를 가진 cache item에 저장된 value를 조회하는 API를 제공한다.

```java
Future<Object> asyncGet(String key)
```

- 주어진 key에 저장된 value를 반환한다.

여러 key들의 value들을 한번에 조회하는 bulk API를 제공한다.

```java
Future<Map<String,Object>> asyncGetBulk(Collection<String> keys)
Future<Map<String,Object>> asyncGetBulk(String... keys)
```

- 다수 key들에 저장된 value를 Map<String, Object> 형태로 반환한다.
- 다수 key들은 String 유형의 Collection이거나 String 유형의 나열된 key 목록일 수 있다.


### Key-Value Item 값의 증감

key-value item에서 value 부분의 값을 증가시키거나 감소시키는 연산이다. 
(**[주의] 증감 연산을 사용하려면, 반드시 value 값이 String 유형의 숫자 값이어야 한다.**)


```java
Future<Long> asyncIncr(String key, int by)
Future<Long> asyncDecr(String key, int by)
```

- key에 저장된 정수형 데이터의 값을 by 만큼 증가/감소시킨다.
  key가 cache에 존재하지 않으면 증감연산은 수행되지 않는다.
- 반환되는 값은 증감 후의 값이다. 


```java
Future<Long> asyncIncr(String key, int by, long def, int exp)
Future<Long> asyncDecr(String key, int by, long def, int exp)
```

- key에 저장된 정수형 데이터의 값을 by 만큼 증가/감소시킨다.
  key가 cache에 존재하지 않으면 \<key, def\> item을 추가하며, exp 초 이후에 삭제된다.
- 반환되는 값은 증감 후의 값이다.


### Key-Value Item 삭제

하나의 key에 대한 item을 삭제하는 API와
여러 key들의 item들을 한번에 삭제하는 bulk API를 제공한다.

```java
Future<Boolean> delete(String key)
```

- 주어진 key를 가진 item을 cache에서 삭제한다.
 
```java
Future<Map<String, OperationStatus>> asyncDeleteBulk(List<String> key)
Future<Map<String, OperationStatus>> asyncDeleteBulk(String... key)
```

- 다수의 key-value item을 한번에 delete한다.
- 다수 key들은 String 유형의 List이거나 String 유형의 나열된 key 목록일 수 있다.

delete 실패한 키와 실패 원인은 future 객체를 통해 Map 형태로 조회할 수 있다.

future.get(key).getStatusCode() | 설명
--------------------------------| ---------
StatusCode.ERR_NOT_FOUND        | Key miss (주어진 key에 해당하는 item이 없음)
