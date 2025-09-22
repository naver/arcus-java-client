# Future API
## get, getSome

Arcus java client는 비동기 연산에 반환 값으로 Future를 구현한 반환 타입을 제공하고 있다.
이 구현체들을 이용해 단순히 비동기 연산 값을 반환 받는 것 뿐만 아니라 추가적인 기능을 지원하고 있다.
그중에 `get()`과 `getSome()` API를 통해 연산된 데이터를 얻는 방법에 차이가 있다. 이를 알아보도록 하자  

### get
일반적인 Future의 `get()`은 timeout 없이 연산이 완료될 때까지 대기하여 연산된 값을 반환한다.
이와 다르게 Arcus java client에서는 default timeout이 설정되어 있어서, 설정된 시간이 지나면 그동안 처리된 결과는
모두 버려지고 OperationTimeoutException과 함께 null이 반환된다. (all or nothing)

```java
V get()
V get(long timeout, TimeUnit unit)
```

- `get()`은 default timeout인 700ms 만큼 대기한다. 
  대기한 700ms 시간 안에 응답이 없으면, `OperationTimeoutException`이 발생한다.
- `get(long timeout, TimeUnit unit)` 메소드는 `get()` 메소드의 동작과 동일하지만, timeout 시간을 명시적으로 지정할 수 있다.
- `get()` 연산의 default timeout은 `ConnectionFactoryBuilder`의
  `setOpTimeout()` 메소드를 이용하여 변경할 수 있다.
- 연산 작업 처리 도중 해당 연산 작업이 인터럽트 되면 `InterruptedException`을 발생시킨다.
- 연산 작업 처리 도중 위와 언급한 예외 이외에 다른 예외가 발생하면 `ExecutionException`을 발생시킨다.

### getSome
`asyncGetBulk()`, `asyncGetsBulk()` API 연산을 통해 반환받는 BulkGetFuture에서는 `getSome()` API를 지원한다.  
이 API는 timeout이 발생하면 `get()` API와 같이 데이터를 버리고 null을 반환하는 것이 아니라,
그 시점까지 처리된 결과를 반환한다 (partial). 따라서, 응용에서는 반환된 데이터를 사용할 수 있고, 
반환되지 않은 데이터만 백엔드 데이터베이스에서 조회하면 되므로 리소스의 낭비를 줄일 수 있다.

```java
V getSome(long timeout, TimeUnit unit)
```
  
아래 코드는 `get()`과 `getSome()`을 실행한 예제코드이다. 이를 통해 `get()`과 `getSome()` 차이를 알 수 있다.
`get()`은 지정한 시간이 지나면 `TimeoutException`을, `getSome()`은 지정 시간동안 처리된 결과를 반환받는다.

```java
List<String> keyList = new ArrayList<String>();
for(int i = 1; i < 300; i++) {
  client.set("key" + i, 5 * 60, "value");
  keyList.add("key"+i);
}

Map<String, Object> map = null;
BulkFuture<Map<String, Object>> future = client.asyncGetBulk(keyList);

try {
  map = future.get(6, TimeUnit.MILLISECONDS);
} catch (ExecutionException e) {
  e.printStackTrace();
} catch (TimeoutException e) {
  e.printStackTrace();
}

System.out.println(map);

Map<String, Object> map2 = null;
BulkFuture<Map<String, Object>> future2 = client.asyncGetBulk(keyList);
try {
  map2 = future2.getSome(6, TimeUnit.MILLISECONDS);
} catch (ExecutionException e) {
  e.printStackTrace();
}

System.out.println(map2);
```

해당 결과는 아래와 같다.
```
# get()
null

# getSome()
{
key118=value, key117=value, key114=value, key234=value, key113=value, key237=value, key115=value, key231=value, key230=value,
key42=value, key111=value, key199=value, key232=value, key37=value, key36=value, key35=value, key34=value, key190=value, 
key38=value, key191=value, key129=value, key128=value, key249=value, key51=value, key125=value, key124=value, key245=value, 
key248=value, key126=value, key247=value, key55=value, key121=value, key242=value, key54=value, key53=value, key243=value,
key47=value, key46=value, key217=value, key216=value, key219=value, key62=value, key61=value, key179=value, key60=value, 
key215=value, key297=value, key65=value, key175=value, key211=value, key299=value, key177=value, key58=value, key171=value, 
key292=value, key174=value, key295=value, key56=value, key170=value, key107=value, key228=value, key106=value, key109=value, 
key103=value, key224=value, key102=value, key71=value, key105=value, key226=value, key225=value, key187=value, key76=value, 
key189=value, key74=value, key100=value, key221=value, key183=value, key68=value, key185=value, key67=value, key180=value
}
```
- get()은 timeout에 의해 null을 반환하였다. (all or nothing)
- getSome()은 timeout 시점까지 조회된 결과를 반환하였다. (partial)

## getStatus, getOperationStatus
Arcus java client의 Future 구현체들 중 `getStatus()` 또는 `getOperationStatus()` API를 제공하는 경우가 있다.
이 API들은 연산의 상태 정보를 반환하는 API이다. 에러가 발생하면 그 상태 정보를 반환하고, 
정상적으로 처리되었으면 성공 상태 정보를 반환한다.

사용자가 직접 Future를 통해 명령을 cancel 시키거나 내부적으로 cancel 된 경우 CANCELED 상태 정보를 반환한다.
하지만 이 정보는 추후 서버로부터 응답이 와서 응답을 읽었을 때 덮어쓰여질 수 있다.

```java
OperationStatus getStatus()
CollectionOperationStatus getOperationStatus()
```
