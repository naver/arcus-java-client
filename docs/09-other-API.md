## Other API

본 절에서는 아래의 나머지 API들을 설명한다.

- [Flush](09-other-API.md#flush)

### Flush

ARCUS는 prefix단위로 flush하는 기능을 제공한다.
캐시 서버에 저장된 모든 데이터들 중 특정 prefix를 사용하는 모든 key들을 한번의 요청으로 삭제할 수 있다. 단, Front Cache의 데이터는 flush 되지 않는다.

```java
OperationFuture<Boolean> flush(String prefix)
```

정상적으로 prefix가 제거되었을 경우 true를 반환하며 만약 해당 prefix가 존재하지 않아 제거에 실패하였을 경우 false를 반환한다.

**특정 prefix의 모든 item들을 삭제하는 기능이기 때문에 그 사용에 주의하여야 한다.**
**특히, prefix를 입력하지 않으면, cache node의 모든 item들이 삭제므로 공용으로 사용하는 cloud에선 각별히 주의해야 한다.**


아래는 특정 prefix를 flush하는 예제이다.

```java
OperationFuture<Boolean> future = null; 
try { 
    future = client.flush(“myprefix”); 
    boolean result = future.get(1000L, TimeUnit.MILLISECONDS); 
    System.out.println(result); 
} catch (InterruptedException e) { 
    future.cancel(true); 
} catch (TimeoutException e) { 
    future.cancel(true); 
} catch (ExecutionException e) { 
    future.cancel(true); 
}
```

예를 들어, ARCUS 서버에 다음 key 들이 저장되어 있다고 가정하자.

- mydata:subkey1
- mydata:subkey2
- yourdata:subkey3
- ourdata:subkey4
- theirdata:subkey5

위 코드에서 client.flush(“myprefix”)를 호출하면 “myprefix”를 prefix로 사용하는 mydata:subkey1, mydata:subkey2 두 key가 ARCUS에서 제거된다. 물론 ARCUS cloud를 구성하는 모든 서버에 대해서 수행된 결과이며 성공했을 경우 future.get()은 true를 반환한다.


