# 12. Hot Key Replication 기능

## 개요
분산 캐시 환경에서 특정 Key에 대한 요청이 급증하는 경우, Key가 저장된 특정 캐시 노드에만 부하가 집중될 수 있다. 
이로 인해 해당 노드의 응답 속도가 저하되거나 장애가 발생하면 전체 서비스에 영향을 줄 수 있다.

ARCUS Java Client는 이러한 문제를 해결하기 위해 Hot Key Replication(복제) 기능을 제공한다.
이 기능은 클라이언트 레벨에서 하나의 아이템을 여러 개의 복제본으로 생성하여 가능한 한 서로 다른 캐시 노드에 분산 저장하고, 
조회 시에는 이 중 하나를 임의로 선택하여 읽어옴으로써 읽기 부하를 효과적으로 분산시킨다.

## 동작 원리
Hot Key Replication은 ARCUS의 표준 분산 방식인 Consistent Hashing을 그대로 활용한다.

1. 저장 (Set): 사용자가 요청한 replicaCount 만큼 복제 키를 생성한다. 
    - 생성 규칙: 원본키 + # + 인덱스 (예: `hot:item#0`, `hot:item#1`, `hot:item#2`)
    - 생성된 복제 키들은 해시 알고리즘에 의해 캐시 클러스터 내 여러 노드로 분산되어 저장된다.

2. 조회 (Get): 사용자가 조회 요청을 하면, 클라이언트는 0 ~ replicaCount-1 사이의 인덱스 중 하나를 무작위로 선택하여 복제 키를 생성하고 조회한다. 
이를 통해 읽기 요청이 여러 노드로 자연스럽게 분산된다.

## 주의 사항
- **데이터를 저장할 때 사용한 replicaCount와 조회할 때 사용하는 replicaCount는 반드시 동일해야 한다.** 
  서로 다를 경우 존재하지 않는 복제 키를 조회하거나, 일부 데이터를 조회하지 못할 수 있다.
- 복제본 간의 데이터 동기화는 클라이언트의 setReplicas 호출 시점에 이루어진다. 
  따라서 갱신 중 아주 짧은 순간이나 일부 노드 저장 실패 시, 복제본 간 데이터가 일치하지 않을 수 있다.
- 현재는 Key-Value (KV) 아이템에 대해서만 지원하며, List, Set, B+Tree 등의 Collection 타입은 지원하지 않는다.

## 사용법
### 1. 데이터 저장 (`setReplicas()`)
Hot Key를 여러 노드에 복제하여 저장한다. 내부적으로 `asyncStoreBulk()`를 사용하여 병렬로 저장을 수행한다.

```java
String key = "product:12345:best_seller";
int replicaCount = 3;
int expTime = 60;
String value = "This is hot data";

Future<Map<String, OperationStatus>> future = 
        client.setReplicas(key, replicaCount, expTime, value); // (1)

try {
  Map<String, OperationStatus> result = future.get(); // (2)

  for (Map.Entry <String, OperationStatus> entry : result.entrySet()) {
    if (!entry.getValue().isSuccess()) { // (3)
      System.err.println("Failed to store replica: " + entry.getKey());
    }
  }
} catch (Exception e) {
  // handle exception
  return;
}
```

1. `setReplicas`를 호출하여 비동기적으로 replicaCount만큼의 복제본 저장을 시도한다. 
  반환된 Future 객체는 각 복제 키별 저장 결과를 담고 있다. 
2. `future.get()`을 통해 실제 저장이 완료될 때까지 대기하고 결과를 받아온다. 
3. 반환된 Map을 순회하며 실패한 노드가 있는지 확인한다. 분산 환경 특성상 일부 노드 저장에 실패할 수 있으므로, 비즈니스 로직에 따라 재시도 여부를 결정해야 한다.
- `set`연산은 멱등성(Idempotent)을 가지므로, 실패한 키에 대해서 반복적으로 재시도해도 데이터 일관성에 문제가 없다.

### 2. 비동기 데이터 조회 (`asyncGetFromReplica()`)
재시도 로직 없이, 단순히 임의의 복제본 하나를 비동기로 조회한다. 속도와 부하 분산이 중요하고 재시도가 필요 없는 경우 사용한다.

```java
GetFuture<Object> future = client.asyncGetFromReplica(key, replicaCount);
Object result = future.get();
```

### 3. 데이터 조회 (`getFromReplica()`)
복제된 데이터 중 하나를 조회한다. 이 메서드는 동기 방식으로 동작하며, 내부적으로 **재시도 로직**이 포함되어 있다.
- 재시도 로직: 임의로 선택한 첫 번째 복제본 조회에 실패(null)하거나 에러가 발생하면, 즉시 null을 반환하지 않고 다른 복제본에 대해 조회를 재시도한다.

```java

Object result = client.getFromReplica(key, replicaCount); // (1)

if (result != null) {
  System.out.println("Value: " + result);
} 
```

1. `getFromReplica`를 호출하여 데이터를 조회한다. 임의로 선택한 첫 번째 복제본 조회에 실패하거나 에러가 발생하면, 
  즉시 null을 반환하지 않고 다른 복제본에 대해 조회를 재시도한다.
