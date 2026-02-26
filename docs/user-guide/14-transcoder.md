# 14. Transcoder

Transcoder는 캐시 데이터와 Java 객체 간 직렬화/역직렬화를 담당한다.

`ConnectionFactoryBuilder`의 `setTranscoder()`, `setCollectionTranscoder()`를 통해 지정하거나, ArcusClient API 메서드의 인자로 직접 지정할 수 있다.

모든 Transcoder 구현체는 압축 시 **GZip** 을 사용하며, 기본 압축 기준은 16,384 bytes이다.

ARCUS Java Client는 아래 세 가지 Transcoder 구현체를 제공한다.

| 구현체                                | 직렬화 방식       | 
|:-----------------------------------|:-------------|
| `SerializingTranscoder`            | Java 직렬화     |
| `JsonSerializingTranscoder`        | Jackson JSON |
| `GenericJsonSerializingTranscoder` | Jackson JSON |

## SerializingTranscoder

- 별도 지정이 없는 경우 기본으로 사용되는 Transcoder
- Java 직렬화 방식을 사용하며, 압축 시 GZip 사용
- `forKV()`, `forCollection()` 팩토리 메서드를 통해 용도에 맞게 생성 가능

| 설정 항목         | 설정 방법  | KV 기본값                      | Collection 기본값                       |
|:--------------|:-------|:----------------------------|:-------------------------------------|
| max size      | 빌더     | `CachedData.MAX_SIZE` (1MB) | `MAX_COLLECTION_ELEMENT_SIZE` (32KB) |
| Character set | setter | UTF-8                       | UTF-8                                |
| 압축 기준         | setter | 16,384 bytes                | 16,384 bytes                         |
| ClassLoader   | 빌더     | null (JVM 기본)               | null (JVM 기본)                        |

**Key-Value 용도**

```java
SerializingTranscoder transcoder = SerializingTranscoder.forKV()
        .maxSize(CachedData.MAX_SIZE)
        .classLoader(this.getClass().getClassLoader())
        .build();

transcoder.setCharset("EUC-KR");
transcoder.setCompressionThreshold(4096);

ConnectionFactoryBuilder cfb = new ConnectionFactoryBuilder();
cfb.setTranscoder(transcoder);
```

**Collection 용도**

```java
SerializingTranscoder transcoder = SerializingTranscoder.forCollection()
        .forceJDKSerializationForCollection()
        .maxSize(16384)
        .classLoader(this.getClass().getClassLoader())
        .build();

transcoder.setCharset("EUC-KR");
transcoder.setCompressionThreshold(4096);

ConnectionFactoryBuilder cfb = new ConnectionFactoryBuilder();
cfb.setCollectionTranscoder(transcoder);
```

### ClassLoader 

- ClassLoader는 역직렬화 시 사용할 ClassLoader를 지정한다.  
- Spring Devtools와 같이 별도의 ClassLoader를 사용하는 환경에서, 역직렬화 시 사용하는 ClassLoader와 프로세스의 ClassLoader를 일치시키기 위해 사용한다.  
- `null`로 지정하면 JVM 기본 ClassLoader를 사용한다.

### forceJDKSerializationForCollection

- Collection Transcoder에서 타입에 관계없이 Java 직렬화를 사용하도록 강제하는 설정이다.
- 기본적으로 String, Integer 등 기본 타입은 네이티브 인코딩을 사용하기 때문에, Object 타입 혼재 시 역직렬화 문제가 발생할 수 있다.
- 따라서, 해당 옵션을 활성화 할 경우 모든 값을 JDK 직렬화로 통일하여 타입 불일치 문제를 방지할 수 있다.


## JsonSerializingTranscoder

- Jackson 라이브러리를 사용하여 객체를 JSON 형식으로 직렬화/역직렬화 수행
- `ObjectMapper`는 내부에서 생성
- 타입 안정성을 보장하기 위해 생성자에서 특정 클래스 타입 또는 `JavaType`을 지정해야 함

제네릭 타입을 `Object`로 지정하지 않으면 `setTranscoder()` 인자로 지정할 수 없으므로, ArcusClient API 메서드 인자로 직접 지정하는 것을 권장한다.

삽입 및 조회 시 동일한 타입을 갖는 Transcoder를 사용해야 한다.

```java
JsonSerializingTranscoder<MyClass> transcoder = new JsonSerializingTranscoder<>(MyClass.class);
arcusClient.set("key", 0, new MyClass("class1"), transcoder).get();
MyClass myClass = arcusClient.asyncGet("key", transcoder).get();
```

> 사용자 정의 타입에 기본 생성자나 `@JsonCreator` 어노테이션이 지정된 생성자가 없으면
> 역직렬화 시 경고 로그와 함께 null이 반환될 수 있다.

## GenericJsonSerializingTranscoder

Jackson 라이브러리를 사용하여 객체를 JSON 형식으로 직렬화/역직렬화한다.

- `JsonSerializingTranscoder`와 달리 다형성 타입을 지원하여 객체의 구체적인 타입 정보를 보존할 수 있다.
- `ObjectMapper`를 외부에서 주입해야 하며, `typeHintPropertyName`으로 타입 힌트 프로퍼티 이름을 지정할 수 있다.

| `typeHintPropertyName` 값 | 동작                                       |
|:-------------------------|:-----------------------------------------|
| `null`                   | `ObjectMapper`의 `DefaultTyping`을 설정하지 않음 |
| `""` (빈 문자열)             | 기본 타입 프로퍼티 이름(`@class`)으로 설정             |
| 그 외 문자열                  | 해당 문자열을 타입 프로퍼티 이름으로 사용                  |


신뢰할 수 없는 JSON 입력을 처리할 경우 보안 취약점 방지를 위해 적절한 `BasicPolymorphicTypeValidator`를 설정하는 것을 권장한다.

```java
ObjectMapper objectMapper = new ObjectMapper();
BasicPolymorphicTypeValidator validator = BasicPolymorphicTypeValidator.builder()
        .allowIfBaseType("net.spy.memcached.")   // 패키지 경로는 커스텀하게 설정
        .allowIfSubType("net.spy.memcached.")    // 패키지 경로는 커스텀하게 설정
        .build();

objectMapper.activateDefaultTyping(validator, ObjectMapper.DefaultTyping.EVERYTHING);

GenericJsonSerializingTranscoder transcoder =
        new GenericJsonSerializingTranscoder(objectMapper, "@class", CachedData.MAX_SIZE);

ConnectionFactoryBuilder cfb = new ConnectionFactoryBuilder();
cfb.setTranscoder(transcoder);
```