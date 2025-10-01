package net.spy.memcached.v2.vo;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

import net.spy.memcached.ops.StatusCode;

public final class SMGetResult<V> {
  private final List<SMGetElement<V>> elements;
  private final List<MissedKey> missedKeys;
  private final List<TrimmedKey> trimmedKeys;

  public SMGetResult(List<SMGetElement<V>> elements,
                     List<MissedKey> missedKeys,
                    List<TrimmedKey> trimmedKeys) {
    if (elements == null || missedKeys == null || trimmedKeys == null) {
      throw new IllegalArgumentException("Arguments cannot be null");
    }
    this.elements = elements;
    this.missedKeys = missedKeys;
    this.trimmedKeys = trimmedKeys;
  }

  public List<SMGetElement<V>> getElements() {
    return Collections.unmodifiableList(elements);
  }

  public List<MissedKey> getMissedKeys() {
    return Collections.unmodifiableList(missedKeys);
  }

  public List<TrimmedKey> getTrimmedKeys() {
    return Collections.unmodifiableList(trimmedKeys);
  }

  public static final class SMGetElement<V> implements Comparable<SMGetElement<V>> {

    private final String key;
    private final BTreeElement<V> element;

    public SMGetElement(String key, BTreeElement<V> element) {
      if (key == null || element == null) {
        throw new IllegalArgumentException("key or element cannot be null");
      }
      this.key = key;
      this.element = element;
    }

    @Override
    public int compareTo(SMGetElement<V> o) {
      int elementComparison = element.compareTo(o.getElement());
      if (elementComparison == 0) {
        return this.key.compareTo(o.key);
      }
      return elementComparison;
    }

    public String getKey() {
      return key;
    }

    public BTreeElement<V> getElement() {
      return element;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }

      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      SMGetElement<?> that = (SMGetElement<?>) o;
      return Objects.equals(key, that.key) && Objects.equals(element, that.element);
    }

    @Override
    public int hashCode() {
      return Objects.hash(key, element);
    }
  }

  public static final class MissedKey implements Comparable<MissedKey> {
    private final String key;
    private final StatusCode statusCode;

    public MissedKey(String key, StatusCode statusCode) {
      if (key == null || statusCode == null) {
        throw new IllegalArgumentException("key or statusCode cannot be null");
      }
      this.key = key;
      this.statusCode = statusCode;
    }

    public String getKey() {
      return key;
    }

    public StatusCode getStatusCode() {
      return statusCode;
    }

    @Override
    public int compareTo(MissedKey o) {
      return this.key.compareTo(o.key);
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }

      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      MissedKey missedKey = (MissedKey) o;
      return Objects.equals(key, missedKey.key) && statusCode == missedKey.statusCode;
    }

    @Override
    public int hashCode() {
      return Objects.hash(key, statusCode);
    }
  }

  public static final class TrimmedKey implements Comparable<TrimmedKey> {
    private final String key;
    private final BKey bKey;

    public TrimmedKey(String key, BKey bKey) {
      if (key == null || bKey == null) {
        throw new IllegalArgumentException("key or bKey cannot be null");
      }
      this.key = key;
      this.bKey = bKey;
    }

    public String getKey() {
      return key;
    }

    public BKey getBKey() {
      return bKey;
    }

    @Override
    public int compareTo(TrimmedKey o) {
      return this.key.compareTo(o.key);
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }

      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      TrimmedKey that = (TrimmedKey) o;
      return Objects.equals(key, that.key) && Objects.equals(bKey, that.bKey);
    }

    @Override
    public int hashCode() {
      return Objects.hash(key, bKey);
    }
  }
}
