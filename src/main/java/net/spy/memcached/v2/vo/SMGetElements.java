package net.spy.memcached.v2.vo;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.PriorityQueue;

import net.spy.memcached.ops.StatusCode;

public final class SMGetElements<V> {
  private final List<Element<V>> elements;
  private final List<MissedKey> missedKeys;
  private final List<TrimmedKey> trimmedKeys;

  public SMGetElements(List<Element<V>> elements,
                       List<MissedKey> missedKeys,
                       List<TrimmedKey> trimmedKeys) {
    if (elements == null || missedKeys == null || trimmedKeys == null) {
      throw new IllegalArgumentException("Arguments cannot be null");
    }
    this.elements = elements;
    this.missedKeys = missedKeys;
    this.trimmedKeys = trimmedKeys;
  }

  public static <T> SMGetElements<T> mergeSMGetElements(List<SMGetElements<T>> smGetElementsList,
                                                        boolean ascending,
                                                        boolean unique, int count) {
    List<Element<T>> elements = new ArrayList<>();
    List<MissedKey> missedKeys = new ArrayList<>();
    List<TrimmedKey> trimmedKeys = new ArrayList<>();

    // 1) Collect elements considering unique, count option.
    mergeSMGetElements(smGetElementsList, elements, missedKeys, trimmedKeys,
            ascending, unique, count);

    // 2) Sort missed keys, and trimmed keys
    Collections.sort(missedKeys);
    Collections.sort(trimmedKeys);

    // 3) Remove trimmed keys outside the final element range
    if (!elements.isEmpty()) {
      BKey lastBKey = elements.get(elements.size() - 1).getbTreeElement().getBkey();
      trimmedKeys.removeIf(trimmedKey -> {
        int comp = trimmedKey.getBKey().compareTo(lastBKey);
        return ascending ? comp >= 0 : comp <= 0;
      });
    }

    return new SMGetElements<>(elements, missedKeys, trimmedKeys);
  }

  private static <T> void mergeSMGetElements(
          List<SMGetElements<T>> smGetElementsList,
          List<Element<T>> elements,
          List<MissedKey> missedKeys,
          List<TrimmedKey> trimmedKeys,
          boolean ascending, boolean unique, int count) {
    // 1) Create Priority queue to hold the current smallest/largest element from each list
    Comparator<ElementWithIndex<T>> comparator = ascending
            ? Comparator.naturalOrder()
            : Comparator.reverseOrder();
    PriorityQueue<ElementWithIndex<T>> pq = new PriorityQueue<>(comparator);

    // 2) Initialize the priority queue with the first element from each list
    //    and collect missed keys and trimmed keys
    for (int i = 0; i < smGetElementsList.size(); i++) {
      SMGetElements<T> smGetElements = smGetElementsList.get(i);
      List<Element<T>> eachElements = smGetElements.getElements();
      if (!eachElements.isEmpty()) {
        pq.offer(new ElementWithIndex<>(eachElements.get(0), i, 0));
      }
      missedKeys.addAll(smGetElements.getMissedKeys());
      trimmedKeys.addAll(smGetElements.getTrimmedKeys());
    }

    // 3) Merge elements until reach desired element count
    while (!pq.isEmpty() && elements.size() < count) {
      ElementWithIndex<T> current = pq.poll();
      BKey bkey = current.element.getbTreeElement().getBkey();
      // Deduplicate based on bkey if unique option is set
      if (unique && !elements.isEmpty()) {
        if (!elements.get(elements.size() - 1).getbTreeElement().getBkey().equals(bkey)) {
          elements.add(current.element);
        }
      } else {
        elements.add(current.element);
      }

      int nextIndex = current.elementIndex + 1;
      List<Element<T>> sourceList = smGetElementsList.get(current.listIndex).getElements();
      if (nextIndex < sourceList.size()) {
        pq.offer(new ElementWithIndex<>(sourceList.get(nextIndex),
                current.listIndex, nextIndex));
      }
    }
  }

  public List<Element<V>> getElements() {
    return Collections.unmodifiableList(elements);
  }

  public List<MissedKey> getMissedKeys() {
    return Collections.unmodifiableList(missedKeys);
  }

  public List<TrimmedKey> getTrimmedKeys() {
    return Collections.unmodifiableList(trimmedKeys);
  }

  public static final class Element<V> implements Comparable<Element<V>> {

    private final String key;
    private final BTreeElement<V> bTreeElement;

    public Element(String key, BTreeElement<V> element) {
      if (key == null || element == null) {
        throw new IllegalArgumentException("key or element cannot be null");
      }
      this.key = key;
      this.bTreeElement = element;
    }

    @Override
    public int compareTo(Element<V> o) {
      int elementComparison = bTreeElement.compareTo(o.getbTreeElement());
      if (elementComparison == 0) {
        return this.key.compareTo(o.key);
      }
      return elementComparison;
    }

    public String getKey() {
      return key;
    }

    public BTreeElement<V> getbTreeElement() {
      return bTreeElement;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }

      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      Element<?> that = (Element<?>) o;
      return Objects.equals(key, that.key) && Objects.equals(bTreeElement, that.bTreeElement);
    }

    @Override
    public int hashCode() {
      return Objects.hash(key, bTreeElement);
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

  /**
   * For K-Way merge
   */
  private static final class ElementWithIndex<T> implements Comparable<ElementWithIndex<T>> {
    private final Element<T> element;
    private final int listIndex;
    private final int elementIndex;

    ElementWithIndex(Element<T> element, int listIndex, int elementIndex) {
      this.element = element;
      this.listIndex = listIndex;
      this.elementIndex = elementIndex;
    }

    @Override
    public int compareTo(ElementWithIndex<T> o) {
      return this.element.compareTo(o.element);
    }
  }
}
