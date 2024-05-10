package net.spy.memcached.internal.result;

import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

import net.spy.memcached.CachedData;
import net.spy.memcached.collection.BKeyObject;
import net.spy.memcached.collection.ByteArrayBKey;
import net.spy.memcached.collection.ByteArrayTreeMap;
import net.spy.memcached.collection.Element;
import net.spy.memcached.transcoders.Transcoder;
import net.spy.memcached.util.BTreeUtil;

public class BopGetResultImpl<K, V> implements GetResult<Map<K, Element<V>>> {
  private final Map<K, CachedData> cachedDataMap;
  private final boolean reverse;
  private final Transcoder<V> transcoder;
  private SortedMap<K, Element<V>> result;

  public BopGetResultImpl(Map<K, CachedData> cachedDataMap,
                          boolean reverse, Transcoder<V> transcoder) {
    this.cachedDataMap = cachedDataMap;
    this.result = new TreeMap<>((reverse) ? Collections.reverseOrder() : null);
    this.reverse = reverse;
    this.transcoder = transcoder;
  }

  @Override
  public Map<K, Element<V>> getDecodedValue() {
    if (result.isEmpty() && !cachedDataMap.isEmpty()) {
      Set<Map.Entry<K, CachedData>> entrySet = cachedDataMap.entrySet();
      K bKey = entrySet.iterator().next().getKey();

      boolean isByteBKey = (bKey instanceof ByteArrayBKey);
      boolean isLongBKey = (bKey instanceof Long);

      SortedMap<K, Element<V>> temp;

      if (isByteBKey) {
        temp = new ByteArrayTreeMap<>(reverse ? Collections.<K>reverseOrder() : null);
      } else if (isLongBKey) {
        temp = new TreeMap<>(reverse ? Collections.<K>reverseOrder() : null);
      } else {
        return result;
      }

      for (Map.Entry<K, CachedData> entry : cachedDataMap.entrySet()) {
        bKey = entry.getKey();
        CachedData cachedData = entry.getValue();
        if (isByteBKey) {
          temp.put(bKey, BTreeUtil.makeBTreeElement(
                  new BKeyObject(((ByteArrayBKey) bKey).getBytes()), cachedData, transcoder));
        } else {
          temp.put(bKey, BTreeUtil.makeBTreeElement(
                  new BKeyObject((Long) bKey), cachedData, transcoder));
        }
      }
      result = temp;
    }
    return result;
  }
}
