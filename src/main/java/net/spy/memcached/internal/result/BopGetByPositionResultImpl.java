package net.spy.memcached.internal.result;

import java.util.Collections;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

import net.spy.memcached.CachedData;
import net.spy.memcached.collection.BKeyObject;
import net.spy.memcached.collection.Element;
import net.spy.memcached.transcoders.Transcoder;
import net.spy.memcached.util.BTreeUtil;

public class BopGetByPositionResultImpl<T> implements GetResult<Map<Integer, Element<T>>> {
  private final Map<Integer, Map.Entry<BKeyObject, CachedData>> cachedDataMap;
  private final Transcoder<T> transcoder;
  private SortedMap<Integer, Element<T>> result;

  public BopGetByPositionResultImpl(Map<Integer, Map.Entry<BKeyObject, CachedData>> cachedDataMap,
                                    boolean reverse,
                                    Transcoder<T> transcoder) {
    this.cachedDataMap = cachedDataMap;
    this.result = new TreeMap<Integer, Element<T>>((reverse) ? Collections.reverseOrder() : null);
    this.transcoder = transcoder;
  }

  @Override
  public Map<Integer, Element<T>> getDecodedValue() {
    if (result.isEmpty() && !cachedDataMap.isEmpty()) {
      SortedMap<Integer, Element<T>> temp = new TreeMap<Integer, Element<T>>(result);
      for (Map.Entry<Integer, Map.Entry<BKeyObject, CachedData>> entry : cachedDataMap.entrySet()) {
        Map.Entry<BKeyObject, CachedData> cachedDataEntry = entry.getValue();
        temp.put(entry.getKey(), BTreeUtil.makeBTreeElement(
                cachedDataEntry.getKey(), cachedDataEntry.getValue(), transcoder));
      }
      result = temp;
    }
    return result;
  }
}
