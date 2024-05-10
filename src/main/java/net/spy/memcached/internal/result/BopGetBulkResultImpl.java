package net.spy.memcached.internal.result;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import net.spy.memcached.CachedData;
import net.spy.memcached.collection.BTreeElement;
import net.spy.memcached.collection.BTreeGetResult;
import net.spy.memcached.ops.CollectionOperationStatus;
import net.spy.memcached.transcoders.Transcoder;

public class BopGetBulkResultImpl<K, V> implements GetResult<Map<String, BTreeGetResult<K, V>>> {
  private final Map<String, List<BTreeElement<K, CachedData>>> cachedDataMap;
  private final Map<String, CollectionOperationStatus> opStatusMap;
  private final boolean reverse;
  private final Transcoder<V> transcoder;
  private Map<String, BTreeGetResult<K, V>> result
          = new HashMap<>();

  public BopGetBulkResultImpl(Map<String, List<BTreeElement<K, CachedData>>> cachedDataMap,
                              Map<String, CollectionOperationStatus> opStatusMap,
                              boolean reverse, Transcoder<V> transcoder) {
    this.cachedDataMap = cachedDataMap;
    this.opStatusMap = opStatusMap;
    this.reverse = reverse;
    this.transcoder = transcoder;
  }

  @Override
  public Map<String, BTreeGetResult<K, V>> getDecodedValue() {
    if (result.isEmpty() && !opStatusMap.isEmpty()) {
      Map<String, BTreeGetResult<K, V>> temp = new HashMap<>(result);
      for (Map.Entry<String, CollectionOperationStatus> entry : opStatusMap.entrySet()) {
        String key = entry.getKey();
        temp.put(key, new BTreeGetResult<>(cachedDataMap.get(key),
                reverse, transcoder, entry.getValue()));
      }
      result = temp;
    }
    return result;
  }
}
