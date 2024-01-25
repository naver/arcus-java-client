package net.spy.memcached.internal.result;

import java.util.HashMap;
import java.util.Map;

import net.spy.memcached.CachedData;
import net.spy.memcached.transcoders.Transcoder;

public class MopGetResultImpl<T> implements GetResult<Map<String, T>> {
  private final Map<String, CachedData> cachedDataMap;
  private final Transcoder<T> transcoder;
  private Map<String, T> result = new HashMap<String, T>();

  public MopGetResultImpl(Map<String, CachedData> cachedDataMap, Transcoder<T> transcoder) {
    this.cachedDataMap = cachedDataMap;
    this.transcoder = transcoder;
  }

  @Override
  public Map<String, T> getDecodedValue() {
    if (result.isEmpty() && !cachedDataMap.isEmpty()) {
      Map<String, T> temp = new HashMap<String, T>();
      for (Map.Entry<String, CachedData> entry : cachedDataMap.entrySet()) {
        temp.put(entry.getKey(), transcoder.decode(entry.getValue()));
      }
      result = temp;
    }
    return result;
  }
}
