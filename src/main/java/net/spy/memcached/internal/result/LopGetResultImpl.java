package net.spy.memcached.internal.result;

import java.util.ArrayList;
import java.util.List;

import net.spy.memcached.CachedData;
import net.spy.memcached.transcoders.Transcoder;

public class LopGetResultImpl<T> implements GetResult<List<T>> {
  private final List<CachedData> cachedDataList;
  private final Transcoder<T> transcoder;
  private List<T> result = new ArrayList<T>();

  public LopGetResultImpl(List<CachedData> cachedDataList, Transcoder<T> transcoder) {
    this.cachedDataList = cachedDataList;
    this.transcoder = transcoder;
  }

  @Override
  public List<T> getDecodedValue() {
    if (result.isEmpty() && !cachedDataList.isEmpty()) {
      List<T> temp = new ArrayList<T>();
      for (CachedData cachedData : cachedDataList) {
        temp.add(transcoder.decode(cachedData));
      }
      result = temp;
    }
    return result;
  }
}
