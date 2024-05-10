package net.spy.memcached.internal.result;

import java.util.HashSet;
import java.util.Set;

import net.spy.memcached.CachedData;
import net.spy.memcached.transcoders.Transcoder;

public class SopGetResultImpl<T> implements GetResult<Set<T>> {
  private final Set<CachedData> cachedDataSet;
  private final Transcoder<T> transcoder;
  private Set<T> result = new HashSet<>();

  public SopGetResultImpl(Set<CachedData> cachedDataSet, Transcoder<T> transcoder) {
    this.cachedDataSet = cachedDataSet;
    this.transcoder = transcoder;
  }

  @Override
  public Set<T> getDecodedValue() {
    if (result.isEmpty() && !cachedDataSet.isEmpty()) {
      Set<T> temp = new HashSet<>();
      for (CachedData cachedData : cachedDataSet) {
        temp.add(transcoder.decode(cachedData));
      }
      result = temp;
    }
    return result;
  }
}
