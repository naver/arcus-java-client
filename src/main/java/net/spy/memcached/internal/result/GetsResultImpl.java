package net.spy.memcached.internal.result;

import net.spy.memcached.CASValue;
import net.spy.memcached.CachedData;
import net.spy.memcached.transcoders.Transcoder;

public class GetsResultImpl<T> implements GetResult<CASValue<T>> {
  private final long cas;
  private final CachedData cachedData;
  private final Transcoder<T> transcoder;
  private volatile CASValue<T> decodedValue = null;

  public GetsResultImpl(long cas, CachedData cachedData, Transcoder<T> transcoder) {
    this.cas = cas;
    this.cachedData = cachedData;
    this.transcoder = transcoder;
  }

  @Override
  public CASValue<T> getDecodedValue() {
    if (decodedValue == null) {
      decodedValue = new CASValue<>(cas, transcoder.decode(cachedData));
    }
    return decodedValue;
  }
}
