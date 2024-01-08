package net.spy.memcached.internal.result;

import net.spy.memcached.CachedData;
import net.spy.memcached.transcoders.Transcoder;

public final class GetResultImpl<T> implements GetResult<T> {
  private final CachedData cachedData;
  private final Transcoder<T> transcoder;
  private volatile T decodedValue = null;

  public GetResultImpl(CachedData cachedData, Transcoder<T> transcoder) {
    this.cachedData = cachedData;
    this.transcoder = transcoder;
  }

  @Override
  public T getDecodedValue() {
    if (decodedValue == null) {
      decodedValue = transcoder.decode(cachedData);
    }
    return decodedValue;
  }
}
