package net.spy.memcached.internal.result;

import net.spy.memcached.CachedData;
import net.spy.memcached.transcoders.Transcoder;

public final class GetResult<T> {
  private final Transcoder<T> transcoder;

  private volatile CachedData cachedData = null;
  private volatile T decodedValue = null;

  public GetResult(Transcoder<T> transcoder) {
    this.transcoder = transcoder;
  }

  public void setCachedData(CachedData cachedData) {
    this.cachedData = cachedData;
  }

  public T getDecodedValue() {
    if (cachedData == null) {
      return null;
    }

    if (decodedValue == null) {
      decodedValue = transcoder.decode(cachedData);
    }

    return decodedValue;
  }
}
