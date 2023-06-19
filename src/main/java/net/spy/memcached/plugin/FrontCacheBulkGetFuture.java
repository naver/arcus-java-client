package net.spy.memcached.plugin;

import net.spy.memcached.OperationTimeoutException;
import net.spy.memcached.internal.BulkGetFuture;

import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static net.spy.memcached.DefaultConnectionFactory.DEFAULT_OPERATION_TIMEOUT;

public class FrontCacheBulkGetFuture<T> extends BulkGetFuture<T> {
  private final LocalCacheManager localCacheManager;

  private final Map<String, T> localCachedData;

  private Map<String, T> result = null;

  public FrontCacheBulkGetFuture(LocalCacheManager localCacheManager,
                                 BulkGetFuture<T> parentFuture,
                                 Map<String, T> localCachedData) {
    super(parentFuture);
    this.localCacheManager = localCacheManager;
    this.localCachedData = localCachedData;
  }

  @Override
  public Map<String, T> get() throws InterruptedException, ExecutionException {
    try {
      return get(DEFAULT_OPERATION_TIMEOUT, TimeUnit.MILLISECONDS);
    } catch (TimeoutException e) {
      throw new OperationTimeoutException(e);
    }
  }

  @Override
  public Map<String, T> get(long duration, TimeUnit units)
          throws InterruptedException, ExecutionException, TimeoutException {
    if (result == null) {
      try {
        result = super.get(duration, units);
      } catch (TimeoutException e) {
        throw new OperationTimeoutException(e);
      }
      putLocalCache(result);
      result.putAll(localCachedData);
    }
    return result;
  }

  @Override
  public Map<String, T> getSome(long duration, TimeUnit units)
          throws InterruptedException, ExecutionException {
    if (result != null) {
      return result;
    }
    Map<String, T> getSomeResult = super.getSome(duration, units);
    if (getSomeResult.size() == getOpCount()) {
      result = getSomeResult;
    }
    putLocalCache(getSomeResult);
    getSomeResult.putAll(localCachedData);
    return getSomeResult;
  }

  private void putLocalCache(Map<String, T> noneCachedValue) {
    for (Map.Entry<String, T> entry : noneCachedValue.entrySet()) {
      String key = entry.getKey();
      T value = entry.getValue();
      if (value != null) {
        localCacheManager.put(key, value);
      }
    }
  }
}
