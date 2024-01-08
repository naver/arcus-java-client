package net.spy.memcached.internal.result;

public interface GetResult<T> {
  T getDecodedValue();
}
