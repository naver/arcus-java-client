package net.spy.memcached.protocol.ascii;

import java.util.Collection;
import java.util.Collections;

import net.spy.memcached.collection.CollectionPipe;
import net.spy.memcached.ops.OperationCallback;
import net.spy.memcached.ops.OperationStatus;
import net.spy.memcached.ops.SingleKeyPipedOperationCallback;

abstract class SingleKeyPipeOperationImpl extends PipeOperationImpl {
  protected final String key;
  protected final SingleKeyPipedOperationCallback cb;

  protected SingleKeyPipeOperationImpl(String key, CollectionPipe pipe,
                                       OperationCallback cb) {
    super(pipe, cb);

    this.key = key;
    this.cb = (SingleKeyPipedOperationCallback) cb;
  }

  @Override
  protected void gotStatus(OperationStatus status) {
    cb.gotStatus(index, status);
  }

  @Override
  protected String getKey(Integer index) {
    return key;
  }

  public Collection<String> getKeys() {
    return Collections.singletonList(key);
  }
}
