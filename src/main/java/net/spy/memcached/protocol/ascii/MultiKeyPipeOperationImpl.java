package net.spy.memcached.protocol.ascii;

import java.util.Collection;
import java.util.Collections;
import java.util.List;

import net.spy.memcached.collection.CollectionPipe;
import net.spy.memcached.ops.MultiKeyPipedOperationCallback;
import net.spy.memcached.ops.OperationCallback;
import net.spy.memcached.ops.OperationStatus;

abstract class MultiKeyPipeOperationImpl extends PipeOperationImpl {
  protected final List<String> keys;
  protected final MultiKeyPipedOperationCallback cb;

  protected MultiKeyPipeOperationImpl(List<String> keys, CollectionPipe pipe,
                                      OperationCallback cb) {
    super(pipe, cb);

    this.keys = keys;
    this.cb = (MultiKeyPipedOperationCallback) cb;
  }

  @Override
  protected void gotStatus(OperationStatus status) {
    cb.gotStatus(keys.get(index), status);
  }

  @Override
  protected String getKey(Integer index) {
    return keys.get(index);
  }

  public Collection<String> getKeys() {
    return Collections.unmodifiableList(keys);
  }
}
