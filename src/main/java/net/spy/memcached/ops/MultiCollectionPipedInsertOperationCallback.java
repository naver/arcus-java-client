package net.spy.memcached.ops;

public class MultiCollectionPipedInsertOperationCallback extends MultiOperationCallback
    implements CollectionPipedInsertOperation.Callback {

  public MultiCollectionPipedInsertOperationCallback(OperationCallback original, int todo) {
    super(original, todo);
  }

  public void gotStatus(Integer index, OperationStatus status) {
    ((CollectionPipedInsertOperation.Callback) originalCallback).gotStatus(index, status);
  }
}
