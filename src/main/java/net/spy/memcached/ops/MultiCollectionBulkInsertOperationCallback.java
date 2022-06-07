package net.spy.memcached.ops;

public class MultiCollectionBulkInsertOperationCallback extends MultiOperationCallback
    implements CollectionBulkInsertOperation.Callback {

  public MultiCollectionBulkInsertOperationCallback(OperationCallback original,
                                                    int todo, OperationStatus status) {
    super(original, status, todo);
  }

  public void gotStatus(Integer index, OperationStatus status) {
    ((CollectionBulkInsertOperation.Callback) originalCallback).gotStatus(index, status);
  }
}
