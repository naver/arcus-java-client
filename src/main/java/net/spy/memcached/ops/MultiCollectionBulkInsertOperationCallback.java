package net.spy.memcached.ops;

/* ENABLE_MIGRATION if */
public class MultiCollectionBulkInsertOperationCallback extends MultiOperationCallback
    implements CollectionBulkInsertOperation.Callback {

  public MultiCollectionBulkInsertOperationCallback(OperationCallback original, int todo) {
    super(original, todo);
  }

  public void gotStatus(Integer index, OperationStatus status) {
    ((CollectionBulkInsertOperation.Callback) originalCallback).gotStatus(index, status);
  }
}
/* ENABLE_MIGRATION end */
