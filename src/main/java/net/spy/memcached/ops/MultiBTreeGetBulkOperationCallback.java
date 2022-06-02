package net.spy.memcached.ops;

/* ENABLE_MIGRATION if */
public class MultiBTreeGetBulkOperationCallback extends MultiOperationCallback
    implements BTreeGetBulkOperation.Callback {

  public MultiBTreeGetBulkOperationCallback(OperationCallback original, int todo) {
    super(original, todo);
  }

  @Override
  public void gotElement(String key, Object subkey, int flags, byte[] eflag, byte[] data) {
    ((BTreeGetBulkOperation.Callback) originalCallback).gotElement(key, subkey, flags, eflag, data);
  }

  @Override
  public void gotKey(String key, int elementCount, OperationStatus status) {
    ((BTreeGetBulkOperation.Callback) originalCallback).gotKey(key, elementCount, status);
  }
}
/* ENABLE_MIGRATION end */
