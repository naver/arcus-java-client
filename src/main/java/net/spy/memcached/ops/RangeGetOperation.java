package net.spy.memcached.ops;

public interface RangeGetOperation extends KeyedOperation {

    interface Callback extends OperationCallback {
        void gotData(byte[] key);
    }

}
