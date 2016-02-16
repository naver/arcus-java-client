package net.spy.memcached.ops;

import net.spy.memcached.collection.CollectionMapGet;

/**
 * Operation that represents collection object retreival.
 */
public interface CollectionMapGetOperation extends KeyedOperation {

    CollectionMapGet<?> getGet();

    interface Callback extends OperationCallback {
        void gotData(String key, String field, int flags, byte[] data);
    }

}