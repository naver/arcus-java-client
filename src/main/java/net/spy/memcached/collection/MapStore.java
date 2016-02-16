package net.spy.memcached.collection;

public class MapStore<T> extends CollectionStore<T> {

    private static final String command = "mop insert";

    public MapStore() { }

    public MapStore(T value, boolean createKeyIfNotExists, RequestMode requestMode, CollectionAttributes attr) {
        super(value, null, createKeyIfNotExists, requestMode, attr);
    }

    public String getCommand() {
        return command;
    }

}