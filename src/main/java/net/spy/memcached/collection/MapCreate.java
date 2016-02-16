package net.spy.memcached.collection;

public class MapCreate extends CollectionCreate {

    private static final String command = "mop create";

    public MapCreate() {
        super();
    }

    public MapCreate(int flags, Integer expTime, Long maxCount, CollectionOverflowAction overflowAction, Boolean readable, boolean noreply) {
        super(flags, expTime, maxCount, overflowAction, readable, noreply);
    }

    public String getCommand() {
        return command;
    }
}