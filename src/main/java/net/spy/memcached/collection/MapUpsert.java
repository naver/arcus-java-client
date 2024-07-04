package net.spy.memcached.collection;

public class MapUpsert<T> extends CollectionInsert<T> {

  private static final String command = "mop upsert";

  public MapUpsert(T value, CollectionAttributes attr) {
    super(CollectionType.map, value, null, null, attr);
  }

  @Override
  public String getCommand() {
    return command;
  }

}
