package net.spy.memcached.collection;

public class MapUpsert<T> extends CollectionInsert<T> {

  private static final String COMMAND = "mop upsert";

  public MapUpsert(T value, CreateAttributes attributes) {
    super(CollectionType.map, value, null, null, attributes);
  }

  @Override
  public String getCommand() {
    return COMMAND;
  }

}
