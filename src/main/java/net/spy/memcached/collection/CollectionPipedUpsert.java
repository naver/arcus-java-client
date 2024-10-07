package net.spy.memcached.collection;

import java.util.List;
import java.util.Map;

import net.spy.memcached.transcoders.Transcoder;

public abstract class CollectionPipedUpsert<T> extends CollectionPipedInsert<T> {

  public CollectionPipedUpsert(String key, CollectionAttributes attribute,
                               Transcoder<T> tc, int itemCount) {
    super(key, attribute, tc, itemCount);
  }

  public static class BTreePipedUpsert<T> extends BTreePipedInsert<T> {
    private static final String COMMAND = "bop upsert";

    public BTreePipedUpsert(String key, Map<Long, T> map,
                            CollectionAttributes attr, Transcoder<T> tc) {
      super(key, map, attr, tc);
    }

    @Override
    public String getCommand() {
      return COMMAND;
    }
  }

  public static class ByteArrayBTreePipedUpsert<T> extends ByteArraysBTreePipedInsert<T> {
    private static final String COMMAND = "bop upsert";

    public ByteArrayBTreePipedUpsert(String key, List<Element<T>> elements,
                                     CollectionAttributes attr, Transcoder<T> tc) {
      super(key, elements, attr, tc);
    }

    @Override
    public String getCommand() {
      return COMMAND;
    }
  }
}
