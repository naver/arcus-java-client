package net.spy.memcached.internal.result;

import net.spy.memcached.CachedData;
import net.spy.memcached.collection.BKeyObject;
import net.spy.memcached.collection.Element;
import net.spy.memcached.transcoders.Transcoder;
import net.spy.memcached.util.BTreeUtil;

public class BopStoreAndGetResultImpl<T> implements GetResult<Element<T>> {
  private final BKeyObject bKeyObject;
  private final CachedData cachedData;
  private final Transcoder<T> transcoder;
  private Element<T> result = null;

  public BopStoreAndGetResultImpl(BKeyObject bKeyObject,
                                  CachedData cachedData,
                                  Transcoder<T> transcoder) {
    this.bKeyObject = bKeyObject;
    this.cachedData = cachedData;
    this.transcoder = transcoder;
  }

  @Override
  public Element<T> getDecodedValue() {
    if (cachedData != null && result == null) {
      result = BTreeUtil.makeBTreeElement(bKeyObject, cachedData, transcoder);
    }
    return result;
  }
}
