package net.spy.memcached;

import java.util.concurrent.atomic.AtomicReference;

public class NodesChange {
  private final AtomicReference<String> cacheList = new AtomicReference<String>(null);
  /* ENABLE_MIGRATION if */
  private final AtomicReference<String> alterList = new AtomicReference<String>(null);
  /* ENABLE_MIGRATION end */

  public NodesChange() { }

  public String getAndClearCacheList() {
    return cacheList.getAndSet(null);
  }

  public void setCacheList(String cacheList) {
    this.cacheList.set(cacheList);
  }

  public boolean hasCacheList() {
    return cacheList.get() != null;
  }

  /* ENABLE_MIGRATION if */
  public String getAndClearAlterList() {
    return alterList.getAndSet(null);
  }

  public void setAlterList(String alterList) {
    this.alterList.set(alterList);
  }
  /* ENABLE_MIGRATION end */
}
