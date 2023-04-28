package net.spy.memcached.protocol.ascii.callback;

import net.spy.memcached.CachedData;
import net.spy.memcached.collection.SMGetElement;
import net.spy.memcached.collection.SMGetTrimKey;
import net.spy.memcached.ops.CollectionOperationStatus;
import net.spy.memcached.ops.OperationStatus;
import net.spy.memcached.transcoders.Transcoder;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

public abstract class BaseBTreeSMGetOperationCallback<T,
    G extends BaseBTreeSMGetOperationCallback.GlobalParams<T>>
    extends BaseOperationCallback {

  protected final int count;
  protected final int totalResultElementCount;

  protected final boolean reverse;
  protected final Transcoder<T> tc;

  protected final List<SMGetElement<T>> eachResult = new ArrayList<SMGetElement<T>>();
  protected final List<SMGetTrimKey> eachTrimmedResult = new ArrayList<SMGetTrimKey>();

  protected final G gp;

  public BaseBTreeSMGetOperationCallback(final int count,
                                         final int totalResultElementCount,
                                         final boolean reverse,
                                         final Transcoder<T> tc,
                                         final G globalParams) {
    this.count = count;
    this.totalResultElementCount = totalResultElementCount;

    this.reverse = reverse;
    this.tc = tc;

    this.gp = globalParams;
  }

  public void complete() {
    gp.blatch.countDown();
  }

  public void gotData(String key, int flags, Object bkey, byte[] eflag, byte[] data) {
    if (gp.stopCollect.get()) {
      return;
    }

    if (bkey instanceof Long) {
      eachResult.add(new SMGetElement<T>(key, (Long) bkey, eflag,
          tc.decode(new CachedData(flags, data, tc.getMaxSize()))));
    } else if (bkey instanceof byte[]) {
      eachResult.add(new SMGetElement<T>(key, (byte[]) bkey, eflag,
          tc.decode(new CachedData(flags, data, tc.getMaxSize()))));
    }
  }

  public abstract void receivedStatus(OperationStatus status);

  public abstract static class GlobalParams<T> {
    protected final CountDownLatch blatch;
    protected final ReentrantLock lock = new ReentrantLock();

    protected final List<String> missedKeyList;
    protected final Map<String, CollectionOperationStatus> missedKeys;

    protected final List<SMGetElement<T>> mergedResult;
    protected final List<SMGetTrimKey> mergedTrimmedKeys;

    protected final List<OperationStatus> resultOperationStatus;
    protected final List<OperationStatus> failedOperationStatus;

    // if processedSMGetCount is 0, then all smget is done.
    protected final AtomicInteger processedSMGetCount;
    protected final AtomicBoolean stopCollect = new AtomicBoolean(false);

    public GlobalParams(final int smGetListSize,
                        final CountDownLatch blatch,
                        final List<String> missedKeyList,
                        final Map<String, CollectionOperationStatus> missedKeys,
                        final List<SMGetElement<T>> mergedResult,
                        final List<SMGetTrimKey> mergedTrimmedKeys,
                        final List<OperationStatus> resultOperationStatus,
                        final List<OperationStatus> failedOperationStatus) {
      this.blatch = blatch;

      this.missedKeyList = missedKeyList;
      this.missedKeys = missedKeys;

      this.mergedResult = mergedResult;
      this.mergedTrimmedKeys = mergedTrimmedKeys;

      this.resultOperationStatus = resultOperationStatus;
      this.failedOperationStatus = failedOperationStatus;

      this.processedSMGetCount = new AtomicInteger(smGetListSize);
    }
  }
}
