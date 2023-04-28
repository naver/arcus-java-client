package net.spy.memcached.protocol.ascii.callback;

import net.spy.memcached.collection.SMGetElement;
import net.spy.memcached.collection.SMGetTrimKey;
import net.spy.memcached.ops.BTreeSortMergeGetOperationOld;
import net.spy.memcached.ops.CollectionOperationStatus;
import net.spy.memcached.ops.OperationStatus;
import net.spy.memcached.transcoders.Transcoder;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

public class BTreeSMGetOperationOldCallback<T>
    extends BaseBTreeSMGetOperationCallback<T, BTreeSMGetOperationOldCallback.GlobalParams<T>>
    implements BTreeSortMergeGetOperationOld.Callback {

  public BTreeSMGetOperationOldCallback(final int offset,
                                        final int count,
                                        final boolean reverse,
                                        final Transcoder<T> tc,
                                        final BTreeSMGetOperationOldCallback.GlobalParams<T> gp) {
    super(count, offset + count, reverse, tc, gp);
  }

  @Override
  public void receivedStatus(OperationStatus status) {
    gp.processedSMGetCount.decrementAndGet();

    if (!status.isSuccess()) {
      getLogger().warn("SMGetFailed. status=%s", status);
      if (!gp.stopCollect.get()) {
        gp.stopCollect.set(true);
        gp.failedOperationStatus.add(status);
      }
      gp.mergedResult.clear();
      return;
    }

    boolean isTrimmed = "TRIMMED".equals(status.getMessage()) ||
        "DUPLICATED_TRIMMED".equals(status.getMessage());
    gp.lock.lock();
    try {
      if (gp.mergedResult.size() == 0) {
        // merged result is empty, add all.
        gp.mergedResult.addAll(eachResult);
        gp.mergedTrim.set(isTrimmed);
      } else {
        boolean addAll = true;
        int pos = 0;
        for (SMGetElement<T> result : eachResult) {
          for (; pos < gp.mergedResult.size(); pos++) {
            if (reverse ?
                0 < result.compareTo(gp.mergedResult.get(pos)) :
                0 > result.compareTo(gp.mergedResult.get(pos))) {
              break;
            }
          }
          if (pos >= totalResultElementCount) {
            addAll = false;
            break;
          }
          if (pos >= gp.mergedResult.size() && gp.mergedTrim.get() &&
              result.compareBkeyTo(gp.mergedResult.get(pos - 1)) != 0) {
            addAll = false;
            break;
          }
          gp.mergedResult.add(pos, result);
          if (gp.mergedResult.size() > totalResultElementCount) {
            gp.mergedResult.remove(totalResultElementCount);
          }
          pos += 1;
        }
        if (isTrimmed && addAll) {
          while (pos < gp.mergedResult.size()) {
            if (gp.mergedResult.get(pos).compareBkeyTo(gp.mergedResult.get(pos - 1)) == 0) {
              pos += 1;
            } else {
              gp.mergedResult.remove(pos);
            }
          }
          gp.mergedTrim.set(true);
        }
        if (gp.mergedResult.size() >= totalResultElementCount) {
          gp.mergedTrim.set(false);
        }
      }

      if (gp.processedSMGetCount.get() == 0) {
        boolean isDuplicated = false;
        for (int i = 1; i < gp.mergedResult.size(); i++) {
          if (gp.mergedResult.get(i).compareBkeyTo(gp.mergedResult.get(i - 1)) == 0) {
            isDuplicated = true;
            break;
          }
        }
        if (gp.mergedTrim.get()) {
          if (isDuplicated) {
            gp.resultOperationStatus.add(new OperationStatus(true, "DUPLICATED_TRIMMED"));
          } else {
            gp.resultOperationStatus.add(new OperationStatus(true, "TRIMMED"));
          }
        } else {
          if (isDuplicated) {
            gp.resultOperationStatus.add(new OperationStatus(true, "DUPLICATED"));
          } else {
            gp.resultOperationStatus.add(new OperationStatus(true, "END"));
          }
        }
      }
    } finally {
      gp.lock.unlock();
    }
  }

  @Override
  public void gotMissedKey(byte[] data) {
    gp.missedKeyList.add(new String(data));
    OperationStatus cause = new OperationStatus(false, "UNDEFINED");
    gp.missedKeys.put(new String(data), new CollectionOperationStatus(cause));
  }

  public static class GlobalParams<T>
      extends BaseBTreeSMGetOperationCallback.GlobalParams<T> {

    private final AtomicBoolean mergedTrim = new AtomicBoolean(false);

    public GlobalParams(final int smGetListSize,
                        final CountDownLatch blatch,
                        final List<String> missedKeyList,
                        final Map<String, CollectionOperationStatus> missedKeys,
                        final List<SMGetElement<T>> mergedResult,
                        final List<SMGetTrimKey> mergedTrimmedKeys,
                        final List<OperationStatus> resultOperationStatus,
                        final List<OperationStatus> failedOperationStatus) {
      super(smGetListSize, blatch, missedKeyList, missedKeys, mergedResult, mergedTrimmedKeys,
          resultOperationStatus, failedOperationStatus);
    }
  }
}
