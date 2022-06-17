package net.spy.memcached.protocol.ascii.callback;

import net.spy.memcached.collection.SMGetElement;
import net.spy.memcached.collection.SMGetMode;
import net.spy.memcached.collection.SMGetTrimKey;
import net.spy.memcached.ops.BTreeSortMergeGetOperation;
import net.spy.memcached.ops.CollectionOperationStatus;
import net.spy.memcached.ops.OperationStatus;
import net.spy.memcached.transcoders.Transcoder;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

public class BTreeSMGetOperationCallback<T>
    extends BaseBTreeSMGetOperationCallback<T, BTreeSMGetOperationCallback.GlobalParams<T>>
    implements BTreeSortMergeGetOperation.Callback {

  private final SMGetMode smGetMode;
  
  public BTreeSMGetOperationCallback(final int count,
                                     final boolean reverse,
                                     final Transcoder<T> tc,
                                     final SMGetMode smGetMode,
                                     final BTreeSMGetOperationCallback.GlobalParams<T> gp) {
    super(count, count, reverse, tc, gp);
    this.smGetMode = smGetMode;
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
      gp.mergedTrimmedKeys.clear();
      return;
    }

    gp.lock.lock();
    try {
      if (gp.mergedResult.size() == 0) {
        // merged result is empty, add all.
        gp.mergedResult.addAll(eachResult);
      } else {
        // do sort merge
        boolean duplicated;
        int comp, pos = 0;
        for (SMGetElement<T> result : eachResult) {
          duplicated = false;
          for (; pos < gp.mergedResult.size(); pos++) {
            // compare b+tree key
            comp = result.compareBkeyTo(gp.mergedResult.get(pos));
            if ((reverse) ? (0 < comp) : (0 > comp)) {
              break;
            }
            if (comp == 0) { // compare key string
              comp = result.compareKeyTo(gp.mergedResult.get(pos));
              if ((reverse) ? (0 < comp) : (0 > comp)) {
                if (smGetMode == SMGetMode.UNIQUE) {
                  gp.mergedResult.remove(pos); // remove dup bkey
                }
                break;
              } else {
                if (smGetMode == SMGetMode.UNIQUE) {
                  duplicated = true;
                  break;
                }
              }
            }
          }
          if (duplicated) { // UNIQUE
            continue;
          }
          if (pos >= totalResultElementCount) {
            // At this point, following conditions are met.
            //   - mergedResult.size() == totalResultElementCount &&
            //   - The current <bkey, key> of eachResult is
            //     behind of the last <bkey, key> of mergedResult.
            // Then, all the next <bkey, key> elements of eachResult are
            // definitely behind of the last <bkey, bkey> of mergedResult.
            // So, stop the current sort-merge.
            break;
          }

          gp.mergedResult.add(pos, result);
          if (gp.mergedResult.size() > totalResultElementCount) {
            gp.mergedResult.remove(totalResultElementCount);
          }
          pos += 1;
        }
      }

      if (eachTrimmedResult.size() > 0) {
        if (gp.mergedTrimmedKeys.size() == 0) {
          gp.mergedTrimmedKeys.addAll(eachTrimmedResult);
        } else {
          // do sort merge trimmed list
          int pos = 0;
          for (SMGetTrimKey result : eachTrimmedResult) {
            for (; pos < gp.mergedTrimmedKeys.size(); pos++) {
              if ((reverse) ?
                  (0 < result.compareTo(gp.mergedTrimmedKeys.get(pos))) :
                  (0 > result.compareTo(gp.mergedTrimmedKeys.get(pos)))) {
                break;
              }
            }
            gp.mergedTrimmedKeys.add(pos, result);
            pos += 1;
          }
        }
      }

      if (gp.processedSMGetCount.get() == 0) {
        if (gp.mergedTrimmedKeys.size() > 0 && count <= gp.mergedResult.size()) {
          // remove trimed keys whose bkeys are behind of the last element.
          SMGetElement<T> lastElement = gp.mergedResult.get(gp.mergedResult.size() - 1);
          SMGetTrimKey lastTrimKey = new SMGetTrimKey(lastElement.getKey(),
              lastElement.getBkeyObject());
          for (int i = gp.mergedTrimmedKeys.size() - 1; i >= 0; i--) {
            SMGetTrimKey me = gp.mergedTrimmedKeys.get(i);
            if ((reverse) ?
                (0 >= me.compareTo(lastTrimKey)) :
                (0 <= me.compareTo(lastTrimKey))) {
              gp.mergedTrimmedKeys.remove(i);
            } else {
              break;
            }
          }
        }
        if (smGetMode == SMGetMode.UNIQUE) {
          gp.resultOperationStatus.add(new OperationStatus(true, "END"));
        } else {
          boolean isDuplicated = false;
          for (int i = 1; i < gp.mergedResult.size(); i++) {
            if (gp.mergedResult.get(i).compareBkeyTo(gp.mergedResult.get(i - 1)) == 0) {
              isDuplicated = true;
              break;
            }
          }
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
  public void gotMissedKey(String key, OperationStatus cause) {
    gp.missedKeyList.add(key);
    gp.missedKeys.put(key, new CollectionOperationStatus(cause));
  }

  @Override
  public void gotTrimmedKey(String key, Object bkey) {
    if (gp.stopCollect.get()) {
      return;
    }

    if (bkey instanceof Long) {
      eachTrimmedResult.add(new SMGetTrimKey(key, (Long) bkey));
    } else if (bkey instanceof byte[]) {
      eachTrimmedResult.add(new SMGetTrimKey(key, (byte[]) bkey));
    }
  }

  public static class GlobalParams<T>
      extends BaseBTreeSMGetOperationCallback.GlobalParams<T> {

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
