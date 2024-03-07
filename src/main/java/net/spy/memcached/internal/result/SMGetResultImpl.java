package net.spy.memcached.internal.result;

import java.util.List;

import net.spy.memcached.collection.SMGetElement;
import net.spy.memcached.collection.SMGetTrimKey;
import net.spy.memcached.ops.CollectionOperationStatus;
import net.spy.memcached.ops.OperationStatus;

public final class SMGetResultImpl<T> extends SMGetResult<T> {
  private final int count;
  private final boolean unique;

  public SMGetResultImpl(int count, boolean unique, boolean reverse) {
    super(count, reverse);

    this.count = count;
    this.unique = unique;
  }

  @Override
  public List<SMGetElement<T>> getFinalResult() {
    return mergedResult;
  }

  public void setFailedOperationStatus(OperationStatus status) {
    if (failedOperationStatus == null) {
      failedOperationStatus = new CollectionOperationStatus(status);
    }
    mergedResult.clear();
    mergedTrimmedKeys.clear();
  }

  public void mergeSMGetElements(final List<SMGetElement<T>> eachResult,
                                 final List<SMGetTrimKey> eachTrimmedResult) {

    mergeSMGetElements(eachResult);
    mergeTrimmedKeys(eachTrimmedResult);
  }

  private void mergeSMGetElements(final List<SMGetElement<T>> eachResult) {
    if (mergedResult.isEmpty()) {
      mergedResult.addAll(eachResult);

      // remove elements that exceed the count
      while (mergedResult.size() > count) {
        mergedResult.remove(count);
      }
      return;
    }

    // do sort merge
    int comp, pos = 0;
    for (SMGetElement<T> result : eachResult) {
      boolean doInsert = true;
      for (; pos < mergedResult.size(); pos++) {
        comp = result.compareBkeyTo(mergedResult.get(pos));
        if ((reverse) ? (comp > 0) : (comp < 0)) {
          break;
        }
        if (comp == 0) {
          // Duplicated bkey. Compare the "cache key".
          int keyComp = result.compareKeyTo(mergedResult.get(pos));
          if ((reverse) ? (keyComp > 0) : (keyComp < 0)) {
            if (unique) {
              // Remove duplicated bkey.
              mergedResult.remove(pos);
            }
            break;
          } else {
            if (unique) {
              // NOT the first cache key with the same bkey. do NOT insert.
              doInsert = false;
              break;
            }
          }
        }
      }
      if (!doInsert) { // UNIQUE
        continue;
      }
      if (pos >= count) {
        // The next element of eachResult must be positioned behind
        // the elements of the mergedResult whose size is count.
        // So, stop the current sort-merge task.
        break;
      }
      mergedResult.add(pos, result);
      if (mergedResult.size() > count) {
        mergedResult.remove(count);
      }
      pos += 1;
    }
  }

  private void mergeTrimmedKeys(final List<SMGetTrimKey> eachTrimmedResult) {
    if (mergedTrimmedKeys.isEmpty()) {
      mergedTrimmedKeys.addAll(eachTrimmedResult);
      return;
    }

    // do sort merge trimmed list
    int comp, pos = 0;
    for (SMGetTrimKey result : eachTrimmedResult) {
      for (; pos < mergedTrimmedKeys.size(); pos++) {
        comp = result.compareTo(mergedTrimmedKeys.get(pos));
        if ((reverse) ? (comp > 0) : (comp < 0)) {
          break;
        }
      }
      mergedTrimmedKeys.add(pos, result);
      pos += 1;
    }
  }

  @Override
  public void makeResultOperationStatus() {
    if (!mergedTrimmedKeys.isEmpty() && count <= mergedResult.size()) {
      // Remove trimmed keys that bkey is behind of the last element
      // when result count is reached to query count.
      SMGetElement<T> lastElement = mergedResult.get(mergedResult.size() - 1);
      SMGetTrimKey lastTrimKey = new SMGetTrimKey(lastElement.getKey(),
              lastElement.getBkeyObject());
      int comp;
      for (int i = mergedTrimmedKeys.size() - 1; i >= 0; i--) {
        comp = mergedTrimmedKeys.get(i).compareTo(lastTrimKey);
        if ((reverse) ? (comp <= 0) : (comp >= 0)) {
          mergedTrimmedKeys.remove(i);
        } else {
          break;
        }
      }
    }

    final OperationStatus status;
    if (!unique && hasDuplicatedBKeyResult()) {
      status = new OperationStatus(true, "DUPLICATED");
    } else {
      status = new OperationStatus(true, "END");
    }
    resultOperationStatus = new CollectionOperationStatus(status);
  }
}
