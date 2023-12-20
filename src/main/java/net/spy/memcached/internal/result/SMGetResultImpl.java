package net.spy.memcached.internal.result;

import java.util.List;

import net.spy.memcached.collection.SMGetElement;
import net.spy.memcached.collection.SMGetTrimKey;
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

  public void addFailedOperationStatus(OperationStatus status) {
    failedOperationStatus.add(status);
    mergedResult.clear();
    mergedTrimmedKeys.clear();
  }

  public void mergeSMGetElements(final List<SMGetElement<T>> eachResult,
                                 final List<SMGetTrimKey> eachTrimmedResult) {

    if (mergedResult.isEmpty()) {
      // merged result is empty, add all.
      mergedResult.addAll(eachResult);

      while (mergedResult.size() > count) {
        mergedResult.remove(count);
      }
    } else {
      // do sort merge
      boolean duplicated;
      int comp, pos = 0;
      for (SMGetElement<T> result : eachResult) {
        duplicated = false;
        for (; pos < mergedResult.size(); pos++) {
          // compare b+tree key
          comp = result.compareBkeyTo(mergedResult.get(pos));
          if ((reverse) ? (0 < comp) : (0 > comp)) {
            break;
          }
          if (comp == 0) { // compare key string
            int keyComp = result.compareKeyTo(mergedResult.get(pos));
            if ((reverse) ? (0 < keyComp) : (0 > keyComp)) {
              if (unique) {
                mergedResult.remove(pos); // remove dup bkey
              }
              break;
            } else {
              if (unique) {
                duplicated = true;
                break;
              }
            }
          }
        }
        if (duplicated) { // UNIQUE
          continue;
        }
        if (pos >= count) {
          // At this point, following conditions are met.
          //   - mergedResult.size() == totalResultElementCount &&
          //   - The current <bkey, key> of eachResult is
          //     behind of the last <bkey, key> of mergedResult.
          // Then, all the next <bkey, key> elements of eachResult are
          // definitely behind of the last <bkey, bkey> of mergedResult.
          // So, stop the current sort-merge.
          break;
        }
        mergedResult.add(pos, result);
        if (mergedResult.size() > count) {
          // Remove elements that exceed the requested count.
          mergedResult.remove(count);
        }
        pos += 1;
      }
    }

    if (!eachTrimmedResult.isEmpty()) {
      if (mergedTrimmedKeys.isEmpty()) {
        mergedTrimmedKeys.addAll(eachTrimmedResult);
      } else {
        // do sort merge trimmed list
        int pos = 0;
        for (SMGetTrimKey result : eachTrimmedResult) {
          for (; pos < mergedTrimmedKeys.size(); pos++) {
            if ((reverse) ? (0 < result.compareTo(mergedTrimmedKeys.get(pos)))
                          : (0 > result.compareTo(mergedTrimmedKeys.get(pos)))) {
              break;
            }
          }
          mergedTrimmedKeys.add(pos, result);
          pos += 1;
        }
      }
    }
  }

  @Override
  public void makeResultOperationStatus() {
    if (!mergedTrimmedKeys.isEmpty() && count <= mergedResult.size()) {
      // remove trimed keys whose bkeys are behind of the last element.
      SMGetElement<T> lastElement = mergedResult.get(mergedResult.size() - 1);
      SMGetTrimKey lastTrimKey = new SMGetTrimKey(lastElement.getKey(),
              lastElement.getBkeyObject());
      for (int i = mergedTrimmedKeys.size() - 1; i >= 0; i--) {
        SMGetTrimKey me = mergedTrimmedKeys.get(i);
        if ((reverse) ? (0 >= me.compareTo(lastTrimKey))
                      : (0 <= me.compareTo(lastTrimKey))) {
          mergedTrimmedKeys.remove(i);
        } else {
          break;
        }
      }
    }

    if (unique) {
      resultOperationStatus.add(new OperationStatus(true, "END"));
    } else {
      boolean isDuplicated = false;
      for (int i = 1; i < mergedResult.size(); i++) {
        if (mergedResult.get(i).compareBkeyTo(mergedResult.get(i - 1)) == 0) {
          isDuplicated = true;
          break;
        }
      }
      if (isDuplicated) {
        resultOperationStatus.add(new OperationStatus(true, "DUPLICATED"));
      } else {
        resultOperationStatus.add(new OperationStatus(true, "END"));
      }
    }
  }
}
