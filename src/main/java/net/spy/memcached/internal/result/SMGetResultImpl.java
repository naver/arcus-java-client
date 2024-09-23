package net.spy.memcached.internal.result;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import net.spy.memcached.collection.BKeyObject;
import net.spy.memcached.collection.SMGetElement;
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
    trimmedKeyMap.clear();
    missedKeyMap.clear();
  }

  @Deprecated
  private void mergeSMGetElementsOld(final List<SMGetElement<T>> eachResult) {
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

  public void mergeSMGetElements(final List<SMGetElement<T>> eachResult) {
    if (mergedResult.isEmpty()) {
      // merged result is empty, add all.
      mergedResult.addAll(eachResult);

      while (mergedResult.size() > count) {
        mergedResult.remove(count);
      }
      return;
    }

    final int eachSize = eachResult.size();
    final int oldMergedSize = mergedResult.size();
    final List<SMGetElement<T>> newMergedResult = new ArrayList<>(count);

    int eachPos = 0, oldMergedPos = 0, comp;
    boolean bkeyDuplicated;

    while (eachPos < eachSize && oldMergedPos < oldMergedSize && newMergedResult.size() < count) {
      final SMGetElement<T> eachElem = eachResult.get(eachPos);
      final SMGetElement<T> oldMergedElem = mergedResult.get(oldMergedPos);

      comp = eachElem.compareBkeyTo(oldMergedElem);
      bkeyDuplicated = (comp == 0);
      if (bkeyDuplicated) {
        // Duplicated bkey. Compare the "cache key".
        comp = eachElem.compareKeyTo(oldMergedElem);
        assert comp != 0 : "Unexpected smget elements. Duplicated cache key : " + eachElem.getKey();
      }
      if ((reverse) ? (comp > 0) : (comp < 0)) {
        newMergedResult.add(eachElem);
        eachPos++;

        if (unique && bkeyDuplicated) {
          // NOT the first cache key with the same bkey. do NOT insert.
          oldMergedPos++;
        }
      } else {
        newMergedResult.add(oldMergedElem);
        oldMergedPos++;

        if (unique && bkeyDuplicated) {
          // NOT the first cache key with the same bkey. do NOT insert.
          eachPos++;
        }
      }
    }

    while (eachPos < eachSize && newMergedResult.size() < count) {
      newMergedResult.add(eachResult.get(eachPos++));
    }
    while (oldMergedPos < oldMergedSize && newMergedResult.size() < count) {
      newMergedResult.add(mergedResult.get(oldMergedPos++));
    }

    mergedResult = newMergedResult;
  }

  @Override
  public void makeResultOperationStatus() {
    refineTrimmedKeys();

    final OperationStatus status;
    if (!unique && hasDuplicatedBKeyResult()) {
      status = new OperationStatus(true, "DUPLICATED");
    } else {
      status = new OperationStatus(true, "END");
    }
    resultOperationStatus = new CollectionOperationStatus(status);
  }

  /**
   * Trimmed keys that are larger/smaller(depends on unique variable) than last element of
   * {@link SMGetResultImpl#mergedResult} should be removed when result's size reached to the count.
   * Because the results are sufficient and don't need to retrieve the data further
   * using trimmed keys.
   */
  private void refineTrimmedKeys() {
    if (!trimmedKeyMap.isEmpty() && count <= mergedResult.size()) {
      SMGetElement<T> lastElement = mergedResult.get(mergedResult.size() - 1);

      // FIXME: use removeIf with entrySet() when upgrading java version 8
      Iterator<Map.Entry<String, BKeyObject>> iterator = trimmedKeyMap.entrySet().iterator();
      while (iterator.hasNext()) {
        Map.Entry<String, BKeyObject> entry = iterator.next();
        int comp = entry.getValue().compareTo(lastElement.getBkeyObject());
        if ((reverse) ? (comp <= 0) : (comp >= 0)) {
          iterator.remove();
        }
      }
    }
  }
}
