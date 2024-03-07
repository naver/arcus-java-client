package net.spy.memcached.internal.result;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import net.spy.memcached.collection.SMGetElement;
import net.spy.memcached.ops.CollectionOperationStatus;
import net.spy.memcached.ops.OperationStatus;

public final class SMGetResultOldImpl<T> extends SMGetResult<T> {
  private final AtomicBoolean isMergedResultTrimmed = new AtomicBoolean(false);
  private final int offset;
  private final boolean isMultiNode;

  private List<SMGetElement<T>> finalResult = null;

  public SMGetResultOldImpl(int offset, int count, boolean reverse, boolean isMultiNode) {
    super(offset + count, reverse);

    this.offset = offset;
    this.isMultiNode = isMultiNode;
  }

  @Override
  public List<SMGetElement<T>> getFinalResult() {
    if (finalResult == null) {
      if (isMultiNode && offset > 0) {
        if (offset < mergedResult.size()) {
          finalResult = mergedResult.subList(offset, mergedResult.size());
        } else {
          finalResult = Collections.emptyList();
        }
      } else {
        finalResult = mergedResult;
      }
    }

    return finalResult;
  }

  public void setFailedOperationStatus(OperationStatus status) {
    if (failedOperationStatus == null) {
      failedOperationStatus = new CollectionOperationStatus(status);
    }
    mergedResult.clear();
  }

  public void mergeSMGetElements(final List<SMGetElement<T>> eachResult,
                                 final boolean isEachResultTrimmed) {

    if (mergedResult.isEmpty()) {
      mergedResult.addAll(eachResult);
      isMergedResultTrimmed.set(isEachResultTrimmed);

      // remove elements that exceed the totalResultElementCount
      while (mergedResult.size() > totalResultElementCount) {
        mergedResult.remove(totalResultElementCount);
      }
      return;
    }

    boolean allAdded = true; // Is all element of eachResult added to mergedResult?
    int comp = 0, pos = 0;
    for (SMGetElement<T> result : eachResult) {
      for (; pos < mergedResult.size(); pos++) {
        comp = result.compareTo(mergedResult.get(pos));
        if ((reverse) ? (comp > 0) : (comp < 0)) {
          break;
        }
      }
      if (pos >= totalResultElementCount) {
        // Can NOT add more than the totalResultElementCount.
        allAdded = false;
        break;
      }
      if (pos >= mergedResult.size() && isMergedResultTrimmed.get() && comp != 0) {
        // Can NOT add to the trimmed area of mergedResult.
        allAdded = false;
        break;
      }
      mergedResult.add(pos, result);
      if (mergedResult.size() > totalResultElementCount) {
        mergedResult.remove(totalResultElementCount);
      }
      pos += 1;
    }
    if (isEachResultTrimmed && allAdded && pos > 0) {
      // If eachResult is trimmed and all element of it is added,
      // trim the elements of mergedResult that exist in the trimmed area of eachResult.
      while (pos < mergedResult.size()) {
        if (mergedResult.get(pos).compareBkeyTo(mergedResult.get(pos - 1)) == 0) {
          pos += 1;
        } else {
          mergedResult.remove(pos);
        }
      }
      isMergedResultTrimmed.set(true);
    }
    if (mergedResult.size() >= totalResultElementCount) {
      // If size of mergedResult is reached to totalResultElementCount,
      // then mergedResult is NOT trimmed.
      isMergedResultTrimmed.set(false);
    }
  }

  @Override
  public void makeResultOperationStatus() {
    final OperationStatus status;
    if (isMergedResultTrimmed.get()) {
      if (hasDuplicatedBKeyResult()) {
        status = new OperationStatus(true, "DUPLICATED_TRIMMED");
      } else {
        status = new OperationStatus(true, "TRIMMED");
      }
    } else {
      if (hasDuplicatedBKeyResult()) {
        status = new OperationStatus(true, "DUPLICATED");
      } else {
        status = new OperationStatus(true, "END");
      }
    }
    resultOperationStatus = new CollectionOperationStatus(status);
  }
}
