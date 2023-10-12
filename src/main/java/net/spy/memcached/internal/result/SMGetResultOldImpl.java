package net.spy.memcached.internal.result;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import net.spy.memcached.collection.SMGetElement;
import net.spy.memcached.ops.OperationStatus;

public final class SMGetResultOldImpl<T> extends SMGetResult<T> {
  private final AtomicBoolean mergedTrim = new AtomicBoolean(false);
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

  public void addFailedOperationStatus(OperationStatus status) {
    failedOperationStatus.add(status);
    mergedResult.clear();
  }

  public void mergeSMGetElements(final List<SMGetElement<T>> eachResult,
                                 final boolean isTrimmed) {

    if (mergedResult.isEmpty()) {
      // merged result is empty, add all.
      mergedResult.addAll(eachResult);
      mergedTrim.set(isTrimmed);

      while (mergedResult.size() > totalResultElementCount) {
        mergedResult.remove(totalResultElementCount);
      }
    } else {
      boolean addAll = true;
      int pos = 0;
      for (SMGetElement<T> result : eachResult) {
        for (; pos < mergedResult.size(); pos++) {
          if ((reverse) ? (0 < result.compareTo(mergedResult.get(pos)))
                        : (0 > result.compareTo(mergedResult.get(pos)))) {
            break;
          }
        }
        if (pos >= totalResultElementCount) {
          addAll = false;
          break;
        }
        if (pos >= mergedResult.size() && mergedTrim.get() &&
                result.compareBkeyTo(mergedResult.get(pos - 1)) != 0) {
          addAll = false;
          break;
        }
        mergedResult.add(pos, result);
        if (mergedResult.size() > totalResultElementCount) {
          mergedResult.remove(totalResultElementCount);
        }
        pos += 1;
      }
      if (isTrimmed && addAll) {
        while (pos < mergedResult.size()) {
          if (mergedResult.get(pos).compareBkeyTo(mergedResult.get(pos - 1)) == 0) {
            pos += 1;
          } else {
            mergedResult.remove(pos);
          }
        }
        mergedTrim.set(true);
      }
      if (mergedResult.size() >= totalResultElementCount) {
        mergedTrim.set(false);
      }
    }
  }

  @Override
  public void makeResultOperationStatus() {
    boolean isDuplicated = false;
    for (int i = 1; i < mergedResult.size(); i++) {
      if (mergedResult.get(i).compareBkeyTo(mergedResult.get(i - 1)) == 0) {
        isDuplicated = true;
        break;
      }
    }
    if (mergedTrim.get()) {
      if (isDuplicated) {
        resultOperationStatus.add(new OperationStatus(true, "DUPLICATED_TRIMMED"));
      } else {
        resultOperationStatus.add(new OperationStatus(true, "TRIMMED"));
      }
    } else {
      if (isDuplicated) {
        resultOperationStatus.add(new OperationStatus(true, "DUPLICATED"));
      } else {
        resultOperationStatus.add(new OperationStatus(true, "END"));
      }
    }
  }
}
