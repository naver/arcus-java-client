package net.spy.memcached.internal.result;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import net.spy.memcached.collection.BKeyObject;
import net.spy.memcached.collection.CollectionResponse;
import net.spy.memcached.collection.SMGetElement;
import net.spy.memcached.collection.SMGetTrimKey;
import net.spy.memcached.ops.CollectionOperationStatus;

public final class SMGetResult<T> {
  private final int count;
  private final boolean unique;
  private final boolean reverse;

  private final Map<String, CollectionOperationStatus> missedKeyMap;
  private final Map<String, BKeyObject> trimmedKeyMap;
  private volatile List<SMGetTrimKey> mergedTrimmedKeys;

  private volatile List<SMGetElement<T>> mergedResult;
  private volatile CollectionOperationStatus resultOperationStatus = null;
  private volatile CollectionOperationStatus failedOperationStatus = null;

  public SMGetResult(int count, boolean unique, boolean reverse) {
    this.count = count;
    this.unique = unique;
    this.reverse = reverse;

    this.missedKeyMap = new ConcurrentHashMap<>();
    this.trimmedKeyMap = new ConcurrentHashMap<>();
    this.mergedTrimmedKeys = new ArrayList<>();

    this.mergedResult = new ArrayList<>(count);
  }

  public List<String> getMissedKeyList() {
    return new ArrayList<>(missedKeyMap.keySet());
  }

  public Map<String, CollectionOperationStatus> getMissedKeyMap() {
    return missedKeyMap;
  }

  /**
   * Return trimmed keys to indicate data that existed in the cache server but has been removed.
   * Trimmed keys are internally managed as Map, but returned as List
   * due to backward compatibility.
   * Use resultTrimmedKeys List to reduce conversion from Map to List.
   *
   * @return List of Trimmed Keys
   */
  public List<SMGetTrimKey> getMergedTrimmedKeys() {
    if (mergedTrimmedKeys.size() != trimmedKeyMap.size()) {
      List<SMGetTrimKey> result = new ArrayList<>();
      for (Map.Entry<String, BKeyObject> entry : trimmedKeyMap.entrySet()) {
        result.add(new SMGetTrimKey(entry.getKey(), entry.getValue()));
      }
      Collections.sort(result);
      mergedTrimmedKeys = result;
    }
    return mergedTrimmedKeys;
  }

  public void addMissedKey(String key, CollectionOperationStatus cstatus) {
    missedKeyMap.put(key, cstatus);
  }

  public void addTrimmedKey(String key, BKeyObject bKeyObject) {
    trimmedKeyMap.put(key, bKeyObject);
  }

  public CollectionOperationStatus getOperationStatus() {
    if (failedOperationStatus != null) {
      return failedOperationStatus;
    }
    return resultOperationStatus;
  }

  private boolean hasDuplicatedBKeyResult() {
    for (int i = 1; i < mergedResult.size(); i++) {
      if (mergedResult.get(i).compareBkeyTo(mergedResult.get(i - 1)) == 0) {
        return true;
      }
    }
    return false;
  }

  public List<SMGetElement<T>> getFinalResult() {
    return mergedResult;
  }

  public void setFailedOperationStatus(CollectionOperationStatus status) {
    if (failedOperationStatus == null) {
      failedOperationStatus = status;
    }
    mergedResult.clear();
    trimmedKeyMap.clear();
    missedKeyMap.clear();
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

  public void makeResultOperationStatus() {
    refineTrimmedKeys();

    if (!unique && hasDuplicatedBKeyResult()) {
      resultOperationStatus = new CollectionOperationStatus(true, "DUPLICATED",
              CollectionResponse.DUPLICATED);
    } else {
      resultOperationStatus = new CollectionOperationStatus(true, "END", CollectionResponse.END);
    }
  }

  /**
   * Trimmed keys that are larger/smaller(depends on unique variable) than last element of
   * {@link SMGetResult#mergedResult} should be removed when result's size reached to the count.
   * Because the results are sufficient and don't need to retrieve the data further
   * using trimmed keys.
   */
  private void refineTrimmedKeys() {
    if (!trimmedKeyMap.isEmpty() && count <= mergedResult.size()) {
      BKeyObject lastBKey = mergedResult.get(mergedResult.size() - 1).getBkeyObject();

      trimmedKeyMap.entrySet().removeIf(entry -> {
        int comp = entry.getValue().compareTo(lastBKey);
        return (reverse) ? (comp <= 0) : (comp >= 0);
      });
    }
  }
}
