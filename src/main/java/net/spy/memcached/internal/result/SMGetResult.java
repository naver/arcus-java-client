package net.spy.memcached.internal.result;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import net.spy.memcached.collection.BKeyObject;
import net.spy.memcached.collection.SMGetElement;
import net.spy.memcached.collection.SMGetTrimKey;
import net.spy.memcached.ops.CollectionOperationStatus;

public abstract class SMGetResult<T>  {
  protected final int totalResultElementCount;
  protected final boolean reverse;

  protected final List<String> missedKeyList;
  protected final Map<String, CollectionOperationStatus> missedKeyMap;
  protected final Map<String, BKeyObject> trimmedKeyMap;
  protected volatile List<SMGetTrimKey> mergedTrimmedKeys;

  protected volatile List<SMGetElement<T>> mergedResult;
  protected volatile CollectionOperationStatus resultOperationStatus = null;
  protected volatile CollectionOperationStatus failedOperationStatus = null;

  public SMGetResult(int totalResultElementCount, boolean reverse) {
    this.totalResultElementCount = totalResultElementCount;
    this.reverse = reverse;

    this.missedKeyList = Collections.synchronizedList(new ArrayList<>());
    this.missedKeyMap
            = Collections.synchronizedMap(new HashMap<>());
    this.trimmedKeyMap = Collections.synchronizedMap(new HashMap<>());
    this.mergedTrimmedKeys = new ArrayList<>();

    this.mergedResult = new ArrayList<>(totalResultElementCount);
  }

  public List<String> getMissedKeyList() {
    return missedKeyList;
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
    missedKeyList.add(key);
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

  protected boolean hasDuplicatedBKeyResult() {
    for (int i = 1; i < mergedResult.size(); i++) {
      if (mergedResult.get(i).compareBkeyTo(mergedResult.get(i - 1)) == 0) {
        return true;
      }
    }
    return false;
  }

  public abstract List<SMGetElement<T>> getFinalResult();
  public abstract void makeResultOperationStatus();
}
