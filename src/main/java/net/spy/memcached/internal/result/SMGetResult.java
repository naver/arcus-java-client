package net.spy.memcached.internal.result;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import net.spy.memcached.collection.SMGetElement;
import net.spy.memcached.collection.SMGetTrimKey;
import net.spy.memcached.ops.CollectionOperationStatus;
import net.spy.memcached.ops.OperationStatus;

public abstract class SMGetResult<T>  {
  protected final int totalResultElementCount;
  protected final boolean reverse;

  protected final List<String> missedKeyList;
  protected final Map<String, CollectionOperationStatus> missedKeyMap;
  protected final List<SMGetTrimKey> mergedTrimmedKeys;

  protected final List<SMGetElement<T>> mergedResult;
  protected final List<OperationStatus> resultOperationStatus;
  protected final List<OperationStatus> failedOperationStatus;

  public SMGetResult(int totalResultElementCount, boolean reverse) {
    this.totalResultElementCount = totalResultElementCount;
    this.reverse = reverse;

    this.missedKeyList = Collections.synchronizedList(new ArrayList<String>());
    this.missedKeyMap
            = Collections.synchronizedMap(new HashMap<String, CollectionOperationStatus>());
    this.mergedTrimmedKeys = Collections.synchronizedList(new ArrayList<SMGetTrimKey>());

    this.mergedResult = new ArrayList<SMGetElement<T>>(totalResultElementCount);
    this.resultOperationStatus = Collections.synchronizedList(new ArrayList<OperationStatus>(1));
    this.failedOperationStatus = Collections.synchronizedList(new ArrayList<OperationStatus>(1));
  }

  public List<String> getMissedKeyList() {
    return missedKeyList;
  }

  public Map<String, CollectionOperationStatus> getMissedKeyMap() {
    return missedKeyMap;
  }

  public List<SMGetTrimKey> getMergedTrimmedKeys() {
    return mergedTrimmedKeys;
  }

  public void addMissedKey(String key, CollectionOperationStatus cstatus) {
    missedKeyList.add(key);
    missedKeyMap.put(key, cstatus);
  }

  public CollectionOperationStatus getOperationStatus() {
    if (!failedOperationStatus.isEmpty()) {
      return new CollectionOperationStatus(failedOperationStatus.get(0));
    }
    return new CollectionOperationStatus(resultOperationStatus.get(0));
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
