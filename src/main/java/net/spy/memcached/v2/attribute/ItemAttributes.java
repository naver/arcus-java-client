package net.spy.memcached.v2.attribute;

import net.spy.memcached.collection.BKeyObject;
import net.spy.memcached.collection.CollectionOverflowAction;
import net.spy.memcached.collection.CollectionType;
import net.spy.memcached.compat.SpyObject;
import net.spy.memcached.util.BTreeUtil;
import net.spy.memcached.v2.vo.BKey;

/**
 * Result of a {@code getAttributes} (getattr) call in the v2 API. Read-only.
 *
 * <p>{@code expireTime} is exposed as {@code long}. The server returns the
 * remaining relative seconds, which always fits in {@code int} range, but the
 * type is kept as {@code Long} for consistency with the v2 write side.
 */
public class ItemAttributes extends SpyObject {

  private Integer flags;
  private Long expireTime;
  private CollectionType type;
  private Long count;
  private Long maxCount;
  private CollectionOverflowAction overflowAction;
  private Boolean readable;
  private BKey maxBKeyRange;
  private BKey minBKey;
  private BKey maxBKey;
  private Long trimmed;

  public void setAttribute(String attribute) {
    String[] split = attribute.split("=");
    if (split.length != 2) {
      return;
    }
    String name = split[0];
    String value = split[1];

    try {
      if ("flags".equals(name)) {
        flags = Integer.parseInt(value);
      } else if ("expiretime".equals(name)) {
        expireTime = Long.parseLong(value);
      } else if ("type".equals(name)) {
        type = CollectionType.find(value);
      } else if ("count".equals(name)) {
        count = Long.parseLong(value);
      } else if ("maxcount".equals(name)) {
        maxCount = Long.parseLong(value);
      } else if ("overflowaction".equals(name)) {
        overflowAction = CollectionOverflowAction.valueOf(value);
      } else if ("readable".equals(name)) {
        readable = "on".equals(value);
      } else if ("maxbkeyrange".equals(name)) {
        maxBKeyRange = parseBKey(value);
      } else if ("minbkey".equals(name)) {
        if (!value.startsWith("-1")) {
          minBKey = parseBKey(value);
        }
      } else if ("maxbkey".equals(name)) {
        if (!value.startsWith("-1")) {
          maxBKey = parseBKey(value);
        }
      } else if ("trimmed".equals(name)) {
        trimmed = Long.parseLong(value);
      }
    } catch (Exception e) {
      getLogger().info(e, e);
    }
  }

  private static BKey parseBKey(String value) {
    if (value.startsWith("0x")) {
      return BKey.of(new BKeyObject(BTreeUtil.hexStringToByteArrays(value.substring(2))));
    }
    return BKey.of(new BKeyObject(Long.parseLong(value)));
  }

  public Integer getFlags() {
    return flags;
  }

  public Long getExpireTime() {
    return expireTime;
  }

  public CollectionType getType() {
    return type;
  }

  public Long getCount() {
    return count;
  }

  public Long getMaxCount() {
    return maxCount;
  }

  public CollectionOverflowAction getOverflowAction() {
    return overflowAction;
  }

  public Boolean getReadable() {
    return readable;
  }

  public BKey getMaxBKeyRange() {
    return maxBKeyRange;
  }

  public BKey getMinBKey() {
    return minBKey;
  }

  public BKey getMaxBKey() {
    return maxBKey;
  }

  public Long getTrimmed() {
    return trimmed;
  }

}
