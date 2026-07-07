/*
 * arcus-java-client : Arcus Java client
 * Copyright 2010-2014 NAVER Corp.
 * Copyright 2014-present JaM2in Co., Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package net.spy.memcached.collection;

/**
 * Attributes used when creating a collection (also carried by insert-with-create).
 *
 * <p>Unlike {@link CollectionAttributes}, the expiration time is kept as a
 * {@code long} so that Unix timestamp values beyond the {@code int} range
 * (e.g. absolute expiration dates after year 2038) can be sent to the server.
 */
public final class CreateAttributes {

  private static final long DEFAULT_EXPIRE_TIME = 0L;
  private static final long DEFAULT_MAX_COUNT = 4_000L;
  private static final boolean DEFAULT_READABLE = true;

  public static final CreateAttributes DEFAULT = CreateAttributes.builder()
      .expireTime(DEFAULT_EXPIRE_TIME)
      .maxCount(DEFAULT_MAX_COUNT)
      .overflowAction(null)
      .readable(DEFAULT_READABLE)
      .build();

  private final long expireTime;
  private final long maxCount;
  private final CollectionOverflowAction overflowAction;
  private final boolean readable;

  private CreateAttributes(Builder builder) {
    this.expireTime = builder.expireTime;
    this.maxCount = builder.maxCount;
    this.overflowAction = builder.overflowAction;
    this.readable = builder.readable;
  }

  public static Builder builder() {
    return new Builder();
  }

  /**
   * Converts the existing API's attributes into a CreateAttributes.
   *
   * @param attr attributes to convert, may be null
   * @return converted CreateAttributes, or null if the given attributes is null
   */
  public static CreateAttributes of(CollectionAttributes attr) {
    if (attr == null) {
      return null;
    }

    return CreateAttributes.builder()
        .expireTime(attr.getExpireTime() != null ? attr.getExpireTime() : DEFAULT_EXPIRE_TIME)
        .maxCount(attr.getMaxCount() != null ? attr.getMaxCount() : DEFAULT_MAX_COUNT)
        .overflowAction(attr.getOverflowAction())
        .readable(attr.getReadable() == null || attr.getReadable())
        .build();
  }

  public long getExpireTime() {
    return this.expireTime;
  }

  public long getMaxCount() {
    return this.maxCount;
  }

  public CollectionOverflowAction getOverflowAction() {
    return this.overflowAction;
  }

  public boolean getReadable() {
    return this.readable;
  }

  public static final class Builder {
    private long expireTime = DEFAULT_EXPIRE_TIME;
    private long maxCount = DEFAULT_MAX_COUNT;
    private CollectionOverflowAction overflowAction;
    private boolean readable = DEFAULT_READABLE;

    private Builder() {
    }

    public Builder expireTime(long expireTime) {
      this.expireTime = expireTime;
      return this;
    }

    public Builder maxCount(long maxCount) {
      this.maxCount = maxCount;
      return this;
    }

    public Builder overflowAction(CollectionOverflowAction overflowAction) {
      this.overflowAction = overflowAction;
      return this;
    }

    public Builder readable(boolean readable) {
      this.readable = readable;
      return this;
    }

    public CreateAttributes build() {
      return new CreateAttributes(this);
    }
  }
}
