
package net.spy.memcached.v2.attribute;

import net.spy.memcached.collection.CollectionOverflowAction;
import net.spy.memcached.collection.SetAttributes;
import net.spy.memcached.v2.vo.BKey;

/**
 * Attributes to set on an existing collection via {@code setAttributes} (v2).
 *
 * <p>Fields are nullable: only fields explicitly set are sent, leaving the
 * others unchanged on the server (partial update). {@code expireTime} is a
 * {@code long}, so Unix timestamps beyond the {@code int} range are supported.
 * {@code maxBkeyRange} is a b+tree-only attribute expressed as a v2 {@link BKey}.
 */
public final class UpdateAttributes implements SetAttributes {

  private final String clause;

  private UpdateAttributes(Builder builder) {
    StringBuilder b = new StringBuilder();
    if (builder.expTime != null) {
      b.append(" expiretime=").append(builder.expTime);
    }
    if (builder.maxCount != null) {
      b.append(" maxcount=").append(builder.maxCount);
    }
    if (builder.overflowAction != null) {
      b.append(" overflowaction=").append(builder.overflowAction);
    }
    if (builder.readable != null) {
      b.append(" readable=").append("on");
    }
    if (builder.maxBKeyRange != null) {
      b.append(" maxbkeyrange=").append(builder.maxBKeyRange);
    }
    this.clause = (b.length() < 1) ? "" : b.substring(1);
  }

  public static Builder builder() {
    return new Builder();
  }

  @Override
  public String stringify() {
    return clause;
  }

  @Override
  public int getLength() {
    return clause.length();
  }

  public static final class Builder {
    private Long expTime;
    private Long maxCount;
    private CollectionOverflowAction overflowAction;
    private Boolean readable;
    private BKey maxBKeyRange;

    private Builder() {
    }

    public Builder expireTime(long expireTime) {
      this.expTime = expireTime;
      return this;
    }

    /**
     * Sets the maximum number of elements the collection may hold.
     *
     * <p>The value is interpreted by the server, and two cases are handled
     * specially:
     * <ul>
     *   <li>
     *     If the value is {@code 0}, the server applies its default count (4,000).
     *   </li>
     *   <li>
     *     If the value is greater than the server's configured maximum,
     *     the server silently clamps it to that maximum
     *     and still reports the operation as successful.
     *   </li>
     * </ul>
     *
     * @param maxCount the maximum number of elements
     * @return this Builder
     */
    public Builder maxCount(long maxCount) {
      this.maxCount = maxCount;
      return this;
    }

    public Builder overflowAction(CollectionOverflowAction overflowAction) {
      this.overflowAction = overflowAction;
      return this;
    }

    public Builder readable() {
      this.readable = true;
      return this;
    }

    public Builder maxBKeyRange(BKey maxBKeyRange) {
      this.maxBKeyRange = maxBKeyRange;
      return this;
    }

    public UpdateAttributes build() {
      UpdateAttributes attributes = new UpdateAttributes(this);
      if (attributes.getLength() == 0) {
        throw new IllegalArgumentException("At least one attribute must be set.");
      }

      return attributes;
    }
  }
}
