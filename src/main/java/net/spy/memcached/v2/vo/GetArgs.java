package net.spy.memcached.v2.vo;

public final class GetArgs {

  public static final GetArgs DEFAULT = new GetArgs.Builder().build();

  private final boolean withDelete;
  private final boolean dropIfEmpty;

  public boolean isWithDelete() {
    return withDelete;
  }

  public boolean isDropIfEmpty() {
    return dropIfEmpty;
  }

  private GetArgs(boolean withDelete, boolean dropIfEmpty) {
    this.withDelete = withDelete;
    this.dropIfEmpty = dropIfEmpty;
  }

  public static class Builder {
    private boolean withDelete = false;
    private boolean dropIfEmpty = false;

    public Builder withDelete() {
      this.withDelete = true;
      return this;
    }

    public Builder dropIfEmpty() {
      this.dropIfEmpty = true;
      return this;
    }

    public GetArgs build() {
      return new GetArgs(withDelete, dropIfEmpty);
    }
  }

}
