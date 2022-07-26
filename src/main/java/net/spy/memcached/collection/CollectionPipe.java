package net.spy.memcached.collection;

import java.nio.ByteBuffer;

public abstract class CollectionPipe extends CollectionObject {

  public static final String PIPE = "pipe";

  protected final int itemCount;
  protected int nextOpIndex = 0;

  protected CollectionPipe(int itemCount) {
    this.itemCount = itemCount;
  }

  public int getItemCount() {
    return this.itemCount;
  }

  /**
   * set next index of operation
   * that will be processed after when operation
   * moved by switchover or redirected by migration.
   */
  public void setNextOpIndex(int i) {
    this.nextOpIndex = i;
  }

  public boolean isNotPiped() {
    /*
     * We can consider two cases in this method.
     *
     * First, itemCount == 1 && nextOpIndex == 0.
     * In this case, there is only one line of ascii command.
     * Of course, there is no "pipe" keyword.
     *
     * Second, itemCount > 1 && nextOpIndex == itemCount - 1.
     * In this case, there is only one last line of ascii command
     * that is moved by switchover or redirected by migration.
     * Of course, there is no "pipe" keyword either.
     */
    return itemCount - nextOpIndex == 1;
  }

  public ByteBuffer getBinaryCommand() {
    throw new RuntimeException("not supported in binary protocol yet.");
  }

  public abstract ByteBuffer getAsciiCommand();
}
