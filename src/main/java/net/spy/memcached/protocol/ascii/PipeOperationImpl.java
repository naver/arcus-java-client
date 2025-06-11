package net.spy.memcached.protocol.ascii;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import net.spy.memcached.collection.CollectionBulkInsert;
import net.spy.memcached.collection.CollectionPipe;
import net.spy.memcached.collection.CollectionPipedInsert;
import net.spy.memcached.collection.CollectionResponse;
import net.spy.memcached.ops.CollectionOperationStatus;
import net.spy.memcached.ops.OperationCallback;
import net.spy.memcached.ops.OperationErrorType;
import net.spy.memcached.ops.OperationException;
import net.spy.memcached.ops.OperationState;
import net.spy.memcached.ops.OperationStatus;
import net.spy.memcached.ops.PipedOperationCallback;
import net.spy.memcached.ops.StatusCode;

abstract class PipeOperationImpl extends OperationImpl {

  protected static final OperationStatus END = new CollectionOperationStatus(
          true, "END", CollectionResponse.END);
  protected static final OperationStatus FAILED_END = new CollectionOperationStatus(
          false, "END", CollectionResponse.END);

  protected static final OperationStatus CREATED_STORED = new CollectionOperationStatus(
          true, "CREATED_STORED", CollectionResponse.CREATED_STORED);
  protected static final OperationStatus STORED = new CollectionOperationStatus(
          true, "STORED", CollectionResponse.STORED);
  protected static final OperationStatus UPDATED = new CollectionOperationStatus(
          true, "UPDATED", CollectionResponse.UPDATED);
  protected static final OperationStatus NOT_FOUND = new CollectionOperationStatus(
          false, "NOT_FOUND", CollectionResponse.NOT_FOUND);
  protected static final OperationStatus NOT_FOUND_ELEMENT = new CollectionOperationStatus(
          false, "NOT_FOUND_ELEMENT", CollectionResponse.NOT_FOUND_ELEMENT);
  protected static final OperationStatus NOTHING_TO_UPDATE = new CollectionOperationStatus(
          false, "NOTHING_TO_UPDATE", CollectionResponse.NOTHING_TO_UPDATE);
  protected static final OperationStatus ELEMENT_EXISTS = new CollectionOperationStatus(
          false, "ELEMENT_EXISTS", CollectionResponse.ELEMENT_EXISTS);
  protected static final OperationStatus OVERFLOWED = new CollectionOperationStatus(
          false, "OVERFLOWED", CollectionResponse.OVERFLOWED);
  protected static final OperationStatus OUT_OF_RANGE = new CollectionOperationStatus(
          false, "OUT_OF_RANGE", CollectionResponse.OUT_OF_RANGE);
  protected static final OperationStatus TYPE_MISMATCH = new CollectionOperationStatus(
          false, "TYPE_MISMATCH", CollectionResponse.TYPE_MISMATCH);
  protected static final OperationStatus BKEY_MISMATCH = new CollectionOperationStatus(
          false, "BKEY_MISMATCH", CollectionResponse.BKEY_MISMATCH);
  protected static final OperationStatus EFLAG_MISMATCH = new CollectionOperationStatus(
          false, "EFLAG_MISMATCH", CollectionResponse.EFLAG_MISMATCH);

  protected static final OperationStatus EXIST = new CollectionOperationStatus(
          true, "EXIST", CollectionResponse.EXIST);
  protected static final OperationStatus NOT_EXIST = new CollectionOperationStatus(
          true, "NOT_EXIST", CollectionResponse.NOT_EXIST);
  protected static final OperationStatus UNREADABLE = new CollectionOperationStatus(
          false, "UNREADABLE", CollectionResponse.UNREADABLE);

  protected boolean successAll = true;

  private final CollectionPipe collectionPipe;
  private final PipedOperationCallback cb;
  private final List<String> keys;
  private final boolean isIdempotent;

  private int index = 0;
  private boolean readUntilLastLine = false;

  protected PipeOperationImpl(List<String> keys, CollectionPipe collectionPipe,
                              OperationCallback cb) {
    super(cb);
    this.cb = (PipedOperationCallback) cb;
    if (keys == null || keys.isEmpty()) {
      throw new IllegalArgumentException("No keys provided");
    }
    this.keys = keys;
    this.collectionPipe = collectionPipe;
    this.isIdempotent = !(collectionPipe instanceof CollectionPipedInsert.ListPipedInsert ||
            collectionPipe instanceof CollectionBulkInsert.ListBulkInsert);
  }

  @Override
  public void handleLine(String line) {
    assert getState() == OperationState.READING
            : "Read ``" + line + "'' when in " + getState() + " state";

    /* ENABLE_REPLICATION if */
    if (isWriteOperation() && hasSwitchedOver(line)) {
      this.collectionPipe.setNextOpIndex(index);
      prepareSwitchover(line);
      return;
    }
    /* ENABLE_REPLICATION end */

    /* ENABLE_MIGRATION if */
    if (hasNotMyKey(line)) {
      if (isBulkOperation()) {
        addRedirectMultiKeyOperation(line, keys.get(index));
        if (collectionPipe.isNotPiped()) {
          transitionState(OperationState.REDIRECT);
        } else {
          index++;
        }
      } else {
        // Only one NOT_MY_KEY is provided in response of
        // single key piped operation when redirection.
        addRedirectSingleKeyOperation(line, keys.get(0));
        if (collectionPipe.isNotPiped()) {
          transitionState(OperationState.REDIRECT);
        } else {
          collectionPipe.setNextOpIndex(index);
        }
      }
      return;
    }
    /* ENABLE_MIGRATION end */

    if (collectionPipe.isNotPiped()) {
      OperationStatus status = checkStatus(line);
      if (!status.isSuccess()) {
        successAll = false;
      }
      cb.gotStatus(index, status);

      complete((successAll) ? END : FAILED_END);
      return;
    }

    /*
      RESPONSE <count>\r\n
      <status of the 1st pipelined command>\r\n
      [ ... ]
      <status of the last pipelined command>\r\n
      END|PIPE_ERROR <error_string>\r\n
    */
    if (line.startsWith("END") || line.startsWith("PIPE_ERROR ")) {
      /* ENABLE_MIGRATION if */
      if (needRedirect()) {
        transitionState(OperationState.REDIRECT);
        return;
      }
      /* ENABLE_MIGRATION end */
      OperationStatus status;
      if (exception == null) {
        status = (successAll) ? END : FAILED_END;
      } else {
        status = new OperationStatus(false, exception.getMessage(), StatusCode.ERR_INTERNAL);
      }
      complete(status);
    } else if (line.startsWith("RESPONSE ")) {
      getLogger().debug("Got line %s", line);

      // TODO server should be fixed
      line = line.replace("   ", " ");
      line = line.replace("  ", " ");

      String[] stuff = line.split(" ");
      assert "RESPONSE".equals(stuff[0]);
      readUntilLastLine = true;
    } else {
      OperationStatus status = checkStatus(line);
      if (!status.isSuccess()) {
        successAll = false;
      }
      cb.gotStatus(index, status);

      index++;
    }
  }

  @Override
  protected void handleError(OperationErrorType eType, String line) throws IOException {
    if (!readUntilLastLine) {
      // this case means that error message came without 'RESPONSE <count>'.
      // so it doesn't need to read 'PIPE_ERROR'.
      super.handleError(eType, line);
    } else {
      // this case means that error message came after 'RESPONSE <count>'.
      // so it needs to read 'PIPE_ERROR'.
      getLogger().error("Error:  %s by %s", line, this);
      exception = new OperationException(eType, line + " @ " + getHandlingNode().getNodeName());
    }
  }

  /**
   * call matchStatus() method with proper statuses in the child class
   *
   * @param line line that is read from the server
   * @return status that is matched with the line
   */
  protected abstract OperationStatus checkStatus(String line);

  @Override
  public void initialize() {
    ByteBuffer buffer = collectionPipe.getAsciiCommand();
    setBuffer(buffer);
    readUntilLastLine = false;

    if (getLogger().isDebugEnabled()) {
      getLogger().debug("Request in ascii protocol: %s",
              (new String(buffer.array())).replace("\r\n", "\\r\\n"));
    }
  }

  public Collection<String> getKeys() {
    return Collections.unmodifiableList(keys);
  }

  public CollectionPipe getCollectionPipe() {
    return collectionPipe;
  }

  @Override
  public final boolean isPipeOperation() {
    return true;
  }

  @Override
  public boolean isIdempotentOperation() {
    return isIdempotent;
  }
}
