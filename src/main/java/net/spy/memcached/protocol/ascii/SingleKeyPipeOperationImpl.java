package net.spy.memcached.protocol.ascii;

import java.util.List;

import net.spy.memcached.collection.CollectionPipe;
import net.spy.memcached.ops.OperationCallback;
import net.spy.memcached.ops.OperationState;
import net.spy.memcached.ops.OperationStatus;

public abstract class SingleKeyPipeOperationImpl extends PipeOperationImpl {

  protected SingleKeyPipeOperationImpl(List<String> keys,
                                       CollectionPipe collectionPipe,
                                       OperationCallback cb) {
    super(keys, collectionPipe, cb);
  }

  @Override
  public void handleLine(String line) {
    assert getState() == OperationState.READING
            : "Read ``" + line + "'' when in " + getState() + " state";

    /* ENABLE_REPLICATION if */
    if (isWriteOperation() && hasSwitchedOver(line)) {
      collectionPipe.setNextOpIndex(index);
      prepareSwitchover(line);
      return;
    }
    /* ENABLE_REPLICATION end */

    /* ENABLE_MIGRATION if */
    if (hasNotMyKey(line)) {
      // Only one NOT_MY_KEY is provided in response of
      // single key piped operation when redirection.
      addRedirectSingleKeyOperation(line, keys.get(0));
      if (collectionPipe.isNotPiped()) {
        transitionState(OperationState.REDIRECT);
      } else {
        collectionPipe.setNextOpIndex(index);
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

      cb.receivedStatus((successAll) ? END : FAILED_END);
      transitionState(OperationState.COMPLETE);
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
      cb.receivedStatus((index == collectionPipe.getItemCount() && successAll) ? END : FAILED_END);
      transitionState(OperationState.COMPLETE);
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
  protected OperationStatus checkStatus(String line) {
    return null;
  }

}
