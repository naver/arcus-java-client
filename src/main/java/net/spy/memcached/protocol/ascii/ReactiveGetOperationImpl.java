// Copyright (c) 2006  Dustin Sallings <dustin@spy.net>

package net.spy.memcached.protocol.ascii;

import net.spy.memcached.ops.*;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;

/**
 * Operation for retrieving data.
 */
public class ReactiveGetOperationImpl extends GetOperationImpl implements GetOperation {
  public static final OperationStatus END =
          new OperationStatus(true, "END", StatusCode.SUCCESS);
  private static final String CMD = "get";
  private boolean valueRead = false;

  public ReactiveGetOperationImpl(String key, Callback c) {
    super(key, c);
    setAPIType(APIType.GET);
  }

  public ReactiveGetOperationImpl(Collection<String> k, Callback c) {
    super(k, c);
    setAPIType(APIType.GET);
  }

  @Override
  public final void handleLine(String line) {
    if (line.equals("END")) {
      getLogger().debug("Get complete!");
      /* ENABLE_MIGRATION if */
      notMyKeyLine = null;
      if (needRedirect()) {
        transitionState(OperationState.REDIRECT);
        return;
      }
      /* ENABLE_MIGRATION end */
      if (!valueRead) {
        transitionState(OperationState.COMPLETE);
      }
      data = null;
    } else if (line.startsWith("VALUE ")) {
      getLogger().debug("Got line %s", line);
      valueRead = true;
      String[] stuff = line.split(" ");
      assert stuff[0].equals("VALUE");
      currentKey = stuff[1];
      currentFlags = Integer.parseInt(stuff[2]);
      data = new byte[Integer.parseInt(stuff[3])];
      if (stuff.length > 4) {
        casValue = Long.parseLong(stuff[4]);
      }
      readOffset = 0;
      getLogger().debug("Set read type to data");
      setReadType(OperationReadType.DATA);
      /* ENABLE_MIGRATION if */
    } else if (hasNotMyKey(line)) {
      notMyKeyLine = line;
    } else if (notMyKeyLine != null) {
      addRedirectMultiKeyOperation(notMyKeyLine, line.trim());
      /* ENABLE_MIGRATION end */
    } else {
      data = null;
//      assert false : "Unknown line type: " + line;
    }
  }

  @Override
  public final void handleRead(ByteBuffer b) {
    assert currentKey != null;
    assert data != null;
    // This will be the case, because we'll clear them when it's not.
    assert readOffset <= data.length : "readOffset is " + readOffset
            + " data.length is " + data.length;

    getLogger().debug("readOffset: %d, length: %d", readOffset, data.length);
    // If we're not looking for termination, we're still looking for data
    if (lookingFor == '\0') {
      int toRead = data.length - readOffset;
      int available = b.remaining();
      toRead = Math.min(toRead, available);
      getLogger().debug("Reading %d bytes", toRead);
      b.get(data, readOffset, toRead);
      readOffset += toRead;
    }
    // Transition us into a ``looking for \r\n'' kind of state if we've
    // read enough and are still in a data state.
    if (readOffset == data.length && lookingFor == '\0') {
      // The callback is most likely a get callback.  If it's not, then
      // it's a gets callback.
      try {
        GetOperation.Callback gcb = (GetOperation.Callback) getCallback();
        gcb.gotData(currentKey, currentFlags, data);
      } catch (ClassCastException e) {
        GetsOperation.Callback gcb = (GetsOperation.Callback)
                getCallback();
        gcb.gotData(currentKey, currentFlags, casValue, data);
      }
      lookingFor = '\r';
    }
    // If we're looking for an ending byte, let's go find it.
    if (lookingFor != '\0' && b.hasRemaining()) {
      do {
        byte tmp = b.get();
        assert tmp == lookingFor : "Expecting " + lookingFor + ", got "
                + (char) tmp;
        switch (lookingFor) {
          case '\r':
            lookingFor = '\n';
            break;
          case '\n':
            lookingFor = '\0';
            break;
          default:
            assert false : "Looking for unexpected char: " + (char) lookingFor;
        }
      } while (lookingFor != '\0' && b.hasRemaining());
      // Completed the read, reset stuff.
      if (lookingFor == '\0') {
        currentKey = null;
        data = null;
        readOffset = 0;
        currentFlags = 0;
        getLogger().debug("Setting read type back to line.");
        setReadType(OperationReadType.LINE);
      }
    }
  }

  @Override
  public final void cancel(String cause) {
    cancelled = true;
    if (handlingNode != null) {
      cancelCause = "Cancelled (" + cause + " : (" + handlingNode.getNodeName() + ")" + ")";
    } else {
      cancelCause = "Cancelled (" + cause + ")";
    }
    getCallback().receivedStatus(CANCELLED);
    callback.complete();
  }
  @Override
  public final void transitionState(OperationState newState) {
    getLogger().debug("Transitioned state from %s to %s", state, newState);
    state = newState;
    // Discard our buffer when we no longer need it.
    if (state != OperationState.WRITE_QUEUED &&
            state != OperationState.WRITING) {
      cmd = null;
    }
    if (state == OperationState.COMPLETE) {
      callback.complete();
    }
  }
}
