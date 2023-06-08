// Copyright (c) 2006  Dustin Sallings <dustin@spy.net>

package net.spy.memcached.protocol.ascii;


import net.spy.memcached.ops.APIType;
import net.spy.memcached.ops.GetOperation;
import net.spy.memcached.ops.OperationState;
import net.spy.memcached.ops.OperationStatus;
import net.spy.memcached.ops.StatusCode;

import java.util.Collection;

/**
 * Operation for retrieving data.
 */
public class ReactiveGetOperationImpl extends GetOperationImpl implements GetOperation {
  public static final OperationStatus END =
          new OperationStatus(true, "END", StatusCode.SUCCESS);
  private static final String CMD = "get";

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
      ((Callback) callback).readComplete();
      transitionState(OperationState.COMPLETE);
      data = null;
    } else if (line.startsWith("VALUE ")) {
      getLogger().debug("Got line %s", line);
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
      assert false : "Unknown line type: " + line;
    }
  }

  interface Callback extends GetOperation.Callback {
    void readComplete();
  }
}
