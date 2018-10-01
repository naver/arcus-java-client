package net.spy.memcached.protocol.ascii;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Iterator;

import net.spy.memcached.KeyUtil;
import net.spy.memcached.ops.GetOperation;
import net.spy.memcached.ops.GetsOperation;
import net.spy.memcached.ops.OperationCallback;
import net.spy.memcached.ops.OperationState;
import net.spy.memcached.ops.OperationStatus;
import net.spy.memcached.ops.OperationType;
import net.spy.memcached.ops.Operation;

/**
 * Base class for get and gets handlers.
 */
abstract class BaseGetOpImpl extends OperationImpl {

  private static final OperationStatus END = new OperationStatus(true, "END");
  private static final String RN_STRING = "\r\n";
  private final String cmd;
  private final Collection<String> keys;
  private String currentKey = null;
  private long casValue = 0;
  private int currentFlags = 0;
  private byte[] data = null;
  private int readOffset = 0;
  private byte lookingFor = '\0';
  /* ENABLE_MIGRATION if */
  private Operation parentOperation = null;
  private int migratingCount = 0;
  private boolean migrating = false;
  /* ENABLE_MIGRATION end */

  public BaseGetOpImpl(String c,
                       OperationCallback cb, Collection<String> k) {
    super(cb);
    cmd = c;
    keys = k;
    setOperationType(OperationType.READ);
  }

  /* ENABLE_MIGRATION if */
  public BaseGetOpImpl(String c,
                       OperationCallback cb, Collection<String> k, Operation p) {
    super(cb);
    cmd = c;
    keys = k;
    parentOperation = p;
    setOperationType(OperationType.READ);
  }
  /* ENABLE_MIGRATION end */

  /**
   * Get the keys this GetOperation is looking for.
   */
  public final Collection<String> getKeys() {
    return keys;
  }

  @Override
  public final void handleLine(String line) {
    if (line.equals("END")) {
      /* ENABLE_MIGRATION if */
      if (migrating) {
        /**
         * If the migrating flag is set,
         * it is completed when the migrated operation
         * of the child operation completes.
         */
        transitionState(OperationState.MIGRATING);
        migrating = false;
      } else {
        getLogger().debug("Get complete!");
        getCallback().receivedStatus(END);
        transitionState(OperationState.COMPLETE);
        if (parentOperation != null) {
          parentOperation.decrMigratingCount(line);
        }
      }
      /* else */
      /*
      getLogger().debug("Get complete!");
      getCallback().receivedStatus(END);
      transitionState(OperationState.COMPLETE);
      */
      /* ENABLE_MIGRATION end */
      data = null;
    /* ENABLE_MIGRATION if */
    } else if (line.startsWith("NOT_MY_KEY ")) {
      receivedMigrateOperations(line, false);
      migrating = true;
    /* ENABLE_MIGRATION end */
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
    } else {
      assert false : "Unknown line type: " + line;
    }
  }

  @Override
  public final void handleRead(ByteBuffer b) {
    assert currentKey != null;
    assert data != null;
    // This will be the case, because we'll clear them when it's not.
    assert readOffset <= data.length
            : "readOffset is " + readOffset + " data.length is " + data.length;

    getLogger().debug("readOffset: %d, length: %d",
            readOffset, data.length);
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
            assert false : "Looking for unexpected char: "
                    + (char) lookingFor;
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
  public final void initialize() {
    int size;
    StringBuilder commandBuilder = new StringBuilder();
    byte[] commandLine;
    ByteBuffer b;

    String keysString = generateKeysString();

    if (cmd.equals("get") || cmd.equals("gets")) {
      // make command string, for example,
      // "get <keys...>\r\n"
      commandBuilder.append(cmd);
      commandBuilder.append(' ');
      commandBuilder.append(keysString);
      commandBuilder.append(RN_STRING);
    } else {
      assert cmd.equals("mget") : "Unknown Command " + cmd;

      int lenKeys = keysString.getBytes().length;
      int numKeys = keys.size();

      // make command string, for example,
      // "mget <lenKeys> <numkeys>\r\n<keys>\r\n"
      commandBuilder.append(cmd);
      commandBuilder.append(' ');
      commandBuilder.append(String.valueOf(lenKeys));
      commandBuilder.append(' ');
      commandBuilder.append(String.valueOf(numKeys));
      commandBuilder.append(RN_STRING);
      commandBuilder.append(keysString);
      commandBuilder.append(RN_STRING);
    }

    commandLine = commandBuilder.toString().getBytes();
    size = commandLine.length;

    b = ByteBuffer.allocate(size);
    b.put(commandLine);
    b.flip();
    setBuffer(b);
  }

  /* ENABLE_MIGRATION if */
  @Override
  public void setMigratingCount(int count) {
    migratingCount = count;
  }

  @Override
  public void decrMigratingCount(String line) {
    migratingCount -= 1;
    if (migratingCount == 0) {
      getLogger().debug("ParentOp Get complete!");
      getCallback().receivedStatus(END);
      transitionState(OperationState.COMPLETE);
    }
  }
  /* ENABLE_MIGRATION end */

  private String generateKeysString() {
    StringBuilder keyString = new StringBuilder();
    Iterator<String> iterator = keys.iterator();

    // make keys line
    while (true) {
      keyString.append(iterator.next());
      if (iterator.hasNext()) {
        keyString.append(' ');
      } else {
        break;
      }
    }

    return keyString.toString();
  }

  @Override
  protected final void wasCancelled() {
    getCallback().receivedStatus(CANCELLED);
  }

}
