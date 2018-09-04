package net.spy.memcached.protocol.ascii;

import net.spy.memcached.KeyUtil;
import net.spy.memcached.collection.CollectionGet;
import net.spy.memcached.collection.CollectionResponse;
import net.spy.memcached.collection.RangeGet;
import net.spy.memcached.ops.*;


import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Collections;

public class RangeGetOperationImpl extends OperationImpl
        implements RangeGetOperation {
    private final ByteArrayOutputStream byteBuffer = new ByteArrayOutputStream();

    private static final OperationStatus GET_CANCELED = new CollectionOperationStatus(
            false, "rangeget canceled", CollectionResponse.CANCELED);

    private static final OperationStatus END = new CollectionOperationStatus(
            true, "END", CollectionResponse.END);
    private static final OperationStatus UNREADABLE = new CollectionOperationStatus(
            false, "UNREADABLE", CollectionResponse.UNREADABLE);

    protected final RangeGet rangeGet;

    protected int count = 0;
    protected byte lookingFor = '\0';
    protected byte[] data = null;
    protected int spaceCount = 0;

    public RangeGetOperationImpl(RangeGet rangeGet,
                                 OperationCallback cb) {
        super(cb);
        this.rangeGet = rangeGet;
        setAPIType(APIType.RNG_GET);
        setOperationType(OperationType.READ);
    }

    /**
     * VALUE <count>\r\n
     * <key1>\r\n
     * ...
     * <keyn>\r\n
     */
    public void handleLine(String line) {
        /* ENABLE_REPLICATION */
        if (line.equals("SWITCHOVER") || line.equals("REPL_SLAVE")) {
            receivedMoveOperations(line);
            return;
        }

        /* ENABLE_REPLICATION end */
        if (line.startsWith("VALUE ")) {
            // Response header
            getLogger().debug("Got line %s", line);

            String[] stuff = line.split(" ");
            assert "VALUE".equals(stuff[0]);

            count = Integer.parseInt(stuff[1]);

            setReadType(OperationReadType.DATA);
        } else {
            OperationStatus status = matchStatus(line, END, UNREADABLE);
            getLogger().debug(status);
            getCallback().receivedStatus(status);
            transitionState(OperationState.COMPLETE);
            return;
        }
    }
    @Override
    public final void handleRead(ByteBuffer bb) {
        // Decode a collection data header.
        if (lookingFor == '\0' && data == null) {
            for (int i = 0; bb.remaining() > 0; i++) {
                byte b = bb.get();

                // Ready to write.
                if (b == '\r') {
                    continue;
                }

                // Finish the operation.
                if (b == '\n') {
                    if (count-- <= 0) {
                        OperationStatus status = matchStatus(byteBuffer.toString(),
                                END, UNREADABLE);
                        getLogger().debug("Get complete!");
                        getCallback().receivedStatus(status);
                        transitionState(OperationState.COMPLETE);
                        return;
                    }
                    data = byteBuffer.toByteArray();
                    RangeGetOperation.Callback cb =
                            (RangeGetOperation.Callback) getCallback();
                    cb.gotData(data);
                    data = null;
                    byteBuffer.reset();
                    continue;
                }
                byteBuffer.write(b);
            }

        }
    }

    public void initialize() {
        String cmd = rangeGet.getCommand();
        String args = rangeGet.stringify();

        ByteBuffer bb = ByteBuffer.allocate(
                KeyUtil.getKeyBytes(rangeGet.getRange()).length + cmd.length() + 10);
        setArguments(bb, cmd, args);

        bb.flip();
        setBuffer(bb);

        if (getLogger().isDebugEnabled()) {
            getLogger().debug("Request in ascii protocol: "
                    + (new String(bb.array())).replace("\r\n","\\r\\n"));
        }
    }

    @Override
    protected void wasCancelled() { getCallback().receivedStatus(GET_CANCELED); }
    public Collection<String> getKeys() { return null; }
    public CollectionGet getGet() { return this.rangeGet; }

}
