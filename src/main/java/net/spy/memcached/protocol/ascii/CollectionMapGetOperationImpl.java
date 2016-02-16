package net.spy.memcached.protocol.ascii;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Collections;

import net.spy.memcached.KeyUtil;
import net.spy.memcached.collection.MapGet;
import net.spy.memcached.collection.CollectionMapGet;
import net.spy.memcached.collection.CollectionResponse;
import net.spy.memcached.ops.APIType;
import net.spy.memcached.ops.CollectionMapGetOperation;
import net.spy.memcached.ops.CollectionOperationStatus;
import net.spy.memcached.ops.OperationCallback;
import net.spy.memcached.ops.OperationState;
import net.spy.memcached.ops.OperationStatus;
import net.spy.memcached.ops.OperationType;


/**
 * Operation to retrieve collection data in a memcached server.
 */
public class CollectionMapGetOperationImpl extends OperationImpl
        implements CollectionMapGetOperation {

    private final ByteArrayOutputStream byteBuffer = new ByteArrayOutputStream();

    private static final OperationStatus GET_CANCELED = new CollectionOperationStatus(
            false, "collection canceled", CollectionResponse.CANCELED);

    private static final OperationStatus END = new CollectionOperationStatus(
            true, "END", CollectionResponse.END);
    private static final OperationStatus TRIMMED = new CollectionOperationStatus(
            true, "TRIMMED", CollectionResponse.TRIMMED);
    private static final OperationStatus DELETED = new CollectionOperationStatus(
            true, "DELETED", CollectionResponse.DELETED);
    private static final OperationStatus DELETED_DROPPED = new CollectionOperationStatus(
            true, "DELETED_DROPPED", CollectionResponse.DELETED_DROPPED);
    private static final OperationStatus NOT_FOUND = new CollectionOperationStatus(
            false, "NOT_FOUND", CollectionResponse.NOT_FOUND);
    private static final OperationStatus NOT_FOUND_ELEMENT = new CollectionOperationStatus(
            false, "NOT_FOUND_ELEMENT", CollectionResponse.NOT_FOUND_ELEMENT);
    private static final OperationStatus OUT_OF_RANGE = new CollectionOperationStatus(
            false, "OUT_OF_RANGE", CollectionResponse.OUT_OF_RANGE);
    private static final OperationStatus TYPE_MISMATCH = new CollectionOperationStatus(
            false, "TYPE_MISMATCH", CollectionResponse.TYPE_MISMATCH);
    private static final OperationStatus BKEY_MISMATCH = new CollectionOperationStatus(
            false, "BKEY_MISMATCH", CollectionResponse.BKEY_MISMATCH);
    private static final OperationStatus UNREADABLE = new CollectionOperationStatus(
            false, "UNREADABLE", CollectionResponse.UNREADABLE);

    protected final String key;
    protected final CollectionMapGet<?> collectionGet;

    protected int flags = 0;
    protected int count = 0;
    protected byte[] data = null;
    protected int readOffset = 0;
    protected byte lookingFor = '\0';
    protected int spaceCount = 0;

    public CollectionMapGetOperationImpl(String key, CollectionMapGet<?> collectionGet,
            OperationCallback cb) {
        super(cb);
        this.key = key;
        this.collectionGet = collectionGet;
        if (this.collectionGet instanceof MapGet)
            setAPIType(APIType.MOP_GET);
        if (collectionGet.isDelete())
            setOperationType(OperationType.WRITE);
        else
            setOperationType(OperationType.READ);
    }

    /**
     * VALUE <flag> <count>\r\n
     */
    public void handleLine(String line) {
        if (line.startsWith("VALUE ")) {
            // Response header
            getLogger().debug("Got line %s", line);

            String[] stuff = line.split(" ");
            assert "VALUE".equals(stuff[0]);

            flags = Integer.parseInt(stuff[1]);
            count = Integer.parseInt(stuff[2]);

            setReadType(OperationReadType.DATA);
        } else {
            OperationStatus status = matchStatus(line, END, TRIMMED, DELETED,
                    DELETED_DROPPED, NOT_FOUND, NOT_FOUND_ELEMENT,
                    OUT_OF_RANGE, TYPE_MISMATCH, BKEY_MISMATCH, UNREADABLE);
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
            for (int i=0; bb.remaining() > 0; i++) {
                byte b = bb.get();

                // Handle spaces.
                if (b == ' ') {
                    spaceCount++;
                    if (collectionGet.headerReady(spaceCount)) {
                        collectionGet.decodeItemHeader(new String(byteBuffer.toByteArray()));
                        byteBuffer.reset();

                        if (collectionGet.headerReady(spaceCount)
                                && collectionGet.eachRecordParseCompleted()) {
//							if (collectionGet.getElementFlag() != null) {
//								collectionGet.setHeaderCount(collectionGet
//										.getHeaderCount() - 1);
//							}
                            data = new byte[collectionGet.getDataLength()];
                            spaceCount = 0;
                            break;
                        }
                    }
                }

                // Ready to finish.
                if (b == '\r') {
                    continue;
                }

                // Finish the operation.
                if (b == '\n') {
                    OperationStatus status = matchStatus(byteBuffer.toString(),
                            END, DELETED, DELETED_DROPPED, NOT_FOUND,
                            NOT_FOUND_ELEMENT, OUT_OF_RANGE, TYPE_MISMATCH,
                            UNREADABLE);

                    getLogger().debug("Get complete!");
                    getCallback().receivedStatus(status);
                    transitionState(OperationState.COMPLETE);
                    data = null;
                    break;
                }

                byteBuffer.write(b);
            }
            return;
        }

        // Read data
        assert key != null;
        assert data != null;
        // This will be the case, because we'll clear them when it's not.
        assert readOffset <= data.length
                : "readOffset is " + readOffset + " data.length is " + data.length;

        getLogger().debug("readOffset: %d, length: %d", readOffset, data.length);

        if (lookingFor == '\0') {
            int toRead = data.length - readOffset;
            int available = bb.remaining();
            toRead = Math.min(toRead, available);

            getLogger().debug("Reading %d bytes", toRead);

            bb.get(data, readOffset, toRead);
            readOffset += toRead;
        }

        if (lookingFor == '\0' && readOffset == data.length) {
            Callback cb =
                    (Callback) getCallback();
            cb.gotData(key, collectionGet.getSubkey(), flags, data);
            lookingFor = '\r';
        }

        if (lookingFor != '\0' && bb.hasRemaining()) {
            do {
                byte tmp = bb.get();
                assert tmp == lookingFor : "Expecting " + lookingFor + ", got "
                        + (char)tmp;

                switch (lookingFor) {
                    case '\r': lookingFor = '\n'; break;
                    case '\n': lookingFor = '\0'; break;
                    default:
                        assert false : "Looking for unexpected char: "
                                + (char)lookingFor;
                }
            } while (lookingFor != '\0' && bb.hasRemaining());

            if (lookingFor == '\0') {
                data = null;
                readOffset = 0;
            }
        }
    }

    public void initialize() {
        String cmd = collectionGet.getCommand();
        String args = collectionGet.stringify();
        ByteBuffer bb = ByteBuffer.allocate(KeyUtil.getKeyBytes(key).length
                + cmd.length() + args.length() + 16);

        setArguments(bb, cmd, key, args);
        bb.flip();
        setBuffer(bb);

        if (getLogger().isDebugEnabled()) {
            getLogger().debug("Request in ascii protocol: "
                    + (new String(bb.array())).replace("\r\n", "\\r\\n"));
        }
    }

    @Override
    protected void wasCancelled() {
        getCallback().receivedStatus(GET_CANCELED);
    }

    public Collection<String> getKeys() {
        return Collections.singleton(key);
    }

    public CollectionMapGet<?> getGet() {
        return collectionGet;
    }

}

