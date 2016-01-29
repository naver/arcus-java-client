/*
 * arcus-java-client : Arcus Java client
 * Copyright 2010-2014 NAVER Corp.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package net.spy.memcached.protocol.ascii;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.util.Collection;

import net.spy.memcached.KeyUtil;
import net.spy.memcached.collection.BTreeSMGet;
import net.spy.memcached.collection.CollectionResponse;
import net.spy.memcached.ops.APIType;
import net.spy.memcached.ops.BTreeSortMergeGetOperationOld;
import net.spy.memcached.ops.CollectionOperationStatus;
import net.spy.memcached.ops.OperationCallback;
import net.spy.memcached.ops.OperationState;
import net.spy.memcached.ops.OperationStatus;
import net.spy.memcached.ops.OperationType;

/**
 * Operation to retrieve b+tree data with multiple keys
 */
public class BTreeSortMergeGetOperationOldImpl extends OperationImpl implements
		BTreeSortMergeGetOperationOld {

	private final ByteArrayOutputStream byteBuffer = new ByteArrayOutputStream();

	private static final OperationStatus GET_CANCELED = new CollectionOperationStatus(
			false, "collection canceled", CollectionResponse.CANCELED);

	private static final OperationStatus END = new CollectionOperationStatus(
			true, "END", CollectionResponse.END);
	private static final OperationStatus DUPLICATED = new CollectionOperationStatus(
			true, "DUPLICATED", CollectionResponse.DUPLICATED);
	private static final OperationStatus TRIMMED = new CollectionOperationStatus(
			true, "TRIMMED", CollectionResponse.TRIMMED);
	private static final OperationStatus DUPLICATED_TRIMMED = new CollectionOperationStatus(
			true, "DUPLICATED_TRIMMED", CollectionResponse.DUPLICATED_TRIMMED);
	private static final OperationStatus OUT_OF_RANGE = new CollectionOperationStatus(
			false, "OUT_OF_RANGE", CollectionResponse.OUT_OF_RANGE);
	private static final OperationStatus ATTR_MISMATCH = new CollectionOperationStatus(
			false, "ATTR_MISMATCH", CollectionResponse.ATTR_MISMATCH);
	private static final OperationStatus TYPE_MISMATCH = new CollectionOperationStatus(
			false, "TYPE_MISMATCH", CollectionResponse.TYPE_MISMATCH);
	private static final OperationStatus BKEY_MISMATCH = new CollectionOperationStatus(
			false, "BKEY_MISMATCH", CollectionResponse.BKEY_MISMATCH);

	protected final BTreeSMGet<?> smGet;

	protected int flags = 0;
	protected int count = 0;
	protected byte[] data = null;
	protected int readOffset = 0;
	protected byte lookingFor = '\0';
	protected int spaceCount = 0;

	protected int readState = 0; // 0 : value, 1 : missed keys
	private int processedValueCount = 0;

	public BTreeSortMergeGetOperationOldImpl(BTreeSMGet<?> smGet,
			OperationCallback cb) {
		super(cb);
		this.smGet = smGet;
		setAPIType(APIType.BOP_SMGET);
		setOperationType(OperationType.READ);
	}

	/**
	 * VALUE <flag> <count>\r\n
	 */
	public void handleLine(String line) {
		// Response header
		getLogger().debug("Got line %s", line);

		if (line.startsWith("VALUE ")) {
			readState = 0;

			String[] stuff = line.split(" ");
			assert "VALUE".equals(stuff[0]);

			count = Integer.parseInt(stuff[1]);

			if (count > 0) {
				setReadType(OperationReadType.DATA);
			}
		} else if (line.startsWith("MISSED_KEYS")) {
			readState = 1;

			String[] stuff = line.split(" ");
			assert "MISSED_KEYS".equals(stuff[0]);

			count = Integer.parseInt(stuff[1]);

			if (count > 0) {
				setReadType(OperationReadType.DATA);
			}
		} else {
			OperationStatus status = matchStatus(line, END, TRIMMED,
					DUPLICATED, DUPLICATED_TRIMMED, OUT_OF_RANGE,
					ATTR_MISMATCH, TYPE_MISMATCH, BKEY_MISMATCH);
			getLogger().debug(status);
			getCallback().receivedStatus(status);
			transitionState(OperationState.COMPLETE);
			return;
		}
	}

	@Override
	public final void handleRead(ByteBuffer bb) {
		if (readState == 0) {
			readValue(bb);
		} else {
			readMissedKeys(bb);
		}
	}

	private final void readValue(ByteBuffer bb) {
		// Decode a collection data header.
		if (lookingFor == '\0' && data == null) {
			for (int i = 0; bb.remaining() > 0; i++) {
				byte b = bb.get();

				// Handle spaces.
				if (b == ' ') {

					// Adjust space count if item header has a element flag.
					String[] chunk = new String(byteBuffer.toByteArray())
							.split(" ");
					if (chunk.length == smGet.headerCount) {
						if (chunk[3].startsWith("0x")) {
							spaceCount--;
						}
					}

					spaceCount++;
					if (smGet.headerReady(spaceCount)) {
						smGet.decodeItemHeader(new String(byteBuffer
								.toByteArray()));
						data = new byte[smGet.getDataLength()];
						byteBuffer.reset();
						spaceCount = 0;
						processedValueCount++;
						break;
					}
				}

				// Ready to finish.
				if (b == '\r') {
					continue;
				}

				// Finish the operation.
				if (b == '\n') {

					if ((new String(byteBuffer.toByteArray()))
							.startsWith("MISSED_KEYS")) {
						readState = 1;
						byteBuffer.reset();
						spaceCount = 0;
						return;
					}

					OperationStatus status = matchStatus(byteBuffer.toString(),
							END, TRIMMED, DUPLICATED, DUPLICATED_TRIMMED,
							OUT_OF_RANGE, ATTR_MISMATCH, TYPE_MISMATCH,
							BKEY_MISMATCH);

					getCallback().receivedStatus(status);
					//transitionState(OperationState.COMPLETE);
					data = null;
					break;
				}

				byteBuffer.write(b);
			}
			return;
		}

		// Read data
		// assert key != null;
		assert data != null;

		// This will be the case, because we'll clear them when it's not.
		assert readOffset <= data.length : "readOffset is " + readOffset
				+ " data.length is " + data.length;

		getLogger()
				.debug("readOffset: %d, length: %d", readOffset, data.length);

		if (lookingFor == '\0') {
			int toRead = data.length - readOffset;
			int available = bb.remaining();
			toRead = Math.min(toRead, available);

			getLogger().debug("Reading %d bytes", toRead);

			bb.get(data, readOffset, toRead);
			readOffset += toRead;
		}

		if (lookingFor == '\0' && readOffset == data.length) {
			BTreeSortMergeGetOperationOld.Callback cb = (BTreeSortMergeGetOperationOld.Callback) getCallback();
			cb.gotData(smGet.getKey(), smGet.getSubkey(), smGet.getFlag(), data);
			lookingFor = '\r';
		}

		if (lookingFor != '\0' && bb.hasRemaining()) {
			do {
				byte tmp = bb.get();
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
			} while (lookingFor != '\0' && bb.hasRemaining());

			if (lookingFor == '\0') {
				data = null;
				readOffset = 0;
			}
		}
	}

	private final void readMissedKeys(ByteBuffer bb) {
		if (lookingFor == '\0' && data == null) {
			for (int i = 0; bb.remaining() > 0; i++) {
				byte b = bb.get();

				// Ready to finish.
				if (b == '\r') {
					continue;
				}

				// Finish the operation.
				if (b == '\n') {
					OperationStatus status = matchStatus(byteBuffer.toString(),
							END, TRIMMED, DUPLICATED, DUPLICATED_TRIMMED,
							OUT_OF_RANGE, ATTR_MISMATCH, TYPE_MISMATCH,
							BKEY_MISMATCH);

					if (status.isSuccess()) {
						getCallback().receivedStatus(status);
						transitionState(OperationState.COMPLETE);
						return;
					} else {
						((BTreeSortMergeGetOperationOld.Callback) getCallback())
								.gotMissedKey(byteBuffer.toByteArray());
					}
					byteBuffer.reset();
				} else
					byteBuffer.write(b);
			}
			return;
		}
	}

	public void initialize() {
		String cmd = smGet.getCommand();
		String args = smGet.stringify();

		ByteBuffer bb = ByteBuffer.allocate(cmd.length() + args.length()
				+ KeyUtil.getKeyBytes(smGet.getCommaSeparatedKeys()).length + 16);

		setArguments(bb, cmd, args);

		setArguments(bb, smGet.getCommaSeparatedKeys());

		bb.flip();
		setBuffer(bb);

		if (getLogger().isDebugEnabled()) {
			getLogger().debug(
					"Request in ascii protocol: "
							+ (new String(bb.array()))
									.replace("\r\n", "\\r\\n"));
		}
	}

	@Override
	protected void wasCancelled() {
		getCallback().receivedStatus(GET_CANCELED);
	}

	public Collection<String> getKeys() {
		return smGet.getKeyList();
	}
}