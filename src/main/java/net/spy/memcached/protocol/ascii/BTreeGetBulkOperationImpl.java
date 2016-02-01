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
import net.spy.memcached.collection.BTreeGetBulk;
import net.spy.memcached.collection.CollectionResponse;
import net.spy.memcached.ops.APIType;
import net.spy.memcached.ops.BTreeGetBulkOperation;
import net.spy.memcached.ops.CollectionOperationStatus;
import net.spy.memcached.ops.OperationCallback;
import net.spy.memcached.ops.OperationState;
import net.spy.memcached.ops.OperationStatus;
import net.spy.memcached.ops.OperationType;

/**
 * Operation to retrieve b+tree data with multiple keys
 */
public class BTreeGetBulkOperationImpl extends OperationImpl implements
		BTreeGetBulkOperation {

	private final ByteArrayOutputStream byteBuffer = new ByteArrayOutputStream();

	private static final OperationStatus GET_CANCELED = new CollectionOperationStatus(
			false, "collection canceled", CollectionResponse.CANCELED);

	private static final OperationStatus END = new CollectionOperationStatus(
			true, "END", CollectionResponse.END);

	private static final OperationStatus OK = new CollectionOperationStatus(
			true, "OK", CollectionResponse.OK);
	private static final OperationStatus TRIMMED = new CollectionOperationStatus(
			true, "TRIMMED", CollectionResponse.TRIMMED);

	private static final OperationStatus NOT_FOUND = new CollectionOperationStatus(
			false, "NOT_FOUND", CollectionResponse.NOT_FOUND);
	private static final OperationStatus NOT_FOUND_ELEMENT = new CollectionOperationStatus(
			false, "NOT_FOUND_ELEMENT", CollectionResponse.NOT_FOUND_ELEMENT);
	private static final OperationStatus OUT_OF_RANGE = new CollectionOperationStatus(
			false, "OUT_OF_RANGE", CollectionResponse.OUT_OF_RANGE);
	private static final OperationStatus TYPE_MISMATCH = new CollectionOperationStatus(
			false, "TYPE_MISMATCH", CollectionResponse.TYPE_MISMATCH);
	private static final OperationStatus BKEY_MISMATCH = new CollectionOperationStatus(
			false, "BKEY_MISMATCH", CollectionResponse.TYPE_MISMATCH);
	private static final OperationStatus UNREADABLE = new CollectionOperationStatus(
			false, "UNREADABLE", CollectionResponse.UNREADABLE);

	protected final BTreeGetBulk<?> getBulk;

	protected int flags = 0;
	protected byte[] data = null;
	protected int readOffset = 0;
	protected byte lookingFor = '\0';
	protected int spaceCount = 0;

	public BTreeGetBulkOperationImpl(BTreeGetBulk<?> getBulk, OperationCallback cb) {
		super(cb);
		this.getBulk = getBulk;
		setAPIType(APIType.BOP_GET);
		setOperationType(OperationType.READ);
	}

	public void handleLine(String line) {
		getLogger().debug("Got line %s", line);

		if (line.startsWith("VALUE ")) {
			readKey(line);
			setReadType(OperationReadType.DATA);
		} else {
			OperationStatus status = matchStatus(line, END);

			getLogger().debug(status);
			getCallback().receivedStatus(status);

			transitionState(OperationState.COMPLETE);
			return;
		}
	}

	@Override
	public final void handleRead(ByteBuffer bb) {
		readValue(bb);
	}

	private final void readKey(String line) {
		// protocol : VALUE key OK flag count
		String[] chunk = line.split(" ");

		OperationStatus status = matchStatus(chunk[2], OK, TRIMMED, NOT_FOUND,
				NOT_FOUND_ELEMENT, OUT_OF_RANGE, TYPE_MISMATCH, BKEY_MISMATCH,
				UNREADABLE);

		getBulk.decodeKeyHeader(line);

		BTreeGetBulkOperation.Callback<?> cb = ((BTreeGetBulkOperation.Callback<?>) getCallback());
		cb.gotKey(chunk[1], (chunk.length > 3) ? Integer.valueOf(chunk[4]) : -1, status);
	}

	private final void readValue(ByteBuffer bb) {
		// protocol : ELEMENT bkey [eflag] len value
		if (lookingFor == '\0' && data == null) {
			for (int i = 0; bb.remaining() > 0; i++) {
				byte b = bb.get();

				// Handle spaces.
				if (b == ' ') {
					spaceCount++;

					String l = new String(byteBuffer.toByteArray());

					if (l.startsWith("ELEMENT")) {
						if (getBulk.elementHeaderReady(spaceCount)) {
							if (spaceCount == 3 && l.split(" ")[2].startsWith("0x")) {
								byteBuffer.write(b);
								continue;
							}

							getBulk.decodeItemHeader(l);
							data = new byte[getBulk.getDataLength()];
							byteBuffer.reset();
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
					String line = byteBuffer.toString();

					if (line.startsWith("VALUE")) {
						readKey(line);
						byteBuffer.reset();
						spaceCount = 0;
						continue;
					} else {
						OperationStatus status = matchStatus(line, END);
						getCallback().receivedStatus(status);
						transitionState(OperationState.COMPLETE);
						data = null;
						break;
					}
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
			BTreeGetBulkOperation.Callback cb = (BTreeGetBulkOperation.Callback) getCallback();
			cb.gotElement(getBulk.getKey(), getBulk.getSubkey(), getBulk.getFlag(), getBulk.getEFlag(), data);
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

	public void initialize() {
		String cmd = getBulk.getCommand();
		String args = getBulk.stringify();

		ByteBuffer bb = ByteBuffer.allocate(cmd.length() + args.length()
				+ KeyUtil.getKeyBytes(getBulk.getCommaSeparatedKeys()).length + 16);

		setArguments(bb, cmd, args);

		setArguments(bb, getBulk.getCommaSeparatedKeys());

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
		return getBulk.getKeyList();
	}
}
