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
// Copyright (c) 2006  Dustin Sallings <dustin@spy.net>

package net.spy.memcached.protocol.ascii;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

import net.spy.memcached.KeyUtil;
import net.spy.memcached.ops.Operation;
import net.spy.memcached.ops.OperationCallback;
import net.spy.memcached.ops.OperationErrorType;
import net.spy.memcached.ops.OperationState;
import net.spy.memcached.ops.OperationStatus;
import net.spy.memcached.protocol.BaseOperationImpl;

/**
 * Operations on a memcached connection.
 */
abstract class OperationImpl extends BaseOperationImpl implements Operation {

	protected static final byte[] CRLF={'\r', '\n'};
	private static final String CHARSET = "UTF-8";

	private final ByteArrayOutputStream byteBuffer=new ByteArrayOutputStream();
	OperationReadType readType=OperationReadType.LINE;
	boolean foundCr=false;

	protected OperationImpl() {
		super();
	}

	protected OperationImpl(OperationCallback cb) {
		super();
		callback=cb;
	}

	/**
	 * Match the status line provided against one of the given
	 * OperationStatus objects.  If none match, return a failure status with
	 * the given line.
	 *
	 * @param line the current line
	 * @param statii several status objects
	 * @return the appropriate status object
	 */
	protected final OperationStatus matchStatus(String line,
			OperationStatus... statii) {
		OperationStatus rv=null;
		for(OperationStatus status : statii) {
			if(line.equals(status.getMessage())) {
				rv=status;
			}
		}
		if(rv == null) {
			rv=new OperationStatus(false, line);
			/* ENABLE_REPLICATION if */
			getLogger().error("Unexpected operation status : %s", line);
			/* ENABLE_REPLICATION end */
		}
		return rv;
	}

	/* (non-Javadoc)
	 * @see net.spy.memcached.protocol.ascii.Operation#getReadType()
	 */
	protected final OperationReadType getReadType() {
		return readType;
	}

	/**
	 * Set the read type of this operation.
	 */
	protected final void setReadType(OperationReadType to) {
		readType=to;
	}

	/**
	 * Set some arguments for an operation into the given byte buffer.
	 */
	protected final void setArguments(ByteBuffer bb, Object... args) {
		boolean wasFirst=true;
		for(Object o : args) {
			String s = String.valueOf(o);
			if(wasFirst) {
				wasFirst=false;
			} else if (!"".equals(s)) {
				bb.put((byte)' ');
			}
			bb.put(KeyUtil.getKeyBytes(s));
		}
		bb.put(CRLF);
	}

	OperationErrorType classifyError(String line) {
		OperationErrorType rv=null;
		if(line.startsWith("ERROR")) {
			rv=OperationErrorType.GENERAL;
		} else if(line.startsWith("CLIENT_ERROR")) {
			rv=OperationErrorType.CLIENT;
		} else if(line.startsWith("SERVER_ERROR")) {
			rv=OperationErrorType.SERVER;
		}
		return rv;
	}

	@Override
	public void readFromBuffer(ByteBuffer data) throws IOException {
		// Loop while there's data remaining to get it all drained.
		while(getState() != OperationState.COMPLETE && data.remaining() > 0) {
			if(readType == OperationReadType.DATA) {
				handleRead(data);
			} else {
				int offset=-1;
				for(int i=0; data.remaining() > 0; i++) {
					byte b=data.get();
					if(b == '\r') {
						foundCr=true;
					} else if(b == '\n') {
						assert foundCr: "got a \\n without a \\r";
						offset=i;
						foundCr=false;
						break;
					} else {
						assert !foundCr : "got a \\r without a \\n";
						byteBuffer.write(b);
					}
				}
				if(offset >= 0) {
					String line=new String(byteBuffer.toByteArray(), CHARSET);
					byteBuffer.reset();
					OperationErrorType eType=classifyError(line);
					if(eType != null) {
						handleError(eType, line);
					} else {
						handleLine(line);
					}
				}
			}
			/* ENABLE_REPLICATION if */
			if (getState() == OperationState.MOVING) {
				break;
			}
			/* ENABLE_REPLICATION end */
		}
	}

	/* (non-Javadoc)
	 * @see net.spy.memcached.protocol.ascii.Operation#handleLine(java.lang.String)
	 */
	public abstract void handleLine(String line);
}
