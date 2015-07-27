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
/**
 *
 */
package net.spy.memcached;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.util.Collection;

import net.spy.memcached.ops.Operation;

public class MockMemcachedNode implements MemcachedNode {
	private final InetSocketAddress socketAddress;
	public SocketAddress getSocketAddress() {return socketAddress;}

	public MockMemcachedNode(InetSocketAddress socketAddress) {
		this.socketAddress = socketAddress;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}

		MockMemcachedNode that = (MockMemcachedNode) o;

		if (socketAddress != null
				? !socketAddress.equals(that.socketAddress)
				: that.socketAddress != null) {
			return false;
		}

		return true;
	}

	@Override
	public int hashCode() {
		return (socketAddress != null ? socketAddress.hashCode() : 0);
	}

	public void copyInputQueue() {
		// noop
	}
	public void setupResend(boolean cancelWrite) {
		// noop
	}
	public void fillWriteBuffer(boolean optimizeGets) {
		// noop
	}
	public void transitionWriteItem() {
		// noop
	}
	public Operation getCurrentReadOp() {return null;}
	public Operation removeCurrentReadOp() {return null;}
	public Operation getCurrentWriteOp() {return null;}
	public Operation removeCurrentWriteOp() {return null;}
	public boolean hasReadOp() {return false;}
	public boolean hasWriteOp() {return false;}
	public void addOp(Operation op) {
		// noop
	}
	public void insertOp(Operation op) {
		// noop
	}
	public int getSelectionOps() {return 0;}
	public ByteBuffer getRbuf() {return null;}
	public ByteBuffer getWbuf() {return null;}
	public boolean isActive() {return false;}
	public void reconnecting() {
		// noop
	}
	public void connected() {
		// noop
	}
	public int getReconnectCount() {return 0;}
	public void registerChannel(SocketChannel ch, SelectionKey selectionKey) {
		// noop
	}
	public void setChannel(SocketChannel to) {
		// noop
	}
	public SocketChannel getChannel() {return null;}
	public void setSk(SelectionKey to) {
		// noop
	}
	public SelectionKey getSk() {return null;}
	public int getBytesRemainingToWrite() {return 0;}
	public int writeSome() throws IOException {return 0;}
	public void fixupOps() {
		// noop
	}

	public Collection<Operation> destroyInputQueue() {
		return null;
	}

	public void authComplete() {
		// noop
	}

	public void setupForAuth() {
		// noop
	}

	public int getContinuousTimeout() {
		return 0;
	}

	public void setContinuousTimeout(boolean timedOut) {
		// noop
	}
	
	public void enableTimeoutRatio() {
		// noop
	}

	public int getTimeoutRatioNow() {
		return -1; // disabled
	}

	public boolean isFake() {
		return false;
	}

	public void shutdown() throws IOException {
		// noop
	}
	
	@Override
	public String getStatus() {
		return "MOCK_STATE";
	}
}
