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
package net.spy.memcached.collection;

public abstract class CollectionGet<K> {

	protected boolean delete = false;
	protected boolean dropIfEmpty = true;

	protected String str;
	protected int headerCount;
	protected String subkey;
	protected int dataLength;

	protected byte[] elementFlag;
	
	public boolean isDelete() {
		return delete;
	}

	public void setDelete(boolean delete) {
		this.delete = delete;
	}

	public String getSubkey() {
		return subkey;
	}

	public int getDataLength() {
		return dataLength;
	}
	
	public byte[] getElementFlag() {
		return elementFlag;
	}
	
	public boolean headerReady(int spaceCount) {
		return headerCount == spaceCount;
	}

	public void setHeaderCount(int headerCount) {
		this.headerCount = headerCount;
	}

	public int getHeaderCount() {
		return headerCount;
	}
	
	public boolean eachRecordParseCompleted() {
		return true;
	}
	
	public abstract String stringify();
	public abstract String getCommand();
	public abstract void decodeItemHeader(String itemHeader);

}
