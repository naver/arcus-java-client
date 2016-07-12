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

import net.spy.memcached.util.BTreeUtil;

public abstract class CollectionStore<T> {

	protected boolean createKeyIfNotExists = false;
	protected int flags = 0;
	protected T value;
	protected RequestMode requestMode;

	protected CollectionAttributes attribute;
	
	protected byte[] elementFlag;
	
	protected String str;

	public CollectionStore() { }

	public CollectionStore(T value, byte[] elementFlag, boolean createKeyIfNotExists, RequestMode requestMode, CollectionAttributes attr) {
		if (attr != null) {
			CollectionOverflowAction overflowAction = attr.getOverflowAction();
			if ((overflowAction == CollectionOverflowAction.head_trim ||
				overflowAction == CollectionOverflowAction.tail_trim) && !(this instanceof ListStore)) {
				throw new IllegalArgumentException(overflowAction + " is available overflow action in only lop.");
			} else if ((overflowAction == CollectionOverflowAction.smallest_trim ||
				overflowAction == CollectionOverflowAction.smallest_silent_trim ||
				overflowAction == CollectionOverflowAction.largest_trim ||
				overflowAction == CollectionOverflowAction.largest_silent_trim) &&
				(!(this instanceof BTreeStore) && !(this instanceof BTreeUpsert) && !(this instanceof  BTreeStoreAndGet))) {
				throw new IllegalArgumentException(overflowAction + " is available overflow action in only bop.");
			}
		}
		if (elementFlag != null && elementFlag.length > ElementFlagFilter.MAX_EFLAG_LENGTH) {
			throw new IllegalArgumentException("Length of elementFlag must be less than " + ElementFlagFilter.MAX_EFLAG_LENGTH);
		}

		this.value = value;
		this.elementFlag = elementFlag;
		this.createKeyIfNotExists = createKeyIfNotExists;
		this.requestMode = requestMode;
		this.attribute = attr;
	}

	public String stringify() {
		if (str != null) return str;
		
		StringBuilder b = new StringBuilder();

		if (createKeyIfNotExists) {
			b.append("create ").append(flags);
			if (attribute != null) {
				b.append(" ")
						.append((attribute.getExpireTime() == null) ? CollectionAttributes.DEFAULT_EXPIRETIME
								: attribute.getExpireTime());
				b.append(" ")
						.append((attribute.getMaxCount() == null) ? CollectionAttributes.DEFAULT_MAXCOUNT
								: attribute.getMaxCount());

				if (null != attribute.getOverflowAction()) {
					b.append(" ").append(attribute.getOverflowAction());
				}
				
				if (null != attribute.getReadable() && !attribute.getReadable()) {
					b.append(" ").append("unreadable");
				}
			}
			else {
				b.append(" ").append(CollectionAttributes.DEFAULT_EXPIRETIME);
				b.append(" ").append(CollectionAttributes.DEFAULT_MAXCOUNT);
			}
		}
		
		// an optional request mode like noreply, pipe and getrim 
		if (requestMode != null) {
			b.append((b.length() <= 0)? "" : " ").append(requestMode.getAscii());
		}

		str = b.toString();
		return str;
	}
	
	public byte[] getElementFlag() {
		return elementFlag;
	}
	
	public String getElementFlagByHex() {
		if (elementFlag == null) {
			return "";
		}
		return BTreeUtil.toHex(elementFlag);
	}
	
	public boolean iscreateKeyIfNotExists() {
		return createKeyIfNotExists;
	}

	public void setcreateKeyIfNotExists(boolean createKeyIfNotExists) {
		this.createKeyIfNotExists = createKeyIfNotExists;
	}

	public int getFlags() {
		return flags;
	}

	public void setFlags(int flags) {
		this.flags = flags;
	}

	public T getValue() {
		return value;
	}

	public void setValue(T value) {
		this.value = value;
	}
	
	public RequestMode getRequestMode() {
		return requestMode;
	}

	public void setRequestMode(RequestMode requestMode) {
		this.requestMode = requestMode;
	}

	public void setElementFlag(byte[] elementFlag) {
		this.elementFlag = elementFlag;
	}
	
	public void setCollectionAttributes(CollectionAttributes attributes) {
		this.attribute = attributes;
	}
	
	public String toString() {
		return (str != null)? str : stringify();
	}
	
	public abstract String getCommand();
	
}
