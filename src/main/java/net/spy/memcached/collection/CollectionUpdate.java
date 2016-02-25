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

import net.spy.memcached.collection.ElementFlagFilter.BitWiseOperands;
import net.spy.memcached.util.BTreeUtil;

public abstract class CollectionUpdate<T> {

	protected boolean createKeyIfNotExists = false;
	protected int flags = 0;
	protected T newValue;
	protected boolean noreply = false;

	protected int flagOffset = -1;
	protected BitWiseOperands bitOp;
	protected byte[] elementFlag;

	protected String str;

	public CollectionUpdate() {
	}

	public CollectionUpdate(T newValue, ElementFlagUpdate elementFlagUpdate, boolean noreply) {
		if (elementFlagUpdate == null) {
			this.newValue = newValue;
			this.flagOffset = -1;
			this.bitOp = null;
			this.elementFlag = null;
		} else {
			if (newValue == null && elementFlagUpdate.getElementFlag() == null) {
				throw new IllegalArgumentException(
						"One of the newValue or elementFlag must not be null.");
			}

			if (elementFlagUpdate.getElementFlag().length > ElementFlagFilter.MAX_EFLAG_LENGTH) {
				throw new IllegalArgumentException(
						"length of element flag cannot exceed "
								+ ElementFlagFilter.MAX_EFLAG_LENGTH);
			}

			this.newValue = newValue;
			this.flagOffset = elementFlagUpdate.getElementFlagOffset();
			this.bitOp = elementFlagUpdate.getBitOp();
			this.elementFlag = elementFlagUpdate.getElementFlag();
		}

		this.noreply = noreply;
	}

	public CollectionUpdate(T newValue, boolean noreply) {
		if (newValue == null) {
			throw new IllegalArgumentException(
					"newValue must not be null.");
		}

		this.newValue = newValue;
		this.noreply = noreply;
	}

	public String stringify() {
		if (str != null)
			return str;

		StringBuilder b = new StringBuilder();

		if (flagOffset > -1 && bitOp != null && elementFlag != null) {
			b.append(flagOffset).append(" ").append(bitOp).append(" ");
		}

		if (elementFlag != null) {
			b.append(getElementFlagByHex());
		}

		if (noreply) {
			b.append((b.length() <= 0) ? "" : " ").append("noreply");
		}

		str = b.toString();
		return str;
	}

	public String getElementFlagByHex() {
		if (elementFlag == null) {
			return "";
		}

		if (elementFlag.length == 0) {
			return "0";
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

	public T getNewValue() {
		return newValue;
	}

	public void setNewValue(T newValue) {
		this.newValue = newValue;
	}

	public boolean isNoreply() {
		return noreply;
	}

	public void setNoreply(boolean noreply) {
		this.noreply = noreply;
	}

	public void setElementFlag(byte[] elementFlag) {
		this.elementFlag = elementFlag;
	}

	public String toString() {
		return (str != null) ? str : stringify();
	}

	public abstract String getCommand();

}
