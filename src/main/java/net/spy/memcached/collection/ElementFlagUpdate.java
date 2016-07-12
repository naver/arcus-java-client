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

/**
 * Element flag
 */
public class ElementFlagUpdate {

	public static final ElementFlagUpdate RESET_FLAG = new ElementFlagUpdate();

	private final int elementFlagOffset;
	private final BitWiseOperands bitOp;
	private final byte[] elementFlag;

	private ElementFlagUpdate() {
		this.elementFlag = new byte[] {};
		this.elementFlagOffset = -1;
		this.bitOp = null;
	}

	/**
	 * create element flag update
	 * 
	 * @param elementFlag
	 *            new element flag
	 */
	public ElementFlagUpdate(byte[] elementFlag) {
		if (elementFlag == null) {
			throw new IllegalArgumentException("element flag may not null.");
		}
		if (elementFlag.length < 1 || elementFlag.length > ElementFlagFilter.MAX_EFLAG_LENGTH) {
			throw new IllegalArgumentException("length of element flag must be between 1 and "
					+ ElementFlagFilter.MAX_EFLAG_LENGTH + ".");
		}
		this.elementFlag = elementFlag;
		this.elementFlagOffset = -1;
		this.bitOp = null;
	}

	/**
	 * create element flag update
	 * 
	 * @param elementFlagOffset
	 *            bitwise update offset
	 * @param bitOp
	 *            bitwise operand
	 * @param elementFlag
	 *            element flag to bitwise operation
	 */
	public ElementFlagUpdate(int elementFlagOffset, BitWiseOperands bitOp,
			byte[] elementFlag) {
		if (elementFlagOffset < 0) {
			throw new IllegalArgumentException(
					"elementFlagOffset must be larger than 0.");
		}
		if (bitOp == null) {
			throw new IllegalArgumentException("bitOp may not null.");
		}
		if (elementFlag == null) {
			throw new IllegalArgumentException("element flag may not null.");
		}
		if (elementFlag.length < 1 || elementFlag.length > ElementFlagFilter.MAX_EFLAG_LENGTH) {
			throw new IllegalArgumentException("length of element flag must be between 1 and "
					+ ElementFlagFilter.MAX_EFLAG_LENGTH + ".");
		}
		this.elementFlagOffset = elementFlagOffset;
		this.bitOp = bitOp;
		this.elementFlag = elementFlag;
	}

	public int getElementFlagOffset() {
		return elementFlagOffset;
	}

	public BitWiseOperands getBitOp() {
		return bitOp;
	}

	public byte[] getElementFlag() {
		return elementFlag;
	}

	/**
	 * get value of element flag by hex.
	 *
	 * @return element flag by hex (e.g. 0x01)
	 */
	public String getElementFlagByHex() {
		if (elementFlag.length == 0) {
			return "0"; // special meaning: remove eflag
		}
		// convert to hex based on its real byte array
		return BTreeUtil.toHex(elementFlag);
	}

}
