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

/**
 * element flag filter
 */
public class ElementFlagFilter {

	/**
	 * Do not filter.
	 */
	public static final ElementFlagFilter DO_NOT_FILTER = null;

	/**
	 * Empty element flag.
	 */
	public static final byte[] EMPTY_ELEMENT_FLAG = null;

	/**
	 * Max element flag length.
	 */
	public static final int MAX_EFLAG_LENGTH = 31;

	// compare offset
	protected int fwhere = 0;

	// bitwise comparison (optional)
	protected BitWiseOperands bitOp = null;
	protected byte[] bitCompValue = null;

	// comparison
	protected CompOperands compOp;
	protected byte[] compValue;

	public ElementFlagFilter() {
	}
	
	/**
	 * create element flag filter
	 * 
	 * @param compOperand
	 *            comparison operand
	 * @param compValue
	 *            comparison value
	 */
	public ElementFlagFilter(CompOperands compOperand, byte[] compValue) {
		if (compOperand == null || compValue == null) {
			throw new NullPointerException("Invalid compOperand and compValue.");
		}

		if (compValue.length == 0) {
			throw new IllegalArgumentException(
					"Length of comparison value must be larger than 0.");
		}

		if (compValue.length > MAX_EFLAG_LENGTH) {
			throw new IllegalArgumentException(
					"Length of comparison value must be less than " + MAX_EFLAG_LENGTH);
		}

		this.compOp = compOperand;
		this.compValue = compValue;
	}

	/**
	 * set bitwise compare
	 * 
	 * @param bitOp
	 *            bitwise operand
	 * @param bitCompValue
	 *            bitwise comparison value
	 * @return element flag filter
	 */
	public ElementFlagFilter setBitOperand(BitWiseOperands bitOp,
			byte[] bitCompValue) {
		if (bitOp == null || bitCompValue == null) {
			throw new NullPointerException("Invalid compOperand and compValue.");
		}

		if (bitCompValue.length == 0) {
			throw new IllegalArgumentException(
					"Length of bit comparison value must be larger than 0.");
		}

		if (bitCompValue.length > MAX_EFLAG_LENGTH) {
			throw new IllegalArgumentException(
					"Length of bit comparison value must be less than " + MAX_EFLAG_LENGTH);
		}

		this.bitOp = bitOp;
		this.bitCompValue = bitCompValue;
		return this;
	}

	/**
	 * set bitwise compare offset
	 * 
	 * @param offset
	 *            0-base offset. this value must less than length of exists
	 *            element flag.
	 * @return element flag filter
	 */
	public ElementFlagFilter setCompareOffset(int offset) {
		this.fwhere = offset;
		return this;
	}

	protected boolean isBitWiseOpEnabled() {
		return bitOp != null && bitCompValue != null;
	}

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();

		sb.append(fwhere).append(" ");

		if (isBitWiseOpEnabled()) {
			sb.append(bitOp).append(" ");
			sb.append(BTreeUtil.toHex(bitCompValue)).append(" ");
		}

		sb.append(compOp).append(" ");
		sb.append(BTreeUtil.toHex(compValue));

		return sb.toString();
	}

	/**
	 * Comparison Operands
	 */
	public enum CompOperands {
		Equal("EQ"), NotEqual("NE"), LessThan("LT"), LessOrEqual("LE"), GreaterThan(
				"GT"), GreaterOrEqual("GE");

		private String op;

		CompOperands(String operand) {
			op = operand;
		}

		public String toString() {
			return op;
		}
	}

	/**
	 * Bitwise comparison operands
	 * 
	 */
	public enum BitWiseOperands {
		AND("&"), OR("|"), XOR("^");

		private String op;

		BitWiseOperands(String operand) {
			op = operand;
		}

		public String toString() {
			return op;
		}
	}
}
