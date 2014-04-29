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

import java.util.ArrayList;

import net.spy.memcached.util.BTreeUtil;

public class ElementMultiFlagsFilter extends ElementFlagFilter {

	final static int MAX_EFLAGS = 100;
	private ArrayList<byte[]> compValue = new ArrayList<byte[]>();

	public ElementMultiFlagsFilter() {
	}

	public ElementMultiFlagsFilter setCompOperand(CompOperands compOperand) {
		if (compOperand == null) {
			throw new NullPointerException("Invalid compOperand");
		}

		if (compOperand != CompOperands.Equal
				&& compOperand != CompOperands.NotEqual) {
			throw new IllegalArgumentException(
					"The only compOperand Equal and NotEqual can compare multi compValues.");
		}

		this.compOp = compOperand;

		return this;
	}

	public ElementMultiFlagsFilter addCompValue(byte[] compValue) {

		if (compValue == null) {
			throw new NullPointerException("Invalid compOperand and compValue.");
		}

		if (compValue.length == 0) {
			throw new IllegalArgumentException(
					"Length of comparison value must be larger than 0.");
		}

		if (compValue.length > MAX_EFLAG_LENGTH) {
			throw new IllegalArgumentException(
					"Length of comparison value must be less than "
							+ MAX_EFLAG_LENGTH);
		}

		if (this.compValue.size() > MAX_EFLAGS) {
			throw new IllegalArgumentException(
					"Count of comparison values must be less than "
							+ MAX_EFLAGS);
		}

		if (this.compValue.size() > 0
				&& this.compValue.get(0).length != compValue.length) {
			throw new IllegalArgumentException(
					"Length of comparison value must be same with "
							+ this.compValue.get(0).length);
		}

		this.compValue.add(compValue);

		return this;
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

		for (int i = 0; i < compValue.size(); i++) {
			if (i > 0) {
				sb.append(",");
			}
			sb.append(BTreeUtil.toHex(compValue.get(i)));
		}

		return sb.toString();
	}
}
