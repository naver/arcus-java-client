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

public abstract class CollectionUpdate<T> {

  protected int flags = 0;
  protected T newValue;
  protected ElementFlagUpdate eflagUpdate;
  protected boolean noreply = false;
  protected String str;

  protected CollectionUpdate(T newValue, ElementFlagUpdate eflagUpdate, boolean noreply) {

    if (eflagUpdate == null) {
      if (newValue == null) {
        throw new IllegalArgumentException(
                "One of the newValue or elementFlag must not be null.");
      }
    } else {
      if (eflagUpdate.getElementFlag().length > ElementFlagFilter.MAX_EFLAG_LENGTH) {
        throw new IllegalArgumentException("length of element flag cannot exceed "
                + ElementFlagFilter.MAX_EFLAG_LENGTH + ".");
      }
    }
    this.newValue = newValue;
    this.eflagUpdate = eflagUpdate;
    this.noreply = noreply;
  }

  public String stringify() {
    if (str != null) {
      return str;
    }

    StringBuilder b = new StringBuilder();

    if (noreply) {
      b.append((b.length() <= 0) ? "" : " ").append("noreply");
    }

    str = b.toString();
    return str;
  }

  public void setFlags(int flags) {
    this.flags = flags;
  }

  public T getNewValue() {
    return newValue;
  }

  public ElementFlagUpdate getElementFlagUpdate() {
    return eflagUpdate;
  }

  public String toString() {
    return (str != null) ? str : stringify();
  }

  public abstract String getCommand();

}
