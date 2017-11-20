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

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Collections;

import net.spy.memcached.KeyUtil;
import net.spy.memcached.collection.BTreeFindPosition;
import net.spy.memcached.collection.CollectionResponse;
import net.spy.memcached.ops.APIType;
import net.spy.memcached.ops.BTreeFindPositionOperation;
import net.spy.memcached.ops.CollectionOperationStatus;
import net.spy.memcached.ops.OperationCallback;
import net.spy.memcached.ops.OperationState;
import net.spy.memcached.ops.OperationStatus;
import net.spy.memcached.ops.OperationType;

public class BTreeFindPositionOperationImpl extends OperationImpl implements
        BTreeFindPositionOperation {

  private static final OperationStatus GET_CANCELED = new CollectionOperationStatus(
          false, "collection canceled", CollectionResponse.CANCELED);

  private static final OperationStatus POSITION = new CollectionOperationStatus(
          true, "POSITION", CollectionResponse.OK); // OK is arbitrary response
  private static final OperationStatus NOT_FOUND = new CollectionOperationStatus(
          false, "NOT_FOUND", CollectionResponse.NOT_FOUND);
  private static final OperationStatus UNREADABLE = new CollectionOperationStatus(
          false, "UNREADABLE", CollectionResponse.UNREADABLE);
  private static final OperationStatus BKEY_MISMATCH = new CollectionOperationStatus(
          false, "BKEY_MISMATCH", CollectionResponse.BKEY_MISMATCH);
  private static final OperationStatus TYPE_MISMATCH = new CollectionOperationStatus(
          false, "TYPE_MISMATCH", CollectionResponse.TYPE_MISMATCH);
  private static final OperationStatus NOT_FOUND_ELEMENT = new CollectionOperationStatus(
          false, "NOT_FOUND_ELEMENT", CollectionResponse.NOT_FOUND_ELEMENT);

  protected final String key;
  protected final BTreeFindPosition get;

  protected byte[] data = null;
  protected int readOffset = 0;
  protected byte lookingFor = '\0';
  protected int spaceCount = 0;

  public BTreeFindPositionOperationImpl(String key, BTreeFindPosition get,
                                        OperationCallback cb) {
    super(cb);
    this.key = key;
    this.get = get;
    setAPIType(APIType.BOP_POSITION);
    setOperationType(OperationType.READ);
  }

  @Override
  public BTreeFindPosition getGet() {
    return get;
  }

  @Override
  public void handleLine(String line) {
    if (getLogger().isDebugEnabled()) {
      getLogger().debug("Got line %s", line);
    }

    Integer position = null;

    if (line.startsWith("POSITION=")) {
      String[] stuff = line.split("=");
      assert stuff.length == 2;
      assert "POSITION".equals(stuff[0]);

      // FIXME exception-based conversion.
      try {
        // expected : POSITION=<position> : 0 or positive integer
        position = Integer.parseInt(stuff[1]);
        BTreeFindPositionOperation.Callback cb = (BTreeFindPositionOperation.Callback) getCallback();
        cb.gotData(position);
        getCallback().receivedStatus(POSITION);
      } catch (Exception e) {
        // expected : <error_string>
      }
    /* ENABLE_MIGRATION if */
    } else if (line.startsWith("NOT_MY_KEY ")) {
      receivedMigrateOperations(line, true);
      return;
		/* ENABLE_MIGRATION end */
    } else {
      OperationStatus status = matchStatus(line, NOT_FOUND, UNREADABLE,
              BKEY_MISMATCH, TYPE_MISMATCH, NOT_FOUND_ELEMENT);
      if (getLogger().isDebugEnabled()) {
        getLogger().debug(status);
      }
      getCallback().receivedStatus(status);
    }

    transitionState(OperationState.COMPLETE);
  }

  @Override
  public void initialize() {
    String cmd = get.getCommand();
    String args = get.stringify();

    ByteBuffer bb = ByteBuffer.allocate(KeyUtil.getKeyBytes(key).length
            + cmd.length() + args.length() + 16);

    setArguments(bb, cmd, key, args);
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
    return Collections.singleton(key);
  }

}
