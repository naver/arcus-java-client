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
import net.spy.memcached.collection.Attributes;
import net.spy.memcached.collection.CollectionAttributes;
import net.spy.memcached.collection.CollectionResponse;
import net.spy.memcached.ops.APIType;
import net.spy.memcached.ops.CollectionOperationStatus;
import net.spy.memcached.ops.OperationCallback;
import net.spy.memcached.ops.OperationState;
import net.spy.memcached.ops.OperationStatus;
import net.spy.memcached.ops.OperationType;
import net.spy.memcached.ops.SetAttrOperation;

class SetAttrOperationImpl extends OperationImpl
        implements SetAttrOperation {

  private static final int OVERHEAD = 64;

  private static final OperationStatus ATTR_CANCELED = new CollectionOperationStatus(
          false, "collection canceled", CollectionResponse.CANCELED);

  private static final OperationStatus OK =
          new CollectionOperationStatus(true, "OK", CollectionResponse.OK);
  private static final OperationStatus NOT_FOUND =
          new CollectionOperationStatus(false, "NOT_FOUND", CollectionResponse.NOT_FOUND);
  private static final OperationStatus ATTR_ERROR_NOT_FOUND =
          new CollectionOperationStatus(false, "ATTR_ERROR not found", CollectionResponse.ATTR_ERROR_NOT_FOUND);
  private static final OperationStatus ATTR_ERROR_BAD_VALUE =
          new CollectionOperationStatus(false, "ATTR_ERROR bad value", CollectionResponse.ATTR_ERROR_BAD_VALUE);

  protected final String key;
  protected final Attributes attrs;

  public SetAttrOperationImpl(String key, Attributes attrs,
                              OperationCallback cb) {
    super(cb);
    this.key = key;
    // If no attributes given, set to default values
    this.attrs = (attrs == null) ? new CollectionAttributes() : attrs;
    setAPIType(APIType.SETATTR);
    setOperationType(OperationType.WRITE);
  }

  @Override
  public void handleLine(String line) {
    assert getState() == OperationState.READING
            : "Read ``" + line + "'' when in " + getState() + " state";
    /* ENABLE_REPLICATION if */
    if (line.equals("SWITCHOVER") || line.equals("REPL_SLAVE")) {
      receivedMoveOperations(line);
    }

    /* ENABLE_REPLICATION end */
    /* ENABLE_MIGRATION if */
    if (line.startsWith("NOT_MY_KEY ")) {
      receivedMigrateOperations(line, true);
      return;
    }
    /* ENABLE_MIGRATION end */
    getCallback().receivedStatus(
            matchStatus(line, OK, NOT_FOUND, ATTR_ERROR_NOT_FOUND,
                    ATTR_ERROR_BAD_VALUE));
    transitionState(OperationState.COMPLETE);
  }

  @Override
  public void initialize() {
    ByteBuffer bb = ByteBuffer.allocate(KeyUtil.getKeyBytes(key).length +
            attrs.getLength() + OVERHEAD);

    setArguments(bb, "setattr", key, attrs);

    bb.flip();
    setBuffer(bb);

    if (getLogger().isDebugEnabled()) {
      getLogger().debug("Request in ascii protocol: "
              + (new String(bb.array())).replace("\r\n", "\\r\\n"));
    }
  }

  @Override
  protected void wasCancelled() {
    getCallback().receivedStatus(ATTR_CANCELED);
  }

  public Collection<String> getKeys() {
    return Collections.singleton(key);
  }

  public Attributes getAttributes() {
    return attrs;
  }

}
