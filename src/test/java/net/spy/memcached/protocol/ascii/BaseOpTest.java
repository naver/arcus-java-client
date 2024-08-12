/*
 * arcus-java-client : Arcus Java client
 * Copyright 2010-2014 NAVER Corp.
 * Copyright 2014-2022 JaM2in Co., Ltd.
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
// Copyright (c) 2006  Dustin Sallings <dustin@spy.net>

package net.spy.memcached.protocol.ascii;

import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

import net.spy.memcached.compat.BaseMockCase;

import static org.junit.Assert.assertThrows;

/**
 * Test the basic operation buffer handling stuff.
 */
public class BaseOpTest extends BaseMockCase {

  public void testAssertions() {
    assertThrows(AssertionError.class, () -> {
      assert false;
    });
  }

  public void testDataReadType() {
    SimpleOp op = new SimpleOp(OperationReadType.DATA);
    assertSame(OperationReadType.DATA, op.getReadType());
    // Make sure lines aren't handled
    assertThrows(AssertionError.class, () -> op.handleLine("x"));
    op.setBytesToRead(2);
    op.handleRead(ByteBuffer.wrap("hi".getBytes()));
  }

  public void testLineReadType() {
    SimpleOp op = new SimpleOp(OperationReadType.LINE);
    assertSame(OperationReadType.LINE, op.getReadType());
    // Make sure lines aren't handled
    assertThrows(AssertionError.class, () -> op.handleRead(ByteBuffer.allocate(3)));
    op.handleLine("x");
  }

  public void testLineParser() throws Exception {
    String input = "This is a multiline string\r\nhere is line two\r\n";
    ByteBuffer b = ByteBuffer.wrap(input.getBytes());
    SimpleOp op = new SimpleOp(OperationReadType.LINE);
    op.linesToRead = 2;
    op.readFromBuffer(b);
    assertEquals("This is a multiline string", op.getLines().get(0));
    assertEquals("here is line two", op.getLines().get(1));
    op.setBytesToRead(2);
    op.readFromBuffer(ByteBuffer.wrap("xy".getBytes()));
    byte[] expected = {'x', 'y'};
    assertTrue("Expected " + Arrays.toString(expected) + " but got "
                    + Arrays.toString(op.getCurrentBytes()),
            Arrays.equals(expected, op.getCurrentBytes()));
  }

  public void testPartialLine() throws Exception {
    String input1 = "this is a ";
    String input2 = "test\r\n";
    ByteBuffer b = ByteBuffer.allocate(20);
    SimpleOp op = new SimpleOp(OperationReadType.LINE);

    b.put(input1.getBytes());
    ((Buffer) b).flip();
    op.readFromBuffer(b);
    assertNull(op.getCurrentLine());
    ((Buffer) b).clear();
    b.put(input2.getBytes());
    ((Buffer) b).flip();
    op.readFromBuffer(b);
    assertEquals("this is a test", op.getCurrentLine());
  }

  private static class SimpleOp extends OperationImpl {

    private final LinkedList<String> lines = new LinkedList<>();
    private byte[] currentBytes = null;
    private int bytesToRead = 0;
    private int linesToRead = 1;

    public SimpleOp(OperationReadType t) {
      setReadType(t);
    }

    public void setBytesToRead(int to) {
      bytesToRead = to;
    }

    public String getCurrentLine() {
      return lines.isEmpty() ? null : lines.getLast();
    }

    public List<String> getLines() {
      return lines;
    }

    public byte[] getCurrentBytes() {
      return currentBytes;
    }

    @Override
    public void handleLine(String line) {
      assert getReadType() == OperationReadType.LINE;
      lines.add(line);
      if (--linesToRead == 0) {
        setReadType(OperationReadType.DATA);
      }
    }

    @Override
    public void handleRead(ByteBuffer data) {
      assert getReadType() == OperationReadType.DATA;
      assert bytesToRead > 0;
      if (bytesToRead > 0) {
        currentBytes = new byte[bytesToRead];
        data.get(currentBytes);
      }
    }

    @Override
    public void initialize() {
      setBuffer(ByteBuffer.allocate(0));
    }

    @Override
    public boolean isBulkOperation() {
      return false;
    }

    @Override
    public boolean isPipeOperation() {
      return false;
    }

    @Override
    public boolean isIdempotentOperation() {
      return false;
    }
  }
}
