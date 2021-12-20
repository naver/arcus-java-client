// Copyright (c) 2006  Dustin Sallings <dustin@spy.net>

package net.spy.memcached.protocol.ascii;

import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

import net.spy.memcached.KeyUtil;
import net.spy.memcached.compat.BaseMockCase;

/**
 * Test the basic operation buffer handling stuff.
 */
public class BaseOpTest extends BaseMockCase {

  public void testAssertions() {
    try {
      assert false;
      fail("Assertions are not enabled.");
    } catch (AssertionError e) {
      // OK
    }
  }

  public void testDataReadType() throws Exception {
    SimpleOp op = new SimpleOp(OperationReadType.DATA);
    assertSame(OperationReadType.DATA, op.getReadType());
    // Make sure lines aren't handled
    try {
      op.handleLine("x");
      fail("Handled a line in data mode");
    } catch (AssertionError e) {
      // ok
    }
    op.setBytesToRead(2);
    op.handleRead(ByteBuffer.wrap(KeyUtil.getKeyBytes("hi")));
  }

  public void testLineReadType() throws Exception {
    SimpleOp op = new SimpleOp(OperationReadType.LINE);
    assertSame(OperationReadType.LINE, op.getReadType());
    // Make sure lines aren't handled
    try {
      op.handleRead(ByteBuffer.allocate(3));
      fail("Handled data in line mode");
    } catch (AssertionError e) {
      // ok
    }
    op.handleLine("x");
  }

  public void testLineParser() throws Exception {
    String input = "This is a multiline string\r\nhere is line two\r\n";
    ByteBuffer b = ByteBuffer.wrap(KeyUtil.getKeyBytes(input));
    SimpleOp op = new SimpleOp(OperationReadType.LINE);
    op.linesToRead = 2;
    op.readFromBuffer(b);
    assertEquals("This is a multiline string", op.getLines().get(0));
    assertEquals("here is line two", op.getLines().get(1));
    op.setBytesToRead(2);
    op.readFromBuffer(ByteBuffer.wrap(KeyUtil.getKeyBytes("xy")));
    byte[] expected = {'x', 'y'};
    assertTrue("Expected " + Arrays.toString(expected) + " but got "
                    + Arrays.toString(op.getCurentBytes()),
            Arrays.equals(expected, op.getCurentBytes()));
  }

  public void testPartialLine() throws Exception {
    String input1 = "this is a ";
    String input2 = "test\r\n";
    ByteBuffer b = ByteBuffer.allocate(20);
    SimpleOp op = new SimpleOp(OperationReadType.LINE);

    b.put(KeyUtil.getKeyBytes(input1));
    ((Buffer) b).flip();
    op.readFromBuffer(b);
    assertNull(op.getCurrentLine());
    ((Buffer) b).clear();
    b.put(KeyUtil.getKeyBytes(input2));
    ((Buffer) b).flip();
    op.readFromBuffer(b);
    assertEquals("this is a test", op.getCurrentLine());
  }

  private static class SimpleOp extends OperationImpl {

    private final LinkedList<String> lines = new LinkedList<String>();
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

    public byte[] getCurentBytes() {
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
  }
}
