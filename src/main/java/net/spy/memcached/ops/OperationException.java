package net.spy.memcached.ops;

import java.io.IOException;


/**
 * Exceptions thrown when protocol errors occur.
 */
public final class OperationException extends IOException {

  private static final long serialVersionUID = 2457625388445818437L;
  private final OperationErrorType type;

  /**
   * Exception with a message.
   *
   * @param eType the type of error that occurred
   * @param msg   the error message
   */
  public OperationException(OperationErrorType eType, String msg) {
    super(msg);
    type = eType;
  }

  /**
   * Get the type of error.
   */
  public OperationErrorType getType() {
    return type;
  }

  @Override
  public String toString() {
    return "OperationException: " + type + ": " + getMessage();
  }
}
