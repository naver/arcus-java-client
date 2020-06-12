package net.spy.memcached.ops;

/**
 * Status indicator.
 */
public class OperationStatus {

  private final boolean isSuccess;
  private final String message;
  private final StatusCode statusCode;

  public OperationStatus(boolean success, String msg) {
    this(success, msg, null);
  }

  public OperationStatus(boolean success, String msg, StatusCode code) {
    isSuccess = success;
    message = msg;
    statusCode = code;
  }

  /**
   * Does this status indicate success?
   */
  public boolean isSuccess() {
    return isSuccess;
  }

  /**
   * Get the message included as part of this status.
   */
  public String getMessage() {
    return message;
  }

  /**
   * Get the status code associated with the operation status.
   */
  public StatusCode getStatusCode() {
    return statusCode;
  }

  @Override
  public String toString() {
    return "{OperationStatus success=" + isSuccess + ":  " + message + "}";
  }
}
