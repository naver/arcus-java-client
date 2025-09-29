package net.spy.memcached.protocol.binary;

import java.io.IOException;

import javax.security.sasl.SaslClient;
import javax.security.sasl.SaslException;

import net.spy.memcached.auth.AuthException;
import net.spy.memcached.internal.ReconnDelay;
import net.spy.memcached.ops.OperationCallback;
import net.spy.memcached.ops.OperationStatus;
import net.spy.memcached.ops.StatusCode;

public abstract class SASLBaseOperationImpl extends OperationImpl {

  private static final OperationStatus SASL_OK =
          new OperationStatus(true, "SASL_OK", StatusCode.SUCCESS);
  private static final OperationStatus SASL_NOT_SUPPORTED =
          new OperationStatus(true, "NOT_SUPPORTED", StatusCode.SUCCESS);

  private static final int SUCCESS = 0x00;
  private static final int AUTH_ERROR = 0x20;
  private static final int SASL_CONTINUE = 0x21;
  private static final int NOT_SUPPORTED = 0x83;

  protected final SaslClient sc;
  protected final byte[] challenge;

  public SASLBaseOperationImpl(int c, SaslClient sc, byte[] challenge,
                               OperationCallback cb) {
    super(c, generateOpaque(), cb);
    this.sc = sc;
    this.challenge = challenge;
  }

  @Override
  public void initialize() {
    try {
      byte[] response = buildResponse(sc);
      String mechanism = sc.getMechanismName();

      prepareBuffer(mechanism, 0, response);
    } catch (SaslException e) {
      // XXX:  Probably something saner can be done here.
      throw new RuntimeException("Can't make SASL go.", e);
    }
  }

  protected abstract byte[] buildResponse(SaslClient sc) throws SaslException;

  @Override
  protected void decodePayload(byte[] pl) {
    getLogger().debug("Auth response:  %s", new String(pl));
    complete(new OperationStatus(true, new String(pl), StatusCode.SUCCESS));
  }

  @Override
  protected void finishedPayload(byte[] pl) throws IOException {
    if (errorCode == SASL_CONTINUE) {
      complete(new OperationStatus(true,
              new String(pl), StatusCode.SUCCESS));
    } else if (errorCode == SUCCESS) {
      complete(SASL_OK);
    } else if (errorCode == NOT_SUPPORTED) {
      complete(SASL_NOT_SUPPORTED);
    } else if (errorCode == AUTH_ERROR) {
      String message = "AUTH_ERROR " + new String(pl);
      complete(new OperationStatus(false, message, StatusCode.ERR_AUTH));
      throw new AuthException(message, ReconnDelay.DEFAULT);
    } else {
      String message = new String(pl);
      OperationStatus status = getStatusForErrorCode(errorCode, pl);
      if (status == null) {
        getLogger().info("Unhandled state: " + message);
        status = new OperationStatus(false, message, StatusCode.fromAsciiLine(message));
      }

      complete(status);
      throw new AuthException(message, ReconnDelay.DEFAULT);
    }
  }

  @Override
  public boolean isIdempotentOperation() {
    return false;
  }

}
