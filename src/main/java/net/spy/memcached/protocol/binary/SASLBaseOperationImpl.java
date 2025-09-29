package net.spy.memcached.protocol.binary;

import java.io.IOException;

import javax.security.sasl.SaslClient;
import javax.security.sasl.SaslException;

import net.spy.memcached.ops.OperationCallback;
import net.spy.memcached.ops.OperationErrorType;
import net.spy.memcached.ops.OperationStatus;
import net.spy.memcached.ops.StatusCode;

public abstract class SASLBaseOperationImpl extends OperationImpl {

  private static final OperationStatus SASL_OK =
          new OperationStatus(true, "SASL_OK", StatusCode.SUCCESS);

  private static final int SUCCESS = 0x00;
  private static final int AUTH_ERROR = 0x20;
  private static final int SASL_CONTINUE = 0x21;

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
    } else if (errorCode == AUTH_ERROR) {
      complete(new OperationStatus(false,
              "AUTH_ERROR " + new String(pl), StatusCode.ERR_AUTH));
    } else {
      super.finishedPayload(pl);
    }
  }

  @Override
  public boolean isIdempotentOperation() {
    return false;
  }

  @Override
  public boolean doThrowException(OperationErrorType eType) {
    return eType != OperationErrorType.GENERAL;
  }
}
