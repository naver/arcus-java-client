package net.spy.memcached.auth;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;

import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.NameCallback;
import javax.security.auth.callback.PasswordCallback;
import javax.security.sasl.SaslClient;
import javax.security.sasl.SaslClientFactory;
import javax.security.sasl.SaslException;

import com.bolyartech.scram_sasl.client.ScramClientFunctionality;
import com.bolyartech.scram_sasl.client.ScramClientFunctionalityImpl;
import com.bolyartech.scram_sasl.common.ScramException;

public class ScramSaslClient implements SaslClient {

  enum State {
    SEND_CLIENT_FIRST_MESSAGE,
    RECEIVE_SERVER_FIRST_MESSAGE,
    RECEIVE_SERVER_FINAL_MESSAGE,
    COMPLETE,
    FAILED
  }

  private final ScramMechanism mechanism;
  private final CallbackHandler callbackHandler;
  private final ScramClientFunctionality scf;
  private State state;

  public ScramSaslClient(ScramMechanism mechanism, CallbackHandler cbh) {
    this.callbackHandler = cbh;
    this.mechanism = mechanism;
    this.scf = new ScramClientFunctionalityImpl(
            mechanism.hashAlgorithm(), mechanism.macAlgorithm());
    this.state = State.SEND_CLIENT_FIRST_MESSAGE;
  }

  @Override
  public String getMechanismName() {
    return this.mechanism.mechanismName();
  }

  @Override
  public boolean hasInitialResponse() {
    return true;
  }

  @Override
  public byte[] evaluateChallenge(byte[] challenge) throws SaslException {
    try {
      switch (this.state) {
        case SEND_CLIENT_FIRST_MESSAGE:
          if (challenge != null && challenge.length != 0) {
            throw new SaslException("Expected empty challenge");
          }

          NameCallback nameCallback = new NameCallback("Name: ");

          try {
            callbackHandler.handle(new Callback[]{nameCallback});
          } catch (Throwable e) {
            throw new SaslException("User name could not be obtained", e);
          }

          String username = nameCallback.getName();
          byte[] clientFirstMessage = this.scf.prepareFirstMessage(username).getBytes();
          this.state = State.RECEIVE_SERVER_FIRST_MESSAGE;
          return clientFirstMessage;
        
        case RECEIVE_SERVER_FIRST_MESSAGE:
          String serverFirstMessage = new String(challenge, StandardCharsets.UTF_8);

          PasswordCallback passwordCallback = new PasswordCallback("Password: ", false);
          try {
            callbackHandler.handle(new Callback[]{passwordCallback});
          } catch (Throwable e) {
            throw new SaslException("Password could not be obtained", e);
          }

          String password = String.valueOf(passwordCallback.getPassword());
          byte[] clientFinalMessage = this.scf.prepareFinalMessage(
                  password, serverFirstMessage).getBytes();
          if (clientFinalMessage == null) {
            throw new SaslException("clientFinalMessage should not be null");
          }
          this.state = State.RECEIVE_SERVER_FINAL_MESSAGE;
          return clientFinalMessage;
        
        case RECEIVE_SERVER_FINAL_MESSAGE:
          String serverFinalMessage = new String(challenge, StandardCharsets.UTF_8);
          if (!this.scf.checkServerFinalMessage(serverFinalMessage)) {
            throw new SaslException("Sasl authentication using " + this.mechanism +
                    " failed with error: invalid server final message");
          }
          this.state = State.COMPLETE;
          return new byte[]{};
      
        default:
          throw new SaslException("Unexpected challenge in Sasl client state " + this.state);
      }
    } catch (ScramException e) {
      this.state = State.FAILED;
      throw new SaslException("ScramException", e);
    } catch (SaslException e) {
      this.state = State.FAILED;
      throw e;
    }
  }

  @Override
  public boolean isComplete() {
    return this.state == State.COMPLETE;
  }

  @Override
  public byte[] unwrap(byte[] incoming, int offset, int len) throws SaslException {
    if (!isComplete()) {
      throw new IllegalStateException("Authentication exchange has not completed");
    }
    return Arrays.copyOfRange(incoming, offset, offset + len);
  }

  @Override
  public byte[] wrap(byte[] outgoing, int offset, int len) throws SaslException {
    if (!isComplete()) {
      throw new IllegalStateException("Authentication exchange has not completed");
    }
    return Arrays.copyOfRange(outgoing, offset, offset + len);
  }

  @Override
  public Object getNegotiatedProperty(String propName) {
    if (!isComplete()) {
      throw new IllegalStateException("Authentication exchange has not completed");
    }
    return null;
  }

  @Override
  public void dispose() throws SaslException {
  }

  public static class ScramSaslClientFactory implements SaslClientFactory {
    @Override
    public SaslClient createSaslClient(String[] mechanisms,
            String authorizationId,
            String protocol,
            String serverName,
            Map<String, ?> props,
            CallbackHandler cbh) throws SaslException {

      ScramMechanism mechanism = null;
      for (String mech : mechanisms) {
        mechanism = ScramMechanism.forMechanismName(mech);
        if (mechanism != null) {
          break;
        }
      }
      if (mechanism == null) {
        throw new SaslException(String.format("Requested mechanisms '%s' not supported."
                + " Supported mechanisms are '%s'.",
                Arrays.asList(mechanisms), ScramMechanism.mechanismNames()));
      }

      return new ScramSaslClient(mechanism, cbh);
    }

    @Override
    public String[] getMechanismNames(Map<String, ?> props) {
      Collection<String> mechanisms = ScramMechanism.mechanismNames();
      return mechanisms.toArray(new String[0]);
    }
  }
}
