/*
 * arcus-java-client : Arcus Java client
 * Copyright 2010-2014 NAVER Corp.
 * Copyright 2014-present JaM2in Co., Ltd.
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

  public ScramSaslClient(ScramMechanism mech, CallbackHandler cbh) {
    callbackHandler = cbh;
    mechanism = mech;
    scf = new ScramClientFunctionalityImpl(
            mech.hashAlgorithm(), mech.macAlgorithm());
    state = State.SEND_CLIENT_FIRST_MESSAGE;
  }

  @Override
  public String getMechanismName() {
    return mechanism.mechanismName();
  }

  @Override
  public boolean hasInitialResponse() {
    return true;
  }

  @Override
  public byte[] evaluateChallenge(byte[] challenge) throws SaslException {
    try {
      switch (state) {
        /**
         * Initiates the authentication exchange by creating the client-first-message,
         * which contains the username and a client-generated nonce.
         *
         * @return A byte array of the client-first-message to be sent.
         * (ex: "n,,n=user,r=nonce")
         */
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
          byte[] clientFirstMessage = scf.prepareFirstMessage(username).getBytes();
          state = State.RECEIVE_SERVER_FIRST_MESSAGE;
          return clientFirstMessage;
        /**
         * Process the server's challenge and generate client's proof.
         * Generate a password-based cryptographic proof by using the server nonce, salt
         * and iteration count.
         *
         * @return A byte array of the client-final-message to be sent.
         * (ex: "c=....,r=...,p=....")
         */
        case RECEIVE_SERVER_FIRST_MESSAGE:
          String serverFirstMessage = new String(challenge, StandardCharsets.UTF_8);

          PasswordCallback passwordCallback = new PasswordCallback("Password: ", false);
          try {
            callbackHandler.handle(new Callback[]{passwordCallback});
          } catch (Throwable e) {
            throw new SaslException("Password could not be obtained", e);
          }

          String password = String.valueOf(passwordCallback.getPassword());
          String clientFinalMessage = scf.prepareFinalMessage(
                  password, serverFirstMessage);
          if (clientFinalMessage == null) {
            throw new SaslException("clientFinalMessage should not be null");
          }
          state = State.RECEIVE_SERVER_FINAL_MESSAGE;
          return clientFinalMessage.getBytes();
        /**
         * Verifies the server's final signature.
         * If success, the authentication exchange is complete,
         *
         * @return An empty byte array.
         */
        case RECEIVE_SERVER_FINAL_MESSAGE:
          String serverFinalMessage = new String(challenge, StandardCharsets.UTF_8);
          if (!scf.checkServerFinalMessage(serverFinalMessage)) {
            throw new SaslException("Sasl authentication using " + mechanism +
                    " failed with error: invalid server final message");
          }
          state = State.COMPLETE;
          return new byte[]{};

        default:
          throw new SaslException("Unexpected challenge in Sasl client state " + state);
      }
    } catch (ScramException e) {
      state = State.FAILED;
      throw new SaslException("Authentication failed due to ScramException", e);
    } catch (SaslException e) {
      state = State.FAILED;
      throw e;
    }
  }

  @Override
  public boolean isComplete() {
    return state == State.COMPLETE;
  }

  @Override
  public byte[] unwrap(byte[] incoming, int offset, int len) throws SaslException {
    if (!isComplete()) {
      throw new IllegalStateException("Authentication exchange has not completed");
    }
    throw new IllegalStateException("SCRAM supports neither integrity nor privacy");
  }

  @Override
  public byte[] wrap(byte[] outgoing, int offset, int len) throws SaslException {
    if (!isComplete()) {
      throw new IllegalStateException("Authentication exchange has not completed");
    }
    throw new IllegalStateException("SCRAM supports neither integrity nor privacy");
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
                Arrays.toString(mechanisms), ScramMechanism.mechanismNames()));
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
