package net.spy.memcached.auth;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import javax.security.sasl.Sasl;
import javax.security.sasl.SaslClient;

import net.spy.memcached.KeyUtil;
import net.spy.memcached.MemcachedConnection;
import net.spy.memcached.MemcachedNode;
import net.spy.memcached.OperationFactory;
import net.spy.memcached.compat.SpyThread;
import net.spy.memcached.internal.ReconnDelay;
import net.spy.memcached.ops.Operation;
import net.spy.memcached.ops.OperationCallback;
import net.spy.memcached.ops.OperationStatus;

public class AuthThread extends SpyThread {

  private final MemcachedConnection conn;
  private final AuthDescriptor authDescriptor;
  private final OperationFactory opFact;
  private final MemcachedNode node;
  private final long timeout;
  private SaslClient sc;

  private volatile boolean done = false;
  private OperationStatus priorStatus = null;

  public AuthThread(MemcachedConnection c, OperationFactory o,
                    AuthDescriptor a, MemcachedNode n, long operationTimeout) {
    conn = c;
    opFact = o;
    authDescriptor = a;
    node = n;
    timeout = operationTimeout;

    initSaslClient();
  }

  private void initSaslClient() {
    try {
      sc = Sasl.createSaslClient(authDescriptor.getMechs(), null,
              "memcached", node.getSocketAddress().toString(), null, authDescriptor.getCallback());
    } catch (Exception e) {
      throw new IllegalStateException("Can't create SaslClient", e);
    }

    if (sc == null) {
      throw new IllegalStateException("SaslClient is null");
    }
  }

  @Override
  public void run() {
    while (!done && !Thread.interrupted()) {
      final CountDownLatch latch = new CountDownLatch(1);

      final OperationCallback cb = new OperationCallback() {
        public void receivedStatus(OperationStatus val) {
          // If the status we found was SASL_OK or NOT_SUPPORTED, we're done.
          String msg = val.getMessage();
          if ("SASL_OK".equals(msg) || "NOT_SUPPORTED".equals(msg)) {
            done = true;
            node.authComplete();
            getLogger().info("Authenticated to "
                    + node.getSocketAddress());
          } else if (msg != null && msg.startsWith("ERROR ")) {
            done = true; // tear down for the new AuthThread object after reconnect
            latch.countDown();

            if (priorStatus == null) { // ERROR response on the 1st auth step
              throw new AuthException("skipping authentication",
                      AuthExceptionType.SKIP_AUTH, ReconnDelay.IMMEDIATE);
            } else { // ERROR response after received SASL_CONTINUE
              throw new AuthException("retrying authentication",
                      AuthExceptionType.RETRY_AUTH, ReconnDelay.DEFAULT);
            }
          } else {
            priorStatus = val;
          }
        }

        public void complete() {
          latch.countDown();
        }
      };

      // Get the prior status to create the correct operation.
      final Operation op = buildOperation(priorStatus, cb);
      conn.insertOperation(node, op);

      try {
        boolean isTimeout = !latch.await(timeout, TimeUnit.MILLISECONDS);
        if (done) {
          break;
        }

        if (isTimeout) {
          node.setupForAuth("authentication timeout");
          conn.setupReconnect(node, ReconnDelay.DEFAULT, "authentication timeout");
          break;
        }

        // Get the new status to inspect it.
        if (priorStatus != null && !priorStatus.isSuccess()) {
          node.authFail();
          getLogger().warn("Authentication failed to "
                  + node.getSocketAddress() + ": " + priorStatus.getMessage());

          priorStatus = null;
          initSaslClient();

          Thread.sleep(1000);
        }
      } catch (InterruptedException e) {
        // we can be interrupted if we were in the
        // process of auth'ing and the connection is
        // lost or dropped due to bad auth
        Thread.currentThread().interrupt();
        if (op != null) {
          op.cancel("interruption to authentication" + e);
        }
        done = true; // If we were interrupted, tear down.
      }
    }
  }

  private Operation buildOperation(OperationStatus st, OperationCallback cb) {
    if (st == null) {
      return opFact.saslAuth(sc, cb);
    } else {
      return opFact.saslStep(sc, KeyUtil.getKeyBytes(st.getMessage()), cb);
    }
  }
}
