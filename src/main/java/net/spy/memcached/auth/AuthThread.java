package net.spy.memcached.auth;

import java.util.concurrent.CountDownLatch;

import javax.security.sasl.Sasl;
import javax.security.sasl.SaslClient;

import net.spy.memcached.KeyUtil;
import net.spy.memcached.MemcachedConnection;
import net.spy.memcached.MemcachedNode;
import net.spy.memcached.OperationFactory;
import net.spy.memcached.compat.SpyThread;
import net.spy.memcached.ops.Operation;
import net.spy.memcached.ops.OperationCallback;
import net.spy.memcached.ops.OperationStatus;
import net.spy.memcached.ops.StatusCode;

public class AuthThread extends SpyThread {

  private final MemcachedConnection conn;
  private final AuthDescriptor authDescriptor;
  private final OperationFactory opFact;
  private final MemcachedNode node;
  private final SaslClient sc;

  private boolean mechDone = false;
  private boolean authDone = false;
  private OperationStatus priorStatus = null;

  public AuthThread(MemcachedConnection c, OperationFactory o,
                    AuthDescriptor a, MemcachedNode n) {
    conn = c;
    opFact = o;
    authDescriptor = a;
    node = n;
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
    while (!authDone) {
      final CountDownLatch latch = new CountDownLatch(1);
      final OperationCallback cb = new OperationCallback() {
        @Override
        public void receivedStatus(OperationStatus val) {
          String msg = val.getMessage();
          // If the status we found was SASL_OK or NOT_SUPPORTED, we're authDone.
          if ("SASL_OK".equals(msg) || "NOT_SUPPORTED".equals(msg)) {
            authDone = true;
            node.authComplete(true);
            getLogger().info("Authenticated to " + node.getSocketAddress());
          } else if (val.getStatusCode() == StatusCode.CANCELLED &&
                     AuthThread.this == Thread.currentThread()) {
            // Don't call authComplete() if this callback is called by auth thread
            // through calling op.cancel() after the InterruptedException .
            authDone = true;
            getLogger().error("Authentication canceled to " + node.getSocketAddress() + ": " + msg);
          } else if (!val.isSuccess()) {
            authDone = true;
            node.authComplete(false);
            getLogger().error("Authentication failed to " + node.getSocketAddress() + ": " + msg);
          } else if (!mechDone) {
            mechDone = true;
          } else {
            // Get the prior status to create the correct operation.
            priorStatus = val;
          }
        }

        @Override
        public void complete() {
          latch.countDown();
        }
      };

      final Operation op;
      if (!mechDone) {
        op = opFact.saslMechs(true, cb);
      } else if (priorStatus == null) {
        op = opFact.saslAuth(sc, cb);
      } else {
        op = opFact.saslStep(sc, KeyUtil.getKeyBytes(priorStatus.getMessage()), cb);
      }
      conn.insertOperation(node, op);

      try {
        latch.await();
      } catch (InterruptedException e) {
        // we can be interrupted if we were in the
        // process of auth'ing and the connection is
        // lost or dropped due to bad auth
        Thread.currentThread().interrupt();
        if (op != null) {
          op.cancel("interruption to authentication: " + e);
        }
        authDone = true; // If we were interrupted, tear down
      }
    }
  }
}
