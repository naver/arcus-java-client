package net.spy.memcached.auth;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

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

public class AuthThread extends SpyThread {

  private final MemcachedConnection conn;
  private final AuthDescriptor authDescriptor;
  private final OperationFactory opFact;
  private final MemcachedNode node;
  private final SaslClient sc;

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
      throw new RuntimeException("Can't create SaslClient", e);
    }
  }

  @Override
  public void run() {
    OperationStatus priorStatus = null;
    final AtomicBoolean done = new AtomicBoolean();

    while (!done.get()) {
      final CountDownLatch latch = new CountDownLatch(1);
      final AtomicReference<OperationStatus> foundStatus =
              new AtomicReference<>();

      final OperationCallback cb = new OperationCallback() {
        public void receivedStatus(OperationStatus val) {
          // If the status we found was null, we're done.
          if (val.getMessage().length() == 0) {
            done.set(true);
            node.authComplete();
            getLogger().info("Authenticated to "
                    + node.getSocketAddress());
          } else {
            foundStatus.set(val);
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
        latch.await();
        Thread.sleep(100);
      } catch (InterruptedException e) {
        // we can be interrupted if we were in the
        // process of auth'ing and the connection is
        // lost or dropped due to bad auth
        Thread.currentThread().interrupt();
        if (op != null) {
          op.cancel("interruption to authentication" + e);
        }
        done.set(true); // If we were interrupted, tear down.
      }

      // Get the new status to inspect it.
      priorStatus = foundStatus.get();
      if (priorStatus != null) {
        if (!priorStatus.isSuccess()) {
          getLogger().warn("Authentication failed to "
                  + node.getSocketAddress());
        }
      }
    }
    return;
  }

  private Operation buildOperation(OperationStatus st, OperationCallback cb) {
    if (st == null) {
      return opFact.saslAuth(sc, cb);
    } else {
      return opFact.saslStep(sc, KeyUtil.getKeyBytes(st.getMessage()), cb);
    }
  }
}
