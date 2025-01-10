package net.spy.memcached.auth;

import java.security.Provider;
import java.security.Security;

import net.spy.memcached.auth.ScramSaslClient.ScramSaslClientFactory;

public final class ScramSaslClientProvider extends Provider {

  private static final long serialVersionUID = 1L;

  @SuppressWarnings("deprecation")
  private ScramSaslClientProvider() {
    super("SASL/SCRAM Client Provider", 1.0, "SASL/SCRAM Client Provider for Arcus");
    put("SaslClientFactory.SCRAM-SHA-256", ScramSaslClientFactory.class.getName());
  }

  public static void initialize() {
    Security.addProvider(new ScramSaslClientProvider());
  }
}
