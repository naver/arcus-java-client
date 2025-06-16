package net.spy.memcached.auth;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public enum ScramMechanism {
  SCRAM_SHA_256("SHA-256", "HmacSHA256");

  private static final Map<String, ScramMechanism> MECHANISMS_MAP;

  private final String mechanismName;
  private final String hashAlgorithm;
  private final String macAlgorithm;

  static {
    Map<String, ScramMechanism> map = new HashMap<>();
    for (ScramMechanism mech : values()) {
      map.put(mech.mechanismName, mech);
    }
    MECHANISMS_MAP = Collections.unmodifiableMap(map);
  }

  private ScramMechanism(String hashAlgorithm, String macAlgorithm) {
    this.mechanismName = "SCRAM-" + hashAlgorithm;
    this.hashAlgorithm = hashAlgorithm;
    this.macAlgorithm = macAlgorithm;
  }

  public final String mechanismName() {
    return this.mechanismName;
  }
  public String hashAlgorithm() {
    return hashAlgorithm;
  }
  
  public String macAlgorithm() {
    return macAlgorithm;
  }

  public static ScramMechanism forMechanismName(String mechanismName) {
    return MECHANISMS_MAP.get(mechanismName);
  }

  public static Collection<String> mechanismNames() {
    return MECHANISMS_MAP.keySet();
  }
}
