package net.spy.memcached;

public class DefaultArcusConnectionFactory extends DefaultConnectionFactory
        implements ConnectionFactory {

  /**
   * Default failureMode : FailureMode.Cancel
   */
  public static final FailureMode DEFAULT_FAILURE_MODE = FailureMode.Cancel;

  /**
   * Default isDaemon : true
   */
  public static final boolean DEFAULT_IS_DAEMON = true;

  /**
   * Maximum amount of time (in seconds) to wait between reconnect attempts.
   */
  public static final long DEFAULT_MAX_RECONNECT_DELAY = 1;

  /**
   * Default hashAlgorithm : HashAlgorithm.KETAMA_HASH
   */
  public static final HashAlgorithm DEFAULT_HASH = HashAlgorithm.KETAMA_HASH;

  /**
   * Maximum number + 2 of timeout exception for shutdown connection
   */
  public static final int DEFAULT_MAX_TIMEOUTEXCEPTION_THRESHOLD = 10;

  /**
   * Maximum timeout duration in milliseconds for shutdown connection
   */
  public static final int DEFAULT_MAX_TIMEOUTDURATION_THRESHOLD = 1000;

  /**
   * Default front cache name prefix : ArcusFrontCache_
   */
  public static final String DEFAULT_FRONT_CACHE_NAME_PREFIX = "ArcusFrontCache_";

  private final String frontCacheName;

  public DefaultArcusConnectionFactory() {
    this.frontCacheName = DEFAULT_FRONT_CACHE_NAME_PREFIX + this.hashCode();
  }

  DefaultArcusConnectionFactory(String frontCacheName) {
    this.frontCacheName = frontCacheName;
  }

  @Override
  public FailureMode getFailureMode() {
    return DEFAULT_FAILURE_MODE;
  }

  @Override
  public boolean isDaemon() {
    return DEFAULT_IS_DAEMON;
  }

  @Override
  public long getMaxReconnectDelay() {
    return DEFAULT_MAX_RECONNECT_DELAY;
  }

  @Override
  public HashAlgorithm getHashAlg() {
    return DEFAULT_HASH;
  }

  @Override
  public int getTimeoutExceptionThreshold() {
    return DEFAULT_MAX_TIMEOUTEXCEPTION_THRESHOLD;
  }

  @Override
  public int getTimeoutDurationThreshold() {
    return DEFAULT_MAX_TIMEOUTDURATION_THRESHOLD;
  }

  @Override
  public String getFrontCacheName() {
    return frontCacheName;
  }
}
