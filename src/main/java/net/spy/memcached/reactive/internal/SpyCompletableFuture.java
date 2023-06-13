package net.spy.memcached.reactive.internal;

import net.spy.memcached.compat.log.Logger;
import net.spy.memcached.compat.log.LoggerFactory;

import java.util.concurrent.CompletableFuture;

public class SpyCompletableFuture<T> extends CompletableFuture<T> {
    private transient Logger logger = null;

    /**
     * Get an instance of SpyObject.
     */
    public SpyCompletableFuture() {
        super();
    }

    /**
     * Get a Logger instance for this class.
     *
     * @return an appropriate logger instance.
     */
    protected Logger getLogger() {
        if (logger == null) {
            logger = LoggerFactory.getLogger(getClass());
        }
        return (logger);
    }
}
