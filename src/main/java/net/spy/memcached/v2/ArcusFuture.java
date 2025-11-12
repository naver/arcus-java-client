package net.spy.memcached.v2;

import java.util.concurrent.CompletionStage;
import java.util.concurrent.Future;

public interface ArcusFuture<T> extends CompletionStage<T>, Future<T> {

  void complete();

}
