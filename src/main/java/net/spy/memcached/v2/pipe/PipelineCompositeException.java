package net.spy.memcached.v2.pipe;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import net.spy.memcached.internal.CompositeException;

/**
 * For Internal Use.
 */
public class PipelineCompositeException extends CompositeException {

  private static final long serialVersionUID = -3901204588665566485L;
  private final ArrayList<Boolean> result = new ArrayList<>();

  public PipelineCompositeException(List<Exception> exceptions, List<Boolean> result) {
    super(exceptions);
    this.result.addAll(result);
  }

  public List<Boolean> getResult() {
    return Collections.unmodifiableList(result);
  }
}
