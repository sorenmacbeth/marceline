package marceline.storm.trident.clojure;

import org.apache.storm.utils.Utils;
import org.apache.storm.trident.operation.CombinerAggregator;
import org.apache.storm.trident.tuple.TridentTuple;
import clojure.lang.IFn;
import clojure.lang.RT;

import java.util.List;
import java.util.ArrayList;

public class ClojureCombinerAggregator implements CombinerAggregator<Object> {

  List<Object> _params;
  List<String> _fnSpec;
  CombinerAggregator _aggregator;
  boolean _booted = false;

  public ClojureCombinerAggregator(List fnSpec, List<Object> params) {
    _params = params;
    _fnSpec = fnSpec;
  }

  private void bootClojure() {
    if(!_booted) {
      try {
        IFn hof = Utils.loadClojureFn(_fnSpec.get(0), _fnSpec.get(1));
        _aggregator = (CombinerAggregator) hof.applyTo(RT.seq(_params));
        _booted = true;
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
  }

  @Override
  public Object init(TridentTuple objects) {
    bootClojure();
    return _aggregator.init(objects);
  }

  @Override
  public Object combine(Object o, Object o2) {
    bootClojure();
    return _aggregator.combine(o, o2);
  }

  @Override
  public Object zero() {
    bootClojure();
    return _aggregator.zero();
  }
}
