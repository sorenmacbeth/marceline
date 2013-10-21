package marceline.storm.trident.clojure;

import backtype.storm.utils.Utils;
import storm.trident.operation.ReducerAggregator;
import storm.trident.tuple.TridentTuple;
import clojure.lang.IFn;
import clojure.lang.RT;

import java.util.List;
import java.util.ArrayList;

public class ClojureReducerAggregator implements ReducerAggregator<Object> {
  List<Object> _params;
  List<String> _fnSpec;
  ReducerAggregator _aggregator;

  public ClojureReducerAggregator(List fnSpec, List<Object> params) {
    _params = params;
    _fnSpec = fnSpec;

    try {
      IFn hof = Utils.loadClojureFn(_fnSpec.get(0), _fnSpec.get(1));
      _aggregator = (ReducerAggregator) hof.applyTo(RT.seq(_params));
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public Object init() {
    return _aggregator.init();
  }

  @Override
  public Object reduce(Object curr, TridentTuple tuple) {
    return _aggregator.reduce(curr, tuple);
  }
}
