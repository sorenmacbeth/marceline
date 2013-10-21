package marceline.storm.trident.clojure;

import backtype.storm.utils.Utils;
import storm.trident.operation.Aggregator;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.TridentOperationContext;
import storm.trident.tuple.TridentTuple;
import clojure.lang.IFn;
import clojure.lang.RT;

import java.util.List;
import java.util.ArrayList;
import java.util.Map;

public class ClojureAggregator implements Aggregator<Object> {
  List<String> _fnSpec;
  List<Object> _params;
  Aggregator _aggregator;

  public ClojureAggregator(List fnSpec, List<Object> params) {
    _fnSpec = fnSpec;
    _params = params;
  }

  @Override
  public void prepare(final Map conf, final TridentOperationContext context) {
    IFn hof = Utils.loadClojureFn(_fnSpec.get(0), _fnSpec.get(1));
    try {
      IFn preparer = (IFn) hof.applyTo(RT.seq(_params));
      List<Object> args = new ArrayList<Object>() {{
          add(conf);
          add(context);
        }};

      _aggregator = (Aggregator) preparer.applyTo(RT.seq(args));

      try {
        _aggregator.prepare(conf, context);
      } catch (AbstractMethodError ame) {

      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public Object init(Object batchId, TridentCollector tridentCollector) {
    return _aggregator.init(batchId, tridentCollector);
  }

  @Override
  public void aggregate(Object val, TridentTuple tuple, TridentCollector tridentCollector) {
    _aggregator.aggregate(val, tuple, tridentCollector);
  }

  @Override
  public void complete(Object val, TridentCollector tridentCollector) {
    _aggregator.complete(val, tridentCollector);
  }

  @Override
  public void cleanup() {

  }
}
