package marceline.storm.trident.clojure;

import backtype.storm.utils.Utils;
import storm.trident.operation.Filter;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.TridentOperationContext;
import storm.trident.tuple.TridentTuple;
import clojure.lang.IFn;
import clojure.lang.RT;

import java.util.List;
import java.util.ArrayList;
import java.util.Map;

public class ClojureFilter implements Filter {
  List<String> _fnSpec;
  List<Object> _params;
  Filter _filter;

  public ClojureFilter(List fnSpec, List<Object> params) {
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

      _filter = (Filter) preparer.applyTo(RT.seq(args));

      try {
        _filter.prepare(conf, context);
      } catch (AbstractMethodError ame) {

      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public boolean isKeep(TridentTuple tuple) {
    return _filter.isKeep(tuple);
  }

  @Override
  public void cleanup() {

  }
}
