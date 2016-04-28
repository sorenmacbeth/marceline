package marceline.storm.trident.clojure;

import org.apache.storm.utils.Utils;
import org.apache.storm.trident.state.State;
import org.apache.storm.trident.state.QueryFunction;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.operation.TridentOperationContext;
import org.apache.storm.trident.tuple.TridentTuple;
import clojure.lang.IFn;
import clojure.lang.RT;

import java.util.List;
import java.util.ArrayList;
import java.util.Map;

public class ClojureQueryFunction implements QueryFunction<State, Object> {
  List<String> _fnSpec;
  List<Object> _params;
  QueryFunction _queryFn;

  public ClojureQueryFunction(List fnSpec, List<Object> params) {
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

      _queryFn = (QueryFunction) preparer.applyTo(RT.seq(args));

      try {
        _queryFn.prepare(conf, context);
      } catch (AbstractMethodError ame) {

      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public List<Object> batchRetrieve(State state, List<TridentTuple> args) {
    return _queryFn.batchRetrieve(state, args);
  }

  @Override
  public void execute(TridentTuple tuple, Object result, TridentCollector tridentCollector) {
    _queryFn.execute(tuple, result, tridentCollector);
  }

  @Override
  public void cleanup() {

  }
}
