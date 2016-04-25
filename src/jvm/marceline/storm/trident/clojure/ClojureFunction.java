package marceline.storm.trident.clojure;

import org.apache.storm.utils.Utils;
import org.apache.storm.trident.operation.Function;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.operation.TridentOperationContext;
import org.apache.storm.trident.tuple.TridentTuple;
import clojure.lang.IFn;
import clojure.lang.RT;

import java.util.List;
import java.util.ArrayList;
import java.util.Map;

public class ClojureFunction implements Function {
  List<String> _fnSpec;
  List<Object> _params;
  Function _fn;

  public ClojureFunction(List fnSpec, List<Object> params) {
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

      _fn = (Function) preparer.applyTo(RT.seq(args));

      try {
        _fn.prepare(conf, context);
      } catch (AbstractMethodError ame) {

      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void execute(TridentTuple tuple, TridentCollector tridentCollector) {
    _fn.execute(tuple, tridentCollector);
  }

  @Override
  public void cleanup() {

  }
}
