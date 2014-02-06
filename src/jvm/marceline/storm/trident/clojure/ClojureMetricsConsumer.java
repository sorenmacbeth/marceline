package marceline.storm.trident.clojure;

import backtype.storm.utils.Utils;
import backtype.storm.metric.api.IMetricsConsumer;
import backtype.storm.task.TopologyContext;
import backtype.storm.task.IErrorReporter;
import clojure.lang.IFn;
import clojure.lang.RT;

import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.Collection;

public class ClojureMetricsConsumer implements IMetricsConsumer {
  List<String> _fnSpec;
  List<Object> _params;
  IMetricsConsumer _fn;

  public ClojureMetricsConsumer(List fnSpec, List<Object> params) {
    _fnSpec = fnSpec;
    _params = params;
  }

  @Override
  public void prepare(final Map conf, final Object registrationArgument, final TopologyContext context, final IErrorReporter errorReporter) {
    IFn hof = Utils.loadClojureFn(_fnSpec.get(0), _fnSpec.get(1));
    try {
      IFn preparer = (IFn) hof.applyTo(RT.seq(_params));
      List<Object> args = new ArrayList<Object>() {{
          add(conf);
          add(registrationArgument);
          add(context);
          add(errorReporter);
        }};

      _fn = (IMetricsConsumer) preparer.applyTo(RT.seq(args));

      try {
        _fn.prepare(conf, registrationArgument, context, errorReporter);
      } catch (AbstractMethodError ame) {

      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void handleDataPoints(TaskInfo taskInfo, Collection<DataPoint> dataPoints) {
    _fn.handleDataPoints(taskInfo, dataPoints);
  }

  @Override
  public void cleanup() {

  }
}
