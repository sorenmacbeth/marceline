package marceline.storm.trident.clojure;

import org.apache.storm.task.IMetricsContext;
import org.apache.storm.metric.api.CountMetric;
import org.apache.storm.metric.api.MultiCountMetric;
import org.apache.storm.utils.Utils;
import marceline.storm.trident.state.map.IInstrumentedMap;
import org.apache.storm.trident.state.map.IBackingMap;
import clojure.lang.IFn;
import clojure.lang.RT;

import java.util.List;
import java.util.ArrayList;
import java.util.Map;

public class ClojureInstrumentedMap implements IBackingMap<Object> {
  List<Object> _params;
  List<String> _fnSpec;
  IInstrumentedMap _instrumentedMap;

  transient MultiCountMetric _mreads;
  transient MultiCountMetric _mwrites;


  public ClojureInstrumentedMap(List fnSpec, List<Object> params) {
    _params = params;
    _fnSpec = fnSpec;

    try {
      IFn hof = Utils.loadClojureFn(_fnSpec.get(0), _fnSpec.get(1));
      _instrumentedMap = (IInstrumentedMap) hof.applyTo(RT.seq(_params));
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public List<Object> multiGet(List<List<Object>> keys) {
    return _instrumentedMap.multiGet(keys, _mreads);
  }

  public void multiPut(List<List<Object>> keys, List<Object> vals) {
    _instrumentedMap.multiPut(keys, vals, _mwrites);
  }

  public void registerMetrics(Map conf, IMetricsContext context, String mapStateMetricName, int bucketSize) {
    _mreads = context.registerMetric(mapStateMetricName + "/multiGet", new MultiCountMetric(), bucketSize);
    _mwrites = context.registerMetric(mapStateMetricName + "/multiPut", new MultiCountMetric(), bucketSize);
  }
}
