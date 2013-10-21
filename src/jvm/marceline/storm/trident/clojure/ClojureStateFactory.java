package marceline.storm.trident.clojure;

import backtype.storm.utils.Utils;
import storm.trident.state.State;
import storm.trident.state.StateFactory;
import backtype.storm.task.IMetricsContext;
import clojure.lang.IFn;
import clojure.lang.RT;

import java.util.List;
import java.util.ArrayList;
import java.util.Map;

public class ClojureStateFactory implements StateFactory {
  List<Object> _params;
  List<String> _fnSpec;
  StateFactory _factory;
  boolean _booted = false;

  public ClojureStateFactory(List fnSpec, List<Object> params) {
    _params = params;
    _fnSpec = fnSpec;
  }

  private void bootClojure() {
    if(!_booted) {
      try {
        IFn hof = Utils.loadClojureFn(_fnSpec.get(0), _fnSpec.get(1));
        _factory = (StateFactory) hof.applyTo(RT.seq(_params));
        _booted = true;
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
  }

  @Override
  public State makeState(Map conf, IMetricsContext metrics, int partitionIndex, int numPartitions) {
    bootClojure();
    return _factory.makeState(conf, metrics, partitionIndex, numPartitions);
  }
}
