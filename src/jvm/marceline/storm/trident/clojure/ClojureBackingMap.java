package marceline.storm.trident.clojure;

import org.apache.storm.utils.Utils;
import org.apache.storm.trident.state.map.IBackingMap;
import clojure.lang.IFn;
import clojure.lang.RT;

import java.util.List;
import java.util.ArrayList;

public class ClojureBackingMap implements IBackingMap<Object> {
  List<Object> _params;
  List<String> _fnSpec;
  IBackingMap _backingMap;

  public ClojureBackingMap(List fnSpec, List<Object> params) {
    _params = params;
    _fnSpec = fnSpec;

    try {
      IFn hof = Utils.loadClojureFn(_fnSpec.get(0), _fnSpec.get(1));
      _backingMap = (IBackingMap) hof.applyTo(RT.seq(_params));
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public List<Object> multiGet(List<List<Object>> keys) {
    return _backingMap.multiGet(keys);
  }

  @Override
  public void multiPut(List<List<Object>> keys, List<Object> vals) {
    _backingMap.multiPut(keys, vals);
  }
}
