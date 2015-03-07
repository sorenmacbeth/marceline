package marceline.storm.trident.state.map;

import backtype.storm.metric.api.CountMetric;
import java.util.List;

public interface IInstrumentedMap<T> {
  List<T> multiGet(List<List<Object>> keys, CountMetric readsCount);
  void multiPut(List<List<Object>> keys, List<T> vals, CountMetric writesCount);
}
