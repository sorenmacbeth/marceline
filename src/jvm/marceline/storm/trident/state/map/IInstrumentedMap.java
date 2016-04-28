package marceline.storm.trident.state.map;

import org.apache.storm.metric.api.CountMetric;
import org.apache.storm.metric.api.MultiCountMetric;
import java.util.List;

public interface IInstrumentedMap<T> {
  List<T> multiGet(List<List<Object>> keys, MultiCountMetric readsCount);
  void multiPut(List<List<Object>> keys, List<T> vals, MultiCountMetric writesCount);
}
