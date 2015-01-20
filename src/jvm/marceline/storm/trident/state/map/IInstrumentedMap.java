package marceline.storm.trident.state.map;

import backtype.storm.task.IMetricsContext;
import storm.trident.state.map.IBackingMap;
import java.util.Map;

public interface IInstrumentedMap<T> extends IBackingMap<T> {
  void instrument(Map conf, IMetricsContext metrics);
}
