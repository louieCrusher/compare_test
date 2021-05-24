package benchmark.utils;

import java.util.HashMap;
import java.util.Map;

public class TimeMonitor {
    private Map<String, Long> beginTime = new HashMap<>();
    private Map<String, Long> endTime = new HashMap<>();

    public void mark(String phaseEnd, String phaseBegin) {
        long t = System.currentTimeMillis();
        endTime.put(phaseEnd, t);
        beginTime.put(phaseBegin, t);
    }

    public void end(String phase) {
        endTime.put(phase, System.currentTimeMillis());
    }

    public void begin(String phase) {
        beginTime.put(phase, System.currentTimeMillis());
    }

    public long duration(String phase) {
        return endTime.get(phase) - beginTime.get(phase);
    }

    public long beginT(String phase) {
        return beginTime.get(phase);
    }

    public long endT(String phase) {
        return endTime.get(phase);
    }
}
