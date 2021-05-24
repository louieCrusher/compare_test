package benchmark.utils;

import java.util.ArrayList;

public interface Topology {
    ArrayList<Pair<Long, Long>> getRoadList(long curCross);
    int earliestArriveTime(long roadId, int when, int until);
}
