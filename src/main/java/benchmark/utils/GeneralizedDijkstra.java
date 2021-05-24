package benchmark.utils;

import benchmark.model.ReachableCrossNode;
import benchmark.transaction.definition.AbstractTransaction;
import benchmark.transaction.definition.ReachableAreaQueryTx;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.PriorityQueue;

public class GeneralizedDijkstra {

    public static AbstractTransaction.Result solve(ReachableAreaQueryTx tx, Topology topology) {
        List<ReachableCrossNode> answer = new ArrayList<>();
        // mark whether the state is visited.
        HashMap<State, Boolean> enq = new HashMap<>();
        // mark whether the cross is visited.
        HashMap<Long, Boolean> vis = new HashMap<>();
        PriorityQueue<State> pq = new PriorityQueue<>();
        State init = new State(tx.getStartCrossId(), tx.getDepartureTime());
        pq.add(init);
        enq.put(init, true);
        while (!pq.isEmpty()) {
            State cur = pq.poll();
            if (vis.get(cur.curCross) != null) continue;
            answer.add(new ReachableCrossNode(cur.curCross, cur.curTime));
            vis.put(cur.curCross, true);
            ArrayList<Pair<Long, Long>> nextList = topology.getRoadList(cur.curCross);
            for (Pair<Long, Long> next : nextList) {
                int time = topology.earliestArriveTime(next.left, cur.curTime, tx.getDepartureTime() + tx.getTravelTime());
                if (time == Integer.MAX_VALUE) continue;
                State nextState = new State(next.right, time);
                if (vis.get(next.right) != null || enq.get(nextState) != null) continue;
                pq.add(nextState);
                enq.put(nextState, true);
            }
        }
        ReachableAreaQueryTx.Result result = new ReachableAreaQueryTx.Result();
        result.setNodeArriveTime(answer);
        return result;
    }

    private static class State implements Comparable<State> {
        private final long curCross;
        private final int curTime;

        State(long curCross, int curTime) {
            this.curCross = curCross;
            this.curTime = curTime;
        }

        @Override
        public int compareTo(State o) {
            return curTime - o.curTime;
        }
    }
}

