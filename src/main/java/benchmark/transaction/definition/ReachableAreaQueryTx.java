package benchmark.transaction.definition;

import benchmark.model.ReachableCrossNode;
import benchmark.utils.Helper;

import java.util.List;


public class ReachableAreaQueryTx extends AbstractTransaction {
    private long startCrossId;
    private int departureTime;
    private int travelTime;

    public ReachableAreaQueryTx() {
        this.setTxType(TxType.TX_QUERY_REACHABLE_AREA);
    }

    public ReachableAreaQueryTx(long startCrossId, int departureTime, int travelTime) {
        this.setTxType(TxType.TX_QUERY_REACHABLE_AREA);
        this.startCrossId = startCrossId;
        this.departureTime = departureTime;
        this.travelTime = travelTime;
    }

    public long getStartCrossId() {
        return startCrossId;
    }

    public int getDepartureTime() {
        return departureTime;
    }

    public int getTravelTime() {
        return travelTime;
    }

    public void setStartCrossId(long startCrossId) {
        this.startCrossId = startCrossId;
    }

    public void setDepartureTime(int departureTime) {
        this.departureTime = departureTime;
    }

    public void setTravelTime(int travelTime) {
        this.travelTime = travelTime;
    }

    @Override
    public void validateResult(AbstractTransaction.Result result) {
        Helper.validateResult(((Result) this.getResult()).getNodeArriveTime(), ((Result) result).getNodeArriveTime());
    }

    public static class Result extends AbstractTransaction.Result {
        List<ReachableCrossNode> nodeArriveTime;

        public List<ReachableCrossNode> getNodeArriveTime() {
            return nodeArriveTime;
        }

        public void setNodeArriveTime(List<ReachableCrossNode> nodeArriveTime) {
            this.nodeArriveTime = nodeArriveTime;
        }
    }
}
