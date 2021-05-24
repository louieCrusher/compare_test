package benchmark.transaction.definition;

import benchmark.utils.Helper;
import benchmark.utils.Triple;

import java.util.List;

public class SnapshotAggrDurationTx extends AbstractTransaction {
    private int t0;
    private int t1;
    private String p;

    public SnapshotAggrDurationTx() {
        this.setTxType(TxType.TX_QUERY_SNAPSHOT_AGGR_DURATION);
    }

    public int getT0() {
        return t0;
    }

    public int getT1() {
        return t1;
    }

    public String getP() {
        return p;
    }

    public void setT0(int t0) {
        this.t0 = t0;
    }

    public void setT1(int t1) {
        this.t1 = t1;
    }

    public void setP(String p) {
        this.p = p;
    }

    public static class Result extends AbstractTransaction.Result {
        List<Triple<String, Integer, Integer>> roadStatDuration;

        public List<Triple<String, Integer, Integer>> getRoadStatDuration() {
            return roadStatDuration;
        }

        public void setRoadStatDuration(List<Triple<String, Integer, Integer>> roadStatDuration) {
            this.roadStatDuration = roadStatDuration;
        }
    }

    @Override
    public void validateResult(AbstractTransaction.Result result) {
        Helper.validateResult(((Result) this.getResult()).getRoadStatDuration(), ((Result) result).getRoadStatDuration());
    }
}
