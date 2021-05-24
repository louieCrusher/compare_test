package benchmark.transaction.definition;

import java.util.List;

public class EntityTemporalConditionTx extends AbstractTransaction {

    private int t0;
    private int t1;
    private String p;
    private int vMin;
    private int vMax;

    public EntityTemporalConditionTx() {
        this.setTxType(TxType.TX_ENTITY_TEMPORAL_CONDITION_QUERY);
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

    public int getVMin() {
        return vMin;
    }

    public int getVMax() {
        return vMax;
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

    public void setVMin(int vMin) {
        this.vMin = vMin;
    }

    public void setVMax(int vMax) {
        this.vMax = vMax;
    }

    public static class Result extends AbstractTransaction.Result {
        List<String> roads;

        public List<String> getRoads() {
            return roads;
        }

        public void setRoads(List<String> roads) {
            this.roads = roads;
        }
    }
}
