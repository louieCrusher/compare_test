package benchmark.transaction.definition;

public abstract class AbstractTransaction {

    public enum TxType {
        TX_IMPORT_STATIC_DATA(false),
        TX_IMPORT_TEMPORAL_DATA(false),
        TX_UPDATE_TEMPORAL_DATA(false),
        TX_QUERY_SNAPSHOT(true),
        TX_QUERY_SNAPSHOT_AGGR_MAX(true),
        TX_QUERY_SNAPSHOT_AGGR_DURATION(true),
        TX_ENTITY_TEMPORAL_CONDITION_QUERY(true),
        TX_QUERY_NODE_NEIGHBOR_ROAD(true),
        TX_QUERY_REACHABLE_AREA(true);
        private boolean isReadTx;

        TxType(boolean isReadTx) {
            this.isReadTx = isReadTx;
        }

        public boolean isReadTx() {
            return isReadTx;
        }
    }

    private TxType txType;
    private Metrics metrics;
    private Result result;

    public TxType getTxType() {
        return txType;
    }

    public void setTxType(TxType txType) {
        this.txType = txType;
    }

    public Metrics getMetrics() {
        return metrics;
    }

    public void setMetrics(Metrics metrics) {
        this.metrics = metrics;
    }

    public Result getResult() {
        return result;
    }

    public void setResult(Result result) {
        this.result = result;
    }

    public void validateResult(Result result) {
    }

    public static class Metrics {
        private int waitTime; // duration, in milliseconds
        private long sendTime; // timestamp, in milliseconds
        private int exeTime; // duration, in milliseconds
        private int connId;
        private int reqSize; // user defined value, maybe bytes or rows
        private int returnSize; // user defined value, maybe bytes or rows

        public int getExeTime() {
            return exeTime;
        }

        public void setExeTime(int exeTime) {
            this.exeTime = exeTime;
        }

        public int getWaitTime() {
            return waitTime;
        }

        public void setWaitTime(int waitTime) {
            this.waitTime = waitTime;
        }

        public long getSendTime() {
            return sendTime;
        }

        public void setSendTime(long sendTime) {
            this.sendTime = sendTime;
        }

        public int getConnId() {
            return connId;
        }

        public void setConnId(int connId) {
            this.connId = connId;
        }

        public int getReqSize() {
            return reqSize;
        }

        public void setReqSize(int reqSize) {
            this.reqSize = reqSize;
        }

        public int getReturnSize() {
            return returnSize;
        }

        public void setReturnSize(int returnSize) {
            this.returnSize = returnSize;
        }
    }

    public static class Result {

    }
}
