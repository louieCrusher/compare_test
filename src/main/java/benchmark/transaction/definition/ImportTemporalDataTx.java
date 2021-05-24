package benchmark.transaction.definition;

import benchmark.model.StatusUpdate;

import java.util.List;

public class ImportTemporalDataTx extends AbstractTransaction {
    public List<StatusUpdate> data;

    public ImportTemporalDataTx() {
    }
    // default constructor and getter setter are needed by json encode/decode.

    public ImportTemporalDataTx(List<StatusUpdate> lines) {
        this.setTxType(TxType.TX_IMPORT_TEMPORAL_DATA);
        this.data = lines;
        Metrics m = new Metrics();
        m.setReqSize(lines.size());
        this.setMetrics(m);
    }

    public List<StatusUpdate> getData() {
        return data;
    }

    public void setData(List<StatusUpdate> data) {
        this.data = data;
    }

}
