package benchmark.transaction.definition;

public class UpdateTemporalDataTx extends AbstractTransaction {
    private String roadId;
    private int startTime;
    private int endTime;
    private int travelTime;
    private int jamStatus;
    private int segmentCount;

    public UpdateTemporalDataTx() {
        this.setTxType(TxType.TX_UPDATE_TEMPORAL_DATA);
    }

    public UpdateTemporalDataTx(String roadId, int startTime, int endTime, int travelTime, int jamStatus, int segmentCount) {
        this.setTxType(TxType.TX_UPDATE_TEMPORAL_DATA);
        this.roadId = roadId;
        this.startTime = startTime;
        this.endTime = endTime;
        this.travelTime = travelTime;
        this.jamStatus = jamStatus;
        this.segmentCount = segmentCount;
    }

    public String getRoadId() {
        return roadId;
    }

    public int getStartTime() {
        return startTime;
    }

    public int getEndTime() {
        return endTime;
    }

    public int getTravelTime() {
        return travelTime;
    }

    public int getJamStatus() {
        return jamStatus;
    }

    public int getSegmentCount() {
        return segmentCount;
    }

    public void setRoadId(String roadId) {
        this.roadId = roadId;
    }

    public void setStartTime(int startTime) {
        this.startTime = startTime;
    }

    public void setEndTime(int endTime) {
        this.endTime = endTime;
    }

    public void setTravelTime(int travelTime) {
        this.travelTime = travelTime;
    }

    public void setJamStatus(int jamStatus) {
        this.jamStatus = jamStatus;
    }

    public void setSegmentCount(int segmentCount) {
        this.segmentCount = segmentCount;
    }
}