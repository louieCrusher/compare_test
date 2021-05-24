package benchmark.model;

import java.util.Calendar;

/**
 * Created by song on 2019-12-26.
 */
public class StatusUpdate {
    private int travelTime;
    private int jamStatus;
    private int segmentCount;
    private String roadId;
    private int time;

    /**
     * data line likes:
     * 0527000512 595640_00033 1 1 18
     * Explain: month(05),day(27),hour(00),minute(05), second(12) grid(595640),chain(00033) jamStatus(1) segCount(1) travelTime(18)
     *
     * @param line traffic data from one road
     */
    public StatusUpdate(String line) {
        String[] fields = line.split(" ");
        time = timeStr2timestamp(fields[0]);
        roadId = fields[1];
        jamStatus = Integer.parseInt(fields[2]);
        segmentCount = Integer.parseInt(fields[3]);
        travelTime = Integer.parseInt(fields[4]);
    }

    /**
     * @param tStr 0527000512: month(05),day(27),hour(00),minute(05), second(12)
     * @return timestamp by seconds
     */
    private int timeStr2timestampImpl(String tStr) {
        String monthStr = tStr.substring(0, 2);
        String dayStr = tStr.substring(2, 4);
        String hourStr = tStr.substring(4, 6);
        String minuteStr = tStr.substring(6, 8);
        String secondStr = tStr.substring(8, 10);
        int month = Integer.parseInt(monthStr) - 1; // month count from 0 to 11, no 12
        int day = Integer.parseInt(dayStr);
        int hour = Integer.parseInt(hourStr);
        int minute = Integer.parseInt(minuteStr);
        int second = Integer.parseInt(secondStr);
        Calendar ca = Calendar.getInstance();
        ca.set(2010, month, day, hour, minute, second);
        long timestamp = ca.getTimeInMillis();
        if (timestamp / 1000 < Integer.MAX_VALUE) {
            return (int) (timestamp / 1000);
        } else {
            throw new RuntimeException("timestamp larger than Integer.MAX_VALUE, this should not happen");
        }
    }

    private int timeStr2timestamp(String tStr) {
        if (tStr.length() != 8 && tStr.length() != 10) {
            throw new RuntimeException("timestamp format error!");
        } else if (tStr.length() == 8) {
            return timeStr2timestampImpl(tStr + "00");
        } else {
            return timeStr2timestampImpl(tStr);
        }
    }

    public StatusUpdate(String roadId, int time, int travelTime, int jamStatus, int segmentCount) {
        this.roadId = roadId;
        this.time = time;
        this.travelTime = travelTime;
        this.jamStatus = jamStatus;
        this.segmentCount = segmentCount;
    }

    public String getRoadId() {
        return roadId;
    }

    public int getTime() {
        return time;
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

}
