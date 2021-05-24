package benchmark.transaction.definition;

import java.util.List;

public class ImportStaticDataTx extends AbstractTransaction {
    private List<StaticCrossNode> crosses;
    private List<StaticRoadRel> roads;

    // This constructor is used while generating benchmarks.
    public ImportStaticDataTx(List<StaticCrossNode> crosses, List<StaticRoadRel> roads) {
        this.setTxType(TxType.TX_IMPORT_STATIC_DATA);
        this.crosses = crosses;
        this.roads = roads;
        Metrics m = new Metrics();
        m.setReqSize(crosses.size() + roads.size());
        this.setMetrics(m);
    }

    public ImportStaticDataTx() {
    }

    public List<StaticCrossNode> getCrosses() {
        return crosses;
    }

    public void setCrosses(List<StaticCrossNode> crosses) {
        this.crosses = crosses;
    }

    public List<StaticRoadRel> getRoads() {
        return roads;
    }

    public void setRoads(List<StaticRoadRel> roads) {
        this.roads = roads;
    }

    public static class StaticCrossNode {
        private long id;
        private String name;

        public long getId() {
            return id;
        }

        public void setId(long id) {
            this.id = id;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }
    }

    public static class StaticRoadRel {
        private long roadId, startCrossId, endCrossId;
        private String id;
        private int length, angle, type;

        public StaticRoadRel(long roadId, long startCrossId, long endCrossId, String id, int length, int angle, int type) {
            this.roadId = roadId;
            this.startCrossId = startCrossId;
            this.endCrossId = endCrossId;
            this.id = id;
            this.length = length;
            this.angle = angle;
            this.type = type;
        }

        public long getRoadId() {
            return roadId;
        }

        public void setRoadId(long roadId) {
            this.roadId = roadId;
        }

        public long getStartCrossId() {
            return startCrossId;
        }

        public void setStartCrossId(long startCrossId) {
            this.startCrossId = startCrossId;
        }

        public long getEndCrossId() {
            return endCrossId;
        }

        public void setEndCrossId(long endCrossId) {
            this.endCrossId = endCrossId;
        }

        public String getId() {
            return id;
        }

        public void setId(String id) {
            this.id = id;
        }

        public int getLength() {
            return length;
        }

        public void setLength(int length) {
            this.length = length;
        }

        public int getAngle() {
            return angle;
        }

        public void setAngle(int angle) {
            this.angle = angle;
        }

        public int getType() {
            return type;
        }

        public void setType(int type) {
            this.type = type;
        }
    }
}
