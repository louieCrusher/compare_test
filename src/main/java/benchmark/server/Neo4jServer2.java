package benchmark.server;

import benchmark.model.StatusUpdate;
import benchmark.transaction.definition.*;
import benchmark.utils.*;
import com.alibaba.fastjson.JSON;
import com.google.common.base.Preconditions;
import org.neo4j.graphdb.*;

import java.io.File;
import java.io.IOException;
import java.util.*;

public class Neo4jServer2 extends Neo4jSocketServer.ReqExecutor {

    // road name -> road id.
    private HashMap<String, Long> name2Id = new HashMap<>();
    // rId -> neo4j road node id.
    private HashMap<Long, Long> rId2Id = new HashMap<>();
    // road id -> latest neo4j relation node id.
    private HashMap<Long, Long> roadLatestRelId = new HashMap<>();
    // crossId -> neo4j cross node id.
    private HashMap<Long, Long> crossId2Id = new HashMap<>();

    public static void main(String[] args) {
        Neo4jSocketServer server = new Neo4jSocketServer(dbDir(), new Neo4jServer2());
        RuntimeEnv env = RuntimeEnv.getCurrentEnv();
        String serverCodeVersion = env.name() + "." + Helper.codeGitVersion();
        System.out.println("server code version:" + serverCodeVersion);
        try {
            server.start();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    private static File dbDir() {
        String path = Helper.mustEnv("DB_PATH");
        Preconditions.checkNotNull(path, "need arg: DB_PATH");
        File dbDir = new File(path);
        if (!dbDir.exists()) {
            if (dbDir.mkdirs()) return dbDir;
            else throw new IllegalArgumentException("invalid dbDir");
        } else if (!dbDir.isDirectory()) {
            throw new IllegalArgumentException("invalid dbDir");
        }
        return dbDir;
    }

    private enum Edge implements RelationshipType {
        REACH_TO
    }

    @Override
    protected void setDB(GraphDatabaseService db) {
        this.db = db;
//        try (Transaction tx = db.beginTx()) {
//            Schema schema = db.schema();
//            schema.indexFor(Label.label("Road")).on("name").create();
//            schema.indexFor(Label.label("Cross")).on("id").create();
//            tx.success();
//        }
    }

    @Override
    protected AbstractTransaction.Result execute(String line) throws RuntimeException {
        AbstractTransaction tx = JSON.parseObject(line, AbstractTransaction.class);
        switch (tx.getTxType()) {
            case TX_IMPORT_STATIC_DATA:
                return execute((ImportStaticDataTx) tx);
            case TX_IMPORT_TEMPORAL_DATA:
                return execute((ImportTemporalDataTx) tx);
            case TX_UPDATE_TEMPORAL_DATA:
                return execute((UpdateTemporalDataTx) tx);
            case TX_QUERY_SNAPSHOT:
                return execute((SnapshotQueryTx) tx);
            case TX_QUERY_SNAPSHOT_AGGR_MAX:
                return execute((SnapshotAggrMaxTx) tx);
            case TX_QUERY_SNAPSHOT_AGGR_DURATION:
                return execute((SnapshotAggrDurationTx) tx);
            case TX_ENTITY_TEMPORAL_CONDITION_QUERY:
                return execute((EntityTemporalConditionTx) tx);
            case TX_QUERY_NODE_NEIGHBOR_ROAD:
                return execute((NodeNeighborRoadTx) tx);
            case TX_QUERY_REACHABLE_AREA:
                return execute((ReachableAreaQueryTx) tx);
            default:
                throw new UnsupportedOperationException();
        }
    }

    private AbstractTransaction.Result execute(ImportStaticDataTx tx) {
        try (Transaction t = db.beginTx()) {
            for (ImportStaticDataTx.StaticRoadRel road : tx.getRoads()) {
                // we need road to get cross id.
                Node node = db.createNode(() -> "Road");
                node.setProperty("length", road.getLength()); // int
                node.setProperty("angle", road.getAngle()); // int
                node.setProperty("type", road.getType()); // int
                node.setProperty("r_id", road.getRoadId()); // long
                node.setProperty("name", road.getId()); // String
                node.setProperty("start_cross_id", road.getStartCrossId()); // long
                node.setProperty("end_cross_id", road.getEndCrossId()); // long
                node.setProperty("relationships", new String(""));
                name2Id.put(road.getId(), road.getRoadId());
                rId2Id.put(road.getRoadId(), node.getId());
                roadLatestRelId.put(road.getRoadId(), -1l);
            }
            for (ImportStaticDataTx.StaticCrossNode cross : tx.getCrosses()) {
                Node node = db.createNode(() -> "Cross");
                node.setProperty("id", cross.getId()); // long
                node.setProperty("name", cross.getName()); // String
                crossId2Id.put(cross.getId(), node.getId());
            }
            t.success();
        }
        return new AbstractTransaction.Result();
    }

    private AbstractTransaction.Result execute(ImportTemporalDataTx tx) {
        try (Transaction t = db.beginTx()) {
            for (StatusUpdate s : tx.getData()) {
                Long rId = name2Id.get(s.getRoadId());
                if (rId == null) {
                    System.out.println("Some strange things occur.");
                    System.out.println("There are no this road.");
                    continue;
                }
                Node road = db.getNodeById(rId2Id.get(rId));
                long start = (long) road.getProperty("start_cross_id");
                long end = (long) road.getProperty("end_cross_id");
                Node startCross = db.getNodeById(crossId2Id.get(start));
                Node endCross = db.getNodeById(crossId2Id.get(end));
                long tag = roadLatestRelId.get(rId);
                Relationship rel = startCross.createRelationshipTo(endCross, Edge.REACH_TO);
                roadLatestRelId.put(rId, rel.getId());
                int status = s.getJamStatus();
                int segCnt = s.getSegmentCount();
                int travelTime = s.getTravelTime();
                // notice that this filed means neo4j road node id.
                rel.setProperty("road_node_id", road.getId());
                rel.setProperty("status", status);
                rel.setProperty("seg_cnt", segCnt);
                rel.setProperty("travel_time", travelTime);
                int time = s.getTime();
                rel.setProperty("start_time", time);
                rel.setProperty("end_time", Integer.MAX_VALUE);
                String rels = (String) road.getProperty("relationships");
                rels = rels + "|" + rel.getId();
                road.setProperty("relationships", rels);
                if (tag != -1l) {
                    Relationship lastRel = db.getRelationshipById(tag);
                    lastRel.setProperty("end_time", time - 1); // make the time range [st, en]
                }
            }
            t.success();
        }
        return new AbstractTransaction.Result();
    }

    private AbstractTransaction.Result execute(UpdateTemporalDataTx tx) {
        try (Transaction t = db.beginTx()) {
            // Do not consider that the old value equals to the new value
            int st = tx.getStartTime();
            int en = tx.getEndTime();
            Node road = db.findNode(() -> "Road", "name", tx.getRoadId());
            Node startCross = db.findNode(() -> "Cross", "id", road.getProperty("start_cross_id"));
            Node endCross = db.findNode(() -> "Cross", "id", road.getProperty("end_cross_id"));
            Iterable<Relationship> rels = startCross.getRelationships(Edge.REACH_TO, Direction.OUTGOING);
            ArrayList<Relationship> relsStart2End = new ArrayList<>();
            for (Relationship rel : rels) {
                if (endCross.getId() == rel.getEndNode().getId()) {
                    relsStart2End.add(rel);
                }
            }
            boolean hasInner = false;
            for (Relationship rel : relsStart2End) {
                int startTime = (int) rel.getProperty("start_time");
                int endTime = (int) rel.getProperty("end_time");
                int status = (int) rel.getProperty("status");
                int segCnt = (int) rel.getProperty("seg_cnt");
                int travelTime = (int) rel.getProperty("travel_time");
                if (en >= startTime && st <= endTime) {
                    rel.delete();
                    if (st >= startTime && en <= endTime) { // inner
                        hasInner = true;
                        // split into three parts : start ~ st - 1, st ~ en - 1, en ~ end
                        if (st > startTime) {
                            Relationship left = startCross.createRelationshipTo(endCross, Edge.REACH_TO);
                            left.setProperty("status", status);
                            left.setProperty("seg_cnt", segCnt);
                            left.setProperty("travel_time", travelTime);
                            left.setProperty("start_time", startTime);
                            left.setProperty("end_time", st - 1);
                        }
                        if (en > st) {
                            Relationship middle = startCross.createRelationshipTo(endCross, Edge.REACH_TO);
                            middle.setProperty("status", tx.getJamStatus());
                            middle.setProperty("seg_cnt", tx.getSegmentCount());
                            middle.setProperty("travel_time", tx.getTravelTime());
                            middle.setProperty("start_time", st);
                            middle.setProperty("end_time", en - 1);
                        }
                        if (endTime > en) {
                            Relationship right = startCross.createRelationshipTo(endCross, Edge.REACH_TO);
                            right.setProperty("status", status);
                            right.setProperty("seg_cnt", segCnt);
                            right.setProperty("travel_time", travelTime);
                            right.setProperty("start_time", en);
                            right.setProperty("end_time", endTime);
                        }
                        break;
                    }
                    if (st > startTime) { // left
                        Relationship left = startCross.createRelationshipTo(endCross, Edge.REACH_TO);
                        left.setProperty("status", status);
                        left.setProperty("seg_cnt", segCnt);
                        left.setProperty("travel_time", travelTime);
                        left.setProperty("start_time", startTime);
                        left.setProperty("end_time", st);
                    } else if (en < endTime) { // right
                        Relationship right = startCross.createRelationshipTo(endCross, Edge.REACH_TO);
                        right.setProperty("status", status);
                        right.setProperty("seg_cnt", segCnt);
                        right.setProperty("travel_time", travelTime);
                        right.setProperty("start_time", en);
                        right.setProperty("end_time", endTime);
                    } else { // middle
                        // nop
                    }
                }
            }
            if (!hasInner) {
                Relationship middle = startCross.createRelationshipTo(endCross, Edge.REACH_TO);
                middle.setProperty("status", tx.getJamStatus());
                middle.setProperty("seg_cnt", tx.getSegmentCount());
                middle.setProperty("travel_time", tx.getTravelTime());
                middle.setProperty("start_time", st);
                middle.setProperty("end_time", en);
            }
            t.success();
        }
        return new AbstractTransaction.Result();
    }

    private AbstractTransaction.Result execute(SnapshotQueryTx tx) {
        try (Transaction t = db.beginTx()) {
            List<Pair<String, Integer>> res = new ArrayList<>();
            int time = tx.getTimestamp();
            HashMap<Long, Boolean> isExists = new HashMap<>();
            // bottleneck. find all time edge. Is there other way lower complexity?
            for (Relationship rel : db.getAllRelationships()) {
                long rId = (long) rel.getProperty("road_node_id");
                Boolean ok = isExists.get(rId);
                if (ok != null && ok) continue;
                int startTime = (int) rel.getProperty("start_time");
                int endTime = (int) rel.getProperty("end_time");
                if (time >= startTime && time <= endTime) {
                    int v = (int) rel.getProperty(tx.getPropertyName());
                    String name = (String) db.getNodeById(rId).getProperty("name");
                    res.add(Pair.of(name, v));
                    isExists.put(rId, true);
                }
            }
            t.success();
            SnapshotQueryTx.Result result = new SnapshotQueryTx.Result();
            result.setRoadStatus(res);
            return result;
        }
    }

    private AbstractTransaction.Result execute(SnapshotAggrMaxTx tx) {
        try (Transaction t = db.beginTx()) {
            List<Pair<String, Integer>> res = new ArrayList<>();
            HashMap<String, Integer> tmp = new HashMap<>();
            int st = tx.getT0();
            int en = tx.getT1();
            for (Relationship rel : db.getAllRelationships()) {
                long rId = (long) rel.getProperty("road_node_id");
                int startTime = (int) rel.getProperty("start_time");
                int endTime = (int) rel.getProperty("end_time");
                if (en >= startTime && st <= endTime) {
                    String name = (String) db.getNodeById(rId).getProperty("name");
                    int newV = (int) rel.getProperty(tx.getP());
                    Integer v = tmp.get(name);
                    if (v == null || newV > v)
                        tmp.put(name, newV);
                }
            }
            t.success();
            tmp.forEach((k, v) -> res.add(Pair.of(k, v)));
            SnapshotAggrMaxTx.Result result = new SnapshotAggrMaxTx.Result();
            result.setRoadTravelTime(res);
            return result;
        }
    }

    private AbstractTransaction.Result execute(SnapshotAggrDurationTx tx) {
        try (Transaction t = db.beginTx()) {
            List<Triple<String, Integer, Integer>> res = new ArrayList<>();
            // id, property_value, duration
            HashMap<String, HashMap<Integer, Integer>> buffer = new HashMap<>();
            int t0 = tx.getT0();
            int t1 = tx.getT1();
            for (Relationship rel : db.getAllRelationships()) {
                int st = (int) rel.getProperty("start_time");
                int en = (int) rel.getProperty("end_time");
                if (t1 >= st && t0 <= en) {
                    long rId = (long) rel.getProperty("road_node_id");
                    String name = (String) db.getNodeById(rId).getProperty("name");
                    int value = (int) rel.getProperty(tx.getP());
                    int duration;
                    if (t0 >= st && t1 <= en) { // inner
                        duration = t1 - t0;
                    } else if (t0 >= st) { // left
                        duration = en - t0 + 1;
                    } else if (t1 >= en) { // middle
                        if (t1 == en) duration = t1 - st;
                        else duration = en - st + 1;
                    } else { // right
                        duration = t1 - st;
                    }
                    HashMap<Integer, Integer> tmp = buffer.get(name);
                    if (tmp != null) {
                        Integer d = tmp.get(value);
                        d = (d != null ? d : 0);
                        tmp.put(value, d + duration);
                    } else {
                        tmp = new HashMap<>();
                        tmp.put(value, duration);
                    }
                    buffer.put(name, tmp);
                }
            }
            t.success();
            buffer.forEach((key, value) -> value.forEach((k, v) -> res.add(Triple.of(key, k, v))));
            SnapshotAggrDurationTx.Result result = new SnapshotAggrDurationTx.Result();
            result.setRoadStatDuration(res);
            return result;
        }
    }

    private AbstractTransaction.Result execute(EntityTemporalConditionTx tx) {
        try (Transaction t = db.beginTx()) {
            int st = tx.getT0();
            int en = tx.getT1();
            int vMin = tx.getVMin();
            int vMax = tx.getVMax();
            ArrayList<String> res = new ArrayList<>();
            HashMap<Long, Boolean> visited = new HashMap<>();
            for (Relationship rel : db.getAllRelationships()) {
                long rId = (long) rel.getProperty("road_node_id");
                Boolean ok = visited.get(rId);
                if (ok != null && ok) continue;
                int startTime = (int) rel.getProperty("start_time");
                int endTime = (int) rel.getProperty("end_time");
                int v = (int) rel.getProperty(tx.getP());
                if (en >= startTime && st <= endTime && v >= vMin && v <= vMax) {
                    res.add((String) db.getNodeById(rId).getProperty("name"));
                    visited.put(rId, true);
                }
            }
            t.success();
            EntityTemporalConditionTx.Result result = new EntityTemporalConditionTx.Result();
            result.setRoads(res);
            return result;
        }
    }

    private AbstractTransaction.Result execute(NodeNeighborRoadTx tx) {
        try (Transaction t = db.beginTx()) {
            long crossId = tx.getNodeId();
            List<String> res = new ArrayList<>();
            ResourceIterator<Node> roads = db.findNodes(Label.label("Road"));
            for (; roads.hasNext(); ) {
                Node road = roads.next();
                if ((int) road.getProperty("start_cross_id") == crossId || (int) road.getProperty("end_cross_id") == crossId) {
                    res.add((String) road.getProperty("name"));
                }
            }
            t.success();
            NodeNeighborRoadTx.Result result = new NodeNeighborRoadTx.Result();
            result.setRoadIds(res);
            return result;
        }
    }

    private AbstractTransaction.Result execute(ReachableAreaQueryTx tx) {
        return GeneralizedDijkstra.solve(tx, new Neo4j2Topology(db));
    }

    private static class Neo4j2Topology implements Topology {

        private final GraphDatabaseService db;

        Neo4j2Topology(GraphDatabaseService db) {
            this.db = db;
        }

        @Override
        public ArrayList<Pair<Long, Long>> getRoadList(long curCross) {
            ArrayList<Pair<Long, Long>> ret = new ArrayList<>();
            try (Transaction tx = db.beginTx()) {
                ResourceIterator<Node> roads = db.findNodes(Label.label("Road"), "start_cross_id", curCross);
                while (roads.hasNext()) {
                    Node road = roads.next();
                    ret.add(Pair.of(road.getId(), (Long) road.getProperty("end_cross_id")));
                }
            }
            return ret;
        }

        @Override
        public int earliestArriveTime(long roadId, int when, int until) {
            int ret = Integer.MAX_VALUE;
            try (Transaction tx = db.beginTx()) {
                Node road = db.getNodeById(roadId);
                String p = (String) road.getProperty("relationships");
                String[] rels = p.substring(1).split("|");
                int nextWhen = when;
                boolean inner = true;
                for (String id : rels) {
                    Relationship rel = db.getRelationshipById(Long.parseLong(id));
                    int stTime = (int) rel.getProperty("start_time");
                    int enTime = (int) rel.getProperty("end_time");
                    int travelTime = (int) rel.getProperty("travel_time");
                    if (stTime <= until && enTime >= when) {
                        if (inner && when >= stTime && until <= enTime) // inner.
                            return when + travelTime <= until ? when + travelTime : Integer.MAX_VALUE;
                        inner = false;
                        if (stTime > when) nextWhen = stTime; // middle and right.
                        int cur = nextWhen + travelTime;
                        if (cur <= enTime && cur <= until && cur < ret) ret = cur;
                    }
                }
            }
            return ret;
        }
    }
}
