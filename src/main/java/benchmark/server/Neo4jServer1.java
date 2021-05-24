package benchmark.server;


import benchmark.model.ReachableCrossNode;
import benchmark.model.StatusUpdate;
import benchmark.transaction.definition.*;
import benchmark.utils.*;
import com.alibaba.fastjson.JSON;
import com.google.common.base.Preconditions;
import org.neo4j.graphdb.*;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.graphdb.schema.Schema;
import scala.Tuple4;

import java.io.File;
import java.io.IOException;
import java.util.*;

public class Neo4jServer1 extends Neo4jSocketServer.ReqExecutor {

    private HashMap<String, Long> name2Id = new HashMap<>();

    public static void main(String[] args) {
        Neo4jSocketServer server = new Neo4jSocketServer(dbDir(), new Neo4jServer1());
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
        TIME_FROM_START_TO_END, REACH_TO, IS_START_CROSS, IS_END_CROSS
    }

    @Override
    protected void setDB(GraphDatabaseService db) {
        this.db = db;
        try (Transaction tx = db.beginTx()) {
            Schema schema = db.schema();
            boolean hasRoadIndex = false;
            boolean hasTimeIndex = false;
            try {
                IndexDefinition roadIndex = schema.getIndexByName("ind_on_road_name");
                hasRoadIndex = true;
                System.out.println("Has the road index already.");
            } catch (IllegalArgumentException e) {
                // There is no this index.
            }
            try {
                IndexDefinition timeIndex = schema.getIndexByName("ind_on_time_st_en_time");
                hasTimeIndex = true;
                System.out.println("Has the time index already.");
            } catch (IllegalArgumentException e) {
                // There is no this index
            }
            if (!hasRoadIndex) {
                schema.indexFor(Label.label("Road")).on("start_cross_id").create();
                schema.indexFor(Label.label("Road")).on("end_cross_id").create();
                schema.indexFor(Label.label("Road")).on("name").withName("ind_on_road_name").create();
                System.out.println("Build the road index.");
            }
            if (!hasTimeIndex) {
                schema.indexFor(Label.label("Time")).on("st_time").on("en_time").withName("ind_on_time_st_en_time").create();
                System.out.println("Build the time index.");
            }
            tx.success();
        }
    }

    private void initTable(GraphDatabaseService db) {
        try (Transaction tx = db.beginTx()) {
            buildMapBetweenNameAndId(db);
            System.out.println("Build the map!");
            tx.success();
        }
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

    private void buildMapBetweenNameAndId(GraphDatabaseService db) {
        try (Transaction tx = db.beginTx()) {
            ResourceIterator<Node> allRoadNodes = db.findNodes(Label.label("Road"));
            while (allRoadNodes.hasNext()) {
                Node road = allRoadNodes.next();
                name2Id.put((String) road.getProperty("name"), road.getId());
            }
            tx.success();
        }
    }

    private AbstractTransaction.Result execute(ImportStaticDataTx tx) {
        try (Transaction t = db.beginTx()) {
            for (ImportStaticDataTx.StaticRoadRel road : tx.getRoads()) {
                Node node = db.createNode(Label.label("Road"));
                node.setProperty("name", road.getId());
                node.setProperty("r_id", road.getRoadId());
                node.setProperty("start_cross_id", road.getStartCrossId());
                node.setProperty("end_cross_id", road.getEndCrossId());
                node.setProperty("length", road.getLength());
                node.setProperty("type", road.getType());
                node.setProperty("last_time_node", -1L);
                node.setProperty("last_time_node_rel", -1L);
            }

            for (ImportStaticDataTx.StaticCrossNode cross : tx.getCrosses()) {
                long id = cross.getId();
                Label roadLabel = Label.label("Road");
                Label crossLabel = Label.label("Cross");
                // build relationship between a and b when a is able to reach b
                ResourceIterator<Node> endNodes = db.findNodes(roadLabel, "end_cross_id", id);
                ResourceIterator<Node> startNodes = db.findNodes(roadLabel, "start_cross_id", id);
                for (ResourceIterator<Node> i = endNodes; i.hasNext(); ) {
                    Node endNode = i.next();
                    for (ResourceIterator<Node> j = startNodes; j.hasNext(); ) {
                        Node startNode = j.next();
                        endNode.createRelationshipTo(startNode, Edge.REACH_TO);
                    }
                }
                // build cross nodes
                Node crossNode = db.createNode(crossLabel);
                crossNode.setProperty("id", cross.getId());
                crossNode.setProperty("name", cross.getName());
                for (ResourceIterator<Node> it = startNodes; it.hasNext(); ) {
                    crossNode.createRelationshipTo(it.next(), Edge.IS_START_CROSS);
                }
                for (ResourceIterator<Node> it = endNodes; it.hasNext(); ) {
                    crossNode.createRelationshipTo(it.next(), Edge.IS_END_CROSS);
                }
            }
            t.success();
            initTable(db);
        }
        return new AbstractTransaction.Result();
    }

    private Node getProperTimeNode(int startTime, int endTime) {
        Node ret;
        try (Transaction tx = db.beginTx()) {
            ResourceIterator<Node> nodes = db.findNodes(Label.label("Time"), "st_time", startTime, "en_time", endTime);
            if (nodes.hasNext()) {
                ret = nodes.next();
            } else {
                ret = db.createNode(Label.label("Time"));
                ret.setProperty("st_time", startTime);
                ret.setProperty("en_time", endTime);
            }
            tx.success();
        }
        return ret;
    }

    // Assume that data comes order by time. Thus, multithreading would not work.
    private AbstractTransaction.Result execute(ImportTemporalDataTx tx) {
        try (Transaction t = db.beginTx()) {
            for (StatusUpdate s : tx.getData()) {
                Long id = name2Id.get(s.getRoadId());
                if (id == null) {
                    System.out.println("Strange case occurred, road name " + s.getRoadId() + " did not be present at static road.");
                }
                if (id == null) continue;
                Node roadNode = db.getNodeById(id);
                long lastTimeNodeId = (long) roadNode.getProperty("last_time_node");
                if (lastTimeNodeId == -1) { // the first append to this road.
                    // find if exists [s.getTime, +inf] time node
                    // if exists, use it, else, create a new time node.
                    Node timeNode = getProperTimeNode(s.getTime(), Integer.MAX_VALUE);
                    Relationship rel = timeNode.createRelationshipTo(roadNode, Edge.TIME_FROM_START_TO_END);
                    rel.setProperty("status", s.getJamStatus());
                    rel.setProperty("travel_time", s.getTravelTime());
                    rel.setProperty("seg_cnt", s.getSegmentCount());
                    roadNode.setProperty("last_time_node", timeNode.getId());
                    roadNode.setProperty("last_time_node_rel", rel.getId());
                } else {
                    // get the last time node. [last_st, +inf]
                    // delete the relation between the time node and road node.
                    // find time nodes [last_st, s.getTime - 1] [s.getTime, +inf]
                    // if exist, use them, else, create two new time nodes.
                    Node lastTimeNode = db.getNodeById(lastTimeNodeId);
                    int startTime = (int) lastTimeNode.getProperty("st_time");
                    Relationship rel = db.getRelationshipById((long) roadNode.getProperty("last_time_node_rel"));
                    int status = (int) rel.getProperty("status");
                    int travelTime = (int) rel.getProperty("travel_time");
                    int segCnt = (int) rel.getProperty("seg_cnt");
                    rel.delete();
                    Node t1 = getProperTimeNode(startTime, s.getTime() - 1);
                    rel = t1.createRelationshipTo(roadNode, Edge.TIME_FROM_START_TO_END);
                    rel.setProperty("status", status);
                    rel.setProperty("travel_time", travelTime);
                    rel.setProperty("seg_cnt", segCnt);
                    Node t2 = getProperTimeNode(s.getTime(), Integer.MAX_VALUE);
                    rel = t2.createRelationshipTo(roadNode, Edge.TIME_FROM_START_TO_END);
                    rel.setProperty("status", s.getJamStatus());
                    rel.setProperty("travel_time", s.getTravelTime());
                    rel.setProperty("seg_cnt", s.getSegmentCount());
                    roadNode.setProperty("last_time_node", t2.getId());
                    roadNode.setProperty("last_time_node_rel", rel.getId());
                }
            }
            t.success();
        }
        return new AbstractTransaction.Result();
    }

    // Just delete the rel with time then create some new time nodes and connect it.
    private AbstractTransaction.Result execute(UpdateTemporalDataTx tx) {
        try (Transaction t = db.beginTx()) {
            String roadName = tx.getRoadId();
            Node road = db.findNode(Label.label("Road"), "name", roadName);
            Iterable<Relationship> timeRel = road.getRelationships(Direction.INCOMING, Edge.TIME_FROM_START_TO_END);
            Relationship minRel = null, maxRel = null;
            int minTime = Integer.MAX_VALUE, maxTime = Integer.MIN_VALUE;
            for (Relationship rel : timeRel) {
                Node endNode = rel.getEndNode();
                int st = (int) endNode.getProperty("st_time");
                int en = (int) endNode.getProperty("en_time");
                if (tx.getEndTime() >= st && tx.getStartTime() <= en) {
                    if (st < minTime) {
                        minRel = rel;
                        minTime = st;
                    }
                    if (st > maxTime) {
                        maxRel = rel;
                        maxTime = st;
                    }
                    rel.delete();
                }
            }
            Node timeNode1 = db.createNode(Label.label("Time"));
            timeNode1.setProperty("st_time", minRel.getEndNode().getProperty("st_time"));
            timeNode1.setProperty("en_time", tx.getStartTime() - 1);
            timeNode1.createRelationshipTo(road, Edge.TIME_FROM_START_TO_END);
            Node timeNode2 = db.createNode(Label.label("Time"));
            timeNode2.setProperty("st_time", tx.getStartTime());
            timeNode2.setProperty("en_time", tx.getEndTime() - 1);
            timeNode2.createRelationshipTo(road, Edge.TIME_FROM_START_TO_END);
            Node timeNode3 = db.createNode(Label.label("Time"));
            timeNode3.setProperty("st_time", tx.getEndTime());
            timeNode3.setProperty("en_time", maxRel.getEndNode().getProperty("en_time"));
            timeNode3.createRelationshipTo(road, Edge.TIME_FROM_START_TO_END);
            t.success();
        }
        return new AbstractTransaction.Result();
    }

    private AbstractTransaction.Result execute(SnapshotQueryTx tx) {
        try (Transaction t = db.beginTx()) {
            List<Pair<String, Integer>> res = new ArrayList<>();
            int time = tx.getTimestamp();
            ResourceIterator<Node> timeNodes = db.findNodes(Label.label("Time"));
            ArrayList<Node> cache = new ArrayList<>();
            while (timeNodes.hasNext()) {
                Node node = timeNodes.next();
                int st = (int) node.getProperty("st_time");
                int en = (int) node.getProperty("en_time");
                if (time >= st && time <= en) cache.add(node);
            }
            for (Node node : cache) {
                Iterable<Relationship> rels = node.getRelationships(Direction.OUTGOING, Edge.TIME_FROM_START_TO_END);
                for (Relationship rel : rels) {
                    res.add(Pair.of((String) rel.getEndNode().getProperty("name"), (int) rel.getProperty(tx.getPropertyName())));
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
            int t0 = tx.getT0(), t1 = tx.getT1();
            ResourceIterator<Node> timeNodes = db.findNodes(Label.label("Time"));
            ArrayList<Node> cache = new ArrayList<>();
            HashMap<String, Integer> ans = new HashMap<>();
            while (timeNodes.hasNext()) {
                Node node = timeNodes.next();
                int st = (int) node.getProperty("st_time");
                int en = (int) node.getProperty("en_time");
                if (t1 >= st && t0 <= en) {
                    cache.add(node);
                }
            }
            for (Node node : cache) {
                Iterable<Relationship> rels = node.getRelationships(Direction.OUTGOING, Edge.TIME_FROM_START_TO_END);
                for (Relationship rel : rels) {
                    ans.merge((String) rel.getEndNode().getProperty("name"), (int) rel.getProperty(tx.getP()), Integer::max);
                }
            }
            for (Map.Entry<String, Integer> entry : ans.entrySet()) {
                res.add(Pair.of(entry.getKey(), entry.getValue()));
            }
            t.success();
            SnapshotAggrMaxTx.Result result = new SnapshotAggrMaxTx.Result();
            result.setRoadTravelTime(res);
            return result;
        }
    }

    private AbstractTransaction.Result execute(SnapshotAggrDurationTx tx) {
        try (Transaction t = db.beginTx()) {
            List<Triple<String, Integer, Integer>> res = new ArrayList<>();
            int t0 = tx.getT0(), t1 = tx.getT1();
            ResourceIterator<Node> timeNodes = db.findNodes(Label.label("Time"));
            HashMap<String, HashMap<Integer, Integer>> ans = new HashMap<>();
            while (timeNodes.hasNext()) {
                Node node = timeNodes.next();
                int st = (int) node.getProperty("st_time");
                int en = (int) node.getProperty("en_time");
                if (t1 >= st && t0 <= en) {
                    Iterable<Relationship> rels = node.getRelationships(Direction.OUTGOING, Edge.TIME_FROM_START_TO_END);
                    int duration;
                    if (t0 >= st && t1 <= en) { // inner
                        duration = t1 - t0;
                    } else if (t0 >= st) { // left  t0 < st || t1 > en
                        // t1 > en => t1 >= en + 1.
                        duration = en - t0 + 1;
                    } else if (t1 >= en) { // middle t0 < st
                        if (t1 == en) duration = t1 - st;
                        else duration = en - st + 1;
                    } else { // right t0 < st && t1 < en
                        duration = t1 - st;
                    }
                    for (Relationship rel : rels) {
                        String rName = (String) rel.getEndNode().getProperty("name");
                        int property = (int) rel.getProperty(tx.getP());
                        HashMap<Integer, Integer> tmp = ans.get(rName);
                        if (tmp != null) {
                            tmp.merge(property, duration, Integer::sum);
                        } else {
                            tmp = new HashMap<>();
                            tmp.put(property, duration);
                        }
                        ans.put(rName, tmp);
                    }
                }
            }
            for (Map.Entry<String, HashMap<Integer, Integer>> entry : ans.entrySet()) {
                for (Map.Entry<Integer, Integer> e : entry.getValue().entrySet()) {
                    res.add(Triple.of(entry.getKey(), e.getKey(), e.getValue()));
                }
            }
            t.success();
            SnapshotAggrDurationTx.Result result = new SnapshotAggrDurationTx.Result();
            result.setRoadStatDuration(res);
            return result;
        }
    }

    private AbstractTransaction.Result execute(EntityTemporalConditionTx tx) {
        try (Transaction t = db.beginTx()) {
            int t0 = tx.getT0(), t1 = tx.getT1();
            int vMin = tx.getVMin(), vMax = tx.getVMax();
            ArrayList<String> res = new ArrayList<>();
            ResourceIterator<Node> timeNodes = db.findNodes(Label.label("Time"));
            HashMap<String, Boolean> visited = new HashMap<>();
            while (timeNodes.hasNext()) {
                Node node = timeNodes.next();
                int st = (int) node.getProperty("st_time");
                int en = (int) node.getProperty("en_time");
                if (t1 >= st && t0 <= en) {
                    Iterable<Relationship> rels = node.getRelationships(Direction.OUTGOING, Edge.TIME_FROM_START_TO_END);
                    for (Relationship rel : rels) {
                        int property = (int) rel.getProperty(tx.getP());
                        String rName = (String) rel.getEndNode().getProperty("name");
                        if ((visited.get(rName) == null || !visited.get(rName)) && property >= vMin && property <= vMax) {
                            res.add(rName);
                            visited.put(rName, true);
                        }
                    }
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
            long id = tx.getNodeId();
            Node cross = db.findNode(() -> "Cross", "id", id);
            Iterable<Relationship> start = cross.getRelationships(Edge.IS_START_CROSS, Direction.OUTGOING);
            Iterable<Relationship> end = cross.getRelationships(Edge.IS_END_CROSS, Direction.OUTGOING);
            List<String> res = new ArrayList<>();
            for (Relationship rel : start) {
                res.add((String) rel.getEndNode().getProperty("name"));
            }
            for (Relationship rel : end) {
                res.add((String) rel.getEndNode().getProperty("name"));
            }
            NodeNeighborRoadTx.Result result = new NodeNeighborRoadTx.Result();
            result.setRoadIds(res);
            t.success();
            return result;
        }
    }

    private AbstractTransaction.Result execute(ReachableAreaQueryTx tx) {
        return GeneralizedDijkstra.solve(tx, new Neo4j1Topology(db));
    }

    private static class Neo4j1Topology implements Topology {

        private final GraphDatabaseService db;

        Neo4j1Topology(GraphDatabaseService db) {
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
        public int earliestArriveTime(long roadId, int when, int until) { // roadId means road node id.
            int ret = Integer.MAX_VALUE;
            try (Transaction tx = db.beginTx()) {
                Node road = db.getNodeById(roadId);
                Iterable<Relationship> rels = road.getRelationships(Direction.INCOMING, Edge.TIME_FROM_START_TO_END);
                int nextWhen = when;
                boolean inner = true;
                for (Relationship rel : rels) {
                    int travelTime = (int) rel.getProperty("travel_time");
                    int stTime = (int) rel.getEndNode().getProperty("st_time");
                    int enTime = (int) rel.getEndNode().getProperty("en_time");
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
