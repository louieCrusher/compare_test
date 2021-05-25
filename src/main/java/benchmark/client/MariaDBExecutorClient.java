package benchmark.client;

import benchmark.utils.*;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import benchmark.transaction.definition.*;
import benchmark.model.StatusUpdate;
import scala.Tuple3;
import scala.Tuple6;

import java.io.IOException;
import java.sql.*;
import java.util.*;
import java.util.concurrent.*;

/**
 * Created by crusher. 2020.10.14
 */
public class MariaDBExecutorClient implements DBProxy {

    private ThreadPoolExecutor exe;
    private BlockingQueue<Connection> connectionPool = new LinkedBlockingQueue<>();
    private ListeningExecutorService service;
    private Map<Connection, Integer> connIdMap = new HashMap<>();
    private HashMap<String, Long> name2Id = new HashMap<>();
    private HashMap<Long, String> id2Name = new HashMap<>();
    // pair(id, (r_id, st_time, en_time, status, travel_t, seg_cnt))
    private ArrayList<Tuple6<Long, Long, Long, Integer, Integer, Integer>> buffer = new ArrayList<>();
    // pair(rid, (r_id, st_time, en_time, status, travel_t, seg_cnt))
    private HashMap<Long, Tuple6<Long, Long, Long, Integer, Integer, Integer>> lastRIdBuffer = new HashMap<>();
    private ExecutorService threadPool;

    public MariaDBExecutorClient(String serverHost, int parallelCnt, int queueLength) throws IOException, ClassNotFoundException, SQLException, InterruptedException {
        Class.forName("org.mariadb.jdbc.Driver");
        String initURL = "jdbc:mariadb://" + serverHost + ":3316";
        String dbURL = initURL + "/beijing_traffic?rewriteBatchedStatements=true";
        Connection initConn = DriverManager.getConnection(initURL, "root", "root");
        initDB(initConn);
        initConn.close();
        this.exe = new ThreadPoolExecutor(parallelCnt, parallelCnt, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>(queueLength), (r, executor) -> {
            if (!executor.isShutdown()) {
                try {
                    executor.getQueue().put(r);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });
        threadPool = Executors.newFixedThreadPool(10);
        exe.prestartAllCoreThreads();
        this.service = MoreExecutors.listeningDecorator(exe);
        for (int i = 0; i < parallelCnt; i++) {
            Connection conn = DriverManager.getConnection(dbURL, "root", "root");
            this.connectionPool.offer(conn);
            connIdMap.put(conn, i);
        }
        buildMapBetweenIdAndName();
    }

    private void initDB(Connection conn) {
        try (Statement stmt = conn.createStatement()) {
            conn.setAutoCommit(true);
            ResultSet rs = stmt.executeQuery("SELECT * FROM information_schema.SCHEMATA WHERE SCHEMA_NAME = 'beijing_traffic'");
            if (rs.next()) return;
            stmt.execute("CREATE DATABASE beijing_traffic");
            stmt.execute("USE beijing_traffic");
            stmt.execute("CREATE table cross_node(cross_id bigint PRIMARY KEY, name varchar(255))");
            stmt.execute("CREATE table road(r_id bigint primary key, r_address CHAR(12), r_start bigint, r_end bigint, r_length INT, r_type INT)");
            stmt.execute("CREATE table temporal_status(ts_id bigint PRIMARY KEY AUTO_INCREMENT, st_time TIMESTAMP(0), en_time TIMESTAMP(0), r_id bigint, status INT, travel_t INT, seg_cnt INT, PERIOD FOR time_period(st_time, en_time))");
            // time would be [st_time, en_time]
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Override
    public String testServerClientCompatibility() throws UnsupportedOperationException {
        return null;
    }

    @Override
    public ListenableFuture<ServerResponse> execute(AbstractTransaction tx) {
        switch (tx.getTxType()) {
            case TX_IMPORT_STATIC_DATA:
                return this.service.submit(execute((ImportStaticDataTx) tx));
            case TX_ENTITY_TEMPORAL_CONDITION_QUERY:
                return this.service.submit(execute((EntityTemporalConditionTx) tx));
            case TX_QUERY_NODE_NEIGHBOR_ROAD:
                return this.service.submit(execute((NodeNeighborRoadTx) tx));
            case TX_QUERY_SNAPSHOT:
                return this.service.submit(execute((SnapshotQueryTx) tx));
            case TX_UPDATE_TEMPORAL_DATA:
                return this.service.submit(execute((UpdateTemporalDataTx) tx));
            case TX_QUERY_SNAPSHOT_AGGR_MAX:
                return this.service.submit(execute((SnapshotAggrMaxTx) tx));
            case TX_QUERY_SNAPSHOT_AGGR_DURATION:
                return this.service.submit(execute((SnapshotAggrDurationTx) tx));
            case TX_QUERY_REACHABLE_AREA:
                return this.service.submit(execute((ReachableAreaQueryTx) tx));
            default:
                throw new UnsupportedOperationException();
        }
    }

    @Override
    public void createDB() throws IOException {
    }


    public void createIndexes() throws InterruptedException {
        Connection conn = connectionPool.take();

        try (PreparedStatement stmt0 = conn.prepareStatement("create index ind_rid_st_en on temporal_status(r_id, st_time, en_time)")) {
            conn.setAutoCommit(false);
            stmt0.execute();
            conn.commit();
        } catch (SQLException throwable) {
            throwable.printStackTrace();
        } finally {
            connectionPool.put(conn);
        }
    }

    @Override
    public void restartDB() throws IOException {
    }

    @Override
    public void shutdownDB() throws IOException {
    }

    private void buildMapBetweenIdAndName() throws InterruptedException {
        Connection conn = connectionPool.take();
        buildMapBetweenIdAndName(conn);
        connectionPool.put(conn);
        System.out.println("Build the map!");
    }

    private void buildMapBetweenIdAndName(Connection conn) {
        try (PreparedStatement stmt0 = conn.prepareStatement("select r_id, r_address from road")) {
            ResultSet rs = stmt0.executeQuery();
            while (rs.next()) {
                Long id = rs.getLong("r_id");
                String name = rs.getString("r_address");
                id2Name.put(id, name);
                name2Id.put(name, id);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void close() throws IOException, InterruptedException {
        try {
            service.shutdown();
            while (!service.isTerminated()) {
                service.awaitTermination(10, TimeUnit.SECONDS);
                long completeCnt = exe.getCompletedTaskCount();
                int remains = exe.getQueue().size();
                System.out.println(completeCnt + " / " + (completeCnt + remains) + " query completed.");
            }
            while (!connectionPool.isEmpty()) {
                Connection conn = connectionPool.take();
                conn.close();
            }
            System.out.println("Client exit. send " + exe.getCompletedTaskCount() + " lines.");
        } catch (SQLException e) {
            e.printStackTrace();
            throw new IOException(e);
        }
    }

    private Callable<ServerResponse> execute(ImportStaticDataTx tx) {
        return new Req() {
            @Override
            protected AbstractTransaction.Result executeQuery(Connection conn) throws Exception {
                try (PreparedStatement stat1 = conn.prepareStatement("INSERT INTO cross_node VALUES (?, ?)");
                     PreparedStatement stat2 = conn.prepareStatement("INSERT INTO road VALUES (?, ?, ?, ?, ?, ?)")) {
                    conn.setAutoCommit(false);
                    for (ImportStaticDataTx.StaticCrossNode p : tx.getCrosses()) {
                        stat1.setLong(1, p.getId());
                        stat1.setString(2, p.getName());
                        stat1.addBatch();
                    }
                    stat1.executeBatch();

                    for (ImportStaticDataTx.StaticRoadRel r : tx.getRoads()) {
                        stat2.setLong(1, r.getRoadId());
                        stat2.setString(2, r.getId());
                        stat2.setLong(3, r.getStartCrossId());
                        stat2.setLong(4, r.getEndCrossId());
                        stat2.setInt(5, r.getLength());
                        stat2.setInt(6, r.getType());
                        stat2.addBatch();
                    }
                    stat2.executeBatch();
                    conn.commit();
                } catch (SQLException e) {
                    e.printStackTrace();
                } finally {
                    conn.setAutoCommit(true);
                }
                return new AbstractTransaction.Result();
            }
        };
    }

    public void updateBuffer(ImportTemporalDataTx tx) {
        for (StatusUpdate s : tx.data) {
            Long rId = name2Id.get(s.getRoadId());
            if (rId == null) {
                System.out.println("Strange case occurred:");
                System.out.println("name " + s.getRoadId() + " did not be present at road table.");
                continue;
            }
            Tuple6<Long, Long, Long, Integer, Integer, Integer> dataOfLastSameRid = lastRIdBuffer.get(rId);
            if (dataOfLastSameRid != null) {
                buffer.add(Tuple6.apply(dataOfLastSameRid._1(), dataOfLastSameRid._2(), (long) s.getTime() - 1L, dataOfLastSameRid._4(), dataOfLastSameRid._5(), dataOfLastSameRid._6()));
            }
            lastRIdBuffer.put(rId, Tuple6.apply(rId, (long) s.getTime(), 2145801600L, s.getJamStatus(), s.getTravelTime(), s.getSegmentCount()));
        }
    }

    public void emptyLastRIdBuffer() {
        // ArrayList<Tuple6<Long, Long, Long, Integer, Integer, Integer>> vals = new ArrayList<Tuple6<Long, Long, Long, Integer, Integer, Integer>>(lastRIdBuffer.values());
        for (Tuple6<Long, Long, Long, Integer, Integer, Integer> val : lastRIdBuffer.values()) {
            buffer.add(val);
        }
    }

    public void writeTemporalToDisk() throws SQLException, InterruptedException {
        Connection conn = connectionPool.take();
        try (PreparedStatement stmt0 = conn.prepareStatement("insert into temporal_status(r_id, st_time, en_time, status, travel_t, seg_cnt) values(?, ?, ?, ?, ?, ?)")) {
            conn.setAutoCommit(false);
            int cnt = 0;
            for (Tuple6<Long, Long, Long, Integer, Integer, Integer> val : buffer) {
                stmt0.setLong(1, val._1());
                stmt0.setTimestamp(2, new Timestamp(val._2() * 1000L));
                stmt0.setTimestamp(3, new Timestamp(val._3() * 1000L));
                stmt0.setInt(4, val._4());
                stmt0.setInt(5, val._5());
                stmt0.setInt(6, val._6());
                stmt0.addBatch();
                ++cnt;
                if (cnt % 1_000_000 == 0) {
                    stmt0.executeBatch();
                    conn.commit();
                }
            }
            stmt0.executeBatch();
            conn.commit();
            buffer.clear();
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            conn.setAutoCommit(true);
            connectionPool.put(conn);
        }
    }

    public int getSize() {
        return buffer.size();
    }

    public void printSize() {
        if (buffer.size() != 0) {
            System.out.println("buffer_size: " + buffer.size());
        }
    }

    public void printTheLastSize() {
        System.out.println("the last buffer size: " + buffer.size());
    }

    private Callable<ServerResponse> execute(UpdateTemporalDataTx tx) {
        return new Req() {
            @Override
            protected AbstractTransaction.Result executeQuery(Connection conn) throws Exception {
                conn.setAutoCommit(false);
                HashMap<Long, Timestamp> mp = new HashMap<>();
                long roadId = name2Id.get(tx.getRoadId());
                try (PreparedStatement stmt0 = conn.prepareStatement("select ts_id, st_time from temporal_status where r_id = ? and st_time <= ? and en_time >= ?");
                     PreparedStatement stmt1 = conn.prepareStatement("insert into temporal_status(st_time, en_time, r_id, status, travel_t, seg_cnt) values(?, ?, ?, ?, ?, ?)");
                     PreparedStatement stmt2 = conn.prepareStatement("delete from temporal_status where ts_id = ?");
                     PreparedStatement stmt3 = conn.prepareStatement("select en_time, status, travel_t, seg_cnt from temporal_status where ts_id = ?")) {
                    stmt0.setLong(1, roadId);
                    stmt0.setTimestamp(2, new Timestamp((long) tx.getEndTime() * 1000l));
                    stmt0.setTimestamp(3, new Timestamp((long) tx.getStartTime() * 1000l));
                    ResultSet rs = stmt0.executeQuery();
                    while (rs.next()) {
                        mp.put(rs.getLong("ts_id"), rs.getTimestamp("st_time"));
                    }
                    long mx = -1, mn = Long.MAX_VALUE;
                    long mxId = -1, mnId = -1;
                    for (HashMap.Entry<Long, Timestamp> entry : mp.entrySet()) {
                        long id = entry.getKey();
                        long time = entry.getValue().getTime();
                        if (time > mx) {
                            mx = time;
                            mxId = id;
                        }
                        if (time < mn) {
                            mn = time;
                            mnId = id;
                        }
                    }
                    // insert the new value.
                    stmt1.setTimestamp(1, new Timestamp(tx.getStartTime() * 1000L));
                    stmt1.setTimestamp(2, new Timestamp(tx.getEndTime() * 1000L));
                    stmt1.setLong(3, roadId);
                    stmt1.setInt(4, tx.getJamStatus());
                    stmt1.setInt(5, tx.getTravelTime());
                    stmt1.setInt(6, tx.getSegmentCount());
                    stmt1.execute();
                    // remain the old value in first and last range.
                    if (mn <= (tx.getStartTime() - 1) * 1000L) {
                        stmt3.setLong(1, mnId);
                        rs = stmt3.executeQuery();
                        if (rs.next()) {
                            stmt1.setTimestamp(1, new Timestamp(mn));
                            stmt1.setTimestamp(2, new Timestamp((tx.getStartTime() - 1) * 1000l));
                            stmt1.setLong(3, roadId);
                            stmt1.setInt(4, rs.getInt("status"));
                            stmt1.setInt(5, rs.getInt("travel_t"));
                            stmt1.setInt(6, rs.getInt("seg_cnt"));
                            stmt1.execute();
                        }
                    }
                    stmt3.setLong(1, mxId);
                    rs = stmt3.executeQuery();
                    if (rs.next()) {
                        Timestamp en = rs.getTimestamp("en_time");
                        if ((tx.getEndTime() + 1) * 1000L <= en.getTime()) {
                            stmt1.setTimestamp(1, new Timestamp((tx.getEndTime() + 1) * 1000l));
                            stmt1.setTimestamp(2, en);
                            stmt1.setLong(3, roadId);
                            stmt1.setInt(4, rs.getInt("status"));
                            stmt1.setInt(5, rs.getInt("travel_t"));
                            stmt1.setInt(6, rs.getInt("seg_cnt"));
                            stmt1.execute();
                            System.out.println("insert the last range.");
                        }
                    }

                    // delete the old value.
                    for (Long id : mp.keySet()) {
                        stmt2.setLong(1, id);
                        stmt2.addBatch();
                    }
                    stmt2.executeBatch();
                    conn.commit();
                } catch (SQLException e) {
                    e.printStackTrace();
                } finally {
                    conn.setAutoCommit(true);
                }
                return new AbstractTransaction.Result();
            }
        };
    }

    private ArrayList<Long> getRoadIdList(Connection conn) throws SQLException {
        ArrayList<Long> res = new ArrayList<>();
        try (PreparedStatement stat = conn.prepareStatement("select r_id from road where 1 = 1")) {
            ResultSet rs = stat.executeQuery();
            while (rs.next()) {
                res.add(rs.getLong("r_id"));
            }
        }
        return res;
    }


    private Callable<ServerResponse> execute(SnapshotQueryTx tx) {
        return new Req() {
            @Override
            protected AbstractTransaction.Result executeQuery(Connection conn) throws Exception {
                List<Pair<String, Integer>> res = new ArrayList<>();
                List<Long> id = getRoadIdList(conn);
                String sql = "select " + tx.getPropertyName() + " as property from temporal_status where ts_id = (" +
                        "select ts_id from temporal_status where r_id = ? and \"" + new Timestamp((long) tx.getTimestamp() * 1000l) +
                        "\" between st_time and en_time)";
                System.out.println(sql);
                for (long rId : id) {
                    try (PreparedStatement stat = conn.prepareStatement(sql)) {
                        stat.setLong(1, rId);
                        ResultSet rs = stat.executeQuery();
                        if (rs.next()) {
                            res.add(Pair.of(id2Name.get(rId), rs.getInt("property")));
                        }
                    }
                }
                SnapshotQueryTx.Result result = new SnapshotQueryTx.Result();
                result.setRoadStatus(res);
                return result;
            }

        };
    }


    private Callable<ServerResponse> execute(SnapshotAggrMaxTx tx) {
        return new Req() {
            @Override
            protected AbstractTransaction.Result executeQuery(Connection conn) throws Exception {
                List<Pair<String, Integer>> res = new ArrayList<>();
                String sql = "select r_id, max(" + tx.getP() + ") as max_p from temporal_status where ts_id in (select ts_id from temporal_status where r_id = ? " +
                        "and st_time <= \"" + new Timestamp((long) tx.getT1() * 1000l) + "\" and en_time >= \"" + new Timestamp((long) tx.getT0() * 1000l) + "\") group by r_id";
                System.out.println(sql);
                ArrayList<Long> id = getRoadIdList(conn);
                for (long rId : id) {
                    try (PreparedStatement stat = conn.prepareStatement(sql)) {
                        conn.setAutoCommit(false);
                        stat.setLong(1, rId);
                        ResultSet rs = stat.executeQuery();
                        if (rs.next()) {
                            res.add(Pair.of(id2Name.get(rId), rs.getInt("max_p")));
                        }
                    } catch (SQLException e) {
                        e.printStackTrace();
                    }
                }
                SnapshotAggrMaxTx.Result result = new SnapshotAggrMaxTx.Result();
                result.setRoadTravelTime(res);
                return result;
            }
        };
    }

    private Callable<ServerResponse> execute(SnapshotAggrDurationTx tx) {
        return new Req() {
            @Override
            protected AbstractTransaction.Result executeQuery(Connection conn) throws Exception {
                List<Triple<String, Integer, Integer>> res = new ArrayList<>();
                ArrayList<Long> id = getRoadIdList(conn);
                int t0 = tx.getT0(), t1 = tx.getT1();
                String sql = "select " + tx.getP() + " as property, st_time, en_time from temporal_status where ts_id in " +
                        "(select ts_id from temporal_status where r_id = ? and st_time <= \"" +
                        new Timestamp((long) t1 * 1000l) + "\" and en_time >= \"" +
                        new Timestamp((long) t0 * 1000l) + "\") order by property";
                System.out.println(sql);
                for (long rId : id) {
                    try (PreparedStatement stat = conn.prepareStatement(sql)) {
                        stat.setLong(1, rId);
                        ResultSet rs = stat.executeQuery();
                        int lastValue = -1, total = 0;
                        String rName = id2Name.get(rId);
                        while (rs.next()) {
                            int value = rs.getInt("property");
                            int st = (int) (rs.getTimestamp("st_time").getTime() / 1000l);
                            int en = (int) (rs.getTimestamp("en_time").getTime() / 1000l);
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
                            if (lastValue == -1 || value == lastValue) {
                                total += duration;
                            } else {
                                res.add(Triple.of(rName, lastValue, total));
                                total = duration;
                            }
                            lastValue = value;
                        }
                        if (lastValue != -1) res.add(Triple.of(rName, lastValue, total));
                    }
                }
                SnapshotAggrDurationTx.Result result = new SnapshotAggrDurationTx.Result();
                result.setRoadStatDuration(res);
                return result;
            }
        };
    }

    private Callable<ServerResponse> execute(EntityTemporalConditionTx tx) {
        return new Req() {
            @Override
            protected AbstractTransaction.Result executeQuery(Connection conn) throws Exception {
                List<String> res = new ArrayList<>();
                ArrayList<Long> id = getRoadIdList(conn);
                String sql = "select ts_id from temporal_status where r_id = ? and st_time <= \"" + new Timestamp((long) tx.getT1() * 1000l) +
                        "\" and en_time >= \"" + new Timestamp((long) tx.getT0() * 1000l) + "\" and " + tx.getP() +
                        " between " + tx.getVMin() + " and " + tx.getVMax() + " limit 1";
                System.out.println(sql);
                for (long rId : id) {
                    try (PreparedStatement stat = conn.prepareStatement(sql)) {
                        stat.setLong(1, rId);
                        ResultSet rs = stat.executeQuery();
                        if (rs.next()) {
                            res.add(id2Name.get(rId));
                        }
                    }
                }

                EntityTemporalConditionTx.Result result = new EntityTemporalConditionTx.Result();
                result.setRoads(res);
                return result;
            }
        };
    }

    private Callable<ServerResponse> execute(NodeNeighborRoadTx tx) {
        return new Req() {
            @Override
            protected AbstractTransaction.Result executeQuery(Connection conn) throws Exception {
                List<String> res = new ArrayList<>();
                try (Statement stat = conn.createStatement()) {
                    conn.setAutoCommit(true);
                    String sql = "select r_name from road where r_start = " + tx.getNodeId() + " or r_end = " + tx.getNodeId();
                    ResultSet rs = stat.executeQuery(sql);
                    while (rs.next()) {
                        res.add(rs.getString("r_name"));
                    }
                } catch (SQLException e) {
                    e.printStackTrace();
                }
                NodeNeighborRoadTx.Result result = new NodeNeighborRoadTx.Result();
                result.setRoadIds(res);
                metrics.setReturnSize(res.size());
                return result;
            }
        };
    }


    private Callable<ServerResponse> execute(ReachableAreaQueryTx tx) {
        return new Req() {
            @Override
            protected AbstractTransaction.Result executeQuery(Connection conn) throws Exception {
                return GeneralizedDijkstra.solve(tx, new MariaDBTopology(conn));
            }
        };
    }

    private static class MariaDBTopology implements Topology {

        private final Connection conn;

        MariaDBTopology(Connection conn) {
            this.conn = conn;
        }

        @Override
        public ArrayList<Pair<Long, Long>> getRoadList(long curCross) {
            ArrayList<Pair<Long, Long>> ret = new ArrayList<>();
            try (PreparedStatement stmt = conn.prepareStatement("select r_id, r_end from road where r_start = ?")) {
                stmt.setLong(1, curCross);
                ResultSet rs = stmt.executeQuery();
                while (rs.next()) ret.add(Pair.of(rs.getLong("r_id"), rs.getLong("r_end")));
            } catch (SQLException e) {
                e.printStackTrace();
            }
            return ret;
        }

        @Override
        public int earliestArriveTime(long roadId, int when, int until) {
            int ret = Integer.MAX_VALUE;
            try (PreparedStatement stmt = conn.prepareStatement("select st_time, en_time, travel_t from temporal_status where r_id = ? and st_time <= ? and en_time >= ?")) {
                stmt.setLong(1, roadId);
                stmt.setTimestamp(2, new Timestamp(until * 1000L));
                stmt.setTimestamp(3, new Timestamp(when * 1000L));
                ResultSet rs = stmt.executeQuery();
                ArrayList<Tuple3<Timestamp, Timestamp, Integer>> l = new ArrayList<>();
                while (rs.next())
                    l.add(Tuple3.apply(rs.getTimestamp("st_time"), rs.getTimestamp("en_time"), rs.getInt("travel_t")));
                int nextWhen = when;
                for (Tuple3<Timestamp, Timestamp, Integer> ele : l) {
                    int stTime = (int) (ele._1().getTime() / 1000L);
                    int enTime = (int) (ele._2().getTime() / 1000L);
                    int travelTime = ele._3();
                    if (when <= enTime && until <= enTime) // inner.
                        return when + travelTime <= until ? when + travelTime : Integer.MAX_VALUE;
                    if (stTime > when) nextWhen = stTime; // middle and right.
                    int cur = nextWhen + travelTime;
                    if (cur <= enTime && cur <= until && cur < ret) ret = cur;
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
            return ret;
        }
    }


    private abstract class Req implements Callable<ServerResponse> {
        private final TimeMonitor timeMonitor = new TimeMonitor();
        final AbstractTransaction.Metrics metrics = new AbstractTransaction.Metrics();

        private Req() {
            timeMonitor.begin("Wait in queue");
        }

        @Override
        public ServerResponse call() throws Exception {
            try {
                Connection conn = connectionPool.take();
                timeMonitor.mark("Wait in queue", "query");
                AbstractTransaction.Result result = executeQuery(conn);
                timeMonitor.end("query");
                if (result == null) throw new RuntimeException("[Got null. Server close connection]");
                connectionPool.put(conn);
                metrics.setWaitTime(Math.toIntExact(timeMonitor.duration("Wait in queue")));
                metrics.setSendTime(timeMonitor.beginT("query"));
                metrics.setExeTime(Math.toIntExact(timeMonitor.duration("query")));
                metrics.setConnId(connIdMap.get(conn));
                ServerResponse response = new ServerResponse();
                response.setMetrics(metrics);
                response.setResult(result);
                return response;
            } catch (Exception e) {
                e.printStackTrace();
                throw e;
            }
        }

        protected abstract AbstractTransaction.Result executeQuery(Connection conn) throws Exception;
    }
}

