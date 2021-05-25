package benchmark.client;

import benchmark.model.ReachableCrossNode;
import benchmark.utils.*;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import benchmark.transaction.definition.*;
import benchmark.model.StatusUpdate;
import scala.Int;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class PostgreSQLExecutorClient implements DBProxy {
    private ThreadPoolExecutor exe;
    private BlockingQueue<Connection> connectionPool = new LinkedBlockingQueue<>();
    private ListeningExecutorService service;
    private Map<Connection, Integer> connIdMap = new HashMap<>();
    private HashMap<Long, String> id2Name = new HashMap<>();
    private HashMap<String, Long> name2Id = new HashMap<>();

    public PostgreSQLExecutorClient(String serverHost, int parallelCnt, int queueLength) throws IOException, ClassNotFoundException, SQLException, InterruptedException {
        Class.forName("org.postgresql.Driver");
        String initURL = "jdbc:postgresql://" + serverHost + ":5432/";
        String dbURL = initURL + "beijing_traffic?reWriteBatchedInserts=true";
        Connection initConn = DriverManager.getConnection(initURL, "postgres", "root");
        createDB(initConn);
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
        exe.prestartAllCoreThreads();
        this.service = MoreExecutors.listeningDecorator(exe);
        for (int i = 0; i < parallelCnt; i++) {
            Connection conn = DriverManager.getConnection(dbURL, "postgres", "root");
            this.connectionPool.offer(conn);
            connIdMap.put(conn, i);
        }
        boolean hasTables = buildMapBetweenIdAndName();
        if (!hasTables) createTables();
    }

    private void createDB(Connection conn) {
        try (Statement stmt = conn.createStatement()) {
            conn.setAutoCommit(true);
            ResultSet rs = stmt.executeQuery("select database.datname from pg_catalog.pg_database database where database.datname = 'beijing_traffic'");
            if (rs.next()) return;
            stmt.execute("create database beijing_traffic");
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private void createTables() throws InterruptedException {
        Connection conn = connectionPool.take();
        try (Statement stmt = conn.createStatement()) {
            conn.setAutoCommit(true);
            stmt.execute("create table road(r_id bigint primary key, r_address char(12), r_start bigint, r_end bigint, r_length int, r_type int)");
            stmt.execute("create table cross_node(cross_id bigint primary key, cross_name text)");
            stmt.execute("create table temporal_status(ts_id serial not null, t int, r_id bigint, status int, travel_t int, seg_cnt int)");
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            connectionPool.put(conn);
        }
    }

    public void createIndexes() throws InterruptedException {
        Connection conn = connectionPool.take();
        try (Statement stmt = conn.createStatement()) {
            conn.setAutoCommit(true);
            stmt.execute("create index ind_id_t on temporal_status(r_id, t)");
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            connectionPool.put(conn);
        }
    }

    @Override
    public ListenableFuture<DBProxy.ServerResponse> execute(AbstractTransaction tx) {
        switch (tx.getTxType()) {
            case TX_IMPORT_STATIC_DATA:
                return this.service.submit(execute((ImportStaticDataTx) tx));
            case TX_IMPORT_TEMPORAL_DATA:
                return this.service.submit(execute((ImportTemporalDataTx) tx));
            case TX_QUERY_SNAPSHOT:
                return this.service.submit(execute((SnapshotQueryTx) tx));
            case TX_QUERY_SNAPSHOT_AGGR_MAX:
                return this.service.submit(execute((SnapshotAggrMaxTx) tx));
            case TX_QUERY_SNAPSHOT_AGGR_DURATION:
                return this.service.submit(execute((SnapshotAggrDurationTx) tx));
            case TX_ENTITY_TEMPORAL_CONDITION_QUERY:
                return this.service.submit(execute((EntityTemporalConditionTx) tx));
            case TX_QUERY_NODE_NEIGHBOR_ROAD:
                return this.service.submit(execute((NodeNeighborRoadTx) tx));
            case TX_UPDATE_TEMPORAL_DATA:
                return this.service.submit(execute((UpdateTemporalDataTx) tx));
            case TX_QUERY_REACHABLE_AREA:
                return this.service.submit(execute((ReachableAreaQueryTx) tx));
            default:
                throw new UnsupportedOperationException();
        }
    }

    @Override
    public void createDB() throws IOException {

    }

    @Override
    public void restartDB() throws IOException {

    }

    @Override
    public void shutdownDB() throws IOException {

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

    @Override
    public String testServerClientCompatibility() {
        return null;
    }

    private boolean buildMapBetweenIdAndName() throws InterruptedException {
        boolean ret = false;
        Connection conn = connectionPool.take();
        ret = buildMapBetweenIdAndName(conn);
        connectionPool.put(conn);
        return ret;
    }

    private boolean buildMapBetweenIdAndName(Connection conn) {
        try (PreparedStatement stmt0 = conn.prepareStatement("select r_id, r_address from road");
             PreparedStatement stmt1 = conn.prepareStatement("select relname from pg_class where relname = 'road'")) {
            ResultSet r = stmt1.executeQuery();
            if (!r.next()) return false;
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
        return true;
    }

    private Callable<DBProxy.ServerResponse> execute(ImportStaticDataTx tx) {
        return new Req() {
            @Override
            protected AbstractTransaction.Result executeQuery(Connection conn) throws Exception {
                try (PreparedStatement stmt0 = conn.prepareStatement("insert into cross_node values(?, ?)");
                     PreparedStatement stmt1 = conn.prepareStatement("insert into road values(?, ?, ?, ?, ?, ?)")) {
                    conn.setAutoCommit(false);
                    for (ImportStaticDataTx.StaticCrossNode cross : tx.getCrosses()) {
                        stmt0.setLong(1, cross.getId());
                        stmt0.setString(2, cross.getName());
                        stmt0.addBatch();
                    }
                    stmt0.executeBatch();
                    for (ImportStaticDataTx.StaticRoadRel road : tx.getRoads()) {
                        stmt1.setLong(1, road.getRoadId());
                        stmt1.setString(2, road.getId());
                        stmt1.setLong(3, road.getStartCrossId());
                        stmt1.setLong(4, road.getEndCrossId());
                        stmt1.setInt(5, road.getLength());
                        stmt1.setInt(6, road.getType());
                        stmt1.addBatch();
                    }
                    stmt1.executeBatch();
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

    private Callable<DBProxy.ServerResponse> execute(ImportTemporalDataTx tx) {
        return new Req() {
            @Override
            protected AbstractTransaction.Result executeQuery(Connection conn) throws Exception {
                try (PreparedStatement stmt = conn.prepareStatement("insert into temporal_status(r_id, t, status, travel_t, seg_cnt) values(?, ?, ?, ?, ?)")) {
                    conn.setAutoCommit(false);
                    for (StatusUpdate s : tx.data) {
                        Long id = name2Id.get(s.getRoadId());
                        if (id == null) {
                            System.out.println("Strange case occurred:");
                            System.out.println("name " + s.getRoadId() + " did not be present at road table.");
                        }
                        stmt.setLong(1, id);
                        stmt.setInt(2, s.getTime());
                        stmt.setInt(3, s.getJamStatus());
                        stmt.setInt(4, s.getTravelTime());
                        stmt.setInt(5, s.getSegmentCount());
                        stmt.addBatch();
                    }
                    stmt.executeBatch();
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

    private Callable<DBProxy.ServerResponse> execute(UpdateTemporalDataTx tx) {
        return new Req() {
            @Override
            protected AbstractTransaction.Result executeQuery(Connection conn) throws Exception {
                conn.setAutoCommit(false);
                Long roadId = name2Id.get(tx.getRoadId());
                try (PreparedStatement stmt1 = conn.prepareStatement("select r_id, max(t) as max_t from temporal_status where r_id = ? and t <= ? group by r_id");
                     PreparedStatement stmt2 = conn.prepareStatement("select status, travel_t, seg_cnt from temporal_status where r_id = ? and t = ?");
                     PreparedStatement stmt3 = conn.prepareStatement("insert into temporal_status(t, r_id, status, travel_t, seg_cnt) values(?, ?, ?, ?, ?)");
                     PreparedStatement stmt4 = conn.prepareStatement("delete from temporal_status where r_id = ? and t >= ? and t <= ?")) {
                    stmt1.setLong(1, roadId);
                    stmt1.setInt(2, tx.getEndTime());
                    ResultSet rs = stmt1.executeQuery();

                    // insert into table from t1 + 1 with the old value.
                    if (rs.next()) {
                        stmt2.setLong(1, rs.getLong("r_id"));
                        stmt2.setInt(2, rs.getInt("max_t"));
                        ResultSet r = stmt2.executeQuery();
                        if (r.next()) {
                            stmt3.setInt(1, tx.getEndTime() + 1);
                            stmt3.setLong(2, roadId);
                            stmt3.setInt(3, r.getInt("status"));
                            stmt3.setInt(4, r.getInt("travel_t"));
                            stmt3.setInt(5, r.getInt("seg_cnt"));
                            stmt3.execute();
                        }
                    }
                    // delete the [t0, t1] rows.
                    stmt4.setLong(1, roadId);
                    stmt4.setInt(2, tx.getStartTime());
                    stmt4.setInt(3, tx.getEndTime());
                    stmt4.execute();
                    // insert into table from t0 with the new value.
                    stmt3.setInt(1, tx.getStartTime());
                    stmt3.setLong(2, roadId);
                    stmt3.setInt(3, tx.getJamStatus());
                    stmt3.setInt(4, tx.getTravelTime());
                    stmt3.setInt(5, tx.getSegmentCount());
                    stmt3.execute();
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

    private Callable<DBProxy.ServerResponse> execute(SnapshotQueryTx tx) {
        return new Req() {
            @Override
            protected AbstractTransaction.Result executeQuery(Connection conn) throws Exception {
                List<Pair<String, Integer>> res = new ArrayList<>();
                try (PreparedStatement stmt0 = conn.prepareStatement("select r_id, max(t) as max_t from temporal_status where t <= ? group by r_id");
                     PreparedStatement stmt1 = conn.prepareStatement("select " + tx.getPropertyName() + " as property from temporal_status where r_id = ? and t = ?")) {
                    conn.setAutoCommit(false);
                    int snapshotTime = tx.getTimestamp();
                    stmt0.setInt(1, snapshotTime);
                    ResultSet rs0 = stmt0.executeQuery();
                    while (rs0.next()) {
                        stmt1.setLong(1, rs0.getLong("r_id"));
                        stmt1.setInt(2, rs0.getInt("max_t"));
                        ResultSet rs1 = stmt1.executeQuery();
                        if (rs1.next()) {
                            res.add(Pair.of(id2Name.get(rs0.getLong("r_id")), rs1.getInt("property")));
                        }
                    }
                } catch (SQLException e) {
                    e.printStackTrace();
                } finally {
                    conn.setAutoCommit(true);
                }
                SnapshotQueryTx.Result result = new SnapshotQueryTx.Result();
                result.setRoadStatus(res);
                return result;
            }
        };
    }

    private Callable<DBProxy.ServerResponse> execute(SnapshotAggrMaxTx tx) {
        return new Req() {
            @Override
            protected AbstractTransaction.Result executeQuery(Connection conn) throws Exception {
                List<Pair<String, Integer>> res = new ArrayList<>();
                try (PreparedStatement stmt0 = conn.prepareStatement("select r_id, max(t) as real_st from temporal_status where t <= ? group by r_id");
                     PreparedStatement stmt2 = conn.prepareStatement("select max(" + tx.getP() + ") as max_p from temporal_status where r_id = ? and t >= ? and t <= ? group by r_id")) {
                    conn.setAutoCommit(false);
                    stmt0.setInt(1, tx.getT0());
                    ResultSet rs = stmt0.executeQuery();
                    HashMap<String, Integer> st = new HashMap<>();
                    while (rs.next()) {
                        Long id = rs.getLong("r_id");
                        stmt2.setLong(1, id);
                        stmt2.setInt(2, rs.getInt("real_st"));
                        stmt2.setInt(3, tx.getT1());
                        ResultSet r = stmt2.executeQuery();
                        while (r.next()) {
                            res.add(Pair.of(id2Name.get(id), r.getInt("max_p")));
                        }
                    }
                } catch (SQLException e) {
                    e.printStackTrace();
                } finally {
                    conn.setAutoCommit(true);
                }
                SnapshotAggrMaxTx.Result result = new SnapshotAggrMaxTx.Result();
                result.setRoadTravelTime(res);
                return result;
            }
        };
    }

    private Callable<DBProxy.ServerResponse> execute(SnapshotAggrDurationTx tx) {
        return new Req() {
            @Override
            protected AbstractTransaction.Result executeQuery(Connection conn) throws Exception {
                List<Triple<String, Integer, Integer>> res = new ArrayList<>();
                try (PreparedStatement stmt0 = conn.prepareStatement("select r_id, max(t) as real_st from temporal_status where t <= ? group by r_id");
                     PreparedStatement stmt1 = conn.prepareStatement("select t, " + tx.getP() + " as property from temporal_status where r_id = ? and t >= ? and t <= ? order by t")) {
                    conn.setAutoCommit(false);
                    stmt0.setInt(1, tx.getT0());
                    ResultSet rs = stmt0.executeQuery();
                    while (rs.next()) {
                        Long id = rs.getLong("r_id");
                        stmt1.setLong(1, id);
                        stmt1.setInt(2, rs.getInt("real_st"));
                        stmt1.setInt(3, tx.getT1());
                        ResultSet r = stmt1.executeQuery();
                        int lastTime = -1, lastStatus = -1;
                        HashMap<Integer, Integer> buffer = new HashMap<>();
                        while (r.next()) {
                            int property = r.getInt("property");
                            int t = r.getInt("t");
                            if (lastTime != -1) {
                                int duration = t - lastTime;
                                buffer.merge(lastStatus, duration, Integer::sum);
                                lastTime = t;
                            } else {
                                lastTime = tx.getT0();
                            }
                            lastStatus = property;
                        }
                        if (lastTime != -1) buffer.merge(lastStatus, tx.getT1() - lastTime, Integer::sum);
                        for (Map.Entry<Integer, Integer> e : buffer.entrySet()) {
                            res.add(Triple.of(id2Name.get(id), e.getKey(), e.getValue()));
                        }
                    }
                } catch (SQLException e) {
                    e.printStackTrace();
                } finally {
                    conn.setAutoCommit(true);
                }
                SnapshotAggrDurationTx.Result result = new SnapshotAggrDurationTx.Result();
                result.setRoadStatDuration(res);
                return result;
            }
        };
    }

    private Callable<DBProxy.ServerResponse> execute(EntityTemporalConditionTx tx) {
        return new Req() {
            @Override
            protected AbstractTransaction.Result executeQuery(Connection conn) throws Exception {
                List<String> res = new ArrayList<>();
                try (PreparedStatement stmt0 = conn.prepareStatement("select r_id, max(t) as real_st from temporal_status where t <= " + tx.getT0() + " group by r_id");
                     PreparedStatement stmt1 = conn.prepareStatement("select r_id from temporal_status where r_id = ? and t >= ? and t <= ? and " + tx.getP() + " >= ? and " + tx.getP() + " <= ? limit 1")) {
                    conn.setAutoCommit(false);
                    ResultSet rs = stmt0.executeQuery();
                    while (rs.next()) {
                        Long id = rs.getLong("r_id");
                        stmt1.setLong(1, id);
                        stmt1.setInt(2, rs.getInt("real_st"));
                        stmt1.setInt(3, tx.getT1());
                        stmt1.setInt(4, tx.getVMin());
                        stmt1.setInt(5, tx.getVMax());
                        ResultSet r = stmt1.executeQuery();
                        if (r.next()) {
                            res.add(id2Name.get(id));
                        }
                    }
                } catch (SQLException e) {
                    e.printStackTrace();
                } finally {
                    conn.setAutoCommit(true);
                }
                EntityTemporalConditionTx.Result result = new EntityTemporalConditionTx.Result();
                result.setRoads(res);
                return result;
            }
        };
    }

    private Callable<DBProxy.ServerResponse> execute(NodeNeighborRoadTx tx) {
        return new Req() {
            @Override
            protected AbstractTransaction.Result executeQuery(Connection conn) throws Exception {
                List<String> answer = new ArrayList<>();
                try (PreparedStatement stmt = conn.prepareStatement("select r_name from road where r_start = " + tx.getNodeId() + " or r_end = " + tx.getNodeId())) {
                    ResultSet rs = stmt.executeQuery();
                    while (rs.next()) {
                        answer.add(rs.getString("r_name"));
                    }
                }
                NodeNeighborRoadTx.Result result = new NodeNeighborRoadTx.Result();
                result.setRoadIds(answer);
                metrics.setReturnSize(answer.size());
                return result;
            }
        };
    }


    private Callable<DBProxy.ServerResponse> execute(ReachableAreaQueryTx tx) {
        return new Req() {
            @Override
            protected AbstractTransaction.Result executeQuery(Connection conn) throws Exception {
                return GeneralizedDijkstra.solve(tx, new PostgreSQLTopology(conn));
            }
        };
    }

    private static class PostgreSQLTopology implements Topology {

        private final Connection conn;

        PostgreSQLTopology(Connection conn) {
            this.conn = conn;
        }

        @Override
        public ArrayList<Pair<Long, Long>> getRoadList(long curCross) {
            ArrayList<Pair<Long, Long>> ret = new ArrayList<>();
            try (PreparedStatement stmt = conn.prepareStatement("select r_id, r_end from road where r_start = ?")) {
                stmt.setLong(1, curCross);
                ResultSet rs = stmt.executeQuery();
                while (rs.next())
                    ret.add(Pair.of(rs.getLong("r_id"), rs.getLong("r_end")));
            } catch (SQLException e) {
                e.printStackTrace();
            }
            return ret;
        }

        @Override
        public int earliestArriveTime(long roadId, int when, int until) {
            int ret = Integer.MAX_VALUE;
            try (PreparedStatement stmt0 = conn.prepareStatement("select r_id, max(t) as max_t from temporal_status where r_id = ? and t <= ? group by r_id");
                 PreparedStatement stmt1 = conn.prepareStatement("select r_id, min(t) as min_t from temporal_status where r_id = ? and t >= ? group by r_id");
                 PreparedStatement stmt2 = conn.prepareStatement("select t, travel_t from temporal_status where t >= ? and t <= ?")) {
                stmt0.setLong(1, roadId);
                stmt0.setInt(2, when);
                ResultSet rs = stmt0.executeQuery();
                int st = -1;
                if (rs.next()) st = rs.getInt("max_t");
                stmt1.setLong(1, roadId);
                stmt1.setInt(2, until);
                rs = stmt1.executeQuery();
                int en = -1;
                if (rs.next()) en = rs.getInt("max_t");
                stmt2.setInt(1, st);
                stmt2.setInt(2, en);
                rs = stmt2.executeQuery();
                List<Pair<Integer, Integer>> l = new ArrayList<>();
                while (rs.next()) l.add(Pair.of(rs.getInt("t"), rs.getInt("travel_t")));
                l.sort(Comparator.comparingInt(o -> o.left));
                int sz = l.size();
                if (sz == 1) return when + l.get(0).right;
                for (int i = 0; i < sz - 1; ++i) {
                    if (i != 0) when = l.get(i).left;
                    int cur = when + l.get(i).right;
                    int next = l.get(i + 1).left;
                    if (cur <= next && cur <= until && cur < ret) ret = cur;
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
            return ret;
        }
    }

    private abstract class Req implements Callable<DBProxy.ServerResponse> {
        private final TimeMonitor timeMonitor = new TimeMonitor();
        final AbstractTransaction.Metrics metrics = new AbstractTransaction.Metrics();

        private Req() {
            timeMonitor.begin("Wait in queue");
        }

        @Override
        public DBProxy.ServerResponse call() throws Exception {
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
