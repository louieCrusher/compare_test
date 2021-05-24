package benchmark.server;

import benchmark.utils.Helper;
import benchmark.utils.TimeMonitor;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.annotation.JSONField;
import com.alibaba.fastjson.serializer.SerializerFeature;
import com.sun.management.OperatingSystemMXBean;
import benchmark.client.DBProxy;
import benchmark.transaction.definition.AbstractTransaction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.schema.IndexDefinition;
import oshi.SystemInfo;
import oshi.hardware.HWDiskStore;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.lang.management.ManagementFactory;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

import static benchmark.transaction.definition.AbstractTransaction.*;

public class Neo4jSocketServer {

    private final File dbPath;
    private final ReqExecutor reqExecutor;

    private GraphDatabaseService db;
    private volatile boolean shouldRun = true;
    private ServerSocket server;
    private final List<Thread> threads = Collections.synchronizedList(new LinkedList<>());

    public Neo4jSocketServer(File dbPath, ReqExecutor reqExecutor) {
        this.dbPath = dbPath;
        this.reqExecutor = reqExecutor;
    }

    public void start() throws IOException {
        db = new GraphDatabaseFactory().newEmbeddedDatabase(dbPath);
        // db = new GraphDatabaseFactory().newEmbeddedDatabaseBuilder(dbPath).loadPropertiesFromFile("E:\\compare_test\\neo4j.conf").newGraphDatabase();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> db.shutdown()));

        ServerStatusMonitor monitor = new ServerStatusMonitor();
        monitor.start();
        reqExecutor.setDB(db);

        server = new ServerSocket(8438);
        System.out.println("waiting for client to connect.");

        try {
            while (shouldRun) {
                Socket client;
                try {
                    client = server.accept();
                } catch (SocketException ignore) { // closed from another thread.
                    break;
                }
                Thread t = new ServerThread(client, monitor);
                threads.add(t);
                System.out.println("GET one more client, currently " + threads.size() + " client");
                t.setDaemon(true);
                t.start();
            }
            for (Thread t : threads) {
                t.join();
            }
        } catch (InterruptedException ignore) {
            // just exit
        }
        db.shutdown();
        System.out.println("main thread exit.");
    }

    public static abstract class ReqExecutor {
        protected GraphDatabaseService db;

        protected void setDB(GraphDatabaseService db) {
            this.db = db;
        }

        protected abstract Result execute(String line) throws RuntimeException;
    }

    public static class TransactionFailedException extends RuntimeException {
    }

    private class ServerStatusMonitor extends Thread {
        volatile ServerStatus serverStatus;

        public void run() {
            final OperatingSystemMXBean bean = (OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean();
            Runtime runtime = Runtime.getRuntime();
            try {
                long lastTime = System.currentTimeMillis();
                long disksWrite = 0, disksRead = 0;
                while (shouldRun) {
                    Thread.sleep(1_000);
                    long curDisksWrite = 0, curDisksRead = 0, curDiskQueueLen = 0;
                    HWDiskStore[] disks = new SystemInfo().getHardware().getDiskStores();
                    for (HWDiskStore disk : disks) {
                        curDisksWrite += disk.getWriteBytes();
                        curDisksRead += disk.getReadBytes();
                        curDiskQueueLen += disk.getCurrentQueueLength();
                    }
                    long now = System.currentTimeMillis();
                    ServerStatus s = new ServerStatus();
                    s.activeConn = threads.size();
                    s.curMem = runtime.totalMemory() - runtime.freeMemory();
                    s.processCpuLoad = bean.getProcessCpuLoad();
                    s.systemCpuLoad = bean.getSystemCpuLoad();
                    s.diskWriteSpeed = (curDisksWrite - disksWrite) / (now - lastTime) * 1000;
                    s.diskReadSpeed = (curDisksRead - disksRead) / (now - lastTime) * 1000;
                    s.diskQueueLength = curDiskQueueLen;
                    this.serverStatus = s;
                    disksRead = curDisksRead;
                    disksWrite = curDisksWrite;
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }


    private class ServerThread extends Thread {
        private final ServerStatusMonitor monitor;
        Socket client;
        BufferedReader fromClient;
        PrintStream toClient;
        long reqCnt = 0;

        ServerThread(Socket client, ServerStatusMonitor monitor) throws IOException {
            this.client = client;
            this.monitor = monitor;
            client.setTcpNoDelay(true);
            this.fromClient = new BufferedReader(new InputStreamReader(client.getInputStream()));
            this.toClient = new PrintStream(client.getOutputStream(), true);
        }

        public void run() {
            long tid = Thread.currentThread().getId();
            Thread.currentThread().setName("TCypher con " + tid);
            System.out.println(Thread.currentThread().getName() + " started.");
            TimeMonitor time = new TimeMonitor();
            time.begin("Send");
            try {
                while (true) {
                    time.mark("Send", "Wait");
                    String line;
                    try {
                        line = fromClient.readLine();
                    } catch (SocketException ignore) { // client close conn.
                        System.out.println("closed by server.");
                        client.close();
                        break;
                    }
                    Result exeResult = null;
                    boolean success = true;
                    if (line == null) {
                        System.out.println("client close connection. read end.");
                        client.close();
                        break;
                    } else if ("EXIT".equals(line)) { //client ask server exit;
                        client.close();
                        server.close();
                        shouldRun = false;
                        System.out.println("client ask server exit.");
                        break;
                    } else {
                        time.mark("Wait", "Transaction");
                        if ("VERSION".equals(line)) {
                            ServerVersionResult versionResult = new ServerVersionResult();
                            versionResult.setVersion(Helper.codeGitVersion());
                            exeResult = versionResult;
                            System.out.println("client ask server version.");
                        } else {
                            try {
                                exeResult = reqExecutor.execute(line);
                            } catch (TransactionFailedException e) {
                                success = false;
                            }
                        }
                        time.mark("Transaction", "Send");

                        Metrics metrics = new Metrics();
                        metrics.setStatus(monitor.serverStatus);
                        metrics.setTxSuccess(success);
                        metrics.setTxTime(Math.toIntExact(time.duration("Transaction")));

                        DBProxy.ServerResponse response = new DBProxy.ServerResponse();
                        response.setResult(exeResult);
                        response.setMetrics(metrics);

                        toClient.println(JSON.toJSONString(response, Helper.serializerFeatures));
                        reqCnt++;
                    }
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
            threads.remove(this);
            System.out.println(Thread.currentThread().getName() + " exit. process " + reqCnt + " queries.");
        }
    }

    public static class Metrics extends AbstractTransaction.Metrics {
        private int txTime;
        private boolean txSuccess;
        // @JSONField(unwrapped = true)
        private ServerStatus status;

        public int getTxTime() {
            return txTime;
        }

        public void setTxTime(int txTime) {
            this.txTime = txTime;
        }

        public boolean isTxSuccess() {
            return txSuccess;
        }

        public void setTxSuccess(boolean txSuccess) {
            this.txSuccess = txSuccess;
        }

        public ServerStatus getStatus() {
            return status;
        }

        public void setStatus(ServerStatus status) {
            this.status = status;
        }
    }

    public static class ServerStatus {
        private long time = System.currentTimeMillis();
        private int activeConn;
        private long curMem;
        private long diskReadSpeed;
        private long diskWriteSpeed;
        private long diskQueueLength;
        private double processCpuLoad;
        private double systemCpuLoad;

        public long getTime() {
            return time;
        }

        public void setTime(long time) {
            this.time = time;
        }

        public int getActiveConn() {
            return activeConn;
        }

        public void setActiveConn(int activeConn) {
            this.activeConn = activeConn;
        }

        public long getCurMem() {
            return curMem;
        }

        public void setCurMem(long curMem) {
            this.curMem = curMem;
        }

        public long getDiskReadSpeed() {
            return diskReadSpeed;
        }

        public void setDiskReadSpeed(long diskReadSpeed) {
            this.diskReadSpeed = diskReadSpeed;
        }

        public long getDiskWriteSpeed() {
            return diskWriteSpeed;
        }

        public void setDiskWriteSpeed(long diskWriteSpeed) {
            this.diskWriteSpeed = diskWriteSpeed;
        }

        public long getDiskQueueLength() {
            return diskQueueLength;
        }

        public void setDiskQueueLength(long diskQueueLength) {
            this.diskQueueLength = diskQueueLength;
        }

        public double getProcessCpuLoad() {
            return processCpuLoad;
        }

        public void setProcessCpuLoad(double processCpuLoad) {
            this.processCpuLoad = processCpuLoad;
        }

        public double getSystemCpuLoad() {
            return systemCpuLoad;
        }

        public void setSystemCpuLoad(double systemCpuLoad) {
            this.systemCpuLoad = systemCpuLoad;
        }
    }

    public static class ServerVersionResult extends Result {
        String version;

        public String getVersion() {
            return version;
        }

        public void setVersion(String version) {
            this.version = version;
        }
    }

}
