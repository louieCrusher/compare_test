package benchmark.client.neo4j;

import benchmark.client.DBProxy;
import benchmark.server.Neo4jSocketServer;
import benchmark.transaction.definition.AbstractTransaction;
import benchmark.utils.Helper;
import benchmark.utils.TimeMonitor;
import com.alibaba.fastjson.JSON;
import com.google.common.util.concurrent.ListenableFuture;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.ExecutionException;

public class Neo4jExecutorClient extends Neo4jSocketClient implements DBProxy {


    public Neo4jExecutorClient(String serverHost, int parallelCnt, int queueLength) throws IOException, ExecutionException, InterruptedException {
        super(serverHost, parallelCnt, queueLength);
    }

    @Override
    public ListenableFuture<ServerResponse> execute(AbstractTransaction tx) throws Exception {
        return this.addQuery(JSON.toJSONString(tx, Helper.serializerFeatures));
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
        this.awaitTermination();
    }

    @Override
    public String testServerClientCompatibility() {
        try {
            return super.testServerClientCompatibility();
        } catch (ExecutionException | InterruptedException e) {
            e.printStackTrace();
            throw new UnsupportedOperationException(e);
        }
    }

    @Override
    protected ServerResponse onResponse(String query, String response, TimeMonitor timeMonitor, Thread thread) throws Exception {
        ServerResponse res = JSON.parseObject(response, ServerResponse.class);
        AbstractTransaction.Metrics metrics = res.getMetrics();
        metrics.setConnId(Math.toIntExact(Thread.currentThread().getId()));
        metrics.setExeTime(Math.toIntExact(timeMonitor.duration("Send query") + timeMonitor.duration("Wait result")));
        metrics.setWaitTime(Math.toIntExact(timeMonitor.duration("Wait in queue")));
        metrics.setSendTime(timeMonitor.beginT("Send query"));
        metrics.setReqSize(query.length());
        metrics.setReturnSize(response.length());
        return res;
    }
}
