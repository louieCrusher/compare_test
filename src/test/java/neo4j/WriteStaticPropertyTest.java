package neo4j;


import benchmark.client.DBProxy;
import benchmark.client.neo4j.Neo4jExecutorClient;
import benchmark.model.TrafficTemporalPropertyGraph;
import benchmark.transaction.definition.AbstractTransaction;
import benchmark.transaction.generation.BenchmarkTxGenerator;
import benchmark.transaction.generation.BenchmarkTxResultProcessor;
import benchmark.utils.Helper;
import com.aliyun.openservices.aliyun.log.producer.Producer;
import com.aliyun.openservices.aliyun.log.producer.errors.ProducerException;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.sql.SQLException;
import java.util.concurrent.ExecutionException;

public class WriteStaticPropertyTest {
    private static int threadCnt = Integer.parseInt(Helper.mustEnv("MAX_CONNECTION_CNT")); // number of threads to send queries.
    private static String serverHost = Helper.mustEnv("DB_HOST"); // hostname of neo4j server.
    // E:\test-data
    private static String dataFilePath = Helper.mustEnv("RAW_DATA_PATH");

    private static DBProxy client;
    private static BenchmarkTxResultProcessor post;

    @BeforeClass
    public static void initClient() throws IOException, ExecutionException, InterruptedException, SQLException, ClassNotFoundException {
        client = new Neo4jExecutorClient(serverHost, threadCnt, 800);
        post = new BenchmarkTxResultProcessor("Neo4j Server1(WriteStatic)", Helper.codeGitVersion());
    }

    @Test
    public void run() throws Exception {
        // import static topology.
        TrafficTemporalPropertyGraph tgraph = new TrafficTemporalPropertyGraph();
        tgraph.importTopology(new File(dataFilePath, "road_topology.csv.gz"));
        AbstractTransaction txStaticImport = BenchmarkTxGenerator.txImportStatic(tgraph);
        post.process(client.execute(txStaticImport), txStaticImport);
    }

    @AfterClass
    public static void close() throws IOException, InterruptedException, ProducerException {
        client.close();
        post.close();
    }
}
