package neo4j;

import benchmark.client.DBProxy;
import benchmark.client.PostgreSQLExecutorClient;
import benchmark.client.neo4j.Neo4jExecutorClient;
import benchmark.transaction.definition.ReachableAreaQueryTx;
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
import java.sql.Timestamp;
import java.util.concurrent.ExecutionException;

public class ReachableAreaTest {
    private static int threadCnt = Integer.parseInt(Helper.mustEnv("MAX_CONNECTION_CNT"));
    private static String serverHost = Helper.mustEnv("DB_HOST");
    private static String resultFile = Helper.mustEnv("SERVER_RESULT_FILE");

    private static Producer logger;
    private static DBProxy client;
    private static BenchmarkTxResultProcessor post;

    @BeforeClass
    public static void initClient() throws IOException, ExecutionException, InterruptedException, SQLException, ClassNotFoundException {
        // logger = Helper.getLogger();
        client = new Neo4jExecutorClient(serverHost, threadCnt, 800);
        post = new BenchmarkTxResultProcessor("Neo4j1(ReachableAreaQuery)", Helper.codeGitVersion());
        // post.setLogger(logger);
        post.setResult(new File(resultFile));
    }

    @Test
    public void test() throws Exception {
        ReachableAreaQueryTx tx = new ReachableAreaQueryTx();
        // startCrossId: long, departureTime: int, travelTime: int
        tx.setDepartureTime((int) (Timestamp.valueOf("2010-05-01 00:52:00").getTime() / 1000L));
        tx.setStartCrossId(36);
        tx.setTravelTime(60);
        post.process(client.execute(tx), tx);
    }

    @AfterClass
    public static void close() throws IOException, InterruptedException, ProducerException {
        client.close();
        Thread.sleep(1000 * 30);
        post.close();
        // logger.close();
    }
}
