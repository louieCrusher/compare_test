package neo4j2;

import benchmark.client.DBProxy;
import benchmark.client.neo4j.Neo4jExecutorClient;
import benchmark.transaction.definition.SnapshotAggrMaxTx;
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

public class SnapshotAggrMaxTest {
    private static int threadCnt = Integer.parseInt(Helper.mustEnv("MAX_CONNECTION_CNT")); // number of threads to send queries.
    private static String serverHost = Helper.mustEnv("DB_HOST");
    private static String resultFile = Helper.mustEnv("SERVER_RESULT_FILE");

    private static Producer logger;
    private static DBProxy client;
    private static BenchmarkTxResultProcessor post;

    @BeforeClass
    public static void initClient() throws IOException, ExecutionException, InterruptedException, SQLException, ClassNotFoundException {
        logger = Helper.getLogger();
        client = new Neo4jExecutorClient(serverHost, threadCnt, 800);
        post = new BenchmarkTxResultProcessor("Neo4j2(SnapShotAggrMaxQuery)", Helper.codeGitVersion());
        post.setLogger(logger);
        post.setResult(new File(resultFile));
    }

    @Test
    public void test() throws Exception {
        for (int i = 0; i < 200; ++i)
            query("travel_time", Helper.timeStr2int("201006300830"), Helper.timeStr2int("201006300930"));
    }

    private void query(String propertyName, int st, int en) throws Exception {
        SnapshotAggrMaxTx tx = new SnapshotAggrMaxTx();
        tx.setP(propertyName);
        tx.setT0(st);
        tx.setT1(en);
        post.process(client.execute(tx), tx);
    }

    @AfterClass
    public static void close() throws IOException, InterruptedException, ProducerException {
        client.close();
        Thread.sleep(1000 * 60 * 2);
        post.close();
        logger.close();
    }
}
