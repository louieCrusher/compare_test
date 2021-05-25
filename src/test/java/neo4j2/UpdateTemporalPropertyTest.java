package neo4j2;

import benchmark.client.DBProxy;
import benchmark.client.neo4j.Neo4jExecutorClient;
import benchmark.transaction.definition.AbstractTransaction;
import benchmark.transaction.definition.UpdateTemporalDataTx;
import benchmark.transaction.generation.BenchmarkTxResultProcessor;
import benchmark.utils.Helper;
import com.aliyun.openservices.aliyun.log.producer.Producer;
import com.aliyun.openservices.aliyun.log.producer.errors.ProducerException;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.concurrent.ExecutionException;

public class UpdateTemporalPropertyTest {
    private static int threadCnt = Integer.parseInt(Helper.mustEnv("MAX_CONNECTION_CNT")); // number of threads to send queries.
    private static String serverHost = Helper.mustEnv("DB_HOST"); // hostname of mariadb server.

    private static Producer logger;
    private static DBProxy client;
    private static BenchmarkTxResultProcessor post;

    @BeforeClass
    public static void initClient() throws IOException, ExecutionException, InterruptedException, SQLException, ClassNotFoundException {
        // logger = Helper.getLogger();
        client = new Neo4jExecutorClient(serverHost, threadCnt, 800);
        post = new BenchmarkTxResultProcessor("Neo4j2(UpdateTemporal)", Helper.codeGitVersion());
        // post.setLogger(logger);
    }

    @Test
    public void run() throws Exception {
        UpdateTemporalDataTx tx = new UpdateTemporalDataTx();
        tx.setStartTime((int) (Timestamp.valueOf("2010-05-01 12:00:00").getTime() / 1000L));
        tx.setEndTime((int) (Timestamp.valueOf("2010-05-01 23:15:00").getTime() / 1000L));
        tx.setJamStatus(10);
        tx.setSegmentCount(4);
        tx.setTravelTime(30);
        tx.setRoadId("595652_00045");
        post.process(client.execute(tx), tx);
    }

    @AfterClass
    public static void close() throws IOException, InterruptedException, ProducerException {
        client.close();
        // Thread.sleep(1000 * 60 * 2);
        post.close();
        // logger.close();
    }
}
