package mariadb;

import benchmark.client.DBProxy;
import benchmark.client.MariaDBExecutorClient;
import benchmark.transaction.definition.ReachableAreaQueryTx;
import benchmark.transaction.definition.SnapshotQueryTx;
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

public class ReachableAreaTest {
    private static int threadCnt = Integer.parseInt(Helper.mustEnv("MAX_CONNECTION_CNT"));
    private static String serverHost = Helper.mustEnv("DB_HOST");
    private static String resultFile = Helper.mustEnv("SERVER_RESULT_FILE");
    private static long startCrossId = Long.parseLong(Helper.mustEnv("START_CROSS_ID"));
    private static int departureTime = Integer.parseInt(Helper.mustEnv("DEPARTURE_TIME"));
    private static int travelTime = Integer.parseInt(Helper.mustEnv("TRAVEL_TIME"));

    private static Producer logger;
    private static DBProxy client;
    private static BenchmarkTxResultProcessor post;

    @BeforeClass
    public static void initClient() throws IOException, ExecutionException, InterruptedException, SQLException, ClassNotFoundException {
        logger = Helper.getLogger();
        client = new MariaDBExecutorClient(serverHost, threadCnt, 800);
        post = new BenchmarkTxResultProcessor("MariaDB(ReachableAreaQuery)", Helper.codeGitVersion());
        post.setLogger(logger);
        post.setResult(new File(resultFile));
    }

    @Test
    public void test() throws Exception {
        ReachableAreaQueryTx tx = new ReachableAreaQueryTx();
        tx.setDepartureTime(departureTime);
        tx.setStartCrossId(startCrossId);
        tx.setTravelTime(travelTime);
        post.process(client.execute(tx), tx);
    }

    @AfterClass
    public static void close() throws IOException, InterruptedException, ProducerException {
        client.close();
        post.close();
        logger.close();
    }
}
