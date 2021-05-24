package mariadb;

import benchmark.client.DBProxy;
import benchmark.client.MariaDBExecutorClient;
import benchmark.client.PostgreSQLExecutorClient;
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
import java.util.concurrent.ExecutionException;

public class UpdateTemporalPropertyTest {
    private static int threadCnt = Integer.parseInt(Helper.mustEnv("MAX_CONNECTION_CNT")); // number of threads to send queries.
    private static String serverHost = Helper.mustEnv("DB_HOST"); // hostname of mariadb server.
    private static int startTime = Integer.parseInt(Helper.mustEnv("START_TIME"));
    private static int endTime = Integer.parseInt(Helper.mustEnv("END_TIME"));

    private static Producer logger;
    private static DBProxy client;
    private static BenchmarkTxResultProcessor post;

    @BeforeClass
    public static void initClient() throws IOException, ExecutionException, InterruptedException, SQLException, ClassNotFoundException {
        logger = Helper.getLogger();
        client = new MariaDBExecutorClient(serverHost, threadCnt, 800);
        post = new BenchmarkTxResultProcessor("MariaDB(UpdateTemporal)", Helper.codeGitVersion());
        post.setLogger(logger);
    }

    @Test
    public void run() throws Exception {
        UpdateTemporalDataTx tx = new UpdateTemporalDataTx();
        tx.setStartTime(startTime);
        tx.setEndTime(endTime);
        tx.setJamStatus(1);
        tx.setSegmentCount(2);
        tx.setTravelTime(15);
        tx.setRoadId("10086");
        tx.setTxType(AbstractTransaction.TxType.TX_UPDATE_TEMPORAL_DATA);
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
