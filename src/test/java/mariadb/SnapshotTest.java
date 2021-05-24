package mariadb;

import benchmark.client.DBProxy;
import benchmark.client.MariaDBExecutorClient;
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

public class SnapshotTest {
    private static int threadCnt = Integer.parseInt(Helper.mustEnv("MAX_CONNECTION_CNT"));
    private static String serverHost = Helper.mustEnv("DB_HOST");
    private static String resultFile = Helper.mustEnv("SERVER_RESULT_FILE");

    private static Producer logger;
    private static DBProxy client;
    private static BenchmarkTxResultProcessor post;

    @BeforeClass
    public static void initClient() throws IOException, ExecutionException, InterruptedException, SQLException, ClassNotFoundException {
        logger = Helper.getLogger();
        client = new MariaDBExecutorClient(serverHost, threadCnt, 800);
        post = new BenchmarkTxResultProcessor("MariaDB(SnapShotQuery)", Helper.codeGitVersion());
        post.setLogger(logger);
        post.setResult(new File(resultFile));
    }

    @Test
    public void test() throws Exception {
        for (int i = 0; i < 200; ++i) {
            query("travel_t", Helper.timeStr2int("201006300940"));
        }
    }

    private void query(String propertyName, int t) throws Exception {
        SnapshotQueryTx tx = new SnapshotQueryTx();
        tx.setPropertyName(propertyName);
        tx.setTimestamp(t);
        post.process(client.execute(tx), tx);
    }

    @AfterClass
    public static void close() throws IOException, InterruptedException, ProducerException {
        client.close();
        post.close();
        logger.close();
    }
}
