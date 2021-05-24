package mariadb;

import benchmark.client.MariaDBExecutorClient;
import benchmark.transaction.definition.ImportTemporalDataTx;
import com.aliyun.openservices.aliyun.log.producer.Producer;
import com.aliyun.openservices.aliyun.log.producer.errors.ProducerException;
import benchmark.transaction.generation.BenchmarkTxGenerator;
import benchmark.transaction.generation.BenchmarkTxResultProcessor;
import benchmark.transaction.definition.AbstractTransaction;
import benchmark.utils.Helper;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.ExecutionException;

public class WriteTemporalPropertyTest {

    private static final int threadCnt = Integer.parseInt(Helper.mustEnv("MAX_CONNECTION_CNT")); // number of threads to send queries.
    private static final int opPerTx = Integer.parseInt(Helper.mustEnv("TEMPORAL_DATA_PER_TX")); // number of mariadb queries executed in one transaction.
    private static final String startDay = Helper.mustEnv("TEMPORAL_DATA_START"); // 0501
    private static final String endDay = Helper.mustEnv("TEMPORAL_DATA_END"); // 0503
    private static final String serverHost = Helper.mustEnv("DB_HOST"); // hostname of mariadb server.
    // E:\test-data
    private final static String dataFilePath = Helper.mustEnv("RAW_DATA_PATH");

    private static Producer logger;
    private static MariaDBExecutorClient client;
    private static BenchmarkTxResultProcessor post;

    @BeforeClass
    public static void initClient() throws IOException, ExecutionException, InterruptedException, SQLException, ClassNotFoundException {
        // logger = Helper.getLogger();
        client = new MariaDBExecutorClient(serverHost, threadCnt, 800);
        // post = new BenchmarkTxResultProcessor("MariaDB(WrtTemporal)", Helper.codeGitVersion());
        // post.setLogger(logger);
    }

    @Test
    public void run() throws Exception {
        List<File> fileList = Helper.trafficFileList(dataFilePath, startDay, endDay);
        try (BenchmarkTxGenerator.TemporalPropertyAppendTxGenerator g = new BenchmarkTxGenerator.TemporalPropertyAppendTxGenerator(opPerTx, fileList)) {
            int cnt = 0;
            while (g.hasNext()) {
                AbstractTransaction tx = g.next();
                client.updateBuffer((ImportTemporalDataTx) tx);
                ++cnt;
                // System.out.println("cnt = " + cnt);
                if (cnt % 5_000 == 0) client.printSize();
                int sz = client.getSize();
                if (sz > 10_000_000) {
                    System.out.println("start write disk.");
                    client.writeTemporalToDisk();
                    System.out.println("end write disk.");
                }
                // if (cnt % 5000 == 0) client.writeTemporalToDisk();
            }
            client.emptyLastRIdBuffer();
            System.out.println("start the last disk write.");
            client.writeTemporalToDisk();
            System.out.println("end the last disk write.");
            client.printTheLastSize();
        }
    }

    @AfterClass
    public static void close() throws IOException, InterruptedException, ProducerException {
        client.close();
        // post.close();
        // logger.close();
    }

}


