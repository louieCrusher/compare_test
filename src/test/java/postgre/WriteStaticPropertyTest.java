package postgre;

import benchmark.client.DBProxy;
import benchmark.client.MariaDBExecutorClient;
import benchmark.client.PostgreSQLExecutorClient;
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
    //    private static int opPerTx = Integer.parseInt(Helper.mustEnv("TEMPORAL_DATA_PER_TX")); // number of mariadb queries executed in one transaction.
//    private static String startDay = Helper.mustEnv("TEMPORAL_DATA_START"); //0501
//    private static String endDay = Helper.mustEnv("TEMPORAL_DATA_END"); //0503
    private static String serverHost = Helper.mustEnv("DB_HOST"); // hostname of mariadb server.
    // E:\tgraph\test-data
    private static String dataFilePath = Helper.mustEnv("RAW_DATA_PATH"); // should be like '/media/song/test/data-set/beijing-traffic/TGraph/byday/'

    private static DBProxy client;
    private static BenchmarkTxResultProcessor post;

    @BeforeClass
    public static void initClient() throws IOException, ExecutionException, InterruptedException, SQLException, ClassNotFoundException {
        client = new PostgreSQLExecutorClient(serverHost, threadCnt, 800);
        post = new BenchmarkTxResultProcessor("Postgre(WriteStatic)", Helper.codeGitVersion());
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
