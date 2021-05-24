package mariadb;

import benchmark.client.MariaDBExecutorClient;
import benchmark.utils.Helper;
import org.junit.Test;

public class CreateIndexes {
    private static int threadCnt = Integer.parseInt(Helper.mustEnv("MAX_CONNECTION_CNT")); // number of threads to send queries.
    private static String serverHost = Helper.mustEnv("DB_HOST"); // hostname of mariadb server.


    @Test
    public void run() throws Exception {
        MariaDBExecutorClient client = new MariaDBExecutorClient(serverHost, threadCnt, 800);
        client.createIndexes();
        client.close();
    }
}
