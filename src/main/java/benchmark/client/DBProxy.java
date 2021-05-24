package benchmark.client;

import benchmark.transaction.definition.AbstractTransaction;
import com.google.common.util.concurrent.ListenableFuture;
import benchmark.transaction.definition.AbstractTransaction.Metrics;
import benchmark.transaction.definition.AbstractTransaction.Result;

import java.io.IOException;

/**
 * Created by song on 16-2-23.
 */
public interface DBProxy {
    // return server version.
    String testServerClientCompatibility() throws UnsupportedOperationException;

    ListenableFuture<ServerResponse> execute(AbstractTransaction tx) throws Exception;

    void createDB() throws IOException;

    // restart database. blocking until service is available or failed.
    void restartDB() throws IOException;

    void shutdownDB() throws IOException;

    void close() throws IOException, InterruptedException;

    class ServerResponse {
        private Result result;
        private Metrics metrics;

        public AbstractTransaction.Result getResult() {
            return result;
        }

        public void setResult(Result result) {
            this.result = result;
        }

        public AbstractTransaction.Metrics getMetrics() {
            return metrics;
        }

        public void setMetrics(Metrics metrics) {
            this.metrics = metrics;
        }
    }
}
