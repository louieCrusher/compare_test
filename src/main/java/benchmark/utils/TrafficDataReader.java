package benchmark.utils;

import com.google.common.collect.AbstractIterator;
import benchmark.model.StatusUpdate;

import java.io.Closeable;
import java.io.File;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class TrafficDataReader extends AbstractIterator<StatusUpdate> implements Closeable {
    private ExecutorService executor = Executors.newSingleThreadExecutor();
    private ConcurrentLinkedQueue<StatusUpdate> cache = new ConcurrentLinkedQueue<>();

    private TrafficMultiFileReader multiFileIter;
    private int cacheSize;

    public TrafficDataReader(List<File> fileList, int cacheSize) {
        this.multiFileIter = new TrafficMultiFileReader(fileList);
        this.cacheSize = cacheSize;
    }

    @Override
    protected StatusUpdate computeNext() {
        if (!multiFileIter.hasNext() && cache.isEmpty()) {
            return endOfData();
        }
        if (cache.size() < cacheSize) {
            fillCache();
        }
        return cache.poll();
    }

    private void fillCache() {
        executor.submit(() -> {
            while (multiFileIter.hasNext() && cache.size() < cacheSize) {
                StatusUpdate s = multiFileIter.next();
                cache.add(s);
            }
        });
    }


    @Override
    public void close() {
        multiFileIter.close();
        executor.shutdownNow();
    }
}
