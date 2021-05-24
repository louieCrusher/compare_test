package benchmark.utils;

import benchmark.model.StatusUpdate;
import com.google.common.collect.AbstractIterator;

import java.io.BufferedReader;
import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.*;

/**
 * 自动下载交通数据，
 */
public class TrafficMultiFileReader extends AbstractIterator<StatusUpdate> implements Closeable {
    private final ExecutorService executor = Executors.newSingleThreadExecutor();
    private final LinkedList<Future<File>> files = new LinkedList<>();
    private BufferedReader curReader;

    public TrafficMultiFileReader(List<File> fileList) {
        for (File file : fileList) {
            TrafficFileProcessingTask task = new TrafficFileProcessingTask(file);
            Future<File> future = executor.submit(task);
            files.add(future);
        }
    }

    @Override
    protected StatusUpdate computeNext() {
        try {
            if (curReader == null) {
                Future<File> f = files.poll();
                if (f == null) return endOfData();
                File file = f.get();
                curReader = Helper.gzipReader(file);
                String line = curReader.readLine();
                return new StatusUpdate(line);
            } else {
                String line = curReader.readLine();
                if (line == null) {
                    curReader.close();
                    Future<File> f = files.poll();
                    if (f == null) return endOfData();
                    File file = f.get();
                    curReader = Helper.gzipReader(file);
                    line = curReader.readLine();
                    return new StatusUpdate(line);
                } else {
                    return new StatusUpdate(line);
                }
            }
        } catch (InterruptedException | ExecutionException | IOException e) {
            e.printStackTrace();
            throw new RuntimeException("fail to read traffic file.");
        }
    }

    public void close() {
        executor.shutdown();
    }


    private static class TrafficFileProcessingTask implements Callable<File> {
        private final File file;

        public TrafficFileProcessingTask(File file) {
            this.file = file;
        }

        @Override
        public File call() throws Exception {
            if (!file.exists())
                Helper.download("http://amitabha.water-crystal.org/TGraphDemo/bj-traffic/" + file.getName(), file);
            return file;
        }
    }
}
