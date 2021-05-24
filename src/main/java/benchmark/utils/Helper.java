package benchmark.utils;

import com.alibaba.fastjson.parser.ParserConfig;
import com.alibaba.fastjson.serializer.SerializerFeature;
import com.aliyun.openservices.aliyun.log.producer.LogProducer;
import com.aliyun.openservices.aliyun.log.producer.Producer;
import com.aliyun.openservices.aliyun.log.producer.ProducerConfig;
import com.aliyun.openservices.aliyun.log.producer.ProjectConfig;
import com.aliyun.openservices.aliyun.log.producer.errors.ProducerException;
import com.google.common.base.Preconditions;
import com.google.common.collect.PeekingIterator;
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;
import org.apache.commons.compress.utils.IOUtils;
import org.apache.commons.lang3.tuple.Triple;

import java.io.*;
import java.net.URL;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.util.*;
import java.util.stream.Collectors;
import java.util.zip.GZIPInputStream;

/**
 * Created by song on 16-2-23.
 */
public class Helper {

    {
        ParserConfig.getGlobalInstance().setAutoTypeSupport(true);
    }

    public static SerializerFeature[] serializerFeatures = new SerializerFeature[]{
            SerializerFeature.WriteClassName,
            SerializerFeature.DisableCircularReferenceDetect
    };

    public static String codeGitVersion() {
        try (InputStream input = Helper.class.getResourceAsStream("/git.properties")) {
            if (input == null) return "NoGit";
            Properties prop = new Properties();
            prop.load(input);
            String gitCommitId = prop.getProperty("git.commit.id.describe-short");
            if (gitCommitId.endsWith("-Modified")) {
                return gitCommitId.replace("-Modified", "(M)");
            } else {
                return gitCommitId;
            }
        } catch (IOException ex) {
            if (ex instanceof FileNotFoundException) return "NoGit";
            ex.printStackTrace();
            return "Git-Err";
        }
    }

    public static Producer getLogger() {
        ProducerConfig pConf = new ProducerConfig();
        pConf.setIoThreadCount(1); // one thread to upload
        Producer onlineLogger = new LogProducer(pConf);
        onlineLogger.putProjectConfig(new ProjectConfig("tgraph-demo-test", "cn-beijing.log.aliyuncs.com", "LTAIeA55PGyOpgZs", "H0XzCznABsioSI4TQpwXblH269eBm6"));
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                onlineLogger.close();
            } catch (InterruptedException | ProducerException e) {
                e.printStackTrace();
            }
        }));
        return onlineLogger;
    }

    public static void deleteAllFilesOfDir(File path) {
        if (!path.exists())
            return;
        if (path.isFile()) {
            path.delete();
            return;
        }
        File[] files = path.listFiles();
        for (int i = 0; i < files.length; i++) {
            deleteAllFilesOfDir(files[i]);
        }
        path.delete();
    }

    public static String getString(int size) {
        char[] chars = new char[size];
        Arrays.fill(chars, 'a');
        return new String(chars);
    }

    public static byte[] intToByteArray(int value) {
        return new byte[]{
                (byte) (value >>> 24),
                (byte) (value >>> 16),
                (byte) (value >>> 8),
                (byte) value};
    }

    public static void getFileRecursive(File dir, List<File> fileList, int level) {
        if (dir.isDirectory()) {
            for (File file : dir.listFiles()) {
                if (!file.isDirectory() && file.getName().startsWith("TJamData_201") && file.getName().endsWith(".csv")) {
                    fileList.add(file);
                }
            }
            if (level > 0) {
                for (File file : dir.listFiles()) {
                    if (file.isDirectory()) {
                        getFileRecursive(file, fileList, level - 1);
                    }
                }
            }
        }
    }

    public static int[] calcSplit(int nthPart, int totalPartCount, int size) {
        if (!(0 < nthPart && nthPart <= totalPartCount && totalPartCount <= size)) {
            throw new RuntimeException("bad argument");
        }
        int[] lengthList = new int[totalPartCount];
        int base = size / totalPartCount;
        int remain = size % totalPartCount;
        int start = 0;
        int end = 0;

//        System.out.println((base+1)+"["+remain+" times], "+base+"["+(totalPartCount-remain)+" times]");
        for (int i = 0; i < totalPartCount; i++) {
            lengthList[i] = base;
            if (remain > 0) {
                remain--;
                lengthList[i]++;
            }
        }
        int j;
        for (j = 0; j < nthPart - 1; j++) {
            start += lengthList[j];
        }
        end = start + lengthList[j] - 1;
        return new int[]{start, end};
    }

    public static int getFileTimeStamp(File file) {
        return timeStr2int(file.getName().substring(9, 21));
    }

    public static void deleteExistDB(File dir) {
        if (dir.exists()) {
            Helper.deleteAllFilesOfDir(dir);
        }
        dir.mkdir();
    }

    private static final char[] hexArray = "0123456789ABCDEF".toCharArray();

    public static String bytesToHex(byte[] bytes) {
        char[] hexChars = new char[bytes.length * 2];
        for (int j = 0; j < bytes.length; j++) {
            int v = bytes[j] & 0xFF;
            hexChars[j * 2] = hexArray[v >>> 4];
            hexChars[j * 2 + 1] = hexArray[v & 0x0F];
        }
        return new String(hexChars);
    }

    // add second by crusher 2.10 2021.
    private static int timeStr2intImpl(String tStr) {
        String yearStr = tStr.substring(0, 4);
        String monthStr = tStr.substring(4, 6);
        String dayStr = tStr.substring(6, 8);
        String hourStr = tStr.substring(8, 10);
        String minuteStr = tStr.substring(10, 12);
        String secondStr = tStr.substring(12, 14);
        int year = Integer.parseInt(yearStr);
        int month = Integer.parseInt(monthStr) - 1; // month count from 0 to 11, not 12
        int day = Integer.parseInt(dayStr);
        int hour = Integer.parseInt(hourStr);
        int minute = Integer.parseInt(minuteStr);
        int second = Integer.parseInt(secondStr);
        Calendar ca = Calendar.getInstance();
        ca.set(year, month, day, hour, minute, second);
        long timestamp = ca.getTimeInMillis();
        if (timestamp / 1000 < Integer.MAX_VALUE) {
            return (int) (timestamp / 1000);
        } else {
            throw new RuntimeException("timestamp larger than Integer.MAX_VALUE, this should not happen");
        }
    }

    public static int timeStr2int(String tStr) {
        if (tStr.length() != 12 && tStr.length() != 14) {
            throw new RuntimeException("timestamp format error!");
        } else if (tStr.length() == 12) {
            return timeStr2intImpl(tStr + "00");
        } else {
            return timeStr2intImpl(tStr);
        }
    }

    public static String timeStamp2String(final int timestamp) {
        Calendar ca = Calendar.getInstance();
        ca.setTimeInMillis(((long) timestamp) * 1000);
        return ca.get(Calendar.YEAR) + "-" + (ca.get(Calendar.MONTH) + 1) + "-" + ca.get(Calendar.DAY_OF_MONTH) + " " +
                String.format("%02d", ca.get(Calendar.HOUR_OF_DAY)) + ":" +
                String.format("%02d", ca.get(Calendar.MINUTE));
    }

    public static <E> PeekingIterator<E> emptyIterator() {
        return new PeekingIterator<E>() {
            @Override
            public boolean hasNext() {
                return false;
            }

            @Override
            public E peek() {
                throw new RuntimeException("empty Iterator!");
            }

            @Override
            public E next() {
                throw new RuntimeException("empty Iterator!");
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException();
            }
        };
    }

    public static List<File> trafficFileList(String dir, String fileStart, String fileEnd) {
        Calendar c = Calendar.getInstance();
        c.setTimeInMillis(monthDayStr2Time(fileStart));
        long endT = monthDayStr2Time(fileEnd);
        File folder = new File(dir);
        if (!folder.exists() && !folder.mkdirs()) throw new RuntimeException("can not create dir.");
        List<String> files = new ArrayList<>();
        while (c.getTimeInMillis() <= endT) {
            files.add(String.format("%02d%02d.csv.gz", c.get(Calendar.MONTH) + 1, c.get(Calendar.DAY_OF_MONTH)));
            c.add(Calendar.HOUR, 24);
        }
        return files.stream().map(s -> new File(folder, s)).collect(Collectors.toList());
    }

    public static List<File> downloadTrafficFiles(String dir, String fileStart, String fileEnd) throws IOException {
        Calendar c = Calendar.getInstance();
        c.setTimeInMillis(monthDayStr2Time(fileStart));
        long endT = monthDayStr2Time(fileEnd);
        File folder = new File(dir);
        if (!folder.exists() && !folder.mkdirs()) throw new RuntimeException("can not create dir.");
        List<File> files = new ArrayList<>();
        while (c.getTimeInMillis() <= endT) {
            File f = new File(folder, String.format("%02d%02d.csv.gz", c.get(Calendar.MONTH) + 1, c.get(Calendar.DAY_OF_MONTH)));
            files.add(f);
            if (!f.exists()) download("http://amitabha.water-crystal.org/TGraphDemo/bj-traffic/" + f.getName(), f);
            c.add(Calendar.HOUR, 24);
        }
        return files;
    }

    public static long monthDayStr2Time(String monthDayStr) {
        Calendar c = Calendar.getInstance();
        int month = Integer.parseInt(monthDayStr.substring(0, 2)) - 1;
        int day = Integer.parseInt(monthDayStr.substring(2, 4));
        c.set(2010, month, day, 0, 0);
        return c.getTimeInMillis();
    }

    public static int monthDayStr2TimeInt(String monthDayStr) {
        return ((int) (monthDayStr2Time(monthDayStr) / 1000L));
    }

    public static String currentCodeVersion() {
        RuntimeEnv env = RuntimeEnv.getCurrentEnv();
        return env.name() + "." + codeGitVersion();
    }

    public static File download(String url, File out) throws IOException {
        if (out.exists() && out.isFile()) {
            return out;
        }
        URL website = new URL(url);
        ReadableByteChannel rbc = Channels.newChannel(website.openStream());
        FileOutputStream fos = new FileOutputStream(out);
        fos.getChannel().transferFrom(rbc, 0, Long.MAX_VALUE);
        return out;
    }


    public static File decompressGZip(File input, File outFile) throws IOException {
        try (GzipCompressorInputStream in = new GzipCompressorInputStream(new FileInputStream(input))) {
            IOUtils.copy(in, new FileOutputStream(outFile));
        }
        return outFile;
    }

    public static BufferedReader gzipReader(File file) throws IOException {
        return new BufferedReader(new InputStreamReader(new GZIPInputStream(new FileInputStream(file))));
    }

//    public static int time2int(TimePoint timestamp){
//        return timestamp.valInt();
//    }

//    public static TimePoint time(int timestamp){
//        return new TimePoint(timestamp);
//    }

    public static String mustEnv(String name) {
        String val = System.getenv(name);
        Preconditions.checkNotNull(val);
        return val;
    }

    public static <T> Triple<Set<T>, Integer, Set<T>> compareSets(Collection<T> correct_c, Collection<T> input_c) {
        Set<T> correct = new HashSet<>(correct_c);
        Set<T> input = new HashSet<>(input_c);
        Set<T> common = new HashSet<>(input);
        common.retainAll(correct);
        correct.removeAll(common);
        input.removeAll(common);
        return Triple.of(correct, common.size(), input);
    }

    public static <T> boolean validateResult(Collection<T> correct_c, Collection<T> input_c) {
        Triple<Set<T>, Integer, Set<T>> r = compareSets(correct_c, input_c);
        Set<T> correctDiff = r.getLeft();
        Set<T> inputDiff = r.getRight();
        if (!correctDiff.isEmpty() || !inputDiff.isEmpty()) {
            StringBuilder sb = new StringBuilder();
            sb.append("-").append(correctDiff.size()).append(", ").append(r.getMiddle()).append(", +").append(inputDiff.size()).append(" [");
            int i = 0;
            for (T p : correctDiff) {
                sb.append(p).append(", ");
                if (++i > 2) break;
            }
            sb.append(" | ");
            i = 0;
            for (T p : inputDiff) {
                sb.append(p).append(", ");
                if (++i > 2) break;
            }
            System.out.println(sb);
            return false;
        } else {
            return true;
        }
    }

    public static int getFileTime(File file) {
        return Integer.parseInt(file.getName().substring(10, 21));
    }
}