package benchmark.transaction.generation;

import com.google.common.collect.AbstractIterator;
import benchmark.transaction.definition.AbstractTransaction;
import benchmark.transaction.definition.ImportStaticDataTx;
import benchmark.transaction.definition.ImportTemporalDataTx;
import benchmark.model.*;
import benchmark.utils.Helper;
import benchmark.utils.TrafficMultiFileReader;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Generate an instance of benchmark (a iterator/list of transactions) from given arguments.
 */
public class BenchmarkTxGenerator {
    public static void main(String[] args) {
        String workDir = Helper.mustEnv("WORK_DIR");
        String benchmarkFileName = Helper.mustEnv("BENCHMARK_FILE_OUTPUT");
        int temporalDataPerTx = Integer.parseInt(Helper.mustEnv("TEMPORAL_DATA_PER_TX"));
        String temporalDataStartT = Helper.mustEnv("TEMPORAL_DATA_START");
        String temporalDataEndT = Helper.mustEnv("TEMPORAL_DATA_END");
        int reachableAreaTxCnt = Integer.parseInt(Helper.mustEnv("REACHABLE_AREA_TX_CNT"));
        try {
            BenchmarkWriter writer = new BenchmarkWriter(new File(workDir, benchmarkFileName + ".gz"));
            //静态数据写入事务1个
            TrafficTemporalPropertyGraph tgraph = new TrafficTemporalPropertyGraph();
            tgraph.importTopology(new File(workDir, "road_topology.csv.gz"));
            writer.write(txImportStatic(tgraph));
            //时态数据写入事务多个
            List<File> temporalDataFiles = Helper.trafficFileList(workDir, temporalDataStartT, temporalDataEndT);
            writer.write(new TemporalPropertyAppendTxGenerator(temporalDataPerTx, temporalDataFiles));
            //Snapshots事务多个
            //ReachableArea事务多个
//            writer.write((Helper.monthDayStr2TimeInt(temporalDataStartT), Helper.monthDayStr2TimeInt(temporalDataEndT), reachableAreaTxCnt));
            writer.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static AbstractTransaction txImportStatic(TrafficTemporalPropertyGraph tgraph) {
        final Map<CrossNode, Long> crossIdMap = new HashMap<>();
        final List<ImportStaticDataTx.StaticCrossNode> crosses = new ArrayList<>();
        final List<ImportStaticDataTx.StaticRoadRel> roads = new ArrayList<>();
        long crossId = 0, roadId = 0;
        for (CrossNode cross : tgraph.getAllCross()) {
            ImportStaticDataTx.StaticCrossNode node = new ImportStaticDataTx.StaticCrossNode();
            node.setId(crossId);
            node.setName(cross.name);
            crosses.add(node);
            crossIdMap.put(cross, crossId);
            crossId++;
        }
        for (RoadRel r : tgraph.getAllRoads()) {
            roads.add(new ImportStaticDataTx.StaticRoadRel(
                    roadId,
                    crossIdMap.get(tgraph.getRoadEndCross(r)),
                    crossIdMap.get(tgraph.getRoadStartCross(r)),
                    r.id, r.length, r.angle, r.getType()));
//            roadIdMap.put(r.id, roadId);
            roadId++;
        }
        return new ImportStaticDataTx(crosses, roads);
    }

    //为何要实现为iterator？因为事务之间是有顺序的
    //这个没有用带缓存的文件读取器（TrafficDataReader），可能会偶尔卡一下。
    public static class TemporalPropertyAppendTxGenerator extends AbstractIterator<AbstractTransaction> implements AutoCloseable {
        final int linePerTx;
        private final TrafficMultiFileReader data;

        public TemporalPropertyAppendTxGenerator(int linePerTx, List<File> files) {
            this.linePerTx = linePerTx;
            this.data = new TrafficMultiFileReader(files);
        }

        @Override
        protected AbstractTransaction computeNext() {
            List<StatusUpdate> lines = new ArrayList<>();
            for (int i = 0; i < linePerTx && data.hasNext(); i++) {
                lines.add(data.next());
            }
            if (!lines.isEmpty()) {
                return new ImportTemporalDataTx(lines);
            } else {
                return endOfData();
            }
        }

        @Override
        public void close() {
            data.close();
        }
    }

}
