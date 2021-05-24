package benchmark.model;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;


/**
 * Created by song on 2019-12-27.
 */
public class RoadRel {
    public final String id;
    public final int length;
    public final int angle;

    public int type;
    private int inNum = 0;
    private int outNum = 0;

    public final List<RoadRel> inChains = new ArrayList<>();
    public final List<RoadRel> outChains = new ArrayList<>();
    public final TemporalValue<Byte> tpJamStatus = new TemporalValue<>();
    public final TemporalValue<Byte> tpSegCount = new TemporalValue<>();
    public final TemporalValue<Integer> tpTravelTime = new TemporalValue<>();
    public final TemporalValue<Integer> updateCount = new TemporalValue<>();

    public static void createOrUpdate(Map<String, RoadRel> map, String line) {
        // field[0] and field[3] are useless for identify a unique road.
        String[] fields = line.split(",");
        String id = buildId(fields[1], fields[2]);
        RoadRel road = map.get(id);
        if (road == null) {
            road = new RoadRel(id, Integer.parseInt(fields[4]), Integer.parseInt(fields[8]));
            map.put(id, road);
        }
        road.type = Byte.parseByte(fields[5]);
        road.inNum = Integer.parseInt(fields[6]);
        road.outNum = Integer.parseInt(fields[7]);
        for (int i = 0; i < road.inNum + road.outNum; i++) {
            String neighborId = buildId(fields[9 + i * 5], fields[10 + i * 5]);
            RoadRel neighbor = map.get(neighborId);
            if (neighbor == null) {
                neighbor = new RoadRel(neighborId, Integer.parseInt(fields[12 + i * 5]), Integer.parseInt(fields[13 + i * 5]));
                // fields[11 + i*5] is useless for identify a unique road.
                map.put(neighborId, neighbor);
            }
            if (i < road.inNum) {
                road.inChains.add(neighbor);
            } else {
                road.outChains.add(neighbor);
            }
        }
    }

    public RoadRel(String roadId, int length, int angle) {
        this.id = roadId;
        this.length = length;
        this.angle = angle;
    }

    public String toString() {
        return "(" + id + ")," + "LEN" + length + ",TYPE" + type + ",ANGLE" + angle + ",IN" + list2String(inChains) + ",OUT" + list2String(outChains);
    }

    public int getOutNum() {
        return outNum;
    }

    public int getInNum() {
        return inNum;
    }

    public int getType() {
        return type;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        RoadRel roadRel = (RoadRel) o;
        return Objects.equals(id, roadRel.id);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id);
    }

    public static String buildId(String gridId, String chainId) {
        StringBuilder sb = new StringBuilder(gridId);
        sb.append('_');
        for (int i = chainId.length(); i < 5; i++) {
            sb.append('0');
        }
        sb.append(chainId);
        return sb.toString();
    }

    public static String list2String(List<RoadRel> list) {
        if (list.isEmpty()) return "[]";
        StringBuilder result = new StringBuilder();
        for (RoadRel roadChain : list) {
            result.append(",").append(roadChain.id);
        }
        return "[" + result.substring(1) + "]";
    }
}
