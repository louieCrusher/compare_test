package benchmark.model;

import java.util.*;

/**
 * Created by song on 19-12-26.
 */
public class CrossNode {

    public final String name;
    public final Set<RoadRel> outRoads;
    public final Set<RoadRel> inRoads;


    public CrossNode(Set<RoadRel> inRoads, Set<RoadRel> outRoads) {
        this.inRoads = inRoads;
        this.outRoads = outRoads;
        this.name = getCrossId(inRoads, outRoads);
    }

    public CrossNode(String name) {
        this.inRoads = new HashSet<>();
        this.outRoads = new HashSet<>();
        this.name = name;
    }

    /**
     * we have to do this because the data has some 'strange' case:
     * CASE 1:
     * let A,B,C,D,E be roads.
     * we found A->C, A->D, B->C, B->E in source data,
     * but there are NO: A->E or B->D!
     * CASE 2:
     * let A,B,C,D,E be roads.
     * we found A->C, B->C, B->D, E->D in source data,
     * but there are NO: A->D or E->C!
     * So we have to repeat filling until we get AB->CDE in CASE 1,
     * and get ABE->CD in CASE 2.
     *
     * @param inSet  roads go into a cross
     * @param outSet roads come from a cross
     */
    public static void fillInOutRoadSetToFull(Set<RoadRel> inSet, Set<RoadRel> outSet) {
        int inSize = 0, outSize = 0;
        while (inSet.size() > inSize || outSet.size() > outSize) {
            inSize = inSet.size();
            outSize = outSet.size();
            for (RoadRel road : outSet) {
                inSet.addAll(road.inChains);
            }
            for (RoadRel road : inSet) {
                outSet.addAll(road.outChains);
            }
        }
    }

    private String getCrossId(Set<RoadRel> inRoads, Set<RoadRel> outRoads) {
        List<RoadRel> inChains = new ArrayList<>(inRoads);
        List<RoadRel> outChains = new ArrayList<>(outRoads);
        inChains.sort(Comparator.comparing(o -> o.id));
        outChains.sort(Comparator.comparing(o -> o.id));
        String inChainStr = RoadRel.list2String(inChains);
        String outChainStr = RoadRel.list2String(outChains);
        return inChainStr + "|" + outChainStr;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        CrossNode crossNode = (CrossNode) o;
        return name.equals(crossNode.name);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name);
    }
}