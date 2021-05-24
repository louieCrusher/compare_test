package benchmark.transaction.definition;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class NodeNeighborRoadTx extends AbstractTransaction {

    private long nodeId;

    public NodeNeighborRoadTx(long nodeId, List<String> roadIds) {
        this.setTxType(TxType.TX_QUERY_NODE_NEIGHBOR_ROAD);
        this.nodeId = nodeId;
        Result r = new Result();
        r.setRoadIds(roadIds);
        this.setResult(r);
    }

    public NodeNeighborRoadTx() {
        this.setTxType(TxType.TX_QUERY_NODE_NEIGHBOR_ROAD);
    }

    public long getNodeId() {
        return nodeId;
    }

    public void setNodeId(long nodeId) {
        this.nodeId = nodeId;
    }

    @Override
    public void validateResult(AbstractTransaction.Result result) {
        List<String> got = ((Result) result).getRoadIds();
        List<String> expected = ((Result) this.getResult()).getRoadIds();
        if (got.size() != expected.size()) {
            System.out.println("size not match, got " + got + " expect " + expected + " for node " + nodeId);
        } else {
            if (!got.isEmpty()) {
                HashSet<String> intersection = new HashSet<>(got);
                intersection.retainAll(expected);
                Set<String> gotS = new HashSet<>(got);
                gotS.removeAll(intersection);
                Set<String> expS = new HashSet<>(expected);
                expS.removeAll(intersection);
                if (!gotS.isEmpty() || !expS.isEmpty()) {
                    System.out.println("result not match, got " + got + " expect " + expected + " for node " + nodeId);
                }
            }
        }

    }

    public static class Result extends AbstractTransaction.Result {
        List<String> roadIds;

        public List<String> getRoadIds() {
            return roadIds;
        }

        public void setRoadIds(List<String> roadIds) {
            this.roadIds = roadIds;
        }
    }
}
