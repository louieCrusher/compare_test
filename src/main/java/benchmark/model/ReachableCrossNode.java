package benchmark.model;

import java.util.Objects;

public class ReachableCrossNode {
    protected enum Status {NotCalculate, Calculating, Calculated}

    public long id;
    int arriveTime = Integer.MAX_VALUE;
    long parent;
    private Status status = Status.NotCalculate;

    public ReachableCrossNode(long id) {
        this.id = id;
    }

    ReachableCrossNode() {
    }

    public ReachableCrossNode(long id, int arriveTime) {
        this.id = id;
        this.arriveTime = arriveTime;
    }

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }

    public int getArriveTime() {
        return arriveTime;
    }

    public void setArriveTime(int arriveTime) {
        this.arriveTime = arriveTime;
    }

    public long getParent() {
        return parent;
    }

    public void setParent(long parent) {
        this.parent = parent;
    }


    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ReachableCrossNode that = (ReachableCrossNode) o;
        return id == that.id &&
                arriveTime == that.arriveTime &&
                parent == that.parent &&
                status == that.status;
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, arriveTime, parent, status);
    }

    @Override
    public String toString() {
        return "ReachableCrossNode {" +
                "id = " + id +
                ", arriveTime = " + arriveTime +
                ", parent = " + parent +
                ", status = " + status +
                '}';
    }
}
