package benchmark.utils;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;

/**
 * Created by song on 16-5-3.
 */
public abstract class TransactionWrapper<T> {
    private T returnValue;
    private boolean willCommit;
    protected Hook<T> finishHook;

    public TransactionWrapper() {
        this(true);
    }

    public TransactionWrapper(boolean willCommit) {
        this.willCommit = willCommit;
    }

    public abstract void runInTransaction();

    public TransactionWrapper start(GraphDatabaseService db) {
        try (Transaction tx = db.beginTx()) {
            runInTransaction();
            if (willCommit) tx.success();
        }
        onFinish(getReturnValue());
        if (finishHook != null) finishHook.handler(getReturnValue());
        return this;
    }

    protected void setReturnValue(T value) {
        returnValue = value;
    }

    public T getReturnValue() {
        return returnValue;
    }

    protected void onFinish(Object returnValue) {
    }

    public void onFinish(Hook<T> hook) {
        this.finishHook = hook;
    }
}
