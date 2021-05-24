package benchmark.model;

import com.google.common.base.Preconditions;

import java.util.Calendar;
import java.util.Objects;
import java.util.TimeZone;

public class TimePointInt implements Comparable<TimePointInt> {
    private static final int INIT_VAL = -2;
    private static final int NOW_VAL = Integer.MAX_VALUE;
    public static final TimePointInt Now = new TimePointInt(true) {
        @Override
        public boolean isNow() {
            return true;
        }

        @Override
        public boolean isInit() {
            return false;
        }

        @Override
        public TimePointInt pre() {
            throw new UnsupportedOperationException("should not call pre on TimePoint.NOW");
        }

        @Override
        public TimePointInt next() {
            throw new UnsupportedOperationException("should not call next on TimePoint.NOW");
        }

        @Override
        public String toString() {
            return "NOW";
        }
    };
    public static final TimePointInt Init = new TimePointInt(false) {
        @Override
        public boolean isNow() {
            return false;
        }

        @Override
        public boolean isInit() {
            return true;
        }

        @Override
        public TimePointInt pre() {
            throw new UnsupportedOperationException("should not call pre on TimePoint.INIT");
        }

        @Override
        public TimePointInt next() {
            throw new UnsupportedOperationException("should not call next on TimePoint.INIT");
        }

        @Override
        public String toString() {
            return "INIT";
        }
    };

    protected int time;

    public TimePointInt(int time) {
        Preconditions.checkArgument(time >= 0 && time < NOW_VAL - 1, "invalid time value " + time + ", only support 0 to " + (NOW_VAL - 2));
        this.time = time;
    }

    // this constructor is used for now and init only.
    TimePointInt(boolean isNow) {
        if (isNow) this.time = NOW_VAL;
        else this.time = INIT_VAL;
    }

    public TimePointInt pre() {
        return new TimePointInt(time - 1);
    }

    public TimePointInt next() {
        return new TimePointInt(time + 1);
    }

    public boolean isNow() {
        return false;
    }

    public boolean isInit() {
        return false;
    }

    public int val() {
        return time;
    }

    @Override
    public int compareTo(TimePointInt o) {
        return Integer.compare(val(), o.val());
    }

    @Override
    public String toString() {
        return String.valueOf(val());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TimePointInt that = (TimePointInt) o;
        return time == that.time;
    }

    @Override
    public int hashCode() {
        return Objects.hash(time);
    }

    public Calendar toCalendar() {
        Calendar c = Calendar.getInstance(TimeZone.getTimeZone("GMT+08:00"));
        c.setTimeInMillis(time * 1000L);
        return c;
    }
}