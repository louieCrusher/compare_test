package benchmark.model;

import com.google.common.collect.AbstractIterator;
import com.google.common.collect.Iterators;
import com.google.common.collect.PeekingIterator;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.lang3.tuple.Triple;
import org.parboiled.common.Preconditions;

import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentSkipListMap;

/**
 * Created by song on 2019-12-28.
 * 用于表示一个时态值：（时间区间，值）元组构成的序列，时间区间不相交
 * 实现时用的是（时间点，值）组成的TreeMap，注意其中时间点不含有TimePoint.NOW，但可以有TimePoint.Init
 * 此处的TemporalValue不考虑Unknown的问题（没有多层需要查询），也不考虑Invalid的问题。
 */
public class TemporalValue<V> {
    private final NavigableMap<TimePointInt, V> map = new ConcurrentSkipListMap<>(); //new TreeMap<>();

    public void set(TimePointInt start, TimePointInt end, V value) {
        Preconditions.checkNotNull(start);
        Preconditions.checkNotNull(end);
        Preconditions.checkNotNull(value);
        Preconditions.checkArgument(start.compareTo(end) <= 0, "invalid time interval! got [" +start + ", " + end + "]");
        V endVal = map.get(end.next());
        map.subMap(start, true, end, true).clear();
        map.put(start, value);
        map.put(end.next(), endVal);
    }

    public void setToNow(TimePointInt start, V value) {
        assert start != null && value != null;
        map.tailMap(start, true).clear();
        map.put(start, value);
    }

    public V get(TimePointInt time) {
        Entry<TimePointInt, V> entry = map.floorEntry(time);
        if (entry!=null) {
            return entry.getValue();
        } else{
            return null;
        }
    }

    public PeekingIterator<Triple<TimePointInt, TimePointInt, V>> intervalEntries() {
        return new IntervalIterator(map);
    }

    public PeekingIterator<Triple<TimePointInt, TimePointInt, V>> intervalEntries(TimePointInt start, TimePointInt end) {
        TimePointInt floor = map.floorKey(start);
        if (floor != null) {
            return new IntervalIterator(map.subMap(floor, true, end, true));
        } else {
            return new IntervalIterator(map.subMap(start, true, end, true));
        }
    }

    public int mergeSameVal() {
        V lastVal = null;
        List<TimePointInt> toRemove = new ArrayList<>();
        for (Entry<TimePointInt, V> entry: map.entrySet()) {
            if (lastVal!=null && lastVal.equals(entry.getValue())) {
                toRemove.add(entry.getKey());
            }
            lastVal = entry.getValue();
        }
        for (TimePointInt t : toRemove) {
            map.remove(t);
        }
        return toRemove.size();
    }

    public TimePointInt latestTime() {
        try {
            return map.lastKey();
        } catch (NoSuchElementException e) {
            return null;
        }
    }

    public PeekingIterator<Entry<TimePointInt,V>> pointEntries() {
        return Iterators.peekingIterator(Iterators.transform( map.entrySet().iterator(), item -> Pair.of( item.getKey(), item.getValue())));
    }

    public PeekingIterator<Entry<TimePointInt,V>> pointEntries(TimePointInt startTime ) {
        Entry<TimePointInt, V> floor = map.floorEntry( startTime );
        if ( floor != null ) {
            return Iterators.peekingIterator( map.tailMap( floor.getKey(), true ).entrySet().iterator() );
        } else {
            return Iterators.peekingIterator(new Iterator<Entry<TimePointInt, V>>() {
                @Override public boolean hasNext() { return false; }
                @Override public Entry<TimePointInt, V> next() { return null; }
            });
        }
    }

    public boolean isEmpty() {
        return map.isEmpty();
    }

    public TemporalValue<V> slice(TimePointInt min, boolean includeMin, V max, boolean includeMax ) {
        return null;
    }

    private class IntervalIterator extends AbstractIterator<Triple<TimePointInt, TimePointInt, V>> implements PeekingIterator<Triple<TimePointInt, TimePointInt, V>>{
        PeekingIterator<Entry<TimePointInt, V>> iterator;
        IntervalIterator(NavigableMap<TimePointInt, V> map){
            this.iterator = Iterators.peekingIterator(map.entrySet().iterator());
        }
        @Override
        protected Triple<TimePointInt, TimePointInt, V> computeNext () {
            if (iterator.hasNext()) {
                Entry<TimePointInt, V> start = iterator.next();
                if (iterator.hasNext()) {
                    Entry<TimePointInt, V> endNext = iterator.peek();
                    return Triple.of(start.getKey(), endNext.getKey().pre(), start.getValue());
                } else {
                    return Triple.of(start.getKey(), TimePointInt.Now, start.getValue());
                }
            } else {
                return endOfData();
            }
        }
    }
}
