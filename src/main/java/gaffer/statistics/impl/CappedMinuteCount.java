/**
 * Copyright 2015 Crown Copyright
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * 	http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package gaffer.statistics.impl;

import gaffer.utils.DateUtilities;
import gaffer.statistics.Statistic;
import org.apache.hadoop.io.WritableUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * A {@link Statistic} that stores counts for minutes (e.g. a count of 5 for Jan 1st 1982, 12:01, and a count of 10 for
 * Jan 2nd 1982, 13:01). As this could take a lot of storage, a maximum number of entries is specified at creation time.
 * If more than this number of entries are entered, then the set marks itself as full and removes all the entries.
 */
public class CappedMinuteCount implements Statistic {

    private final static SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("yyyyMMddHHmm");
    static {
        DATE_FORMAT.setCalendar(new GregorianCalendar(SimpleTimeZone.getTimeZone("UTC"), Locale.UK));
    }

    private static final long serialVersionUID = -3018304334468951325L;

    private int maxEntries;
    private boolean full;
    private SortedMap<Integer, Long> minuteToCount = new TreeMap<Integer, Long>();

    /**
     * Initialises this to have an unlimited maximum number of entries.
     */
    public CappedMinuteCount() {
        this(Integer.MAX_VALUE);
    }

    /**
     * Initialises this to have the specified maximum number of entries.
     *
     * @param maxEntries
     */
    public CappedMinuteCount(int maxEntries) {
        if (maxEntries < 1L) {
            throw new IllegalArgumentException("Cannot construct a " + this.getClass().getSimpleName()
                    + " with the maximum number of entries less than 1");
        }
        this.maxEntries = maxEntries;
        this.full = false;
    }

    /**
     * Initialises this to have the specified maximum number of entries, with a count of 1 at the minute
     * corresponding to the given {@link Date}.
     *
     * @param maxEntries
     */
    public CappedMinuteCount(int maxEntries, Date date) {
        this(maxEntries);
        increment(date);
    }

    /**
     * Initialises this to have the specified maximum number of entries, with a count of <code>count</code>
     * at the minute corresponding to the given {@link Date}.
     *
     * @param maxEntries
     */
    public CappedMinuteCount(int maxEntries, Date date, long count) {
        this(maxEntries);
        increment(date, count);
    }

    /**
     * Increments the count at the minute corresponding to the given {@link Date} by 1.
     *
     * @param date
     */
    public void increment(Date date) {
        increment(date, 1L);
    }

    /**
     * Increments the count at the minute corresponding to the given {@link Date} by <code>count</code>.
     *
     * @param date
     */
    public void increment(Date date, long count) {
        increment(getMinuteFromDate(date), count);
    }

    private void increment(int minuteAsInt, long value) {
        if (full) {
            return;
        }
        if (minuteToCount.containsKey(minuteAsInt)) {
            minuteToCount.put(minuteAsInt, minuteToCount.get(minuteAsInt) + value);
        } else {
            minuteToCount.put(minuteAsInt, value);
        }
        emptyIfFull();
    }

    /**
     * Returns the maximum number of entries (i.e. the maximum number of distinct minutes) that this
     * can have counts for.
     *
     * @return
     */
    public int getMaxEntries() {
       return maxEntries;
    }

    /**
     * Returns the count associated with the minute represented by the given {@link Date}.
     *
     * @param date
     * @return
     */
    public long getCount(Date date) {
        return minuteToCount.get(getMinuteFromDate(date));
    }

    /**
     * Given an int of the number of minutes since the epoch, returns the associated count.
     *
     * @param minutesSinceEpoch
     * @return
     */
    public long getCount(int minutesSinceEpoch) {
        return minuteToCount.get(minutesSinceEpoch);
    }

    /**
     * Returns <code>true</code> if the maximum number of entries has been exceeded.
     *
     * @return
     */
    public boolean isFull() {
        return full;
    }

    /**
     * Returns all active minutes, i.e. all minutes for which the count is greater than 0, in
     * increasing order of {@link java.util.Date}.
     *
     * @return
     */
    public Date[] getActiveMinutes() {
        int numActiveMinutes = minuteToCount.keySet().size();
        Date[] activeMinutes = new Date[numActiveMinutes];
        int count = 0;
        for (int i : minuteToCount.keySet()) {
            activeMinutes[count] = getDateFromMinute(i);
            count++;
        }
        return activeMinutes;
    }

    /**
     * Returns the number of active minutes, i.e. minutes for which the count is positive. If the set is full then 0
     * will be returned, so this method should be used with {@link #isFull()}.
     *
     * @return
     */
    public int getNumberOfActiveMinutes() {
        return minuteToCount.size();
    }

    /**
     * Removes all entries from the map from minute to count, and ensures that it is marked as not full.
     */
    public void clear() {
        full = false;
        minuteToCount.clear();
    }

    @Override
    public void merge(Statistic s) throws IllegalArgumentException {
        if (s instanceof CappedMinuteCount) {
            CappedMinuteCount other = (CappedMinuteCount) s;
            // Can only merge two CappedMinuteCounts with the same maxSize.
            if (maxEntries != other.maxEntries) {
                throw new IllegalArgumentException("Can't merge a CappedMinuteCount with maxEntries of "
                        + other.maxEntries + " with this one of maxEntries " + maxEntries);
            }
            // If this is already full, or the other is full, then the merged one must be full.
            if (full || other.full) {
                full = true;
                minuteToCount.clear(); // Need to do this if other is full.
                return;
            }
            // Otherwise add all of the items in the other set.
            for (int minute : other.minuteToCount.keySet()) {
                increment(minute, other.minuteToCount.get(minute));
            }
        } else {
            throw new IllegalArgumentException("Trying to merge a Statistic of type " + s.getClass()
                    + " with a " + this.getClass());
        }
    }

    @Override
    public CappedMinuteCount clone() {
        CappedMinuteCount clone = new CappedMinuteCount(maxEntries);
        if (full) {
            clone.full = true;
        } else {
            for (int i : this.minuteToCount.keySet()) {
                clone.minuteToCount.put(i, this.minuteToCount.get(i));
            }
        }
        return clone;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        /**
         * Write out:
         *  - the max number of entries
         *  - whether this is full or not
         *  - if not full then write:
         *      - the number of entries in the map int -> long
         *      - the smallest int in the map and its long
         *      - from then on write the next int in the map by subtracting the smallest int
         *      (this will make the int smaller and hence potentially more compact to write out
         *      using the WritableUtils method) and its long
         */
        WritableUtils.writeVInt(out, maxEntries);
        out.writeBoolean(full);
        if (!full) {
            int size = minuteToCount.keySet().size();
            WritableUtils.writeVInt(out, size);
            int smallestInt = -1;
            for (int i : minuteToCount.keySet()) {
                if (smallestInt == -1) {
                    smallestInt = i;
                    WritableUtils.writeVInt(out, smallestInt);
                } else {
                    WritableUtils.writeVInt(out, i - smallestInt);
                }
                WritableUtils.writeVLong(out, minuteToCount.get(i));
            }
        }
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        minuteToCount.clear();
        maxEntries = WritableUtils.readVInt(in);
        full = in.readBoolean();
        if (!full) {
            int numberOfEntries = WritableUtils.readVInt(in);
            int smallestInt = -1;
            for (int i = 0; i < numberOfEntries; i++) {
                if (i == 0) {
                    smallestInt = WritableUtils.readVInt(in);
                    minuteToCount.put(smallestInt, WritableUtils.readVLong(in));
                } else {
                    minuteToCount.put(WritableUtils.readVInt(in) + smallestInt, WritableUtils.readVLong(in));
                }
            }
        }
    }

    private void emptyIfFull() {
        if (minuteToCount.keySet().size() > maxEntries) {
            minuteToCount.clear();
            full = true;
        }
    }

    private static int getMinuteFromDate(Date date) {
        return DateUtilities.getMinutesSinceEpoch(date);
    }

    private static Date getDateFromMinute(int minute) {
        return DateUtilities.getDateFromMinutesSinceEpoch(minute);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("MinuteCount {");
        if (full) {
            sb.append(">").append(maxEntries);
            return sb.toString();
        }
        for (int i : minuteToCount.keySet()) {
            sb.append(DATE_FORMAT.format(getDateFromMinute(i)));
            sb.append("=");
            sb.append(minuteToCount.get(i));
            sb.append(", ");
        }
        return sb.substring(0, sb.length() - 2) + "}";
    }


    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        CappedMinuteCount that = (CappedMinuteCount) o;

        if (full != that.full) return false;
        if (maxEntries != that.maxEntries) return false;
        if (minuteToCount != null ? !minuteToCount.equals(that.minuteToCount) : that.minuteToCount != null)
            return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = maxEntries;
        result = 31 * result + (full ? 1 : 0);
        result = 31 * result + (minuteToCount != null ? minuteToCount.hashCode() : 0);
        return result;
    }
}
