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
 * A {@link Statistic} that stores counts for days (e.g. a count of 10 for Jan 1st 1982, and a count of 20 for
 * Jan 2nd 1982).
 */
public class DailyCount implements Statistic {

    private final static SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("yyyyMMdd");
    static {
        DATE_FORMAT.setCalendar(new GregorianCalendar(SimpleTimeZone.getTimeZone("UTC"), Locale.UK));
    }

    private static final long serialVersionUID = -8438582942345754371L;

    private Map<Integer, Long> dayToCount;

    /**
     * Initialises with empty set of counts.
     */
    public DailyCount() {
        dayToCount = new HashMap<Integer, Long>();
    }

    /**
     * Initialises with a value of count associated to the day corresponding to the
     * given {@link Date}.
     *
     * @param date
     * @param count
     */
    public DailyCount(Date date, long count) {
        this();
        dayToCount.put(DateUtilities.getDaysSinceEpoch(date), count);
    }

    /**
     * Increments the count for the day corresponding to the given {@link Date} by 1.
     *
     * @param date
     */
    public void increment(Date date) {
        increment(date, 1L);
    }

    /**
     * Increments the count for the day corresponding to the given {@link Date} by
     * count.
     *
     * @param date
     * @param count
     */
    public void increment(Date date, long count) {
        int dayAsInt = DateUtilities.getDaysSinceEpoch(date);
        if (dayToCount.containsKey(dayAsInt)) {
            dayToCount.put(dayAsInt, dayToCount.get(dayAsInt) + count);
        } else {
            dayToCount.put(dayAsInt, count);
        }
    }

    /**
     * Returns the count associated with the day represented by the given {@link Date}.
     *
     * @param date
     * @return
     */
    public long getCount(Date date) {
        return dayToCount.get(DateUtilities.getDaysSinceEpoch(date));
    }

    /**
     * Given an int of the number of days since the epoch, returns the associated count.
     *
     * @param daysSinceEpoch
     * @return
     */
    public long getCount(int daysSinceEpoch) {
        return dayToCount.get(daysSinceEpoch);
    }

    /**
     * Returns all active days, i.e. all days for which the count is greater than 0, in
     * increasing order of {@link Date}.
     *
     * @return
     */
    public Date[] getActiveDays() {
        SortedMap<Integer, Long> sortedMap = new TreeMap<Integer, Long>(dayToCount);
        Date[] activeDays = new Date[sortedMap.keySet().size()];
        int count = 0;
        for (int i : sortedMap.keySet()) {
            activeDays[count] = DateUtilities.getDateFromDaysSinceEpoch(i);
            count++;
        }
        return activeDays;
    }

    @Override
    public void merge(Statistic s) throws IllegalArgumentException {
        if (s instanceof DailyCount) {
            DailyCount otherDailyCount = (DailyCount) s;
            for (int i : otherDailyCount.dayToCount.keySet()) {
                if (!this.dayToCount.containsKey(i)) {
                    this.dayToCount.put(i, otherDailyCount.dayToCount.get(i));
                } else {
                    this.dayToCount.put(i, this.dayToCount.get(i) + otherDailyCount.dayToCount.get(i));
                }
            }
        } else {
            throw new IllegalArgumentException("Trying to merge a Statistic of type " + s.getClass()
                    + " with a " + this.getClass());
        }
    }

    @Override
    public DailyCount clone() {
        DailyCount clone = new DailyCount();
        for (int i : this.dayToCount.keySet()) {
            clone.dayToCount.put(i, this.dayToCount.get(i));
        }
        return clone;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        WritableUtils.writeVInt(out, dayToCount.keySet().size());
        for (int i : dayToCount.keySet()) {
            WritableUtils.writeVInt(out, i);
            WritableUtils.writeVLong(out, dayToCount.get(i));
        }
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        int numberOfEntries = WritableUtils.readVInt(in);
        dayToCount.clear();
        for (int i = 0; i < numberOfEntries; i++) {
            dayToCount.put(WritableUtils.readVInt(in), WritableUtils.readVLong(in));
        }
    }

    @Override
    public String toString() {
        // Sort the map before printing
        SortedMap<Integer, Long> sortedMap = new TreeMap<Integer, Long>(dayToCount);
        StringBuilder sb = new StringBuilder("DailyCount {");
        for (int i : sortedMap.keySet()) {
            sb.append(DATE_FORMAT.format(DateUtilities.getDateFromDaysSinceEpoch(i)));
            sb.append("=");
            sb.append(sortedMap.get(i));
            sb.append(", ");
        }
        return sb.substring(0, sb.length() - 2) + "}";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        DailyCount that = (DailyCount) o;

        if (!dayToCount.equals(that.dayToCount)) return false;

        return true;
    }

    @Override
    public int hashCode() {
        return dayToCount.hashCode();
    }
}
