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
 * A {@link Statistic} that stores counts for given hours (e.g. a count of 5 for
 * Jan 1st 1982, 1pm).
 */
public class HourlyCount implements Statistic {

    private final static SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("yyyyMMddHH");
    static {
        DATE_FORMAT.setCalendar(new GregorianCalendar(SimpleTimeZone.getTimeZone("UTC"), Locale.UK));
    }

    private static final long serialVersionUID = 4641026985209063766L;

    private Map<Integer, Long> hourToCount;

    /**
     * Initialises with empty set of counts.
     */
    public HourlyCount() {
        hourToCount = new HashMap<Integer, Long>();
    }

    /**
     * Initialises with a value of count associated to the hour corresponding to the
     * given {@link java.util.Date}.
     *
     * @param date
     * @param count
     */
    public HourlyCount(Date date, long count) {
        this();
        hourToCount.put(DateUtilities.getHoursSinceEpoch(date), count);
    }

    /**
     * Increments the count for the hour corresponding to the given {@link java.util.Date} by 1.
     *
     * @param date
     */
    public void increment(Date date) {
        increment(date, 1L);
    }

    /**
     * Increments the count for the hour corresponding to the given {@link java.util.Date} by
     * count.
     *
     * @param date
     * @param count
     */
    public void increment(Date date, long count) {
        int hourAsInt = DateUtilities.getHoursSinceEpoch(date);
        if (hourToCount.containsKey(hourAsInt)) {
            hourToCount.put(hourAsInt, hourToCount.get(hourAsInt) + count);
        } else {
            hourToCount.put(hourAsInt, count);
        }
    }

    /**
     * Returns the count associated with the hour represented by the given {@link java.util.Date}.
     *
     * @param date
     * @return
     */
    public long getCount(Date date) {
        return hourToCount.get(DateUtilities.getHoursSinceEpoch(date));
    }

    /**
     * Given an int of the number of hours since the epoch, returns the associated count.
     *
     * @param hoursSinceEpoch
     * @return
     */
    public long getCount(int hoursSinceEpoch) {
        return hourToCount.get(hoursSinceEpoch);
    }

    /**
     * Returns all active hours, i.e. all hours for which the count is greater than 0, in
     * increasing order of {@link java.util.Date}.
     *
     * @return
     */
    public Date[] getActiveHours() {
        SortedMap<Integer, Long> sortedMap = new TreeMap<Integer, Long>(hourToCount);
        Date[] activeHours = new Date[sortedMap.keySet().size()];
        int count = 0;
        for (int i : sortedMap.keySet()) {
            activeHours[count] = DateUtilities.getDateFromHoursSinceEpoch(i);
            count++;
        }
        return activeHours;
    }

    @Override
    public void merge(Statistic s) throws IllegalArgumentException {
        if (s instanceof HourlyCount) {
            HourlyCount otherHourlyCount = (HourlyCount) s;
            for (int i : otherHourlyCount.hourToCount.keySet()) {
                if (!this.hourToCount.containsKey(i)) {
                    this.hourToCount.put(i, otherHourlyCount.hourToCount.get(i));
                } else {
                    this.hourToCount.put(i, this.hourToCount.get(i) + otherHourlyCount.hourToCount.get(i));
                }
            }
        } else {
            throw new IllegalArgumentException("Trying to merge a Statistic of type " + s.getClass()
                    + " with a " + this.getClass());
        }
    }

    @Override
    public HourlyCount clone() {
        HourlyCount clone = new HourlyCount();
        for (int i : this.hourToCount.keySet()) {
            clone.hourToCount.put(i, this.hourToCount.get(i));
        }
        return clone;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        WritableUtils.writeVInt(out, hourToCount.keySet().size());
        for (int i : hourToCount.keySet()) {
            WritableUtils.writeVInt(out, i);
            WritableUtils.writeVLong(out, hourToCount.get(i));
        }
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        int numberOfEntries = WritableUtils.readVInt(in);
        hourToCount.clear();
        for (int i = 0; i < numberOfEntries; i++) {
            hourToCount.put(WritableUtils.readVInt(in), WritableUtils.readVLong(in));
        }
    }

    @Override
    public String toString() {
        // Sort the map before printing
        SortedMap<Integer, Long> sortedMap = new TreeMap<Integer, Long>(hourToCount);
        StringBuilder sb = new StringBuilder("DailyCount {");
        for (int i : sortedMap.keySet()) {
            sb.append(DATE_FORMAT.format(DateUtilities.getDateFromHoursSinceEpoch(i)));
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

        HourlyCount that = (HourlyCount) o;

        if (!hourToCount.equals(that.hourToCount)) return false;

        return true;
    }

    @Override
    public int hashCode() {
        return hourToCount.hashCode();
    }
}
