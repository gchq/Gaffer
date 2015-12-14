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
import gaffer.statistics.SetOfStatistics;
import gaffer.statistics.Statistic;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.Iterator;
import java.util.Locale;
import java.util.SimpleTimeZone;

import org.roaringbitmap.RoaringBitmap;

/**
 * A {@link Statistic} that records whether something is active in each minute.
 * If there is activity in each minute of a year, then the total storage is only
 * 72KB.
 */
public class MinuteBitMap implements Statistic {

	private final static SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("yyyyMMddHHmm");
	static {
		DATE_FORMAT.setCalendar(new GregorianCalendar(SimpleTimeZone.getTimeZone("UTC"), Locale.UK));
	}
	public final static String DERIVED_FIRST_SEEN = "DERIVED_FIRST_SEEN";
	public final static String DERIVED_LAST_SEEN = "DERIVED_LAST_SEEN";
	public final static String DERIVED_HOUR_OF_DAY_COUNT = "DERIVED_HOUR_OF_DAY_COUNT";
	private static final long serialVersionUID = 5372423703060881765L;

	private RoaringBitmap bitmap;
	
	public MinuteBitMap() {
		bitmap = new RoaringBitmap();
	}
	
	public MinuteBitMap(Date date) {
		this();
		add(date);
	}
	
	public MinuteBitMap(RoaringBitmap bitmap) {
		this.bitmap = bitmap;
	}
	
	public void add(Date date) {
		bitmap.add(DateUtilities.getMinutesSinceEpoch(date));
	}
	
	public RoaringBitmap getRoaringBitmap() {
		return bitmap;
	}
	
	/**
	 * Indicates whether the bit map is set at the minute corresponding to
	 * the given date.
	 * 
	 * @param date
	 * @return
	 */
	public boolean active(Date date) {
		return bitmap.contains(DateUtilities.getMinutesSinceEpoch(date));
	}
	
	/**
	 * Indicates whether the bit map is set during the provided interval.
	 * The dates are considered inclusively, i.e. if the bit map is set
	 * at the minute corresponding to the endDate then true is returned.
	 * 
	 * @param startDate
	 * @param endDate
	 * @return
	 */
	public boolean active(Date startDate, Date endDate) {
		int startDateMinsSinceEpoch = DateUtilities.getMinutesSinceEpoch(startDate);
		int endDateMinsSinceEpoch = DateUtilities.getMinutesSinceEpoch(endDate);
		for (int i = startDateMinsSinceEpoch; i <= endDateMinsSinceEpoch; i++) {
			if (bitmap.contains(i)) {
				return true;
			}
		}
		return false;
	}
	
	/**
	 * Counts the number of times the bit map is set during the provided
	 * interval. The dates are considered inclusively, i.e. if the bit map is set
	 * at the minute corresponding to the endDate then this is included in
	 * the count.
	 * 
	 * @param startDate
	 * @param endDate
	 * @return
	 */
	public int getNumberOfActiveMinutesInRange(Date startDate, Date endDate) {
		int startDateMinsSinceEpoch = DateUtilities.getMinutesSinceEpoch(startDate);
		int endDateMinsSinceEpoch = DateUtilities.getMinutesSinceEpoch(endDate);
		int count = 0;
		for (int i = startDateMinsSinceEpoch; i <= endDateMinsSinceEpoch; i++) {
			if (bitmap.contains(i)) {
				count++;
			}
		}
		return count;
	}

	/**
	 * Returns the earliest minute that the bit map is active. NB: Requires scanning
	 * through the entire bit map as it does not expose a getFirst() method (although
	 * in practice the first one may always be the earliest).
	 *
	 * @return
	 */
	public Date getEarliestActiveMinute() {
		Iterator<Integer> it = bitmap.iterator();
		Date earliest = new Date(Long.MAX_VALUE);
		while (it.hasNext()) {
			Date date = DateUtilities.getDateFromMinutesSinceEpoch(it.next());
			if (date.before(earliest)) {
				earliest = date;
			}
		}
		return earliest;
	}

	/**
	 * Returns the latest minute that the bit map is active. NB: Requires scanning
	 * through the entire bit map as it does not expose a getLast() method (although
	 * in practice the last one may always be the latest).
	 *
	 * @return
	 */
	public Date getLatestActiveMinute() {
		Iterator<Integer> it = bitmap.iterator();
		Date latest = new Date(0);
		while (it.hasNext()) {
			Date date = DateUtilities.getDateFromMinutesSinceEpoch(it.next());
			if (date.after(latest)) {
				latest = date;
			}
		}
		return latest;
	}

	/**
	 * Converts each minute for which the bitmap was set to a human-readable long,
	 * e.g. 12:56 on January 17th 2014 would be converted to 201401171256, and returns
	 * an array of these. 
	 * 
	 * @return
	 */
	public long[] getHumanReadableMinutes() {
		long[] minutesAsLongs = new long[bitmap.getCardinality()];
		Iterator<Integer> it = bitmap.iterator();
		int count = 0;
		while (it.hasNext()) {
			minutesAsLongs[count] = Long.parseLong(DATE_FORMAT.format(DateUtilities.getDateFromMinutesSinceEpoch(it.next())));
			count++;
		}
		return minutesAsLongs;
	}
	
	/**
	 * Returns an array containing each minute for which the bitmap was set, represented
	 * as an integer giving the number of minutes since the epoch.
	 * 
	 * @return
	 */
	public int[] getMinutesSinceEpoch() {
		return bitmap.toArray();
	}
	
	/**
	 * Returns an {@link HourlyBitMap} derived from this object.
	 * 
	 * @return
	 */
	public HourlyBitMap getHourlyBitMap() {
		HourlyBitMap hbm = new HourlyBitMap();
		Iterator<Integer> it = bitmap.iterator();
		while (it.hasNext()) {
			hbm.add(DateUtilities.getDateFromMinutesSinceEpoch(it.next()));
		}
		return hbm;
	}
	
	/**
	 * Returns a {@link SetOfStatistics} containing statistics that can be derived.
	 * 
	 * @return
	 */
	public SetOfStatistics getDerivedStatistics() {
		DateUtilities dateUtils = new DateUtilities();
		SetOfStatistics derivedStatistics = new SetOfStatistics();
		int min = Integer.MAX_VALUE;
		int max = Integer.MIN_VALUE;
		int[] hourOfDayCount = new int[24];
		for (int i = 0; i < hourOfDayCount.length; i++) {
			hourOfDayCount[i] = 0;
		}
		Iterator<Integer> it = bitmap.iterator();
		while (it.hasNext()) {
			int i = it.next();
			if (i < min) {
				min = i;
			}
			if (i > max) {
				max = i;
			}
			hourOfDayCount[dateUtils.getHourOfDay(DateUtilities.getDateFromMinutesSinceEpoch(i))]++;
		}
		derivedStatistics.addStatistic(DERIVED_FIRST_SEEN, new FirstSeen(DateUtilities.getDateFromMinutesSinceEpoch(min)));
		derivedStatistics.addStatistic(DERIVED_LAST_SEEN, new LastSeen(DateUtilities.getDateFromMinutesSinceEpoch(max)));
		derivedStatistics.addStatistic(DERIVED_HOUR_OF_DAY_COUNT, new IntArray(hourOfDayCount));
		return derivedStatistics;
	}
	
	public String toString() {
		StringBuilder sb = new StringBuilder();
		Iterator<Integer> it = bitmap.iterator();
		while (it.hasNext()) {
			if (sb.length() > 0) {
				sb.append(" ");
			}
			int minute = it.next();
			Date date = DateUtilities.getDateFromMinutesSinceEpoch(minute);
			sb.append(DATE_FORMAT.format(date));
		}
		return sb.toString();
	}
	
	@Override
	public void write(DataOutput out) throws IOException {
		bitmap.serialize(out);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		bitmap.deserialize(in);
	}

	@Override
	public void merge(Statistic s) throws IllegalArgumentException {
		if (s instanceof MinuteBitMap) {
			MinuteBitMap mbm = (MinuteBitMap) s;
			bitmap.or(mbm.bitmap);
		} else {
			throw new IllegalArgumentException("Trying to merge a Statistic of type " + s.getClass()
					+ " with a " + this.getClass());
		}
	}

	@Override
	public MinuteBitMap clone() {
		MinuteBitMap mbm = new MinuteBitMap();
		mbm.bitmap.or(this.bitmap);
		return mbm;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((bitmap == null) ? 0 : bitmap.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		MinuteBitMap other = (MinuteBitMap) obj;
		if (bitmap == null) {
			if (other.bitmap != null)
				return false;
		} else if (!bitmap.equals(other.bitmap))
			return false;
		return true;
	}
	
}
