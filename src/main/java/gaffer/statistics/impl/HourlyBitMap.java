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
 * A {@link Statistic} that records which hours something is active in (e.g. 12:00 on January
 * 1st, 14:00 on January 2nd).
 */
public class HourlyBitMap implements Statistic {

	private final static SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("yyyyMMddHH");
	static {
		DATE_FORMAT.setCalendar(new GregorianCalendar(SimpleTimeZone.getTimeZone("UTC"), Locale.UK));
	}

	private static final long serialVersionUID = -7732998628064561983L;

	private RoaringBitmap bitmap;

	public HourlyBitMap() {
		bitmap = new RoaringBitmap();
	}

	public HourlyBitMap(Date date) {
		this();
		add(date);
	}

	public void add(Date date) {
		bitmap.add(DateUtilities.getHoursSinceEpoch(date));
	}

	/**
	 * Indicates whether the bit map is set at the hour corresponding to
	 * the given date.
	 * 
	 * @param date
	 * @return
	 */
	public boolean active(Date date) {
		return bitmap.contains(DateUtilities.getHoursSinceEpoch(date));
	}

	/**
	 * Indicates whether the bit map is set during the provided interval.
	 * The dates are considered inclusively, i.e. if the bit map is set
	 * at the hour corresponding to the endDate then true is returned.
	 * 
	 * @param startDate
	 * @param endDate
	 * @return
	 */
	public boolean active(Date startDate, Date endDate) {
		int startDateHoursSinceEpoch = DateUtilities.getHoursSinceEpoch(startDate);
		int endDateHoursSinceEpoch = DateUtilities.getHoursSinceEpoch(endDate);
		for (int i = startDateHoursSinceEpoch; i <= endDateHoursSinceEpoch; i++) {
			if (bitmap.contains(i)) {
				return true;
			}
		}
		return false;
	}

	/**
	 * Counts the number of times the bit map is set during the provided
	 * interval. The dates are considered inclusively, i.e. if the bit map is set
	 * at the hour corresponding to the endDate then this is included in
	 * the count.
	 * 
	 * @param startDate
	 * @param endDate
	 * @return
	 */
	public int getNumberOfActiveHoursInRange(Date startDate, Date endDate) {
		int startDateHoursSinceEpoch = DateUtilities.getHoursSinceEpoch(startDate);
		int endDateHoursSinceEpoch = DateUtilities.getHoursSinceEpoch(endDate);
		int count = 0;
		for (int i = startDateHoursSinceEpoch; i <= endDateHoursSinceEpoch; i++) {
			if (bitmap.contains(i)) {
				count++;
			}
		}
		return count;
	}

	/**
	 * Converts each hour for which the bitmap was set to a human-readable long,
	 * e.g. 10am on January 17th 2014 would be converted to 2014011710, and returns
	 * an array of these. 
	 * 
	 * @return
	 */
	public long[] getHumanReadableHours() {
		long[] hoursAsLongs = new long[bitmap.getCardinality()];
		Iterator<Integer> it = bitmap.iterator();
		int count = 0;
		while (it.hasNext()) {
			hoursAsLongs[count] = Long.parseLong(DATE_FORMAT.format(DateUtilities.getDateFromHoursSinceEpoch(it.next())));
			count++;
		}
		return hoursAsLongs;
	}

	/**
	 * Returns an array containing each hour for which the bitmap was set, represented
	 * as an integer giving the number of hours since the epoch.
	 * 
	 * @return
	 */
	public int[] getHoursSinceEpoch() {
		return bitmap.toArray();
	}

	public String toString() {
		StringBuilder sb = new StringBuilder();
		Iterator<Integer> it = bitmap.iterator();
		while (it.hasNext()) {
			if (sb.length() > 0) {
				sb.append(" ");
			}
			int hour = it.next();
			Date date = DateUtilities.getDateFromHoursSinceEpoch(hour);
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
		if (s instanceof HourlyBitMap) {
			HourlyBitMap mbm = (HourlyBitMap) s;
			bitmap.or(mbm.bitmap);
		} else {
			throw new IllegalArgumentException("Trying to merge a Statistic of type " + s.getClass()
					+ " with a " + this.getClass());
		}
	}

	@Override
	public HourlyBitMap clone() {
		HourlyBitMap mbm = new HourlyBitMap();
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
		HourlyBitMap other = (HourlyBitMap) obj;
		if (bitmap == null) {
			if (other.bitmap != null)
				return false;
		} else if (!bitmap.equals(other.bitmap))
			return false;
		return true;
	}

}
