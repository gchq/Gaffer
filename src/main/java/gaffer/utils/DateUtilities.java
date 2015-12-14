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
package gaffer.utils;

import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.Locale;
import java.util.SimpleTimeZone;

/**
 * Utility methods for dates.
 */
public class DateUtilities {

	public final static long MILLISECONDS_IN_MINUTE = 60 * 1000L;
	public final static long MILLISECONDS_IN_HOUR = 60 * MILLISECONDS_IN_MINUTE;
	public final static long DAY_IN_MILLISECONDS = 24 * MILLISECONDS_IN_HOUR;

	private Calendar calendar;

	public DateUtilities() {
		calendar = new GregorianCalendar(SimpleTimeZone.getTimeZone("UTC"), Locale.UK);
	}

	/**
	 * Given a {@link Date} this method returns a new {@link Date}
	 * set to be the last midnight prior to the supplied date.
	 * 
	 * @param date
	 * @return
	 */
	public Date setDateToMidnight(Date date) {
		calendar.setTime(date);
		calendar.set(Calendar.HOUR_OF_DAY, 0);
		calendar.set(Calendar.MINUTE, 0);
		calendar.set(Calendar.SECOND, 0);
		calendar.set(Calendar.MILLISECOND, 0);
		return calendar.getTime();
	}

	/**
	 * Given a {@link Date} this method returns a new {@link Date}
	 * set to be the first midnight after the supplied date.
	 * 
	 * @param date
	 * @return
	 */
	public Date setDateToNextMidnight(Date date) {
		calendar.setTime(date);
		calendar.setTimeInMillis(calendar.getTimeInMillis() + DAY_IN_MILLISECONDS);
		calendar.set(Calendar.HOUR_OF_DAY, 0);
		calendar.set(Calendar.MINUTE, 0);
		calendar.set(Calendar.SECOND, 0);
		calendar.set(Calendar.MILLISECOND, 0);
		return calendar.getTime();
	}

	/**
	 * Returns the hour of day for the supplied {@link Date}.
	 * 
	 * @param date
	 * @return
	 */
	public int getHourOfDay(Date date) {
		calendar.setTime(date);
		return calendar.get(Calendar.HOUR_OF_DAY);
	}

	/**
	 * Returns the number of minutes from the given date to the
	 * preceding midnight.
	 * 
	 * @param date
	 * @return
	 */
	public int getMinuteOfDay(Date date) {
		calendar.setTime(date);
		int numMinutes = (60 * calendar.get(Calendar.HOUR_OF_DAY)) + calendar.get(Calendar.MINUTE);
		return numMinutes;
	}

	/**
	 * Returns the difference in days between the two supplied dates.
	 * 
	 * @param date1
	 * @param date2
	 * @return
	 */
	public static int differenceInDays(Date date1, Date date2) {
		return (int) ((date2.getTime() - date1.getTime()) / DAY_IN_MILLISECONDS);
	}

	/**
	 * Returns the number of minutes that have passed between the epoch
	 * and the supplied {@link Date}.
	 * 
	 * @param date
	 * @return
	 */
	public static int getMinutesSinceEpoch(Date date) {
		return (int) (date.getTime() / MILLISECONDS_IN_MINUTE);
	}

	/**
	 * Given a number of minutes since the epoch, this returns the
	 * corresponding {@link Date}.
	 * 
	 * @param minute
	 */
	public static Date getDateFromMinutesSinceEpoch(int minute) {
		return new Date(minute * MILLISECONDS_IN_MINUTE);
	}

	/**
	 * Returns the number of hours that have passed between the epoch
	 * and the supplied {@link Date}.
	 * 
	 * @param date
	 * @return
	 */
	public static int getHoursSinceEpoch(Date date) {
		return (int) (date.getTime() / MILLISECONDS_IN_HOUR);
	}

	/**
	 * Given a number of hours since the epoch, this returns the
	 * corresponding {@link Date}.
	 * 
	 * @param hour
	 */
	public static Date getDateFromHoursSinceEpoch(int hour) {
		return new Date(hour * MILLISECONDS_IN_HOUR);
	}

	/**
	 * Returns the number of days that have passed between the epoch
	 * and the supplied {@link Date}.
	 *
	 * @param date
	 * @return
	 */
	public static int getDaysSinceEpoch(Date date) {
		return (int) (date.getTime() / DAY_IN_MILLISECONDS);
	}

	/**
	 * Given a number of days since the epoch, this returns the
	 * corresponding {@link Date}.
	 *
	 * @param days
	 * @return
	 */
	public static Date getDateFromDaysSinceEpoch(int days) {
		return new Date(days * DAY_IN_MILLISECONDS);
	}
}
