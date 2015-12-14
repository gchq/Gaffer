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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import gaffer.utils.DateUtilities;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.Locale;
import java.util.SimpleTimeZone;

import org.junit.Test;

/**
 * Unit tests for the {@link DateUtilities} class.
 */
public class TestDateUtilities {

	private final static SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("yyyyMMddHHmmss");
	private final static SimpleDateFormat DAY_DATE_FORMAT = new SimpleDateFormat("yyyyMMdd");
	private final DateUtilities dateUtilities = new DateUtilities();
	static {
		DATE_FORMAT.setCalendar(new GregorianCalendar(SimpleTimeZone.getTimeZone("UTC"), Locale.UK));
		DAY_DATE_FORMAT.setCalendar(new GregorianCalendar(SimpleTimeZone.getTimeZone("UTC"), Locale.UK));
	}

	@Test
	public void testDifferenceInDays() {
		try {
			Date date1 = DAY_DATE_FORMAT.parse("20140101");
			Date date2 = DAY_DATE_FORMAT.parse("20140102");
			Date date3 = DAY_DATE_FORMAT.parse("20140101");
			Date date4 = DAY_DATE_FORMAT.parse("20140105");
			assertEquals(1, DateUtilities.differenceInDays(date1, date2));
			assertEquals(0, DateUtilities.differenceInDays(date1, date3));
			assertEquals(-1, DateUtilities.differenceInDays(date2, date1));
			assertEquals(4, DateUtilities.differenceInDays(date3, date4));
			assertEquals(-4, DateUtilities.differenceInDays(date4, date3));
		} catch (ParseException e) {
			fail("Exception parsing date: " + e);
		}
	}

	@Test
	public void testSetDateToMidnight() {
		try {
			Date date = DATE_FORMAT.parse("20140101123456");
			Date midnight = dateUtilities.setDateToMidnight(date);
			Date expected = DATE_FORMAT.parse("20140101000000");
			assertEquals(expected, midnight);
		} catch (ParseException e) {
			fail("Exception parsing date: " + e);
		}
	}

	@Test
	public void testSetDateToNextMidnight() {
		try {
			Date date = DATE_FORMAT.parse("20140101123456");
			Date nextMidnight = dateUtilities.setDateToNextMidnight(date);
			Date expected = DATE_FORMAT.parse("20140102000000");
			assertEquals(expected, nextMidnight);
		} catch (ParseException e) {
			fail("Exception parsing date: " + e);
		}
	}

	@Test
	public void testSetDateToMidnightClones() {
		try {
			Date date = DATE_FORMAT.parse("20140101123456");
			@SuppressWarnings("unused")
			Date midnight = dateUtilities.setDateToMidnight(date);
			// Check that date hasn't changed
			assertEquals(DATE_FORMAT.parse("20140101123456"), date);
		} catch (ParseException e) {
			fail("Exception parsing date: " + e);
		}
	}

	@Test
	public void testDateToHoursSinceEpochAndBack() {
		try {
			Date date = DATE_FORMAT.parse("20140101120000");
			int hoursSinceEpoch = DateUtilities.getHoursSinceEpoch(date);
			Date date2 = DateUtilities.getDateFromHoursSinceEpoch(hoursSinceEpoch);
			assertEquals(date, date2);
		} catch (ParseException e) {
			fail("Exception parsing date: " + e);
		}
	}

	@Test
	public void testDateToMinutesSinceEpochAndBack() {
		try {
			Date date = DATE_FORMAT.parse("20140101123400");
			int minutesSinceEpoch = DateUtilities.getMinutesSinceEpoch(date);
			Date date2 = DateUtilities.getDateFromMinutesSinceEpoch(minutesSinceEpoch);
			assertEquals(date, date2);
		} catch (ParseException e) {
			fail("Exception parsing date: " + e);
		}
	}

	@Test
	public void testDateToDaysSinceEpochAndBack() {
		try {
			Date date = DATE_FORMAT.parse("20140101000000");
			int daysSinceEpoch = DateUtilities.getDaysSinceEpoch(date);
			Date date2 = DateUtilities.getDateFromDaysSinceEpoch(daysSinceEpoch);
			assertEquals(date, date2);
		} catch (ParseException e) {
			fail("Exception parsing date: " + e);
		}
	}
}
