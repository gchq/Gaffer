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

import org.junit.Test;

import java.io.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.Locale;
import java.util.SimpleTimeZone;

import static org.junit.Assert.*;

/**
 * Basic tests for {@link HourlyCount}. The <code>write()</code>, <code>read()</code>,
 * <code>merge()</code>, <code>increment()</code>, <code>getCount()</code> and <code>getActiveDays()</code>
 * methods are tested.
 */
public class TestHourlyCount {

	private final static SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("yyyyMMdd HHmmss");
	static {
		DATE_FORMAT.setCalendar(new GregorianCalendar(SimpleTimeZone.getTimeZone("UTC"), Locale.UK));
	}

	@Test
	public void testWriteAndRead() {
		try {
			HourlyCount hc1 = new HourlyCount(DATE_FORMAT.parse("20140102 123456"), 1L);
			hc1.increment(DATE_FORMAT.parse("20130102 123456"));
			HourlyCount hc2 = new HourlyCount(DATE_FORMAT.parse("20141212 123456"), 50L);
			hc2.increment(DATE_FORMAT.parse("20150102 123456"), 500L);

			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			DataOutput out = new DataOutputStream(baos);
			hc1.write(out);
			hc2.write(out);
			
			DataInput in = new DataInputStream(new ByteArrayInputStream(baos.toByteArray()));
			HourlyCount readHc1 = new HourlyCount();
			readHc1.readFields(in);
			HourlyCount readHc2 = new HourlyCount();
			readHc2.readFields(in);
			assertEquals(hc1, readHc1);
			assertEquals(hc2, readHc2);
		} catch (IOException e) {
			fail("Exception in testWriteAndRead() " + e);
		} catch (ParseException e) {
			fail("Exception in testWriteAndRead() " + e);
		}
	}

	@Test
	public void testMerge() {
		try {
			HourlyCount hc1 = new HourlyCount(DATE_FORMAT.parse("20140102 123456"), 1L);
			hc1.increment(DATE_FORMAT.parse("20130102 123456"));
			HourlyCount hc2 = new HourlyCount(DATE_FORMAT.parse("20140102 121111"), 50L);
			hc2.increment(DATE_FORMAT.parse("20150102 123456"), 500L);

			HourlyCount expectedResult = new HourlyCount();
			expectedResult.increment(DATE_FORMAT.parse("20130102 123456"), 1L);
			expectedResult.increment(DATE_FORMAT.parse("20140102 123456"), 51L);
			expectedResult.increment(DATE_FORMAT.parse("20150102 123456"), 500L);

			hc1.merge(hc2);

			assertEquals(expectedResult, hc1);
		} catch (ParseException e) {
			fail("Exception parsing date in testMerge() " + e);
		}
	}

	@Test
	public void testFailedMerge() {
		try {
			HourlyCount hc1 = new HourlyCount(DATE_FORMAT.parse("20140102 123456"), 1L);
			IntArray ia1 = new IntArray(new int[]{1,2,3,4});
			hc1.merge(ia1);
		} catch (IllegalArgumentException e) {
			return;
		} catch (ParseException e) {
			fail("Exception parsing date in testFailedMerge()");
		}
		fail("IllegalArgumentException should have been thrown.");
	}

	@Test
	public void testEquals() {
		try {
			HourlyCount hc1 = new HourlyCount(DATE_FORMAT.parse("20140102 123456"), 1L);
			hc1.increment(DATE_FORMAT.parse("20130102 123456"));
			HourlyCount hc2 = new HourlyCount(DATE_FORMAT.parse("20140102 123456"), 1L);
			hc2.increment(DATE_FORMAT.parse("20130102 123456"));
			HourlyCount hc3 = new HourlyCount(DATE_FORMAT.parse("20140102 121111"), 50L);
			hc3.increment(DATE_FORMAT.parse("20150102 123456"), 500L);

			assertEquals(hc1, hc2);
			assertNotEquals(hc1, hc3);
		} catch (ParseException e) {
			fail("Exception in testEquals() " + e);
		}
	}

	@Test
	public void testClone() {
		try {
			HourlyCount hc1 = new HourlyCount(DATE_FORMAT.parse("20140102 123456"), 1L);
			hc1.increment(DATE_FORMAT.parse("20130102 123456"));

			HourlyCount expected = new HourlyCount(DATE_FORMAT.parse("20140102 123456"), 1L);
			expected.increment(DATE_FORMAT.parse("20130102 123456"));

			assertEquals(expected, hc1.clone());
		} catch (ParseException e) {
			fail("Exception in testEquals() " + e);
		}
	}

	@Test
	public void testIncrement() {
		try {
			HourlyCount hc1 = new HourlyCount(DATE_FORMAT.parse("20140102 123456"), 1L);
			hc1.increment(DATE_FORMAT.parse("20140102 124556"), 100L);
			assertEquals(101L, hc1.getCount(DATE_FORMAT.parse("20140102 120000")));
		} catch (ParseException e) {
			fail("Exception parsing date in testFailedMerge()");
		}
	}

	@Test
	public void testGetCount() {
		try {
			HourlyCount hc1 = new HourlyCount(DATE_FORMAT.parse("20140102 123456"), 1L);
			hc1.increment(DATE_FORMAT.parse("20150102 234556"), 100L);
			assertEquals(100L, hc1.getCount(DATE_FORMAT.parse("20150102 230000")));
			assertEquals(1L, hc1.getCount(DATE_FORMAT.parse("20140102 120000")));
		} catch (ParseException e) {
			fail("Exception parsing date in testFailedMerge()");
		}
	}

	@Test
	public void testGetActiveHours() {
		try {
			HourlyCount hc1 = new HourlyCount(DATE_FORMAT.parse("20140102 123456"), 1L);
			hc1.increment(DATE_FORMAT.parse("20140102 234556"), 100L);
			hc1.increment(DATE_FORMAT.parse("20150103 234556"), 100L);
			hc1.increment(DATE_FORMAT.parse("20151112 234556"), 1000L);

			Date[] expectedHours = new Date[4];
			expectedHours[0] = DATE_FORMAT.parse("20140102 120000");
			expectedHours[1] = DATE_FORMAT.parse("20140102 230000");
			expectedHours[2] = DATE_FORMAT.parse("20150103 230000");
			expectedHours[3] = DATE_FORMAT.parse("20151112 230000");

			assertArrayEquals(expectedHours, hc1.getActiveHours());
		} catch (ParseException e) {
			fail("Exception parsing date in testFailedMerge()");
		}
	}
}
