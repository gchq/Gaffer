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
import java.util.*;

import static org.junit.Assert.*;

/**
 * Basic tests for {@link DailyCount}. The <code>write()</code>, <code>read()</code>,
 * <code>merge()</code>, <code>increment</code>, <code>getCount()</code> and <code>getActiveDays()</code> methods are tested.
 */
public class TestDailyCount {

	private final static SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("yyyyMMdd HHmmss");
	static {
		DATE_FORMAT.setCalendar(new GregorianCalendar(SimpleTimeZone.getTimeZone("UTC"), Locale.UK));
	}

	@Test
	public void testWriteAndRead() {
		try {
			DailyCount dc1 = new DailyCount(DATE_FORMAT.parse("20140102 123456"), 1L);
			dc1.increment(DATE_FORMAT.parse("20130102 123456"));
			DailyCount dc2 = new DailyCount(DATE_FORMAT.parse("20141212 123456"), 50L);
			dc2.increment(DATE_FORMAT.parse("20150102 123456"), 500L);

			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			DataOutput out = new DataOutputStream(baos);
			dc1.write(out);
			dc2.write(out);
			
			DataInput in = new DataInputStream(new ByteArrayInputStream(baos.toByteArray()));
			DailyCount readDc1 = new DailyCount();
			readDc1.readFields(in);
			DailyCount readDc2 = new DailyCount();
			readDc2.readFields(in);
			assertEquals(dc1, readDc1);
			assertEquals(dc2, readDc2);
		} catch (IOException e) {
			fail("Exception in testWriteAndRead() " + e);
		} catch (ParseException e) {
			fail("Exception in testWriteAndRead() " + e);
		}
	}

	@Test
	public void testMerge() {
		try {
			DailyCount dc1 = new DailyCount(DATE_FORMAT.parse("20140102 123456"), 1L);
			dc1.increment(DATE_FORMAT.parse("20130102 123456"));
			DailyCount dc2 = new DailyCount(DATE_FORMAT.parse("20140102 123456"), 50L);
			dc2.increment(DATE_FORMAT.parse("20150102 123456"), 500L);

			DailyCount expectedResult = new DailyCount();
			expectedResult.increment(DATE_FORMAT.parse("20130102 123456"), 1L);
			expectedResult.increment(DATE_FORMAT.parse("20140102 123456"), 51L);
			expectedResult.increment(DATE_FORMAT.parse("20150102 123456"), 500L);

			dc1.merge(dc2);

			assertEquals(expectedResult, dc1);
		} catch (ParseException e) {
			fail("Exception parsing date in testMerge() " + e);
		}
	}

	@Test
	public void testFailedMerge() {
		try {
			DailyCount dc1 = new DailyCount(DATE_FORMAT.parse("20140102 123456"), 1L);
			IntArray ia1 = new IntArray(new int[]{1,2,3,4});
			dc1.merge(ia1);
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
			DailyCount dc1 = new DailyCount(DATE_FORMAT.parse("20140102 123456"), 1L);
			dc1.increment(DATE_FORMAT.parse("20130102 123456"));
			DailyCount dc2 = new DailyCount(DATE_FORMAT.parse("20140102 123456"), 1L);
			dc2.increment(DATE_FORMAT.parse("20130102 123456"));
			DailyCount dc3 = new DailyCount(DATE_FORMAT.parse("20140102 123456"), 50L);
			dc3.increment(DATE_FORMAT.parse("20150102 123456"), 500L);

			assertEquals(dc1, dc2);
			assertNotEquals(dc1, dc3);
		} catch (ParseException e) {
			fail("Exception in testEquals() " + e);
		}
	}

	@Test
	public void testClone() {
		try {
			DailyCount dc1 = new DailyCount(DATE_FORMAT.parse("20140102 123456"), 1L);
			dc1.increment(DATE_FORMAT.parse("20130102 123456"));

			DailyCount expected = new DailyCount(DATE_FORMAT.parse("20140102 123456"), 1L);
			expected.increment(DATE_FORMAT.parse("20130102 123456"));

			assertEquals(expected, dc1.clone());
		} catch (ParseException e) {
			fail("Exception in testEquals() " + e);
		}
	}

	@Test
	public void testIncrement() {
		try {
			DailyCount dc1 = new DailyCount(DATE_FORMAT.parse("20140102 123456"), 1L);
			dc1.increment(DATE_FORMAT.parse("20140102 234556"), 100L);
			assertEquals(101L, dc1.getCount(DATE_FORMAT.parse("20140102 000000")));
		} catch (ParseException e) {
			fail("Exception parsing date in testFailedMerge()");
		}
	}

	@Test
	public void testGetCount() {
		try {
			DailyCount dc1 = new DailyCount(DATE_FORMAT.parse("20140102 123456"), 1L);
			dc1.increment(DATE_FORMAT.parse("20150102 234556"), 100L);
			assertEquals(100L, dc1.getCount(DATE_FORMAT.parse("20150102 000000")));
			assertEquals(1L, dc1.getCount(DATE_FORMAT.parse("20140102 123456")));
		} catch (ParseException e) {
			fail("Exception parsing date in testFailedMerge()");
		}
	}

	@Test
	public void testGetActiveDays() {
		try {
			DailyCount dc1 = new DailyCount(DATE_FORMAT.parse("20140102 123456"), 1L);
			dc1.increment(DATE_FORMAT.parse("20140102 234556"), 100L);
			dc1.increment(DATE_FORMAT.parse("20150103 234556"), 100L);
			dc1.increment(DATE_FORMAT.parse("20151112 234556"), 1000L);

			Date[] expectedDays = new Date[3];
			expectedDays[0] = DATE_FORMAT.parse("20140102 000000");
			expectedDays[1] = DATE_FORMAT.parse("20150103 000000");
			expectedDays[2] = DATE_FORMAT.parse("20151112 000000");

			assertArrayEquals(expectedDays, dc1.getActiveDays());
		} catch (ParseException e) {
			fail("Exception parsing date in testFailedMerge()");
		}
	}
}
