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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import gaffer.utils.DateUtilities;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.Locale;
import java.util.SimpleTimeZone;

import org.junit.Test;

/**
 * Test of {@link HourlyBitMap}. Contains tests for: active, adding dates from before and
 * after the current date, writing and reading, cloning, etc.
 */
public class TestHourlyBitMap {

	private final static SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("yyyyMMddHHmmss");
	static {
		DATE_FORMAT.setCalendar(new GregorianCalendar(SimpleTimeZone.getTimeZone("UTC"), Locale.UK));
	}

	@Test
	public void testActive() {
		try {
			// Set one hour
			HourlyBitMap hbm1 = new HourlyBitMap();
			hbm1.add(DATE_FORMAT.parse("20140101020000"));
			assertTrue(hbm1.active(DATE_FORMAT.parse("20140101020000")));
			assertFalse(hbm1.active(DATE_FORMAT.parse("20140101010000")));
			for (int i = 0; i < 24; i++) {
				if (i != 2) {
					assertFalse(hbm1.active(DATE_FORMAT.parse(String.format("20140101%02d0000", i))));
				}
			}
			assertFalse(hbm1.active(DATE_FORMAT.parse("19991225020000")));
			assertFalse(hbm1.active(DATE_FORMAT.parse("20140105020000")));

			// Set multiple hours
			HourlyBitMap hbm2 = new HourlyBitMap();
			hbm2.add(DATE_FORMAT.parse("20140301020000"));
			hbm2.add(DATE_FORMAT.parse("20140301050000"));
			hbm2.add(DATE_FORMAT.parse("20140301230000"));
			assertTrue(hbm2.active(DATE_FORMAT.parse("20140301020000")));
			assertTrue(hbm2.active(DATE_FORMAT.parse("20140301050000")));
			assertTrue(hbm2.active(DATE_FORMAT.parse("20140301230000")));
			for (int i = 0; i < 24; i++) {
				if (i != 2 && i != 5 && i != 23) {
					assertFalse(hbm2.active(DATE_FORMAT.parse(String.format("20140101%02d0000", i))));
				}
			}
			assertFalse(hbm2.active(DATE_FORMAT.parse("19991225020000")));
			assertFalse(hbm2.active(DATE_FORMAT.parse("20140105020000")));
		} catch (ParseException e) {
			fail("Exception parsing date: " + e);
		}
	}

	@Test
	public void testAddDataBeyondInitialStartDate() {
		try {
			// Set one hour initially
			HourlyBitMap hbm1 = new HourlyBitMap();
			hbm1.add(DATE_FORMAT.parse("20140101020000"));
			assertTrue(hbm1.active(DATE_FORMAT.parse("20140101020000")));
			assertFalse(hbm1.active(DATE_FORMAT.parse("20140101000000")));
			// Add hour from a day later
			hbm1.add(DATE_FORMAT.parse("20140102070000"));
			assertTrue(hbm1.active(DATE_FORMAT.parse("20140101020000")));
			assertTrue(hbm1.active(DATE_FORMAT.parse("20140102070000")));
			// Add hour from a week later
			hbm1.add(DATE_FORMAT.parse("20140109210000"));
			assertTrue(hbm1.active(DATE_FORMAT.parse("20140101020000")));
			assertTrue(hbm1.active(DATE_FORMAT.parse("20140102070000")));
			assertTrue(hbm1.active(DATE_FORMAT.parse("20140109210000")));
			// Add hour from a year later
			hbm1.add(DATE_FORMAT.parse("20150109210000"));
			assertTrue(hbm1.active(DATE_FORMAT.parse("20140101020000")));
			assertTrue(hbm1.active(DATE_FORMAT.parse("20140102070000")));
			assertTrue(hbm1.active(DATE_FORMAT.parse("20140109210000")));
			assertTrue(hbm1.active(DATE_FORMAT.parse("20150109210000")));
			// Check some hours are not active
			assertFalse(hbm1.active(DATE_FORMAT.parse("20140101010000")));
			assertFalse(hbm1.active(DATE_FORMAT.parse("20140102080000")));
			assertFalse(hbm1.active(DATE_FORMAT.parse("20140109220000")));
			assertFalse(hbm1.active(DATE_FORMAT.parse("20150109200000")));
		} catch (ParseException e) {
			fail("Exception parsing date: " + e);
		}
	}

	@Test
	public void testAddDateBeforeInitialStartDate() {
		try {
			// Set one hour initially
			HourlyBitMap hbm1 = new HourlyBitMap();
			hbm1.add(DATE_FORMAT.parse("20140101020000"));
			assertTrue(hbm1.active(DATE_FORMAT.parse("20140101020000")));
			assertFalse(hbm1.active(DATE_FORMAT.parse("20140101000000")));
			// Add hour from a day earlier
			hbm1.add(DATE_FORMAT.parse("20131231070000"));
			assertTrue(hbm1.active(DATE_FORMAT.parse("20140101020000")));
			assertTrue(hbm1.active(DATE_FORMAT.parse("20131231070000")));
			// Add hour from a week earlier
			hbm1.add(DATE_FORMAT.parse("20131224070000"));
			assertTrue(hbm1.active(DATE_FORMAT.parse("20140101020000")));
			assertTrue(hbm1.active(DATE_FORMAT.parse("20131231070000")));
			assertTrue(hbm1.active(DATE_FORMAT.parse("20131224070000")));
			// Add hour from a month later
			hbm1.add(DATE_FORMAT.parse("20140201230000"));
			assertTrue(hbm1.active(DATE_FORMAT.parse("20140101020000")));
			assertTrue(hbm1.active(DATE_FORMAT.parse("20131231070000")));
			assertTrue(hbm1.active(DATE_FORMAT.parse("20131224070000")));
			assertTrue(hbm1.active(DATE_FORMAT.parse("20140201230000")));
			// Add hour from a long time earlier
			hbm1.add(DATE_FORMAT.parse("20120201210000"));
			assertTrue(hbm1.active(DATE_FORMAT.parse("20140101020000")));
			assertTrue(hbm1.active(DATE_FORMAT.parse("20131231070000")));
			assertTrue(hbm1.active(DATE_FORMAT.parse("20131224070000")));
			assertTrue(hbm1.active(DATE_FORMAT.parse("20140201230000")));
			assertTrue(hbm1.active(DATE_FORMAT.parse("20120201210000")));
		} catch (ParseException e) {
			fail("Exception parsing date: " + e);
		}
	}

	@Test
	public void testNoArgsConstructor() {
		try {
			HourlyBitMap hbm = new HourlyBitMap();
			hbm.add(DATE_FORMAT.parse("20140102111111"));
			hbm.add(DATE_FORMAT.parse("20131012123456"));
			assertTrue(hbm.active(DATE_FORMAT.parse("20140102111111")));
			assertTrue(hbm.active(DATE_FORMAT.parse("20131012123456")));
			assertFalse(hbm.active(DATE_FORMAT.parse("20121012123456")));
		} catch (ParseException e) {
			fail("Exception parsing date: " + e);
		}
	}

	@Test
	public void testGetHumanReadableHours() {
		try {
			HourlyBitMap hbm = new HourlyBitMap();
			hbm.add(DATE_FORMAT.parse("20140102111111"));
			hbm.add(DATE_FORMAT.parse("20131012123456"));
			long[] humanReadableHours = hbm.getHumanReadableHours();
			assertEquals(2, humanReadableHours.length);
			assertEquals(2013101212L, humanReadableHours[0]);
			assertEquals(2014010211L, humanReadableHours[1]);
		} catch (ParseException e) {
			fail("Exception parsing date: " + e);
		}
	}
	
	@Test
	public void testGetHoursSinceEpoch() {
		try {
			Date date1 = DATE_FORMAT.parse("20131012123456");
			Date date2 = DATE_FORMAT.parse("20140102111111");
			HourlyBitMap hbm = new HourlyBitMap();
			hbm.add(date1);
			hbm.add(date2);
			int[] expected = new int[]{DateUtilities.getHoursSinceEpoch(date1),
					DateUtilities.getHoursSinceEpoch(date2)};
			assertArrayEquals(expected, hbm.getHoursSinceEpoch());
		} catch (ParseException e) {
			fail("Exception parsing date: " + e);
		}
	}
	
	@Test
	public void testWriteRead() {
		try {
			HourlyBitMap hbm1 = new HourlyBitMap();
			hbm1.add(DATE_FORMAT.parse("20140102111111"));
			hbm1.add(DATE_FORMAT.parse("20141012123456"));
			HourlyBitMap hbm2 = new HourlyBitMap();
			hbm2.add(DATE_FORMAT.parse("20130102111111"));
			hbm2.add(DATE_FORMAT.parse("20131012123456"));
			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			DataOutput out = new DataOutputStream(baos);
			hbm1.write(out);
			hbm2.write(out);
			DataInput in = new DataInputStream(new ByteArrayInputStream(baos.toByteArray()));
			HourlyBitMap hbm3 = new HourlyBitMap();
			hbm3.readFields(in);
			HourlyBitMap hbm4 = new HourlyBitMap();
			hbm4.readFields(in);
			assertEquals(hbm1, hbm3);
			assertEquals(hbm2, hbm4);
		} catch (ParseException e) {
			fail("Exception parsing date: " + e);
		} catch (IOException e) {
			fail("IOException: " + e);
		}
	}

	@Test
	public void testMerge() {
		try {
			// Merge an identical HourlyBitMap in
			HourlyBitMap hbm1 = new HourlyBitMap();
			hbm1.add(DATE_FORMAT.parse("20140102111111"));
			HourlyBitMap hbm2 = new HourlyBitMap();
			hbm2.add(DATE_FORMAT.parse("20140102111111"));
			hbm1.merge(hbm2);
			assertEquals(hbm2, hbm1);
			// Merge two different HourlyBitMaps
			HourlyBitMap hbm3 = new HourlyBitMap();
			hbm3.add(DATE_FORMAT.parse("20140102111111"));
			HourlyBitMap hbm4 = new HourlyBitMap();
			hbm4.add(DATE_FORMAT.parse("20130102111111"));
			hbm3.merge(hbm4);
			HourlyBitMap expected1 = new HourlyBitMap();
			expected1.add(DATE_FORMAT.parse("20140102111111"));
			expected1.add(DATE_FORMAT.parse("20130102111111"));
			assertEquals(expected1, hbm3);
			// Check hbm4 hasn't changed
			HourlyBitMap hbm5 = new HourlyBitMap();
			hbm5.add(DATE_FORMAT.parse("20130102111111"));
			assertEquals(hbm5, hbm4);
		} catch (ParseException e) {
			fail("Exception parsing date: " + e);
		}
	}

	@Test
	public void testClone() {
		try {
			// Create an HourlyBitMap
			HourlyBitMap hbm = new HourlyBitMap();
			hbm.add(DATE_FORMAT.parse("20140102111111"));
			hbm.add(DATE_FORMAT.parse("20141012123456"));
			// Clone it
			HourlyBitMap cloneOfHBM = hbm.clone();
			// Check that the two are equal
			assertEquals(hbm, cloneOfHBM);
			// Change the original
			hbm.add(DATE_FORMAT.parse("20141111111111"));
			// Check that the cloned one hasn't changed
			assertNotEquals(hbm, cloneOfHBM);
			HourlyBitMap expected = new HourlyBitMap();
			expected.add(DATE_FORMAT.parse("20140102111111"));
			expected.add(DATE_FORMAT.parse("20141012123456"));
			assertEquals(expected, cloneOfHBM);
		} catch (ParseException e) {
			fail("Exception parsing date: " + e);
		}
	}

}
