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
 * Unit tests for {@link MinuteBitMap}. Contains tests for: active, adding dates,
 * writing and reading, cloning, merging, etc.
 */
public class TestMinuteBitMap {

	private final static SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("yyyyMMddHHmmss");
	static {
		DATE_FORMAT.setCalendar(new GregorianCalendar(SimpleTimeZone.getTimeZone("UTC"), Locale.UK));
	}

	@Test
	public void testActive() {
		try {
			// Set one minute
			MinuteBitMap mbm1 = new MinuteBitMap();
			mbm1.add(DATE_FORMAT.parse("20140101021300"));
			assertTrue(mbm1.active(DATE_FORMAT.parse("20140101021300")));
			assertFalse(mbm1.active(DATE_FORMAT.parse("20140101021400")));
			assertFalse(mbm1.active(DATE_FORMAT.parse("19991225020000")));
			assertFalse(mbm1.active(DATE_FORMAT.parse("20140105020000")));

			// Set multiple minutes
			MinuteBitMap mbm2 = new MinuteBitMap();
			mbm2.add(DATE_FORMAT.parse("20140301021300"));
			mbm2.add(DATE_FORMAT.parse("20140301052700"));
			mbm2.add(DATE_FORMAT.parse("20140301235300"));
			assertTrue(mbm2.active(DATE_FORMAT.parse("20140301021300")));
			assertTrue(mbm2.active(DATE_FORMAT.parse("20140301052700")));
			assertTrue(mbm2.active(DATE_FORMAT.parse("20140301235300")));
			assertFalse(mbm2.active(DATE_FORMAT.parse("19991225020000")));
			assertFalse(mbm2.active(DATE_FORMAT.parse("20140105020000")));
		} catch (ParseException e) {
			fail("Exception parsing date: " + e);
		}
	}

	@Test
	public void testActiveRange() {
		try {
			// Set multiple minutes
			MinuteBitMap mbm = new MinuteBitMap();
			mbm.add(DATE_FORMAT.parse("20140301021300"));
			mbm.add(DATE_FORMAT.parse("20140301052700"));
			mbm.add(DATE_FORMAT.parse("20140301235300"));

			// Test range outside of active range
			assertFalse(mbm.active(DATE_FORMAT.parse("20140301010101"), DATE_FORMAT.parse("20140301020101")));

			// Test range including all set minutes
			assertTrue(mbm.active(DATE_FORMAT.parse("20140101123456"), DATE_FORMAT.parse("20150708123456")));

			// Test range including just one of the set minutes
			assertTrue(mbm.active(DATE_FORMAT.parse("20140301021200"), DATE_FORMAT.parse("20140301021400")));

			// Test range corresponding to exactly one of the set minutes
			assertTrue(mbm.active(DATE_FORMAT.parse("20140301021300"), DATE_FORMAT.parse("20140301021300")));

			// Test range is inclusive (at start)
			assertTrue(mbm.active(DATE_FORMAT.parse("20140301235300"), DATE_FORMAT.parse("20140301235800")));

			// Test range is inclusive (at end)
			assertTrue(mbm.active(DATE_FORMAT.parse("20140301235200"), DATE_FORMAT.parse("20140301235300")));

		} catch (ParseException e) {
			fail("Exception parsing date: " + e);
		}
	}

	@Test
	public void testNumberOfActiveMinutesInRange() {
		try {
			// Set multiple minutes
			MinuteBitMap mbm = new MinuteBitMap();
			mbm.add(DATE_FORMAT.parse("20140301021300"));
			mbm.add(DATE_FORMAT.parse("20140301052700"));
			mbm.add(DATE_FORMAT.parse("20140301235300"));

			// Test range outside of active range
			assertEquals(0, mbm.getNumberOfActiveMinutesInRange(DATE_FORMAT.parse("20140301010101"),
					DATE_FORMAT.parse("20140301020101")));

			// Test range including all set minutes
			assertEquals(3, mbm.getNumberOfActiveMinutesInRange(DATE_FORMAT.parse("20140101123456"),
					DATE_FORMAT.parse("20150708123456")));

			// Test range including just one of the set minutes
			assertEquals(1, mbm.getNumberOfActiveMinutesInRange(DATE_FORMAT.parse("20140301021200"),
					DATE_FORMAT.parse("20140301021400")));

			// Test range corresponding to exactly one of the set minutes
			assertEquals(1, mbm.getNumberOfActiveMinutesInRange(DATE_FORMAT.parse("20140301021300"),
					DATE_FORMAT.parse("20140301021300")));

			// Test range is inclusive (at start)
			assertEquals(1, mbm.getNumberOfActiveMinutesInRange(DATE_FORMAT.parse("20140301235300"),
					DATE_FORMAT.parse("20140301235800")));

			// Test range is inclusive (at end)
			assertEquals(1, mbm.getNumberOfActiveMinutesInRange(DATE_FORMAT.parse("20140301235200"),
					DATE_FORMAT.parse("20140301235300")));
		} catch (ParseException e) {
			fail("Exception parsing date: " + e);
		}
	}

	@Test
	public void testGetEarliestActiveMinute() {
		try {
			// Set multiple minutes
			MinuteBitMap mbm = new MinuteBitMap();
			mbm.add(DATE_FORMAT.parse("20140301091327"));
			mbm.add(DATE_FORMAT.parse("20140301052735"));
			mbm.add(DATE_FORMAT.parse("20140305235354"));

			// Test earliest minute
			assertEquals(DATE_FORMAT.parse("20140301052700"), mbm.getEarliestActiveMinute());
		} catch (ParseException e) {
			fail("Exception parsing date: " + e);
		}
	}

	@Test
	public void testGetLatestActiveMinute() {
		try {
			// Set multiple minutes
			MinuteBitMap mbm = new MinuteBitMap();
			mbm.add(DATE_FORMAT.parse("20140301091327"));
			mbm.add(DATE_FORMAT.parse("20140301052735"));
			mbm.add(DATE_FORMAT.parse("20140305235354"));

			// Test latest minute
			assertEquals(DATE_FORMAT.parse("20140305235300"), mbm.getLatestActiveMinute());
		} catch (ParseException e) {
			fail("Exception parsing date: " + e);
		}
	}

	@Test
	public void testGetHumanReadableMinutes() {
		try {
			// Set multiple minutes
			MinuteBitMap mbm = new MinuteBitMap();
			mbm.add(DATE_FORMAT.parse("20140301021300"));
			mbm.add(DATE_FORMAT.parse("20140301052700"));
			mbm.add(DATE_FORMAT.parse("20140301235300"));

			// Expected results
			long[] expected = new long[]{201403010213L, 201403010527L, 201403012353L};

			// Check agree
			assertArrayEquals(expected, mbm.getHumanReadableMinutes());
		} catch (ParseException e) {
			fail("Exception parsing date: " + e);
		}
	}

	@Test
	public void testGetMinutesSinceEpoch() {
		try {
			// Create some minutes
			Date date1 = DATE_FORMAT.parse("20140301021300");
			Date date2 = DATE_FORMAT.parse("20140301052700");
			Date date3 = DATE_FORMAT.parse("20140301235300");

			// Set multiple minutes
			MinuteBitMap mbm = new MinuteBitMap();
			mbm.add(date1);
			mbm.add(date2);
			mbm.add(date3);

			// Expected results
			int[] expected = new int[]{DateUtilities.getMinutesSinceEpoch(date1),
					DateUtilities.getMinutesSinceEpoch(date2),
					DateUtilities.getMinutesSinceEpoch(date3)};

			// Check agree
			assertArrayEquals(expected, mbm.getMinutesSinceEpoch());
		} catch (ParseException e) {
			fail("Exception parsing date: " + e);
		}
	}

	@Test
	public void testGetHourlyBitMap() {
		try {
			// Create some minutes
			Date date1 = DATE_FORMAT.parse("20140301021300");
			Date date2 = DATE_FORMAT.parse("20140301052700");
			Date date3 = DATE_FORMAT.parse("20140301235300");

			// Set multiple minutes
			MinuteBitMap mbm = new MinuteBitMap();
			mbm.add(date1);
			mbm.add(date2);
			mbm.add(date3);
			
			// Create HourlyBitMap
			HourlyBitMap hbm = mbm.getHourlyBitMap();
			assertTrue(hbm.active(DATE_FORMAT.parse("20140301021300")));
			assertTrue(hbm.active(DATE_FORMAT.parse("20140301052700")));
			assertTrue(hbm.active(DATE_FORMAT.parse("20140301235300")));
			assertFalse(hbm.active(DATE_FORMAT.parse("20140301110000")));
		} catch (ParseException e) {
			fail("Exception parsing date: " + e);
		}
	}

	@Test
	public void testWriteRead() {
		try {
			MinuteBitMap mbm1 = new MinuteBitMap();
			mbm1.add(DATE_FORMAT.parse("20140102111111"));
			mbm1.add(DATE_FORMAT.parse("20141012123456"));
			MinuteBitMap mbm2 = new MinuteBitMap();
			mbm2.add(DATE_FORMAT.parse("20130102111111"));
			mbm2.add(DATE_FORMAT.parse("20131012123456"));
			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			DataOutput out = new DataOutputStream(baos);
			mbm1.write(out);
			mbm2.write(out);
			DataInput in = new DataInputStream(new ByteArrayInputStream(baos.toByteArray()));
			MinuteBitMap hbm3 = new MinuteBitMap();
			hbm3.readFields(in);
			MinuteBitMap hbm4 = new MinuteBitMap();
			hbm4.readFields(in);
			assertEquals(mbm1, hbm3);
			assertEquals(mbm2, hbm4);
		} catch (ParseException e) {
			fail("Exception parsing date: " + e);
		} catch (IOException e) {
			fail("IOException: " + e);
		}
	}

	@Test
	public void testMerge() {
		try {
			// Merge an identical MinuteBitMap in
			MinuteBitMap mbm1 = new MinuteBitMap();
			mbm1.add(DATE_FORMAT.parse("20140102111111"));
			MinuteBitMap mbm2 = new MinuteBitMap();
			mbm2.add(DATE_FORMAT.parse("20140102111111"));
			mbm1.merge(mbm2);
			assertEquals(mbm1, mbm2);
			// Merge two different MinuteBitMaps
			MinuteBitMap mbm3 = new MinuteBitMap();
			mbm3.add(DATE_FORMAT.parse("20140102111111"));
			MinuteBitMap mbm4 = new MinuteBitMap();
			mbm4.add(DATE_FORMAT.parse("20130102111111"));
			mbm3.merge(mbm4);
			MinuteBitMap expected1 = new MinuteBitMap();
			expected1.add(DATE_FORMAT.parse("20140102111111"));
			expected1.add(DATE_FORMAT.parse("20130102111111"));
			assertEquals(expected1, mbm3);
			// Check mbm4 hasn't changed
			MinuteBitMap mbm5 = new MinuteBitMap();
			mbm5.add(DATE_FORMAT.parse("20130102111111"));
			assertEquals(mbm4, mbm5);
		} catch (ParseException e) {
			fail("Exception parsing date: " + e);
		}
	}

	@Test
	public void testClone() {
		try {
			// Create an MinuteBitMap
			MinuteBitMap mbm = new MinuteBitMap();
			mbm.add(DATE_FORMAT.parse("20140102111111"));
			mbm.add(DATE_FORMAT.parse("20141012123456"));
			// Clone it
			MinuteBitMap cloneOfMBM = mbm.clone();
			// Check that the two are equal
			assertEquals(mbm, cloneOfMBM);
			// Change the original
			mbm.add(DATE_FORMAT.parse("20141111111111"));
			// Check that the cloned one hasn't changed
			assertNotEquals(mbm, cloneOfMBM);
			MinuteBitMap expected = new MinuteBitMap();
			expected.add(DATE_FORMAT.parse("20140102111111"));
			expected.add(DATE_FORMAT.parse("20141012123456"));
			assertEquals(expected, cloneOfMBM);
		} catch (ParseException e) {
			fail("Exception parsing date: " + e);
		}
	}

	@Test
	public void testHash() {
		try {
			// Create an MinuteBitMap
			MinuteBitMap mbm = new MinuteBitMap();
			mbm.add(DATE_FORMAT.parse("20140102111111"));
			mbm.add(DATE_FORMAT.parse("20141012123456"));
			// Clone it
			MinuteBitMap cloneOfMBM = mbm.clone();
			// Check that the two are equal
			assertEquals(mbm, cloneOfMBM);
			// Check that they hash to the same value
			assertEquals(mbm.hashCode(), cloneOfMBM.hashCode());

			// Create another one that's identical to mbm
			MinuteBitMap mbm2 = new MinuteBitMap();
			mbm2.add(DATE_FORMAT.parse("20140102111111"));
			mbm2.add(DATE_FORMAT.parse("20141012123456"));
			// Check that the two are equal
			assertEquals(mbm, mbm2);
			// Check that they hash to the same value
			assertEquals(mbm.hashCode(), mbm2.hashCode());

		} catch (ParseException e) {
			fail("Exception parsing date: " + e);
		}
	}

	/**
	 * Utility method to calculate the serialised size of {@link MinuteBitMap}s with varying numbers of elements.
	 *
	 * @param args
	 * @throws IOException
	 */
	public static void main(String[] args) throws IOException, ParseException {
		// Serialised size based on different percentages of minutes from within a year
		int[] daysInMonth = new int[]{31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};
		int numTrials = 10;
		for (double percentage : new double[]{1.0, 0.5, 0.1, 0.01, 0.001, 0.0001, 0.00001}) {
			int totalCount = 0;
			int totalSize = 0;
			for (int i = 0; i < numTrials; i++) {
				int count = 0;
				MinuteBitMap mbm = new MinuteBitMap();
				for (int month = 1; month <= 12; month++) {
					for (int day = 1; day <= daysInMonth[month - 1]; day++) {
						for (int hour = 0; hour <= 23; hour++) {
							for (int minute = 0; minute <= 59; minute++) {
								if (Math.random() < percentage) {
									mbm.add(DATE_FORMAT.parse(String.format("2014%02d%02d%02d%02d00", month, day, hour, minute)));
									count++;
								}
							}
						}
					}
				}
				ByteArrayOutputStream baos = new ByteArrayOutputStream();
				DataOutput out = new DataOutputStream(baos);
				mbm.write(out);
				totalCount += count;
				totalSize += baos.size();
			}
			System.out.println("Percentage = " + percentage + " Added " + (totalCount / (double) numTrials) + " minutes - storage required = " + (totalSize / (double) numTrials));
		}
		// Serialised size of 2^n minutes randomly chosen from within a year
		int numMinutesInYear = 365 * 24 * 60;
		long start2014 = DATE_FORMAT.parse("20140101000000").getTime(); // Start of 2014 as number of milliseconds since epoch
		for (int n = 0; n < 18; n++) {
			int numMinutes = (int) Math.pow(2.0, n);
			int totalSize = 0;
			for (int i = 0; i < numTrials; i++) {
				MinuteBitMap mbm = new MinuteBitMap();
				while (mbm.getMinutesSinceEpoch().length < numMinutes) {
					int minute = (int) (Math.random() * numMinutesInYear);
					mbm.add(new Date(start2014 + (minute * 60 * 1000L)));
				}
				ByteArrayOutputStream baos = new ByteArrayOutputStream();
				DataOutput out = new DataOutputStream(baos);
				mbm.write(out);
				totalSize += baos.size();
			}
			System.out.println("Num minutes = " + numMinutes + " Size = " + (totalSize / (double) numTrials));
		}
	}

}
