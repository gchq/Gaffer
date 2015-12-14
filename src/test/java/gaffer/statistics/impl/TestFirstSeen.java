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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.Locale;
import java.util.SimpleTimeZone;

import org.junit.Test;

/**
 * Unit tests for {@link FirstSeen}. Contains tests for writing and reading, and
 * for merging.
 */
public class TestFirstSeen {

	private static final DateFormat DATE_FORMAT = new SimpleDateFormat("yyyyMMdd HHmmss");
	static {
		DATE_FORMAT.setCalendar(new GregorianCalendar(SimpleTimeZone.getTimeZone("UTC"), Locale.UK));
	}
	
	@Test
	public void testEquals() {
		try {
			FirstSeen firstSeen1 = new FirstSeen();
			firstSeen1.setFirstSeen(DATE_FORMAT.parse("20131027 123456"));
			FirstSeen firstSeen2 = new FirstSeen();
			firstSeen2.setFirstSeen(DATE_FORMAT.parse("20131027 123456"));

			assertEquals(firstSeen1, firstSeen2);
		} catch (ParseException e) {
			fail("Exception in testEquals() " + e);
		}
	}

	@Test
	public void testWriteAndRead() {
		try {
			FirstSeen firstSeen1 = new FirstSeen();
			firstSeen1.setFirstSeen(DATE_FORMAT.parse("20131027 123456"));
			FirstSeen firstSeen2 = new FirstSeen();
			firstSeen2.setFirstSeen(DATE_FORMAT.parse("20131028 130102"));

			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			DataOutput out = new DataOutputStream(baos);
			firstSeen1.write(out);
			firstSeen2.write(out);

			DataInput in = new DataInputStream(new ByteArrayInputStream(baos.toByteArray()));
			FirstSeen firstSeen3 = new FirstSeen();
			firstSeen3.readFields(in);
			FirstSeen firstSeen4 = new FirstSeen();
			firstSeen4.readFields(in);
			assertEquals(firstSeen1, firstSeen3);
			assertEquals(firstSeen2, firstSeen4);

		} catch (IOException e) {
			fail("Exception in testWriteAndRead() " + e);
		} catch (ParseException e) {
			fail("Exception in testWriteAndRead() " + e);
		}
	}

	@Test
	public void testMerge() {
		try {
			FirstSeen firstSeen1 = new FirstSeen();
			firstSeen1.setFirstSeen(DATE_FORMAT.parse("20131027 123456"));
			FirstSeen firstSeen2 = new FirstSeen();
			firstSeen2.setFirstSeen(DATE_FORMAT.parse("20131026 130102"));

			firstSeen1.merge(firstSeen2);

			assertEquals(new FirstSeen(DATE_FORMAT.parse("20131026 130102")), firstSeen1);

			FirstSeen firstSeen3 = new FirstSeen();
			firstSeen3.setFirstSeen(DATE_FORMAT.parse("20131028 123456"));
			FirstSeen firstSeen4 = new FirstSeen();
			firstSeen4.setFirstSeen(DATE_FORMAT.parse("20131029 130102"));

			firstSeen3.merge(firstSeen4);

			assertEquals(new FirstSeen(DATE_FORMAT.parse("20131028 123456")), firstSeen3);

		} catch (ParseException e) {
			fail("Exception in testMerge() " + e);
		}
	}

	@Test
	@SuppressWarnings("deprecation")
	public void testConstructorClonesDate() {
		try {
			String dateString = "20131027 123456";
			// Date
			Date date = DATE_FORMAT.parse(dateString);
			// Create FirstSeen from Date
			FirstSeen firstSeen = new FirstSeen(date);
			// Change Date
			date.setHours(1);
			// Date within FirstSeen should not have changed
			assertEquals(DATE_FORMAT.parse(dateString), firstSeen.getFirstSeen());
		} catch (ParseException e) {
			fail("Exception in testMerge() " + e);
		}
	}

}
