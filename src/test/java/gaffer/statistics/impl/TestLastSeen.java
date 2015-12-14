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
 * Unit tests for {@link LastSeen}. Contains tests for writing and reading, and
 * for merging.
 */
public class TestLastSeen {

	private static final DateFormat DATE_FORMAT = new SimpleDateFormat("yyyyMMdd HHmmss");
	static {
		DATE_FORMAT.setCalendar(new GregorianCalendar(SimpleTimeZone.getTimeZone("UTC"), Locale.UK));
	}

	@Test
	public void testEquals() {
		try {
			LastSeen lastSeen1 = new LastSeen();
			lastSeen1.setLastSeen(DATE_FORMAT.parse("20131027 123456"));
			LastSeen lastSeen2 = new LastSeen();
			lastSeen2.setLastSeen(DATE_FORMAT.parse("20131027 123456"));
			
			assertEquals(lastSeen1, lastSeen2);
		} catch (ParseException e) {
			fail("Exception in testEquals() " + e);
		}
	}
	
	@Test
	public void testWriteAndRead() {
		try {
			LastSeen lastSeen1 = new LastSeen();
			lastSeen1.setLastSeen(DATE_FORMAT.parse("20131027 123456"));
			LastSeen lastSeen2 = new LastSeen();
			lastSeen2.setLastSeen(DATE_FORMAT.parse("20131028 130102"));

			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			DataOutput out = new DataOutputStream(baos);
			lastSeen1.write(out);
			lastSeen2.write(out);

			DataInput in = new DataInputStream(new ByteArrayInputStream(baos.toByteArray()));
			LastSeen lastSeen3 = new LastSeen();
			lastSeen3.readFields(in);
			LastSeen lastSeen4 = new LastSeen();
			lastSeen4.readFields(in);
			assertEquals(lastSeen1, lastSeen3);
			assertEquals(lastSeen2, lastSeen4);

		} catch (IOException e) {
			fail("Exception in testWriteAndRead() " + e);
		} catch (ParseException e) {
			fail("Exception in testWriteAndRead() " + e);
		}

	}

	@Test
	public void testMerge() {
		try {
			LastSeen lastSeen1 = new LastSeen();
			lastSeen1.setLastSeen(DATE_FORMAT.parse("20131027 123456"));
			LastSeen lastSeen2 = new LastSeen();
			lastSeen2.setLastSeen(DATE_FORMAT.parse("20131026 130102"));
			
			lastSeen1.merge(lastSeen2);

			assertEquals(new LastSeen(DATE_FORMAT.parse("20131027 123456")), lastSeen1);
			
			LastSeen lastSeen3 = new LastSeen();
			lastSeen3.setLastSeen(DATE_FORMAT.parse("20131028 123456"));
			LastSeen lastSeen4 = new LastSeen();
			lastSeen4.setLastSeen(DATE_FORMAT.parse("20131029 130102"));
			
			lastSeen3.merge(lastSeen4);
			
			assertEquals(new LastSeen(DATE_FORMAT.parse("20131029 130102")), lastSeen3);
			
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
			// Create LastSeen from Date
			LastSeen lastSeen = new LastSeen(date);
			// Change Date
			date.setHours(1);
			// Date within LastSeen should not have changed
			assertEquals(DATE_FORMAT.parse(dateString), lastSeen.getLastSeen());
		} catch (ParseException e) {
			fail("Exception in testMerge() " + e);
		}
	}

}
