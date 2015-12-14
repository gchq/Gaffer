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
 * Unit tests for {@link CappedMinuteCount}. The <code>write()</code>, <code>read()</code>,
 * <code>merge()</code>, <code>increment()</code>, <code>getCount()</code> and <code>getActiveMinutes()</code>
 * methods are tested.
 */
public class TestCappedMinuteCount {

    private final static SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("yyyyMMdd HHmmss");
    static {
        DATE_FORMAT.setCalendar(new GregorianCalendar(SimpleTimeZone.getTimeZone("UTC"), Locale.UK));
    }

    @Test
    public void testWriteAndRead() {
        try {
            CappedMinuteCount cmc1 = new CappedMinuteCount(100, DATE_FORMAT.parse("20140102 123456"));
            cmc1.increment(DATE_FORMAT.parse("20130102 123456"), 3L);
            CappedMinuteCount cmc2 = new CappedMinuteCount(100, DATE_FORMAT.parse("20140102 123456"));
            cmc2.increment(DATE_FORMAT.parse("20150102 123456"), 500L);

            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            DataOutput out = new DataOutputStream(baos);
            cmc1.write(out);
            cmc2.write(out);

            DataInput in = new DataInputStream(new ByteArrayInputStream(baos.toByteArray()));
            CappedMinuteCount readCmc1 = new CappedMinuteCount();
            readCmc1.readFields(in);
            CappedMinuteCount readCmc2 = new CappedMinuteCount();
            readCmc2.readFields(in);
            assertEquals(cmc1, readCmc1);
            assertEquals(cmc2, readCmc2);
        } catch (IOException e) {
            fail("Exception in testWriteAndRead() " + e);
        } catch (ParseException e) {
            fail("Exception in testWriteAndRead() " + e);
        }
    }

    @Test
    public void testWriteAndReadWhenEmpty() {
        try {
            CappedMinuteCount cmc1 = new CappedMinuteCount(5);
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            DataOutput out = new DataOutputStream(baos);
            cmc1.write(out);
            DataInput in = new DataInputStream(new ByteArrayInputStream(baos.toByteArray()));
            CappedMinuteCount readCmc1 = new CappedMinuteCount();
            readCmc1.readFields(in);
            assertEquals(cmc1, readCmc1);
        } catch (IOException e) {
            fail("Exception in testWriteAndRead() " + e);
        }
    }

    @Test
    public void testWriteAndReadWhenFull() {
        try {
            CappedMinuteCount cmc1 = new CappedMinuteCount(2);
            cmc1.increment(DATE_FORMAT.parse("20130102 123456"), 3L);
            cmc1.increment(DATE_FORMAT.parse("20140102 123456"), 3L);
            cmc1.increment(DATE_FORMAT.parse("20150102 123456"), 3L);
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            DataOutput out = new DataOutputStream(baos);
            cmc1.write(out);
            DataInput in = new DataInputStream(new ByteArrayInputStream(baos.toByteArray()));
            CappedMinuteCount readCmc1 = new CappedMinuteCount();
            readCmc1.readFields(in);
            assertTrue(cmc1.isFull());
            assertTrue(readCmc1.isFull());
            assertEquals(cmc1, readCmc1);
        } catch (IOException e) {
            fail("Exception in testWriteAndRead() " + e);
        } catch (ParseException e) {
            fail("Exception in testWriteAndRead() " + e);
        }
    }

    @Test
    public void testWriteAndReadWhenOnlyOneEntry() {
        try {
            CappedMinuteCount cmc1 = new CappedMinuteCount(2);
            cmc1.increment(DATE_FORMAT.parse("20130102 123456"), 3L);
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            DataOutput out = new DataOutputStream(baos);
            cmc1.write(out);
            DataInput in = new DataInputStream(new ByteArrayInputStream(baos.toByteArray()));
            CappedMinuteCount readCmc1 = new CappedMinuteCount();
            readCmc1.readFields(in);
            assertEquals(cmc1, readCmc1);
        } catch (IOException e) {
            fail("Exception in testWriteAndRead() " + e);
        } catch (ParseException e) {
            fail("Exception in testWriteAndRead() " + e);
        }
    }

    @Test
    public void testMerge() {
        try {
            CappedMinuteCount cmc1 = new CappedMinuteCount(100, DATE_FORMAT.parse("20140102 123456"));
            cmc1.increment(DATE_FORMAT.parse("20130102 123456"), 3L);
            CappedMinuteCount cmc2 = new CappedMinuteCount(100, DATE_FORMAT.parse("20140102 123456"), 5L);
            cmc2.increment(DATE_FORMAT.parse("20150102 123456"), 500L);

            CappedMinuteCount expectedResult = new CappedMinuteCount(100);
            expectedResult.increment(DATE_FORMAT.parse("20130102 123456"), 3L);
            expectedResult.increment(DATE_FORMAT.parse("20140102 123456"), 6L);
            expectedResult.increment(DATE_FORMAT.parse("20150102 123456"), 500L);

            cmc1.merge(cmc2);

            assertEquals(expectedResult, cmc1);
        } catch (ParseException e) {
            fail("Exception parsing date in testMerge() " + e);
        }
    }

    @Test
    public void testMergeWhenOneIsFull() {
        try {
            CappedMinuteCount cmc1 = new CappedMinuteCount(3, DATE_FORMAT.parse("20140102 123456"), 5L);
            CappedMinuteCount cmc2 = new CappedMinuteCount(3, DATE_FORMAT.parse("20140102 123456"), 5L);
            cmc2.increment(DATE_FORMAT.parse("20130102 123456"), 1);
            cmc2.increment(DATE_FORMAT.parse("201550102 123456"), 1);
            cmc2.increment(DATE_FORMAT.parse("20160102 123456"), 1);
            assertFalse(cmc1.isFull());
            assertTrue(cmc2.isFull());
            cmc1.merge(cmc2);
            assertTrue(cmc1.isFull());
        } catch (ParseException e) {
            fail("Exception parsing date in testMerge() " + e);
        }
    }

    @Test
    public void testFailedMerge() {
        try {
            CappedMinuteCount cmc1 = new CappedMinuteCount(100, DATE_FORMAT.parse("20140102 123456"));
            IntArray ia1 = new IntArray(new int[]{1,2,3,4});
            cmc1.merge(ia1);
        } catch (IllegalArgumentException e) {
            return;
        } catch (ParseException e) {
            fail("Exception parsing date in testFailedMerge()");
        }
        fail("IllegalArgumentException should have been thrown.");
    }

    @Test
    public void testMergeFailsWhenDifferentMaxSizes() {
        CappedMinuteCount cmc1 = new CappedMinuteCount(100);
        CappedMinuteCount cmc2 = new CappedMinuteCount(10);
        try {
            cmc1.merge(cmc2);
        } catch (IllegalArgumentException e) {
            return;
        }
        fail("IllegalArgumentException should have been thrown");
    }

    @Test
    public void testEquals() {
        try {
            CappedMinuteCount cmc1 = new CappedMinuteCount(100, DATE_FORMAT.parse("20140102 123456"), 5L);
            cmc1.increment(DATE_FORMAT.parse("20130102 123456"), 3);
            CappedMinuteCount cmc2 = new CappedMinuteCount(100, DATE_FORMAT.parse("20140102 123456"), 5L);
            cmc2.increment(DATE_FORMAT.parse("20130102 123456"), 1);
            cmc2.increment(DATE_FORMAT.parse("20130102 123456"), 1);
            cmc2.increment(DATE_FORMAT.parse("20130102 123456"), 1);
            CappedMinuteCount cmc3 = new CappedMinuteCount(100, DATE_FORMAT.parse("20170102 123456"), 5L);

            assertEquals(cmc1, cmc2);
            assertNotEquals(cmc1, cmc3);
        } catch (ParseException e) {
            fail("Exception in testEquals() " + e);
        }
    }

    @Test
    public void testNotEqualIfDifferentMaximumSizes() {
        try {
            CappedMinuteCount cmc1 = new CappedMinuteCount(100, DATE_FORMAT.parse("20140102 123456"), 5L);
            CappedMinuteCount cmc2 = new CappedMinuteCount(101, DATE_FORMAT.parse("20140102 123456"), 5L);
            assertNotEquals(cmc1, cmc2);
        } catch (ParseException e) {
            fail("Exception in testEquals() " + e);
        }
    }

    @Test
    public void testEqualsWhenFull() {
        try {
            // Test equals when one full and the other isn't
            CappedMinuteCount cmc1 = new CappedMinuteCount(2, DATE_FORMAT.parse("20140102 123456"), 5L);
            cmc1.increment(DATE_FORMAT.parse("20140102 234556"), 100L);
            cmc1.increment(DATE_FORMAT.parse("20150103 234556"), 1L);
            cmc1.increment(DATE_FORMAT.parse("20151112 234556"), 1000L);
            CappedMinuteCount cmc2 = new CappedMinuteCount(2, DATE_FORMAT.parse("20140102 123456"), 5L);
            assertTrue(cmc1.isFull());
            assertFalse(cmc2.isFull());
            assertNotEquals(cmc1, cmc2);
        } catch (ParseException e) {
            fail("Exception in testEqualsWhenFull() " + e);
        }
    }

    @Test
    public void testIncrement() {
        try {
            CappedMinuteCount cmc1 = new CappedMinuteCount(100, DATE_FORMAT.parse("20140102 123456"), 5L);
            cmc1.increment(DATE_FORMAT.parse("20140102 123412"), 1L);
            assertEquals(6L, cmc1.getCount(DATE_FORMAT.parse("20140102 123400")));
        } catch (ParseException e) {
            fail("Exception parsing date in testFailedMerge()");
        }
    }

    @Test
    public void testGetCount() {
        try {
            CappedMinuteCount cmc1 = new CappedMinuteCount(100, DATE_FORMAT.parse("20140102 123456"), 5L);
            cmc1.increment(DATE_FORMAT.parse("20140102 123412"), 1L);
            cmc1.increment(DATE_FORMAT.parse("20140110 105612"), 1L);

            assertEquals(6L, cmc1.getCount(DATE_FORMAT.parse("20140102 123401")));
            assertEquals(1L, cmc1.getCount(DATE_FORMAT.parse("20140110 105600")));
        } catch (ParseException e) {
            fail("Exception parsing date in testFailedMerge()");
        }
    }

    @Test
    public void testGetActiveMinutes() {
        try {
            CappedMinuteCount cmc1 = new CappedMinuteCount(100, DATE_FORMAT.parse("20140102 123456"), 1L);
            cmc1.increment(DATE_FORMAT.parse("20140102 234556"), 100L);
            cmc1.increment(DATE_FORMAT.parse("20150103 234556"), 1L);
            cmc1.increment(DATE_FORMAT.parse("20151112 234556"), 1000L);

            Date[] expectedMinutes = new Date[4];
            expectedMinutes[0] = DATE_FORMAT.parse("20140102 123400");
            expectedMinutes[1] = DATE_FORMAT.parse("20140102 234500");
            expectedMinutes[2] = DATE_FORMAT.parse("20150103 234500");
            expectedMinutes[3] = DATE_FORMAT.parse("20151112 234500");

            assertArrayEquals(expectedMinutes, cmc1.getActiveMinutes());
        } catch (ParseException e) {
            fail("Exception parsing date in testFailedMerge()");
        }
    }

    @Test
    public void testFull() {
        try {
            CappedMinuteCount cmc1 = new CappedMinuteCount(3);
            cmc1.increment(DATE_FORMAT.parse("20140102 234556"), 1L);
            assertFalse(cmc1.isFull());
            assertEquals(1, cmc1.getNumberOfActiveMinutes());
            cmc1.increment(DATE_FORMAT.parse("20140102 234556"), 100L);
            assertFalse(cmc1.isFull());
            assertEquals(1, cmc1.getNumberOfActiveMinutes());
            cmc1.increment(DATE_FORMAT.parse("20151212 091011"), 500L);
            assertFalse(cmc1.isFull());
            assertEquals(2, cmc1.getNumberOfActiveMinutes());
            cmc1.increment(DATE_FORMAT.parse("20130707 091011"), 500L);
            assertFalse(cmc1.isFull());
            assertEquals(3, cmc1.getNumberOfActiveMinutes());
            cmc1.increment(DATE_FORMAT.parse("20130808 091011"), 17L);
            assertTrue(cmc1.isFull());
            assertEquals(0, cmc1.getNumberOfActiveMinutes());
        } catch (ParseException e) {
            fail("Exception parsing date in testFailedMerge()");
        }
    }

    @Test
    public void testCloneOfFullSet() {
        try {
            CappedMinuteCount cmc1 = new CappedMinuteCount(3);
            cmc1.increment(DATE_FORMAT.parse("20130102 234556"), 1L);
            cmc1.increment(DATE_FORMAT.parse("20140102 234556"), 1L);
            cmc1.increment(DATE_FORMAT.parse("20150102 234556"), 1L);
            cmc1.increment(DATE_FORMAT.parse("20160102 234556"), 1L);
            assertTrue(cmc1.isFull());
            CappedMinuteCount clone = cmc1.clone();
            assertTrue(clone.isFull());
            assertEquals(cmc1.getMaxEntries(), clone.getMaxEntries());
            assertArrayEquals(cmc1.getActiveMinutes(), clone.getActiveMinutes());
        } catch (ParseException e) {
            fail("Exception parsing date in testFailedMerge()");
        }
    }

}
