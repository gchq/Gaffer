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
package gaffer.predicate.time.impl;

import org.junit.Test;

import java.io.*;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

/**
 * Unit tests for {@link AfterTimePredicate}. Contains tests for writing and reading,
 * equals and accept.
 */
public class TestAfterTimePredicate {

    private static Date sevenDaysBefore;
    private static Date sixDaysBefore;
    private static Date fiveDaysBefore;

    static {
        Calendar sevenDaysBeforeCalendar = new GregorianCalendar();
        sevenDaysBeforeCalendar.setTime(new Date(System.currentTimeMillis()));
        sevenDaysBeforeCalendar.add(Calendar.DAY_OF_MONTH, -7);
        sevenDaysBefore = sevenDaysBeforeCalendar.getTime();
        sevenDaysBeforeCalendar.add(Calendar.DAY_OF_MONTH, 1);
        sixDaysBefore = sevenDaysBeforeCalendar.getTime();
        sevenDaysBeforeCalendar.add(Calendar.DAY_OF_MONTH, 1);
        fiveDaysBefore = sevenDaysBeforeCalendar.getTime();
    }

    @Test
    public void testWriteRead() throws IOException {
        AfterTimePredicate predicate = new AfterTimePredicate(sevenDaysBefore);
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(baos);
        predicate.write(out);
        ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
        DataInputStream in = new DataInputStream(bais);
        AfterTimePredicate read = new AfterTimePredicate();
        read.readFields(in);
        assertEquals(predicate, read);
    }

    @Test
    public void testAccept() {
        AfterTimePredicate predicate = new AfterTimePredicate(sevenDaysBefore);
        assertTrue(predicate.accept(sevenDaysBefore, sixDaysBefore));
        assertTrue(predicate.accept(sixDaysBefore, fiveDaysBefore));
        predicate = new AfterTimePredicate(sixDaysBefore);
        assertFalse(predicate.accept(sevenDaysBefore, sixDaysBefore));
        assertTrue(predicate.accept(sixDaysBefore, fiveDaysBefore));
    }

    @Test
    public void testEquals() {
        AfterTimePredicate predicate1 = new AfterTimePredicate(sevenDaysBefore);
        AfterTimePredicate predicate2 = new AfterTimePredicate(sevenDaysBefore);
        assertEquals(predicate1, predicate2);
        predicate2 = new AfterTimePredicate(sixDaysBefore);
        assertNotEquals(predicate1, predicate2);
    }

}
