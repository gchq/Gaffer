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

import static org.junit.Assert.*;

/**
 * Unit tests for {@link BeforeTimePredicate}. Contains tests for writing and reading,
 * equals and accept.
 */
public class TestBeforeTimePredicate {

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
        BeforeTimePredicate predicate = new BeforeTimePredicate(sevenDaysBefore);
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(baos);
        predicate.write(out);
        ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
        DataInputStream in = new DataInputStream(bais);
        BeforeTimePredicate read = new BeforeTimePredicate();
        read.readFields(in);
        assertEquals(predicate, read);
    }

    @Test
    public void testAccept() {
        BeforeTimePredicate predicate = new BeforeTimePredicate(sixDaysBefore);
        assertTrue(predicate.accept(sevenDaysBefore, sixDaysBefore));
        assertFalse(predicate.accept(sixDaysBefore, fiveDaysBefore));
        predicate = new BeforeTimePredicate(fiveDaysBefore);
        assertTrue(predicate.accept(sevenDaysBefore, sixDaysBefore));
        assertTrue(predicate.accept(sixDaysBefore, fiveDaysBefore));
    }

    @Test
    public void testEquals() {
        BeforeTimePredicate predicate1 = new BeforeTimePredicate(sevenDaysBefore);
        BeforeTimePredicate predicate2 = new BeforeTimePredicate(sevenDaysBefore);
        assertEquals(predicate1, predicate2);
        predicate2 = new BeforeTimePredicate(sixDaysBefore);
        assertNotEquals(predicate1, predicate2);
    }

}
