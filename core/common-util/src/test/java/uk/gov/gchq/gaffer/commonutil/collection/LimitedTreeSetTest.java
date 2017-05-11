/*
 * Copyright 2017 Crown Copyright
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package uk.gov.gchq.gaffer.commonutil.collection;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.SortedSet;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class LimitedTreeSetTest {

    LimitedTreeSet ts;
    Object objArray[] = new Object[1000];

    @Test
    public void test_ConstructorLjava_util_Comparator() {
        // Test for method java.util.LimitedTreeSet(java.util.Comparator)
        LimitedTreeSet myLimitedTreeSet = new LimitedTreeSet<>(new ReversedIntegerComparator(), 2);
        assertTrue("Did not construct correct LimitedTreeSet", myLimitedTreeSet.isEmpty());
        myLimitedTreeSet.add(new Integer(1));
        myLimitedTreeSet.add(new Integer(2));
        assertTrue(
                "Answered incorrect first element--did not use custom comparator ",
                myLimitedTreeSet.first().equals(new Integer(2)));
        assertTrue(
                "Answered incorrect last element--did not use custom comparator ",
                myLimitedTreeSet.last().equals(new Integer(1)));
    }

    @Test
    public void test_addLjava_lang_Object() {
        // Test for method boolean java.util.LimitedTreeSet.add(java.lang.Object)
        ts.add(new Integer(-8));
        assertTrue("Failed to add Object", ts.contains(new Integer(-8)));
        ts.add(objArray[0]);
        assertTrue("Added existing element", ts.size() == objArray.length + 1);
    }

    @Test
    public void test_addAllLjava_util_Collection() {
        // Test for method boolean
        // java.util.LimitedTreeSet.addAll(java.util.Collection)
        LimitedTreeSet s = new LimitedTreeSet();
        s.addAll(ts);
        assertTrue("Incorrect size after add", s.size() == ts.size());
        Iterator i = ts.iterator();
        while (i.hasNext())
            assertTrue("Returned incorrect set", s.contains(i.next()));
    }

    @Test
    public void test_clear() {
        // Test for method void java.util.LimitedTreeSet.clear()
        ts.clear();
        assertEquals("Returned non-zero size after clear", 0, ts.size());
        assertTrue("Found element in cleared set", !ts.contains(objArray[0]));
    }

    @Test
    public void test_clone() {
        // Test for method java.lang.Object java.util.LimitedTreeSet.clone()
        LimitedTreeSet s = (LimitedTreeSet) ts.clone();
        Iterator i = ts.iterator();
        while (i.hasNext())
            assertTrue("Clone failed to copy all elements", s
                    .contains(i.next()));
    }

    @Test
    public void test_comparator() {
        // Test for method java.util.Comparator java.util.LimitedTreeSet.comparator()
        ReversedIntegerComparator comp = new ReversedIntegerComparator();
        LimitedTreeSet myLimitedTreeSet = new LimitedTreeSet(comp, 10);
        assertTrue("Answered incorrect comparator",
                myLimitedTreeSet.comparator() == comp);
    }

    @Test
    public void test_containsLjava_lang_Object() {
        // Test for method boolean java.util.LimitedTreeSet.contains(java.lang.Object)
        assertTrue("Returned false for valid Object", ts
                .contains(objArray[objArray.length / 2]));
        assertTrue("Returned true for invalid Object", !ts
                .contains(new Integer(-9)));
        try {
            ts.contains(new Object());
        } catch (ClassCastException e) {
            // Correct
            return;
        }
        fail("Failed to throw exception when passed invalid element");
    }

    @Test
    public void test_first() {
        // Test for method java.lang.Object java.util.LimitedTreeSet.first()
        assertTrue("Returned incorrect first element",
                ts.first() == objArray[0]);
    }

    @Test
    public void test_headSetLjava_lang_Object() {
        // Test for method java.util.SortedSet
        // java.util.LimitedTreeSet.headSet(java.lang.Object)
        Set s = ts.headSet(new Integer(100));
        assertEquals("Returned set of incorrect size", 100, s.size());
        for (int i = 0; i < 100; i++)
            assertTrue("Returned incorrect set", s.contains(objArray[i]));
    }

    @Test
    public void test_isEmpty() {
        // Test for method boolean java.util.LimitedTreeSet.isEmpty()
        assertTrue("Empty set returned false", new LimitedTreeSet().isEmpty());
        assertTrue("Non-Empty returned true", !ts.isEmpty());
    }

    @Test
    public void test_iterator() {
        // Test for method java.util.Iterator java.util.LimitedTreeSet.iterator()
        LimitedTreeSet s = new LimitedTreeSet();
        s.addAll(ts);
        Iterator i = ts.iterator();
        Set as = new HashSet(Arrays.asList(objArray));
        while (i.hasNext())
            as.remove(i.next());
        assertEquals("Returned incorrect iterator", 0, as.size());
    }

    @Test
    public void test_last() {
        // Test for method java.lang.Object java.util.LimitedTreeSet.last()
        assertTrue("Returned incorrect last element",
                ts.last() == objArray[objArray.length - 1]);
    }

    @Test
    public void test_removeLjava_lang_Object() {
        // Test for method boolean java.util.LimitedTreeSet.remove(java.lang.Object)
        ts.remove(objArray[0]);
        assertTrue("Failed to remove object", !ts.contains(objArray[0]));
        assertTrue("Failed to change size after remove",
                ts.size() == objArray.length - 1);
        try {
            ts.remove(new Object());
        } catch (ClassCastException e) {
            // Correct
            return;
        }
        fail("Failed to throw exception when past uncomparable value");
    }

    @Test
    public void test_size() {
        // Test for method int java.util.LimitedTreeSet.size()
        assertTrue("Returned incorrect size", ts.size() == objArray.length);
    }

    @Test
    public void test_subSetLjava_lang_ObjectLjava_lang_Object() {
        // Test for method java.util.SortedSet
        // java.util.LimitedTreeSet.subSet(java.lang.Object, java.lang.Object)
        final int startPos = objArray.length / 4;
        final int endPos = 3 * objArray.length / 4;
        SortedSet aSubSet = ts.subSet(objArray[startPos], objArray[endPos]);
        assertTrue("Subset has wrong number of elements",
                aSubSet.size() == (endPos - startPos));
        for (int counter = startPos; counter < endPos; counter++)
            assertTrue("Subset does not contain all the elements it should",
                    aSubSet.contains(objArray[counter]));
        int result;
        try {
            ts.subSet(objArray[3], objArray[0]);
            result = 0;
        } catch (IllegalArgumentException e) {
            result = 1;
        }
        assertEquals("end less than start should throw", 1, result);
    }

    @Test
    public void test_tailSetLjava_lang_Object() {
        // Test for method java.util.SortedSet
        // java.util.LimitedTreeSet.tailSet(java.lang.Object)
        Set s = ts.tailSet(new Integer(900));
        assertEquals("Returned set of incorrect size", 100, s.size());
        for (int i = 900; i < objArray.length; i++)
            assertTrue("Returned incorrect set", s.contains(objArray[i]));
    }

    @Test
    public void test_equals() throws Exception {
        // comparing LimitedTreeSets with different object types
        Set s1 = new LimitedTreeSet();
        Set s2 = new LimitedTreeSet();
        s1.add("key1");
        s1.add("key2");
        s2.add(new Integer(1));
        s2.add(new Integer(2));
        assertFalse("Sets should not be equal 1", s1.equals(s2));
        assertFalse("Sets should not be equal 2", s2.equals(s1));
        // comparing LimitedTreeSet with HashSet
        s1 = new LimitedTreeSet();
        s2 = new HashSet();
        s1.add("key");
        s2.add(new Object());
        assertFalse("Sets should not be equal 3", s1.equals(s2));
        assertFalse("Sets should not be equal 4", s2.equals(s1));
        // comparing LimitedTreeSets with not-comparable objects inside
        s1 = new LimitedTreeSet();
        s2 = new LimitedTreeSet();
        s1.add(new Object());
        s2.add(new Object());
        assertFalse("Sets should not be equal 5", s1.equals(s2));
        assertFalse("Sets should not be equal 6", s2.equals(s1));
    }

    /**
     * Sets up the fixture, for example, open a network connection. This method
     * is called before a test is executed.
     */
    @Before
    public void setUp() {
        ts = new LimitedTreeSet();
        for (int i = 0; i < objArray.length; i++) {
            Object x = objArray[i] = new Integer(i);
            ts.add(x);
        }
    }

    public static class ReversedIntegerComparator implements Comparator {
        public int compare(Object o1, Object o2) {
            return -(((Integer) o1).compareTo((Integer) o2));
        }

        public boolean equals(Object o1, Object o2) {
            return ((Integer) o1).compareTo((Integer) o2) == 0;
        }
    }
}
