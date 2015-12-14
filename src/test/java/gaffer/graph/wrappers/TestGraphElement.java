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
package gaffer.graph.wrappers;

import gaffer.graph.Edge;
import gaffer.graph.Entity;

import java.io.ByteArrayOutputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.TreeSet;

import org.apache.hadoop.io.WritableComparator;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Unit tests for {@link GraphElement}. In particular checks that the compare method
 * works appropriately, which ensures that MapReduce jobs group together elements correctly.
 */
public class TestGraphElement {

	private static final String visibility1 = "private";
	private static final String visibility2 = "public";
	
	@Test
	public void testCompareSimple() {
		// Want an entity A to appear before an edge with src of A,
		// which should be before an entity B which should be before an
		// edge with source B, etc.
		// Create start and end dates
		Calendar startCalendar = new GregorianCalendar();
		startCalendar.clear();
		startCalendar.set(2014, Calendar.JANUARY, 1);
		Date startDate = startCalendar.getTime();
		Calendar endCalendar = new GregorianCalendar();
		endCalendar.clear();
		endCalendar.set(2014, Calendar.JANUARY, 2);
		Date endDate = endCalendar.getTime();

		// Create entities, edges and corresponding graph elements
		Entity entity1 = new Entity("type", "A", "infotype", "infosubtype", visibility1, startDate, endDate);
		Entity entity2 = new Entity("type", "B", "infotype", "infosubtype", visibility1, startDate, endDate);
		Edge edge1 = new Edge("type", "A", "dtype", "dvalue", "type", "subtype", true, visibility1, startDate, endDate);
		Edge edge2 = new Edge("type", "B", "dtype", "dvalue", "type", "subtype", true, visibility1, startDate, endDate);
		GraphElement gEn1 = new GraphElement(entity1);
		GraphElement gEn2 = new GraphElement(entity2);
		GraphElement gEd1 = new GraphElement(edge1);
		GraphElement gEd2 = new GraphElement(edge2);

		// Add to ordered set
		TreeSet<GraphElement> set = new TreeSet<GraphElement>();
		set.add(gEn1);
		set.add(gEn2);
		set.add(gEd1);
		set.add(gEd2);

		// Check ordering
		Iterator<GraphElement> it = set.iterator();
		assertEquals(gEn1, it.next());
		assertEquals(gEd1, it.next());
		assertEquals(gEn2, it.next());
		assertEquals(gEd2, it.next());
	}

	@Test
	public void testCompareComplex() {
		// Some dates
		Calendar calendar1 = new GregorianCalendar();
		calendar1.clear();
		calendar1.set(2014, Calendar.JANUARY, 1);
		Date date1 = calendar1.getTime();
		Calendar calendar2 = new GregorianCalendar();
		calendar2.clear();
		calendar2.set(2014, Calendar.JANUARY, 2);
		Date date2 = calendar2.getTime();
		Calendar calendar3 = new GregorianCalendar();
		calendar3.clear();
		calendar3.set(2014, Calendar.JANUARY, 3);
		Date date3 = calendar3.getTime();

		// Create entities, edges and corresponding graph elements
		Entity entity1 = new Entity("typeX", "A", "typeX", "subtypeX", visibility1, date1, date2);
		Entity entity2 = new Entity("typeX", "A", "typeX", "subtypeX", visibility1, date1, date3);
		Entity entity3 = new Entity("typeX", "A", "typeX", "subtypeX", visibility1, date2, date3);
		Entity entity4 = new Entity("typeX", "A", "typeX", "subtypeX", visibility2, date2, date3);
		Entity entity5 = new Entity("typeX", "A", "typeX", "subtypeY", visibility2, date2, date3);
		Entity entity6 = new Entity("typeX", "A", "typeY", "subtypeY", visibility2, date2, date3);
		Entity entity7 = new Entity("typeX", "B", "typeX", "subtypeX", visibility1, date1, date2);
		Edge edge1 = new Edge("typeX", "A", "utypeX", "dvalueX", "typeX", "subtypeX", true, visibility1, date1, date2);
		Edge edge2 = new Edge("typeX", "B", "utypeX", "dvalueX", "typeX", "subtypeX", false, visibility2, date2, date3);
		Edge edge3 = new Edge("typeX", "B", "utypeX", "dvalueX", "typeX", "subtypeX", true, visibility1, date1, date2);
		Edge edge4 = new Edge("typeX", "B", "utypeX", "dvalueX", "typeX", "subtypeX", true, visibility1, date1, date3);
		Edge edge5 = new Edge("typeX", "B", "utypeX", "dvalueX", "typeX", "subtypeX", true, visibility1, date2, date3);
		Edge edge6 = new Edge("typeX", "B", "utypeX", "dvalueX", "typeX", "subtypeX", true, visibility2, date2, date3);
		Edge edge7 = new Edge("typeX", "B", "utypeX", "dvalueX", "typeX", "subtypeY", true, visibility2, date2, date3);
		Edge edge8 = new Edge("typeX", "B", "utypeX", "dvalueX", "typeY", "subtypeY", true, visibility2, date2, date3);
		Edge edge9 = new Edge("typeX", "B", "utypeX", "dvalueY", "typeY", "subtypeY", true, visibility2, date2, date3);
		Edge edge10 = new Edge("typeX", "B", "utypeY", "dvalueY", "typeY", "subtypeY", true, visibility2, date2, date3);
		Edge edge11 = new Edge("typeX", "C", "utypeY", "dvalueY", "typeY", "subtypeY", true, visibility2, date2, date3);
		Edge edge12 = new Edge("typeY", "C", "utypeY", "dvalueY", "typeY", "subtypeY", true, visibility2, date2, date3);
		
		GraphElement gEn1 = new GraphElement(entity1);
		GraphElement gEn2 = new GraphElement(entity2);
		GraphElement gEn3 = new GraphElement(entity3);
		GraphElement gEn4 = new GraphElement(entity4);
		GraphElement gEn5 = new GraphElement(entity5);
		GraphElement gEn6 = new GraphElement(entity6);
		GraphElement gEn7 = new GraphElement(entity7);
		GraphElement gEd1 = new GraphElement(edge1);
		GraphElement gEd2 = new GraphElement(edge2);
		GraphElement gEd3 = new GraphElement(edge3);
		GraphElement gEd4 = new GraphElement(edge4);
		GraphElement gEd5 = new GraphElement(edge5);
		GraphElement gEd6 = new GraphElement(edge6);
		GraphElement gEd7 = new GraphElement(edge7);
		GraphElement gEd8 = new GraphElement(edge8);
		GraphElement gEd9 = new GraphElement(edge9);
		GraphElement gEd10 = new GraphElement(edge10);
		GraphElement gEd11 = new GraphElement(edge11);
		GraphElement gEd12 = new GraphElement(edge12);

		// Add to ordered set
		TreeSet<GraphElement> set = new TreeSet<GraphElement>();
		set.add(gEn1);
		set.add(gEn2);
		set.add(gEn3);
		set.add(gEn4);
		set.add(gEn5);
		set.add(gEn6);
		set.add(gEn7);
		set.add(gEd1);
		set.add(gEd2);
		set.add(gEd3);
		set.add(gEd4);
		set.add(gEd5);
		set.add(gEd6);
		set.add(gEd7);
		set.add(gEd8);
		set.add(gEd9);
		set.add(gEd10);
		set.add(gEd11);
		set.add(gEd12);

		// Check ordering
		Iterator<GraphElement> it = set.iterator();
		assertEquals(gEn1, it.next());
		assertEquals(gEn2, it.next());
		assertEquals(gEn3, it.next());
		assertEquals(gEn4, it.next());
		assertEquals(gEn5, it.next());
		assertEquals(gEn6, it.next());
		assertEquals(gEd1, it.next());
		assertEquals(gEn7, it.next());
		assertEquals(gEd2, it.next());
		assertEquals(gEd3, it.next());
		assertEquals(gEd4, it.next());
		assertEquals(gEd5, it.next());
		assertEquals(gEd6, it.next());
		assertEquals(gEd7, it.next());
		assertEquals(gEd8, it.next());
		assertEquals(gEd9, it.next());
		assertEquals(gEd10, it.next());
		assertEquals(gEd11, it.next());
		assertEquals(gEd12, it.next());
		assertFalse(it.hasNext());
	}

	@Test
	public void testCompareIdentical() {
		// Create start and end dates
		Calendar startCalendar = new GregorianCalendar();
		startCalendar.clear();
		startCalendar.set(2014, Calendar.JANUARY, 1);
		Date startDate = startCalendar.getTime();
		Calendar endCalendar = new GregorianCalendar();
		endCalendar.clear();
		endCalendar.set(2014, Calendar.JANUARY, 2);
		Date endDate = endCalendar.getTime();
		Entity entity1 = new Entity("type", "A", "infotype", "infosubtype", visibility1, startDate, endDate);
		Entity entity2 = new Entity("type", "A", "infotype", "infosubtype", visibility1, startDate, endDate);
		GraphElement gEn1 = new GraphElement(entity1);
		GraphElement gEn2 = new GraphElement(entity2);
		
		// Check compare is 0 and only 1 element when added to a set
		assertEquals(0, gEn1.compareTo(gEn2));
		Set<GraphElement> set = new HashSet<GraphElement>();
		set.add(gEn1);
		set.add(gEn2);
		Iterator<GraphElement> it = set.iterator();
		GraphElement found = it.next();
		assertEquals(gEn1, found);
		assertEquals(gEn2, found);
		assertFalse(it.hasNext());
	}

	@Test
	public void testRawComparator() {
		try {
			GraphElement[] elements = createGraphElementsForTestingCompare();

			// Serialise edges
			byte[][] serialisedElements = new byte[elements.length][];
			for (int i = 0; i < elements.length; i++) {
				serialisedElements[i] = getBytes(elements[i]);
			}

			// Check order
			GraphElement.GraphElementComparator rawComparator = new GraphElement.GraphElementComparator();
			for (int i = 0; i < serialisedElements.length - 1; i++) {
				assertTrue(rawComparator.compare(serialisedElements[i], 0, serialisedElements[i].length,
						serialisedElements[i + 1], 0, serialisedElements[i + 1].length) < 0);
			}
		} catch (IOException e) {
			fail("Exception in testRawComparator() " + e);
		}
	}

	@Test
	public void testRawComparatorIsRegistered() {
		assertEquals(GraphElement.GraphElementComparator.class, WritableComparator.get(GraphElement.class).getClass());
	}

	private static GraphElement[] createGraphElementsForTestingCompare() {
		// Some dates
		Calendar calendar1 = new GregorianCalendar();
		calendar1.clear();
		calendar1.set(2014, Calendar.JANUARY, 1);
		Date date1 = calendar1.getTime();
		Calendar calendar2 = new GregorianCalendar();
		calendar2.clear();
		calendar2.set(2014, Calendar.JANUARY, 2);
		Date date2 = calendar2.getTime();
		Calendar calendar3 = new GregorianCalendar();
		calendar3.clear();
		calendar3.set(2014, Calendar.JANUARY, 3);
		Date date3 = calendar3.getTime();

		Entity entity1 = new Entity("typeX", "A", "typeX", "subtypeX", visibility1, date1, date2);
		Entity entity2 = new Entity("typeX", "A", "typeX", "subtypeX", visibility1, date1, date3);
		Entity entity3 = new Entity("typeX", "A", "typeX", "subtypeX", visibility1, date2, date3);
		Entity entity4 = new Entity("typeX", "A", "typeX", "subtypeX", visibility2, date2, date3);
		Entity entity5 = new Entity("typeX", "A", "typeX", "subtypeY", visibility2, date2, date3);
		Entity entity6 = new Entity("typeX", "A", "typeY", "subtypeY", visibility2, date2, date3);
		Entity entity7 = new Entity("typeX", "B", "typeX", "subtypeX", visibility1, date1, date2);
		Edge edge1 = new Edge("typeX", "A", "utypeX", "dvalueX", "typeX", "subtypeX", true, visibility1, date1, date2);
		Edge edge2 = new Edge("typeX", "B", "utypeX", "dvalueX", "typeX", "subtypeX", false, visibility2, date2, date3);
		Edge edge3 = new Edge("typeX", "B", "utypeX", "dvalueX", "typeX", "subtypeX", true, visibility1, date1, date2);
		Edge edge4 = new Edge("typeX", "B", "utypeX", "dvalueX", "typeX", "subtypeX", true, visibility1, date1, date3);
		Edge edge5 = new Edge("typeX", "B", "utypeX", "dvalueX", "typeX", "subtypeX", true, visibility1, date2, date3);
		Edge edge6 = new Edge("typeX", "B", "utypeX", "dvalueX", "typeX", "subtypeX", true, visibility2, date2, date3);
		Edge edge7 = new Edge("typeX", "B", "utypeX", "dvalueX", "typeX", "subtypeY", true, visibility2, date2, date3);
		Edge edge8 = new Edge("typeX", "B", "utypeX", "dvalueX", "typeY", "subtypeY", true, visibility2, date2, date3);
		Edge edge9 = new Edge("typeX", "B", "utypeX", "dvalueY", "typeY", "subtypeY", true, visibility2, date2, date3);
		Edge edge10 = new Edge("typeX", "B", "utypeY", "dvalueY", "typeY", "subtypeY", true, visibility2, date2, date3);
		Edge edge11 = new Edge("typeX", "C", "utypeY", "dvalueY", "typeY", "subtypeY", true, visibility2, date2, date3);
		Edge edge12 = new Edge("typeY", "C", "utypeY", "dvalueY", "typeY", "subtypeY", true, visibility2, date2, date3);

		// Insert graph elements in correct order
		GraphElement[] elements = new GraphElement[19];
		elements[0] = new GraphElement(entity1);
		elements[1] = new GraphElement(entity2);
		elements[2] = new GraphElement(entity3);
		elements[3] = new GraphElement(entity4);
		elements[4] = new GraphElement(entity5);
		elements[5] = new GraphElement(entity6);
		elements[6] = new GraphElement(edge1);
		elements[7] = new GraphElement(entity7);
		elements[8] = new GraphElement(edge2);
		elements[9] = new GraphElement(edge3);
		elements[10] = new GraphElement(edge4);
		elements[11] = new GraphElement(edge5);
		elements[12] = new GraphElement(edge6);
		elements[13] = new GraphElement(edge7);
		elements[14] = new GraphElement(edge8);
		elements[15] = new GraphElement(edge9);
		elements[16] = new GraphElement(edge10);
		elements[17] = new GraphElement(edge11);
		elements[18] = new GraphElement(edge12);

		return elements;
	}

	private static byte[] getBytes(GraphElement element) throws IOException {
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		DataOutput out = new DataOutputStream(baos);
		element.write(out);
		return baos.toByteArray();
	}

	@Test
	public void testCloneWhenWrapsEntity() {
		// Create a GraphElement
		Calendar startCalendar = new GregorianCalendar();
		startCalendar.clear();
		startCalendar.set(2014, Calendar.JANUARY, 1);
		Date startDate = startCalendar.getTime();
		Calendar endCalendar = new GregorianCalendar();
		endCalendar.clear();
		endCalendar.set(2014, Calendar.JANUARY, 2);
		Date endDate = endCalendar.getTime();
		Entity entity = new Entity("type", "A", "infotype", "infosubtype", visibility1, startDate, endDate);
		GraphElement element = new GraphElement(entity);

		// Clone it
		GraphElement clonedElement = element.clone();

		// Check clone is the same as the original
		assertTrue(clonedElement.equals(element));

		// Change original
		element.getEntity().setEntityType("newType");

		// Check clone is not equal to the orginal
		assertNotEquals(element, clonedElement);
	}

	@Test
	public void testCloneWhenWrapsEdge() {
		// Create a GraphElement
		Calendar startCalendar = new GregorianCalendar();
		startCalendar.clear();
		startCalendar.set(2014, Calendar.JANUARY, 1);
		Date startDate = startCalendar.getTime();
		Calendar endCalendar = new GregorianCalendar();
		endCalendar.clear();
		endCalendar.set(2014, Calendar.JANUARY, 2);
		Date endDate = endCalendar.getTime();
		Edge edge = new Edge("type", "A", "type", "B", "infotype", "infosubtype", true, visibility1, startDate, endDate);
		GraphElement element = new GraphElement(edge);

		// Clone it
		GraphElement clonedElement = element.clone();

		// Check clone is the same as the original
		assertEquals(element, clonedElement);

		// Change original
		element = new GraphElement(new Edge("type", "C", "type", "B", "infotype", "infosubtype", true, visibility1, startDate, endDate));

		// Check clone is not equal to the orginal
		assertNotEquals(element, clonedElement);
	}

}
