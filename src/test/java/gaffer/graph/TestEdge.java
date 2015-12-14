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
package gaffer.graph;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.*;

import org.apache.hadoop.io.WritableComparator;
import org.junit.Test;

/**
 * Methods to test write/read, compare and clone methods on {@link Edge}.
 * Includes tests for behaviour when null or zero length values are supplied to
 * the constructor.
 */
public class TestEdge {

	private static final String visibility1 = "private";
	private static final String visibility2 = "public";
	private static Date startDate;
	private static Date endDate;
	static {
		Calendar startCalendar = new GregorianCalendar();
		startCalendar.clear();
		startCalendar.set(2014, Calendar.JANUARY, 1);
		startDate = startCalendar.getTime();
		Calendar endCalendar = new GregorianCalendar();
		endCalendar.clear();
		endCalendar.set(2014, Calendar.JANUARY, 2);
		endDate = endCalendar.getTime();
	}
	
	@Test
	public void testConstructorWhenNullSupplied() {
		// Create edge with null source type
		try {
			@SuppressWarnings("unused")
			Edge edge = new Edge(null, "A", "product", "P", "purchase", "instore", true, visibility1, startDate, endDate);
			fail("IllegalArgumentException should have been thrown for null source type");
		} catch (IllegalArgumentException e) { }

		// Create edge with null source value
		try {
			@SuppressWarnings("unused")
			Edge edge = new Edge("customer", null, "product", "P", "purchase", "instore", true, visibility1, startDate, endDate);
			fail("IllegalArgumentException should have been thrown for null source value");
		} catch (IllegalArgumentException e) { }

		// Create edge with null destination type
		try {
			@SuppressWarnings("unused")
			Edge edge = new Edge("customer", "A", null, "P", "purchase", "instore", true, visibility1, startDate, endDate);
			fail("IllegalArgumentException should have been thrown for null destination type");
		} catch (IllegalArgumentException e) { }

		// Create edge with null destination value
		try {
			@SuppressWarnings("unused")
			Edge edge = new Edge("customer", "A", "product", null, "purchase", "instore", true, visibility1, startDate, endDate);
			fail("IllegalArgumentException should have been thrown for null destination value");
		} catch (IllegalArgumentException e) { }

		// Create edge with null type
		try {
			@SuppressWarnings("unused")
			Edge edge = new Edge("customer", "A", "product", "P", null, "instore", true, visibility1, startDate, endDate);
			fail("IllegalArgumentException should have been thrown for null edge type");
		} catch (IllegalArgumentException e) { }

		// Create edge with null subType
		try {
			@SuppressWarnings("unused")
			Edge edge = new Edge("customer", "A", "product", "P", "purchase", null, true, visibility1, startDate, endDate);
			fail("IllegalArgumentException should have been thrown for null source value");
		} catch (IllegalArgumentException e) { }

		// Create edge with null startDate
		try {
			@SuppressWarnings("unused")
			Edge edge = new Edge("customer", "A", "product", "P", "purchase", "instore", true, visibility1, null, endDate);
			fail("IllegalArgumentException should have been thrown for null startDate");
		} catch (IllegalArgumentException e) { }

		// Create edge with null endDate
		try {
			@SuppressWarnings("unused")
			Edge edge = new Edge("customer", "A", "product", "P", "purchase", "instore", true, visibility1, startDate, null);
			fail("IllegalArgumentException should have been thrown for null endDate");
		} catch (IllegalArgumentException e) { }
	}

	@Test
	public void testConstructorWhenZeroLengthArgumentsSupplied() {
		// Create edge with zero length source type
		try {
			@SuppressWarnings("unused")
			Edge edge = new Edge("", "A", "product", "P", "purchase", "instore", true, visibility1, startDate, endDate);
			fail("IllegalArgumentException should have been thrown for zero length source type");
		} catch (IllegalArgumentException e) { }

		// Create edge with zero length source value
		try {
			@SuppressWarnings("unused")
			Edge edge = new Edge("customer", "", "product", "P", "purchase", "instore", true, visibility1, startDate, endDate);
			fail("IllegalArgumentException should have been thrown for zero length source type");
		} catch (IllegalArgumentException e) { }

		// Create edge with zero length destination type
		try {
			@SuppressWarnings("unused")
			Edge edge = new Edge("customer", "A", "", "P", "purchase", "instore", true, visibility1, startDate, endDate);
			fail("IllegalArgumentException should have been thrown for zero length destination type");
		} catch (IllegalArgumentException e) { }

		// Create edge with zero length destination value
		try {
			@SuppressWarnings("unused")
			Edge edge = new Edge("customer", "A", "product", "", "purchase", "instore", true, visibility1, startDate, endDate);
			fail("IllegalArgumentException should have been thrown for zero length destination value");
		} catch (IllegalArgumentException e) { }

		// Create edge with zero length type - this is allowed
		try {
			@SuppressWarnings("unused")
			Edge edge = new Edge("customer", "A", "product", "P", "", "instore", true, visibility1, startDate, endDate);
		} catch (IllegalArgumentException e) {
			fail("IllegalArgumentException was thrown for zero length edge type");
		}

		// Create edge with zero length subType - this is allowed
		try {
			@SuppressWarnings("unused")
			Edge edge = new Edge("customer", "A", "product", "P", "purchase", "", true, visibility1, startDate, endDate);
		} catch (IllegalArgumentException e) {
			fail("IllegalArgumentException was thrown for zero length edge subType");
		}
	}

	@Test
	public void testConstructorWhenStartDateAfterEndDate() {
		try {
			@SuppressWarnings("unused")
			Edge edge = new Edge("customer", "A", "product", "P", "purchase", "subtype", true, visibility1, endDate, startDate);
			fail("IllegalArgumentException should have been thrown for startDate before endDate");
		} catch (IllegalArgumentException e) { }
	}

	@Test
	public void testWriteAndRead() {
		try {
			// Create edge
			Edge edge = new Edge("customer", "A", "product", "P", "purchase", "instore", true, visibility1, startDate, endDate);

			// Write edge
			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			DataOutput out = new DataOutputStream(baos);
			edge.write(out);
			baos.close();

			// Read edge
			DataInput in = new DataInputStream(new ByteArrayInputStream(baos.toByteArray()));
			Edge readEdge = new Edge();
			readEdge.readFields(in);

			// Check get an equal edge back
			assertEquals(edge, readEdge);

		} catch (IOException e) {
			fail("Exception in testWriteAndRead() " + e);
		}
	}

	@Test
	public void testUndirectedEdge() {
		// Create first edge - undirected
		Edge edge1 = new Edge("customer", "A", "product", "P", "purchase", "customer", false, visibility1, startDate, endDate);

		// Create second edge, with the order of the type-values changed in the constructor
		Edge edge2 = new Edge("product", "P", "customer", "A", "purchase", "customer", false, visibility1, startDate, endDate);

		// Check equal
		assertEquals(edge1, edge2);
		assertEquals(edge2, edge1);
	}

	@Test
	public void testUndirectedEdgeReorders() {
		// Create undirected edge with source type greater than destination type
		Edge edge1 = new Edge("user", "A", "product", "P", "bought", "instore", false, visibility1, startDate, endDate);
		assertEquals("product", edge1.getSourceType());
		assertEquals("P", edge1.getSourceValue());
		assertEquals("user", edge1.getDestinationType());
		assertEquals("A", edge1.getDestinationValue());
		
		// Create undirected edge with equal types and source value greater than destination value
		Edge edge2 = new Edge("user", "Z", "user", "A", "bought", "instore", false, visibility1, startDate, endDate);
		assertEquals("user", edge2.getSourceType());
		assertEquals("A", edge2.getSourceValue());
		assertEquals("user", edge2.getDestinationType());
		assertEquals("Z", edge2.getDestinationValue());
		
	}

	@Test
	public void testSettersReorder() {
		// Create directed edge with source greater than destination - setters shouldn't change anything
		Edge edge1 = new Edge("user", "A", "product", "P", "bought", "instore", true, visibility1, startDate, endDate);
		edge1.setSourceTypeAndValue("userX", "B");
		edge1.setDestinationTypeAndValue("productX", "Q");
		assertEquals("userX", edge1.getSourceType());
		assertEquals("B", edge1.getSourceValue());
		assertEquals("productX", edge1.getDestinationType());
		assertEquals("Q", edge1.getDestinationValue());
		
		// Create undirected edge, then use setters to set source type greater than destination type
		Edge edge2 = new Edge("user", "A", "product", "P", "bought", "instore", false, visibility1, startDate, endDate);
		edge2.setSourceTypeAndValue("user", "A");
		edge2.setDestinationTypeAndValue("product", "P");
		assertEquals("product", edge2.getSourceType());
		assertEquals("P", edge2.getSourceValue());
		assertEquals("user", edge2.getDestinationType());
		assertEquals("A", edge2.getDestinationValue());
	}
	
	@Test
	public void testDirectedEdge() {
		// Create first edge - directed
		Edge edge1 = new Edge("customer", "A", "product", "P", "purchase", "instore", true, visibility1, startDate, endDate);

		// Create second edge, with the order of the type-values changed in the constructor
		Edge edge2 = new Edge("product", "P", "customer", "A", "purchase", "instore", true, visibility1, startDate, endDate);

		// Check not equal
		assertNotEquals(edge1, edge2);
		assertNotEquals(edge2, edge1);
	}

	@Test
	public void testCompareDirectedAndUndirected() {
		Edge edge1 = new Edge("customer", "A", "product", "P", "purchase", "instore", false, visibility1, startDate, endDate);
		Edge edge2 = new Edge("customer", "A", "product", "P", "purchase", "instore", true, visibility1, startDate, endDate);
		assertTrue(edge1.compareTo(edge2) < 0);
	}

	@Test
	public void testCompare() {
		Edge[] edges = createEdgesForTestingCompare();

		// Add to ordered set
		TreeSet<Edge> set = new TreeSet<Edge>();
		Collections.addAll(set, edges);

		// Check ordering
		Iterator<Edge> it = set.iterator();
		for (int i = 0; i < edges.length; i++) {
			assertEquals(edges[i], it.next());
		}
		assertFalse(it.hasNext());
	}

	@Test
	public void testRawComparator() {
		try {
			Edge[] edges = createEdgesForTestingCompare();

			// Serialise edges
			byte[][] serialisedEdges = new byte[edges.length][];
			for (int i = 0; i < edges.length; i++) {
				serialisedEdges[i] = getBytes(edges[i]);
			}

			// Check order
			Edge.EdgeComparator rawComparator = new Edge.EdgeComparator();
			for (int i = 0; i < serialisedEdges.length - 1; i++) {
				assertTrue(rawComparator.compare(serialisedEdges[i], 0, serialisedEdges[i].length,
						serialisedEdges[i + 1], 0, serialisedEdges[i + 1].length) < 0);
			}
		} catch (IOException e) {
			fail("Exception in testRawComparator() " + e);
		}
	}

	@Test
	public void testRawComparatorIsRegistered() {
		assertEquals(Edge.EdgeComparator.class, WritableComparator.get(Edge.class).getClass());
	}

	private static Edge[] createEdgesForTestingCompare() {
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

		// Create edges
		Edge[] edges = new Edge[22];
		edges[0] = new Edge("typeX", "A", "utypeX", "dvalueX", "typeX", "subtypeX", true, visibility1, date1, date2);
		edges[1] = new Edge("typeX", "B", "utypeX", "dvalueX", "typeX", "subtypeX", false, visibility2, date2, date3);
		edges[2] = new Edge("typeX", "B", "utypeX", "dvalueX", "typeX", "subtypeX", true, visibility1, date1, date2);
		edges[3] = new Edge("typeX", "B", "utypeX", "dvalueX", "typeX", "subtypeX", true, visibility1, date1, date3);
		edges[4] = new Edge("typeX", "B", "utypeX", "dvalueX", "typeX", "subtypeX", true, visibility1, date2, date3);
		edges[5] = new Edge("typeX", "B", "utypeX", "dvalueX", "typeX", "subtypeX", true, visibility2, date2, date3);
		edges[6] = new Edge("typeX", "B", "utypeX", "dvalueX", "typeX", "subtypeY", true, visibility2, date2, date3);
		edges[7] = new Edge("typeX", "B", "utypeX", "dvalueX", "typeY", "subtypeY", true, visibility2, date2, date3);
		edges[8] = new Edge("typeX", "B", "utypeX", "dvalueY", "typeY", "subtypeY", true, visibility2, date2, date3);
		edges[9] = new Edge("typeX", "B", "utypeY", "dvalueY", "typeY", "subtypeY", true, visibility2, date2, date3);
		edges[10] = new Edge("typeX", "C", "utypeY", "dvalueY", "typeY", "subtypeY", true, visibility2, date2, date3);
		edges[11] = new Edge("typeY", "C", "utypeY", "dvalueY", "typeY", "subtypeY", true, visibility2, date2, date3);
		edges[12] = new Edge("typeY2", "C", "utypeY", "dvalueY", "typeY", "subtypeY", true, visibility2, date2, date3);
		edges[13] = new Edge("typeY2", "C2", "utypeY", "dvalueY", "typeY", "subtypeY", true, visibility2, date2, date3);
		edges[14] = new Edge("typeY2", "C2", "utypeY2", "dvalueY", "typeY", "subtypeY", true, visibility2, date2, date3);
		edges[15] = new Edge("typeY2", "C2", "utypeY2", "dvalueY2", "typeY", "subtypeY", true, visibility2, date2, date3);
		edges[16] = new Edge("typeY2", "C2", "utypeY2", "dvalueY2", "typeY2", "subtypeY", true, visibility2, date2, date3);
		edges[17] = new Edge("typeY2", "C2", "utypeY2", "dvalueY2", "typeY2", "subtypeY2", false, visibility2, date2, date3);
		edges[18] = new Edge("typeY2", "C2", "utypeY2", "dvalueY2", "typeY2", "subtypeY2", true, visibility2, date2, date3);
		edges[19] = new Edge("typeY2", "C2", "utypeY2", "dvalueY2", "typeY2", "subtypeY2", true, visibility2 + "X", date2, date3);
		edges[20] = new Edge("typeY2", "C2", "utypeY2", "dvalueY2", "typeY2", "subtypeY2", true, visibility2 + "X", new Date(date2.getTime() + 1L), date3);
		edges[21] = new Edge("typeY2", "C2", "utypeY2", "dvalueY2", "typeY2", "subtypeY2", true, visibility2 + "X", new Date(date2.getTime() + 1L), new Date(date3.getTime() + 1L));
		return edges;
	}

	private static byte[] getBytes(Edge edge) throws IOException {
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		DataOutput out = new DataOutputStream(baos);
		edge.write(out);
		return baos.toByteArray();
	}

	@Test
	public void testClone() {
		// Create an Edge
		Edge edge = new Edge("customer", "A", "product", "P", "purchase", "instore", true, visibility1, startDate, endDate);

		// Clone it
		Edge clonedEdge = edge.clone();

		// Check clone is the same as the original
		assertTrue(edge.equals(clonedEdge));

		// Change original
		edge = new Edge("customer", "B", "product", "Q", "purchase", "instore", true, visibility1, startDate, endDate);

		// Check clone is not equal to the original
		assertNotEquals(clonedEdge, edge);
	}

	@Test
	public void testGetOtherEnd() {
		Edge edge = new Edge("customer", "A", "product", "P", "purchase", "instore", true, visibility1, startDate, endDate);
		assertEquals(new TypeValue("product", "P"), edge.getOtherEnd(new TypeValue("customer", "A")));
		assertEquals(new TypeValue("customer", "A"), edge.getOtherEnd(new TypeValue("product", "P")));
		try {
			edge.getOtherEnd(new TypeValue("NOT", "IN EDGE"));
			fail("IllegalArgumentException should have been thrown");
		} catch (IllegalArgumentException e) { }
	}

	@Test
	public void testEquals() {
		Edge edge1 = new Edge("customer", "A", "product", "P", "purchase", "instore", true, "visibility", new Date(100L), new Date(1000L));
		Edge edge2 = new Edge("customer", "A", "product", "P", "purchase", "instore", true, "visibility", new Date(100L), new Date(1000L));
		assertEquals(edge1, edge2);
		assertEquals(edge1.hashCode(), edge2.hashCode());
		edge2 = new Edge("customer1", "A", "product", "P", "purchase", "instore", true, "visibility", new Date(100L), new Date(1000L));
		assertNotEquals(edge1, edge2);
		edge2 = new Edge("customer", "A2", "product", "P", "purchase", "instore", true, "visibility", new Date(100L), new Date(1000L));
		assertNotEquals(edge1, edge2);
		edge2 = new Edge("customer", "A", "product2", "P", "purchase", "instore", true, "visibility", new Date(100L), new Date(1000L));
		assertNotEquals(edge1, edge2);
		edge2 = new Edge("customer1", "A", "product", "P2", "purchase", "instore", true, "visibility", new Date(100L), new Date(1000L));
		assertNotEquals(edge1, edge2);
		edge2 = new Edge("customer1", "A", "product", "P", "purchase2", "instore", true, "visibility", new Date(100L), new Date(1000L));
		assertNotEquals(edge1, edge2);
		edge2 = new Edge("customer1", "A", "product", "P", "purchase", "instore2", true, "visibility", new Date(100L), new Date(1000L));
		assertNotEquals(edge1, edge2);
		edge2 = new Edge("customer1", "A", "product", "P", "purchase", "instore", false, "visibility", new Date(100L), new Date(1000L));
		assertNotEquals(edge1, edge2);
		edge2 = new Edge("customer1", "A", "product", "P", "purchase", "instore", true, "visibility2", new Date(100L), new Date(1000L));
		assertNotEquals(edge1, edge2);
		edge2 = new Edge("customer1", "A", "product", "P", "purchase", "instore", true, "visibility", new Date(101L), new Date(1000L));
		assertNotEquals(edge1, edge2);
		edge2 = new Edge("customer1", "A", "product", "P", "purchase", "instore", true, "visibility", new Date(100L), new Date(1001L));
		assertNotEquals(edge1, edge2);
	}
}
