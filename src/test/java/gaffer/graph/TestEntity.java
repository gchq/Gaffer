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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;

import org.apache.hadoop.io.WritableComparator;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Methods to test write/read, compare and clone methods on {@link Entity}.
 * Includes tests for behaviour when null or zero length values are supplied to
 * the constructor.
 */
public class TestEntity {

	private static final String visibility1 = "private";
	private static final String visibility2 = "public";
	private static Date startDate;
	private static Date endDate;
	static {
		// Create start and end dates for entity
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
		// Create entity with null entity type
		try {
			@SuppressWarnings("unused")
			Entity entity = new Entity(null, "A", "purchase", "jinstore", visibility1, startDate, endDate);
			fail("IllegalArgumentException should have been thrown for null entity type");
		} catch (IllegalArgumentException e) { }

		// Create entity with null entity value
		try {
			@SuppressWarnings("unused")
			Entity entity = new Entity("customer", null, "purchase", "jinstore", visibility1, startDate, endDate);
			fail("IllegalArgumentException should have been thrown for null entity value");
		} catch (IllegalArgumentException e) { }

		// Create entity with null summary type
		try {
			@SuppressWarnings("unused")
			Entity entity = new Entity("customer", "A", null, "jinstore", visibility1, startDate, endDate);
			fail("IllegalArgumentException should have been thrown for null summary type");
		} catch (IllegalArgumentException e) { }

		// Create entity with null summary subType
		try {
			@SuppressWarnings("unused")
			Entity entity = new Entity("customer", "A", "purchase", null, visibility1, startDate, endDate);
			fail("IllegalArgumentException should have been thrown for null summary subType");
		} catch (IllegalArgumentException e) { }

		// Create entity with null startDate
		try {
			@SuppressWarnings("unused")
			Entity entity = new Entity("customer", "A", "purchase", "jinstore", visibility1, null, endDate);
			fail("IllegalArgumentException should have been thrown for null startDate");
		} catch (IllegalArgumentException e) { }

		// Create entity with null endDate
		try {
			@SuppressWarnings("unused")
			Entity entity = new Entity("customer", "A", "purchase", "jinstore", visibility1, startDate, null);
			fail("IllegalArgumentException should have been thrown for null endDate");
		} catch (IllegalArgumentException e) { }
	}

	@Test
	public void testConstructorWhenZeroLengthArgumentsSupplied() {
		// Create entity with zero length entity type
		try {
			@SuppressWarnings("unused")
			Entity entity = new Entity("", "A", "purchase", "jinstore", visibility1, startDate, endDate);
			fail("IllegalArgumentException should have been thrown for zero length entity type");
		} catch (IllegalArgumentException e) { }

		// Create entity with zero length entity value
		try {
			@SuppressWarnings("unused")
			Entity entity = new Entity("customer", "", "purchase", "jinstore", visibility1, startDate, endDate);
			fail("IllegalArgumentException should have been thrown for zero length entity value");
		} catch (IllegalArgumentException e) { }

		// Create entity with zero length summary type - this is allowed
		try {
			@SuppressWarnings("unused")
			Entity entity = new Entity("customer", "A", "", "jinstore", visibility1, startDate, endDate);
		} catch (IllegalArgumentException e) {
			fail("IllegalArgumentException was thrown for zero length summary type");
		}

		// Create entity with zero length summary subType - this is allowed
		try {
			@SuppressWarnings("unused")
			Entity entity = new Entity("customer", "A", "purchase", "", visibility1, startDate, endDate);
		} catch (IllegalArgumentException e) {
			fail("IllegalArgumentException was thrown for zero length summary subType");
		}
	}

	@Test
	public void testConstructorWhenStartDateAfterEndDate() {
		try {
			@SuppressWarnings("unused")
			Entity entity = new Entity("customer", "A", "purchase", "online", visibility1, endDate, startDate);
			fail("IllegalArgumentException should have been thrown for startDate before endDate");
		} catch (IllegalArgumentException e) { }
	}

	@Test
	public void testSettersWhenBadArgumentsSupplied() {
		// Create legitimate entity
		Entity entity = new Entity("customer", "A", "purchase", "jinstore", visibility1, startDate, endDate);

		// Set entity type to null
		try {
			entity.setEntityType(null);
			fail("IllegalArgumentException should have been thrown when setting entity type to null");
		} catch (IllegalArgumentException e) { }

		// Set entity type to zero length string
		try {
			entity.setEntityType("");
			fail("IllegalArgumentException should have been thrown when setting entity type to zero length string");
		} catch (IllegalArgumentException e) { }

		// Set entity value to null
		try {
			entity.setEntityValue(null);
			fail("IllegalArgumentException should have been thrown when setting entity value to null");
		} catch (IllegalArgumentException e) { }

		// Set entity value to zero length string
		try {
			entity.setEntityValue("");
			fail("IllegalArgumentException should have been thrown when setting entity value to zero length string");
		} catch (IllegalArgumentException e) { }

		// Set summary type to null
		try {
			entity.setSummaryType(null);
			fail("IllegalArgumentException should have been thrown when setting summary type to null");
		} catch (IllegalArgumentException e) { }

		// Set summary type to zero length string - this is allowed
		try {
			entity.setSummaryType("");
		} catch (IllegalArgumentException e) {
			fail("IllegalArgumentException should not have been thrown when setting summary type to zero length string");
		}

		// Set summary subType to null
		try {
			entity.setSummarySubType(null);
			fail("IllegalArgumentException should have been thrown when setting summary subType to null");
		} catch (IllegalArgumentException e) { }

		// Set summary subType to zero length string - this is allowed
		try {
			entity.setSummarySubType("");
		} catch (IllegalArgumentException e) {
			fail("IllegalArgumentException should not have been thrown when setting summary subType to zero length string");
		}
	}

	@Test
	public void testWriteAndRead() {
		try {
			// Create entity
			Entity entity = new Entity("customer", "A", "purchase", "jinstore", visibility1, startDate, endDate);

			// Write entity
			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			DataOutput out = new DataOutputStream(baos);
			entity.write(out);
			baos.close();

			// Read edge
			DataInput in = new DataInputStream(new ByteArrayInputStream(baos.toByteArray()));
			Entity readEntity = new Entity();
			readEntity.readFields(in);

			// Check get an equal edge back
			assertEquals(entity, readEntity);

		} catch (IOException e) {
			fail("Exception in testWriteAndRead() " + e);
		}
	}

	@Test
	public void testCompareEntities() {
		// Create entities
		Entity[] entities = createEntitiesForTestingCompare();

		// Check order
		for (int i = 0; i < entities.length - 1; i++) {
			assertTrue(entities[i].compareTo(entities[i + 1]) < 0);
		}
	}

	@Test
	public void testRawComparator() {
		try {
			// Create entities
			Entity[] entities = createEntitiesForTestingCompare();

			// Serialise entities
			byte[][] serialisedEntities = new byte[entities.length][];
			for (int i = 0; i < entities.length; i++) {
				serialisedEntities[i] = getBytes(entities[i]);
			}

			// Check order
			Entity.EntityComparator rawComparator = new Entity.EntityComparator();
			for (int i = 0; i < serialisedEntities.length - 1; i++) {
				assertTrue(rawComparator.compare(serialisedEntities[i], 0, serialisedEntities[i].length,
						serialisedEntities[i + 1], 0, serialisedEntities[i + 1].length) < 0);
			}
		} catch (IOException e) {
			fail("Exception in testRawComparator() " + e);
		}
	}

	@Test
	public void testRawComparatorIsRegistered() {
		assertEquals(Entity.EntityComparator.class, WritableComparator.get(Entity.class).getClass());
	}

	private static Entity[] createEntitiesForTestingCompare() {
		Entity[] entities = new Entity[14];
		entities[0] = new Entity("customer", "A", "purchase", "jinstore", visibility1, startDate, endDate);
		entities[1] = new Entity("dustomer", "A", "purchase", "jinstore", visibility1, startDate, endDate);
		entities[2] = new Entity("dustomer", "B", "purchase", "jinstore", visibility1, startDate, endDate);
		entities[3] = new Entity("dustomer", "B", "qurchase", "jinstore", visibility1, startDate, endDate);
		entities[4] = new Entity("dustomer", "B", "qurchase", "tsubinfo", visibility1, startDate, endDate);
		entities[5] = new Entity("dustomer", "B", "qurchase", "tsubinfo", visibility2, startDate, endDate);
		entities[6] = new Entity("dustomer", "B", "qurchase", "tsubinfo", visibility2, endDate, endDate);
		entities[7] = new Entity("dustomer", "B2", "qurchase", "jinstore", visibility1, startDate, endDate);
		entities[8] = new Entity("dustomer", "B2", "qurchasee", "jinstore", visibility1, startDate, endDate);
		entities[9] = new Entity("dustomer", "B2", "qurchasee", "jinstore2", visibility1, startDate, endDate);
		entities[10] = new Entity("dustomerA", "A", "purchase", "jinstore", visibility1, startDate, endDate);
		entities[11] = new Entity("dustomerA", "A", "purchase", "jinstore", visibility1 + "n", startDate, endDate);
		entities[12] = new Entity("dustomerA", "A", "purchase", "jinstore", visibility1 + "n", startDate, new Date(endDate.getTime() + 1L));
		entities[13] = new Entity("e", "A", "purchase", "jinstore", visibility1 + "n", new Date(0L), new Date(1L));
		return entities;
	}

	private static byte[] getBytes(Entity entity) throws IOException {
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		DataOutput out = new DataOutputStream(baos);
		entity.write(out);
		return baos.toByteArray();
	}

	@Test
	public void testClone() {
		Entity entity = new Entity("customer", "A", "purchase", "jinstore", visibility1, startDate, endDate);

		// Clone it
		Entity clonedEntity = entity.clone();

		// Check clone is the same as the original
		assertEquals(entity, clonedEntity);

		// Change original
		entity.setEntityType("newType");

		// Check clone is not equal to the orginal
		assertNotEquals(entity, clonedEntity);
	}
}
