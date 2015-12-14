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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import gaffer.graph.Entity;
import gaffer.statistics.SetOfStatistics;
import gaffer.statistics.impl.Count;

import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;

import org.junit.Test;

/**
 * Basic unit test for {@link EntityWithStatistics}.
 */
public class TestEntityWithStatistics {

	private static final String visibility1 = "private";
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
	public void testEquals() {
		Entity entity1 = new Entity("T1", "V1", "X", "Y", visibility1, startDate, endDate);
		SetOfStatistics stats1 = new SetOfStatistics();
		stats1.addStatistic("count", new Count(1));
		Entity entity2 = new Entity("T1", "V1", "X", "Y", visibility1, startDate, endDate);
		SetOfStatistics stats2 = new SetOfStatistics();
		stats2.addStatistic("count", new Count(1));
		EntityWithStatistics entityWithStatistics1 = new EntityWithStatistics(entity1, stats1);
		EntityWithStatistics entityWithStatistics2 = new EntityWithStatistics(entity2, stats2);
		assertEquals(entityWithStatistics1, entityWithStatistics2);

		entity2 = new Entity("T1", "V1", "X", "Y2", visibility1, startDate, endDate);
		entityWithStatistics2 = new EntityWithStatistics(entity2, stats2);
		assertNotEquals(entityWithStatistics1, entityWithStatistics2);
	}

	@Test
	public void testHash() {
		Entity entity1 = new Entity("T1", "V1", "X", "Y", visibility1, startDate, endDate);
		SetOfStatistics stats1 = new SetOfStatistics();
		stats1.addStatistic("count", new Count(1));
		Entity entity2 = new Entity("T1", "V1", "X", "Y", visibility1, startDate, endDate);
		SetOfStatistics stats2 = new SetOfStatistics();
		stats2.addStatistic("count", new Count(1));
		EntityWithStatistics entityWithStatistics1 = new EntityWithStatistics(entity1, stats1);
		EntityWithStatistics entityWithStatistics2 = new EntityWithStatistics(entity2, stats2);
		assertEquals(entityWithStatistics1.hashCode(), entityWithStatistics2.hashCode());
	}
	
}
