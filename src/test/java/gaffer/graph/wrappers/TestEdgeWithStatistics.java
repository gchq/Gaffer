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
import gaffer.graph.Edge;
import gaffer.statistics.SetOfStatistics;
import gaffer.statistics.impl.Count;

import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;

import org.junit.Test;

/**
 * Basic unit test for {@link EdgeWithStatistics}.
 */
public class TestEdgeWithStatistics {

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
		Edge edge1 = new Edge("T1", "V1", "T2", "V2", "X", "Y", true, visibility1, startDate, endDate);
		SetOfStatistics stats1 = new SetOfStatistics();
		stats1.addStatistic("count", new Count(1));
		Edge edge2 = new Edge("T1", "V1", "T2", "V2", "X", "Y", true, visibility1, startDate, endDate);
		SetOfStatistics stats2 = new SetOfStatistics();
		stats2.addStatistic("count", new Count(1));
		EdgeWithStatistics edgeWithStatistics1 = new EdgeWithStatistics(edge1, stats1);
		EdgeWithStatistics edgeWithStatistics2 = new EdgeWithStatistics(edge2, stats2);
		assertEquals(edgeWithStatistics1, edgeWithStatistics2);

		edge2 = new Edge("T1", "V1", "T2", "V2", "X", "Y", false, visibility1, startDate, endDate);
		edgeWithStatistics2 = new EdgeWithStatistics(edge2, stats2);
		assertNotEquals(edgeWithStatistics1, edgeWithStatistics2);
	}

	@Test
	public void testHash() {
		Edge edge1 = new Edge("T1", "V1", "T2", "V2", "X", "Y", true, visibility1, startDate, endDate);
		SetOfStatistics stats1 = new SetOfStatistics();
		stats1.addStatistic("count", new Count(1));
		Edge edge2 = new Edge("T1", "V1", "T2", "V2", "X", "Y", true, visibility1, startDate, endDate);
		SetOfStatistics stats2 = new SetOfStatistics();
		stats2.addStatistic("count", new Count(1));
		EdgeWithStatistics edgeWithStatistics1 = new EdgeWithStatistics(edge1, stats1);
		EdgeWithStatistics edgeWithStatistics2 = new EdgeWithStatistics(edge2, stats2);
		assertEquals(edgeWithStatistics1.hashCode(), edgeWithStatistics2.hashCode());
	}
	
}
