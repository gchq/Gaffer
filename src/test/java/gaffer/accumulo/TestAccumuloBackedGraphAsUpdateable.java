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
package gaffer.accumulo;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import gaffer.GraphAccessException;
import gaffer.graph.Edge;
import gaffer.graph.Entity;
import gaffer.graph.TypeValue;
import gaffer.graph.wrappers.GraphElement;
import gaffer.graph.wrappers.GraphElementWithStatistics;
import gaffer.statistics.SetOfStatistics;
import gaffer.statistics.impl.Count;

import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.HashSet;
import java.util.Set;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.mock.MockInstance;
import org.apache.accumulo.core.security.Authorizations;
import org.junit.Test;

/**
 * Contains unit tests for the updateable functionality of
 * {@link AccumuloBackedGraph}.
 */
public class TestAccumuloBackedGraphAsUpdateable {

	private static Date sevenDaysBefore;
	private static Date sixDaysBefore;
	private static Date fiveDaysBefore;
	private static String visibilityString1 = "public";
	private static String visibilityString2 = "private";

	static {
		Calendar sevenDaysBeforeCalendar = new GregorianCalendar();
		sevenDaysBeforeCalendar.setTime(new Date(System.currentTimeMillis()));
		sevenDaysBeforeCalendar.add(Calendar.DAY_OF_MONTH, -7);
		sevenDaysBefore = sevenDaysBeforeCalendar.getTime();
		sevenDaysBeforeCalendar.add(Calendar.DAY_OF_MONTH, 1);
		sixDaysBefore = sevenDaysBeforeCalendar.getTime();
		sevenDaysBeforeCalendar.add(Calendar.DAY_OF_MONTH, 1);
		fiveDaysBefore = sevenDaysBeforeCalendar.getTime();
		sevenDaysBeforeCalendar.add(Calendar.DAY_OF_MONTH, -45);
	}
	
	@Test
	public void testAddGraphElements() {
		Instance instance = new MockInstance();
		String tableName = "Test";
		long ageOffTimeInMilliseconds = (30 * 24 * 60 * 60 * 1000L); // 30 days in milliseconds

		try {
			// Open connection
			Connector conn = instance.getConnector("user", "password");

			// Create table
			// (this method creates the table, removes the versioning iterator, and adds the SetOfStatisticsCombiner iterator,
			// and sets the age off iterator to age data off after it is more than ageOffTimeInMilliseconds milliseconds old).
			TableUtils.createTable(conn, tableName, ageOffTimeInMilliseconds);

			// Create set of GraphElementWithStatistics
			Set<GraphElement> geSet = new HashSet<GraphElement>();
			
			// Edge 1 and some statistics for it
			Edge edge1 = new Edge("customer", "A", "product", "P", "purchase", "instore", true, visibilityString1, sevenDaysBefore, sixDaysBefore);
			geSet.add(new GraphElement(edge1));

			// Edge 2 and some statistics for it
			Edge edge2 = new Edge("customer", "B", "product", "P", "purchase", "instore", true, visibilityString1, sixDaysBefore, fiveDaysBefore);
			geSet.add(new GraphElement(edge2));

			// Edge 3 and some statistics for it
			Edge edge3 = new Edge("customer", "C", "product", "P", "purchase", "instore", true, visibilityString2, sixDaysBefore, fiveDaysBefore);
			geSet.add(new GraphElement(edge3));

			// Edge 4 and some statistics for it
			Edge edge4 = new Edge("customer", "D", "product", "P", "purchase", "instore", false, visibilityString2, sixDaysBefore, fiveDaysBefore);
			geSet.add(new GraphElement(edge4));

			// Edge 5 and some statistics for it
			Edge edge5 = new Edge("customer", "E", "product", "Q", "purchase", "instore", true, visibilityString2, sixDaysBefore, fiveDaysBefore);
			geSet.add(new GraphElement(edge5));

			// Entity 1 and some statistics for it
			Entity entity1 = new Entity("customer", "A", "purchase", "count", visibilityString1, sevenDaysBefore, sixDaysBefore);
			geSet.add(new GraphElement(entity1));

			// Entity 2 and some statistics for it
			Entity entity2 = new Entity("product", "C", "purchase", "count", visibilityString1, sevenDaysBefore, sixDaysBefore);
			geSet.add(new GraphElement(entity2));

			// Create Accumulo backed graph and add data
			AccumuloBackedGraph graph = new AccumuloBackedGraph(conn, tableName);
			graph.setAuthorizations(new Authorizations(visibilityString1, visibilityString2));
			graph.addGraphElements(geSet);
			
			// Create TypeValues from data we put in
			Set<TypeValue> typeValues = new HashSet<TypeValue>();
			for (GraphElement ge : geSet) {
				if (ge.isEntity()) {
					typeValues.add(new TypeValue(ge.getEntity().getEntityType(), ge.getEntity().getEntityValue()));
				} else {
					typeValues.add(new TypeValue(ge.getEdge().getSourceType(), ge.getEdge().getSourceValue()));
					typeValues.add(new TypeValue(ge.getEdge().getDestinationType(), ge.getEdge().getDestinationValue()));
				}
			}
			
			// Get data out by querying for TypeValues and check it matches what we put in
			Set<GraphElement> results = new HashSet<GraphElement>();
			for (GraphElement ge : graph.getGraphElements(typeValues)) {
				results.add(ge);
			}
			assertEquals(geSet, results);
		} catch (AccumuloException e) {
			fail("Failed to add graph elements with statistics: " + e);
		} catch (AccumuloSecurityException e) {
			fail("Failed to add graph elements with statistics: " + e);
		} catch (TableExistsException e) {
			fail("Failed to add graph elements with statistics: " + e);
		} catch (TableNotFoundException e) {
			fail("Failed to add graph elements with statistics: " + e);
		} catch (GraphAccessException e) {
			fail("Failed to add graph elements with statistics: " + e);
		}
	}
	
	@Test
	public void testAddGraphElementsWithStatistics() {
		Instance instance = new MockInstance();
		String tableName = "Test";
		long ageOffTimeInMilliseconds = (30 * 24 * 60 * 60 * 1000L); // 30 days in milliseconds

		try {
			// Open connection
			Connector conn = instance.getConnector("user", "password");

			// Create table
			// (this method creates the table, removes the versioning iterator, and adds the SetOfStatisticsCombiner iterator,
			// and sets the age off iterator to age data off after it is more than ageOffTimeInMilliseconds milliseconds old).
			TableUtils.createTable(conn, tableName, ageOffTimeInMilliseconds);

			// Create set of GraphElementWithStatistics
			Set<GraphElementWithStatistics> gewsSet = new HashSet<GraphElementWithStatistics>();
			
			// Edge 1 and some statistics for it
			Edge edge1 = new Edge("customer", "A", "product", "P", "purchase", "customer", true, visibilityString1, sevenDaysBefore, sixDaysBefore);
			SetOfStatistics statistics1 = new SetOfStatistics();
			statistics1.addStatistic("count", new Count(1));
			gewsSet.add(new GraphElementWithStatistics(new GraphElement(edge1), statistics1));

			// Edge 2 and some statistics for it
			Edge edge2 = new Edge("customer", "B", "product", "P", "purchase", "customer", true, visibilityString1, sixDaysBefore, fiveDaysBefore);
			SetOfStatistics statistics2 = new SetOfStatistics();
			statistics2.addStatistic("count", new Count(2));
			statistics2.addStatistic("anotherCount", new Count(1000000));
			gewsSet.add(new GraphElementWithStatistics(new GraphElement(edge2), statistics2));

			// Edge 3 and some statistics for it
			Edge edge3 = new Edge("customer", "C", "product", "P", "purchase", "customer", true, visibilityString2, sixDaysBefore, fiveDaysBefore);
			SetOfStatistics statistics3 = new SetOfStatistics();
			statistics3.addStatistic("count", new Count(17));
			gewsSet.add(new GraphElementWithStatistics(new GraphElement(edge3), statistics3));

			// Edge 4 and some statistics for it
			Edge edge4 = new Edge("customer", "D", "product", "P", "purchase", "customer", false, visibilityString2, sixDaysBefore, fiveDaysBefore);
			SetOfStatistics statistics4 = new SetOfStatistics();
			statistics4.addStatistic("countSomething", new Count(123456));
			gewsSet.add(new GraphElementWithStatistics(new GraphElement(edge4), statistics4));

			// Edge 5 and some statistics for it
			Edge edge5 = new Edge("customer", "E", "product", "Q", "purchase", "customer", true, visibilityString2, sixDaysBefore, fiveDaysBefore);
			SetOfStatistics statistics5 = new SetOfStatistics();
			statistics5.addStatistic("count", new Count(99));
			gewsSet.add(new GraphElementWithStatistics(new GraphElement(edge5), statistics5));

			// Entity 1 and some statistics for it
			Entity entity1 = new Entity("customer", "A", "purchase", "count", visibilityString1, sevenDaysBefore, sixDaysBefore);
			SetOfStatistics statisticsEntity1 = new SetOfStatistics();
			statisticsEntity1.addStatistic("entity_count", new Count(1000000));
			gewsSet.add(new GraphElementWithStatistics(new GraphElement(entity1), statisticsEntity1));

			// Entity 2 and some statistics for it
			Entity entity2 = new Entity("product", "C", "purchase", "count", visibilityString1, sevenDaysBefore, sixDaysBefore);
			SetOfStatistics statisticsEntity2 = new SetOfStatistics();
			statisticsEntity2.addStatistic("entity_count", new Count(12345));
			gewsSet.add(new GraphElementWithStatistics(new GraphElement(entity2), statisticsEntity2));

			// Create Accumulo backed graph and add data
			AccumuloBackedGraph graph = new AccumuloBackedGraph(conn, tableName);
			graph.setAuthorizations(new Authorizations(visibilityString1, visibilityString2));
			graph.addGraphElementsWithStatistics(gewsSet);
			
			// Create TypeValues from data we put in
			Set<TypeValue> typeValues = new HashSet<TypeValue>();
			for (GraphElementWithStatistics gews : gewsSet) {
				if (gews.getGraphElement().isEntity()) {
					typeValues.add(new TypeValue(gews.getGraphElement().getEntity().getEntityType(), gews.getGraphElement().getEntity().getEntityValue()));
				} else {
					typeValues.add(new TypeValue(gews.getGraphElement().getEdge().getSourceType(), gews.getGraphElement().getEdge().getSourceValue()));
					typeValues.add(new TypeValue(gews.getGraphElement().getEdge().getDestinationType(), gews.getGraphElement().getEdge().getDestinationValue()));
				}
			}
			
			// Get data out by querying for TypeValues and check it matches what we put in
			Set<GraphElementWithStatistics> results = new HashSet<GraphElementWithStatistics>();
			for (GraphElementWithStatistics gews : graph.getGraphElementsWithStatistics(typeValues)) {
				results.add(gews);
			}
			assertEquals(gewsSet, results);
		} catch (AccumuloException e) {
			fail("Failed to add graph elements with statistics: " + e);
		} catch (AccumuloSecurityException e) {
			fail("Failed to add graph elements with statistics: " + e);
		} catch (TableExistsException e) {
			fail("Failed to add graph elements with statistics: " + e);
		} catch (TableNotFoundException e) {
			fail("Failed to add graph elements with statistics: " + e);
		} catch (GraphAccessException e) {
			fail("Failed to add graph elements with statistics: " + e);
		}
	}

}
