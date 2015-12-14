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
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import gaffer.CloseableIterable;
import gaffer.GraphAccessException;
import gaffer.Pair;
import gaffer.accumulo.retrievers.impl.*;
import gaffer.graph.Edge;
import gaffer.graph.Entity;
import gaffer.graph.TypeValue;
import gaffer.graph.TypeValueRange;
import gaffer.graph.transform.Transform;
import gaffer.graph.wrappers.EdgeWithStatistics;
import gaffer.graph.wrappers.GraphElement;
import gaffer.graph.wrappers.GraphElementWithStatistics;
import gaffer.predicate.summarytype.SummaryTypePredicate;
import gaffer.predicate.summarytype.impl.CombinedPredicates;
import gaffer.predicate.summarytype.impl.RegularExpressionPredicate;
import gaffer.predicate.summarytype.impl.SummaryTypeInSetPredicate;
import gaffer.predicate.time.TimePredicate;
import gaffer.predicate.typevalue.impl.TypeInSetPredicate;
import gaffer.predicate.typevalue.impl.ValueRegularExpressionPredicate;
import gaffer.statistics.SetOfStatistics;
import gaffer.statistics.impl.Count;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.*;
import java.util.regex.Pattern;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.mock.MockInstance;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.io.Text;
import org.junit.Test;

/**
 * Tests the functionality in AccumuloBackedGraph. Tests that the various query methods return the correct results
 * with some of the combinations of views set.
 */
public class TestAccumuloBackedGraphAsQueryable {

	private static Date sevenDaysBefore;
	private static Date sixDaysBefore;
	private static Date fiveDaysBefore;
	private static Date fiftyDaysBefore;
	private static Date thirtyDaysBefore;
	private static String visibilityString1 = "private";
	private static String visibilityString2 = "public";

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
		fiftyDaysBefore = sevenDaysBeforeCalendar.getTime();
		sevenDaysBeforeCalendar.add(Calendar.DAY_OF_MONTH, 20);
		thirtyDaysBefore = sevenDaysBeforeCalendar.getTime();
	}

	@Test
	public void testGetGraphElementsWhenQueryForEmptySet() {
		AccumuloBackedGraph graph = setupGraph(true, null, null);

		Set<TypeValue> empty = new HashSet<TypeValue>();
		Set<GraphElement> results = new HashSet<GraphElement>();
		CloseableIterable<GraphElement> retriever = graph.getGraphElements(empty);
		for (GraphElement element : retriever) {
			results.add(element);
		}
		retriever.close();
		assertEquals(0, results.size());
	}

	@Test
	public void testSetTimeWindowToEverythingAfter() {
		AccumuloBackedGraph graph = setupGraph(true, null, null);
		graph.setTimeWindowToEverythingAfter(sixDaysBefore);

		// Get all elements involving customer A
		Set<GraphElement> results = new HashSet<GraphElement>();
		CloseableIterable<GraphElement> retriever = graph.getGraphElements(new TypeValue("customer", "A"));
		for (GraphElement element : retriever) {
			results.add(element);
		}
		retriever.close();
		Set<GraphElement> expectedResults = new HashSet<GraphElement>();
		Edge expectedResult1 = new Edge("customer", "A", "product", "P", "purchase", "instore", false,
				visibilityString2, sixDaysBefore, fiveDaysBefore);
		expectedResults.add(new GraphElement(expectedResult1));
		Edge expectedResult2 = new Edge("customer", "A", "product", "P", "purchase", "instore", true,
				visibilityString1 + "&" + visibilityString2, sixDaysBefore, fiveDaysBefore);
		expectedResults.add(new GraphElement(expectedResult2));
		assertEquals(expectedResults, results);

		// Get all elements involving customer B
		Set<GraphElement> results2 = new HashSet<GraphElement>();
		retriever = graph.getGraphElements(new TypeValue("customer", "B"));
		for (GraphElement element : retriever) {
			results2.add(element);
		}
		retriever.close();
		Set<GraphElement> expectedResults2 = new HashSet<GraphElement>();
		Edge expectedResult3 = new Edge("customer", "B", "product", "Q", "purchase", "instore", true,
				visibilityString2, sixDaysBefore, fiveDaysBefore);
		expectedResults2.add(new GraphElement(expectedResult3));
		assertEquals(expectedResults2, results2);

		// Get all elements involving product R
		Set<GraphElement> results3 = new HashSet<GraphElement>();
		retriever = graph.getGraphElements(new TypeValue("product", "R"));
		for (GraphElement element : retriever) {
			results3.add(element);
		}
		retriever.close();
		Set<GraphElement> expectedResults3 = new HashSet<GraphElement>();
		assertEquals(expectedResults3, results3);
	}

	@Test
	public void testSetTimeWindowToEverythingBefore() {
		AccumuloBackedGraph graph = setupGraph(true, null, null);
		graph.setTimeWindowToEverythingBefore(sixDaysBefore);

		// Get all elements involving customer A
		Set<GraphElement> results = new HashSet<GraphElement>();
		CloseableIterable<GraphElement> retriever = graph.getGraphElements(new TypeValue("customer", "A"));
		for (GraphElement element : retriever) {
			results.add(element);
		}
		retriever.close();
		Set<GraphElement> expectedResults = new HashSet<GraphElement>();
		Edge expectedResult2 = new Edge("customer", "A", "product", "P", "purchase", "instore", true,
				visibilityString1, sevenDaysBefore, sixDaysBefore);
		expectedResults.add(new GraphElement(expectedResult2));
		Entity entity1 = new Entity("customer", "A", "purchase", "count", visibilityString1, sevenDaysBefore, sixDaysBefore);
		expectedResults.add(new GraphElement(entity1));
		assertEquals(expectedResults, results);

		// Get all elements involving customer B
		Set<GraphElement> results2 = new HashSet<GraphElement>();
		retriever = graph.getGraphElements(new TypeValue("customer", "B"));
		for (GraphElement element : retriever) {
			results2.add(element);
		}
		retriever.close();
		Set<GraphElement> expectedResults2 = new HashSet<GraphElement>();
		assertEquals(expectedResults2, results2);

		// Get all elements involving product R
		Set<GraphElement> results3 = new HashSet<GraphElement>();
		retriever = graph.getGraphElements(new TypeValue("product", "R"));
		for (GraphElement element : retriever) {
			results3.add(element);
		}
		retriever.close();
		Set<GraphElement> expectedResults3 = new HashSet<GraphElement>();
		Entity entity2 = new Entity("product", "R", "purchase", "count", visibilityString1, sevenDaysBefore, sixDaysBefore);
		expectedResults3.add(new GraphElement(entity2));
		assertEquals(expectedResults3, results3);
	}

	@Test
	public void testGetGraphElementsAllTimeAndRollUpOn() {
		AccumuloBackedGraph graph = setupGraph(true, null, null);

		// Get all elements involving customer A
		Set<GraphElement> results = new HashSet<GraphElement>();
		CloseableIterable<GraphElement> retriever = graph.getGraphElements(new TypeValue("customer", "A"));
		for (GraphElement element : retriever) {
			results.add(element);
		}
		retriever.close();
		Set<GraphElement> expectedResults = new HashSet<GraphElement>();
		Edge expectedResult1 = new Edge("customer", "A", "product", "P", "purchase", "instore", false,
				visibilityString2, sixDaysBefore, fiveDaysBefore);
		expectedResults.add(new GraphElement(expectedResult1));
		Edge expectedResult2 = new Edge("customer", "A", "product", "P", "purchase", "instore", true,
				visibilityString1 + "&" + visibilityString2, sevenDaysBefore, fiveDaysBefore);
		expectedResults.add(new GraphElement(expectedResult2));
		Entity entity1 = new Entity("customer", "A", "purchase", "count", visibilityString1, sevenDaysBefore, sixDaysBefore);
		expectedResults.add(new GraphElement(entity1));
		assertEquals(expectedResults, results);

		// Get all elements involving customer B
		Set<GraphElement> results2 = new HashSet<GraphElement>();
		retriever = graph.getGraphElements(new TypeValue("customer", "B"));
		for (GraphElement element : retriever) {
			results2.add(element);
		}
		retriever.close();
		Set<GraphElement> expectedResults2 = new HashSet<GraphElement>();
		Edge expectedResult3 = new Edge("customer", "B", "product", "Q", "purchase", "instore", true,
				visibilityString2, sixDaysBefore, fiveDaysBefore);
		expectedResults2.add(new GraphElement(expectedResult3));
		assertEquals(expectedResults2, results2);

		// Get all elements involving product R
		Set<GraphElement> results3 = new HashSet<GraphElement>();
		retriever = graph.getGraphElements(new TypeValue("product", "R"));
		for (GraphElement element : retriever) {
			results3.add(element);
		}
		retriever.close();
		Set<GraphElement> expectedResults3 = new HashSet<GraphElement>();
		Entity entity2 = new Entity("product", "R", "purchase", "count", visibilityString1, sevenDaysBefore, sixDaysBefore);
		expectedResults3.add(new GraphElement(entity2));
		assertEquals(expectedResults3, results3);

		// Get all elements involving aaA - there shouldn't be any
		for (@SuppressWarnings("unused") GraphElement element : graph.getGraphElements(new TypeValue("customer", "aaA"))) {
			fail("There shouldn't be any results");
		}
	}

	@Test
	public void testGetGraphElementsTimeFilterAndRollUpOn() {
		{
			// Set time window to sixDaysBefore to fiveDaysBefore and test
			AccumuloBackedGraph graph = setupGraph(true, sixDaysBefore, fiveDaysBefore);

			// Get all elements involving customer A
			Set<GraphElement> results = new HashSet<GraphElement>();
			CloseableIterable<GraphElement> retriever = graph.getGraphElements(new TypeValue("customer", "A"));
			for (GraphElement element : retriever) {
				results.add(element);
			}
			retriever.close();
			Set<GraphElement> expectedResults = new HashSet<GraphElement>();
			Edge expectedResult1 = new Edge("customer", "A", "product", "P", "purchase", "instore", false,
					visibilityString2, sixDaysBefore, fiveDaysBefore);
			expectedResults.add(new GraphElement(expectedResult1));
			Edge expectedResult2 = new Edge("customer", "A", "product", "P", "purchase", "instore", true,
					visibilityString1 + "&" + visibilityString2, sixDaysBefore, fiveDaysBefore);
			expectedResults.add(new GraphElement(expectedResult2));
			assertEquals(expectedResults, results);

			// Get all elements involving customer B
			Set<GraphElement> results2 = new HashSet<GraphElement>();
			retriever = graph.getGraphElements(new TypeValue("customer", "B"));
			for (GraphElement element : retriever) {
				results2.add(element);
			}
			retriever.close();
			Set<GraphElement> expectedResults2 = new HashSet<GraphElement>();
			Edge expectedResult3 = new Edge("customer", "B", "product", "Q", "purchase", "instore", true,
					visibilityString2, sixDaysBefore, fiveDaysBefore);
			expectedResults2.add(new GraphElement(expectedResult3));
			assertEquals(expectedResults2, results2);

			// Get all elements involving product R
			Set<GraphElement> results3 = new HashSet<GraphElement>();
			retriever = graph.getGraphElements(new TypeValue("product", "R"));
			for (GraphElement element : retriever) {
				results3.add(element);
			}
			retriever.close();
			Set<GraphElement> expectedResults3 = new HashSet<GraphElement>();
			assertEquals(expectedResults3, results3);

			// Get all elements involving aaA - there shouldn't be any
			for (@SuppressWarnings("unused") GraphElement element : graph.getGraphElements(new TypeValue("customer", "aaA"))) {
				fail("There shouldn't be any results");
			}
		}

		{
			// Set time window to sevenDaysBefore to sixDaysBefore and test
			AccumuloBackedGraph graph = setupGraph(true, sevenDaysBefore, sixDaysBefore);

			// Get all elements involving customer A
			Set<GraphElement> results = new HashSet<GraphElement>();
			CloseableIterable<GraphElement> retriever = graph.getGraphElements(new TypeValue("customer", "A"));
			for (GraphElement element : retriever) {
				results.add(element);
			}
			retriever.close();
			Set<GraphElement> expectedResults = new HashSet<GraphElement>();
			Edge expectedResult1 = new Edge("customer", "A", "product", "P", "purchase", "instore", true,
					visibilityString1, sevenDaysBefore, sixDaysBefore);
			expectedResults.add(new GraphElement(expectedResult1));
			Entity entity1 = new Entity("customer", "A", "purchase", "count", visibilityString1, sevenDaysBefore, sixDaysBefore);
			expectedResults.add(new GraphElement(entity1));
			assertEquals(expectedResults, results);

			// Get all elements involving customer B
			Set<GraphElement> results2 = new HashSet<GraphElement>();
			retriever = graph.getGraphElements(new TypeValue("customer", "B"));
			for (GraphElement element : retriever) {
				results2.add(element);
			}
			retriever.close();
			Set<GraphElement> expectedResults2 = new HashSet<GraphElement>();
			assertEquals(expectedResults2, results2);

			// Get all elements involving product R
			Set<GraphElement> results3 = new HashSet<GraphElement>();
			retriever = graph.getGraphElements(new TypeValue("product", "R"));
			for (GraphElement element : retriever) {
				results3.add(element);
			}
			retriever.close();
			Set<GraphElement> expectedResults3 = new HashSet<GraphElement>();
			Entity entity2 = new Entity("product", "R", "purchase", "count", visibilityString1, sevenDaysBefore, sixDaysBefore);
			expectedResults3.add(new GraphElement(entity2));
			assertEquals(expectedResults3, results3);

			// Get all elements involving aaA - there shouldn't be any
			for (@SuppressWarnings("unused") GraphElement element : graph.getGraphElements(new TypeValue("customer", "aaA"))) {
				fail("There shouldn't be any results");
			}
		}

		{
			// Set time window to fiftyDaysBefore to thirtyDaysBefore and test that there are no results
			AccumuloBackedGraph graph = setupGraph(true, fiftyDaysBefore, thirtyDaysBefore);

			// Get all elements involving customer A
			for (@SuppressWarnings("unused") GraphElement element : graph.getGraphElements(new TypeValue("customer", "A"))) {
				fail("There shouldn't be any results");
			}

			// Get all elements involving customer B
			for (@SuppressWarnings("unused") GraphElement element : graph.getGraphElements(new TypeValue("customer", "B"))) {
				fail("There shouldn't be any results");
			}

			// Get all elements involving product R
			for (@SuppressWarnings("unused") GraphElement element : graph.getGraphElements(new TypeValue("product", "R"))) {
				fail("There shouldn't be any results");
			}
		}
	}

	@Test
	public void testGetGraphElementsAllTimeAndRollUpOff() {
		AccumuloBackedGraph graph = setupGraph(false, null, null);

		// Get all elements involving customer A
		Set<GraphElement> results = new HashSet<GraphElement>();
		CloseableIterable<GraphElement> retriever = graph.getGraphElements(new TypeValue("customer", "A"));
		for (GraphElement element : retriever) {
			results.add(element);
		}
		retriever.close();
		Set<GraphElement> expectedResults = new HashSet<GraphElement>();
		Edge expectedResult1 = new Edge("customer", "A", "product", "P", "purchase", "instore", true,
				visibilityString1, sevenDaysBefore, sixDaysBefore);
		expectedResults.add(new GraphElement(expectedResult1));
		Edge expectedResult2 = new Edge("customer", "A", "product", "P", "purchase", "instore", true,
				visibilityString1, sixDaysBefore, fiveDaysBefore);
		expectedResults.add(new GraphElement(expectedResult2));
		Edge expectedResult3 = new Edge("customer", "A", "product", "P", "purchase", "instore", true,
				visibilityString2, sixDaysBefore, fiveDaysBefore);
		expectedResults.add(new GraphElement(expectedResult3));
		Edge expectedResult4 = new Edge("customer", "A", "product", "P", "purchase", "instore", false,
				visibilityString2, sixDaysBefore, fiveDaysBefore);
		expectedResults.add(new GraphElement(expectedResult4));
		Entity entity1 = new Entity("customer", "A", "purchase", "count", visibilityString1, sevenDaysBefore, sixDaysBefore);
		expectedResults.add(new GraphElement(entity1));
		assertEquals(expectedResults, results);

		// Get all elements involving customer B
		Set<GraphElement> results2 = new HashSet<GraphElement>();
		retriever = graph.getGraphElements(new TypeValue("customer", "B"));
		for (GraphElement element : retriever) {
			results2.add(element);
		}
		retriever.close();
		Set<GraphElement> expectedResults2 = new HashSet<GraphElement>();
		Edge expectedResult5 = new Edge("customer", "B", "product", "Q", "purchase", "instore", true,
				visibilityString2, sixDaysBefore, fiveDaysBefore);
		expectedResults2.add(new GraphElement(expectedResult5));
		assertEquals(expectedResults2, results2);

		// Get all elements involving product R
		Set<GraphElement> results3 = new HashSet<GraphElement>();
		retriever = graph.getGraphElements(new TypeValue("product", "R"));
		for (GraphElement element : retriever) {
			results3.add(element);
		}
		retriever.close();
		Set<GraphElement> expectedResults3 = new HashSet<GraphElement>();
		Entity entity2 = new Entity("product", "R", "purchase", "count", visibilityString1, sevenDaysBefore, sixDaysBefore);
		expectedResults3.add(new GraphElement(entity2));
		assertEquals(expectedResults3, results3);

		// Get all elements involving aaA - there shouldn't be any
		for (@SuppressWarnings("unused") GraphElement element : graph.getGraphElements(new TypeValue("customer", "aaA"))) {
			fail("There shouldn't be any results");
		}
	}

	@Test
	public void testGetGraphElementsTimeFilterAndRollUpOff() {
		{
			// Set time window to sixDaysBefore to fiveDaysBefore and test
			AccumuloBackedGraph graph = setupGraph(false, sixDaysBefore, fiveDaysBefore);

			// Get all elements involving customer A
			Set<GraphElement> results = new HashSet<GraphElement>();
			CloseableIterable<GraphElement> retriever = graph.getGraphElements(new TypeValue("customer", "A"));
			for (GraphElement element : retriever) {
				results.add(element);
			}
			retriever.close();
			Set<GraphElement> expectedResults = new HashSet<GraphElement>();
			Edge expectedResult1 = new Edge("customer", "A", "product", "P", "purchase", "instore", true,
					visibilityString1, sixDaysBefore, fiveDaysBefore);
			expectedResults.add(new GraphElement(expectedResult1));
			Edge expectedResult2 = new Edge("customer", "A", "product", "P", "purchase", "instore", true,
					visibilityString2, sixDaysBefore, fiveDaysBefore);
			expectedResults.add(new GraphElement(expectedResult2));
			Edge expectedResult3 = new Edge("customer", "A", "product", "P", "purchase", "instore", false,
					visibilityString2, sixDaysBefore, fiveDaysBefore);
			expectedResults.add(new GraphElement(expectedResult3));
			assertEquals(expectedResults, results);

			// Get all elements involving customer B
			Set<GraphElement> results2 = new HashSet<GraphElement>();
			retriever = graph.getGraphElements(new TypeValue("customer", "B"));
			for (GraphElement element : retriever) {
				results2.add(element);
			}
			retriever.close();
			Set<GraphElement> expectedResults2 = new HashSet<GraphElement>();
			Edge expectedResult4 = new Edge("customer", "B", "product", "Q", "purchase", "instore", true,
					visibilityString2, sixDaysBefore, fiveDaysBefore);
			expectedResults2.add(new GraphElement(expectedResult4));
			assertEquals(expectedResults2, results2);

			// Get all elements involving product R
			for (@SuppressWarnings("unused") GraphElement element : graph.getGraphElements(new TypeValue("product", "R"))) {
				fail("There shouldn't be any results");
			}

			// Get all elements involving aaA - there shouldn't be any
			for (@SuppressWarnings("unused") GraphElement element : graph.getGraphElements(new TypeValue("customer", "aaA"))) {
				fail("There shouldn't be any results");
			}
		}

		{
			// Set time window to sevenDaysBefore to sixDaysBefore and test
			AccumuloBackedGraph graph = setupGraph(true, sevenDaysBefore, sixDaysBefore);

			// Get all elements involving customer A
			Set<GraphElement> results = new HashSet<GraphElement>();
			CloseableIterable<GraphElement> retriever = graph.getGraphElements(new TypeValue("customer", "A"));
			for (GraphElement element : retriever) {
				results.add(element);
			}
			retriever.close();
			Set<GraphElement> expectedResults = new HashSet<GraphElement>();
			Edge expectedResult1 = new Edge("customer", "A", "product", "P", "purchase", "instore", true,
					visibilityString1, sevenDaysBefore, sixDaysBefore);
			expectedResults.add(new GraphElement(expectedResult1));
			Entity entity1 = new Entity("customer", "A", "purchase", "count", visibilityString1, sevenDaysBefore, sixDaysBefore);
			expectedResults.add(new GraphElement(entity1));
			assertEquals(expectedResults, results);

			// Get all elements involving customer B
			for (@SuppressWarnings("unused") GraphElement element : graph.getGraphElements(new TypeValue("customer", "B"))) {
				fail("There shouldn't be any results");
			}

			// Get all elements involving product R
			Set<GraphElement> results3 = new HashSet<GraphElement>();
			retriever = graph.getGraphElements(new TypeValue("product", "R"));
			for (GraphElement element : retriever) {
				results3.add(element);
			}
			retriever.close();
			Set<GraphElement> expectedResults3 = new HashSet<GraphElement>();
			Entity entity2 = new Entity("product", "R", "purchase", "count", visibilityString1, sevenDaysBefore, sixDaysBefore);
			expectedResults3.add(new GraphElement(entity2));
			assertEquals(expectedResults3, results3);

			// Get all elements involving aaA - there shouldn't be any
			for (@SuppressWarnings("unused") GraphElement element : graph.getGraphElements(new TypeValue("customer", "aaA"))) {
				fail("There shouldn't be any results");
			}
		}

		{
			// Set time window to fiftyDaysBefore to thirtyDaysBefore and test that there are no results
			AccumuloBackedGraph graph = setupGraph(true, fiftyDaysBefore, thirtyDaysBefore);

			// Get all elements involving customer A
			for (@SuppressWarnings("unused") GraphElement element : graph.getGraphElements(new TypeValue("customer", "A"))) {
				fail("There shouldn't be any results");
			}

			// Get all elements involving customer B
			for (@SuppressWarnings("unused") GraphElement element : graph.getGraphElements(new TypeValue("customer", "B"))) {
				fail("There shouldn't be any results");
			}

			// Get all elements involving product R
			for (@SuppressWarnings("unused") GraphElement element : graph.getGraphElements(new TypeValue("product", "R"))) {
				fail("There shouldn't be any results");
			}
		}
	}

	@Test
	public void testGetGraphElementsSummaryTypePredicateFilter() {
		AccumuloBackedGraph graph = setupGraphForTypeTest(true, null, null);

		// Create set of all TypeValues found in the graph
		Set<TypeValue> typeValues = new HashSet<TypeValue>();
		typeValues.add(new TypeValue("customer", "A"));
		typeValues.add(new TypeValue("product", "P"));
		typeValues.add(new TypeValue("customer", "B"));
		typeValues.add(new TypeValue("product", "Q"));
		typeValues.add(new TypeValue("product", "R"));

		// Get all elements of type purchase3
		Set<String> types = new HashSet<String>();
		types.add("purchase3");
		SummaryTypePredicate predicate = new SummaryTypeInSetPredicate(types);
		graph.setSummaryTypePredicate(predicate);
		Set<GraphElement> results = new HashSet<GraphElement>();
		CloseableIterable<GraphElement> retriever = graph.getGraphElements(typeValues);
		for (GraphElement element : retriever) {
			results.add(element);
		}
		retriever.close();
		Set<GraphElement> expectedResults1 = new HashSet<GraphElement>();
		Edge expectedResult1 = new Edge("customer", "A", "product", "P", "purchase3", "customer3", true, visibilityString2, sixDaysBefore, fiveDaysBefore);
		expectedResults1.add(new GraphElement(expectedResult1));
		assertEquals(expectedResults1, results);

		// Reset to get all types
		graph.setReturnAllSummaryTypesAndSubTypes();
		results = new HashSet<GraphElement>();
		retriever = graph.getGraphElements(typeValues);
		for (GraphElement element : retriever) {
			results.add(element);
		}
		retriever.close();
		assertTrue(results.size() > 1);
	}

	@Test
	public void testGetGraphElementsSummaryTypePredicateCombinedFilter() {
		AccumuloBackedGraph graph = setupGraphForTypeTest(true, null, null);

		// Create set of all TypeValues found in the graph
		Set<TypeValue> typeValues = new HashSet<TypeValue>();
		typeValues.add(new TypeValue("customer", "A"));
		typeValues.add(new TypeValue("product", "P"));
		typeValues.add(new TypeValue("customer", "B"));
		typeValues.add(new TypeValue("product", "Q"));
		typeValues.add(new TypeValue("product", "R"));

		// Get all elements of type purchase3 and where type matches anything and subtype matches "cust"
		SummaryTypePredicate predicate1 = new SummaryTypeInSetPredicate(Collections.singleton("purchase3"));
		SummaryTypePredicate predicate2 = new RegularExpressionPredicate(Pattern.compile(""), Pattern.compile("cust"));
		SummaryTypePredicate combinedPredicates = new CombinedPredicates(predicate1, predicate2, CombinedPredicates.Combine.AND);
		graph.setSummaryTypePredicate(combinedPredicates);
		Set<GraphElement> results = new HashSet<GraphElement>();
		CloseableIterable<GraphElement> retriever = graph.getGraphElements(typeValues);
		for (GraphElement element : retriever) {
			results.add(element);
		}
		retriever.close();
		Set<GraphElement> expectedResults1 = new HashSet<GraphElement>();
		Edge expectedResult1 = new Edge("customer", "A", "product", "P", "purchase3", "customer3", true, visibilityString2, sixDaysBefore, fiveDaysBefore);
		expectedResults1.add(new GraphElement(expectedResult1));
		assertEquals(expectedResults1, results);

		// Reset to get all types
		graph.setReturnAllSummaryTypesAndSubTypes();
		results = new HashSet<GraphElement>();
		retriever = graph.getGraphElements(typeValues);
		for (GraphElement element : retriever) {
			results.add(element);
		}
		retriever.close();
		assertTrue(results.size() > 1);
	}

	@Test
	public void testGetGraphElementsTypeFilter() {
		AccumuloBackedGraph graph = setupGraphForTypeTest(true, null, null);

		// Create set of all TypeValues found in the graph
		Set<TypeValue> typeValues = new HashSet<TypeValue>();
		typeValues.add(new TypeValue("customer", "A"));
		typeValues.add(new TypeValue("product", "P"));
		typeValues.add(new TypeValue("customer", "B"));
		typeValues.add(new TypeValue("product", "Q"));
		typeValues.add(new TypeValue("product", "R"));

		// Get all elements of type purchase3
		Set<String> types = new HashSet<String>();
		types.add("purchase3");
		graph.setSummaryTypes(types);
		Set<GraphElement> results = new HashSet<GraphElement>();
		CloseableIterable<GraphElement> retriever = graph.getGraphElements(typeValues);
		for (GraphElement element : retriever) {
			results.add(element);
		}
		retriever.close();
		Set<GraphElement> expectedResults1 = new HashSet<GraphElement>();
		Edge expectedResult1 = new Edge("customer", "A", "product", "P", "purchase3", "customer3", true, visibilityString2, sixDaysBefore, fiveDaysBefore);
		expectedResults1.add(new GraphElement(expectedResult1));
		assertEquals(expectedResults1, results);

		// Reset to get all types
		graph.setReturnAllSummaryTypesAndSubTypes();
		results = new HashSet<GraphElement>();
		retriever = graph.getGraphElements(typeValues);
		for (GraphElement element : retriever) {
			results.add(element);
		}
		retriever.close();
		assertTrue(results.size() > 1);
	}

	@Test
	public void testGetGraphElementsTypeSubTypeFilter() {
		AccumuloBackedGraph graph = setupGraphForTypeTest(true, null, null);

		// Create set of all TypeValues found in the graph
		Set<TypeValue> typeValues = new HashSet<TypeValue>();
		typeValues.add(new TypeValue("customer", "A"));
		typeValues.add(new TypeValue("product", "P"));
		typeValues.add(new TypeValue("customer", "B"));
		typeValues.add(new TypeValue("product", "Q"));
		typeValues.add(new TypeValue("product", "R"));

		// Get all elements of type purchase1 and subtype count1
		Set<Pair<String>> typeSubTypes = new HashSet<Pair<String>>();
		typeSubTypes.add(new Pair<String>("purchase1", "count1"));
		graph.setSummaryTypesAndSubTypes(typeSubTypes);
		Set<GraphElement> results = new HashSet<GraphElement>();
		CloseableIterable<GraphElement> retriever = graph.getGraphElements(typeValues);
		for (GraphElement element : retriever) {
			results.add(element);
		}
		retriever.close();
		Set<GraphElement> expectedResults1 = new HashSet<GraphElement>();
		Entity expectedResult1 = new Entity("customer", "A", "purchase1", "count1", visibilityString1, sevenDaysBefore, sixDaysBefore);
		expectedResults1.add(new GraphElement(expectedResult1));
		assertEquals(expectedResults1, results);

		// Reset to get all types
		graph.setReturnAllSummaryTypesAndSubTypes();
		results = new HashSet<GraphElement>();
		retriever = graph.getGraphElements(typeValues);
		for (GraphElement element : retriever) {
			results.add(element);
		}
		retriever.close();
		assertTrue(results.size() > 1);
	}

	@Test
	public void testGetGraphElementsWithStatisticsWhenQueryForEmptySet() {
		AccumuloBackedGraph graph = setupGraph(true, null, null);

		Set<TypeValue> empty = new HashSet<TypeValue>();
		Set<GraphElementWithStatistics> results = new HashSet<GraphElementWithStatistics>();
		CloseableIterable<GraphElementWithStatistics> retriever = graph.getGraphElementsWithStatistics(empty);
		for (GraphElementWithStatistics elementStats : retriever) {
			results.add(elementStats);
		}
		retriever.close();
		assertEquals(0, results.size());
	}

	@Test
	public void testGetGraphElementsWithStatisticsAllTimeAndRollUpOn() {
		AccumuloBackedGraph graph = setupGraph(true, null, null);

		// Get all elements involving customer A
		Set<GraphElementWithStatistics> results = new HashSet<GraphElementWithStatistics>();
		CloseableIterable<GraphElementWithStatistics> retriever = graph.getGraphElementsWithStatistics(new TypeValue("customer", "A"));
		for (GraphElementWithStatistics elementStats : retriever) {
			results.add(elementStats);
		}
		retriever.close();
		Set<GraphElementWithStatistics> expectedResults1 = new HashSet<GraphElementWithStatistics>();
		Edge expectedResult1 = new Edge("customer", "A", "product", "P", "purchase", "instore", false,
				visibilityString2, sixDaysBefore, fiveDaysBefore);
		SetOfStatistics expectedSetOfStatistics1 = new SetOfStatistics();
		expectedSetOfStatistics1.addStatistic("countSomething", new Count(123456));
		expectedResults1.add(new GraphElementWithStatistics(new GraphElement(expectedResult1), expectedSetOfStatistics1));
		Edge expectedResult2 = new Edge("customer", "A", "product", "P", "purchase", "instore", true,
				visibilityString1 + "&" + visibilityString2, sevenDaysBefore, fiveDaysBefore);
		SetOfStatistics expectedSetOfStatistics2 = new SetOfStatistics();
		expectedSetOfStatistics2.addStatistic("count", new Count(20));
		expectedSetOfStatistics2.addStatistic("anotherCount", new Count(1000000));
		expectedResults1.add(new GraphElementWithStatistics(new GraphElement(expectedResult2), expectedSetOfStatistics2));
		Entity entity1 = new Entity("customer", "A", "purchase", "count", visibilityString1, sevenDaysBefore, sixDaysBefore);
		SetOfStatistics statisticsEntity1 = new SetOfStatistics();
		statisticsEntity1.addStatistic("entity_count", new Count(1000000));
		expectedResults1.add(new GraphElementWithStatistics(new GraphElement(entity1), statisticsEntity1));
		assertEquals(expectedResults1, results);

		// Get all elements involving customer B
		Set<GraphElementWithStatistics> results2 = new HashSet<GraphElementWithStatistics>();
		retriever = graph.getGraphElementsWithStatistics(new TypeValue("customer", "B"));
		for (GraphElementWithStatistics elementStats : retriever) {
			results2.add(elementStats);
		}
		retriever.close();
		Set<GraphElementWithStatistics> expectedResults2 = new HashSet<GraphElementWithStatistics>();
		Edge expectedResult3 = new Edge("customer", "B", "product", "Q", "purchase", "instore", true,
				visibilityString2, sixDaysBefore, fiveDaysBefore);
		SetOfStatistics expectedSetOfStatistics3 = new SetOfStatistics();
		expectedSetOfStatistics3.addStatistic("count", new Count(99));
		expectedResults2.add(new GraphElementWithStatistics(new GraphElement(expectedResult3), expectedSetOfStatistics3));
		assertEquals(expectedResults2, results2);

		// Get all elements involving product R
		Set<GraphElementWithStatistics> results3 = new HashSet<GraphElementWithStatistics>();
		retriever = graph.getGraphElementsWithStatistics(new TypeValue("product", "R"));
		for (GraphElementWithStatistics elementStats : retriever) {
			results3.add(elementStats);
		}
		retriever.close();
		Set<GraphElementWithStatistics> expectedResults3 = new HashSet<GraphElementWithStatistics>();
		Entity entity2 = new Entity("product", "R", "purchase", "count", visibilityString1, sevenDaysBefore, sixDaysBefore);
		SetOfStatistics statisticsEntity2 = new SetOfStatistics();
		statisticsEntity2.addStatistic("entity_count", new Count(12345));
		expectedResults3.add(new GraphElementWithStatistics(new GraphElement(entity2), statisticsEntity2));
		assertEquals(expectedResults3, results3);

		// Get all graph elements involving aaA - there shouldn't be any
		for (@SuppressWarnings("unused") GraphElementWithStatistics elementWithStats : graph.getGraphElementsWithStatistics(new TypeValue("customer", "aaA"))) {
			fail("There shouldn't be any results");
		}
	}

	@Test
	public void testGetGraphElementsWithStatisticsAllTimeAndRollUpOnRemovedStatistics() {
		AccumuloBackedGraph graph = setupGraph(true, null, null);

		// Get all elements involving customer A
		Set<GraphElementWithStatistics> results = new HashSet<GraphElementWithStatistics>();
		graph.setStatisticsToRemoveByName(Collections.singleton("count"));
		CloseableIterable<GraphElementWithStatistics> retriever = graph.getGraphElementsWithStatistics(new TypeValue("customer", "A"));
		for (GraphElementWithStatistics elementStats : retriever) {
			results.add(elementStats);
		}
		retriever.close();
		Set<GraphElementWithStatistics> expectedResults1 = new HashSet<GraphElementWithStatistics>();
		Edge expectedResult1 = new Edge("customer", "A", "product", "P", "purchase", "instore", false,
				visibilityString2, sixDaysBefore, fiveDaysBefore);
		SetOfStatistics expectedSetOfStatistics1 = new SetOfStatistics();
		expectedSetOfStatistics1.addStatistic("countSomething", new Count(123456));
		expectedResults1.add(new GraphElementWithStatistics(new GraphElement(expectedResult1), expectedSetOfStatistics1));
		Edge expectedResult2 = new Edge("customer", "A", "product", "P", "purchase", "instore", true,
				visibilityString1 + "&" + visibilityString2, sevenDaysBefore, fiveDaysBefore);
		SetOfStatistics expectedSetOfStatistics2 = new SetOfStatistics();
		expectedSetOfStatistics2.addStatistic("anotherCount", new Count(1000000));
		expectedResults1.add(new GraphElementWithStatistics(new GraphElement(expectedResult2), expectedSetOfStatistics2));
		Entity entity1 = new Entity("customer", "A", "purchase", "count", visibilityString1, sevenDaysBefore, sixDaysBefore);
		SetOfStatistics statisticsEntity1 = new SetOfStatistics();
		statisticsEntity1.addStatistic("entity_count", new Count(1000000));
		expectedResults1.add(new GraphElementWithStatistics(new GraphElement(entity1), statisticsEntity1));
		assertEquals(expectedResults1, results);

		// Get all elements involving customer B
		Set<GraphElementWithStatistics> results2 = new HashSet<GraphElementWithStatistics>();
		retriever = graph.getGraphElementsWithStatistics(new TypeValue("customer", "B"));
		for (GraphElementWithStatistics elementStats : retriever) {
			results2.add(elementStats);
		}
		retriever.close();
		Set<GraphElementWithStatistics> expectedResults2 = new HashSet<GraphElementWithStatistics>();
		Edge expectedResult3 = new Edge("customer", "B", "product", "Q", "purchase", "instore", true,
				visibilityString2, sixDaysBefore, fiveDaysBefore);
		SetOfStatistics expectedSetOfStatistics3 = new SetOfStatistics();
		expectedResults2.add(new GraphElementWithStatistics(new GraphElement(expectedResult3), expectedSetOfStatistics3));
		assertEquals(expectedResults2, results2);

		// Get all elements involving product R
		Set<GraphElementWithStatistics> results3 = new HashSet<GraphElementWithStatistics>();
		retriever = graph.getGraphElementsWithStatistics(new TypeValue("product", "R"));
		for (GraphElementWithStatistics elementStats : retriever) {
			results3.add(elementStats);
		}
		retriever.close();
		Set<GraphElementWithStatistics> expectedResults3 = new HashSet<GraphElementWithStatistics>();
		Entity entity2 = new Entity("product", "R", "purchase", "count", visibilityString1, sevenDaysBefore, sixDaysBefore);
		SetOfStatistics statisticsEntity2 = new SetOfStatistics();
		statisticsEntity2.addStatistic("entity_count", new Count(12345));
		expectedResults3.add(new GraphElementWithStatistics(new GraphElement(entity2), statisticsEntity2));
		assertEquals(expectedResults3, results3);

		// Get all graph elements involving aaA - there shouldn't be any
		for (@SuppressWarnings("unused") GraphElementWithStatistics elementWithStats : graph.getGraphElementsWithStatistics(new TypeValue("customer", "aaA"))) {
			fail("There shouldn't be any results");
		}
	}

	@Test
	public void testGetGraphElementsWithStatisticsAllTimeAndRollUpOnKeepStatistics() {
		AccumuloBackedGraph graph = setupGraph(true, null, null);

		// Get all elements involving customer A
		Set<GraphElementWithStatistics> results = new HashSet<GraphElementWithStatistics>();
		graph.setStatisticsToKeepByName(Collections.singleton("count"));
		CloseableIterable<GraphElementWithStatistics> retriever = graph.getGraphElementsWithStatistics(new TypeValue("customer", "A"));
		for (GraphElementWithStatistics elementStats : retriever) {
			results.add(elementStats);
		}
		retriever.close();
		Set<GraphElementWithStatistics> expectedResults1 = new HashSet<GraphElementWithStatistics>();
		Edge expectedResult1 = new Edge("customer", "A", "product", "P", "purchase", "instore", false,
				visibilityString2, sixDaysBefore, fiveDaysBefore);
		SetOfStatistics expectedSetOfStatistics1 = new SetOfStatistics();
		expectedResults1.add(new GraphElementWithStatistics(new GraphElement(expectedResult1), expectedSetOfStatistics1));
		Edge expectedResult2 = new Edge("customer", "A", "product", "P", "purchase", "instore", true,
				visibilityString1 + "&" + visibilityString2, sevenDaysBefore, fiveDaysBefore);
		SetOfStatistics expectedSetOfStatistics2 = new SetOfStatistics();
		expectedSetOfStatistics2.addStatistic("count", new Count(20));
		expectedResults1.add(new GraphElementWithStatistics(new GraphElement(expectedResult2), expectedSetOfStatistics2));
		Entity entity1 = new Entity("customer", "A", "purchase", "count", visibilityString1, sevenDaysBefore, sixDaysBefore);
		SetOfStatistics statisticsEntity1 = new SetOfStatistics();
		expectedResults1.add(new GraphElementWithStatistics(new GraphElement(entity1), statisticsEntity1));
		assertEquals(expectedResults1, results);

		// Get all elements involving customer B
		Set<GraphElementWithStatistics> results2 = new HashSet<GraphElementWithStatistics>();
		retriever = graph.getGraphElementsWithStatistics(new TypeValue("customer", "B"));
		for (GraphElementWithStatistics elementStats : retriever) {
			results2.add(elementStats);
		}
		retriever.close();
		Set<GraphElementWithStatistics> expectedResults2 = new HashSet<GraphElementWithStatistics>();
		Edge expectedResult3 = new Edge("customer", "B", "product", "Q", "purchase", "instore", true,
				visibilityString2, sixDaysBefore, fiveDaysBefore);
		SetOfStatistics expectedSetOfStatistics3 = new SetOfStatistics();
		expectedSetOfStatistics3.addStatistic("count", new Count(99));
		expectedResults2.add(new GraphElementWithStatistics(new GraphElement(expectedResult3), expectedSetOfStatistics3));
		assertEquals(expectedResults2, results2);

		// Get all elements involving product R
		Set<GraphElementWithStatistics> results3 = new HashSet<GraphElementWithStatistics>();
		retriever = graph.getGraphElementsWithStatistics(new TypeValue("product", "R"));
		for (GraphElementWithStatistics elementStats : retriever) {
			results3.add(elementStats);
		}
		retriever.close();
		Set<GraphElementWithStatistics> expectedResults3 = new HashSet<GraphElementWithStatistics>();
		Entity entity2 = new Entity("product", "R", "purchase", "count", visibilityString1, sevenDaysBefore, sixDaysBefore);
		SetOfStatistics statisticsEntity2 = new SetOfStatistics();
		expectedResults3.add(new GraphElementWithStatistics(new GraphElement(entity2), statisticsEntity2));
		assertEquals(expectedResults3, results3);

		// Get all graph elements involving aaA - there shouldn't be any
		for (@SuppressWarnings("unused") GraphElementWithStatistics elementWithStats : graph.getGraphElementsWithStatistics(new TypeValue("customer", "aaA"))) {
			fail("There shouldn't be any results");
		}
	}

	@Test
	public void testGetEntitiesWithStatisticsAllTimeAndRollUpOn() {
		AccumuloBackedGraph graph = setupGraph(true, null, null);

		// Get all entities involving customer A
		Set<GraphElementWithStatistics> results = new HashSet<GraphElementWithStatistics>();
		graph.setReturnEntitiesOnly();
		CloseableIterable<GraphElementWithStatistics> retriever = graph.getGraphElementsWithStatistics(new TypeValue("customer", "A"));
		for (GraphElementWithStatistics entityStats : retriever) {
			results.add(entityStats);
		}
		retriever.close();
		Set<GraphElementWithStatistics> expectedResults1 = new HashSet<GraphElementWithStatistics>();
		Entity entity1 = new Entity("customer", "A", "purchase", "count", visibilityString1, sevenDaysBefore, sixDaysBefore);
		SetOfStatistics statisticsEntity1 = new SetOfStatistics();
		statisticsEntity1.addStatistic("entity_count", new Count(1000000));
		expectedResults1.add(new GraphElementWithStatistics(new GraphElement(entity1), statisticsEntity1));
		assertEquals(expectedResults1, results);

		// Get all entities involving product R
		Set<GraphElementWithStatistics> results2 = new HashSet<GraphElementWithStatistics>();
		retriever = graph.getGraphElementsWithStatistics(new TypeValue("product", "R"));
		for (GraphElementWithStatistics entityStats : retriever) {
			results2.add(entityStats);
		}
		retriever.close();
		Set<GraphElementWithStatistics> expectedResults2 = new HashSet<GraphElementWithStatistics>();
		Entity entity2 = new Entity("product", "R", "purchase", "count", visibilityString1, sevenDaysBefore, sixDaysBefore);
		SetOfStatistics statisticsEntity2 = new SetOfStatistics();
		statisticsEntity2.addStatistic("entity_count", new Count(12345));
		expectedResults2.add(new GraphElementWithStatistics(new GraphElement(entity2), statisticsEntity2));
		assertEquals(expectedResults2, results2);

		// Get all entities involving aaA - there shouldn't be any
		for (@SuppressWarnings("unused") GraphElementWithStatistics entityWithStats : graph.getGraphElementsWithStatistics(new TypeValue("customer", "aaA"))) {
			fail("There shouldn't be any results");
		}
	}

	@Test
	public void testGetEdgesWithStatisticsFromPairsAllTimeAndRollUpOn() {
		AccumuloBackedGraph graph = setupGraph(true, null, null);

		// Get all edges involving customer A and product P
		Set<EdgeWithStatistics> results = new HashSet<EdgeWithStatistics>();
		Pair<TypeValue> typeValuePair = new Pair<TypeValue>(new TypeValue("customer", "A"), new TypeValue("product", "P"));
		Set<Pair<TypeValue>> typeValuePairs = new HashSet<Pair<TypeValue>>();
		typeValuePairs.add(typeValuePair);
		CloseableIterable<EdgeWithStatistics> retriever = graph.getEdgesWithStatisticsFromPairs(typeValuePairs);
		for (EdgeWithStatistics edgeWithStatistics : retriever) {
			results.add(edgeWithStatistics);
		}
		retriever.close();
		Set<EdgeWithStatistics> expectedResults1 = new HashSet<EdgeWithStatistics>();
		Edge edge1 = new Edge("customer", "A", "product", "P", "purchase", "instore", true, visibilityString1 + "&" + visibilityString2,
				sevenDaysBefore, fiveDaysBefore);
		SetOfStatistics statistics1 = new SetOfStatistics();
		statistics1.addStatistic("count", new Count(20));
		statistics1.addStatistic("anotherCount", new Count(1000000));
		expectedResults1.add(new EdgeWithStatistics(edge1, statistics1));
		Edge edge4 = new Edge("customer", "A", "product", "P", "purchase", "instore", false, visibilityString2, sixDaysBefore, fiveDaysBefore);
		SetOfStatistics statistics4 = new SetOfStatistics();
		statistics4.addStatistic("countSomething", new Count(123456));
		expectedResults1.add(new EdgeWithStatistics(edge4, statistics4));
		assertEquals(expectedResults1, results);

		// Repeat but reverse the order of A and P
		results.clear();
		typeValuePair = new Pair<TypeValue>(new TypeValue("product", "P"), new TypeValue("customer", "A"));
		typeValuePairs.clear();
		typeValuePairs.add(typeValuePair);
		retriever = graph.getEdgesWithStatisticsFromPairs(typeValuePairs);
		for (EdgeWithStatistics edgeWithStatistics : retriever) {
			results.add(edgeWithStatistics);
		}
		retriever.close();
		assertEquals(expectedResults1, results);

		// Get all edges involving customer B and product Q
		results.clear();
		typeValuePair = new Pair<TypeValue>(new TypeValue("customer", "B"), new TypeValue("product", "Q"));
		typeValuePairs.clear();
		typeValuePairs.add(typeValuePair);
		retriever = graph.getEdgesWithStatisticsFromPairs(typeValuePairs);
		for (EdgeWithStatistics edgeWithStatistics : retriever) {
			results.add(edgeWithStatistics);
		}
		retriever.close();
		Set<EdgeWithStatistics> expectedResults2 = new HashSet<EdgeWithStatistics>();
		Edge edge5 = new Edge("customer", "B", "product", "Q", "purchase", "instore", true, visibilityString2, sixDaysBefore, fiveDaysBefore);
		SetOfStatistics statistics5 = new SetOfStatistics();
		statistics5.addStatistic("count", new Count(99));
		expectedResults2.add(new EdgeWithStatistics(edge5, statistics5));
		assertEquals(expectedResults2, results);
	}

	@Test
	public void testGetEdgesWithStatisticsFromPairsAllTimeAndRollUpOnRemovedStatistics() {
		AccumuloBackedGraph graph = setupGraph(true, null, null);
		graph.setStatisticsToRemoveByName(Collections.singleton("count"));

		// Get all edges involving customer A and product P
		Set<EdgeWithStatistics> results = new HashSet<EdgeWithStatistics>();
		Pair<TypeValue> typeValuePair = new Pair<TypeValue>(new TypeValue("customer", "A"), new TypeValue("product", "P"));
		Set<Pair<TypeValue>> typeValuePairs = new HashSet<Pair<TypeValue>>();
		typeValuePairs.add(typeValuePair);
		CloseableIterable<EdgeWithStatistics> retriever = graph.getEdgesWithStatisticsFromPairs(typeValuePairs);
		for (EdgeWithStatistics edgeWithStatistics : retriever) {
			results.add(edgeWithStatistics);
		}
		retriever.close();
		Set<EdgeWithStatistics> expectedResults1 = new HashSet<EdgeWithStatistics>();
		Edge edge1 = new Edge("customer", "A", "product", "P", "purchase", "instore", true, visibilityString1 + "&" + visibilityString2,
				sevenDaysBefore, fiveDaysBefore);
		SetOfStatistics statistics1 = new SetOfStatistics();
		statistics1.addStatistic("anotherCount", new Count(1000000));
		expectedResults1.add(new EdgeWithStatistics(edge1, statistics1));
		Edge edge4 = new Edge("customer", "A", "product", "P", "purchase", "instore", false, visibilityString2, sixDaysBefore, fiveDaysBefore);
		SetOfStatistics statistics4 = new SetOfStatistics();
		statistics4.addStatistic("countSomething", new Count(123456));
		expectedResults1.add(new EdgeWithStatistics(edge4, statistics4));
		assertEquals(expectedResults1, results);

		// Repeat but reverse the order of A and P
		results.clear();
		typeValuePair = new Pair<TypeValue>(new TypeValue("product", "P"), new TypeValue("customer", "A"));
		typeValuePairs.clear();
		typeValuePairs.add(typeValuePair);
		retriever = graph.getEdgesWithStatisticsFromPairs(typeValuePairs);
		for (EdgeWithStatistics edgeWithStatistics : retriever) {
			results.add(edgeWithStatistics);
		}
		retriever.close();
		assertEquals(expectedResults1, results);

		// Get all edges involving customer B and product Q
		results.clear();
		typeValuePair = new Pair<TypeValue>(new TypeValue("customer", "B"), new TypeValue("product", "Q"));
		typeValuePairs.clear();
		typeValuePairs.add(typeValuePair);
		retriever = graph.getEdgesWithStatisticsFromPairs(typeValuePairs);
		for (EdgeWithStatistics edgeWithStatistics : retriever) {
			results.add(edgeWithStatistics);
		}
		retriever.close();
		Set<EdgeWithStatistics> expectedResults2 = new HashSet<EdgeWithStatistics>();
		Edge edge5 = new Edge("customer", "B", "product", "Q", "purchase", "instore", true, visibilityString2, sixDaysBefore, fiveDaysBefore);
		SetOfStatistics statistics5 = new SetOfStatistics();
		expectedResults2.add(new EdgeWithStatistics(edge5, statistics5));
		assertEquals(expectedResults2, results);
	}

	@Test
	public void testGetEdgesWithStatisticsFromPairsAllTimeAndRollUpOnKeepStatistics() {
		AccumuloBackedGraph graph = setupGraph(true, null, null);
		graph.setStatisticsToKeepByName(Collections.singleton("count"));

		// Get all edges involving customer A and product P
		Set<EdgeWithStatistics> results = new HashSet<EdgeWithStatistics>();
		Pair<TypeValue> typeValuePair = new Pair<TypeValue>(new TypeValue("customer", "A"), new TypeValue("product", "P"));
		Set<Pair<TypeValue>> typeValuePairs = new HashSet<Pair<TypeValue>>();
		typeValuePairs.add(typeValuePair);
		CloseableIterable<EdgeWithStatistics> retriever = graph.getEdgesWithStatisticsFromPairs(typeValuePairs);
		for (EdgeWithStatistics edgeWithStatistics : retriever) {
			results.add(edgeWithStatistics);
		}
		retriever.close();
		Set<EdgeWithStatistics> expectedResults1 = new HashSet<EdgeWithStatistics>();
		Edge edge1 = new Edge("customer", "A", "product", "P", "purchase", "instore", true, visibilityString1 + "&" + visibilityString2,
				sevenDaysBefore, fiveDaysBefore);
		SetOfStatistics statistics1 = new SetOfStatistics();
		statistics1.addStatistic("count", new Count(20));
		expectedResults1.add(new EdgeWithStatistics(edge1, statistics1));
		Edge edge4 = new Edge("customer", "A", "product", "P", "purchase", "instore", false, visibilityString2, sixDaysBefore, fiveDaysBefore);
		SetOfStatistics statistics4 = new SetOfStatistics();
		expectedResults1.add(new EdgeWithStatistics(edge4, statistics4));
		assertEquals(expectedResults1, results);

		// Repeat but reverse the order of A and P
		results.clear();
		typeValuePair = new Pair<TypeValue>(new TypeValue("product", "P"), new TypeValue("customer", "A"));
		typeValuePairs.clear();
		typeValuePairs.add(typeValuePair);
		retriever = graph.getEdgesWithStatisticsFromPairs(typeValuePairs);
		for (EdgeWithStatistics edgeWithStatistics : retriever) {
			results.add(edgeWithStatistics);
		}
		retriever.close();
		assertEquals(expectedResults1, results);

		// Get all edges involving customer B and product Q
		results.clear();
		typeValuePair = new Pair<TypeValue>(new TypeValue("customer", "B"), new TypeValue("product", "Q"));
		typeValuePairs.clear();
		typeValuePairs.add(typeValuePair);
		retriever = graph.getEdgesWithStatisticsFromPairs(typeValuePairs);
		for (EdgeWithStatistics edgeWithStatistics : retriever) {
			results.add(edgeWithStatistics);
		}
		retriever.close();
		Set<EdgeWithStatistics> expectedResults2 = new HashSet<EdgeWithStatistics>();
		Edge edge5 = new Edge("customer", "B", "product", "Q", "purchase", "instore", true, visibilityString2, sixDaysBefore, fiveDaysBefore);
		SetOfStatistics statistics5 = new SetOfStatistics();
		statistics5.addStatistic("count", new Count(99));
		expectedResults2.add(new EdgeWithStatistics(edge5, statistics5));
		assertEquals(expectedResults2, results);
	}

	@Test
	public void testGetGraphElementsWithStatisticsFromRangesWhenQueryForEmptySetOfRanges() {
		AccumuloBackedGraph graph = setupGraphForRangeTest(true, null, null);

		Set<TypeValueRange> empty = new HashSet<TypeValueRange>();
		Set<GraphElementWithStatistics> results = new HashSet<GraphElementWithStatistics>();
		CloseableIterable<GraphElementWithStatistics> retriever = graph.getGraphElementsWithStatisticsFromRanges(empty);
		for (GraphElementWithStatistics element : retriever) {
			results.add(element);
		}
		retriever.close();
		assertEquals(0, results.size());
	}

	@Test
	public void testGetGraphElementsWithStatisticsFromRanges() {
		AccumuloBackedGraph graph = setupGraphForRangeTest(true, null, null);

		// Get everything for the range customer|b1 - customer|b3 (including anything like customer|b33)
		TypeValueRange typeValueRange = new TypeValueRange("customer", "b1", "customer", "b4");
		Set<TypeValueRange> typeValueRanges = new HashSet<TypeValueRange>();
		typeValueRanges.add(typeValueRange);
		Set<GraphElementWithStatistics> results = new HashSet<GraphElementWithStatistics>();
		CloseableIterable<GraphElementWithStatistics> retriever = graph.getGraphElementsWithStatisticsFromRanges(typeValueRanges);
		for (GraphElementWithStatistics elementStats : retriever) {
			results.add(elementStats);
		}
		retriever.close();
		Set<GraphElementWithStatistics> expectedResults1 = new HashSet<GraphElementWithStatistics>();
		Edge expectedResult1 = new Edge("customer", "b1", "product", "P", "purchase", "instore", true,
				visibilityString1, sevenDaysBefore, sixDaysBefore);
		SetOfStatistics expectedSetOfStatistics1 = new SetOfStatistics();
		expectedSetOfStatistics1.addStatistic("count", new Count(2));
		expectedSetOfStatistics1.addStatistic("anotherCount", new Count(1000000));
		expectedResults1.add(new GraphElementWithStatistics(new GraphElement(expectedResult1), expectedSetOfStatistics1));
		Edge expectedResult2 = new Edge("customer", "b2", "product", "P", "purchase", "instore", true,
				visibilityString1, sevenDaysBefore, sixDaysBefore);
		SetOfStatistics expectedSetOfStatistics2 = new SetOfStatistics();
		expectedSetOfStatistics2.addStatistic("count", new Count(17));
		expectedResults1.add(new GraphElementWithStatistics(new GraphElement(expectedResult2), expectedSetOfStatistics2));
		Edge expectedResult3 = new Edge("customer", "b3", "product", "P", "purchase", "instore", false,
				visibilityString1, sevenDaysBefore, sixDaysBefore);
		SetOfStatistics expectedSetOfStatistics3 = new SetOfStatistics();
		expectedSetOfStatistics3.addStatistic("countSomething", new Count(123456));
		expectedResults1.add(new GraphElementWithStatistics(new GraphElement(expectedResult3), expectedSetOfStatistics3));
		Edge expectedResult4 = new Edge("customer", "b33", "product", "P", "purchase", "instore", false,
				visibilityString1, sevenDaysBefore, sixDaysBefore);
		SetOfStatistics expectedSetOfStatistics4 = new SetOfStatistics();
		expectedSetOfStatistics4.addStatistic("countSomething", new Count(7890123));
		expectedResults1.add(new GraphElementWithStatistics(new GraphElement(expectedResult4), expectedSetOfStatistics4));

		assertEquals(expectedResults1, results);
	}

	@Test
	public void testGetGraphElementsWithStatisticsFromRangesEntityOnly() {
		AccumuloBackedGraph graph = setupGraphForRangeTest(true, null, null);
		graph.setReturnEntitiesOnly();

		// Get entities in the range customer|b1 - customer|b3 (including anything like customer|b33) -
		// there shouldn't be anything
		TypeValueRange typeValueRange = new TypeValueRange("customer", "b1", "customer", "b4");
		Set<TypeValueRange> typeValueRanges = new HashSet<TypeValueRange>();
		typeValueRanges.add(typeValueRange);
		for (@SuppressWarnings("unused") GraphElementWithStatistics elementStats : graph.getGraphElementsWithStatisticsFromRanges(typeValueRanges)) {
			fail("There shouldn't have been any results - no Entitys in the range " + typeValueRange);
		}

		// Get entities in the range customer|a - customer|az - there should be one entity
		typeValueRange = new TypeValueRange("customer", "a", "customer", "az");
		typeValueRanges.clear();
		typeValueRanges.add(typeValueRange);
		Set<GraphElementWithStatistics> results = new HashSet<GraphElementWithStatistics>();
		CloseableIterable<GraphElementWithStatistics> retriever = graph.getGraphElementsWithStatisticsFromRanges(typeValueRanges);
		for (GraphElementWithStatistics elementStats : retriever) {
			results.add(elementStats);
		}
		retriever.close();
		Set<GraphElementWithStatistics> expectedResults = new HashSet<GraphElementWithStatistics>();
		Entity expectedResult1 = new Entity("customer", "a", "purchase", "customer", visibilityString1, sevenDaysBefore, sixDaysBefore);
		SetOfStatistics expectedSetOfStatistics1 = new SetOfStatistics();
		expectedSetOfStatistics1.addStatistic("count", new Count(128));
		expectedResults.add(new GraphElementWithStatistics(new GraphElement(expectedResult1), expectedSetOfStatistics1));
		assertEquals(expectedResults, results);
	}

	@Test
	public void testGetGraphElementsWithStatisticsFromRangesEdgeOnly() {
		AccumuloBackedGraph graph = setupGraphForRangeTest(true, null, null);
		graph.setReturnEdgesOnly();

		// Get edges for the range customer|a - customer|az
		TypeValueRange typeValueRange = new TypeValueRange("customer", "a", "customer", "az");
		Set<TypeValueRange> typeValueRanges = new HashSet<TypeValueRange>();
		typeValueRanges.add(typeValueRange);
		Set<GraphElementWithStatistics> results = new HashSet<GraphElementWithStatistics>();
		CloseableIterable<GraphElementWithStatistics> retriever = graph.getGraphElementsWithStatisticsFromRanges(typeValueRanges);
		for (GraphElementWithStatistics elementStats : retriever) {
			results.add(elementStats);
		}
		retriever.close();
		Set<GraphElementWithStatistics> expectedResults1 = new HashSet<GraphElementWithStatistics>();
		Edge expectedResult1 = new Edge("customer", "a", "product", "P", "purchase", "instore", true, visibilityString1, sevenDaysBefore, sixDaysBefore);
		SetOfStatistics expectedSetOfStatistics1 = new SetOfStatistics();
		expectedSetOfStatistics1.addStatistic("count", new Count(1));
		expectedResults1.add(new GraphElementWithStatistics(new GraphElement(expectedResult1), expectedSetOfStatistics1));

		assertEquals(expectedResults1, results);

		// Get edges for the range customer|b1 - customer|b3 (including anything like customer|b33)
		typeValueRange = new TypeValueRange("customer", "b1", "customer", "b4");
		typeValueRanges = new HashSet<TypeValueRange>();
		typeValueRanges.add(typeValueRange);
		results = new HashSet<GraphElementWithStatistics>();
		retriever = graph.getGraphElementsWithStatisticsFromRanges(typeValueRanges);
		for (GraphElementWithStatistics elementStats : retriever) {
			results.add(elementStats);
		}
		retriever.close();
		expectedResults1 = new HashSet<GraphElementWithStatistics>();
		expectedResult1 = new Edge("customer", "b1", "product", "P", "purchase", "instore", true,
				visibilityString1, sevenDaysBefore, sixDaysBefore);
		expectedSetOfStatistics1 = new SetOfStatistics();
		expectedSetOfStatistics1.addStatistic("count", new Count(2));
		expectedSetOfStatistics1.addStatistic("anotherCount", new Count(1000000));
		expectedResults1.add(new GraphElementWithStatistics(new GraphElement(expectedResult1), expectedSetOfStatistics1));
		Edge expectedResult2 = new Edge("customer", "b2", "product", "P", "purchase", "instore", true,
				visibilityString1, sevenDaysBefore, sixDaysBefore);
		SetOfStatistics expectedSetOfStatistics2 = new SetOfStatistics();
		expectedSetOfStatistics2.addStatistic("count", new Count(17));
		expectedResults1.add(new GraphElementWithStatistics(new GraphElement(expectedResult2), expectedSetOfStatistics2));
		Edge expectedResult3 = new Edge("customer", "b3", "product", "P", "purchase", "instore", false,
				visibilityString1, sevenDaysBefore, sixDaysBefore);
		SetOfStatistics expectedSetOfStatistics3 = new SetOfStatistics();
		expectedSetOfStatistics3.addStatistic("countSomething", new Count(123456));
		expectedResults1.add(new GraphElementWithStatistics(new GraphElement(expectedResult3), expectedSetOfStatistics3));
		Edge expectedResult4 = new Edge("customer", "b33", "product", "P", "purchase", "instore", false,
				visibilityString1, sevenDaysBefore, sixDaysBefore);
		SetOfStatistics expectedSetOfStatistics4 = new SetOfStatistics();
		expectedSetOfStatistics4.addStatistic("countSomething", new Count(7890123));
		expectedResults1.add(new GraphElementWithStatistics(new GraphElement(expectedResult4), expectedSetOfStatistics4));

		assertEquals(expectedResults1, results);
	}

	@Test
	public void testGetGraphElementsWithStatisticsFromRangesRemovedStatistics() {
		AccumuloBackedGraph graph = setupGraphForRangeTest(true, null, null);
		graph.setStatisticsToRemoveByName(Collections.singleton("count"));

		// Get everything for the range customer|b1 - customer|b3 (including anything like customer|b33)
		TypeValueRange typeValueRange = new TypeValueRange("customer", "b1", "customer", "b4");
		Set<TypeValueRange> typeValueRanges = new HashSet<TypeValueRange>();
		typeValueRanges.add(typeValueRange);
		Set<GraphElementWithStatistics> results = new HashSet<GraphElementWithStatistics>();
		CloseableIterable<GraphElementWithStatistics> retriever = graph.getGraphElementsWithStatisticsFromRanges(typeValueRanges);
		for (GraphElementWithStatistics elementStats : retriever) {
			results.add(elementStats);
		}
		retriever.close();
		Set<GraphElementWithStatistics> expectedResults1 = new HashSet<GraphElementWithStatistics>();
		Edge expectedResult1 = new Edge("customer", "b1", "product", "P", "purchase", "instore", true,
				visibilityString1, sevenDaysBefore, sixDaysBefore);
		SetOfStatistics expectedSetOfStatistics1 = new SetOfStatistics();
		expectedSetOfStatistics1.addStatistic("anotherCount", new Count(1000000));
		expectedResults1.add(new GraphElementWithStatistics(new GraphElement(expectedResult1), expectedSetOfStatistics1));
		Edge expectedResult2 = new Edge("customer", "b2", "product", "P", "purchase", "instore", true,
				visibilityString1, sevenDaysBefore, sixDaysBefore);
		SetOfStatistics expectedSetOfStatistics2 = new SetOfStatistics();
		expectedResults1.add(new GraphElementWithStatistics(new GraphElement(expectedResult2), expectedSetOfStatistics2));
		Edge expectedResult3 = new Edge("customer", "b3", "product", "P", "purchase", "instore", false,
				visibilityString1, sevenDaysBefore, sixDaysBefore);
		SetOfStatistics expectedSetOfStatistics3 = new SetOfStatistics();
		expectedSetOfStatistics3.addStatistic("countSomething", new Count(123456));
		expectedResults1.add(new GraphElementWithStatistics(new GraphElement(expectedResult3), expectedSetOfStatistics3));
		Edge expectedResult4 = new Edge("customer", "b33", "product", "P", "purchase", "instore", false,
				visibilityString1, sevenDaysBefore, sixDaysBefore);
		SetOfStatistics expectedSetOfStatistics4 = new SetOfStatistics();
		expectedSetOfStatistics4.addStatistic("countSomething", new Count(7890123));
		expectedResults1.add(new GraphElementWithStatistics(new GraphElement(expectedResult4), expectedSetOfStatistics4));

		assertEquals(expectedResults1, results);
	}

	@Test
	public void testGetGraphElementsWithStatisticsFromRangesKeepStatistics() {
		AccumuloBackedGraph graph = setupGraphForRangeTest(true, null, null);
		graph.setStatisticsToKeepByName(Collections.singleton("count"));

		// Get everything for the range customer|b1 - customer|b3 (including anything like customer|b33)
		TypeValueRange typeValueRange = new TypeValueRange("customer", "b1", "customer", "b4");
		Set<TypeValueRange> typeValueRanges = new HashSet<TypeValueRange>();
		typeValueRanges.add(typeValueRange);
		Set<GraphElementWithStatistics> results = new HashSet<GraphElementWithStatistics>();
		CloseableIterable<GraphElementWithStatistics> retriever = graph.getGraphElementsWithStatisticsFromRanges(typeValueRanges);
		for (GraphElementWithStatistics elementStats : retriever) {
			results.add(elementStats);
		}
		retriever.close();
		Set<GraphElementWithStatistics> expectedResults1 = new HashSet<GraphElementWithStatistics>();
		Edge expectedResult1 = new Edge("customer", "b1", "product", "P", "purchase", "instore", true,
				visibilityString1, sevenDaysBefore, sixDaysBefore);
		SetOfStatistics expectedSetOfStatistics1 = new SetOfStatistics();
		expectedSetOfStatistics1.addStatistic("count", new Count(2));
		expectedResults1.add(new GraphElementWithStatistics(new GraphElement(expectedResult1), expectedSetOfStatistics1));
		Edge expectedResult2 = new Edge("customer", "b2", "product", "P", "purchase", "instore", true,
				visibilityString1, sevenDaysBefore, sixDaysBefore);
		SetOfStatistics expectedSetOfStatistics2 = new SetOfStatistics();
		expectedSetOfStatistics2.addStatistic("count", new Count(17));
		expectedResults1.add(new GraphElementWithStatistics(new GraphElement(expectedResult2), expectedSetOfStatistics2));
		Edge expectedResult3 = new Edge("customer", "b3", "product", "P", "purchase", "instore", false,
				visibilityString1, sevenDaysBefore, sixDaysBefore);
		SetOfStatistics expectedSetOfStatistics3 = new SetOfStatistics();
		expectedResults1.add(new GraphElementWithStatistics(new GraphElement(expectedResult3), expectedSetOfStatistics3));
		Edge expectedResult4 = new Edge("customer", "b33", "product", "P", "purchase", "instore", false,
				visibilityString1, sevenDaysBefore, sixDaysBefore);
		SetOfStatistics expectedSetOfStatistics4 = new SetOfStatistics();
		expectedResults1.add(new GraphElementWithStatistics(new GraphElement(expectedResult4), expectedSetOfStatistics4));

		assertEquals(expectedResults1, results);
	}

	@Test
	public void testGetCorrectSelfEdges() {
		AccumuloBackedGraph graph = setupGraphForSelfEdgeTest(true, null, null);

		// Get all elements involving customer a - should get one directed edge
		Set<GraphElementWithStatistics> results = new HashSet<GraphElementWithStatistics>();
		int count = 0;
		CloseableIterable<GraphElementWithStatistics> retriever = graph.getGraphElementsWithStatistics(new TypeValue("customer", "a"));
		for (GraphElementWithStatistics elementStats : retriever) {
			results.add(elementStats);
			count++;
		}
		retriever.close();
		Set<GraphElementWithStatistics> expectedResults1 = new HashSet<GraphElementWithStatistics>();
		Edge expectedResult1 = new Edge("customer", "a", "customer", "a", "purchase", "instore", true, visibilityString1, sevenDaysBefore, sixDaysBefore);
		SetOfStatistics expectedSetOfStatistics1 = new SetOfStatistics();
		expectedSetOfStatistics1.addStatistic("count", new Count(1));
		expectedResults1.add(new GraphElementWithStatistics(new GraphElement(expectedResult1), expectedSetOfStatistics1));
		assertEquals(expectedResults1, results);
		assertEquals(1, count);

		// Get all elements involving customer g - should get one undirected edge
		results = new HashSet<GraphElementWithStatistics>();
		count = 0;
		retriever = graph.getGraphElementsWithStatistics(new TypeValue("customer", "g"));
		for (GraphElementWithStatistics elementStats : retriever) {
			results.add(elementStats);
			count++;
		}
		retriever.close();
		expectedResults1 = new HashSet<GraphElementWithStatistics>();
		expectedResult1 = new Edge("customer", "g", "customer", "g", "purchase", "instore", false, visibilityString1, sevenDaysBefore, sixDaysBefore);
		expectedSetOfStatistics1 = new SetOfStatistics();
		expectedSetOfStatistics1.addStatistic("count", new Count(10));
		expectedResults1.add(new GraphElementWithStatistics(new GraphElement(expectedResult1), expectedSetOfStatistics1));
		assertEquals(expectedResults1, results);
		assertEquals(1, count);

		// Get all graph elements involving aaA - there shouldn't be any
		for (@SuppressWarnings("unused") GraphElementWithStatistics elementWithStats : graph.getGraphElementsWithStatistics(new TypeValue("customer", "aaA"))) {
			fail("There shouldn't be any results");
		}
	}

	@Test
	public void testVisibility() {
		AccumuloBackedGraph graph = setupGraphForVisibilityTest();

		// Set auths to be empty, query for customer A, should get nothing
		Authorizations auths = new Authorizations();
		graph.setAuthorizations(auths);
		int count = 0;
		CloseableIterable<GraphElementWithStatistics> retriever = graph.getGraphElementsWithStatistics(new TypeValue("customer", "A"));
		for (@SuppressWarnings("unused") GraphElementWithStatistics elementStats : retriever) {
			count++;
		}
		retriever.close();
		assertEquals(0, count);

		// Set auths to be visibilityString1, query for customer A, should get edge1 and edge4
		auths = new Authorizations(visibilityString1);
		graph.setAuthorizations(auths);
		count = 0;
		Set<GraphElementWithStatistics> results = new HashSet<GraphElementWithStatistics>();
		retriever = graph.getGraphElementsWithStatistics(new TypeValue("customer", "A"));
		for (GraphElementWithStatistics elementStats : retriever) {
			results.add(elementStats);
			count++;
		}
		retriever.close();
		Set<GraphElementWithStatistics> expectedResults1 = new HashSet<GraphElementWithStatistics>();
		Edge expectedResult1 = new Edge("customer", "A", "product", "P", "purchase", "instore", true, visibilityString1, sevenDaysBefore, sixDaysBefore);
		SetOfStatistics expectedSetOfStatistics1 = new SetOfStatistics();
		expectedSetOfStatistics1.addStatistic("count", new Count(1));
		expectedResults1.add(new GraphElementWithStatistics(new GraphElement(expectedResult1), expectedSetOfStatistics1));
		Edge expectedResult2 = new Edge("customer", "A", "product", "S", "purchase", "instore", false, visibilityString1 + "|" + visibilityString2,
				sixDaysBefore, fiveDaysBefore);
		SetOfStatistics expectedSetOfStatistics2 = new SetOfStatistics();
		expectedSetOfStatistics2.addStatistic("countSomething", new Count(123456));
		expectedResults1.add(new GraphElementWithStatistics(new GraphElement(expectedResult2), expectedSetOfStatistics2));
		assertEquals(expectedResults1, results);
		assertEquals(2, count);

		// Set auths to be visibilityString1 and visibilityString2, query for customer A, should get all edges
		auths = new Authorizations(visibilityString1, visibilityString2);
		graph.setAuthorizations(auths);
		count = 0;
		results = new HashSet<GraphElementWithStatistics>();
		retriever = graph.getGraphElementsWithStatistics(new TypeValue("customer", "A"));
		for (GraphElementWithStatistics elementStats : retriever) {
			results.add(elementStats);
			count++;
		}
		retriever.close();
		Edge expectedResult3 = new Edge("customer", "A", "product", "Q", "purchase", "instore", true, visibilityString2, sixDaysBefore, fiveDaysBefore);
		SetOfStatistics expectedSetOfStatistics3 = new SetOfStatistics();
		expectedSetOfStatistics3.addStatistic("count", new Count(2));
		expectedSetOfStatistics3.addStatistic("anotherCount", new Count(1000000));
		expectedResults1.add(new GraphElementWithStatistics(new GraphElement(expectedResult3), expectedSetOfStatistics3));
		Edge expectedResult4 = new Edge("customer", "A", "product", "R", "purchase", "instore", true, visibilityString1 + "&" + visibilityString2,
				sixDaysBefore, fiveDaysBefore);
		SetOfStatistics expectedSetOfStatistics4 = new SetOfStatistics();
		expectedSetOfStatistics4.addStatistic("count", new Count(17));
		expectedResults1.add(new GraphElementWithStatistics(new GraphElement(expectedResult4), expectedSetOfStatistics4));
		assertEquals(expectedResults1, results);
		assertEquals(4, count);
	}

	@Test
	public void testUndirectedEdgesOnlyAndDirectedEdgesOnly() {
		AccumuloBackedGraph graph = setupGraph(true, null, null);

		// Set undirected edges only - should get edge4 and entity1
		graph.setUndirectedEdgesOnly();
		int count = 0;
		Set<GraphElementWithStatistics> results = new HashSet<GraphElementWithStatistics>();
		CloseableIterable<GraphElementWithStatistics> retriever = graph.getGraphElementsWithStatistics(new TypeValue("customer", "A"));
		for (GraphElementWithStatistics elementStats : retriever) {
			results.add(elementStats);
			count++;
		}
		retriever.close();
		Set<GraphElementWithStatistics> expectedResults1 = new HashSet<GraphElementWithStatistics>();
		Edge expectedResult1 = new Edge("customer", "A", "product", "P", "purchase", "instore", false, visibilityString2, sixDaysBefore, fiveDaysBefore);
		SetOfStatistics expectedSetOfStatistics1 = new SetOfStatistics();
		expectedSetOfStatistics1.addStatistic("countSomething", new Count(123456));
		expectedResults1.add(new GraphElementWithStatistics(new GraphElement(expectedResult1), expectedSetOfStatistics1));
		Entity expectedResult2 = new Entity("customer", "A", "purchase", "count", visibilityString1, sevenDaysBefore, sixDaysBefore);
		SetOfStatistics statisticsEntity1 = new SetOfStatistics();
		statisticsEntity1.addStatistic("entity_count", new Count(1000000));
		expectedResults1.add(new GraphElementWithStatistics(new GraphElement(expectedResult2), statisticsEntity1));
		assertEquals(expectedResults1, results);
		assertEquals(2, count);

		// Now set directed edges only - should get edge A-P and entity1
		graph.setDirectedEdgesOnly();
		count = 0;
		results = new HashSet<GraphElementWithStatistics>();
		retriever = graph.getGraphElementsWithStatistics(new TypeValue("customer", "A"));
		for (GraphElementWithStatistics elementStats : retriever) {
			results.add(elementStats);
			count++;
		}
		retriever.close();
		Set<GraphElementWithStatistics> expectedResults2 = new HashSet<GraphElementWithStatistics>();
		Edge expectedResult3 = new Edge("customer", "A", "product", "P", "purchase", "instore", true,
				visibilityString1 + "&" + visibilityString2, sevenDaysBefore, fiveDaysBefore);
		SetOfStatistics expectedSetOfStatistics3 = new SetOfStatistics();
		expectedSetOfStatistics3.addStatistic("count", new Count(20));
		expectedSetOfStatistics3.addStatistic("anotherCount", new Count(1000000));
		expectedResults2.add(new GraphElementWithStatistics(new GraphElement(expectedResult3), expectedSetOfStatistics3));
		expectedResults2.add(new GraphElementWithStatistics(new GraphElement(expectedResult2), statisticsEntity1));
		assertEquals(expectedResults2, results);
		assertEquals(2, count);

		// Now set undirected edges only with return edges only set - should only get an edge
		graph.setUndirectedEdgesOnly();
		graph.setReturnEdgesOnly();
		count = 0;
		results = new HashSet<GraphElementWithStatistics>();
		retriever = graph.getGraphElementsWithStatistics(new TypeValue("customer", "A"));
		for (GraphElementWithStatistics elementStats : retriever) {
			results.add(elementStats);
			count++;
		}
		retriever.close();
		Set<GraphElementWithStatistics> expectedResults3 = new HashSet<GraphElementWithStatistics>();
		expectedResults3.add(new GraphElementWithStatistics(new GraphElement(expectedResult1), expectedSetOfStatistics1));
		assertEquals(expectedResults3, results);
		assertEquals(1, count);

		// Now set directed edges only with return edges only set - should only get an edge
		graph.setDirectedEdgesOnly();
		graph.setReturnEdgesOnly();
		count = 0;
		results = new HashSet<GraphElementWithStatistics>();
		retriever = graph.getGraphElementsWithStatistics(new TypeValue("customer", "A"));
		for (GraphElementWithStatistics elementStats : retriever) {
			results.add(elementStats);
			count++;
		}
		retriever.close();
		Set<GraphElementWithStatistics> expectedResults4 = new HashSet<GraphElementWithStatistics>();
		expectedResults4.add(new GraphElementWithStatistics(new GraphElement(expectedResult3), expectedSetOfStatistics3));
		assertEquals(expectedResults4, results);
		assertEquals(1, count);
	}

	@Test
	public void testOutgoingEdgesOnly() {
		AccumuloBackedGraph graph = setupGraph(true, null, null);

		// Set undirected edges only and outgoing edges only - should get edge4 and entity1
		graph.setUndirectedEdgesOnly();
		graph.setOutgoingEdgesOnly();
		int count = 0;
		Set<GraphElementWithStatistics> results = new HashSet<GraphElementWithStatistics>();
		CloseableIterable<GraphElementWithStatistics> retriever = graph.getGraphElementsWithStatistics(new TypeValue("customer", "A"));
		for (GraphElementWithStatistics elementStats : retriever) {
			results.add(elementStats);
			count++;
		}
		retriever.close();
		Set<GraphElementWithStatistics> expectedResults1 = new HashSet<GraphElementWithStatistics>();
		Edge expectedResult1 = new Edge("customer", "A", "product", "P", "purchase", "instore", false, visibilityString2, sixDaysBefore, fiveDaysBefore);
		SetOfStatistics expectedSetOfStatistics1 = new SetOfStatistics();
		expectedSetOfStatistics1.addStatistic("countSomething", new Count(123456));
		expectedResults1.add(new GraphElementWithStatistics(new GraphElement(expectedResult1), expectedSetOfStatistics1));
		Entity expectedResult2 = new Entity("customer", "A", "purchase", "count", visibilityString1, sevenDaysBefore, sixDaysBefore);
		SetOfStatistics statisticsEntity1 = new SetOfStatistics();
		statisticsEntity1.addStatistic("entity_count", new Count(1000000));
		expectedResults1.add(new GraphElementWithStatistics(new GraphElement(expectedResult2), statisticsEntity1));
		assertEquals(expectedResults1, results);
		assertEquals(2, count);

		// Now set directed edges only and outgoing edges only - should get edge A-P and entity1
		graph.setDirectedEdgesOnly();
		graph.setOutgoingEdgesOnly();
		count = 0;
		results = new HashSet<GraphElementWithStatistics>();
		retriever = graph.getGraphElementsWithStatistics(new TypeValue("customer", "A"));
		for (GraphElementWithStatistics elementStats : retriever) {
			results.add(elementStats);
			count++;
		}
		retriever.close();
		Set<GraphElementWithStatistics> expectedResults2 = new HashSet<GraphElementWithStatistics>();
		Edge expectedResult3 = new Edge("customer", "A", "product", "P", "purchase", "instore", true,
				visibilityString1 + "&" + visibilityString2, sevenDaysBefore, fiveDaysBefore);
		SetOfStatistics expectedSetOfStatistics3 = new SetOfStatistics();
		expectedSetOfStatistics3.addStatistic("count", new Count(20));
		expectedSetOfStatistics3.addStatistic("anotherCount", new Count(1000000));
		expectedResults2.add(new GraphElementWithStatistics(new GraphElement(expectedResult3), expectedSetOfStatistics3));
		expectedResults2.add(new GraphElementWithStatistics(new GraphElement(expectedResult2), statisticsEntity1));
		assertEquals(expectedResults2, results);
		assertEquals(2, count);
	}

	@Test
	public void testIncomingEdgesOnly() {
		AccumuloBackedGraph graph = setupGraph(true, null, null);

		// Set undirected edges only and incoming edges only - should get edge4 and entity1
		graph.setUndirectedEdgesOnly();
		graph.setIncomingEdgesOnly();
		int count = 0;
		Set<GraphElementWithStatistics> results = new HashSet<GraphElementWithStatistics>();
		CloseableIterable<GraphElementWithStatistics> retriever = graph.getGraphElementsWithStatistics(new TypeValue("customer", "A"));
		for (GraphElementWithStatistics elementStats : retriever) {
			results.add(elementStats);
			count++;
		}
		retriever.close();
		Set<GraphElementWithStatistics> expectedResults1 = new HashSet<GraphElementWithStatistics>();
		Edge expectedResult1 = new Edge("customer", "A", "product", "P", "purchase", "instore", false, visibilityString2, sixDaysBefore, fiveDaysBefore);
		SetOfStatistics expectedSetOfStatistics1 = new SetOfStatistics();
		expectedSetOfStatistics1.addStatistic("countSomething", new Count(123456));
		expectedResults1.add(new GraphElementWithStatistics(new GraphElement(expectedResult1), expectedSetOfStatistics1));
		Entity expectedResult2 = new Entity("customer", "A", "purchase", "count", visibilityString1, sevenDaysBefore, sixDaysBefore);
		SetOfStatistics statisticsEntity1 = new SetOfStatistics();
		statisticsEntity1.addStatistic("entity_count", new Count(1000000));
		expectedResults1.add(new GraphElementWithStatistics(new GraphElement(expectedResult2), statisticsEntity1));
		assertEquals(expectedResults1, results);
		assertEquals(2, count);

		// Now set directed edges only and incoming edges only (from product P) - should get edge A-P
		graph.setDirectedEdgesOnly();
		graph.setIncomingEdgesOnly();
		count = 0;
		results = new HashSet<GraphElementWithStatistics>();
		retriever = graph.getGraphElementsWithStatistics(new TypeValue("product", "P"));
		for (GraphElementWithStatistics elementStats : retriever) {
			results.add(elementStats);
			count++;
		}
		retriever.close();
		Set<GraphElementWithStatistics> expectedResults2 = new HashSet<GraphElementWithStatistics>();
		Edge expectedResult3 = new Edge("customer", "A", "product", "P", "purchase", "instore", true,
				visibilityString1 + "&" + visibilityString2, sevenDaysBefore, fiveDaysBefore);
		SetOfStatistics expectedSetOfStatistics3 = new SetOfStatistics();
		expectedSetOfStatistics3.addStatistic("count", new Count(20));
		expectedSetOfStatistics3.addStatistic("anotherCount", new Count(1000000));
		expectedResults2.add(new GraphElementWithStatistics(new GraphElement(expectedResult3), expectedSetOfStatistics3));
		assertEquals(expectedResults2, results);
		assertEquals(1, count);
	}

	@Test
	public void testOtherEndOfEdgePredicate() throws AccumuloSecurityException, AccumuloException, TableNotFoundException {
		AccumuloBackedGraph graph = setupGraph(true, null, null);

		// Set other end predicate to be type in the set containing "product" - query for customer B, should get product Q
		graph.setOtherEndOfEdgePredicate(new TypeInSetPredicate("product"));
		int count = 0;
		Set<GraphElementWithStatistics> results = new HashSet<GraphElementWithStatistics>();
		CloseableIterable<GraphElementWithStatistics> retriever = graph.getGraphElementsWithStatistics(new TypeValue("customer", "B"));
		for (GraphElementWithStatistics elementStats : retriever) {
			results.add(elementStats);
			count++;
		}
		retriever.close();
		Set<GraphElementWithStatistics> expectedResults1 = new HashSet<GraphElementWithStatistics>();
		Edge expectedResult1 = new Edge("customer", "B", "product", "Q", "purchase", "instore", true, visibilityString2, sixDaysBefore, fiveDaysBefore);
		SetOfStatistics expectedSetOfStatistics1 = new SetOfStatistics();
		expectedSetOfStatistics1.addStatistic("count", new Count(99));
		expectedResults1.add(new GraphElementWithStatistics(new GraphElement(expectedResult1), expectedSetOfStatistics1));
		assertEquals(expectedResults1, results);
		assertEquals(1, count);

		// Set other end predicate to be type in the set containing "abc" - shouldn't get anything
		graph.setOtherEndOfEdgePredicate(new TypeInSetPredicate("abc"));
		count = 0;
		retriever = graph.getGraphElementsWithStatistics(new TypeValue("customer", "B"));
		for (@SuppressWarnings("unused") GraphElementWithStatistics elementStats : retriever) {
			count++;
		}
		retriever.close();
		assertEquals(0, count);

		// Set other end of edge predicate to be value matches Q - query for customer B, should get product Q
		graph.setOtherEndOfEdgePredicate(new ValueRegularExpressionPredicate(Pattern.compile("Q")));
		count = 0;
		results.clear();
		retriever = graph.getGraphElementsWithStatistics(new TypeValue("customer", "B"));
		for (GraphElementWithStatistics elementStats : retriever) {
			results.add(elementStats);
			count++;
		}
		retriever.close();
		assertEquals(expectedResults1, results);
		assertEquals(1, count);

		// Set other end of edge predicate to be value matches QQ - query for customer B, shouldn't get anything
		graph.setOtherEndOfEdgePredicate(new ValueRegularExpressionPredicate(Pattern.compile("QQ")));
		count = 0;
		retriever = graph.getGraphElementsWithStatistics(new TypeValue("customer", "B"));
		for (@SuppressWarnings("unused") GraphElementWithStatistics elementStats : retriever) {
			count++;
		}
		retriever.close();
		assertEquals(0, count);
	}

	@Test
	public void testSetPostRollUpTransform() throws AccumuloSecurityException, AccumuloException, TableNotFoundException {
		AccumuloBackedGraph graph = setupGraph(true, null, null);

		// Set post roll up transform and check it is applied correctly
		graph.setPostRollUpTransform(new SimpleTransform("abc"));

		int count = 0;
		Set<GraphElementWithStatistics> results = new HashSet<GraphElementWithStatistics>();
		CloseableIterable<GraphElementWithStatistics> retriever = graph.getGraphElementsWithStatistics(new TypeValue("customer", "B"));
		for (GraphElementWithStatistics elementStats : retriever) {
			results.add(elementStats);
			count++;
		}
		retriever.close();
		Set<GraphElementWithStatistics> expectedResults1 = new HashSet<GraphElementWithStatistics>();
		Edge expectedResult1 = new Edge("customer", "B", "product", "Q", "abc", "instore", true, visibilityString2, sixDaysBefore, fiveDaysBefore);
		SetOfStatistics expectedSetOfStatistics1 = new SetOfStatistics();
		expectedSetOfStatistics1.addStatistic("count", new Count(99));
		expectedResults1.add(new GraphElementWithStatistics(new GraphElement(expectedResult1), expectedSetOfStatistics1));
		assertEquals(expectedResults1, results);
		assertEquals(1, count);
	}

	public static class SimpleTransform implements Transform {

		private String s;

		public SimpleTransform() { }

		public SimpleTransform(String s) {
			this.s = s;
		}

		@Override
		public GraphElementWithStatistics transform(GraphElementWithStatistics graphElementWithStatistics) {
			if (graphElementWithStatistics.isEntity()) {
				graphElementWithStatistics.getGraphElement().getEntity().setSummaryType(s);
			} else {
				graphElementWithStatistics.getGraphElement().getEdge().setSummaryType(s);
			}
			return graphElementWithStatistics;
		}

		@Override
		public void write(DataOutput out) throws IOException {
			Text.writeString(out, s);
		}

		@Override
		public void readFields(DataInput in) throws IOException {
			s = Text.readString(in);
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) return true;
			if (o == null || getClass() != o.getClass()) return false;

			SimpleTransform that = (SimpleTransform) o;

			if (s != null ? !s.equals(that.s) : that.s != null) return false;

			return true;
		}

		@Override
		public int hashCode() {
			return s != null ? s.hashCode() : 0;
		}
	}

	public static class SimpleTransformAndFilter implements Transform {

		private String s;

		public SimpleTransformAndFilter() { }

		public SimpleTransformAndFilter(String s) {
			this.s = s;
		}

		@Override
		public GraphElementWithStatistics transform(GraphElementWithStatistics graphElementWithStatistics) {
			if (graphElementWithStatistics.isEntity()) {
				return null;
			}
			graphElementWithStatistics.getGraphElement().getEdge().setDestinationTypeAndValue(s, s);
			return graphElementWithStatistics;
		}

		@Override
		public void write(DataOutput out) throws IOException {
			Text.writeString(out, s);
		}

		@Override
		public void readFields(DataInput in) throws IOException {
			s = Text.readString(in);
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) return true;
			if (o == null || getClass() != o.getClass()) return false;

			SimpleTransformAndFilter that = (SimpleTransformAndFilter) o;

			if (s != null ? !s.equals(that.s) : that.s != null) return false;

			return true;
		}

		@Override
		public int hashCode() {
			return s != null ? s.hashCode() : 0;
		}
	}

	@Test
	public void testTimePredicate() {
		AccumuloBackedGraph graph = setupGraphForTimePredicateTest();

		// Set time predicate and check it gets the right result
		graph.setTimePredicate(new SimpleTimePredicate(new Date(100L), new Date(200L)));

		int count = 0;
		Set<GraphElementWithStatistics> results = new HashSet<GraphElementWithStatistics>();
		CloseableIterable<GraphElementWithStatistics> retriever = graph.getGraphElementsWithStatistics(new TypeValue("customer", "A"));
		for (GraphElementWithStatistics elementStats : retriever) {
			results.add(elementStats);
			count++;
		}
		retriever.close();
		assertEquals(1, count);

		Set<GraphElementWithStatistics> expectedResults1 = new HashSet<GraphElementWithStatistics>();
		Edge expectedResult1 = new Edge("customer", "A", "product", "P", "purchase", "instore", true,
				visibilityString1, new Date(100L), new Date(200L));
		SetOfStatistics expectedSetOfStatistics1 = new SetOfStatistics();
		expectedSetOfStatistics1.addStatistic("count", new Count(1));
		expectedResults1.add(new GraphElementWithStatistics(new GraphElement(expectedResult1), expectedSetOfStatistics1));
		assertEquals(expectedResults1, results);
	}

	public static class SimpleTimePredicate extends TimePredicate {

		private Date startDate;
		private Date endDate;

		public SimpleTimePredicate() { }

		public SimpleTimePredicate(Date startDate, Date endDate) {
			this.startDate = startDate;
			this.endDate = endDate;
		}

		@Override
		public boolean accept(Date start, Date end) {
			return startDate.equals(start) && endDate.equals(end);
		}

		@Override
		public void write(DataOutput out) throws IOException {
			out.writeLong(startDate.getTime());
			out.writeLong(endDate.getTime());
		}

		@Override
		public void readFields(DataInput in) throws IOException {
			startDate = new Date(in.readLong());
			endDate = new Date(in.readLong());
		}
	}

	/**
	 * Tests that the post roll-up transform is applied when a {@link EdgeWithStatisticsRetrieverFromPairs} is being used.
	 */
	@Test
	public void testPostRollUpTransformWhenGettingEdges() {
		AccumuloBackedGraph graph = setupGraph(true, null, null);
		graph.setPostRollUpTransform(new ExampleTransform("abc"));
		Pair<TypeValue> pair = new Pair<TypeValue>(new TypeValue("customer", "B"), new TypeValue("product", "Q"));
		CloseableIterable<EdgeWithStatistics> retriever = graph.getEdgesWithStatisticsFromPairs(Collections.singleton(pair));
		Set<EdgeWithStatistics> results = new HashSet<EdgeWithStatistics>();
		for (EdgeWithStatistics ews : retriever) {
			results.add(ews);
		}
		retriever.close();
		Set<EdgeWithStatistics> expectedResults = new HashSet<EdgeWithStatistics>();
		Edge edge5 = new Edge("customer", "B", "product", "Q", "abc", "instore", true, visibilityString2, sixDaysBefore, fiveDaysBefore);
		expectedResults.add(new EdgeWithStatistics(edge5, new SetOfStatistics()));
		assertEquals(expectedResults, results);
	}

	/**
	 * Tests that the post roll-up transform is applied when a {@link GraphElementRetrieverFromEntities} is being used.
	 */
	@Test
	public void testPostRollUpTransformWhenQueryingForGE() {
		AccumuloBackedGraph graph = setupGraph(true, null, null);
		graph.setPostRollUpTransform(new ExampleTransform("abc"));
		Set<TypeValue> seeds = new HashSet<TypeValue>();
		seeds.add(new TypeValue("customer", "B"));
		seeds.add(new TypeValue("product", "Q"));
		CloseableIterable<GraphElement> retriever = graph.getGraphElements(seeds);
		Set<GraphElement> results = new HashSet<GraphElement>();
		for (GraphElement ge : retriever) {
			results.add(ge);
		}
		retriever.close();
		Set<GraphElement> expectedResults = new HashSet<GraphElement>();
		Edge edge5 = new Edge("customer", "B", "product", "Q", "abc", "instore", true, visibilityString2, sixDaysBefore, fiveDaysBefore);
		expectedResults.add(new GraphElement(edge5));
		assertEquals(expectedResults, results);
	}

	/**
	 * Tests that the post roll-up transform is applied when a {@link GraphElementWithStatisticsBetweenSetsRetriever} is being used.
	 */
	@Test
	public void testPostRollUpTransformWhenQueryingBetweenSets() {
		AccumuloBackedGraph graph = setupGraph(true, null, null);
		graph.setPostRollUpTransform(new ExampleTransform("abc"));
		Set<TypeValue> seedsA = new HashSet<TypeValue>();
		seedsA.add(new TypeValue("customer", "B"));
		Set<TypeValue> seedsB = new HashSet<TypeValue>();
		seedsB.add(new TypeValue("product", "Q"));
		for (Boolean b : new Boolean[]{true, false}) {
			CloseableIterable<GraphElementWithStatistics> retriever = graph.getGraphElementsWithStatisticsBetweenSets(seedsA, seedsB, b);
			Set<GraphElementWithStatistics> results = new HashSet<GraphElementWithStatistics>();
			for (GraphElementWithStatistics gews : retriever) {
				results.add(gews);
			}
			retriever.close();
			Set<GraphElementWithStatistics> expectedResults = new HashSet<GraphElementWithStatistics>();
			Edge edge5 = new Edge("customer", "B", "product", "Q", "abc", "instore", true, visibilityString2, sixDaysBefore, fiveDaysBefore);
			SetOfStatistics statistics5 = new SetOfStatistics();
			expectedResults.add(new GraphElementWithStatistics(new GraphElement(edge5), statistics5));
			assertEquals(expectedResults, results);
		}
	}

	/**
	 * Tests that the post roll-up transform is applied when a {@link GraphElementWithStatisticsRetrieverFromEntities} is being used.
	 */
	@Test
	public void testPostRollUpTransformWhenQueryingForGEWS() {
		AccumuloBackedGraph graph = setupGraph(true, null, null);
		graph.setPostRollUpTransform(new ExampleTransform("abc"));
		Set<TypeValue> seeds = new HashSet<TypeValue>();
		seeds.add(new TypeValue("customer", "B"));
		seeds.add(new TypeValue("product", "Q"));
		CloseableIterable<GraphElementWithStatistics> retriever = graph.getGraphElementsWithStatistics(seeds);
		Set<GraphElementWithStatistics> results = new HashSet<GraphElementWithStatistics>();
		for (GraphElementWithStatistics gews : retriever) {
			results.add(gews);
		}
		retriever.close();
		Set<GraphElementWithStatistics> expectedResults = new HashSet<GraphElementWithStatistics>();
		Edge edge5 = new Edge("customer", "B", "product", "Q", "abc", "instore", true, visibilityString2, sixDaysBefore, fiveDaysBefore);
		SetOfStatistics statistics5 = new SetOfStatistics();
		expectedResults.add(new GraphElementWithStatistics(new GraphElement(edge5), statistics5));
		assertEquals(expectedResults, results);
	}

	/**
	 * Tests that the post roll-up transform is applied when a {@link GraphElementWithStatisticsRetrieverFromRanges} is being used.
	 */
	@Test
	public void testPostRollUpTransformWhenQueryingForRanges() {
		AccumuloBackedGraph graph = setupGraph(true, null, null);
		graph.setPostRollUpTransform(new ExampleTransform("abc"));
		TypeValueRange range = new TypeValueRange("customer", "B", "customer", "B2");
		Collection<TypeValueRange> ranges = Collections.singleton(range);
		CloseableIterable<GraphElementWithStatistics> retriever = graph.getGraphElementsWithStatisticsFromRanges(ranges);
		Set<GraphElementWithStatistics> results = new HashSet<GraphElementWithStatistics>();
		for (GraphElementWithStatistics gews : retriever) {
			results.add(gews);
		}
		retriever.close();
		Set<GraphElementWithStatistics> expectedResults = new HashSet<GraphElementWithStatistics>();
		Edge edge5 = new Edge("customer", "B", "product", "Q", "abc", "instore", true, visibilityString2, sixDaysBefore, fiveDaysBefore);
		SetOfStatistics statistics5 = new SetOfStatistics();
		expectedResults.add(new GraphElementWithStatistics(new GraphElement(edge5), statistics5));
		assertEquals(expectedResults, results);
	}

	/**
	 * Tests that the post roll-up transform is applied when a {@link GraphElementWithStatisticsWithinSetRetriever} is being used.
	 */
	@Test
	public void testPostRollUpTransformWhenQueryingWithinSet() {
		AccumuloBackedGraph graph = setupGraph(true, null, null);
		graph.setPostRollUpTransform(new ExampleTransform("abc"));
		Set<TypeValue> seeds = new HashSet<TypeValue>();
		seeds.add(new TypeValue("customer", "B"));
		seeds.add(new TypeValue("product", "Q"));
		for (Boolean b : new Boolean[]{true, false}) {
			CloseableIterable<GraphElementWithStatistics> retriever = graph.getGraphElementsWithStatisticsWithinSet(seeds, b);
			Set<GraphElementWithStatistics> results = new HashSet<GraphElementWithStatistics>();
			for (GraphElementWithStatistics gews : retriever) {
				results.add(gews);
			}
			retriever.close();
			Set<GraphElementWithStatistics> expectedResults = new HashSet<GraphElementWithStatistics>();
			Edge edge5 = new Edge("customer", "B", "product", "Q", "abc", "instore", true, visibilityString2, sixDaysBefore, fiveDaysBefore);
			SetOfStatistics statistics5 = new SetOfStatistics();
			expectedResults.add(new GraphElementWithStatistics(new GraphElement(edge5), statistics5));
			assertEquals(expectedResults, results);
		}
	}

	/**
	 * Tests that retrieval works for type-value pairs of varying lengths.
	 *
	 * @throws GraphAccessException
	 */
	@Test
	public void testQueryingForVariableLengthTypesAndValues() throws GraphAccessException {
		AccumuloBackedGraph graph = setupGraph(true, null, null);

		for (int i = 1; i < 50; i++) {
			String type = "";
			while (type.length() < i) {
				type += "c";
			}
			for (int j = 1; j < 50; j++) {
				String value = "";
				while (value.length() < j) {
					value += "A";
				}
				// Create entity and edge with the above type|value, and add to graph
				Entity entity = new Entity(type, value, "summaryType", "summarySubType", visibilityString1, sevenDaysBefore, sixDaysBefore);
				Edge edge = new Edge(type, value, "dType", "dValue", "summaryType", "summarySubType", true, visibilityString1, sevenDaysBefore, sixDaysBefore);
				Set<GraphElement> elements = new HashSet<GraphElement>();
				elements.add(new GraphElement(entity));
				elements.add(new GraphElement(edge));
				graph.addGraphElements(elements);
				// Query graph for that type|value and check that get correct results back
				CloseableIterable<GraphElement> retriever = graph.getGraphElements(new TypeValue(type, value));
				int count = 0;
				for (GraphElement element : retriever) {
					if (element.isEdge()) {
						assertEquals(edge, element.getEdge());
					} else {
						assertEquals(entity, element.getEntity());
					}
					count++;
				}
				retriever.close();
				assertEquals(2, count);
			}
		}

	}

	public static class ExampleTransform implements Transform {

		private String s;

		ExampleTransform(String s) {
			this.s = s;
		}

		@Override
		public GraphElementWithStatistics transform(GraphElementWithStatistics graphElementWithStatistics) {
			if (graphElementWithStatistics.isEntity()) {
				Entity entity = graphElementWithStatistics.getGraphElement().getEntity();
				entity.setSummaryType(s);
				return new GraphElementWithStatistics(new GraphElement(entity), new SetOfStatistics());
			}
			Edge edge = graphElementWithStatistics.getGraphElement().getEdge();
			edge.setSummaryType(s);
			return new GraphElementWithStatistics(new GraphElement(edge), new SetOfStatistics());
		}

		@Override
		public void write(DataOutput out) throws IOException {
			Text.writeString(out, s);
		}

		@Override
		public void readFields(DataInput in) throws IOException {
			s = Text.readString(in);
		}
	}

	private static AccumuloBackedGraph setupGraphForTimePredicateTest() {
		Instance instance = new MockInstance();
		String tableName = "Test";
		long ageOffTimeInMilliseconds = Long.MAX_VALUE;

		try {
			// Open connection
			Connector conn = instance.getConnector("user", "password");

			// Create table
			// (this method creates the table, removes the versioning iterator, and adds the SetOfStatisticsCombiner iterator,
			// and sets the age off iterator to age data off after it is more than ageOffTimeInMilliseconds milliseconds old).
			TableUtils.createTable(conn, tableName, ageOffTimeInMilliseconds);

			// Create set of GraphElementWithStatistics to store data before adding it to the graph.
			Set<GraphElementWithStatistics> data = new HashSet<GraphElementWithStatistics>();

			// An edge and some statistics for it
			Edge edge1 = new Edge("customer", "A", "product", "P", "purchase", "instore", true, visibilityString1, new Date(100L), new Date(200L));
			SetOfStatistics statistics1 = new SetOfStatistics();
			statistics1.addStatistic("count", new Count(1));
			data.add(new GraphElementWithStatistics(new GraphElement(edge1), statistics1));

			// An edge with different start and end dates and some statistics for it
			Edge edge2 = new Edge("customer", "A", "product", "Q", "purchase", "instore", true, visibilityString1, new Date(300L), new Date(400L));
			SetOfStatistics statistics2 = new SetOfStatistics();
			statistics2.addStatistic("count", new Count(2));
			statistics2.addStatistic("anotherCount", new Count(1000000));
			data.add(new GraphElementWithStatistics(new GraphElement(edge2), statistics2));

			// Create Accumulo backed graph
			AccumuloBackedGraph graph = new AccumuloBackedGraph(conn, tableName);
			graph.setAuthorizations(new Authorizations(visibilityString1, visibilityString2));

			// Add data
			graph.addGraphElementsWithStatistics(data);

			return graph;
		} catch (AccumuloException e) {
			fail("Failed to set up graph in Accumulo with exception: " + e);
		} catch (AccumuloSecurityException e) {
			fail("Failed to set up graph in Accumulo with exception: " + e);
		} catch (TableExistsException e) {
			fail("Failed to set up graph in Accumulo with exception: " + e);
		} catch (TableNotFoundException e) {
			fail("Failed to set up graph in Accumulo with exception: " + e);
		} catch (GraphAccessException e) {
			fail("Failed to set up graph in Accumulo with exception: " + e);
		}
		return null;
	}

	private static AccumuloBackedGraph setupGraphForVisibilityTest() {
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

			// Create set of GraphElementWithStatistics to store data before adding it to the graph.
			Set<GraphElementWithStatistics> data = new HashSet<GraphElementWithStatistics>();

			// A visibilityString1 edge and some statistics for it
			Edge edge1 = new Edge("customer", "A", "product", "P", "purchase", "instore", true, visibilityString1, sevenDaysBefore, sixDaysBefore);
			SetOfStatistics statistics1 = new SetOfStatistics();
			statistics1.addStatistic("count", new Count(1));
			data.add(new GraphElementWithStatistics(new GraphElement(edge1), statistics1));

			// A visibilityString2 edge and some statistics for it
			Edge edge2 = new Edge("customer", "A", "product", "Q", "purchase", "instore", true, visibilityString2, sixDaysBefore, fiveDaysBefore);
			SetOfStatistics statistics2 = new SetOfStatistics();
			statistics2.addStatistic("count", new Count(2));
			statistics2.addStatistic("anotherCount", new Count(1000000));
			data.add(new GraphElementWithStatistics(new GraphElement(edge2), statistics2));

			// A visibilityString1 and visibilityString2 edge and some statistics for it
			Edge edge3 = new Edge("customer", "A", "product", "R", "purchase", "instore", true, visibilityString1 + "&" + visibilityString2,
					sixDaysBefore, fiveDaysBefore);
			SetOfStatistics statistics3 = new SetOfStatistics();
			statistics3.addStatistic("count", new Count(17));
			data.add(new GraphElementWithStatistics(new GraphElement(edge3), statistics3));

			// A visibilityString1 or visibilityString2 edge and some statistics for it
			Edge edge4 = new Edge("customer", "A", "product", "S", "purchase", "instore", false, visibilityString1 + "|" + visibilityString2,
					sixDaysBefore, fiveDaysBefore);
			SetOfStatistics statistics4 = new SetOfStatistics();
			statistics4.addStatistic("countSomething", new Count(123456));
			data.add(new GraphElementWithStatistics(new GraphElement(edge4), statistics4));

			// Create Accumulo backed graph
			AccumuloBackedGraph graph = new AccumuloBackedGraph(conn, tableName);

			// Add data
			graph.addGraphElementsWithStatistics(data);

			return graph;
		} catch (AccumuloException e) {
			fail("Failed to set up graph in Accumulo with exception: " + e);
		} catch (AccumuloSecurityException e) {
			fail("Failed to set up graph in Accumulo with exception: " + e);
		} catch (TableExistsException e) {
			fail("Failed to set up graph in Accumulo with exception: " + e);
		} catch (TableNotFoundException e) {
			fail("Failed to set up graph in Accumulo with exception: " + e);
		} catch (GraphAccessException e) {
			fail("Failed to set up graph in Accumulo with exception: " + e);
		}
		return null;
	}

	private static AccumuloBackedGraph setupGraphForTypeTest(boolean useRollUpOverTimeAndVisibilityIterator, Date startTimeWindow, Date endTimeWindow) {
		Instance instance = new MockInstance();
		String tableName = "Test";
		long ageOffTimeInMilliseconds = (30 * 24 * 60 * 60 * 1000L); // 30 days in milliseconds
		// NB: Essential to have the 'L' here so that
		// it is a long (if not then multiplication
		// happens as int and overflows).

		try {
			// Open connection
			Connector conn = instance.getConnector("user", "password");

			// Create table
			// (this method creates the table, removes the versioning iterator, and adds the SetOfStatisticsCombiner iterator,
			// and sets the age off iterator to age data off after it is more than ageOffTimeInMilliseconds milliseconds old).
			TableUtils.createTable(conn, tableName, ageOffTimeInMilliseconds);

			// Create set of GraphElementWithStatistics to store data before adding it to the graph.
			Set<GraphElementWithStatistics> data = new HashSet<GraphElementWithStatistics>();

			// Edge 1 and some statistics for it
			Edge edge1 = new Edge("customer", "A", "product", "P", "purchase1", "customer1", true, visibilityString1, sevenDaysBefore, sixDaysBefore);
			SetOfStatistics statistics1 = new SetOfStatistics();
			statistics1.addStatistic("count", new Count(1));
			data.add(new GraphElementWithStatistics(new GraphElement(edge1), statistics1));

			// Edge 2 and some statistics for it
			Edge edge2 = new Edge("customer", "A", "product", "P", "purchase2", "customer2", true, visibilityString1, sixDaysBefore, fiveDaysBefore);
			SetOfStatistics statistics2 = new SetOfStatistics();
			statistics2.addStatistic("count", new Count(2));
			statistics2.addStatistic("anotherCount", new Count(1000000));
			data.add(new GraphElementWithStatistics(new GraphElement(edge2), statistics2));

			// Edge 3 and some statistics for it
			Edge edge3 = new Edge("customer", "A", "product", "P", "purchase3", "customer3", true, visibilityString2, sixDaysBefore, fiveDaysBefore);
			SetOfStatistics statistics3 = new SetOfStatistics();
			statistics3.addStatistic("count", new Count(17));
			data.add(new GraphElementWithStatistics(new GraphElement(edge3), statistics3));

			// Edge 4 and some statistics for it
			Edge edge4 = new Edge("customer", "A", "product", "P", "purchase4", "customer4", false, visibilityString2, sixDaysBefore, fiveDaysBefore);
			SetOfStatistics statistics4 = new SetOfStatistics();
			statistics4.addStatistic("countSomething", new Count(123456));
			data.add(new GraphElementWithStatistics(new GraphElement(edge4), statistics4));

			// Edge 5 and some statistics for it
			Edge edge5 = new Edge("customer", "B", "product", "Q", "purchase5", "customer5", true, visibilityString2, sixDaysBefore, fiveDaysBefore);
			SetOfStatistics statistics5 = new SetOfStatistics();
			statistics5.addStatistic("count", new Count(99));
			data.add(new GraphElementWithStatistics(new GraphElement(edge5), statistics5));

			// Entity 1 and some statistics for it
			Entity entity1 = new Entity("customer", "A", "purchase1", "count1", visibilityString1, sevenDaysBefore, sixDaysBefore);
			SetOfStatistics statisticsEntity1 = new SetOfStatistics();
			statisticsEntity1.addStatistic("entity_count", new Count(1000000));
			data.add(new GraphElementWithStatistics(new GraphElement(entity1), statisticsEntity1));

			// Entity 2 and some statistics for it
			Entity entity2 = new Entity("product", "R", "purchase2", "count2", visibilityString1, sevenDaysBefore, sixDaysBefore);
			SetOfStatistics statisticsEntity2 = new SetOfStatistics();
			statisticsEntity2.addStatistic("entity_count", new Count(12345));
			data.add(new GraphElementWithStatistics(new GraphElement(entity2), statisticsEntity2));

			// Create Accumulo backed graph
			AccumuloBackedGraph graph = new AccumuloBackedGraph(conn, tableName);

			// Add data
			graph.addGraphElementsWithStatistics(data);

			// Set graph up for query
			graph.setAuthorizations(new Authorizations(visibilityString1, visibilityString2));
			graph.rollUpOverTimeAndVisibility(useRollUpOverTimeAndVisibilityIterator);
			if (startTimeWindow != null && endTimeWindow != null) {
				graph.setTimeWindow(startTimeWindow, endTimeWindow);
			}
			return graph;
		} catch (AccumuloException e) {
			fail("Failed to set up graph in Accumulo with exception: " + e);
		} catch (AccumuloSecurityException e) {
			fail("Failed to set up graph in Accumulo with exception: " + e);
		} catch (TableExistsException e) {
			fail("Failed to set up graph in Accumulo with exception: " + e);
		} catch (TableNotFoundException e) {
			fail("Failed to set up graph in Accumulo with exception: " + e);
		} catch (GraphAccessException e) {
			fail("Failed to set up graph in Accumulo with exception: " + e);
		}
		return null;
	}

	private static AccumuloBackedGraph setupGraphForSelfEdgeTest(boolean useRollUpOverTimeAndVisibilityIterator, Date startTimeWindow, Date endTimeWindow) {
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

			// Create set of GraphElementWithStatistics to store data before adding it to the graph.
			Set<GraphElementWithStatistics> data = new HashSet<GraphElementWithStatistics>();

			// A directed self-edge and some statistics for it
			Edge edge1 = new Edge("customer", "a", "customer", "a", "purchase", "instore", true, visibilityString1, sevenDaysBefore, sixDaysBefore);
			SetOfStatistics statistics1 = new SetOfStatistics();
			statistics1.addStatistic("count", new Count(1));
			data.add(new GraphElementWithStatistics(new GraphElement(edge1), statistics1));

			// An undirected self-edge and some statistics for it
			Edge edge2 = new Edge("customer", "g", "customer", "g", "purchase", "instore", false, visibilityString1, sevenDaysBefore, sixDaysBefore);
			SetOfStatistics statistics2 = new SetOfStatistics();
			statistics2.addStatistic("count", new Count(10));
			data.add(new GraphElementWithStatistics(new GraphElement(edge2), statistics2));

			// Create Accumulo backed graph
			AccumuloBackedGraph graph = new AccumuloBackedGraph(conn, tableName);

			// Add data
			graph.addGraphElementsWithStatistics(data);

			// Set graph up for query
			graph.setAuthorizations(new Authorizations(visibilityString1, visibilityString2));
			graph.rollUpOverTimeAndVisibility(useRollUpOverTimeAndVisibilityIterator);
			if (startTimeWindow != null && endTimeWindow != null) {
				graph.setTimeWindow(startTimeWindow, endTimeWindow);
			}
			return graph;
		} catch (AccumuloException e) {
			fail("Failed to set up graph in Accumulo with exception: " + e);
		} catch (AccumuloSecurityException e) {
			fail("Failed to set up graph in Accumulo with exception: " + e);
		} catch (TableExistsException e) {
			fail("Failed to set up graph in Accumulo with exception: " + e);
		} catch (TableNotFoundException e) {
			fail("Failed to set up graph in Accumulo with exception: " + e);
		} catch (GraphAccessException e) {
			fail("Failed to set up graph in Accumulo with exception: " + e);
		}
		return null;
	}

	private static AccumuloBackedGraph setupGraphForRangeTest(boolean useRollUpOverTimeAndVisibilityIterator, Date startTimeWindow, Date endTimeWindow) {
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

			// Create set of GraphElementWithStatistics to store data before adding it to the graph.
			Set<GraphElementWithStatistics> data = new HashSet<GraphElementWithStatistics>();

			// Edge 1 and some statistics for it
			Edge edge1 = new Edge("customer", "a", "product", "P", "purchase", "instore", true, visibilityString1, sevenDaysBefore, sixDaysBefore);
			SetOfStatistics statistics1 = new SetOfStatistics();
			statistics1.addStatistic("count", new Count(1));
			data.add(new GraphElementWithStatistics(new GraphElement(edge1), statistics1));

			// Edge 2 and some statistics for it
			Edge edge2 = new Edge("customer", "b1", "product", "P", "purchase", "instore", true, visibilityString1, sevenDaysBefore, sixDaysBefore);
			SetOfStatistics statistics2 = new SetOfStatistics();
			statistics2.addStatistic("count", new Count(2));
			statistics2.addStatistic("anotherCount", new Count(1000000));
			data.add(new GraphElementWithStatistics(new GraphElement(edge2), statistics2));

			// Edge 3 and some statistics for it
			Edge edge3 = new Edge("customer", "b2", "product", "P", "purchase", "instore", true, visibilityString1, sevenDaysBefore, sixDaysBefore);
			SetOfStatistics statistics3 = new SetOfStatistics();
			statistics3.addStatistic("count", new Count(17));
			data.add(new GraphElementWithStatistics(new GraphElement(edge3), statistics3));

			// Edge 4 and some statistics for it
			Edge edge4 = new Edge("customer", "b3", "product", "P", "purchase", "instore", false, visibilityString1, sevenDaysBefore, sixDaysBefore);
			SetOfStatistics statistics4 = new SetOfStatistics();
			statistics4.addStatistic("countSomething", new Count(123456));
			data.add(new GraphElementWithStatistics(new GraphElement(edge4), statistics4));

			// Edge 5 and some statistics for it
			Edge edge5 = new Edge("customer", "b33", "product", "P", "purchase", "instore", false, visibilityString1, sevenDaysBefore, sixDaysBefore);
			SetOfStatistics statistics5 = new SetOfStatistics();
			statistics5.addStatistic("countSomething", new Count(7890123));
			data.add(new GraphElementWithStatistics(new GraphElement(edge5), statistics5));

			// Edge 6 and some statistics for it
			Edge edge6 = new Edge("customer", "b4", "product", "Q", "purchase", "instore", true, visibilityString2, sixDaysBefore, fiveDaysBefore);
			SetOfStatistics statistics6 = new SetOfStatistics();
			statistics6.addStatistic("count", new Count(99));
			data.add(new GraphElementWithStatistics(new GraphElement(edge6), statistics6));

			// Edge 7 and some statistics for it
			Edge edge7 = new Edge("customer", "c", "product", "Q", "purchase", "instore", true, visibilityString2, sixDaysBefore, fiveDaysBefore);
			SetOfStatistics statistics7 = new SetOfStatistics();
			statistics7.addStatistic("count", new Count(100));
			data.add(new GraphElementWithStatistics(new GraphElement(edge7), statistics7));

			// Entity 1 and some statistics for it
			Entity entity1 = new Entity("customer", "a", "purchase", "customer", visibilityString1, sevenDaysBefore, sixDaysBefore);
			SetOfStatistics statisticsEntity1 = new SetOfStatistics();
			statisticsEntity1.addStatistic("count", new Count(128));
			data.add(new GraphElementWithStatistics(new GraphElement(entity1), statisticsEntity1));

			// Create Accumulo backed graph
			AccumuloBackedGraph graph = new AccumuloBackedGraph(conn, tableName);

			// Add data
			graph.addGraphElementsWithStatistics(data);

			// Set graph up for query
			graph.setAuthorizations(new Authorizations(visibilityString1, visibilityString2));
			graph.rollUpOverTimeAndVisibility(useRollUpOverTimeAndVisibilityIterator);
			if (startTimeWindow != null && endTimeWindow != null) {
				graph.setTimeWindow(startTimeWindow, endTimeWindow);
			}
			return graph;
		} catch (AccumuloException e) {
			fail("Failed to set up graph in Accumulo with exception: " + e);
		} catch (AccumuloSecurityException e) {
			fail("Failed to set up graph in Accumulo with exception: " + e);
		} catch (TableExistsException e) {
			fail("Failed to set up graph in Accumulo with exception: " + e);
		} catch (TableNotFoundException e) {
			fail("Failed to set up graph in Accumulo with exception: " + e);
		} catch (GraphAccessException e) {
			fail("Failed to set up graph in Accumulo with exception: " + e);
		}
		return null;
	}

	private static AccumuloBackedGraph setupGraph(boolean useRollUpOverTimeAndVisibilityIterator, Date startTimeWindow, Date endTimeWindow) {
		Instance instance = new MockInstance();
		String tableName = "Test";
		long ageOffTimeInMilliseconds = (30 * 24 * 60 * 60 * 1000L); // 30 days in milliseconds
		// NB: Essential to have the 'L' here so that
		// it is a long (if not then multiplication
		// happens as int and overflows).

		try {
			// Open connection
			Connector conn = instance.getConnector("user", "password");

			// Create table
			// (this method creates the table, removes the versioning iterator, and adds the SetOfStatisticsCombiner iterator,
			// and sets the age off iterator to age data off after it is more than ageOffTimeInMilliseconds milliseconds old).
			TableUtils.createTable(conn, tableName, ageOffTimeInMilliseconds);

			// Create set of GraphElementWithStatistics to store data before adding it to the graph.
			Set<GraphElementWithStatistics> data = new HashSet<GraphElementWithStatistics>();

			// Edge 1 and some statistics for it
			Edge edge1 = new Edge("customer", "A", "product", "P", "purchase", "instore", true, visibilityString1, sevenDaysBefore, sixDaysBefore);
			SetOfStatistics statistics1 = new SetOfStatistics();
			statistics1.addStatistic("count", new Count(1));
			data.add(new GraphElementWithStatistics(new GraphElement(edge1), statistics1));

			// Edge 2 and some statistics for it
			Edge edge2 = new Edge("customer", "A", "product", "P", "purchase", "instore", true, visibilityString1, sixDaysBefore, fiveDaysBefore);
			SetOfStatistics statistics2 = new SetOfStatistics();
			statistics2.addStatistic("count", new Count(2));
			statistics2.addStatistic("anotherCount", new Count(1000000));
			data.add(new GraphElementWithStatistics(new GraphElement(edge2), statistics2));

			// Edge 3 and some statistics for it
			Edge edge3 = new Edge("customer", "A", "product", "P", "purchase", "instore", true, visibilityString2, sixDaysBefore, fiveDaysBefore);
			SetOfStatistics statistics3 = new SetOfStatistics();
			statistics3.addStatistic("count", new Count(17));
			data.add(new GraphElementWithStatistics(new GraphElement(edge3), statistics3));

			// Edge 4 and some statistics for it
			Edge edge4 = new Edge("customer", "A", "product", "P", "purchase", "instore", false, visibilityString2, sixDaysBefore, fiveDaysBefore);
			SetOfStatistics statistics4 = new SetOfStatistics();
			statistics4.addStatistic("countSomething", new Count(123456));
			data.add(new GraphElementWithStatistics(new GraphElement(edge4), statistics4));

			// Edge 5 and some statistics for it
			Edge edge5 = new Edge("customer", "B", "product", "Q", "purchase", "instore", true, visibilityString2, sixDaysBefore, fiveDaysBefore);
			SetOfStatistics statistics5 = new SetOfStatistics();
			statistics5.addStatistic("count", new Count(99));
			data.add(new GraphElementWithStatistics(new GraphElement(edge5), statistics5));

			// Entity 1 and some statistics for it
			Entity entity1 = new Entity("customer", "A", "purchase", "count", visibilityString1, sevenDaysBefore, sixDaysBefore);
			SetOfStatistics statisticsEntity1 = new SetOfStatistics();
			statisticsEntity1.addStatistic("entity_count", new Count(1000000));
			data.add(new GraphElementWithStatistics(new GraphElement(entity1), statisticsEntity1));

			// Entity 2 and some statistics for it
			Entity entity2 = new Entity("product", "R", "purchase", "count", visibilityString1, sevenDaysBefore, sixDaysBefore);
			SetOfStatistics statisticsEntity2 = new SetOfStatistics();
			statisticsEntity2.addStatistic("entity_count", new Count(12345));
			data.add(new GraphElementWithStatistics(new GraphElement(entity2), statisticsEntity2));

			// Create Accumulo backed graph
			AccumuloBackedGraph graph = new AccumuloBackedGraph(conn, tableName);

			// Add data
			graph.addGraphElementsWithStatistics(data);

			// Set graph up for query
			graph.setAuthorizations(new Authorizations(visibilityString1, visibilityString2));
			graph.rollUpOverTimeAndVisibility(useRollUpOverTimeAndVisibilityIterator);
			if (startTimeWindow != null && endTimeWindow != null) {
				graph.setTimeWindow(startTimeWindow, endTimeWindow);
			}
			return graph;
		} catch (AccumuloException e) {
			fail("Failed to set up graph in Accumulo with exception: " + e);
		} catch (AccumuloSecurityException e) {
			fail("Failed to set up graph in Accumulo with exception: " + e);
		} catch (TableExistsException e) {
			fail("Failed to set up graph in Accumulo with exception: " + e);
		} catch (TableNotFoundException e) {
			fail("Failed to set up graph in Accumulo with exception: " + e);
		} catch (GraphAccessException e) {
			fail("Failed to set up graph in Accumulo with exception: " + e);
		}
		return null;
	}

}
