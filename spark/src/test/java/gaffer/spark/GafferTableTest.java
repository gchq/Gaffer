/*
 * Copyright 2016 Crown Copyright
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

package gaffer.spark;

import static org.junit.Assert.assertEquals;
import org.junit.Test;
import org.junit.BeforeClass;
import org.junit.AfterClass;

import gaffer.accumulostore.AccumuloStore;
import gaffer.accumulostore.key.exception.RangeFactoryException;
import gaffer.accumulostore.utils.Pair;
import gaffer.data.element.Edge;
import gaffer.data.element.Element;
import gaffer.data.element.Entity;
import gaffer.data.element.Properties;
import gaffer.operation.OperationException;
import gaffer.store.StoreException;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import scala.Tuple2;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

/**
*
*/
public class GafferTableTest {

	private static JavaSparkContext sc;
	private static GafferTable table;	
	protected static GafferTableData data;

	@BeforeClass
	public static void beforeAll() throws OperationException, AccumuloException, AccumuloSecurityException, StoreException {	  
		Logger.getLogger("org").setLevel(Level.ERROR);
		Logger.getLogger("akka").setLevel(Level.ERROR);
		data = new GafferTableData();
		sc = new JavaSparkContext(new SparkConf().setAppName("gafferjavatest").setMaster("local")
		        .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
		        .set("spark.kryo.registrator", "gaffer.spark.GafferRegistrator")); 	
		table = new GafferTable((AccumuloStore) data.getStore(), sc);
	}

	@AfterClass
	public static void afterAll() {
		sc.stop();
		System.clearProperty("spark.master.port");
		data.stopCluster();
	}

	@Test
	public void returnEntireTable() {
		Set<Tuple2<Element, Properties>> set = new HashSet<>(table.query().collect());
		assertEquals(set, data.expectedOutput);
	}
	@Test
	public void onlyEntities() {
		Set<Tuple2<Element, Properties>> set = new HashSet<>(table.onlyEntities().query().collect());
		assertEquals(set, data.entityExpectedOutput);
	}

	@Test
	public void onlyEdges() {
		Set<Tuple2<Element, Properties>> set = new HashSet<>(table.onlyEdges().query().collect());
		assertEquals(set, data.edgeExpectedOutput);
	}

	@Test
	public void entitiesAndEdges() {
		Set<Tuple2<Element, Properties>> set = new HashSet<>(table.entitiesAndEdges().query().collect());
		assertEquals(set, data.expectedOutput);
	}
	
	@Test
	public void defaultRollup() {
		Set<Tuple2<Element, Properties>> set = new HashSet<>(table.query().collect());
		Set<Tuple2<Element, Properties>> set2 = new HashSet<>(table.withRollup().query().collect());
		assertEquals(set, set2);
	}

	@Test
	public void withoutRollup() {
		Set<Tuple2<Element, Properties>> set = new HashSet<>(table.withoutRollup().query().collect());
		assertEquals(set, data.expectedUnrolledOutput);
	}

	@Test
	public void revertToRollup() {
		Set<Tuple2<Element, Properties>> set = new HashSet<>(table.withoutRollup().withRollup().query().collect());
		assertEquals(set, data.expectedOutput);
	}

	
	@Test
	public void elementWithVertex() throws RangeFactoryException {
		Object vertex1 = "filmA";
		Set<Tuple2<Element, Properties>> vertexResults = new HashSet<>();
		for(Tuple2<Element, Properties> t : data.expectedOutput) {
			if(t._1 instanceof Entity) {
				Entity e  = (Entity) t._1;
				if(e.getVertex().equals(vertex1))
					vertexResults.add(t);		
			}
			else if(t._1 instanceof Edge) {
				Edge e  = (Edge) t._1;
				if(e.getSource().equals(vertex1) || e.getDestination().equals(vertex1))
					vertexResults.add(t);		
			}
		}
		Set<Tuple2<Element, Properties>> set = new HashSet<>(table.query(vertex1).collect());    
		assertEquals(set, vertexResults);
		
		Object vertex2 = "filmB";
		for(Tuple2<Element, Properties> t : data.expectedOutput) {
			if(t._1 instanceof Entity) {
				Entity e  = (Entity) t._1;
				if(e.getVertex().equals(vertex2))
					vertexResults.add(t);		
			}
			else if(t._1 instanceof Edge) {
				Edge e  = (Edge) t._1;
				if(e.getSource().equals(vertex2) || e.getDestination().equals(vertex2))
					vertexResults.add(t);		
			}
		}
		Set<Tuple2<Element, Properties>> set2 = 
				new HashSet<>(table.query(new HashSet<Object>(Arrays.asList(vertex1, vertex2))).collect());    
		assertEquals(set2, vertexResults);
	}
	
	public boolean inRange(String vertex1, String vertex2, Element el) {
		if (el instanceof Edge) {
			Edge edge = (Edge) el;
			String source = (String) edge.getSource();
			String destination = (String) edge.getDestination();
			if(((source.compareTo(vertex1) > 0 || (source.equals(vertex1)))) &
					(source.compareTo(vertex2) < 0 || (source.equals(vertex2))) ||
			((destination.compareTo(vertex1) > 0 || (destination.equals(vertex1))) &
					(destination.compareTo(vertex2) < 0 || (destination.equals(vertex2)))))
			return true;
		} else if (el instanceof Entity){
			Entity entity = (Entity) el;
			String vertex = (String) entity.getVertex();
			if(((vertex.compareTo(vertex1) > 0 || (vertex.equals(vertex1)))) &
			(vertex.compareTo(vertex2)) < 0 || (vertex.equals(vertex2)))
			return true;
		}
		return false;
	}

	@Test
	public void elementsWithinRange() throws RangeFactoryException {
		String vertex1 = "user01";
		String vertex2 = "user02";
		
		Set<Tuple2<Element, Properties>> rangeResults = new HashSet<>();
		for(Tuple2<Element, Properties> t : data.expectedOutput) {
			if(inRange(vertex1, vertex2, t._1))
				rangeResults.add(t);
		}

		Set<Tuple2<Element, Properties>> set = 
				new HashSet<>(table.rangeQuery(new Pair<Object>(vertex1, vertex2)).collect());

		assertEquals(set, rangeResults);
	}
	
	@Test
	public void edgesForPair() throws RangeFactoryException {
		Object value1 = "filmC";
		Object value2 = "user02";

		Set<Tuple2<Element, Properties>> pairResults = new HashSet<>();
		for(Tuple2<Element, Properties> t : data.expectedOutput) {
			if(t._1 instanceof Edge) {
				Edge edge = (Edge) t._1;
				if((edge.getSource().equals(value1) & edge.getDestination().equals(value2)) ||
						edge.getSource().equals(value2) & edge.getDestination().equals(value1))
					pairResults.add(t);
			}
		}
		Tuple2<Object, Object> pair = new Tuple2<>(value1, value2);
		Set<Tuple2<Element, Properties>> set = new HashSet<>(table.pairQuery(pair).collect());

		assertEquals(set, pairResults);
		
		Object value3 = "filmA";
		Object value4 = "user01";
		for(Tuple2<Element, Properties> t : data.expectedOutput) {
			if(t._1 instanceof Edge) {
				Edge edge = (Edge) t._1;
				if((edge.getSource().equals(value3) & edge.getDestination().equals(value4)) ||
						edge.getSource().equals(value4) & edge.getDestination().equals(value3))
					pairResults.add(t);
			}
		}
		Tuple2<Object, Object> pair2 = new Tuple2<>(value3, value4);
		
		Set<Tuple2<Element, Properties>> set2 = 
				new HashSet<>(table.pairQuery(new HashSet<Tuple2<Object, Object>>(Arrays.asList(pair, pair2))).collect());
		assertEquals(set2, pairResults);
	}
	
	@Test
	public void elementsOfGroup() {
		String group1 = "user";
		Set<Tuple2<Element, Properties>> groupResults = new HashSet<>();
		for(Tuple2<Element, Properties> t : data.expectedOutput) {
			if(t._1.getGroup().equals(group1)){
				groupResults.add(t);
			}
		}
		Set<Tuple2<Element, Properties>> set = new HashSet<>(table.onlyGroups(group1).query().collect());
		assertEquals(set, groupResults);
		
		String group2 = "review";
		for(Tuple2<Element, Properties> t : data.expectedOutput) {
			if(t._1.getGroup().equals(group2)){
				groupResults.add(t);
			}
		}
		Set<Tuple2<Element, Properties>> set2 = 
				new HashSet<>(table.onlyGroups(new HashSet<String>(Arrays.asList(group1, group2))).query().collect());
		assertEquals(set2, groupResults);
	}
	
	@Test
	public void elementsWithinDates() {
		String prop = "startTime";
		long rangeStart = 1401000000000L;
		long rangeEnd = 1408000000000L;

		Set<Tuple2<Element, Properties>> inRangeResults = new HashSet<>();
		for(Tuple2<Element, Properties> t : data.expectedOutput) {
			if(t._2.containsKey(prop)) {
				long startTime = (long) t._2.get(prop);
				if(startTime > rangeStart & startTime < rangeEnd) 
					inRangeResults.add(t);
			}
		}	
		Set<Tuple2<Element, Properties>> set = new HashSet<>(table.between(prop, rangeStart, rangeEnd).query().collect());
		assertEquals(set, inRangeResults);
	}

	@Test
	public void elementsAfterDate() {
		String prop = "startTime";
		long start = 1402000000000L;
		Set<Tuple2<Element, Properties>> afterDateResults = new HashSet<>();
		for(Tuple2<Element, Properties> t : data.expectedOutput) {
			if(t._2.containsKey(prop)) {
				long startTime = (long) t._2.get(prop);
				if(startTime > start) 
					afterDateResults.add(t);
			}
		}
		
		Set<Tuple2<Element, Properties>> set = new HashSet<>(table.after(prop, start).query().collect());
		assertEquals(set, afterDateResults);

	}

	@Test
	public void elementsBeforeDate() {
		String prop = "startTime";
		long end = 1408000000000L;
		Set<Tuple2<Element, Properties>> beforeDateResults = new HashSet<>();
		for(Tuple2<Element, Properties> t : data.expectedOutput) {
			if(t._2.containsKey(prop)) {
				long startTime = (long) t._2.get(prop);
				if(startTime < end) 
					beforeDateResults.add(t);
			}
		}
		
		Set<Tuple2<Element, Properties>> set = new HashSet<>(table.before(prop, end).query().collect());
		assertEquals(set, beforeDateResults);

	}	
}
