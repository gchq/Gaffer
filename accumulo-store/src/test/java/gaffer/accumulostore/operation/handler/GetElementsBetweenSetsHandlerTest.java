/*
 * Copyright 2016 Crown Copyright
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

package gaffer.accumulostore.operation.handler;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.Before;
import org.junit.Test;

import gaffer.accumulostore.AccumuloStore;
import gaffer.accumulostore.MockAccumuloStoreForTest;
import gaffer.accumulostore.key.core.impl.byteEntity.ByteEntityKeyPackage;
import gaffer.accumulostore.key.core.impl.classic.ClassicKeyPackage;
import gaffer.accumulostore.operation.impl.GetElementsBetweenSets;
import gaffer.accumulostore.utils.AccumuloPropertyNames;
import gaffer.accumulostore.utils.Constants;
import gaffer.commonutil.TestGroups;
import gaffer.data.element.Edge;
import gaffer.data.element.Element;
import gaffer.data.element.Entity;
import gaffer.data.elementdefinition.view.View;
import gaffer.data.elementdefinition.view.ViewEdgeDefinition;
import gaffer.data.elementdefinition.view.ViewEntityDefinition;
import gaffer.operation.GetOperation.IncludeEdgeType;
import gaffer.operation.GetOperation.IncludeIncomingOutgoingType;
import gaffer.operation.OperationChain;
import gaffer.operation.OperationException;
import gaffer.operation.data.EntitySeed;
import gaffer.operation.impl.add.AddElements;
import gaffer.store.StoreException;

public class GetElementsBetweenSetsHandlerTest {
	
	private static final String AUTHS = "Test";
	private static final long TIMESTAMP = System.currentTimeMillis();
	// Query for all edges between the set {A0} and the set {A23}
    private static final List<EntitySeed> seedsA = Arrays.asList(new EntitySeed("A0"));
    private static final List<EntitySeed> seedsB = Arrays.asList(new EntitySeed("A23"));

	private static View defaultView;
	private static AccumuloStore byteEntityStore;
	private static AccumuloStore Gaffer1KeyStore;
	private static Element EXPECTED_EDGE_1 = new Edge(TestGroups.EDGE, "A0", "A23", true);
	private static Element EXPECTED_EDGE_2 = new Edge(TestGroups.EDGE, "A0", "A23", true);    
	private static Element EXPECTED_EDGE_3 = new Edge(TestGroups.EDGE, "A0", "A23", true);  
    private static Element EXPECTED_ENTITY_1 = new Entity(TestGroups.ENTITY, "A0");
    private static Element EXPECTED_SUMMARAISED_EDGE = new Edge(TestGroups.EDGE, "A0", "A23", true);

    static {
    	 EXPECTED_EDGE_1.putProperty(AccumuloPropertyNames.COLUMN_QUALIFIER, 1);
    	 EXPECTED_EDGE_1.putProperty(AccumuloPropertyNames.COUNT, 23);
    	 EXPECTED_EDGE_1.putProperty(AccumuloPropertyNames.TIMESTAMP, TIMESTAMP);
    	 EXPECTED_EDGE_2.putProperty(AccumuloPropertyNames.COLUMN_QUALIFIER, 2);
         EXPECTED_EDGE_2.putProperty(AccumuloPropertyNames.COUNT, 23);
    	 EXPECTED_EDGE_2.putProperty(AccumuloPropertyNames.TIMESTAMP, TIMESTAMP);
         EXPECTED_EDGE_3.putProperty(AccumuloPropertyNames.COLUMN_QUALIFIER, 3);
         EXPECTED_EDGE_3.putProperty(AccumuloPropertyNames.COUNT, 23);
    	 EXPECTED_EDGE_3.putProperty(AccumuloPropertyNames.TIMESTAMP, TIMESTAMP);
    	 EXPECTED_ENTITY_1.putProperty(AccumuloPropertyNames.COUNT, 10000);
    	 EXPECTED_ENTITY_1.putProperty(AccumuloPropertyNames.TIMESTAMP, TIMESTAMP);
    	 EXPECTED_SUMMARAISED_EDGE.putProperty(AccumuloPropertyNames.COLUMN_QUALIFIER, 1 * 3);
    	 EXPECTED_SUMMARAISED_EDGE.putProperty(AccumuloPropertyNames.COUNT, 23 * 3);
    	 EXPECTED_SUMMARAISED_EDGE.putProperty(AccumuloPropertyNames.TIMESTAMP, TIMESTAMP);   
    }
	
	
	@Before
	public void setup() throws StoreException, IOException {
	    byteEntityStore = new MockAccumuloStoreForTest(ByteEntityKeyPackage.class);
	    Gaffer1KeyStore = new MockAccumuloStoreForTest(ClassicKeyPackage.class);
	
	    defaultView = new View.Builder().edge(TestGroups.EDGE, new ViewEdgeDefinition()).entity(TestGroups.ENTITY, new ViewEntityDefinition()).build();
	}
	
	@Test
	public void testNoSummarisation() throws StoreException {
		testNoSummarisation(byteEntityStore);
		testNoSummarisation(Gaffer1KeyStore);
	}
	
	private void testNoSummarisation(final AccumuloStore store) throws StoreException {
		// Create mock Accumulo instance and table
	    // Populate graph
	    setupGraph(store);
		
		GetElementsBetweenSets<Element> op = new GetElementsBetweenSets<>(seedsA, seedsB, defaultView);
        op.addOption(Constants.OPERATION_AUTHORISATIONS, AUTHS);
        GetElementsBetweenSetsHandler handler = new GetElementsBetweenSetsHandler();
        Iterable<Element> elements = handler.doOperation(op, store);
        List<Element> results = new ArrayList<>();
        for (Element elm : elements) {
            results.add(elm);
        }
        List<Element> expectedResults = new ArrayList<>();
        
        expectedResults.add(EXPECTED_EDGE_1);
        expectedResults.add(EXPECTED_EDGE_2);
        expectedResults.add(EXPECTED_EDGE_2);
        expectedResults.add(EXPECTED_ENTITY_1);
 
        for (Element expectedResult : expectedResults) {
            assertTrue(results.contains(expectedResult));
        }
       
        //Without query compaction the result size should be 4
        assertEquals(4, results.size());
        
	}
	
	@Test
	public void testShouldSummarise() throws StoreException {
		testShouldSummarise(byteEntityStore);
		testShouldSummarise(Gaffer1KeyStore);
	}
	
	public void testShouldSummarise(final AccumuloStore store) throws StoreException {
		// Create mock Accumulo instance and table
	    // Populate graph
	    setupGraph(store);
		
	    GetElementsBetweenSets<Element> op = new GetElementsBetweenSets<>(seedsA, seedsB, defaultView);
        op.setSummarise(true);
        op.addOption(Constants.OPERATION_AUTHORISATIONS, AUTHS);
        GetElementsBetweenSetsHandler handler = new GetElementsBetweenSetsHandler();
        Iterable<Element> elements = handler.doOperation(op, store);
        List<Element> results = new ArrayList<>();
        for (Element elm : elements) {
            results.add(elm);
        }
        List<Element> expectedResults = new ArrayList<>();
        expectedResults.add(EXPECTED_SUMMARAISED_EDGE);
        expectedResults.add(EXPECTED_ENTITY_1);

        
        for (Element expectedResult : expectedResults) {
            assertTrue(results.contains(expectedResult));
        }
       
        //With query compaction the result size should be 2
        assertEquals(2, results.size());

	}
	
	@Test
	public void testShouldReturnOnlyEdgesWhenOptionSet() throws StoreException {
		testShouldReturnOnlyEdgesWhenOptionSet(byteEntityStore);
		testShouldReturnOnlyEdgesWhenOptionSet(Gaffer1KeyStore);
	}
	
	public void testShouldReturnOnlyEdgesWhenOptionSet(final AccumuloStore store) throws StoreException {
		// Create mock Accumulo instance and table
	    // Populate graph
	    setupGraph(store);

        GetElementsBetweenSets<Element> op = new GetElementsBetweenSets<>(seedsA, seedsB, defaultView);
        op.setSummarise(true);
        op.setIncludeEdges(IncludeEdgeType.ALL);
        op.setIncludeEntities(false);
        op.addOption(Constants.OPERATION_AUTHORISATIONS, AUTHS);
        GetElementsBetweenSetsHandler handler = new GetElementsBetweenSetsHandler();
        Iterable<Element> elements = handler.doOperation(op, store);
        List<Element> results = new ArrayList<>();
        for (Element elm : elements) {
            results.add(elm);
        }
        List<Element> expectedResults = new ArrayList<>();
        expectedResults.add(EXPECTED_SUMMARAISED_EDGE);
        
        for (Element expectedResult : expectedResults) {
            assertTrue(results.contains(expectedResult));
        }
       
        //With query compaction the result size should be 1
        assertEquals(1, results.size());
        
        assertEquals(expectedResults, results);
	}
	
	@Test
	public void testShouldReturnOnlyEntitiesWhenOptionSet() throws StoreException {
		testShouldReturnOnlyEntitiesWhenOptionSet(byteEntityStore);
		testShouldReturnOnlyEntitiesWhenOptionSet(Gaffer1KeyStore);
	}
	
	public void testShouldReturnOnlyEntitiesWhenOptionSet(final AccumuloStore store) throws StoreException {
		// Create mock Accumulo instance and table
	    // Populate graph
	    setupGraph(store);
		
	    GetElementsBetweenSets<Element> op = new GetElementsBetweenSets<>(seedsA, seedsB, defaultView);
        op.setIncludeEdges(IncludeEdgeType.NONE);
        op.addOption(Constants.OPERATION_AUTHORISATIONS, AUTHS);
        GetElementsBetweenSetsHandler handler = new GetElementsBetweenSetsHandler();
        Iterable<Element> elements = handler.doOperation(op, store);
        List<Element> results = new ArrayList<>();
        for (Element elm : elements) {
            results.add(elm);
        }
        List<Element> expectedResults = new ArrayList<>();
        expectedResults.add(EXPECTED_ENTITY_1);
        for (Element expectedResult : expectedResults) {
            assertTrue(results.contains(expectedResult));
        }
       
        //The result size should be 1
        assertEquals(1, results.size());
        
        assertEquals(expectedResults, results);
	}
	
	 public void testShouldSummariseOutGoingEdgesOnly() throws StoreException {
    	testShouldSummariseOutGoingEdgesOnly(byteEntityStore);
    	testShouldSummariseOutGoingEdgesOnly(Gaffer1KeyStore);
    }
    
    public void testShouldSummariseOutGoingEdgesOnly(final AccumuloStore store) throws StoreException {
    	// Create mock Accumulo instance and table
	    // Populate graph
	    setupGraph(store);
	
	    
        GetElementsBetweenSets<Element> op = new GetElementsBetweenSets<>(seedsA, seedsB, defaultView);
        op.setSummarise(true);
        op.setIncludeIncomingOutGoing(IncludeIncomingOutgoingType.OUTGOING);
        op.addOption(Constants.OPERATION_AUTHORISATIONS, AUTHS);
        GetElementsBetweenSetsHandler handler = new GetElementsBetweenSetsHandler();
        Iterable<Element> elements = handler.doOperation(op, store);
        List<Element> results = new ArrayList<>();
        for (Element elm : elements) {
            results.add(elm);
        }
        List<Element> expectedResults = new ArrayList<>();
        expectedResults.add(EXPECTED_SUMMARAISED_EDGE);
        expectedResults.add(EXPECTED_ENTITY_1);
        
        for (Element expectedResult : expectedResults) {
            assertTrue(results.contains(expectedResult));
        }

        //With query compaction the result size should be 2
        assertEquals(2, results.size());
        
        assertEquals(expectedResults, results);
    }
    
    @Test
    public void testShouldHaveNoIncomingEdges() throws StoreException {
    	testShouldHaveNoIncomingEdges(byteEntityStore);
    	testShouldHaveNoIncomingEdges(Gaffer1KeyStore);
    }
    
    public void testShouldHaveNoIncomingEdges(final AccumuloStore store) throws StoreException {
    	// Create mock Accumulo instance and table
	    // Populate graph
	    setupGraph(store);
		
        GetElementsBetweenSets<Element> op = new GetElementsBetweenSets<>(seedsA, seedsB, defaultView);
        op.setSummarise(true);
        op.setIncludeIncomingOutGoing(IncludeIncomingOutgoingType.INCOMING);
        op.addOption(Constants.OPERATION_AUTHORISATIONS, AUTHS);
        GetElementsBetweenSetsHandler handler = new GetElementsBetweenSetsHandler();
        Iterable<Element> elements = handler.doOperation(op, store);
        List<Element> results = new ArrayList<>();
        for (Element elm : elements) {
            results.add(elm);
        }
        List<Element> expectedResults = new ArrayList<>();
        expectedResults.add(EXPECTED_ENTITY_1);
        
        for (Element expectedResult : expectedResults) {
            assertTrue(results.contains(expectedResult));
        }
        //The result size should be 1
        assertEquals(1, results.size());
        
        assertEquals(expectedResults, results);

    }
	
	private static void setupGraph(final AccumuloStore store) {
        List<Element> data = new ArrayList<>();

        // Create edges A0 -> A1, A0 -> A2, ..., A0 -> A99. Also create an Entity for each.
        Entity entity = new Entity(TestGroups.ENTITY, "A0");
        entity.putProperty(AccumuloPropertyNames.COUNT, 10000);
        entity.putProperty(AccumuloPropertyNames.TIMESTAMP, TIMESTAMP);
        data.add(entity);
        for (int i = 1; i < 100; i++) {
            Edge edge = new Edge(TestGroups.EDGE, "A0", "A" + i, true);
            edge.putProperty(AccumuloPropertyNames.COUNT, 1);
            edge.putProperty(AccumuloPropertyNames.COLUMN_QUALIFIER, 1);
            edge.putProperty(AccumuloPropertyNames.TIMESTAMP, TIMESTAMP);
            Edge edge2 = new Edge(TestGroups.EDGE, "A0", "A" + i, true);
            edge2.putProperty(AccumuloPropertyNames.COUNT, 1);
            edge2.putProperty(AccumuloPropertyNames.COLUMN_QUALIFIER, 2);
            edge2.putProperty(AccumuloPropertyNames.TIMESTAMP, TIMESTAMP);
            Edge edge3 = new Edge(TestGroups.EDGE, "A0", "A" + i, true);
            edge3.putProperty(AccumuloPropertyNames.COUNT, 1);
            edge3.putProperty(AccumuloPropertyNames.COLUMN_QUALIFIER, 3);
            edge3.putProperty(AccumuloPropertyNames.TIMESTAMP, TIMESTAMP);
            data.add(edge);
            data.add(edge2);
            data.add(edge3);
            entity = new Entity(TestGroups.ENTITY, "A" + i);
            entity.putProperty(AccumuloPropertyNames.COUNT, i);
            entity.putProperty(AccumuloPropertyNames.TIMESTAMP, TIMESTAMP);
            data.add(entity);
        }
        addElements(data, store);
    }


    private static void addElements(final Iterable<Element> data, final AccumuloStore store) {
        AddElements add = new AddElements(data);
        add.addOption(Constants.OPERATION_AUTHORISATIONS, AUTHS);
        try {
            store.execute(new OperationChain<>(add));
        } catch (OperationException e) {
            fail("Failed to set up graph in Accumulo with exception: " + e);
        }
    }
}