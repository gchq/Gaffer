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

package uk.gov.gchq.gaffer.federatedstore;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import uk.gov.gchq.gaffer.federatedstore.operation.GetAllGraphIds;
import uk.gov.gchq.gaffer.mapstore.MapStoreProperties;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.library.HashMapGraphLibrary;
import uk.gov.gchq.gaffer.store.schema.Schema;

public class FederatedStorePublicAccessTest {


    public static final String GRAPH_1 = "graph1";
    public static final String PROP_1 = "prop1";
    public static final String SCHEMA_1 = "schema1";
    private FederatedStore store;
    private FederatedStoreProperties fedProps;
    private HashMapGraphLibrary library;
    private Context blankContext;

    @Before
    public void setUp() throws Exception {
        fedProps = new FederatedStoreProperties();
        fedProps.setGraphIds(GRAPH_1);
        fedProps.setGraphPropId(GRAPH_1, PROP_1);
        fedProps.setGraphSchemaId(GRAPH_1, SCHEMA_1);

        store = new FederatedStore();
        library = new HashMapGraphLibrary();
        HashMapGraphLibrary.clear();

        MapStoreProperties mapStoreProperties = new MapStoreProperties();
        mapStoreProperties.setId(PROP_1);

        library.addProperties(mapStoreProperties);
        library.addSchema(new Schema.Builder().id(SCHEMA_1).build());
        store.setGraphLibrary(library);
        blankContext = new Context(FederatedStoreUser.blankUser());
    }

    @Test
    public void shouldNotBePublicWhenAllGraphsDefaultedPrivateAndGraphIsDefaultedPrivate() throws Exception {
        store.initialise("testFedStore", null, fedProps);
        final Iterable<? extends String> execute = store.execute(new GetAllGraphIds(), blankContext);
        Assert.assertFalse(execute.iterator().hasNext());
    }


    @Test
    public void shouldBePublicWhenAllGraphsDefaultedPrivateAndGraphIsSetPublic() throws Exception {
        fedProps.setTrueGraphIsPublicValue(GRAPH_1);
        store.initialise("testFedStore", null, fedProps);
        final Iterable<? extends String> execute = store.execute(new GetAllGraphIds(), blankContext);
        Assert.assertTrue(execute.iterator().hasNext());
    }

    @Test
    public void shouldNotBePublicWhenAllGraphsDefaultedPrivateAndGraphIsSetPrivate() throws Exception {
        fedProps.setFalseGraphIsPublicValue(GRAPH_1);
        store.initialise("testFedStore", null, fedProps);
        final Iterable<? extends String> execute = store.execute(new GetAllGraphIds(), blankContext);
        Assert.assertFalse(execute.iterator().hasNext());
    }

    @Test
    public void shouldNotBePublicWhenAllGraphsSetPrivateAndGraphIsSetPublic() throws Exception {
        fedProps.setFalseGraphsCanHavePublicAccess();
        fedProps.setTrueGraphIsPublicValue(GRAPH_1);
        store.initialise("testFedStore", null, fedProps);
        final Iterable<? extends String> execute = store.execute(new GetAllGraphIds(), blankContext);
        Assert.assertFalse(execute.iterator().hasNext());
    }

    @Test
    public void shouldNotBePublicWhenAllGraphsSetPrivateAndGraphIsSetPrivate() throws Exception {
        fedProps.setFalseGraphsCanHavePublicAccess();
        fedProps.setFalseGraphIsPublicValue(GRAPH_1);
        store.initialise("testFedStore", null, fedProps);
        final Iterable<? extends String> execute = store.execute(new GetAllGraphIds(), blankContext);
        Assert.assertFalse(execute.iterator().hasNext());
    }


    @Test
    public void shouldNotBePublicWhenAllGraphsSetPublicAndGraphIsSetPrivate() throws Exception {
        fedProps.setTrueGraphsCanHavePublicAccess();
        fedProps.setFalseGraphIsPublicValue(GRAPH_1);
        store.initialise("testFedStore", null, fedProps);
        final Iterable<? extends String> execute = store.execute(new GetAllGraphIds(), blankContext);
        Assert.assertFalse(execute.iterator().hasNext());
    }

    @Test
    public void shouldBePublicWhenAllGraphsSetPublicAndGraphIsSetPublic() throws Exception {
        fedProps.setTrueGraphsCanHavePublicAccess();
        fedProps.setTrueGraphIsPublicValue(GRAPH_1);
        store.initialise("testFedStore", null, fedProps);
        final Iterable<? extends String> execute = store.execute(new GetAllGraphIds(), blankContext);
        Assert.assertTrue(execute.iterator().hasNext());
    }

}
