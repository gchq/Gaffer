///*
// * Copyright 2016 Crown Copyright
// *
// * Licensed under the Apache License, Version 2.0 (the "License");
// * you may not use this file except in compliance with the License.
// * You may obtain a copy of the License at
// *
// *     http://www.apache.org/licenses/LICENSE-2.0
// *
// * Unless required by applicable law or agreed to in writing, software
// * distributed under the License is distributed on an "AS IS" BASIS,
// * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// * See the License for the specific language governing permissions and
// * limitations under the License.
// */
//
//package gaffer.store;
//
//import static gaffer.store.StoreTrait.AGGREGATION;
//import static gaffer.store.StoreTrait.FILTERING;
//import static gaffer.store.StoreTrait.TRANSFORMATION;
//import static junit.framework.Assert.assertEquals;
//import static org.junit.Assert.assertNotNull;
//import static org.junit.Assert.assertSame;
//import static org.junit.Assert.assertTrue;
//import static org.junit.Assert.fail;
//import static org.mockito.BDDMockito.given;
//import static org.mockito.Mockito.mock;
//import static org.mockito.Mockito.times;
//import static org.mockito.Mockito.verify;
//
//import com.google.common.collect.Sets;
//import gaffer.commonutil.TestGroups;
//import gaffer.commonutil.TestPropertyNames;
//import gaffer.data.element.Element;
//import gaffer.data.element.IdentifierType;
//import gaffer.data.elementdefinition.schema.exception.SchemaException;
//import gaffer.operation.Operation;
//import gaffer.operation.OperationChain;
//import gaffer.operation.Validatable;
//import gaffer.operation.data.ElementSeed;
//import gaffer.operation.data.EntitySeed;
//import gaffer.operation.impl.Validate;
//import gaffer.operation.impl.add.AddElements;
//import gaffer.operation.impl.generate.GenerateElements;
//import gaffer.operation.impl.generate.GenerateObjects;
//import gaffer.operation.impl.get.GetAdjacentEntitySeeds;
//import gaffer.operation.impl.get.GetEdgesBySeed;
//import gaffer.operation.impl.get.GetElements;
//import gaffer.operation.impl.get.GetElementsSeed;
//import gaffer.operation.impl.get.GetEntitiesBySeed;
//import gaffer.operation.impl.get.GetRelatedElements;
//import gaffer.operation.impl.get.GetRelatedEntities;
//import gaffer.store.operation.handler.GenerateElementsHandler;
//import gaffer.store.operation.handler.GenerateObjectsHandler;
//import gaffer.store.operation.handler.OperationHandler;
//import gaffer.store.schema.DataEdgeDefinition;
//import gaffer.store.schema.DataElementDefinition;
//import gaffer.store.schema.DataEntityDefinition;
//import gaffer.store.schema.DataSchema;
//import org.junit.Assert;
//import org.junit.Before;
//import org.junit.Test;
//import org.mockito.Mockito;
//import java.util.ArrayList;
//import java.util.Arrays;
//import java.util.Collection;
//import java.util.HashMap;
//import java.util.List;
//import java.util.Map;
//
//public class StoreTest {
//    private OperationHandler<AddElements, Void> addElementsHandler;
//    private OperationHandler<GetElements<ElementSeed, Element>, Iterable<Element>> getElementsHandler;
//    private OperationHandler<GetAdjacentEntitySeeds, Iterable<EntitySeed>> getAdjacentEntitySeedsHandler;
//    private OperationHandler<Validatable<Integer>, Integer> validatableHandler;
//    private OperationHandler<Validate, Iterable<Element>> validateHandler;
//    private DataSchema dataSchema;
//    private DataElementDefinition storeElementDef;
//    private DataEdgeDefinition dataEdgeDef;
//    private DataEntityDefinition dataEntityDef;
//
//    @Before
//    public void setup() {
//        addElementsHandler = mock(OperationHandler.class);
//        getElementsHandler = mock(OperationHandler.class);
//        getAdjacentEntitySeedsHandler = mock(OperationHandler.class);
//        validatableHandler = mock(OperationHandler.class);
//        validateHandler = mock(OperationHandler.class);
//        storeElementDef = mock(DataElementDefinition.class);
//        dataEdgeDef = mock(DataEdgeDefinition.class);
//        dataEntityDef = mock(DataEntityDefinition.class);
//
//        dataSchema = new DataSchema.Builder()
//                .edge(TestGroups.EDGE, new DataEdgeDefinition.Builder()
//                        .property(TestPropertyNames.PROP_1, String.class)
//                        .property(TestPropertyNames.PROP_2, String.class)
//                        .build())
//                .edge(TestGroups.EDGE_2, new DataEdgeDefinition.Builder()
//                        .property(TestPropertyNames.PROP_1, String.class)
//                        .property(TestPropertyNames.PROP_2, String.class)
//                        .build())
//                .entity(TestGroups.ENTITY, new DataEntityDefinition.Builder()
//                        .property(TestPropertyNames.PROP_1, String.class)
//                        .property(TestPropertyNames.PROP_2, String.class)
//                        .build())
//                .entity(TestGroups.ENTITY_2, new DataEntityDefinition.Builder()
//                        .property(TestPropertyNames.PROP_1, String.class)
//                        .property(TestPropertyNames.PROP_2, String.class)
//                        .build())
//                .build();
//
//        dataSchema = new DataSchema.Builder()
//                .edge(TestGroups.EDGE, new DataElementDefinition.Builder()
//                        .property(TestPropertyNames.PROP_1, storePropertyDef)
//                        .property(TestPropertyNames.PROP_2, storePropertyDef)
//                        .build())
//                .edge(TestGroups.EDGE_2, new DataElementDefinition.Builder()
//                        .property(TestPropertyNames.PROP_1, storePropertyDef)
//                        .property(TestPropertyNames.PROP_2, storePropertyDef)
//                        .build())
//                .entity(TestGroups.ENTITY, new DataElementDefinition.Builder()
//                        .property(TestPropertyNames.PROP_1, storePropertyDef)
//                        .property(TestPropertyNames.PROP_2, storePropertyDef)
//                        .build())
//                .entity(TestGroups.ENTITY_2, new DataElementDefinition.Builder()
//                        .property(TestPropertyNames.PROP_1, storePropertyDef)
//                        .property(TestPropertyNames.PROP_2, storePropertyDef)
//                        .build())
//                .build();
//    }
//
//    @Test
//    public void shouldThrowExceptionWhenPropertyIsNotSerialisable() throws StoreException {
//        // Given
//        DataSchema myDataSchema = new DataSchema.Builder()
//                .edge(TestGroups.EDGE, new DataEdgeDefinition.Builder()
//                        .property(TestPropertyNames.PROP_1, Store.class)
//                        .build())
//                .build();
//        DataSchema myDataSchema = new DataSchema.Builder()
//                .edge(TestGroups.EDGE, new DataElementDefinition.Builder()
//                        .property(TestPropertyNames.PROP_1, storePropertyDef)
//                        .build())
//                .build();
//        final StoreProperties properties = mock(StoreProperties.class);
//
//        final StoreImpl store = new StoreImpl();
//
//        // When
//        try {
//            store.initialise(myDataSchema, properties);
//            fail();
//        } catch (SchemaException exception) {
//            Assert.assertEquals("ERROR: Store schema property serialiser cannot handle a property in the data schema", exception.getMessage());
//        }
//    }
//
//    @Test
//    public void shouldCreateStoreWithValidSchemasAndRegisterOperations() throws StoreException {
//        // Given
//        final StoreProperties properties = mock(StoreProperties.class);
//        final StoreImpl store = new StoreImpl();
//
//        // When
//        store.initialise(dataSchema, properties);
//
//        // Then
//        assertNotNull(store.getOperationHandlerExposed(Validate.class));
//        assertSame(addElementsHandler, store.getOperationHandlerExposed(AddElements.class));
//
//        assertSame(getElementsHandler, store.getOperationHandlerExposed(GetElementsSeed.class));
//        assertSame(getElementsHandler, store.getOperationHandlerExposed(GetRelatedElements.class));
//        assertSame(getElementsHandler, store.getOperationHandlerExposed(GetEntitiesBySeed.class));
//        assertSame(getElementsHandler, store.getOperationHandlerExposed(GetRelatedEntities.class));
//        assertSame(getElementsHandler, store.getOperationHandlerExposed(GetEdgesBySeed.class));
//        assertSame(getElementsHandler, store.getOperationHandlerExposed(GetRelatedEntities.class));
//        assertSame(getAdjacentEntitySeedsHandler, store.getOperationHandlerExposed(GetAdjacentEntitySeeds.class));
//
//        assertTrue(store.getOperationHandlerExposed(GenerateElements.class) instanceof GenerateElementsHandler);
//        assertTrue(store.getOperationHandlerExposed(GenerateObjects.class) instanceof GenerateObjectsHandler);
//
//        assertEquals(1, store.getCreateOperationHandlersCallCount());
//        assertSame(dataSchema, store.getDataSchema());
//        assertSame(dataSchema, store.getDataSchema());
//        assertSame(properties, store.getProperties());
//    }
//
//    @Test
//    public void shouldThrowAnExceptionWhenValidatingSchemasWhereDataSchemaContainsOneLessEdgeThanDataSchema() {
//        // Given
//        dataSchema = new DataSchema.Builder(dataSchema).edge("extraEdge", storeElementDef).build();
//
//        // When / Then
//        shouldThrowExceptionWhenValidatingSchemas(dataSchema);
//    }
//
//    @Test
//    public void shouldThrowAnExceptionWhenValidatingSchemasWhereDataSchemaContainsOneLessEdgeThanDataSchema() {
//        // Given
//        dataSchema = new DataSchema.Builder(dataSchema).edge("extraEdge", dataEdgeDef).build();
//
//        // When / Then
//        shouldThrowExceptionWhenValidatingSchemas(dataSchema);
//    }
//
//    @Test
//    public void shouldThrowAnExceptionWhenValidatingSchemasWhereDataSchemaContainsOneLessEntityThanDataSchema() {
//        // Given
//        dataSchema = new DataSchema.Builder(dataSchema).entity("extraEntity", storeElementDef).build();
//
//        // When / Then
//        shouldThrowExceptionWhenValidatingSchemas(dataSchema);
//    }
//
//    @Test
//    public void shouldThrowAnExceptionWhenValidatingSchemasWhereDataSchemaContainsOneLessEntityThanDataSchema() {
//        // Given
//        dataSchema = new DataSchema.Builder(dataSchema).entity("extraEntity", dataEntityDef).build();
//
//        // When / Then
//        shouldThrowExceptionWhenValidatingSchemas(dataSchema);
//    }
//
//    @Test
//    public void shouldThrowAnExceptionWhenValidatingSchemasWhereDataSchemaContainsOneEdgeWithDifferentNameThanDataSchema() {
//        // Given
//        dataSchema = new DataSchema.Builder(dataSchema).edge("extraEdgeWithName1", dataEdgeDef).build();
//        dataSchema = new DataSchema.Builder(dataSchema).edge("extraEdgeWithName2", storeElementDef).build();
//
//        // When / Then
//        shouldThrowExceptionWhenValidatingSchemas(dataSchema);
//    }
//
//    @Test
//    public void shouldThrowAnExceptionWhenValidatingSchemasWhereDataSchemaContainsOneEntityWithDifferentNameThanDataSchema() {
//        // Given
//        dataSchema = new DataSchema.Builder(dataSchema).entity("extraEntityWithName1", dataEntityDef).build();
//        dataSchema = new DataSchema.Builder(dataSchema).entity("extraEntityWithName2", storeElementDef).build();
//
//        // When / Then
//        shouldThrowExceptionWhenValidatingSchemas(dataSchema);
//    }
//
//    @Test
//    public void shouldThrowAnExceptionWhenValidatingSchemasWhereOneEdgeInDataSchemaContainsOneLessPropertyThanDataSchema() {
//        // Given
//        new DataElementDefinition.Builder(dataSchema.getEdge(TestGroups.EDGE_2)).property("extraProperty", storePropertyDef);
//
//        // When / Then
//        shouldThrowExceptionWhenValidatingSchemas(dataSchema);
//    }
//
//    @Test
//    public void shouldThrowAnExceptionWhenValidatingSchemasWhereOneEdgeInDataSchemaContainsOneLessPropertyThanDataSchema() {
//        // Given
//        new DataEdgeDefinition.Builder(dataSchema.getEdge(TestGroups.EDGE_2)).property("extraProperty", String.class);
//
//        // When / Then
//        shouldThrowExceptionWhenValidatingSchemas(dataSchema);
//    }
//
//    @Test
//    public void shouldThrowAnExceptionWhenValidatingSchemasWhereOneEntityInDataSchemaContainsOneLessPropertyThanDataSchema() {
//        // Given
//        new DataElementDefinition.Builder(dataSchema.getEntity(TestGroups.ENTITY_2)).property("extraProperty", storePropertyDef);
//
//        // When / Then
//        shouldThrowExceptionWhenValidatingSchemas(dataSchema);
//    }
//
//    @Test
//    public void shouldThrowAnExceptionWhenValidatingSchemasWhereOneEntityInDataSchemaContainsOneLessPropertyThanDataSchema() {
//        // Given
//        new DataEntityDefinition.Builder(dataSchema.getEntity(TestGroups.ENTITY_2)).property("extraProperty", String.class);
//
//        // When / Then
//        shouldThrowExceptionWhenValidatingSchemas(dataSchema);
//    }
//
//    @Test
//    public void shouldThrowAnExceptionWhenValidatingSchemasWhereOneEdgeInDataSchemaContainsOnePropertyWithDifferentNameThanDataSchema() {
//        // Given
//        new DataEdgeDefinition.Builder(dataSchema.getEdge(TestGroups.EDGE_2)).property("extraPropertyWithName1", String.class);
//        new DataElementDefinition.Builder(dataSchema.getEdge(TestGroups.EDGE_2)).property("extraPropertyWithName2", storePropertyDef);
//
//
//        // When / Then
//        shouldThrowExceptionWhenValidatingSchemas(dataSchema);
//    }
//
//    @Test
//    public void shouldThrowAnExceptionWhenValidatingSchemasWhereOneEntityInDataSchemaContainsOnePropertyWithDifferentNameThanDataSchema() {
//        // Given
//        new DataEntityDefinition.Builder(dataSchema.getEntity(TestGroups.ENTITY_2)).property("extraPropertyWithName1", String.class);
//        new DataElementDefinition.Builder(dataSchema.getEntity(TestGroups.ENTITY_2)).property("extraPropertyWithName2", storePropertyDef);
//
//        // When / Then
//        shouldThrowExceptionWhenValidatingSchemas(dataSchema);
//    }
//
//    @Test
//    public void shouldThrowAnExceptionWhenValidatingSchemasWhereDataSchemaHasOneEdgeWhichIsAnEntityInDataSchema() {
//        // Given
//        dataSchema = new DataSchema.Builder(dataSchema).edge("edgeOrEntity", dataEdgeDef).build();
//        dataSchema = new DataSchema.Builder(dataSchema).entity("edgeOrEntity", storeElementDef).build();
//
//        // When / Then
//        shouldThrowExceptionWhenValidatingSchemas(dataSchema);
//    }
//
//    @Test
//    public void shouldThrowAnExceptionWhenValidatingSchemasWhereDataSchemaHasOneEdgeWhichIsAnEntityInDataSchema() {
//        // Given
//        dataSchema = new DataSchema.Builder(dataSchema).entity("edgeOrEntity", dataEntityDef).build();
//        dataSchema = new DataSchema.Builder(dataSchema).edge("edgeOrEntity", storeElementDef).build();
//
//        // When / Then
//        shouldThrowExceptionWhenValidatingSchemas(dataSchema);
//    }
//
//    @Test
//    public void shouldDelegateDoOperationToOperationHandler() throws Exception {
//        // Given
//        final DataSchema dataSchema = mock(DataSchema.class);
//        final StoreProperties properties = mock(StoreProperties.class);
//        final AddElements addElements = new AddElements();
//        final OperationChain opChain = new OperationChain(addElements);
//        final StoreImpl store = new StoreImpl();
//
//        store.initialise(dataSchema, properties);
//
//        // When
//        store.execute(opChain);
//
//        // Then
//        verify(addElementsHandler).doOperation(addElements, store);
//    }
//
//    @Test
//    public void shouldCallDoUnhandledOperationWhenDoOperationWithUnknownOperationClass() throws Exception {
//        // Given
//        final DataSchema dataSchema = mock(DataSchema.class);
//        final StoreProperties properties = mock(StoreProperties.class);
//        final Operation<String, String> operation = mock(Operation.class);
//        final OperationChain opChain = new OperationChain(operation);
//        final StoreImpl store = new StoreImpl();
//
//        store.initialise(dataSchema, properties);
//
//        // When
//        store.execute(opChain);
//
//        // Then
//        assertEquals(1, store.getDoUnhandledOperationCalls().size());
//        assertSame(operation, store.getDoUnhandledOperationCalls().get(0));
//    }
//
//    @Test
//    public void shouldFullyLoadLazyElement() throws StoreException {
//        // Given
//        final String group = "group1";
//        final DataSchema dataSchema = mock(DataSchema.class);
//        final StoreProperties properties = mock(StoreProperties.class);
//        final Element lazyElement = mock(Element.class);
//        final Store store = new StoreImpl();
//        final DataEdgeDefinition edgeDef1 = mock(DataEdgeDefinition.class);
//        final String propertyName1 = "property name 1";
//        final IdentifierType idType1 = IdentifierType.VERTEX;
//
//        given(lazyElement.getGroup()).willReturn(group);
//
//        final Map<String, DataEdgeDefinition> edgeDefMap = new HashMap<>();
//        edgeDefMap.put(group, edgeDef1);
//        given(dataSchema.getEdges()).willReturn(edgeDefMap);
//        given(edgeDef1.getProperties()).willReturn(Sets.newHashSet(propertyName1));
//        given(edgeDef1.getIdentifiers()).willReturn(Sets.newHashSet(idType1));
//        given(dataSchema.getElement(group)).willReturn(edgeDef1);
//
//        store.initialise(dataSchema, properties);
//
//        // When
//        store.populateElement(lazyElement);
//
//        // Then
//        verify(lazyElement).getGroup();
//        verify(lazyElement).getProperty(propertyName1);
//        verify(lazyElement).getIdentifier(idType1);
//    }
//
//    @Test
//    public void shouldHandleMultiStepOperations() throws Exception {
//        // Given
//        final DataSchema dataSchema = mock(DataSchema.class);
//        final StoreProperties properties = mock(StoreProperties.class);
//        final StoreImpl store = new StoreImpl();
//        final Iterable<Element> getElementsResult = mock(Iterable.class);
//
//        final AddElements addElements1 = new AddElements();
//        final GetElementsSeed<ElementSeed, Element> getElementsSeed = new GetElementsSeed<>();
//        final OperationChain<Iterable<Element>> opChain = new OperationChain.Builder()
//                .first(addElements1)
//                .then(getElementsSeed)
//                .build();
//
//        given(addElementsHandler.doOperation(addElements1, store)).willReturn(null);
//        given(getElementsHandler.doOperation(getElementsSeed, store)).willReturn(getElementsResult);
//
//        store.initialise(dataSchema, properties);
//
//        // When
//        final Iterable<Element> result = store.execute(opChain);
//
//        // Then
//        assertSame(getElementsResult, result);
//    }
//
//    @Test
//    public void shouldAddValidateOperationForValidatableOperation() throws Exception {
//        // Given
//        final DataSchema dataSchema = mock(DataSchema.class);
//        final StoreProperties properties = mock(StoreProperties.class);
//        final StoreImpl store = new StoreImpl();
//        final int expectedResult = 5;
//        final Validatable<Integer> validatable1 = mock(Validatable.class);
//        final boolean skipInvalidElements = true;
//        final Iterable<Element> elements = mock(Iterable.class);
//        final OperationChain<Integer> opChain = new OperationChain<>(validatable1);
//
//        given(validatable1.isSkipInvalidElements()).willReturn(skipInvalidElements);
//        given(validatable1.isValidate()).willReturn(true);
//        given(validatable1.getElements()).willReturn(elements);
//        given(validatableHandler.doOperation(validatable1, store)).willReturn(expectedResult);
//
//        store.initialise(dataSchema, properties);
//
//        // When
//        final int result = store.execute(opChain);
//
//        // Then
//        Assert.assertEquals(expectedResult, result);
//        verify(validateHandler).doOperation(Mockito.any(Validate.class), Mockito.eq(store));
//    }
//
//    @Test
//    public void shouldNotAddValidateOperationWhenValidatableHasValidateSetToFalse() throws Exception {
//        // Given
//        final DataSchema dataSchema = mock(DataSchema.class);
//        final StoreProperties properties = mock(StoreProperties.class);
//        final StoreImpl store = new StoreImpl();
//        final int expectedResult = 5;
//        final Validatable<Integer> validatable1 = mock(Validatable.class);
//        final OperationChain<Integer> opChain = new OperationChain<>(validatable1);
//
//        given(validatable1.isValidate()).willReturn(false);
//        given(validatableHandler.doOperation(validatable1, store)).willReturn(expectedResult);
//
//        store.initialise(dataSchema, properties);
//
//        // When
//        int result = store.execute(opChain);
//
//        // Then
//        Assert.assertEquals(expectedResult, result);
//        verify(validateHandler, Mockito.never()).doOperation(Mockito.any(Validate.class), Mockito.eq(store));
//    }
//
//    @Test
//    public void shouldThrowExceptionIfValidatableHasValidateSetToFalseAndStoreRequiresValidation() throws Exception {
//        // Given
//        final DataSchema dataSchema = mock(DataSchema.class);
//        final StoreProperties properties = mock(StoreProperties.class);
//        final StoreImpl store = new StoreImpl();
//        final Validatable<Integer> validatable1 = mock(Validatable.class);
//        final OperationChain<Integer> opChain = new OperationChain<>(validatable1);
//
//        store.setValidationRequired(true);
//        given(validatable1.isValidate()).willReturn(false);
//
//        store.initialise(dataSchema, properties);
//
//        // When / then
//        try {
//            store.execute(opChain);
//            fail("Exception expected");
//        } catch (UnsupportedOperationException e) {
//            assertNotNull(e);
//        }
//    }
//
//    @Test
//    public void shouldAddValidateOperationsForAllValidatableOperations() throws Exception {
//        // Given
//        final DataSchema dataSchema = mock(DataSchema.class);
//        final StoreProperties properties = mock(StoreProperties.class);
//        final StoreImpl store = new StoreImpl();
//        final int expectedResult = 5;
//        final Validatable<Integer> validatable1 = mock(Validatable.class);
//        final Operation<Iterable<Element>, Iterable<Element>> nonValidatable1 = mock(Operation.class);
//        final Validatable<Iterable<Element>> validatable2 = mock(Validatable.class);
//        final Validatable<Iterable<Element>> validatable3 = mock(Validatable.class);
//        final Operation<Iterable<Element>, Iterable<Element>> nonValidatable2 = mock(Operation.class);
//        final boolean skipInvalidElements = true;
//        final OperationChain<Integer> opChain = new OperationChain.Builder()
//                .first(nonValidatable2)
//                .then(validatable3)
//                .then(validatable2)
//                .then(nonValidatable1)
//                .then(validatable1)
//                .build();
//
//
//        given(validatable1.isSkipInvalidElements()).willReturn(skipInvalidElements);
//        given(validatable2.isSkipInvalidElements()).willReturn(skipInvalidElements);
//
//        given(validatable1.isValidate()).willReturn(true);
//        given(validatable2.isValidate()).willReturn(true);
//        given(validatable3.isValidate()).willReturn(false);
//
//        given(validatableHandler.doOperation(validatable1, store)).willReturn(expectedResult);
//
//        store.initialise(dataSchema, properties);
//
//        // When
//        int result = store.execute(opChain);
//
//        // Then
//        Assert.assertEquals(expectedResult, result);
//        verify(validateHandler, Mockito.times(2)).doOperation(Mockito.any(Validate.class), Mockito.eq(store));
//    }
//
//    @Test
//    public void shouldCopyOptionsIntoValidateOperations() throws Exception {
//        // Given
//        final DataSchema dataSchema = mock(DataSchema.class);
//        final StoreProperties properties = mock(StoreProperties.class);
//        final StoreImpl store = new StoreImpl();
//        final int expectedResult = 5;
//        final Validatable<Integer> validatable = mock(Validatable.class);
//        final Map<String, String> options = mock(HashMap.class);
//        final OperationChain<Integer> opChain = new OperationChain<>(validatable);
//
//        given(validatable.isValidate()).willReturn(true);
//        given(validatable.getOptions()).willReturn(options);
//        given(validatableHandler.doOperation(validatable, store)).willReturn(expectedResult);
//        store.initialise(dataSchema, properties);
//
//        // When
//        int result = store.execute(opChain);
//
//        //Then
//        verify(validatable, times(1)).getOptions();
//
//        // Then
//        Assert.assertEquals(expectedResult, result);
//    }
//
//    private void shouldThrowExceptionWhenValidatingSchemas(final DataSchema dataSchema) {
//        //Given
//        final StoreImpl store = new StoreImpl();
//
//        // When
//        try {
//            store.initialise(dataSchema, mock(StoreProperties.class));
//            fail("No exception thrown");
//        } catch (SchemaException e) {
//            // Then
//            assertNotNull(e.getMessage());
//        } catch (StoreException e) {
//            fail("Wrong exception thrown");
//        }
//    }
//
//    private class StoreImpl extends Store {
//        private final List<StoreTrait> TRAITS = Arrays.asList(AGGREGATION, FILTERING, TRANSFORMATION);
//
//        private int createOperationHandlersCallCount;
//        private final ArrayList<Operation> doUnhandledOperationCalls = new ArrayList<>();
//        private boolean validationRequired;
//
//        @Override
//        protected Collection<StoreTrait> getTraits() {
//            return TRAITS;
//        }
//
//        public OperationHandler getOperationHandlerExposed(final Class<? extends Operation> opClass) {
//            return super.getOperationHandler(opClass);
//        }
//
//        @Override
//        protected void addAdditionalOperationHandlers() {
//            createOperationHandlersCallCount++;
//            addOperationHandler(mock(AddElements.class).getClass(), (OperationHandler) addElementsHandler);
//            addOperationHandler(mock(GetElements.class).getClass(), (OperationHandler) getElementsHandler);
//            addOperationHandler(mock(GetAdjacentEntitySeeds.class).getClass(), (OperationHandler) getElementsHandler);
//            addOperationHandler(mock(Validatable.class).getClass(), (OperationHandler) validatableHandler);
//            addOperationHandler(Validate.class, (OperationHandler) validateHandler);
//        }
//
//        @Override
//        protected OperationHandler<GetElements<ElementSeed, Element>, Iterable<Element>> getGetElementsHandler() {
//            return getElementsHandler;
//        }
//
//        @Override
//        protected OperationHandler<? extends GetAdjacentEntitySeeds, Iterable<EntitySeed>> getAdjacentEntitySeedsHandler() {
//            return getAdjacentEntitySeedsHandler;
//        }
//
//        @Override
//        protected OperationHandler<? extends AddElements, Void> getAddElementsHandler() {
//            return addElementsHandler;
//        }
//
//        @Override
//        protected <OUTPUT> OUTPUT doUnhandledOperation(final Operation<?, OUTPUT> operation) {
//            doUnhandledOperationCalls.add(operation);
//            return null;
//        }
//
//        public int getCreateOperationHandlersCallCount() {
//            return createOperationHandlersCallCount;
//        }
//
//        public ArrayList<Operation> getDoUnhandledOperationCalls() {
//            return doUnhandledOperationCalls;
//        }
//
//        @Override
//        public boolean isValidationRequired() {
//            return validationRequired;
//        }
//
//        public void setValidationRequired(final boolean validationRequired) {
//            this.validationRequired = validationRequired;
//        }
//    }
//}
