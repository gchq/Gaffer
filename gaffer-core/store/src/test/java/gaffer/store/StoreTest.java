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

package gaffer.store;

import static gaffer.store.StoreTrait.AGGREGATION;
import static gaffer.store.StoreTrait.FILTERING;
import static gaffer.store.StoreTrait.TRANSFORMATION;
import static junit.framework.Assert.assertFalse;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.BDDMockito.given;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import gaffer.commonutil.TestGroups;
import gaffer.commonutil.TestPropertyNames;
import gaffer.data.element.Element;
import gaffer.data.element.Entity;
import gaffer.data.element.IdentifierType;
import gaffer.data.element.LazyEntity;
import gaffer.data.elementdefinition.exception.SchemaException;
import gaffer.data.elementdefinition.view.View;
import gaffer.operation.Operation;
import gaffer.operation.OperationChain;
import gaffer.operation.OperationException;
import gaffer.operation.Validatable;
import gaffer.operation.data.ElementSeed;
import gaffer.operation.data.EntitySeed;
import gaffer.operation.impl.Validate;
import gaffer.operation.impl.add.AddElements;
import gaffer.operation.impl.cache.FetchCache;
import gaffer.operation.impl.cache.FetchCachedResult;
import gaffer.operation.impl.cache.UpdateCache;
import gaffer.operation.impl.generate.GenerateElements;
import gaffer.operation.impl.generate.GenerateObjects;
import gaffer.operation.impl.get.GetAdjacentEntitySeeds;
import gaffer.operation.impl.get.GetAllEdges;
import gaffer.operation.impl.get.GetAllElements;
import gaffer.operation.impl.get.GetAllEntities;
import gaffer.operation.impl.get.GetEdgesBySeed;
import gaffer.operation.impl.get.GetElements;
import gaffer.operation.impl.get.GetElementsSeed;
import gaffer.operation.impl.get.GetEntitiesBySeed;
import gaffer.operation.impl.get.GetRelatedElements;
import gaffer.operation.impl.get.GetRelatedEntities;
import gaffer.store.operation.handler.FetchCacheHandler;
import gaffer.store.operation.handler.FetchCachedResultHandler;
import gaffer.store.operation.handler.GenerateElementsHandler;
import gaffer.store.operation.handler.GenerateObjectsHandler;
import gaffer.store.operation.handler.OperationHandler;
import gaffer.store.operation.handler.UpdateCacheHandler;
import gaffer.store.schema.Schema;
import gaffer.store.schema.SchemaEdgeDefinition;
import gaffer.store.schema.SchemaEntityDefinition;
import gaffer.store.schema.ViewValidator;
import gaffer.user.User;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class StoreTest {
    private final User user = new User();
    private final Context context = new Context(user);

    private OperationHandler<AddElements, Void> addElementsHandler;
    private OperationHandler<GetElements<ElementSeed, Element>, Iterable<Element>> getElementsHandler;
    private OperationHandler<GetAllElements<Element>, Iterable<Element>> getAllElementsHandler;
    private OperationHandler<GetAdjacentEntitySeeds, Iterable<EntitySeed>> getAdjacentEntitySeedsHandler;
    private OperationHandler<Validatable<Integer>, Integer> validatableHandler;
    private OperationHandler<Validate, Iterable<Element>> validateHandler;
    private Schema schema;

    @Before
    public void setup() {
        addElementsHandler = mock(OperationHandler.class);
        getElementsHandler = mock(OperationHandler.class);
        getAllElementsHandler = mock(OperationHandler.class);
        getAdjacentEntitySeedsHandler = mock(OperationHandler.class);
        validatableHandler = mock(OperationHandler.class);
        validateHandler = mock(OperationHandler.class);

        schema = new Schema.Builder()
                .edge(TestGroups.EDGE, new SchemaEdgeDefinition.Builder()
                        .source(String.class)
                        .destination(String.class)
                        .directed(Boolean.class)
                        .property(TestPropertyNames.PROP_1, String.class)
                        .property(TestPropertyNames.PROP_2, String.class)
                        .build())
                .edge(TestGroups.EDGE_2, new SchemaEdgeDefinition.Builder()
                        .source(String.class)
                        .destination(String.class)
                        .directed(Boolean.class)
                        .property(TestPropertyNames.PROP_1, String.class)
                        .property(TestPropertyNames.PROP_2, String.class)
                        .build())
                .entity(TestGroups.ENTITY, new SchemaEntityDefinition.Builder()
                        .vertex(Integer.class)
                        .property(TestPropertyNames.PROP_1, String.class)
                        .property(TestPropertyNames.PROP_2, String.class)
                        .build())
                .entity(TestGroups.ENTITY_2, new SchemaEntityDefinition.Builder()
                        .vertex(String.class)
                        .property(TestPropertyNames.PROP_1, String.class)
                        .property(TestPropertyNames.PROP_2, String.class)
                        .build())
                .build();
    }

    @Test
    public void shouldThrowExceptionWhenPropertyIsNotSerialisable() throws StoreException {
        // Given
        final Schema mySchema = new Schema.Builder()
                .edge(TestGroups.EDGE, new SchemaEdgeDefinition.Builder()
                        .property(TestPropertyNames.PROP_1, Store.class)
                        .build())
                .build();
        final StoreProperties properties = mock(StoreProperties.class);
        final StoreImpl store = new StoreImpl();

        // When
        try {
            store.initialise(mySchema, properties);
            fail();
        } catch (SchemaException exception) {
            assertNotNull(exception.getMessage());
        }
    }

    @Test
    public void shouldCreateStoreWithValidSchemasAndRegisterOperations() throws StoreException {
        // Given
        final StoreProperties properties = mock(StoreProperties.class);
        final StoreImpl store = new StoreImpl();

        // When
        store.initialise(schema, properties);

        // Then
        assertNotNull(store.getOperationHandlerExposed(Validate.class));
        assertSame(addElementsHandler, store.getOperationHandlerExposed(AddElements.class));

        assertSame(getElementsHandler, store.getOperationHandlerExposed(GetElementsSeed.class));
        assertSame(getElementsHandler, store.getOperationHandlerExposed(GetRelatedElements.class));
        assertSame(getElementsHandler, store.getOperationHandlerExposed(GetEntitiesBySeed.class));
        assertSame(getElementsHandler, store.getOperationHandlerExposed(GetRelatedEntities.class));
        assertSame(getElementsHandler, store.getOperationHandlerExposed(GetEdgesBySeed.class));
        assertSame(getElementsHandler, store.getOperationHandlerExposed(GetRelatedEntities.class));
        assertSame(getAllElementsHandler, store.getOperationHandlerExposed(GetAllElements.class));
        assertSame(getAllElementsHandler, store.getOperationHandlerExposed(GetAllEntities.class));
        assertSame(getAllElementsHandler, store.getOperationHandlerExposed(GetAllEdges.class));
        assertSame(getAdjacentEntitySeedsHandler, store.getOperationHandlerExposed(GetAdjacentEntitySeeds.class));

        assertTrue(store.getOperationHandlerExposed(GenerateElements.class) instanceof GenerateElementsHandler);
        assertTrue(store.getOperationHandlerExposed(GenerateObjects.class) instanceof GenerateObjectsHandler);

        assertTrue(store.getOperationHandlerExposed(UpdateCache.class) instanceof UpdateCacheHandler);
        assertTrue(store.getOperationHandlerExposed(FetchCachedResult.class) instanceof FetchCachedResultHandler);
        assertTrue(store.getOperationHandlerExposed(FetchCache.class) instanceof FetchCacheHandler);

        assertEquals(1, store.getCreateOperationHandlersCallCount());
        assertSame(schema, store.getSchema());
        assertSame(properties, store.getProperties());
    }

    @Test
    public void shouldDelegateDoOperationToOperationHandler() throws Exception {
        // Given
        final Schema schema = mock(Schema.class);
        final StoreProperties properties = mock(StoreProperties.class);
        final AddElements addElements = new AddElements();
        final StoreImpl store = new StoreImpl();
        given(schema.validate()).willReturn(true);
        store.initialise(schema, properties);

        // When
        store.execute(addElements, user);

        // Then
        verify(addElementsHandler).doOperation(addElements, context, store);
    }

    @Test
    public void shouldThrowExceptionIfOperationViewIsInvalid() throws OperationException, StoreException {
        // Given
        // Given
        final Schema schema = mock(Schema.class);
        final StoreProperties properties = mock(StoreProperties.class);
        final AddElements addElements = new AddElements();
        final View view = mock(View.class);
        final ViewValidator viewValidator = mock(ViewValidator.class);
        final StoreImpl store = new StoreImpl();

        addElements.setView(view);
        given(schema.validate()).willReturn(true);
        given(viewValidator.validate(view, schema)).willReturn(false);
        store.initialise(schema, properties);
        store.setViewValidator(viewValidator);

        // When / Then
        try {
            store.execute(addElements, user);
            fail("Exception expected");
        } catch (final SchemaException e) {
            verify(viewValidator).validate(view, schema);
            assertTrue(e.getMessage().contains("View"));
        }
    }

    @Test
    public void shouldCallDoUnhandledOperationWhenDoOperationWithUnknownOperationClass() throws Exception {
        // Given
        final Schema schema = mock(Schema.class);
        final StoreProperties properties = mock(StoreProperties.class);
        final Operation<String, String> operation = mock(Operation.class);
        final StoreImpl store = new StoreImpl();

        given(schema.validate()).willReturn(true);
        store.initialise(schema, properties);

        // When
        store.execute(operation, user);

        // Then
        assertEquals(1, store.getDoUnhandledOperationCalls().size());
        assertSame(operation, store.getDoUnhandledOperationCalls().get(0));
    }

    @Test
    public void shouldFullyLoadLazyElement() throws StoreException {
        // Given
        final StoreProperties properties = mock(StoreProperties.class);
        final LazyEntity lazyElement = mock(LazyEntity.class);
        final Entity entity = mock(Entity.class);
        final Store store = new StoreImpl();
        given(lazyElement.getGroup()).willReturn(TestGroups.ENTITY);
        given(lazyElement.getElement()).willReturn(entity);

        store.initialise(schema, properties);

        // When
        final Element result = store.populateElement(lazyElement);

        // Then
        assertSame(entity, result);
        verify(lazyElement).getGroup();
        verify(lazyElement).getProperty(TestPropertyNames.PROP_1);
        verify(lazyElement).getIdentifier(IdentifierType.VERTEX);
    }

    @Test
    public void shouldHandleMultiStepOperations() throws Exception {
        // Given
        final Schema schema = mock(Schema.class);
        final StoreProperties properties = mock(StoreProperties.class);
        final StoreImpl store = new StoreImpl();
        final Iterable<Element> getElementsResult = mock(Iterable.class);

        final AddElements addElements1 = new AddElements();
        final GetElementsSeed<ElementSeed, Element> getElementsSeed = new GetElementsSeed<>();
        final OperationChain<Iterable<Element>> opChain = new OperationChain.Builder()
                .first(addElements1)
                .then(getElementsSeed)
                .build();

        given(schema.validate()).willReturn(true);

        given(addElementsHandler.doOperation(addElements1, context, store)).willReturn(null);
        given(getElementsHandler.doOperation(getElementsSeed, context, store)).willReturn(getElementsResult);

        store.initialise(schema, properties);

        // When
        final Iterable<Element> result = store.execute(opChain, user);

        // Then
        assertSame(getElementsResult, result);
    }

    @Test
    public void shouldAddValidateOperationForValidatableOperation() throws Exception {
        // Given
        final Schema schema = mock(Schema.class);
        final StoreProperties properties = mock(StoreProperties.class);
        final StoreImpl store = new StoreImpl();
        final int expectedResult = 5;
        final Validatable<Integer> validatable1 = mock(Validatable.class);
        final boolean skipInvalidElements = true;
        final Iterable<Element> elements = mock(Iterable.class);
        final OperationChain<Integer> opChain = new OperationChain<>(validatable1);

        given(schema.validate()).willReturn(true);
        given(validatable1.isSkipInvalidElements()).willReturn(skipInvalidElements);
        given(validatable1.isValidate()).willReturn(true);
        given(validatable1.getElements()).willReturn(elements);
        given(validatableHandler.doOperation(validatable1, context, store)).willReturn(expectedResult);

        store.initialise(schema, properties);

        // When
        final int result = store.execute(opChain, user);

        // Then
        assertEquals(expectedResult, result);
        verify(validateHandler).doOperation(Mockito.any(Validate.class), eq(context), eq(store));
    }

    @Test
    public void shouldNotAddValidateOperationWhenValidatableHasValidateSetToFalse() throws Exception {
        // Given
        final Schema schema = mock(Schema.class);
        final StoreProperties properties = mock(StoreProperties.class);
        final StoreImpl store = new StoreImpl();
        final int expectedResult = 5;
        final Validatable<Integer> validatable1 = mock(Validatable.class);

        given(schema.validate()).willReturn(true);
        given(validatable1.isValidate()).willReturn(false);
        given(validatableHandler.doOperation(validatable1, context, store)).willReturn(expectedResult);

        store.initialise(schema, properties);

        // When
        int result = store.execute(validatable1, user);

        // Then
        assertEquals(expectedResult, result);
        verify(validateHandler, Mockito.never()).doOperation(Mockito.any(Validate.class), eq(context), eq(store));
    }

    @Test
    public void shouldThrowExceptionIfValidatableHasValidateSetToFalseAndStoreRequiresValidation() throws Exception {
        // Given
        final Schema schema = mock(Schema.class);
        final StoreProperties properties = mock(StoreProperties.class);
        final StoreImpl store = new StoreImpl();
        final Validatable<Integer> validatable1 = mock(Validatable.class);

        given(schema.validate()).willReturn(true);
        store.setValidationRequired(true);
        given(validatable1.isValidate()).willReturn(false);

        store.initialise(schema, properties);

        // When / then
        try {
            store.execute(validatable1, user);
            fail("Exception expected");
        } catch (UnsupportedOperationException e) {
            assertNotNull(e);
        }
    }

    @Test
    public void shouldAddValidateOperationsForAllValidatableOperations() throws Exception {
        // Given
        final Schema schema = mock(Schema.class);
        final StoreProperties properties = mock(StoreProperties.class);
        final StoreImpl store = new StoreImpl();
        final int expectedResult = 5;
        final Validatable<Integer> validatable1 = mock(Validatable.class);
        final Operation<Iterable<Element>, Iterable<Element>> nonValidatable1 = mock(Operation.class);
        final Validatable<Iterable<Element>> validatable2 = mock(Validatable.class);
        final Validatable<Iterable<Element>> validatable3 = mock(Validatable.class);
        final Operation<Iterable<Element>, Iterable<Element>> nonValidatable2 = mock(Operation.class);
        final boolean skipInvalidElements = true;
        final OperationChain<Integer> opChain = new OperationChain.Builder()
                .first(nonValidatable2)
                .then(validatable3)
                .then(validatable2)
                .then(nonValidatable1)
                .then(validatable1)
                .build();


        given(schema.validate()).willReturn(true);
        given(validatable1.isSkipInvalidElements()).willReturn(skipInvalidElements);
        given(validatable2.isSkipInvalidElements()).willReturn(skipInvalidElements);

        given(validatable1.isValidate()).willReturn(true);
        given(validatable2.isValidate()).willReturn(true);
        given(validatable3.isValidate()).willReturn(false);

        given(validatableHandler.doOperation(validatable1, context, store)).willReturn(expectedResult);

        store.initialise(schema, properties);

        // When
        int result = store.execute(opChain, user);

        // Then
        assertEquals(expectedResult, result);
        verify(validateHandler, Mockito.times(2)).doOperation(Mockito.any(Validate.class), eq(context), eq(store));
    }

    @Test
    public void shouldCopyOptionsIntoValidateOperations() throws Exception {
        // Given
        final Schema schema = mock(Schema.class);
        final StoreProperties properties = mock(StoreProperties.class);
        final StoreImpl store = new StoreImpl();
        final int expectedResult = 5;
        final Validatable<Integer> validatable = mock(Validatable.class);
        final Map<String, String> options = mock(HashMap.class);

        given(schema.validate()).willReturn(true);
        given(validatable.isValidate()).willReturn(true);
        given(validatable.getOptions()).willReturn(options);
        given(validatableHandler.doOperation(validatable, context, store)).willReturn(expectedResult);
        store.initialise(schema, properties);

        // When
        int result = store.execute(validatable, user);

        //Then
        verify(validatable, times(1)).getOptions();

        // Then
        assertEquals(expectedResult, result);
    }

    private void shouldThrowExceptionWhenValidatingSchemas(final Schema schema) {
        //Given
        final StoreImpl store = new StoreImpl();

        // When
        try {
            store.initialise(schema, mock(StoreProperties.class));
            fail("No exception thrown");
        } catch (SchemaException e) {
            // Then
            assertNotNull(e.getMessage());
        } catch (StoreException e) {
            fail("Wrong exception thrown");
        }
    }

    @Test
    public void shouldReturnAllSupportedOperations() throws Exception {
        // Given
        final Schema schema = mock(Schema.class);
        final StoreProperties properties = mock(StoreProperties.class);
        final Validatable<Integer> validatable = mock(Validatable.class);
        final Map<String, String> options = mock(HashMap.class);

        final StoreImpl store = new StoreImpl();
        final int expectedNumberOfOperations = 21;

        given(schema.validate()).willReturn(true);
        given(validatable.isValidate()).willReturn(true);
        given(validatable.getOptions()).willReturn(options);
        given(validatableHandler.doOperation(validatable, context, store)).willReturn(expectedNumberOfOperations);
        store.initialise(schema, properties);

        // When
        final Set<Class<? extends Operation>> supportedOperations = store.getSupportedOperations();

        // Then
        assertNotNull(supportedOperations);
        assertEquals(expectedNumberOfOperations, supportedOperations.size());
    }

    @Test
    public void shouldReturnTrueWhenOperationSupported() throws Exception {
        // Given
        final Schema schema = mock(Schema.class);
        final StoreProperties properties = mock(StoreProperties.class);
        final Validatable<Integer> validatable = mock(Validatable.class);
        final Map<String, String> options = mock(HashMap.class);

        final StoreImpl store = new StoreImpl();
        final int expectedNumberOfOperations = 15;

        given(schema.validate()).willReturn(true);
        given(validatable.isValidate()).willReturn(true);
        given(validatable.getOptions()).willReturn(options);
        given(validatableHandler.doOperation(validatable, context, store)).willReturn(expectedNumberOfOperations);
        store.initialise(schema, properties);

        // WHen
        final Set<Class<? extends Operation>> supportedOperations = store.getSupportedOperations();
        for (final Class<? extends Operation> operationClass : supportedOperations) {
            final boolean isOperationClassSupported = store.isSupported(operationClass);

            // Then
            assertTrue(isOperationClassSupported);
        }
    }

    @Test
    public void shouldReturnFalseWhenUnsupportedOperationRequested() throws Exception {
        // Given
        final Schema schema = mock(Schema.class);
        final StoreProperties properties = mock(StoreProperties.class);
        final Validatable<Integer> validatable = mock(Validatable.class);
        final Map<String, String> options = mock(HashMap.class);

        final StoreImpl store = new StoreImpl();

        given(schema.validate()).willReturn(true);
        given(validatable.isValidate()).willReturn(true);
        given(validatable.getOptions()).willReturn(options);
        store.initialise(schema, properties);

        // When
        final boolean supported = store.isSupported(GetElements.class);

        // Then
        assertFalse(supported);
    }

    @Test
    public void shouldHandleNullOperationSupportRequest() throws Exception {
        // Given
        final Schema schema = mock(Schema.class);
        final StoreProperties properties = mock(StoreProperties.class);
        final Validatable<Integer> validatable = mock(Validatable.class);
        final Map<String, String> options = mock(HashMap.class);

        final StoreImpl store = new StoreImpl();

        given(schema.validate()).willReturn(true);
        given(validatable.isValidate()).willReturn(true);
        given(validatable.getOptions()).willReturn(options);
        store.initialise(schema, properties);

        // When
        final boolean supported = store.isSupported(null);

        // Then
        assertFalse(supported);
    }

    private class StoreImpl extends Store {
        private final Set<StoreTrait> TRAITS = new HashSet<>(Arrays.asList(AGGREGATION, FILTERING, TRANSFORMATION));

        private int createOperationHandlersCallCount;
        private final ArrayList<Operation> doUnhandledOperationCalls = new ArrayList<>();
        private boolean validationRequired;

        @Override
        public Set<StoreTrait> getTraits() {
            return TRAITS;
        }

        public OperationHandler getOperationHandlerExposed(final Class<? extends Operation> opClass) {
            return super.getOperationHandler(opClass);
        }

        @Override
        protected void addAdditionalOperationHandlers() {
            createOperationHandlersCallCount++;
            addOperationHandler(mock(AddElements.class).getClass(), (OperationHandler) addElementsHandler);
            addOperationHandler(mock(GetElements.class).getClass(), (OperationHandler) getElementsHandler);
            addOperationHandler(mock(GetAdjacentEntitySeeds.class).getClass(), (OperationHandler) getElementsHandler);
            addOperationHandler(mock(Validatable.class).getClass(), (OperationHandler) validatableHandler);
            addOperationHandler(Validate.class, (OperationHandler) validateHandler);
        }

        @Override
        protected OperationHandler<GetElements<ElementSeed, Element>, Iterable<Element>> getGetElementsHandler() {
            return getElementsHandler;
        }

        @Override
        protected OperationHandler<GetAllElements<Element>, Iterable<Element>> getGetAllElementsHandler() {
            return getAllElementsHandler;
        }

        @Override
        protected OperationHandler<? extends GetAdjacentEntitySeeds, Iterable<EntitySeed>> getAdjacentEntitySeedsHandler() {
            return getAdjacentEntitySeedsHandler;
        }

        @Override
        protected OperationHandler<? extends AddElements, Void> getAddElementsHandler() {
            return addElementsHandler;
        }

        @Override
        protected <OUTPUT> OUTPUT doUnhandledOperation(final Operation<?, OUTPUT> operation, final Context context) {
            doUnhandledOperationCalls.add(operation);
            return null;
        }

        public int getCreateOperationHandlersCallCount() {
            return createOperationHandlersCallCount;
        }

        public ArrayList<Operation> getDoUnhandledOperationCalls() {
            return doUnhandledOperationCalls;
        }

        @Override
        public boolean isValidationRequired() {
            return validationRequired;
        }

        public void setValidationRequired(final boolean validationRequired) {
            this.validationRequired = validationRequired;
        }

        @Override
        protected Context createContext(final User user) {
            return context;
        }
    }
}
