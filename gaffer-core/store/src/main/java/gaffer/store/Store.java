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

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import gaffer.data.element.Element;
import gaffer.data.element.IdentifierType;
import gaffer.data.elementdefinition.exception.SchemaException;
import gaffer.operation.Operation;
import gaffer.operation.OperationChain;
import gaffer.operation.OperationException;
import gaffer.operation.Validatable;
import gaffer.operation.cache.CacheOperation;
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
import gaffer.operation.impl.get.GetRelatedEdges;
import gaffer.operation.impl.get.GetRelatedElements;
import gaffer.operation.impl.get.GetRelatedEntities;
import gaffer.serialisation.Serialisation;
import gaffer.store.operation.handler.GenerateElementsHandler;
import gaffer.store.operation.handler.GenerateObjectsHandler;
import gaffer.store.operation.handler.OperationHandler;
import gaffer.store.operation.handler.ValidateHandler;
import gaffer.store.schema.Schema;
import gaffer.store.schema.SchemaElementDefinition;
import gaffer.store.schema.ViewValidator;
import gaffer.user.User;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * A <code>Store</code> backs a Graph and is responsible for storing the {@link gaffer.data.element.Element}s and
 * handling {@link Operation}s.
 * {@link Operation}s and their corresponding {@link OperationHandler}s are registered in a map and used to handle
 * provided operations - allowing different store implementations to handle the same operations in their own store specific way.
 * Optional functionality can be added to store implementations defined by the {@link gaffer.store.StoreTrait}s.
 */
public abstract class Store {
    private static final Logger LOGGER = LoggerFactory.getLogger(Store.class);

    /**
     * The schema - contains the type of {@link gaffer.data.element.Element}s to be stored and how to aggregate the elements.
     */
    private Schema schema;

    /**
     * The store properties - contains specific configuration information for the store - such as database connection strings.
     */
    private StoreProperties properties;

    private final Map<Class<? extends Operation>, OperationHandler> operationHandlers = new HashMap<>();

    private ViewValidator viewValidator;

    public void initialise(final Schema schema, final StoreProperties properties) throws StoreException {
        this.schema = schema;
        this.properties = properties;
        viewValidator = new ViewValidator();
        addOpHandlers();
        optimiseSchemas();
        validateSchemas();
    }

    /**
     * Returns true if the Store can handle the provided trait and false if it cannot.
     *
     * @param storeTrait the Class of the Processor to be checked.
     * @return true if the Processor can be handled and false if it cannot.
     */
    public boolean hasTrait(final StoreTrait storeTrait) {
        return getTraits().contains(storeTrait);
    }

    /**
     * Returns the {@link gaffer.store.StoreTrait}s for this store. Most stores should support FILTERING.
     * <p>
     * If you use Operation.validateFilter(Element) in you handlers, it will deal with the filtering for you.
     *
     * @return the {@link gaffer.store.StoreTrait}s for this store.
     */
    public abstract Set<StoreTrait> getTraits();

    /**
     * @return true if the store requires validation, so it requires Validatable operations to have a validation step.
     */
    protected abstract boolean isValidationRequired();

    /**
     * Executes a given operation and returns the result.
     *
     * @param operation   the operation to execute.
     * @param <OPERATION> the operation type
     * @param user        the user executing the operation
     * @param <OUTPUT>    the output type.
     * @return the result from the operation
     * @throws OperationException thrown by the operation handler if the operation fails.
     */
    public <OPERATION extends Operation<?, OUTPUT>, OUTPUT> OUTPUT execute(
            final OPERATION operation, final User user) throws OperationException {
        return execute(new OperationChain<>(operation), user);
    }

    /**
     * Executes a given operation chain and returns the result.
     *
     * @param operationChain the operation chain to execute.
     * @param user           the user executing the operation chain
     * @param <OUTPUT>       the output type of the operation.
     * @return the result of executing the operation.
     * @throws OperationException thrown by an operation handler if an operation fails
     */
    public <OUTPUT> OUTPUT execute(final OperationChain<OUTPUT> operationChain, final User user) throws OperationException {
        final Iterator<Operation> opsItr = getValidatedOperations(operationChain).iterator();
        if (!opsItr.hasNext()) {
            throw new IllegalArgumentException("Operation chain contains no operations");
        }

        final Map<String, Iterable<?>> cache = new HashMap<>();
        Object result = null;
        Operation op = opsItr.next();
        while (null != op) {
            if (op instanceof CacheOperation) {
                result = handleCacheOperation(op, user, cache);
            } else {
                result = handleOperation(op, user);
            }

            if (opsItr.hasNext()) {
                op = updateOperationInput(opsItr.next(), result);
            } else {
                op = null;
            }
        }

        return (OUTPUT) result;
    }

    /**
     * @param operationClass the operation class to check
     * @return true if the provided operation is supported.
     */
    public boolean isSupported(final Class<? extends Operation> operationClass) {
        final OperationHandler operationHandler = operationHandlers.get(operationClass);
        return operationHandler != null;
    }

    /**
     * @return a collection of all the supported {@link Operation}s.
     */
    public Set<Class<? extends Operation>> getSupportedOperations() {
        return operationHandlers.keySet();
    }

    /**
     * Ensures all identifier and property values are populated on an element by triggering getters on the element for
     * all identifier and properties in the {@link Schema} forcing a lazy element to load all of its values.
     *
     * @param lazyElement the lazy element
     * @return the fully populated unwrapped element
     */
    @SuppressFBWarnings(value = "RV_RETURN_VALUE_IGNORED_NO_SIDE_EFFECT",
            justification = "Getters are called to trigger the loading data")
    public Element populateElement(final Element lazyElement) {
        final SchemaElementDefinition elementDefinition = getSchema().getElement(lazyElement.getGroup());
        if (null != elementDefinition) {
            for (final IdentifierType identifierType : elementDefinition.getIdentifiers()) {
                lazyElement.getIdentifier(identifierType);
            }

            for (final String propertyName : elementDefinition.getProperties()) {
                lazyElement.getProperty(propertyName);
            }
        }

        return lazyElement.getElement();
    }

    /**
     * Get this Store's {@link Schema}.
     *
     * @return the instance of {@link Schema} used for describing the type of
     * {@link gaffer.data.element.Element}s to be stored and how to aggregate the elements.
     */
    public Schema getSchema() {
        return schema;
    }

    /**
     * Get this Store's {@link gaffer.store.StoreProperties}.
     *
     * @return the instance of {@link gaffer.store.StoreProperties}, this may contain details such as database connection details.
     */
    public StoreProperties getProperties() {
        return properties;
    }

    protected Iterable<?> updateCache(final UpdateCache updateCache, final Map<String, Iterable<?>> cache) {
        final List<?> input = Lists.newArrayList(updateCache.getInput());
        final Collection cacheList = (Collection) cache.get(updateCache.getKey());
        if (null == cacheList) {
            cache.put(updateCache.getKey(), input);
        } else {
            Iterables.addAll(cacheList, input);
        }
        return Collections.unmodifiableCollection(input);
    }

    protected Operation updateOperationInput(final Operation op, final Object result) {
        if (null != result && null == op.getInput()) {
            try {
                op.setInput(result);
            } catch (final ClassCastException e) {
                throw new UnsupportedOperationException("Operation chain is not compatible. "
                        + op.getClass().getName() + " cannot take " + result.getClass().getName() + " as an input");
            }
        }
        return op;
    }

    /**
     * Removes any types in the schema that are not used.
     */
    public void optimiseSchemas() {
        final Set<String> usedTypeNames = new HashSet<>();
        final Set<SchemaElementDefinition> schemaElements = new HashSet<>();
        schemaElements.addAll(getSchema().getEdges().values());
        schemaElements.addAll(getSchema().getEntities().values());
        for (SchemaElementDefinition elDef : schemaElements) {
            usedTypeNames.addAll(elDef.getIdentifierTypeNames());
            usedTypeNames.addAll(elDef.getPropertyTypeNames());
        }

        if (null != getSchema().getTypes()) {
            for (String typeName : new HashSet<>(getSchema().getTypes().keySet())) {
                if (!usedTypeNames.contains(typeName)) {
                    getSchema().getTypes().remove(typeName);
                }
            }
        }
    }

    public void validateSchemas() {
        boolean valid = schema.validate();

        final HashMap<String, SchemaElementDefinition> schemaElements = new HashMap<>();
        schemaElements.putAll(getSchema().getEdges());
        schemaElements.putAll(getSchema().getEntities());
        for (Map.Entry<String, SchemaElementDefinition> schemaElementDefinitionEntry : schemaElements.entrySet()) {
            for (String propertyName : schemaElementDefinitionEntry.getValue().getProperties()) {
                Class propertyClass = schemaElementDefinitionEntry.getValue().getPropertyClass(propertyName);
                Serialisation serialisation = schemaElementDefinitionEntry.getValue().getPropertyTypeDef(propertyName).getSerialiser();

                if (!serialisation.canHandle(propertyClass)) {
                    valid = false;
                    LOGGER.error("Schema serialiser (" + serialisation.getClass().getName() + ") for property '" + propertyName + "' in the group '" + schemaElementDefinitionEntry.getKey() + "' cannot handle property found in the schema");
                }
            }
        }
        if (!valid) {
            throw new SchemaException("Schema is not valid. Check the logs for more information.");
        }
    }

    protected ViewValidator getViewValidator() {
        return viewValidator;
    }

    protected void setViewValidator(final ViewValidator viewValidator) {
        this.viewValidator = viewValidator;
    }

    /**
     * Any additional operations that a store can handle should be registered in this method by calling addOperationHandler(...)
     */
    protected abstract void addAdditionalOperationHandlers();

    /**
     * Get this Stores implementation of the handler for {@link gaffer.operation.impl.get.GetElements}. All Stores must implement this.
     *
     * @return the implementation of the handler for {@link gaffer.operation.impl.get.GetElements}
     */
    protected abstract OperationHandler<GetElements<ElementSeed, Element>, Iterable<Element>> getGetElementsHandler();

    /**
     * Get this Stores implementation of the handler for {@link gaffer.operation.impl.get.GetAllElements}. All Stores must implement this.
     *
     * @return the implementation of the handler for {@link gaffer.operation.impl.get.GetAllElements}
     */
    protected abstract OperationHandler<GetAllElements<Element>, Iterable<Element>> getGetAllElementsHandler();

    /**
     * Get this Stores implementation of the handler for {@link gaffer.operation.impl.get.GetAdjacentEntitySeeds}.
     * All Stores must implement this.
     *
     * @return the implementation of the handler for {@link gaffer.operation.impl.get.GetAdjacentEntitySeeds}
     */
    protected abstract OperationHandler<? extends GetAdjacentEntitySeeds, Iterable<EntitySeed>> getAdjacentEntitySeedsHandler();

    /**
     * Get this Stores implementation of the handler for {@link gaffer.operation.impl.add.AddElements}. All Stores must implement this.
     *
     * @return the implementation of the handler for {@link gaffer.operation.impl.add.AddElements}
     */
    protected abstract OperationHandler<? extends AddElements, Void> getAddElementsHandler();

    /**
     * Should deal with any unhandled operations, could simply throw an {@link UnsupportedOperationException}.
     *
     * @param operation the operation that does not have a registered handler.
     * @param <OUTPUT>  the operation output type
     * @return the result of the operation.
     */
    protected abstract <OUTPUT> OUTPUT doUnhandledOperation(final Operation<?, OUTPUT> operation);

    protected final void addOperationHandler(final Class<? extends Operation> opClass, final OperationHandler handler) {
        operationHandlers.put(opClass, handler);
    }

    protected final <OPERATION extends Operation<?, OUTPUT>, OUTPUT> OperationHandler<OPERATION, OUTPUT> getOperationHandler(final Class<? extends Operation> opClass) {
        return operationHandlers.get(opClass);
    }

    protected <OPERATION extends Operation<?, OUTPUT>, OUTPUT> OUTPUT handleOperation(final OPERATION operation, final User user) throws OperationException {
        final OperationHandler<OPERATION, OUTPUT> handler = getOperationHandler(operation.getClass());
        final OUTPUT result;
        if (null != handler) {
            result = handler.doOperation(operation, user, this);
        } else {
            result = doUnhandledOperation(operation);
        }

        return result;
    }

    private <OPERATION extends Operation<?, OUTPUT>, OUTPUT> OUTPUT handleCacheOperation(final OPERATION op, final User user, final Map<String, Iterable<?>> cache) {
        final Object result;
        if (op instanceof UpdateCache) {
            result = updateCache((UpdateCache) op, cache);
        } else if (op instanceof FetchCache) {
            result = cache;
        } else if (op instanceof FetchCachedResult) {
            result = cache.get(((FetchCachedResult) op).getKey());
        } else {
            result = doUnhandledOperation(op);
        }

        return (OUTPUT) result;
    }

    private void addOpHandlers() {
        addCoreOpHandlers();
        addAdditionalOperationHandlers();
    }

    private void addCoreOpHandlers() {
        addOperationHandler(GenerateElements.class, new GenerateElementsHandler<>());
        addOperationHandler(GenerateObjects.class, new GenerateObjectsHandler<>());
        addOperationHandler(Validate.class, new ValidateHandler());

        // Add elements
        addOperationHandler(AddElements.class, getAddElementsHandler());

        // Get Elements
        addOperationHandler(GetElementsSeed.class, (OperationHandler) getGetElementsHandler());
        addOperationHandler(GetEntitiesBySeed.class, (OperationHandler) getGetElementsHandler());
        addOperationHandler(GetEdgesBySeed.class, (OperationHandler) getGetElementsHandler());

        addOperationHandler(GetRelatedElements.class, (OperationHandler) getGetElementsHandler());
        addOperationHandler(GetRelatedEntities.class, (OperationHandler) getGetElementsHandler());
        addOperationHandler(GetRelatedEdges.class, (OperationHandler) getGetElementsHandler());

        addOperationHandler(GetAdjacentEntitySeeds.class, (OperationHandler) getAdjacentEntitySeedsHandler());

        addOperationHandler(GetAllElements.class, (OperationHandler) getGetAllElementsHandler());
        addOperationHandler(GetAllEntities.class, (OperationHandler) getGetAllElementsHandler());
        addOperationHandler(GetAllEdges.class, (OperationHandler) getGetAllElementsHandler());
    }

    private List<Operation> getValidatedOperations(final OperationChain<?> operationChain) {
        final List<Operation> ops = new ArrayList<>();

        boolean isParentAValidateOp = false;
        for (Operation<?, ?> op : operationChain.getOperations()) {
            if (!viewValidator.validate(op.getView(), schema)) {
                throw new SchemaException("View for operation "
                        + op.getClass().getName()
                        + " is not valid. See the logs for more information.");
            }

            if (doesOperationNeedValidating(op, isParentAValidateOp)) {
                final Validatable<?> validatable = (Validatable) op;
                final Validate validate = new Validate(validatable.isSkipInvalidElements());
                validate.setOptions(validatable.getOptions());

                // Move input to new validate operation
                validate.setElements(validatable.getElements());
                op.setInput(null);

                ops.add(validate);
            }

            isParentAValidateOp = op instanceof Validate;
            ops.add(op);
        }

        return ops;
    }

    private boolean doesOperationNeedValidating(final Operation<?, ?> op, final boolean isParentAValidateOp) {
        if (op instanceof Validatable) {
            if (((Validatable<?>) op).isValidate()) {
                return !isParentAValidateOp;
            }

            if (isValidationRequired()) {
                throw new UnsupportedOperationException("Validation is required by the store for all validatable "
                        + "operations so it cannot be disabled");
            }
        }

        return false;
    }
}
