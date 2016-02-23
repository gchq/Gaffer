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

import com.google.common.collect.Sets;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import gaffer.data.element.Element;
import gaffer.data.element.IdentifierType;
import gaffer.data.elementdefinition.schema.DataElementDefinition;
import gaffer.data.elementdefinition.schema.DataSchema;
import gaffer.data.elementdefinition.schema.exception.SchemaException;
import gaffer.operation.Operation;
import gaffer.operation.OperationChain;
import gaffer.operation.OperationException;
import gaffer.operation.Validatable;
import gaffer.operation.data.ElementSeed;
import gaffer.operation.data.EntitySeed;
import gaffer.operation.impl.Validate;
import gaffer.operation.impl.add.AddElements;
import gaffer.operation.impl.generate.GenerateElements;
import gaffer.operation.impl.generate.GenerateObjects;
import gaffer.operation.impl.get.GetAdjacentEntitySeeds;
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
import gaffer.store.schema.StoreElementDefinition;
import gaffer.store.schema.StoreSchema;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
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
     * The data schema - contains the type of {@link gaffer.data.element.Element}s to be stored and how to aggregate the elements.
     */
    private DataSchema dataSchema;

    /**
     * The store schema - contains information on how to store and process the graph elements, such as position and serialisers.
     */
    private StoreSchema storeSchema;

    /**
     * The store properties - contains specific configuration information for the store - such as database connection strings.
     */
    private StoreProperties properties;

    private final Map<Class<? extends Operation>, OperationHandler> operationHandlers = new HashMap<>();

    public void initialise(final DataSchema dataSchema, final StoreSchema storeSchema,
                           final StoreProperties properties) throws StoreException {
        this.storeSchema = storeSchema;
        this.dataSchema = dataSchema;
        this.properties = properties;
        addOpHandlers();
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
     * Returns the {@link gaffer.store.StoreTrait}s for this store. Most stores should support VALIDATION and FILTERING.
     * <p>
     * This abstract store handles validation automatically using {@link gaffer.store.operation.handler.ValidateHandler}.
     * If you use Operation.validateFilter(Element) in you handlers, it will deal with the filtering for you.
     *
     * @return the {@link gaffer.store.StoreTrait}s for this store.
     */
    protected abstract Collection<StoreTrait> getTraits();

    /**
     * @return true if the store requires validation, so it requires Validatable operations to have a validation step.
     */
    protected abstract boolean isValidationRequired();

    /**
     * Executes a given operation and returns the result.
     *
     * @param operation   the operation to execute.
     * @param <OPERATION> the operation type
     * @param <OUTPUT>    the output type.
     * @return the result from the operation
     * @throws OperationException thrown by the operation handler if the operation fails.
     */
    public <OPERATION extends Operation<?, OUTPUT>, OUTPUT> OUTPUT execute(final OPERATION operation) throws OperationException {
        return execute(new OperationChain<>(operation));
    }

    /**
     * Executes a given operation chain and returns the result.
     *
     * @param operationChain the operation chain to execute.
     * @param <OUTPUT>       the output type of the operation.
     * @return the result of executing the operation.
     * @throws OperationException thrown by an operation handler if an operation fails
     */
    public <OUTPUT> OUTPUT execute(final OperationChain<OUTPUT> operationChain) throws OperationException {
        final Iterator<Operation> opsItr;

        if (hasTrait(StoreTrait.VALIDATION)) {
            opsItr = getValidatedOperations(operationChain).iterator();
        } else {
            opsItr = operationChain.getOperations().iterator();
        }

        if (!opsItr.hasNext()) {
            throw new IllegalArgumentException("Operation chain contains no operations");
        }

        Object result = null;
        Operation op = opsItr.next();
        while (null != op) {
            result = handleOperation(op);

            // Setup next operation seeds
            if (opsItr.hasNext()) {
                op = opsItr.next();
                if (null != result && null == op.getInput()) {
                    try {
                        op.setInput(result);
                    } catch (final ClassCastException e) {
                        throw new UnsupportedOperationException("Operation chain is not compatible. "
                                + op.getClass().getName() + " cannot take " + result.getClass().getName() + " as an input");
                    }
                }
            } else {
                op = null;
            }
        }

        return (OUTPUT) result;
    }

    /**
     * Ensures all identifier and property values are populated on an element by triggering getters on the element for
     * all identifier and properties in the {@link DataSchema} forcing a lazy element to load all of its values.
     *
     * @param lazyElement the lazy element
     * @return the fully populated unwrapped element
     */
    @SuppressFBWarnings(value = "RV_RETURN_VALUE_IGNORED_NO_SIDE_EFFECT",
            justification = "Getters are called to trigger the loading data")
    public Element populateElement(final Element lazyElement) {
        final DataElementDefinition elementDefinition = getDataSchema().getElement(lazyElement.getGroup());
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
     * Get this Store's {@link gaffer.data.elementdefinition.schema.DataSchema}.
     *
     * @return the instance of {@link gaffer.data.elementdefinition.schema.DataSchema} used for describing the type of
     * {@link gaffer.data.element.Element}s to be stored and how to aggregate the elements.
     */
    public DataSchema getDataSchema() {
        return dataSchema;
    }

    /**
     * Get this Store's {@link gaffer.store.schema.StoreSchema}
     *
     * @return the instance of {@link gaffer.store.schema.StoreSchema} used for describing how to store the
     * {@link gaffer.data.element.Element}s, e.g the positions and serialisers to use.
     */
    public StoreSchema getStoreSchema() {
        return storeSchema;
    }

    /**
     * Get this Store's {@link gaffer.store.StoreProperties}.
     *
     * @return the instance of {@link gaffer.store.StoreProperties}, this may contain details such as database connection details.
     */
    public StoreProperties getProperties() {
        return properties;
    }

    protected void validateSchemas() {
        boolean valid = validateTwoSetsContainSameElements(getDataSchema().getEdgeGroups(), getStoreSchema().getEdgeGroups(), "edges")
                && validateTwoSetsContainSameElements(getDataSchema().getEntityGroups(), getStoreSchema().getEntityGroups(), "entities");

        if (!valid) {
            throw new SchemaException("ERROR: the store schema did not pass validation because the store schema and data schema contain different numbers of elements. Please check the logs for more detailed information");
        }

        for (String group : getDataSchema().getEdgeGroups()) {
            valid &= validateTwoSetsContainSameElements(getDataSchema().getEdge(group).getProperties(), getStoreSchema().getEdge(group).getProperties(), "properties in the edge \"" + group + "\"");
        }

        for (String group : getDataSchema().getEntityGroups()) {
            valid &= validateTwoSetsContainSameElements(getDataSchema().getEntity(group).getProperties(), getStoreSchema().getEntity(group).getProperties(), "properties in the entity \"" + group + "\"");
        }

        if (!valid) {
            throw new SchemaException("ERROR: the store schema did not pass validation because at least one of the elements in the store schema and data schema contain different numbers of properties. Please check the logs for more detailed information");
        }
        HashMap<String, StoreElementDefinition> storeSchemaElements = new HashMap<>();
        storeSchemaElements.putAll(getStoreSchema().getEdges());
        storeSchemaElements.putAll(getStoreSchema().getEntities());
        for (Map.Entry<String, StoreElementDefinition> storeElementDefinitionEntry : storeSchemaElements.entrySet()) {
            DataElementDefinition dataElementDefinition = getDataSchema().getElement(storeElementDefinitionEntry.getKey());
            for (String propertyName : storeElementDefinitionEntry.getValue().getProperties()) {
                Class propertyClass = dataElementDefinition.getPropertyClass(propertyName);
                Serialisation serialisation = storeElementDefinitionEntry.getValue().getProperty(propertyName).getSerialiser();

                if (!serialisation.canHandle(propertyClass)) {
                    valid = false;
                    LOGGER.error("Store schema serialiser for property '" + propertyName + "' in the group '" + storeElementDefinitionEntry.getKey() + "' cannot handle property found in the data schema");
                }
            }
        }
        if (!valid) {
            throw new SchemaException("ERROR: Store schema property serialiser cannot handle a property in the data schema");
        }

    }

    protected boolean validateTwoSetsContainSameElements(final Set<String> firstSet, final Set<String> secondSet, final String type) {
        final Set<String> firstSetCopy = Sets.newHashSet(firstSet);
        final Set<String> secondSetCopy = Sets.newHashSet(secondSet);
        firstSetCopy.removeAll(secondSetCopy);
        secondSetCopy.removeAll(firstSet);
        boolean valid = true;
        if (!firstSetCopy.isEmpty()) {
            valid = false;
            LOGGER.warn("the data schema contains the following " + type + " which are not in the store schema: {" + StringUtils.join(firstSetCopy, ", ") + " }");
        }
        if (!secondSetCopy.isEmpty()) {
            valid = false;
            LOGGER.warn("the store schema contains the following " + type + " which are not in the data schema: {" + StringUtils.join(secondSetCopy, ", ") + " }");
        }
        return valid;
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

    protected final <OPERATION extends Operation<?, OUTPUT>, OUTPUT> void addOperationHandler(final Class<OPERATION> opClass, final OperationHandler handler) {
        operationHandlers.put(opClass, handler);
    }

    protected final <OPERATION extends Operation<?, OUTPUT>, OUTPUT> OperationHandler<OPERATION, OUTPUT> getOperationHandler(final Class<? extends Operation> opClass) {
        return operationHandlers.get(opClass);
    }

    protected <OPERATION extends Operation<?, OUTPUT>, OUTPUT> OUTPUT handleOperation(final OPERATION operation) throws OperationException {
        final OUTPUT result;

        if (!hasTrait(StoreTrait.VALIDATION) && operation instanceof Validate) {
            result = (OUTPUT) ((Validate) operation).getElements();
        } else {
            final OperationHandler<OPERATION, OUTPUT> handler = getOperationHandler(operation.getClass());
            if (null != handler) {
                result = handler.doOperation(operation, this);
            } else {
                result = doUnhandledOperation(operation);
            }
        }

        return result;
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
    }

    private List<Operation> getValidatedOperations(final OperationChain<?> operationChain) {
        final List<Operation> ops = new ArrayList<>();

        boolean isParentAValidateOp = false;
        for (Operation<?, ?> op : operationChain.getOperations()) {
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
