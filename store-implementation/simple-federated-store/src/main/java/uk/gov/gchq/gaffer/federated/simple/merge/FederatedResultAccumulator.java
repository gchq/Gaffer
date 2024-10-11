/*
 * Copyright 2024 Crown Copyright
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

package uk.gov.gchq.gaffer.federated.simple.merge;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.federated.simple.merge.operator.ElementAggregateOperator;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.koryphe.binaryoperator.BinaryOperatorMap;
import uk.gov.gchq.koryphe.impl.binaryoperator.And;
import uk.gov.gchq.koryphe.impl.binaryoperator.CollectionConcat;
import uk.gov.gchq.koryphe.impl.binaryoperator.Last;
import uk.gov.gchq.koryphe.impl.binaryoperator.StringConcat;
import uk.gov.gchq.koryphe.impl.binaryoperator.Sum;

import java.util.Collection;
import java.util.Map;
import java.util.Properties;
import java.util.function.BinaryOperator;

import static uk.gov.gchq.gaffer.federated.simple.FederatedStoreProperties.PROP_DEFAULT_MERGE_ELEMENTS;
import static uk.gov.gchq.gaffer.federated.simple.FederatedStoreProperties.PROP_MERGE_CLASS_BOOLEAN;
import static uk.gov.gchq.gaffer.federated.simple.FederatedStoreProperties.PROP_MERGE_CLASS_COLLECTION;
import static uk.gov.gchq.gaffer.federated.simple.FederatedStoreProperties.PROP_MERGE_CLASS_ELEMENTS;
import static uk.gov.gchq.gaffer.federated.simple.FederatedStoreProperties.PROP_MERGE_CLASS_MAP;
import static uk.gov.gchq.gaffer.federated.simple.FederatedStoreProperties.PROP_MERGE_CLASS_NUMBER;
import static uk.gov.gchq.gaffer.federated.simple.FederatedStoreProperties.PROP_MERGE_CLASS_STRING;

/**
 * Abstract base class for accumulators that merge results from multiple
 * graphs together. Has default operators set for common data types.
 */
public abstract class FederatedResultAccumulator<T> implements BinaryOperator<T> {
    private static final Logger LOGGER = LoggerFactory.getLogger(FederatedResultAccumulator.class);

    // Default merge operators for different data types
    protected BinaryOperator<Number> numberMergeOperator = new Sum();
    protected BinaryOperator<String> stringMergeOperator = new StringConcat();
    protected BinaryOperator<Boolean> booleanMergeOperator = new And();
    protected BinaryOperator<Collection<Object>> collectionMergeOperator = new CollectionConcat<>();
    protected BinaryOperator<Iterable<Element>> elementAggregateOperator = new ElementAggregateOperator();
    // For map merging define a sub operator for if values are the same
    protected BinaryOperator<Object> mapValueMergeOperator = new Last();
    protected BinaryOperator<Map<Object, Object>> mapMergeOperator = new BinaryOperatorMap<>(mapValueMergeOperator);

    // Should the element aggregation operator be used, can be slower so disabled by default
    protected boolean aggregateElements = false;

    protected FederatedResultAccumulator() {
        // Construct with defaults
    }

    protected FederatedResultAccumulator(final Properties properties) {
        // Use the store properties to configure the merging
        if (properties.containsKey(PROP_MERGE_CLASS_NUMBER)) {
            numberMergeOperator = loadMergeClass(numberMergeOperator, properties.get(PROP_MERGE_CLASS_NUMBER));
        }
        if (properties.containsKey(PROP_MERGE_CLASS_STRING)) {
            stringMergeOperator = loadMergeClass(stringMergeOperator, properties.get(PROP_MERGE_CLASS_STRING));
        }
        if (properties.containsKey(PROP_MERGE_CLASS_BOOLEAN)) {
            booleanMergeOperator = loadMergeClass(booleanMergeOperator, properties.get(PROP_MERGE_CLASS_BOOLEAN));
        }
        if (properties.containsKey(PROP_MERGE_CLASS_COLLECTION)) {
            collectionMergeOperator = loadMergeClass(collectionMergeOperator, properties.get(PROP_MERGE_CLASS_COLLECTION));
        }
        if (properties.containsKey(PROP_MERGE_CLASS_ELEMENTS)) {
            elementAggregateOperator = loadMergeClass(elementAggregateOperator, properties.get(PROP_MERGE_CLASS_ELEMENTS));
        }
        if (properties.containsKey(PROP_MERGE_CLASS_MAP)) {
            mapValueMergeOperator = loadMergeClass(mapValueMergeOperator, properties.get(PROP_MERGE_CLASS_MAP));
            mapMergeOperator = new BinaryOperatorMap<>(mapValueMergeOperator);
        }
        // Should we do element aggregation by default
        if (properties.containsKey(PROP_DEFAULT_MERGE_ELEMENTS)) {
            setAggregateElements(Boolean.parseBoolean((String) properties.get(PROP_DEFAULT_MERGE_ELEMENTS)));
        }
    }

    /**
     * Set whether the element aggregation operator should be used. This will
     * attempt to aggregate elements based on the current schema.
     *
     * @param aggregateElements should elements be aggregated.
     */
    public void setAggregateElements(final boolean aggregateElements) {
        this.aggregateElements = aggregateElements;
    }

    /**
     * Access to see if this accumulator is set to try element aggregation or
     * not.
     *
     * @return Is aggregation set.
     */
    public boolean aggregateElements() {
        return aggregateElements;
    }

    /**
     * Sets the schema to use for the {@link ElementAggregateOperator}.
     *
     * @param schema The schema.
     */
    public void setSchema(final Schema schema) {
        ((ElementAggregateOperator) elementAggregateOperator).setSchema(schema);
    }

    @SuppressWarnings("unchecked")
    private <I> BinaryOperator<I> loadMergeClass(final BinaryOperator<I> originalOperator, final Object clazzName) {
        BinaryOperator<I> mergeOperator = originalOperator;
        try {
            Class<?> clazz = Class.forName((String) clazzName);
            mergeOperator = (BinaryOperator<I>) clazz.newInstance();
        } catch (final ClassCastException | ClassNotFoundException | InstantiationException | IllegalAccessException e) {
            LOGGER.warn("Failed to load alternative merge function: {} The default will be used instead.", clazzName);
        }
        return mergeOperator;
    }
}
