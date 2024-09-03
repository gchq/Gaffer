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

import uk.gov.gchq.gaffer.federated.simple.merge.operator.ElementAggregateOperator;
import uk.gov.gchq.gaffer.federated.simple.merge.operator.SortedElementAggregateOperator;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.koryphe.impl.binaryoperator.And;
import uk.gov.gchq.koryphe.impl.binaryoperator.CollectionConcat;
import uk.gov.gchq.koryphe.impl.binaryoperator.StringConcat;
import uk.gov.gchq.koryphe.impl.binaryoperator.Sum;

import java.util.Collection;
import java.util.function.BinaryOperator;

/**
 * Abstract base class for accumulators that merge results from multiple
 * graphs together. Has default operators set for common data types.
 */
public abstract class FederatedResultAccumulator<T> implements BinaryOperator<T> {
    // Default merge operators for different data types
    protected BinaryOperator<Number> numberMergeOperator = new Sum();
    protected BinaryOperator<String> stringMergeOperator = new StringConcat();
    protected BinaryOperator<Boolean> booleanMergeOperator = new And();
    protected BinaryOperator<Collection<Object>> collectionMergeOperator = new CollectionConcat<>();
    // protected ElementAggregateOperator elementAggregateOperator = new ElementAggregateOperator();
    protected SortedElementAggregateOperator elementAggregateOperator = new SortedElementAggregateOperator();

    // Should the element aggregation operator be used, can be slower so disabled by default
    protected boolean aggregateElements = false;

    /**
     * Set whether the element aggregation operator should be used. This will
     * attempt to aggregate elements based on the current schema.
     *
     * @param aggregateElements should elements be aggregated.
     */
    public void aggregateElements(final boolean aggregateElements) {
        this.aggregateElements = aggregateElements;
    }

    /**
     * Sets the schema to use for the {@link ElementAggregateOperator}.
     *
     * @param schema The schema.
     */
    public void setSchema(final Schema schema) {
        elementAggregateOperator.setSchema(schema);
    }
}
