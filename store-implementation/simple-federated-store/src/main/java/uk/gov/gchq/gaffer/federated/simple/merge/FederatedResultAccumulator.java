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

import java.util.Collection;
import java.util.function.BinaryOperator;

import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.federated.simple.merge.operator.ElementAggregateOperator;
import uk.gov.gchq.koryphe.impl.binaryoperator.CollectionConcat;
import uk.gov.gchq.koryphe.impl.binaryoperator.StringConcat;
import uk.gov.gchq.koryphe.impl.binaryoperator.Sum;
import uk.gov.gchq.koryphe.impl.binaryoperator.And;

public abstract class FederatedResultAccumulator<T> implements BinaryOperator<T> {
    // Default merge operators for different data types
    protected BinaryOperator<Number> numberMergeOperator = new Sum();
    protected BinaryOperator<String> stringMergeOperator = new StringConcat();
    protected BinaryOperator<Boolean> booleanMergeOperator = new And();
    protected BinaryOperator<Collection<Object>> collectionMergeOperator = new CollectionConcat<>();
    protected BinaryOperator<Iterable<Element>> elementMergeOperator = new ElementAggregateOperator();

    // Should the element merge operator be used
    protected boolean mergeElements = true;

    /**
     * Set whether the element merge operator should be used.
     *
     * @param mergeElements should merge.
     */
    public void setMergeElements(boolean mergeElements) {
        this.mergeElements = mergeElements;
    }
}
