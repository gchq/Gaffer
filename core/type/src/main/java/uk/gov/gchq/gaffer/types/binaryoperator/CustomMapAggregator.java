/*
 * Copyright 2019 Crown Copyright
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

package uk.gov.gchq.gaffer.types.binaryoperator;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.annotation.JsonTypeInfo.As;
import com.fasterxml.jackson.annotation.JsonTypeInfo.Id;

import uk.gov.gchq.gaffer.types.CustomMap;
import uk.gov.gchq.koryphe.Since;
import uk.gov.gchq.koryphe.Summary;
import uk.gov.gchq.koryphe.binaryoperator.KorypheBinaryOperator;

import java.util.Iterator;
import java.util.Map.Entry;
import java.util.function.BinaryOperator;

@Since("1.10.0")
@Summary("Merges 2 CustomMaps by applying a binary operator to each of the values")
public class CustomMapAggregator<K, V> extends KorypheBinaryOperator<CustomMap<K, V>> {
    @JsonTypeInfo(
            use = Id.CLASS,
            include = As.PROPERTY,
            property = "class"
    )
    private BinaryOperator<V> binaryOperator;

    public CustomMapAggregator() {
    }

    public CustomMapAggregator(final BinaryOperator<V> binaryOperator) {
        this.setBinaryOperator(binaryOperator);
    }

    public void setBinaryOperator(final BinaryOperator<V> binaryOperator) {
        this.binaryOperator = binaryOperator;
    }

    public BinaryOperator<? super V> getBinaryOperator() {
        return this.binaryOperator;
    }

    public CustomMap<K, V> _apply(final CustomMap<K, V> state, final CustomMap<K, V> input) {
        Iterator inputIt = input.entrySet().iterator();

        while (inputIt.hasNext()) {
            Entry<K, V> entry = (Entry) inputIt.next();
            final K inputKey = entry.getKey();
            final V inputValue = entry.getValue();
            final V stateValue = state.get(inputKey);
            V newValue = this.binaryOperator.apply(stateValue, inputValue);
            state.put(entry.getKey(), newValue);
        }

        return state;
    }
}
