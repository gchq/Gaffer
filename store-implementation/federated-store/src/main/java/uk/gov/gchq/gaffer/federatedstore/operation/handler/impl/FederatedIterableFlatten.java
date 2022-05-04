/*
 * Copyright 2022 Crown Copyright
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
package uk.gov.gchq.gaffer.federatedstore.operation.handler.impl;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.codehaus.jackson.annotate.JsonCreator;

import uk.gov.gchq.koryphe.impl.function.IterableFlatten;

import java.util.Collection;
import java.util.Objects;
import java.util.function.BinaryOperator;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static java.util.Objects.nonNull;

@Deprecated
public class FederatedIterableFlatten<I_ITEM> extends IterableFlatten<I_ITEM> {

    @JsonCreator()
    public FederatedIterableFlatten(@JsonProperty("operator") final BinaryOperator<I_ITEM> operator) {
        super(operator);
    }

    @Override
    public I_ITEM apply(final Iterable<I_ITEM> items) {
        if (nonNull(items)) {
            final Collection<I_ITEM> nonNulls = StreamSupport.stream(items.spliterator(), false)
                    .filter(Objects::nonNull)
                    .collect(Collectors.toList());
            return super.apply(nonNulls);
        }

        return null;
    }
}
