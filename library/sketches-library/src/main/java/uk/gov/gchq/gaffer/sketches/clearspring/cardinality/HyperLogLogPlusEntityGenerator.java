/*
 * Copyright 2016-2018 Crown Copyright
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

package uk.gov.gchq.gaffer.sketches.clearspring.cardinality;

import com.clearspring.analytics.stream.cardinality.HyperLogLogPlus;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;

import uk.gov.gchq.gaffer.sketches.CardinalityEntityGenerator;
import uk.gov.gchq.gaffer.sketches.clearspring.cardinality.function.ToHyperLogLogPlus;
import uk.gov.gchq.koryphe.Since;
import uk.gov.gchq.koryphe.Summary;

@JsonInclude(JsonInclude.Include.NON_DEFAULT)
@Since("1.8.0")
@Summary("Generates HyperLogLogPlus sketch Entities for each end of an Edge")
@JsonPropertyOrder(value = {"group", "cardinalityPropertyName", "edgeGroupPropertyName", "propertiesToCopy"}, alphabetic = true)
public class HyperLogLogPlusEntityGenerator extends CardinalityEntityGenerator<HyperLogLogPlus> {
    private static final ToHyperLogLogPlus TO_HHLP = new ToHyperLogLogPlus();

    public HyperLogLogPlusEntityGenerator() {
        super(TO_HHLP);
    }

    @Override
    public HyperLogLogPlusEntityGenerator propertyToCopy(final String propertyToCopy) {
        return (HyperLogLogPlusEntityGenerator) super.propertyToCopy(propertyToCopy);
    }

    @Override
    public HyperLogLogPlusEntityGenerator propertiesToCopy(final String... propertiesToCopy) {
        return (HyperLogLogPlusEntityGenerator) super.propertiesToCopy(propertiesToCopy);
    }

    @Override
    public HyperLogLogPlusEntityGenerator group(final String group) {
        return (HyperLogLogPlusEntityGenerator) super.group(group);
    }

    @Override
    public HyperLogLogPlusEntityGenerator cardinalityPropertyName(final String cardinalityPropertyName) {
        return (HyperLogLogPlusEntityGenerator) super.cardinalityPropertyName(cardinalityPropertyName);
    }

    @Override
    public HyperLogLogPlusEntityGenerator countProperty(final String countProperty) {
        return (HyperLogLogPlusEntityGenerator) super.countProperty(countProperty);
    }

    @Override
    public HyperLogLogPlusEntityGenerator edgeGroupProperty(final String edgeGroupProperty) {
        return (HyperLogLogPlusEntityGenerator) super.edgeGroupProperty(edgeGroupProperty);
    }
}
