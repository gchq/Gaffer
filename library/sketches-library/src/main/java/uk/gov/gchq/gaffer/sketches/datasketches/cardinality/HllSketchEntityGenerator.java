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

package uk.gov.gchq.gaffer.sketches.datasketches.cardinality;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.yahoo.sketches.hll.HllSketch;

import uk.gov.gchq.gaffer.sketches.CardinalityEntityGenerator;
import uk.gov.gchq.gaffer.sketches.datasketches.cardinality.function.ToHllSketch;
import uk.gov.gchq.koryphe.Since;
import uk.gov.gchq.koryphe.Summary;

@JsonInclude(JsonInclude.Include.NON_DEFAULT)
@Since("1.8.0")
@Summary("Generates HllSketch sketch Entities for each end of an Edge")
@JsonPropertyOrder(value = {"group", "cardinalityPropertyName", "edgeGroupPropertyName", "propertiesToCopy"}, alphabetic = true)
public class HllSketchEntityGenerator extends CardinalityEntityGenerator<HllSketch> {
    private static final ToHllSketch TO_HLL_SKETCH = new ToHllSketch();

    public HllSketchEntityGenerator() {
        super(TO_HLL_SKETCH);
    }

    @Override
    public HllSketchEntityGenerator propertyToCopy(final String propertyToCopy) {
        return (HllSketchEntityGenerator) super.propertyToCopy(propertyToCopy);
    }

    @Override
    public HllSketchEntityGenerator propertiesToCopy(final String... propertiesToCopy) {
        return (HllSketchEntityGenerator) super.propertiesToCopy(propertiesToCopy);
    }

    @Override
    public HllSketchEntityGenerator group(final String group) {
        return (HllSketchEntityGenerator) super.group(group);
    }

    @Override
    public HllSketchEntityGenerator cardinalityPropertyName(final String cardinalityPropertyName) {
        return (HllSketchEntityGenerator) super.cardinalityPropertyName(cardinalityPropertyName);
    }

    @Override
    public HllSketchEntityGenerator countProperty(final String countProperty) {
        return (HllSketchEntityGenerator) super.countProperty(countProperty);
    }

    @Override
    public HllSketchEntityGenerator edgeGroupProperty(final String edgeGroupProperty) {
        return (HllSketchEntityGenerator) super.edgeGroupProperty(edgeGroupProperty);
    }
}
