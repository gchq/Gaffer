/*
 * Copyright 2017. Crown Copyright
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
package uk.gov.gchq.gaffer.parquetstore.operation.handler.spark.utilities;

import org.apache.spark.api.java.function.Function2;

import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.function.ElementAggregator;
import uk.gov.gchq.gaffer.store.schema.Schema;

/**
 * Used to aggregate Elements. Used by <code>AddElementsFromRDD</code>.
 */
public class AggregateGafferElements implements Function2<Element, Element, Element> {
    private static final long serialVersionUID = -256158555820968598L;
    private final byte[] jsonGafferSchema;
    private transient Schema gafferSchema;

    public AggregateGafferElements(final Schema gafferSchema) {
        jsonGafferSchema = gafferSchema.toCompactJson();
    }

    @Override
    public Element call(final Element v1, final Element v2) {
        if (null == gafferSchema) {
            gafferSchema = Schema.fromJson(jsonGafferSchema);
        }
        final ElementAggregator aggregator = gafferSchema.getElement(v2.getGroup()).getIngestAggregator();
        return aggregator.apply(v1, v2);
    }
}
