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
package uk.gov.gchq.gaffer.hbasestore.filter;

import org.apache.hadoop.hbase.exceptions.DeserializationException;
import uk.gov.gchq.gaffer.data.elementdefinition.view.View;
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.store.ElementValidator;
import uk.gov.gchq.gaffer.store.schema.Schema;
import java.io.IOException;

public class ElementPreAggregationFilter extends AbstractElementFilter {
    private final View view;

    public ElementPreAggregationFilter(final Schema schema, final View view) {
        super(schema, new ElementValidator(view));
        this.view = view;
    }

    public static ElementPreAggregationFilter parseFrom(final byte[] pbBytes) throws DeserializationException {
        final FilterProperties props;
        try {
            props = JSON_SERIALISER.deserialise(pbBytes, FilterProperties.class);
        } catch (SerialisationException e) {
            throw new DeserializationException(e);
        }

        return new ElementPreAggregationFilter(props.getSchema(), props.getView());
    }

    @Override
    public byte[] toByteArray() throws IOException {
        return JSON_SERIALISER.serialise(new FilterProperties(schema, view));
    }
}
