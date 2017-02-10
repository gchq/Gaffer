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

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.filter.FilterBase;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.hbasestore.serialisation.ElementSerialisation;
import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;
import uk.gov.gchq.gaffer.store.ElementValidator;
import uk.gov.gchq.gaffer.store.schema.Schema;
import java.io.IOException;

/**
 * The AbstractElementFilter will filter out elements based on the filtering
 * instructions given in the schema or view that is passed to this iterator
 */
public abstract class AbstractElementFilter extends FilterBase {
    protected static final JSONSerialiser JSON_SERIALISER = new JSONSerialiser();
    protected final Schema schema;
    private final ElementSerialisation elementSerialisation;
    private ElementValidator validator;

    public AbstractElementFilter(final Schema schema, final ElementValidator validator) {
        this.schema = schema;
        this.elementSerialisation = new ElementSerialisation(schema);
        this.validator = validator;
    }

    @Override
    public ReturnCode filterKeyValue(final Cell cell) throws IOException {
        final Element element = elementSerialisation.getElement(cell, null);
        return validate(element);
    }

    protected ReturnCode validate(final Element element) {
        if (validator.validateInput(element)) {
            return ReturnCode.INCLUDE;
        }

        return ReturnCode.SKIP;
    }

    @Override
    public byte[] toByteArray() throws IOException {
        return JSON_SERIALISER.serialise(new FilterProperties(schema));
    }
}
