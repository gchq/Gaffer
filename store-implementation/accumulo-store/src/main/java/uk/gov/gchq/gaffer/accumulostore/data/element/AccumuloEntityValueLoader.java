/*
 * Copyright 2017-2018 Crown Copyright
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
package uk.gov.gchq.gaffer.accumulostore.data.element;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;

import uk.gov.gchq.gaffer.accumulostore.key.AccumuloElementConverter;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.data.element.id.EntityId;
import uk.gov.gchq.gaffer.store.schema.Schema;

public class AccumuloEntityValueLoader extends AccumuloElementValueLoader {
    private static final long serialVersionUID = -2926043462653982497L;

    public AccumuloEntityValueLoader(final String group,
                                     final Key key,
                                     final Value value,
                                     final AccumuloElementConverter elementConverter,
                                     final Schema schema) {
        super(group, key, value, elementConverter, schema);
    }

    @SuppressFBWarnings(value = "BC_UNCONFIRMED_CAST", justification = "element provided should always be an Entity")
    @Override
    public void loadIdentifiers(final Element entity) {
        ((Entity) entity).setVertex(((EntityId) elementConverter.getElementId(key, false)).getVertex());
    }
}
