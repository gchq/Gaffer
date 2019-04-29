/*
 * Copyright 2016-2019 Crown Copyright
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

package uk.gov.gchq.gaffer.store.serialiser;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import uk.gov.gchq.gaffer.commonutil.StringUtil;
import uk.gov.gchq.gaffer.data.element.Edge;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.serialisation.util.LengthValueBytesSerialiserUtil;
import uk.gov.gchq.gaffer.store.schema.Schema;

/**
 * Serialiser to serialise and deserialise {@link Element} objects in a byte array
 * representation by delegating to the relevant serialiser.
 *
 * @see EdgeSerialiser
 * @see EntitySerialiser
 */
public class ElementSerialiser extends PropertiesSerialiser<Element> {
    private static final long serialVersionUID = 4640352297806229672L;
    private final EntitySerialiser entitySerialiser;
    private final EdgeSerialiser edgeSerialiser;

    // Required for serialisation
    ElementSerialiser() {
        entitySerialiser = null;
        edgeSerialiser = null;
    }

    public ElementSerialiser(final Schema schema) {
        super(schema);
        entitySerialiser = new EntitySerialiser(schema);
        edgeSerialiser = new EdgeSerialiser(schema);
    }

    @Override
    public boolean canHandle(final Class clazz) {
        return Element.class.isAssignableFrom(clazz);
    }

    @SuppressFBWarnings(value = "BC_UNCONFIRMED_CAST", justification = "If an element is not an Entity it must be an Edge")
    @Override
    public byte[] serialise(final Element element) throws SerialisationException {
        if (element instanceof Entity) {
            return entitySerialiser.serialise(((Entity) element));
        }

        return edgeSerialiser.serialise(((Edge) element));
    }

    @Override
    public Element deserialise(final byte[] bytes) throws SerialisationException {
        final String group = getGroup(bytes);
        if (null != schema.getEntity(group)) {
            return entitySerialiser.deserialise(bytes);
        }

        return edgeSerialiser.deserialise(bytes);
    }

    public String getGroup(final byte[] bytes) throws SerialisationException {
        return StringUtil.toString(LengthValueBytesSerialiserUtil.deserialise(bytes, 0));
    }

    @Override
    public Element deserialiseEmpty() throws SerialisationException {
        return null;
    }

    @Override
    public boolean isConsistent() {
        return false;
    }

    @Override
    public boolean equals(final Object obj) {
        if (this == obj) {
            return true;
        }

        if (null == obj || getClass() != obj.getClass()) {
            return false;
        }

        final ElementSerialiser serialiser = (ElementSerialiser) obj;

        return new EqualsBuilder()
                .append(entitySerialiser, serialiser.entitySerialiser)
                .append(edgeSerialiser, serialiser.edgeSerialiser)
                .isEquals();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(17, 37)
                .append(entitySerialiser)
                .append(edgeSerialiser)
                .toHashCode();
    }
}
