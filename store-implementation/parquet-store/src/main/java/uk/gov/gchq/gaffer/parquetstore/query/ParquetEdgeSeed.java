/*
 * Copyright 2017-2018. Crown Copyright
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

package uk.gov.gchq.gaffer.parquetstore.query;

import org.apache.commons.lang3.StringUtils;

import uk.gov.gchq.gaffer.commonutil.ToStringBuilder;
import uk.gov.gchq.gaffer.data.element.id.DirectedType;
import uk.gov.gchq.gaffer.data.element.id.ElementId;

public class ParquetEdgeSeed extends ParquetElementSeed {

    private final Object[] source;
    private final Object[] destination;
    private final DirectedType directedType;

    public ParquetEdgeSeed(final ElementId elementId,
                           final Object[] source,
                           final Object[] destination,
                           final DirectedType directedType) {
        this.elementId = elementId;
        this.source = source;
        this.destination = destination;
        this.directedType = directedType;
    }

    public Object[] getSource() {
        return source;
    }

    public Object[] getDestination() {
        return destination;
    }

    public DirectedType getDirectedType() {
        return directedType;
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("source", StringUtils.join(source, ','))
                .append("destination", StringUtils.join(destination, ','))
                .append("directedType", directedType)
                .build();
    }
}
