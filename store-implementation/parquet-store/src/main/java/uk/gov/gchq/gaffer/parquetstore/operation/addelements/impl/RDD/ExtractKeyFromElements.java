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
package uk.gov.gchq.gaffer.parquetstore.operation.addelements.impl.RDD;

import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;
import uk.gov.gchq.gaffer.data.element.Edge;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.store.schema.Schema;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * A function that will extract the key of an Element ready to aggregateByKey
 */
public class ExtractKeyFromElements implements PairFunction<Element, List<Object>, Element>, Serializable {
    private static final long serialVersionUID = 6741839811757475786L;

    private final byte[] jsonGafferSchema;
    private transient Schema gafferSchema;

    public ExtractKeyFromElements(final Schema gafferSchema) {
        jsonGafferSchema = gafferSchema.toCompactJson();
    }

    @Override
    public Tuple2<List<Object>, Element> call(final Element element) throws Exception {
        if (null == gafferSchema) {
            gafferSchema = Schema.fromJson(jsonGafferSchema);
        }
        final String group = element.getGroup();
        final List<Object> list = new ArrayList<>();
        list.add(group);
        if (gafferSchema.getEntityGroups().contains(group)) {
            final Entity entity = (Entity) element;
            list.add(entity.getVertex());
        } else {
            final Edge edge = (Edge) element;
            list.add(edge.getSource());
            list.add(edge.getDestination());
            list.add(edge.getDirectedType());
        }
        for (final String property : gafferSchema.getElement(group).getGroupBy()) {
            list.add(element.getProperty(property));
        }
        return new Tuple2<>(list, element);
    }
}
