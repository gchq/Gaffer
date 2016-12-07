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
package uk.gov.gchq.gaffer.sparkaccumulo.operation.handler.dataframe;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.Row$;
import scala.runtime.AbstractFunction1;
import uk.gov.gchq.gaffer.data.element.Edge;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.spark.operation.dataframe.ConversionException;
import uk.gov.gchq.gaffer.spark.operation.dataframe.Converter;
import java.io.Serializable;
import java.util.LinkedHashSet;
import java.util.Map;

public class ConvertElementToRow extends AbstractFunction1<Element, Row>
        implements Serializable {

    private static final long serialVersionUID = -361239524365928808L;
    private final LinkedHashSet<String> properties;
    private final Map<String, Boolean> propertyNeedsConversion;
    private final Map<String, Converter> convertersByProperty;

    public ConvertElementToRow(final LinkedHashSet<String> properties,
                               final Map<String, Boolean> propertyNeedsConversion,
                               final Map<String, Converter> convertersByProperty) {
        this.properties = properties;
        this.propertyNeedsConversion = propertyNeedsConversion;
        this.convertersByProperty = convertersByProperty;
    }

    @Override
    public Row apply(final Element element) {
        final scala.collection.mutable.MutableList<Object> fields = new scala.collection.mutable.MutableList<>();
        for (final String property : properties) {
            switch (property) {
                case AccumuloStoreRelation.GROUP:
                    fields.appendElem(element.getGroup());
                    break;
                case AccumuloStoreRelation.SRC_COL_NAME:
                    if (element instanceof Edge) {
                        fields.appendElem(((Edge) element).getSource());
                    } else {
                        fields.appendElem(null);
                    }
                    break;
                case AccumuloStoreRelation.DST_COL_NAME:
                    if (element instanceof Edge) {
                        fields.appendElem(((Edge) element).getDestination());
                    } else {
                        fields.appendElem(null);
                    }
                    break;
                case AccumuloStoreRelation.VERTEX_COL_NAME:
                    if (element instanceof Entity) {
                        fields.appendElem(((Entity) element).getVertex());
                    } else {
                        fields.appendElem(null);
                    }
                    break;
                default:
                    final Object value = element.getProperties().get(property);
                    if (value == null) {
                        fields.appendElem(null);
                    } else {
                        if (!propertyNeedsConversion.get(property)) {
                            fields.appendElem(element.getProperties().get(property));
                        } else {
                            final Converter converter = convertersByProperty.get(property);
                            if (converter != null) {
                                try {
                                    fields.appendElem(converter.convert(value));
                                } catch (final ConversionException e) {
                                    fields.appendElem(null);
                                }
                            } else {
                                fields.appendElem(null);
                            }
                        }
                    }
            }
        }
        return Row$.MODULE$.fromSeq(fields);
    }
}
