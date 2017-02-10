/*
 * Copyright 2016-2017 Crown Copyright
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
package uk.gov.gchq.gaffer.hbasestore.coprocessor.processor;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.Properties;
import uk.gov.gchq.gaffer.data.element.function.ElementAggregator;
import uk.gov.gchq.gaffer.data.elementdefinition.view.View;
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.hbasestore.serialisation.ElementCell;
import uk.gov.gchq.gaffer.hbasestore.serialisation.ElementSerialisation;
import uk.gov.gchq.gaffer.hbasestore.utils.ByteUtils;
import uk.gov.gchq.gaffer.hbasestore.utils.GroupComparatorUtils;
import uk.gov.gchq.gaffer.store.schema.Schema;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

public class QueryAggregationProcessor implements GafferScannerProcessor {
    private final ElementSerialisation serialisation;
    private final Schema schema;
    private final View view;

    public QueryAggregationProcessor(final ElementSerialisation serialisation,
                                     final Schema schema,
                                     final View view) {
        this.serialisation = serialisation;
        this.schema = schema;
        this.view = view;
    }

    @Override
    public List<ElementCell> process(final List<ElementCell> elementCells) {
        if (elementCells.size() <= 1) {
            return elementCells;
        }

        try {
            final List<ElementCell> output = new ArrayList<>();
            ElementAggregator aggregator = null;
            String group = null;
            ElementCell firstElementCell = null;
            for (final ElementCell elementCell : elementCells) {
                if (elementCell.isDeleted()) {
                    continue;
                }

                if (null == firstElementCell) {
                    firstElementCell = elementCell;
                } else {
                    if (elementCell.isElementLoaded()) {
                        group = elementCell.getElement().getGroup();
                    } else {
                        group = serialisation.getGroup(elementCell.getCell());
                    }
                    final Set<String> schemaGroupBy = schema.getElement(group).getGroupBy();
                    final Set<String> groupBy = view.getElementGroupBy(group);
                    if (!compareGroupByKeys(firstElementCell.getCell(), elementCell.getCell(), group, schemaGroupBy, groupBy)) {
                        completeAggregator(firstElementCell, group, aggregator, output);
                        firstElementCell = elementCell;
                        aggregator = null;
                    } else {
                        if (null == aggregator) {
                            aggregator = schema.getElement(group).getAggregator();
                            final Properties properties = firstElementCell.getElement().getProperties();
                            properties.remove(groupBy);
                            aggregator.aggregate(properties);
                        }

                        final Properties properties = elementCell.getElement().getProperties();
                        properties.remove(groupBy);
                        aggregator.aggregate(properties);
                    }
                }
            }
            completeAggregator(firstElementCell, group, aggregator, output);
            return output;
        } catch (SerialisationException e) {
            throw new RuntimeException(e);
        }
    }

    private void completeAggregator(final ElementCell elementCell, final String group, final ElementAggregator aggregator, final List<ElementCell> output) {
        if (null == aggregator) {
            if (null != elementCell) {
                output.add(elementCell);
            }
        } else {
            try {
                final Cell firstCell = elementCell.getCell();
                final Element element = elementCell.getElement();
                aggregator.state(element);

                final Cell aggregatedCell = CellUtil.createCell(
                        CellUtil.cloneRow(firstCell),
                        CellUtil.cloneFamily(firstCell),
                        serialisation.buildColumnQualifier(element),
                        serialisation.buildTimestamp(element),
                        firstCell.getTypeByte(),
                        serialisation.getValue(element),
                        CellUtil.getTagArray(firstCell),
                        0);

                elementCell.setCell(aggregatedCell);
                elementCell.setElement(element);
                output.add(elementCell);

            } catch (SerialisationException e) {
                throw new RuntimeException(e);
            }
        }
    }

    private boolean compareGroupByKeys(final Cell left, final Cell right, final String group, final Set<String> schemaGroupBy, final Set<String> groupBy) throws SerialisationException {
        if (null != groupBy && groupBy.isEmpty()) {
            return true;
        }

        if (GroupComparatorUtils.compareQualifier(left, right) == 0) {
            return true;
        }

        if (null == groupBy || groupBy.equals(schemaGroupBy)) {
            return false;
        }

        final byte[] groupByPropBytesLeft = serialisation.getPropertiesAsBytesFromColumnQualifier(group, CellUtil.cloneQualifier(left), groupBy.size());
        final byte[] groupByPropBytesRight = serialisation.getPropertiesAsBytesFromColumnQualifier(group, CellUtil.cloneQualifier(right), groupBy.size());
        return ByteUtils.areKeyBytesEqual(groupByPropBytesLeft, groupByPropBytesRight);
    }

}
