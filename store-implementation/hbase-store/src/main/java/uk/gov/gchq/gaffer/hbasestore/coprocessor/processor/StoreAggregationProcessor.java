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
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.hbasestore.serialisation.ElementCell;
import uk.gov.gchq.gaffer.hbasestore.serialisation.ElementSerialisation;
import uk.gov.gchq.gaffer.hbasestore.utils.GroupComparatorUtils;
import uk.gov.gchq.gaffer.store.schema.Schema;
import java.util.ArrayList;
import java.util.List;

public class StoreAggregationProcessor implements GafferScannerProcessor {
    private final ElementSerialisation serialisation;
    private final Schema schema;

    public StoreAggregationProcessor(final ElementSerialisation serialisation,
                                     final Schema schema) {
        this.serialisation = serialisation;
        this.schema = schema;
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
                } else if (!GroupComparatorUtils.compareKeys(firstElementCell.getCell(), elementCell.getCell())) {
                    completeAggregator(firstElementCell, aggregator, output);
                    firstElementCell = elementCell;
                    aggregator = null;
                } else {
                    if (null == aggregator) {
                        group = serialisation.getGroup(firstElementCell.getCell());
                        aggregator = schema.getElement(group).getAggregator();

                        final Properties properties = firstElementCell.getElement().getProperties();
                        aggregator.aggregate(properties);
                    }

                    final byte[] value = CellUtil.cloneValue(elementCell.getCell());
                    final Properties properties = serialisation.getPropertiesFromValue(group, value);
                    aggregator.aggregate(properties);
                }
            }
            completeAggregator(firstElementCell, aggregator, output);
            return output;
        } catch (SerialisationException e) {
            throw new RuntimeException(e);
        }
    }

    private void completeAggregator(final ElementCell elementCell, final ElementAggregator aggregator, final List<ElementCell> output) {
        if (null == aggregator) {
            if (null != elementCell) {
                output.add(elementCell);
            }
        } else {
            try {
                final Cell firstCell = elementCell.getCell();
                final byte[] rowId = CellUtil.cloneRow(firstCell);
                final Element element = elementCell.getElement();
                aggregator.state(element);

                final Cell aggregatedCell = CellUtil.createCell(
                        rowId,
                        CellUtil.cloneFamily(firstCell),
                        CellUtil.cloneQualifier(firstCell),
                        serialisation.buildTimestamp(element.getProperties()),
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

}
