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
import uk.gov.gchq.gaffer.hbasestore.serialisation.ElementSerialisation;
import uk.gov.gchq.gaffer.hbasestore.serialisation.LazyElementCell;
import uk.gov.gchq.gaffer.hbasestore.utils.HBaseUtil;
import uk.gov.gchq.gaffer.store.schema.Schema;

import java.util.ArrayList;
import java.util.List;

public class StoreAggregationProcessor implements GafferScannerProcessor {
    private final ElementSerialisation serialisation;
    private final Schema schema;
    private final List<String> aggregatedGroups;

    public StoreAggregationProcessor(final ElementSerialisation serialisation,
                                     final Schema schema) {
        this.serialisation = serialisation;
        this.schema = schema;
        aggregatedGroups = schema.getAggregatedGroups();
    }

    @Override
    public List<LazyElementCell> process(final List<LazyElementCell> elementCells) {
        if (elementCells.size() <= 1) {
            return elementCells;
        }

        final List<LazyElementCell> output = new ArrayList<>();
        ElementAggregator aggregator = null;
        Properties aggregatedProperties = null;
        LazyElementCell firstElementCell = null;
        for (final LazyElementCell elementCell : elementCells) {
            if (elementCell.isDeleted()) {
                continue;
            }

            if (null == firstElementCell) {
                firstElementCell = elementCell;
                aggregatedProperties = null;
                aggregator = null;
            } else if (!aggregatedGroups.contains(elementCell.getGroup())
                    || !HBaseUtil.compareKeys(firstElementCell.getCell(), elementCell.getCell())) {
                completeAggregator(firstElementCell, aggregatedProperties, output);
                firstElementCell = elementCell;
                aggregatedProperties = null;
                aggregator = null;
            } else {
                final String group = firstElementCell.getGroup();
                if (null == aggregator) {
                    aggregator = schema.getElement(group).getIngestAggregator();
                    aggregatedProperties = firstElementCell.getElement().getProperties();
                }

                final Properties properties = elementCell.getElement().getProperties();
                aggregatedProperties = aggregator.apply(properties, aggregatedProperties);
            }
        }
        completeAggregator(firstElementCell, aggregatedProperties, output);
        return output;
    }

    private void completeAggregator(final LazyElementCell elementCell, final Properties aggregatedProperties, final List<LazyElementCell> output) {
        if (null == aggregatedProperties) {
            if (null != elementCell) {
                output.add(elementCell);
            }
        } else {
            try {
                final Cell firstCell = elementCell.getCell();
                final Element element = elementCell.getElement();
                element.copyProperties(aggregatedProperties);

                final Cell aggregatedCell = CellUtil.createCell(
                        CellUtil.cloneRow(firstCell),
                        CellUtil.cloneFamily(firstCell),
                        CellUtil.cloneQualifier(firstCell),
                        serialisation.getTimestamp(element),
                        firstCell.getTypeByte(),
                        serialisation.getValue(element),
                        CellUtil.getTagArray(firstCell),
                        0);

                elementCell.setCell(aggregatedCell);
                elementCell.setElement(element);
                output.add(elementCell);

            } catch (final SerialisationException e) {
                throw new RuntimeException(e);
            }
        }
    }

    public Schema getSchema() {
        return schema;
    }
}
