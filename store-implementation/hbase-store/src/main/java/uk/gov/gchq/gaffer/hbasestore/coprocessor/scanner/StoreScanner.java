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
package uk.gov.gchq.gaffer.hbasestore.coprocessor.scanner;

import org.apache.hadoop.hbase.regionserver.InternalScanner;

import uk.gov.gchq.gaffer.hbasestore.coprocessor.processor.GafferScannerProcessor;
import uk.gov.gchq.gaffer.hbasestore.coprocessor.processor.StoreAggregationProcessor;
import uk.gov.gchq.gaffer.hbasestore.coprocessor.processor.ValidationProcessor;
import uk.gov.gchq.gaffer.hbasestore.serialisation.ElementSerialisation;
import uk.gov.gchq.gaffer.store.schema.Schema;

import java.util.ArrayList;
import java.util.List;

public class StoreScanner extends GafferScanner {
    public StoreScanner(final InternalScanner scanner,
                        final Schema schema,
                        final ElementSerialisation serialisation,
                        final boolean includeMatchedVertex) {
        super(scanner, serialisation, createProcessors(schema, serialisation), includeMatchedVertex);
    }

    protected static List<GafferScannerProcessor> createProcessors(final Schema schema, final ElementSerialisation serialisation) {
        final List<GafferScannerProcessor> processors = new ArrayList<>();
        if (schema.isAggregationEnabled()) {
            processors.add(new StoreAggregationProcessor(serialisation, schema));
        }
        processors.add(new ValidationProcessor(schema));

        return processors;
    }
}
