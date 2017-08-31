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
package uk.gov.gchq.gaffer.hbasestore.coprocessor;

import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.regionserver.ScanType;
import org.apache.hadoop.hbase.regionserver.Store;
import org.apache.hadoop.hbase.regionserver.compactions.CompactionRequest;
import org.apache.hadoop.hbase.util.Bytes;

import uk.gov.gchq.gaffer.commonutil.StringUtil;
import uk.gov.gchq.gaffer.hbasestore.coprocessor.scanner.QueryScanner;
import uk.gov.gchq.gaffer.hbasestore.coprocessor.scanner.StoreScanner;
import uk.gov.gchq.gaffer.hbasestore.serialisation.ElementSerialisation;
import uk.gov.gchq.gaffer.hbasestore.utils.HBaseStoreConstants;
import uk.gov.gchq.gaffer.store.schema.Schema;

import java.io.IOException;

public class GafferCoprocessor extends BaseRegionObserver {
    private Schema schema;
    private ElementSerialisation serialisation;
    private boolean includeMatchedVertex;

    @Override
    public void start(final CoprocessorEnvironment e) throws IOException {
        final String schemaJson = StringUtil.unescapeComma(e.getConfiguration().get(HBaseStoreConstants.SCHEMA));
        schema = Schema.fromJson(Bytes.toBytes(schemaJson));
        serialisation = new ElementSerialisation(schema);
        includeMatchedVertex = e.getConfiguration().getBoolean(HBaseStoreConstants.INCLUDE_MATCHED_VERTEX, false);
    }

    @Override
    public InternalScanner preFlush(final ObserverContext<RegionCoprocessorEnvironment> e,
                                    final Store store,
                                    final InternalScanner scanner) throws IOException {
        return new StoreScanner(scanner, schema, serialisation, includeMatchedVertex);
    }

    @Override
    public InternalScanner preCompact(final ObserverContext<RegionCoprocessorEnvironment> e,
                                      final Store store,
                                      final InternalScanner scanner,
                                      final ScanType scanType,
                                      final CompactionRequest request) throws IOException {
        return new StoreScanner(scanner, schema, serialisation, includeMatchedVertex);
    }

    @Override
    public InternalScanner preCompact(final ObserverContext<RegionCoprocessorEnvironment> e,
                                      final Store store,
                                      final InternalScanner scanner,
                                      final ScanType scanType) throws IOException {
        return new StoreScanner(scanner, schema, serialisation, includeMatchedVertex);
    }

    @Override
    public RegionScanner postScannerOpen(final ObserverContext<RegionCoprocessorEnvironment> e, final Scan scan, final RegionScanner scanner) throws IOException {
        return new QueryScanner(scanner, scan, schema, serialisation, includeMatchedVertex);
    }
}
