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

package uk.gov.gchq.gaffer.operation.export.resultcache.handler;

import uk.gov.gchq.gaffer.graph.Graph;
import uk.gov.gchq.gaffer.operation.export.resultcache.GafferResultCacheExporter;
import uk.gov.gchq.gaffer.operation.export.resultcache.handler.util.GafferResultCacheUtil;
import uk.gov.gchq.gaffer.operation.impl.export.resultcache.GetGafferResultCacheExport;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.Store;
import uk.gov.gchq.gaffer.store.StoreProperties;
import uk.gov.gchq.gaffer.store.operation.handler.export.GetExportHandler;
import java.nio.file.Paths;

public class GetGafferResultCacheExportHandler extends GetExportHandler<GetGafferResultCacheExport, GafferResultCacheExporter> {
    /**
     * Time to live in milliseconds.
     */
    private Long timeToLive = GafferResultCacheUtil.DEFAULT_TIME_TO_LIVE;

    private String visibility;

    private StoreProperties storeProperties;

    @Override
    protected Class<GafferResultCacheExporter> getExporterClass() {
        return GafferResultCacheExporter.class;
    }

    @Override
    protected GafferResultCacheExporter createExporter(final GetGafferResultCacheExport export, final Context context, final Store store) {
        final String jobId = null != export.getJobId() ? export.getJobId() : context.getJobId();
        return new GafferResultCacheExporter(
                context.getUser(), jobId, createGraph(store), visibility, null);
    }

    protected Graph createGraph(final Store store) {
        return GafferResultCacheUtil.createGraph(storeProperties, timeToLive);
    }

    public Long getTimeToLive() {
        return timeToLive;
    }

    public void setTimeToLive(final Long timeToLive) {
        this.timeToLive = timeToLive;
    }

    public String getVisibility() {
        return visibility;
    }

    public void setVisibility(final String visibility) {
        this.visibility = visibility;
    }

    public StoreProperties getStoreProperties() {
        return storeProperties;
    }

    public void setStoreProperties(final StoreProperties storeProperties) {
        this.storeProperties = storeProperties;
    }

    public void setStorePropertiesPath(final String path) {
        setStoreProperties(StoreProperties.loadStoreProperties(Paths.get(path)));
    }
}
