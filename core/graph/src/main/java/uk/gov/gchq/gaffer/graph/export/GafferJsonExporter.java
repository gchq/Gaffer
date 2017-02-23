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

package uk.gov.gchq.gaffer.graph.export;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import uk.gov.gchq.gaffer.commonutil.iterable.CloseableIterable;
import uk.gov.gchq.gaffer.commonutil.iterable.LimitedCloseableIterable;
import uk.gov.gchq.gaffer.data.TransformOneToManyIterable;
import uk.gov.gchq.gaffer.data.element.Edge;
import uk.gov.gchq.gaffer.data.element.function.ElementFilter;
import uk.gov.gchq.gaffer.data.elementdefinition.view.View;
import uk.gov.gchq.gaffer.data.elementdefinition.view.ViewElementDefinition;
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.export.Exporter;
import uk.gov.gchq.gaffer.function.filter.IsEqual;
import uk.gov.gchq.gaffer.graph.Graph;
import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.data.EdgeSeed;
import uk.gov.gchq.gaffer.operation.impl.add.AddElements;
import uk.gov.gchq.gaffer.operation.impl.get.GetEdges;
import uk.gov.gchq.gaffer.store.Store;
import uk.gov.gchq.gaffer.user.User;
import java.util.List;

@SuppressFBWarnings(value = "UWF_FIELD_NOT_INITIALIZED_IN_CONSTRUCTOR", justification = "Fields are initialised in the initialise methods")
public class GafferJsonExporter extends Exporter<Store, InitialiseGafferJsonExport> {
    protected static final JSONSerialiser JSON_SERIALISER = new JSONSerialiser();
    private Graph graph;
    private Long timeToLive;
    private String visibility;

    public GafferJsonExporter() {
    }

    public void initialise(final Graph graph, final InitialiseGafferJsonExport initialiseExport, final Store store, final User user, final String jobId) {
        super.initialise(initialiseExport, store, user, jobId);
        this.graph = graph;
        timeToLive = initialiseExport.getTimeToLive();
        if (null == timeToLive) {
            timeToLive = InitialiseGafferJsonExport.DEFAULT_TIME_TO_LIVE;
        }
        visibility = initialiseExport.getVisibility();
    }

    @Override
    protected void _add(final Iterable<?> values, final User user) throws OperationException {
        try {
            final long timestamp = System.currentTimeMillis();
            graph.execute(new AddElements.Builder()
                    .elements(new Edge.Builder()
                            .group("result")
                            .source(getExportName())
                            .dest(getKey())
                            .directed(true)
                            .property("userId", getPlainTextUserId())
                            .property("timestamp", timestamp)
                            .property("deletionTimestamp", timestamp + timeToLive)
                            .property("visibility", visibility)
                            .property("result", JSON_SERIALISER.serialise(values))
                            .build())
                    .build(), user);
        } catch (SerialisationException e) {
            throw new OperationException("Unable to serialise results to json", e);
        }
    }

    @Override
    protected CloseableIterable<?> _get(final User user, final int start, final int end) throws OperationException {

        final GetEdges<EdgeSeed> getEdges = new GetEdges.Builder<EdgeSeed>()
                .addSeed(new EdgeSeed(getExportName(), getKey(), true))
                .view(new View.Builder()
                        .edge("result", new ViewElementDefinition.Builder()
                                .preAggregationFilter(new ElementFilter.Builder()
                                        .select("userId")
                                        .execute(new IsEqual(getPlainTextUserId()))
                                        .build())
                                .build())
                        .build())
                .build();

        try (final CloseableIterable<Edge> edges = graph.execute(getEdges, user)) {
            return new LimitedCloseableIterable<>(new TransformOneToManyIterable<Edge, Object>(edges) {
                @Override
                protected Iterable<Object> transform(final Edge edge) {
                    final byte[] resultBytes = (byte[]) edge.getProperty("result");
                    if (null == resultBytes || resultBytes.length == 0) {
                        return null;
                    }

                    try {
                        return JSON_SERIALISER.deserialise(resultBytes, List.class);
                    } catch (SerialisationException e) {
                        throw new RuntimeException(e);
                    }
                }
            }, start, end);
        }
    }
}
