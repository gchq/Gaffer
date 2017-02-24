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

package uk.gov.gchq.gaffer.operation.export.resultcache;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.gov.gchq.gaffer.commonutil.CommonConstants;
import uk.gov.gchq.gaffer.commonutil.iterable.CloseableIterable;
import uk.gov.gchq.gaffer.data.AlwaysValid;
import uk.gov.gchq.gaffer.data.TransformIterable;
import uk.gov.gchq.gaffer.data.element.Edge;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.function.ElementFilter;
import uk.gov.gchq.gaffer.data.elementdefinition.view.View;
import uk.gov.gchq.gaffer.data.elementdefinition.view.ViewElementDefinition;
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.function.filter.IsEqual;
import uk.gov.gchq.gaffer.graph.Graph;
import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.data.EdgeSeed;
import uk.gov.gchq.gaffer.operation.impl.add.AddElements;
import uk.gov.gchq.gaffer.operation.impl.export.Exporter;
import uk.gov.gchq.gaffer.operation.impl.get.GetEdges;
import uk.gov.gchq.gaffer.user.User;
import java.io.UnsupportedEncodingException;

public class GafferResultCacheExporter implements Exporter {
    private static final Logger LOGGER = LoggerFactory.getLogger(GafferResultCacheExporter.class);
    protected static final JSONSerialiser JSON_SERIALISER = new JSONSerialiser();
    private final String jobId;
    private final User user;
    private Graph resultCache;
    private String visibility;

    public GafferResultCacheExporter(final User user,
                                     final String jobId,
                                     final Graph resultCache,
                                     final String visibility) {
        this.user = user;
        this.jobId = jobId;
        this.resultCache = resultCache;
        this.visibility = visibility;
    }

    public void add(final Iterable<?> values, final String key) throws OperationException {
        final long timestamp = System.currentTimeMillis();
        final Iterable<Element> elements = new TransformIterable<Object, Element>((Iterable) values) {
            @Override
            protected Element transform(final Object value) {
                try {
                    final Class<?> valueClass;
                    final byte[] valueJson;
                    if (null == value) {
                        valueClass = Object.class;
                        valueJson = null;
                    } else {
                        valueClass = value.getClass();
                        valueJson = JSON_SERIALISER.serialise(value);
                    }

                    return new Edge.Builder()
                            .group("result")
                            .source(jobId)
                            .dest(key)
                            .directed(true)
                            .property("userId", user.getUserId())
                            .property("timestamp", timestamp)
                            .property("visibility", visibility)
                            .property("resultClass", valueClass.getName())
                            .property("result", valueJson)
                            .build();
                } catch (final SerialisationException e) {
                    throw new RuntimeException("Unable to serialise results to json", e);
                }
            }
        };

        resultCache.execute(new AddElements.Builder()
                .elements(elements)
                .build(), user);

    }

    public CloseableIterable<?> get(final String key) throws OperationException {
        final GetEdges<EdgeSeed> getEdges = new GetEdges.Builder<EdgeSeed>()
                .addSeed(new EdgeSeed(jobId, key, true))
                .view(new View.Builder()
                        .edge("result", new ViewElementDefinition.Builder()
                                .preAggregationFilter(new ElementFilter.Builder()
                                        .select("userId")
                                        .execute(new IsEqual(user.getUserId()))
                                        .build())
                                .build())
                        .build())
                .build();

        final CloseableIterable<Edge> edges = resultCache.execute(getEdges, user);
        return new TransformJsonResult(edges);
    }

    private static class TransformJsonResult extends TransformIterable<Edge, Object> {
        TransformJsonResult(final Iterable<Edge> input) {
            super(input, new AlwaysValid<>(), false, true);
        }

        @Override
        protected Object transform(final Edge edge) {
            final String resultClassName = (String) edge.getProperty("resultClass");
            final byte[] resultBytes = (byte[]) edge.getProperty("result");
            if (null == resultClassName || null == resultBytes) {
                return null;
            }

            final Class<?> resultClass;
            try {
                resultClass = Class.forName(resultClassName);
            } catch (ClassNotFoundException e) {
                LOGGER.error("Result class name was not found: " + resultClassName, e);
                throw new RuntimeException(e);
            }

            try {
                return JSON_SERIALISER.deserialise(resultBytes, resultClass);
            } catch (final SerialisationException e) {
                try {
                    LOGGER.error("Unable to deserialise result: " + new String(resultBytes, CommonConstants.UTF_8), e);
                } catch (final UnsupportedEncodingException e1) {
                    throw new RuntimeException(e);
                }
                throw new RuntimeException(e);
            }
        }
    }
}
