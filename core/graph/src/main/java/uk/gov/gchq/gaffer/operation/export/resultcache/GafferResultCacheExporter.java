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

package uk.gov.gchq.gaffer.operation.export.resultcache;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.gov.gchq.gaffer.commonutil.CollectionUtil;
import uk.gov.gchq.gaffer.commonutil.CommonConstants;
import uk.gov.gchq.gaffer.commonutil.iterable.CloseableIterable;
import uk.gov.gchq.gaffer.commonutil.iterable.WrappedCloseableIterable;
import uk.gov.gchq.gaffer.data.AlwaysValid;
import uk.gov.gchq.gaffer.data.TransformIterable;
import uk.gov.gchq.gaffer.data.element.Edge;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.function.ElementFilter;
import uk.gov.gchq.gaffer.data.elementdefinition.view.View;
import uk.gov.gchq.gaffer.data.elementdefinition.view.ViewElementDefinition;
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.function.filter.AreIn;
import uk.gov.gchq.gaffer.graph.Graph;
import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.data.EdgeSeed;
import uk.gov.gchq.gaffer.operation.impl.add.AddElements;
import uk.gov.gchq.gaffer.operation.impl.export.Exporter;
import uk.gov.gchq.gaffer.operation.impl.get.GetEdges;
import uk.gov.gchq.gaffer.user.User;
import java.io.UnsupportedEncodingException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.TreeSet;

public class GafferResultCacheExporter implements Exporter {
    private static final Logger LOGGER = LoggerFactory.getLogger(GafferResultCacheExporter.class);
    private final String jobId;
    private final User user;
    private final Graph resultCache;
    private final JSONSerialiser jsonSerialiser;
    private final String visibility;
    private final TreeSet<String> requiredOpAuths;
    private final Set<String> userOpAuths;

    public GafferResultCacheExporter(final User user,
                                     final String jobId,
                                     final Graph resultCache,
                                     final JSONSerialiser jsonSerialiser,
                                     final String visibility,
                                     final Set<String> requiredOpAuths) {
        this.user = user;
        this.jobId = jobId;
        this.resultCache = resultCache;
        this.jsonSerialiser = jsonSerialiser;
        this.visibility = visibility;
        if (null == requiredOpAuths) {
            this.requiredOpAuths = CollectionUtil.treeSet(user.getUserId());
        } else {
            this.requiredOpAuths = new TreeSet<>(requiredOpAuths);
        }

        userOpAuths = new HashSet<>(user.getOpAuths());
        userOpAuths.add(user.getUserId());
    }

    public void add(final String key, final Iterable<?> values) throws OperationException {
        if (null == values) {
            return;
        }

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
                        valueJson = jsonSerialiser.serialise(value);
                    }

                    return new Edge.Builder()
                            .group("result")
                            .source(jobId)
                            .dest(key)
                            .directed(true)
                            .property("opAuths", requiredOpAuths)
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
                                        .select("opAuths")
                                        .execute(new AreIn(userOpAuths))
                                        .build())
                                .build())
                        .build())
                .build();

        final CloseableIterable<Edge> edges = resultCache.execute(getEdges, user);
        if (null == edges) {
            return new WrappedCloseableIterable<>(Collections.emptyList());
        }
        return new TransformJsonResult(edges, jsonSerialiser);
    }

    private static class TransformJsonResult extends TransformIterable<Edge, Object> {
        private final JSONSerialiser jsonSerialiser;

        TransformJsonResult(final Iterable<Edge> input, final JSONSerialiser jsonSerialiser) {
            super(input, new AlwaysValid<>(), false, true);
            this.jsonSerialiser = jsonSerialiser;
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
                return jsonSerialiser.deserialise(resultBytes, resultClass);
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

    protected String getJobId() {
        return jobId;
    }

    protected User getUser() {
        return user;
    }

    protected Graph getResultCache() {
        return resultCache;
    }

    protected JSONSerialiser getJsonSerialiser() {
        return jsonSerialiser;
    }

    protected String getVisibility() {
        return visibility;
    }

    protected TreeSet<String> getRequiredOpAuths() {
        return requiredOpAuths;
    }

    protected Set<String> getUserOpAuths() {
        return userOpAuths;
    }
}
