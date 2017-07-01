/*
 * Copyright 2017 Crown Copyright
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
package uk.gov.gchq.gaffer.flink.operation.handler;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.impl.add.AddElements;
import uk.gov.gchq.gaffer.store.Store;
import uk.gov.gchq.gaffer.store.StoreException;
import uk.gov.gchq.gaffer.store.StoreProperties;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.gaffer.user.User;

import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentLinkedQueue;

@SuppressFBWarnings(value = "SE_TRANSIENT_FIELD_NOT_RESTORED", justification = "There are null checks that will initialise the fields")
public class GafferSink extends RichSinkFunction<Iterable<? extends Element>> {
    private static final Logger LOGGER = LoggerFactory.getLogger(GafferSink.class);

    private static final long serialVersionUID = 1569145256866410621L;
    private final byte[] schema;
    private final StoreProperties storeProperties;

    private transient Store store;
    private transient ConcurrentLinkedQueue<Element> queue;
    private transient boolean restart;

    public GafferSink(final Store store) {
        schema = store.getSchema().toCompactJson();
        storeProperties = store.getProperties();
    }

    @Override
    public void open(final Configuration parameters) throws Exception {
        super.open(parameters);
        store.initialise(Schema.fromJson(schema), storeProperties);
    }

    @Override
    public void invoke(final Iterable<? extends Element> elements) throws Exception {
        if (null == queue) {
            queue = new ConcurrentLinkedQueue<>();
            restart = true;
        }

        if (null != elements) {
            for (final Element element : elements) {
                if (null != element) {
                    queue.add(element);
                }
            }
        }

        if (restart && !queue.isEmpty()) {
            restart = false;
            new Thread(() -> {
                try {
                    if (null == store) {
                        store.initialise(Schema.fromJson(schema), storeProperties);
                    }

                    final Iterable<Element> wrappedQueue = new Iterable<Element>() {
                        private boolean singleIteratorInUse = false;

                        @Override
                        public Iterator<Element> iterator() {
                            if (singleIteratorInUse) {
                                throw new RuntimeException("Only 1 iterator can be used at a time");
                            }
                            singleIteratorInUse = true;
                            return new Iterator<Element>() {
                                @Override
                                public boolean hasNext() {
                                    return !queue.isEmpty();
                                }

                                @Override
                                public Element next() {
                                    if (queue.isEmpty()) {
                                        throw new NoSuchElementException("No more elements");
                                    }

                                    return queue.poll();
                                }
                            };
                        }
                    };

                    store.execute(new AddElements.Builder()
                                    .input(wrappedQueue)
                                    .build(),
                            new User());

                    restart = true;

                } catch (final OperationException e) {
                    throw new RuntimeException(e);
                } catch (final StoreException e) {
                    throw new RuntimeException(e);
                }
            }).start();
        }
    }
}
