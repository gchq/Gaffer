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

import com.google.common.collect.Iterables;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.Validatable;
import uk.gov.gchq.gaffer.operation.impl.add.AddElements;
import uk.gov.gchq.gaffer.store.Store;
import uk.gov.gchq.gaffer.store.StoreProperties;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.gaffer.user.User;
import javax.annotation.Nonnull;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Properties;
import java.util.concurrent.ConcurrentLinkedQueue;

@SuppressFBWarnings(value = "SE_TRANSIENT_FIELD_NOT_RESTORED", justification = "There are null checks that will initialise the fields")
public class GafferSink extends RichSinkFunction<Iterable<? extends Element>> {
    private static final long serialVersionUID = 1569145256866410621L;
    private final byte[] schema;
    private final Properties properties;
    private final boolean validate;
    private final boolean skipInvalid;

    private transient Store store;
    private transient GafferQueue<Element> queue;
    private transient boolean restart;

    public GafferSink(final Validatable validatable, final Store store) {
        this.store = store;
        this.validate = validatable.isValidate();
        this.skipInvalid = validatable.isSkipInvalidElements();
        schema = store.getSchema().toCompactJson();
        properties = store.getProperties().getProperties();
    }

    @Override
    public void invoke(final Iterable<? extends Element> elements) throws Exception {
        if (null == queue) {
            queue = new GafferQueue<>();
            restart = true;
        }

        if (null != elements) {
            Iterables.addAll(queue, elements);
        }

        if (restart && !queue.isEmpty()) {
            restart = false;
            checkStore();
            store.run(() -> {
                try {
                    store.execute(new AddElements.Builder()
                                    .input(queue)
                                    .validate(validate)
                                    .skipInvalidElements(skipInvalid)
                                    .build(),
                            new User());
                    restart = true;
                } catch (final OperationException e) {
                    throw new RuntimeException(e);
                }
            });
        }
    }

    private void checkStore() {
        if (null == store) {
            store = Store.createStore(Schema.fromJson(schema), StoreProperties.loadStoreProperties(properties));
        }
    }

    private static final class GafferQueue<T> extends ConcurrentLinkedQueue<T> {
        private static final long serialVersionUID = -5222649835225228337L;

        @Override
        @Nonnull
        public Iterator<T> iterator() {
            return new Iterator<T>() {
                @Override
                public boolean hasNext() {
                    return !isEmpty();
                }

                @Override
                public T next() {
                    if (!hasNext()) {
                        throw new NoSuchElementException("No more elements");
                    }
                    return poll();
                }
            };
        }
    }
}
