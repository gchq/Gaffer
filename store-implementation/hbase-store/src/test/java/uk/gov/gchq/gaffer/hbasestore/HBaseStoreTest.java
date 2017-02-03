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

package uk.gov.gchq.gaffer.hbasestore;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.gov.gchq.gaffer.commonutil.StreamUtil;
import uk.gov.gchq.gaffer.commonutil.TestGroups;
import uk.gov.gchq.gaffer.commonutil.iterable.CloseableIterable;
import uk.gov.gchq.gaffer.data.element.Edge;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.data.element.function.ElementFilter;
import uk.gov.gchq.gaffer.data.elementdefinition.view.View;
import uk.gov.gchq.gaffer.data.elementdefinition.view.ViewElementDefinition;
import uk.gov.gchq.gaffer.function.filter.IsMoreThan;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.impl.add.AddElements;
import uk.gov.gchq.gaffer.operation.impl.get.GetAllElements;
import uk.gov.gchq.gaffer.store.StoreException;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.gaffer.user.User;

public class HBaseStoreTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(HBaseStore.class);

    private static final HBaseProperties PROPERTIES = HBaseProperties.loadStoreProperties(StreamUtil.storeProps(HBaseStoreTest.class));
    private static final Schema SCHEMA = Schema.fromJson(StreamUtil.schemas(HBaseStoreTest.class));

    @Test
    public void test() throws StoreException, OperationException {
        final MiniHBaseStore store = new MiniHBaseStore();
        store.initialise(SCHEMA, PROPERTIES);

        store.execute(new AddElements.Builder()
                .elements(new Edge.Builder()
                                .source("source")
                                .dest("dest")
                                .group(TestGroups.EDGE)
                                .property("property1", 1)
                                .property("columnQualifier", 10)
                                .property("visibility", "public")
                                .build(),
                        new Edge.Builder()
                                .source("source")
                                .dest("dest")
                                .group(TestGroups.EDGE)
                                .property("property1", 5)
                                .property("columnQualifier", 10)
                                .property("visibility", "public")
                                .build(),
                        new Entity.Builder()
                                .vertex("vertex")
                                .group(TestGroups.ENTITY)
                                .property("property1", 2)
                                .property("columnQualifier", 20)
                                .property("visibility", "private")
                                .build())
                .build(), new User());
        final CloseableIterable<Element> elements = store.execute(new GetAllElements.Builder()
                .view(new View.Builder()
                        .edge(TestGroups.EDGE, new ViewElementDefinition.Builder()
                                .preAggregationFilter(new ElementFilter.Builder()
                                        .select("property1")
                                        .execute(new IsMoreThan(1))
                                        .build())
                                .build())
                        .build())
                .build(), new User());
        for (Element element : elements) {
            LOGGER.info("Element: " + element);
        }
    }
}
