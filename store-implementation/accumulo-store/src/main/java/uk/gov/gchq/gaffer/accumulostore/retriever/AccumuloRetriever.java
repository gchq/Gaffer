/*
 * Copyright 2016-2018 Crown Copyright
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

package uk.gov.gchq.gaffer.accumulostore.retriever;

import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.gov.gchq.gaffer.accumulostore.AccumuloStore;
import uk.gov.gchq.gaffer.accumulostore.key.AccumuloElementConverter;
import uk.gov.gchq.gaffer.accumulostore.key.IteratorSettingFactory;
import uk.gov.gchq.gaffer.accumulostore.key.RangeFactory;
import uk.gov.gchq.gaffer.commonutil.iterable.CloseableIterable;
import uk.gov.gchq.gaffer.commonutil.iterable.CloseableIterator;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.function.ElementFilter;
import uk.gov.gchq.gaffer.data.element.function.ElementTransformer;
import uk.gov.gchq.gaffer.data.elementdefinition.view.ViewElementDefinition;
import uk.gov.gchq.gaffer.operation.graph.GraphFilters;
import uk.gov.gchq.gaffer.operation.io.Output;
import uk.gov.gchq.gaffer.store.StoreException;
import uk.gov.gchq.gaffer.user.User;

import java.util.Set;

public abstract class AccumuloRetriever<OP extends Output & GraphFilters, O_ITEM> implements CloseableIterable<O_ITEM> {
    private static final Logger LOGGER = LoggerFactory.getLogger(AccumuloRetriever.class);

    protected CloseableIterator<O_ITEM> iterator;
    protected final AccumuloStore store;
    protected final Authorizations authorisations;
    protected final User user;
    protected final RangeFactory rangeFactory;
    protected final IteratorSettingFactory iteratorSettingFactory;
    protected final OP operation;
    protected final AccumuloElementConverter elementConverter;
    protected final IteratorSetting[] iteratorSettings;

    protected AccumuloRetriever(final AccumuloStore store, final OP operation,
                                final User user, final IteratorSetting... iteratorSettings)
            throws StoreException {
        this.store = store;
        this.rangeFactory = store.getKeyPackage().getRangeFactory();
        this.iteratorSettingFactory = store.getKeyPackage().getIteratorFactory();
        this.elementConverter = store.getKeyPackage().getKeyConverter();
        this.operation = operation;
        this.iteratorSettings = iteratorSettings;
        this.user = user;
        if (null != user && null != user.getDataAuths()) {
            this.authorisations = new Authorizations(
                    user.getDataAuths().toArray(new String[user.getDataAuths().size()]));
        } else {
            this.authorisations = new Authorizations();
        }
    }

    /**
     * Performs any transformations specified in a view on an element
     *
     * @param element the element to transform
     */
    public void doTransformation(final Element element) {
        final ViewElementDefinition viewDef = operation.getView().getElement(element.getGroup());
        if (null != viewDef) {
            transform(element, viewDef.getTransformer());
        }
    }

    /**
     * Performs any post Filtering specified in a view on an element
     *
     * @param element the element to post Filter
     * @return the result of validating the element against the post filters
     */
    public boolean doPostFilter(final Element element) {
        final ViewElementDefinition viewDef = operation.getView().getElement(element.getGroup());
        if (null != viewDef) {
            return postFilter(element, viewDef.getPostTransformFilter());
        }
        return true;
    }

    @Override
    public void close() {
        if (null != iterator) {
            iterator.close();
        }
    }

    /**
     * Create a scanner to use used in your query.
     * <p>
     *
     * @param ranges the ranges to get the scanner for
     * @return A {@link org.apache.accumulo.core.client.BatchScanner} for the
     * table specified in the properties with the ranges provided.
     * @throws TableNotFoundException if an accumulo table could not be found
     * @throws StoreException         if a connection to accumulo could not be created.
     */
    protected BatchScanner getScanner(final Set<Range> ranges) throws TableNotFoundException, StoreException {
        final BatchScanner scanner = store.getConnection().createBatchScanner(store.getTableName(),
                authorisations, store.getProperties().getThreadsForBatchScanner());
        LOGGER.debug("Initialised BatchScanner on table {} with authorisations {} using {} threads",
                store.getTableName(), authorisations, store.getProperties().getThreadsForBatchScanner());
        if (null != iteratorSettings) {
            for (final IteratorSetting iteratorSetting : iteratorSettings) {
                if (null != iteratorSetting) {
                    scanner.addScanIterator(iteratorSetting);
                    LOGGER.debug("Added iterator to BatchScanner: {}", iteratorSetting);
                }
            }
        }
        scanner.setRanges(ranges);
        LOGGER.debug("Added {} ranges to BatchScanner", ranges.size());

        for (final String col : operation.getView().getEdgeGroups()) {
            scanner.fetchColumnFamily(new Text(col));
            LOGGER.debug("Added {} as a column family to fetch", col);
        }
        for (final String col : operation.getView().getEntityGroups()) {
            scanner.fetchColumnFamily(new Text(col));
            LOGGER.debug("Added {} as a column family to fetch", col);
        }
        return scanner;
    }

    protected void transform(final Element element, final ElementTransformer transformer) {
        if (null != transformer) {
            transformer.apply(element);
        }
    }

    protected boolean postFilter(final Element element, final ElementFilter postFilter) {
        return null == postFilter || postFilter.test(element);
    }
}
