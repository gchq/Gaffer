/*
 * Copyright 2017. Crown Copyright
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
package uk.gov.gchq.gaffer.parquetstore.operation.getelements.impl;


import org.apache.hadoop.fs.Path;
import org.apache.parquet.filter2.compat.FilterCompat;
import org.apache.parquet.filter2.predicate.FilterPredicate;
import org.apache.parquet.hadoop.ParquetReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.gov.gchq.gaffer.commonutil.elementvisibilityutil.Authorisations;
import uk.gov.gchq.gaffer.commonutil.elementvisibilityutil.ElementVisibility;
import uk.gov.gchq.gaffer.commonutil.elementvisibilityutil.VisibilityEvaluator;
import uk.gov.gchq.gaffer.commonutil.elementvisibilityutil.exception.VisibilityParseException;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.function.ElementFilter;
import uk.gov.gchq.gaffer.data.elementdefinition.view.View;
import uk.gov.gchq.gaffer.data.elementdefinition.view.ViewElementDefinition;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.parquetstore.io.reader.ParquetElementReader;
import uk.gov.gchq.gaffer.parquetstore.utils.GafferGroupObjectConverter;
import uk.gov.gchq.gaffer.parquetstore.utils.SchemaUtils;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.gaffer.user.User;

import java.io.IOException;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Used to retrieve the elements from a single file and put the elements into a shared {@link java.util.concurrent.ConcurrentLinkedQueue}
 */
public class RetrieveElementsFromFile implements Callable<OperationException> {

    private static final Logger LOGGER = LoggerFactory.getLogger(RetrieveElementsFromFile.class);
    private final Path filePath;
    private final FilterPredicate filter;
    private transient byte[] jsonGafferSchema;
    private final ConcurrentLinkedQueue<Element> queue;
    private transient ElementFilter elementFilter;
    private final byte[] elementDefinitionJson;
    private final boolean needsValidation;
    private final String group;
    private final Authorisations auths;
    private final String visibility;

    public RetrieveElementsFromFile(final Path filePath, final FilterPredicate filter, final Schema gafferSchema,
                                    final ConcurrentLinkedQueue<Element> queue, final boolean needsValidation,
                                    final View view, final User user) {
        this.filePath = filePath;
        this.filter = filter;
        this.jsonGafferSchema = gafferSchema.toCompactJson();

        if (gafferSchema.getVisibilityProperty() != null) {
            this.visibility = gafferSchema.getVisibilityProperty();
        } else {
            this.visibility = new String();
        }

        if (user != null && user.getDataAuths() != null) {
            this.auths = new Authorisations(user.getDataAuths().toArray(new String[user.getDataAuths().size()]));
        } else {
            this.auths = new Authorisations();
        }

        this.queue = queue;
        this.needsValidation = needsValidation;
        if (filePath.getName().contains("=")) {
            group = filePath.getName().split("=")[1];
        } else {
            group = filePath.getParent().getName().split("=")[1];
        }
        elementDefinitionJson = view.getElement(group).toCompactJson();
    }

    @Override
    public OperationException call() throws Exception {
        if (null == elementFilter) {
            elementFilter = new ViewElementDefinition.Builder().json(elementDefinitionJson).build().getPreAggregationFilter();
        }
        try {
            final ParquetReader<Element> fileReader = openParquetReader();
            Element e = fileReader.read();
            while (e != null) {
                if (!visibility.isEmpty()) {
                    if (isVisible(e)) {
                        if (needsValidation) {
                            if (elementFilter.test(e)) {
                                queue.add(e);
                            }
                        } else {
                            queue.add(e);
                        }
                    }
                } else if (needsValidation) {
                    if (elementFilter.test(e)) {
                        queue.add(e);
                    }
                } else {
                    queue.add(e);
                }
                e = fileReader.read();
            }
            fileReader.close();
        } catch (final IOException ignore) {
            // ignore as this file does not exist
        }
        return null;
    }

    private ParquetReader<Element> openParquetReader() throws IOException {
        final SchemaUtils schemaUtils = new SchemaUtils(Schema.fromJson(jsonGafferSchema));
        final boolean isEntity = schemaUtils.getEntityGroups().contains(group);
        final GafferGroupObjectConverter converter = schemaUtils.getConverter(group);
        LOGGER.debug("Opening a new Parquet reader for file: {}", filePath);
        if (null != filter) {
            return new ParquetElementReader.Builder<Element>(filePath)
                    .isEntity(isEntity)
                    .usingConverter(converter)
                    .withFilter(FilterCompat.get(filter))
                    .build();
        } else {
            return new ParquetElementReader.Builder<Element>(filePath)
                    .isEntity(isEntity)
                    .usingConverter(converter)
                    .build();
        }
    }

    private Boolean isVisible(final Element e) throws VisibilityParseException {
        if (e.getProperty(visibility) != null) {
            final VisibilityEvaluator visibilityEvaluator = new VisibilityEvaluator(auths);
            final ElementVisibility elementVisibility = new ElementVisibility((String) e.getProperty(visibility));
            return visibilityEvaluator.evaluate(elementVisibility);
        } else {
            e.putProperty(visibility, new String());
            return true;
        }
    }
}
