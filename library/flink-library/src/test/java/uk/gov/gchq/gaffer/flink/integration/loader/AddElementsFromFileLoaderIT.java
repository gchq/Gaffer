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

package uk.gov.gchq.gaffer.flink.integration.loader;

import org.apache.commons.io.FileUtils;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;

import uk.gov.gchq.gaffer.commonutil.CommonTestConstants;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.integration.AbstractStoreIT;
import uk.gov.gchq.gaffer.integration.generators.JsonToElementGenerator;
import uk.gov.gchq.gaffer.integration.impl.loader.AbstractLoaderIT;
import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.impl.add.AddElementsFromFile;
import uk.gov.gchq.gaffer.store.StoreProperties;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

public class AddElementsFromFileLoaderIT extends AbstractLoaderIT<AddElementsFromFile> {

    @Rule
    public final TemporaryFolder testFolder = new TemporaryFolder(CommonTestConstants.TMP_DIRECTORY);
    private File file;

    @Override
    protected void configure(final Iterable<? extends Element> elements) throws Exception {
        file = testFolder.newFile("inputFile.txt");

        final List<String> lines = new ArrayList<>();

        for (final Element element : elements) {
            final String json = JSONSerialiser.createDefaultMapper().writeValueAsString(element);
            lines.add(json);
        }

        FileUtils.writeLines(file, lines);

        final StoreProperties storeProperties = getStoreProperties();
        storeProperties.addOperationDeclarationPaths("FlinkOperationDeclarations.json");
        AbstractStoreIT.setStoreProperties(storeProperties);
    }

    @Override
    protected AddElementsFromFile createOperation(final Iterable<? extends Element> elements) {
        return new AddElementsFromFile.Builder()
                .filename(file.getAbsolutePath())
                .generator(JsonToElementGenerator.class)
                .validate(true)
                .skipInvalidElements(false)
                .build();
    }

    @Override
    protected void addElements() throws OperationException {
        graph.execute(createOperation(input), getUser());

        // Wait for elements to be ingested.
        try {
            Thread.sleep(2000);
        } catch (final InterruptedException ex) {
            throw new OperationException(ex);
        }
    }
}
