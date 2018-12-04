/*
 * Copyright 2018 Crown Copyright
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
import uk.gov.gchq.gaffer.data.generator.JsonToElementGenerator;
import uk.gov.gchq.gaffer.integration.impl.loader.ParameterizedLoaderIT;
import uk.gov.gchq.gaffer.integration.impl.loader.schemas.SchemaLoader;
import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.impl.add.AddElementsFromFile;
import uk.gov.gchq.gaffer.store.StoreProperties;
import uk.gov.gchq.gaffer.store.schema.TestSchema;
import uk.gov.gchq.gaffer.user.User;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

//TODO - Run on Accumulo
public class AddElementsFromFileLoaderIT extends ParameterizedLoaderIT<AddElementsFromFile> {
    @Rule
    public final TemporaryFolder testFolder = new TemporaryFolder(CommonTestConstants.TMP_DIRECTORY);
    private File file;

    public AddElementsFromFileLoaderIT(final TestSchema schema, final SchemaLoader loader, final Map<String, User> userMap) {
        super(schema, loader, userMap);
        StoreProperties props = getStoreProperties();
        props.addOperationDeclarationPaths("../../library/flink-library/src/main/resources/FlinkOperationDeclarations.json");
        setStoreProperties(props);
    }

    @Override
    protected void addElements(final Iterable<? extends Element> input) throws OperationException {
        try {
            createInputFiles(input);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        graph.execute(new AddElementsFromFile.Builder()
                .filename(file.getAbsolutePath())
                .generator(JsonToElementGenerator.class)
                .validate(false)
                .skipInvalidElements(false)
                .build(), getUser());
    }

    private void createInputFiles(final Iterable<? extends Element> elements) throws Exception {
        file = testFolder.newFile("inputFile.txt");

        final List<String> lines = new ArrayList<>();

        for (final Element element : elements) {
            final String json = JSONSerialiser.createDefaultMapper().writeValueAsString(element);
            lines.add(json);
        }
        FileUtils.writeLines(file, lines);
    }
}