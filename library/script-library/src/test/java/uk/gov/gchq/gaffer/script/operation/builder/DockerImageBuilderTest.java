/*
 * Copyright 2019 Crown Copyright
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
package uk.gov.gchq.gaffer.script.operation.builder;

import org.junit.Assert;
import org.junit.Test;

import uk.gov.gchq.gaffer.script.operation.DockerFileUtils;
import uk.gov.gchq.gaffer.script.operation.ScriptTestConstants;
import uk.gov.gchq.gaffer.script.operation.image.Image;
import uk.gov.gchq.gaffer.script.operation.provider.GitScriptProvider;

import java.nio.file.FileSystems;
import java.nio.file.Path;

public class DockerImageBuilderTest {

    @Test
    public void shouldBuildImage() {

        // Given
        final String currentWorkingDirectory = FileSystems.getDefault().getPath("").toAbsolutePath().toString();
        final String directoryPath = currentWorkingDirectory.concat(ScriptTestConstants.CURRENT_WORKING_DIRECTORY);
        Path pathAbsoluteScriptRepo = DockerFileUtils.getPathAbsoluteScriptRepo(directoryPath, ScriptTestConstants.REPO_NAME);
        DockerImageBuilder bIFD = new DockerImageBuilder();

        final GitScriptProvider pOrC = new GitScriptProvider();
        pOrC.getScripts(pathAbsoluteScriptRepo.toString(), ScriptTestConstants.REPO_URI);

        // When
        bIFD.getFiles(directoryPath, "");
        Image returnedImage = bIFD.buildImage(ScriptTestConstants.SCRIPT_NAME, null, directoryPath);
        String returnedImageId = returnedImage.getImageString();

        // Then
        Assert.assertNotNull(returnedImageId);
    }
}
