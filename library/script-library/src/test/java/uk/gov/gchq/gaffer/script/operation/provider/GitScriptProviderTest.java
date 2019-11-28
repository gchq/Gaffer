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
package uk.gov.gchq.gaffer.script.operation.provider;

import org.junit.Assert;
import org.junit.Test;
import uk.gov.gchq.gaffer.script.operation.DockerFileUtils;
import uk.gov.gchq.gaffer.script.operation.ScriptTestConstants;

import java.nio.file.FileSystems;
import java.nio.file.Path;

public class GitScriptProviderTest {

    @Test
    public void shouldCloneIfNotAlreadyCloned() {
        // Given
        final GitScriptProvider gsp = new GitScriptProvider();
        final String currentWorkingDirectory = FileSystems.getDefault().getPath("").toAbsolutePath().toString();
        final String directoryPath = currentWorkingDirectory.concat(ScriptTestConstants.CURRENT_WORKING_DIRECTORY);
        final Path pathAbsoluteScriptRepo = DockerFileUtils.getPathAbsoluteScriptRepo(directoryPath, ScriptTestConstants.REPO_NAME);

        // When
        gsp.getScripts(pathAbsoluteScriptRepo.toString(), ScriptTestConstants.REPO_URI);
        final String[] files = pathAbsoluteScriptRepo.toFile().list();

        // Then
        Assert.assertNotNull(files);
    }

    @Test
    public void shouldPullIfAlreadyCloned() {
        // Given
        GitScriptProvider gsp = new GitScriptProvider();
        final String currentWorkingDirectory = FileSystems.getDefault().getPath(".").toAbsolutePath().toString();
        final String directoryPath = currentWorkingDirectory.concat(ScriptTestConstants.CURRENT_WORKING_DIRECTORY);

        Path pathAbsoluteScriptRepo = DockerFileUtils.getPathAbsoluteScriptRepo(directoryPath, ScriptTestConstants.REPO_NAME);

        //When
        for (int i = 0; i < 2; i++) {
            gsp.getScripts(pathAbsoluteScriptRepo.toString(), ScriptTestConstants.REPO_URI);
        }
    }
}

