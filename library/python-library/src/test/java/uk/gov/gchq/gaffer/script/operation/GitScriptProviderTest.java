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
package uk.gov.gchq.gaffer.script.operation;

import org.eclipse.jgit.api.Git;
import org.junit.Assert;
import org.junit.Test;

import java.nio.file.FileSystems;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class GitScriptProviderTest {

    @Test
    public void shouldCloneIfNotAlreadyCloned() {
        // Given
        GitScriptProvider pOrC = new GitScriptProvider();
        Git git = null;
        final String currentWorkingDirectory = FileSystems.getDefault().getPath("").toAbsolutePath().toString();
        final String directoryPath = currentWorkingDirectory.concat(ScriptTestConstants.CURRENT_WORKING_DIRECTORY);

        Path pathAbsoluteScriptRepo = DockerFileUtils.getPathAbsoluteScriptRepo(directoryPath, ScriptTestConstants.REPO_NAME);

        final RunScript<String, String> operation =
                new RunScript.Builder<String, String>()
                        .build();


        // When
        pOrC.getScripts(pathAbsoluteScriptRepo.toString(), ScriptTestConstants.REPO_URI);
        String[] files = pathAbsoluteScriptRepo.toFile().list();

        // Then
        Assert.assertNotNull(files);
    }

    @Test
    public void shouldPullIfAlreadyCloned() {
        // Given
        GitScriptProvider pOrC = new GitScriptProvider();
        Git git = mock(Git.class);
        final String currentWorkingDirectory = FileSystems.getDefault().getPath(".").toAbsolutePath().toString();
        final String directoryPath = currentWorkingDirectory.concat(ScriptTestConstants.CURRENT_WORKING_DIRECTORY);

        Path pathAbsoluteScriptRepo = DockerFileUtils.getPathAbsoluteScriptRepo(directoryPath, ScriptTestConstants.REPO_NAME);

        final RunScript<String, String> operation =
                new RunScript.Builder<String, String>()
                        .build();

        // When
        when(git.pull()).thenThrow(new NullPointerException("Pull method called") {
        });

        // Then
        Exception exception = assertThrows(NullPointerException.class, () -> pOrC.getScripts(pathAbsoluteScriptRepo.toString(), ScriptTestConstants.REPO_URI));
        Assert.assertEquals("Pull method called", exception.getMessage());

    }
}

