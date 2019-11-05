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
package uk.gov.gchq.gaffer.python.operation;

import org.eclipse.jgit.api.Git;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.nio.file.Paths;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class PullOrCloneGitRepoTest  {

    @Test
    public void shouldCloneIfNotAlreadyCloned() {
        //Given
        PullOrCloneGitRepo pOrC = new PullOrCloneGitRepo();
        Git git = null;
        final String repoName = "test";
        final String repoURI = "https://github.com/g609bmsma/test";
        final String currentWorkingDirectory = FileSystems.getDefault().getPath(".").toAbsolutePath().toString();
        final String directoryPath = currentWorkingDirectory.concat("PythonBin");
        final File directory = new File(directoryPath);
        if (!directory.exists()) {
            directory.mkdir();
        }
        final Path pathAbsolutePythonRepo = Paths.get(directoryPath, repoName);
        BuildImageFromDockerfile bIFD = new BuildImageFromDockerfile();
        try {
            bIFD.getFiles(pathAbsolutePythonRepo.toString());
        } catch (IOException e) {
            e.printStackTrace();
        }
        final RunPythonScript<String, String> operation =
                new RunPythonScript.Builder<String, String>()
                        .build();

        //When
        pOrC.pullOrClone(git, pathAbsolutePythonRepo.toString(), repoURI);
        String[] files = pathAbsolutePythonRepo.toFile().list();

        //Then
        Assert.assertNotNull(files);
    }

    @Test
    public void shouldPullIfAlreadyCloned() {
        //Given
        PullOrCloneGitRepo pOrC = new PullOrCloneGitRepo();
        Git git = mock(Git.class);
        final String repoName = "test";
        final String repoURI = "https://github.com/g609bmsma/test";
        final String currentWorkingDirectory = FileSystems.getDefault().getPath(".").toAbsolutePath().toString();
        final String directoryPath = currentWorkingDirectory.concat("PythonBin");
        final File directory = new File(directoryPath);
        if (!directory.exists()) {
            directory.mkdir();
        }
        final Path pathAbsolutePythonRepo = Paths.get(directoryPath, repoName);
        BuildImageFromDockerfile bIFD = new BuildImageFromDockerfile();
        try {
            bIFD.getFiles(pathAbsolutePythonRepo.toString());
        } catch (IOException e) {
            e.printStackTrace();
        }
        final RunPythonScript<String, String> operation =
                new RunPythonScript.Builder<String, String>()
                        .build();

        //When
        when(git.pull()).thenThrow(new NullPointerException("Pull method called") {
        });

        //Then
        Exception exception = assertThrows(NullPointerException.class, () -> pOrC.pullOrClone(git, pathAbsolutePythonRepo.toString(), repoURI));
        Assert.assertEquals("Pull method called", exception.getMessage());

    }
}

