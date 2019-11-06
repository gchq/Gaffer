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

import com.spotify.docker.client.DefaultDockerClient;
import com.spotify.docker.client.DockerClient;
import com.spotify.docker.client.exceptions.DockerCertificateException;
import com.spotify.docker.client.exceptions.DockerException;
import org.eclipse.jgit.api.Git;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Path;

public class BuildImageFromDockerfileTest {

    @Test
    public void shouldBuildImage() {
        // Given

        DockerClient docker = null;
        Git git = null;
        final String currentWorkingDirectory = FileSystems.getDefault().getPath(".").toAbsolutePath().toString();
        final String directoryPath = currentWorkingDirectory.concat(PythonTestConstants.CURRENTWORKINGDIRECTORY);
        Path pathAbsolutePythonRepo = DockerFileUtils.getPathAbsolutePythonRepo(directoryPath, PythonTestConstants.REPONAME);
        BuildImageFromDockerfile bIFD = new BuildImageFromDockerfile();

        final RunPythonScript<String, String> operation =
                new RunPythonScript.Builder<String, String>()
                        .build();
        final PullOrCloneGitRepo pOrC = new PullOrCloneGitRepo();
        pOrC.pullOrClone(git, pathAbsolutePythonRepo.toString(), PythonTestConstants.REPOURI);

        try {
            docker = DefaultDockerClient.fromEnv().build();
        } catch (DockerCertificateException e) {
            e.printStackTrace();
        }

        // When
        String returnedImageId = null;
        try {
            bIFD.getFiles(directoryPath);
            returnedImageId = bIFD.buildImage("script1", null, ScriptInputType.DATAFRAME, docker,
                    directoryPath);
        } catch (DockerException | InterruptedException | IOException e) {
            e.printStackTrace();
            Assert.fail();
        }

        // Then
        Assert.assertNotNull(returnedImageId);
    }
}
