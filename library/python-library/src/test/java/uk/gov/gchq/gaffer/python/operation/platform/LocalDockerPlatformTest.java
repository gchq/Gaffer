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
package uk.gov.gchq.gaffer.python.operation.platform;

import com.spotify.docker.client.DefaultDockerClient;
import com.spotify.docker.client.DockerClient;
import com.spotify.docker.client.exceptions.DockerCertificateException;
import com.spotify.docker.client.exceptions.DockerException;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import uk.gov.gchq.gaffer.python.operation.DockerFileUtils;
import uk.gov.gchq.gaffer.python.operation.ScriptTestConstants;
import uk.gov.gchq.gaffer.python.operation.container.Container;
import uk.gov.gchq.gaffer.python.operation.container.LocalDockerContainer;
import uk.gov.gchq.gaffer.python.operation.platform.LocalDockerPlatform;
import uk.gov.gchq.gaffer.python.operation.provider.GitScriptProvider;

import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class LocalDockerPlatformTest {

    @Before
    public void setup() {
        GitScriptProvider scriptProvider = new GitScriptProvider();
        final String currentWorkingDirectory = FileSystems.getDefault().getPath("").toAbsolutePath().toString();
        final String directoryPath = currentWorkingDirectory.concat(ScriptTestConstants.CURRENT_WORKING_DIRECTORY);
        Path pathAbsoluteScriptRepo = DockerFileUtils.getPathAbsoluteScriptRepo(directoryPath, ScriptTestConstants.REPO_NAME);
        scriptProvider.getScripts(pathAbsoluteScriptRepo.toString(), ScriptTestConstants.REPO_URI);
    }

    @Test
    public void shouldCreateAContainer() {
        // Given
        LocalDockerPlatform platform = new LocalDockerPlatform();
        String scriptName = "script1";
        final String currentWorkingDirectory = FileSystems.getDefault().getPath("").toAbsolutePath().toString();
        final String directoryPath = currentWorkingDirectory.concat("/src/main/resources/" + ".ScriptBin");

        // When
        LocalDockerContainer container = (LocalDockerContainer) platform.createContainer(scriptName, null, directoryPath, ScriptTestConstants.LOCALHOST);

        try {
            DockerClient docker = DefaultDockerClient.fromEnv().build();
            docker.removeContainer(container.getContainerId());
        } catch (DockerException | InterruptedException | DockerCertificateException e) {
            e.printStackTrace();
        }

        // Then
        Assert.assertTrue(container instanceof LocalDockerContainer);
        Assert.assertNotNull(container);
    }

    @Test
    public void shouldRunTheContainer() {
        // Given
        LocalDockerPlatform platform = new LocalDockerPlatform();
        String scriptName = "script1";
        Map<String, Object> scriptParameters = null;
        final String currentWorkingDirectory = FileSystems.getDefault().getPath("").toAbsolutePath().toString();
        final String directoryPath = currentWorkingDirectory.concat("/src/main/resources/" + ".ScriptBin");
        String ip = "127.0.0.1";
        Container container = platform.createContainer(scriptName, scriptParameters, directoryPath, ip);
        List data = new ArrayList();
        data.add("testData");

        // When
        StringBuilder result = platform.runContainer(container, data);

        // Then
        Assert.assertEquals("[\"testData\"]", result.toString());
    }
}
