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
package uk.gov.gchq.gaffer.script.operation.platform;

import com.spotify.docker.client.DefaultDockerClient;
import com.spotify.docker.client.DockerClient;
import com.spotify.docker.client.exceptions.DockerCertificateException;
import com.spotify.docker.client.exceptions.DockerException;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.gov.gchq.gaffer.script.operation.ScriptTestConstants;
import uk.gov.gchq.gaffer.script.operation.builder.DockerImageBuilder;
import uk.gov.gchq.gaffer.script.operation.container.Container;
import uk.gov.gchq.gaffer.script.operation.container.LocalDockerContainer;
import uk.gov.gchq.gaffer.script.operation.handler.RunScriptHandler;
import uk.gov.gchq.gaffer.script.operation.image.Image;

import java.io.IOException;
import java.nio.file.FileSystems;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeoutException;

public class LocalDockerPlatformTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(LocalDockerPlatformTest.class);

    @Test
    public void shouldCreateAContainer() {
        // Given
        LocalDockerPlatform platform = new LocalDockerPlatform();
        final String currentWorkingDirectory = FileSystems.getDefault().getPath("").toAbsolutePath().toString();
        final String directoryPath = currentWorkingDirectory.concat("/src/main/resources/" + ".ScriptBin");
        DockerImageBuilder imageBuilder = new DockerImageBuilder();
        try {
            imageBuilder.getFiles(directoryPath, "");
        } catch (final IOException e) {
            Assert.fail("Failed to get the build files");
        }
        DockerClient docker = null;
        try {
            docker = DefaultDockerClient.fromEnv().build();
        } catch (DockerCertificateException e) {
            LOGGER.error(e.getMessage());
        }
        Image image = null;
        try {
            image = platform.buildImage(ScriptTestConstants.SCRIPT_NAME, null, directoryPath);
        } catch (Exception e) {
            LOGGER.error(e.getMessage());
        }

        // When
        LocalDockerContainer container = null;
        try {
            container = (LocalDockerContainer) platform.createContainer(image);
        } catch (Exception e) {
            e.printStackTrace();
        }

        try {
            if (docker != null) {
                docker.removeContainer(container.getContainerId());
            }
        } catch (DockerException | InterruptedException e) {
            LOGGER.error(e.getMessage());
        }

        // Then
        Assert.assertTrue(container instanceof LocalDockerContainer);
        Assert.assertNotNull(container);
    }

    @Test
    public void shouldRunTheContainer() {
        // Given
        LocalDockerPlatform platform = new LocalDockerPlatform();
        final String currentWorkingDirectory = FileSystems.getDefault().getPath("").toAbsolutePath().toString();
        final String directoryPath = currentWorkingDirectory.concat("/src/main/resources/" + ".ScriptBin");
        DockerImageBuilder imageBuilder = new DockerImageBuilder();
        try {
            imageBuilder.getFiles(directoryPath, "");
        } catch (final IOException e) {
            Assert.fail("Failed to get the build files");
        }
        Image image = null;
        try {
            image = platform.buildImage(ScriptTestConstants.SCRIPT_NAME, null, directoryPath);
        } catch (Exception e) {
            LOGGER.error(e.getMessage());
        }
        Container container = null;
        try {
            container = platform.createContainer(image);
        } catch (Exception e) {
            e.printStackTrace();
        }
        List<String> data = new ArrayList<>();
        data.add("testData");

        // When
        StringBuilder result = null;
        try {
            platform.runContainer(container, data);
            result = (StringBuilder) new RunScriptHandler().receiveData(container.receiveData());
        } catch (final DockerException | InterruptedException | IOException | TimeoutException e) {
            Assert.fail();
        }

        // Then
        Assert.assertEquals("[\"testData\"]", result.toString());
    }
}
