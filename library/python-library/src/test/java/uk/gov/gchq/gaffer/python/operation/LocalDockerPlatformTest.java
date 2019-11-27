package uk.gov.gchq.gaffer.python.operation;

import com.spotify.docker.client.DefaultDockerClient;
import com.spotify.docker.client.DockerClient;
import com.spotify.docker.client.exceptions.DockerCertificateException;
import com.spotify.docker.client.exceptions.DockerException;
import org.eclipse.jgit.api.Git;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Path;
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
        Map<String, Object> scriptParameters = null;
        final String currentWorkingDirectory = FileSystems.getDefault().getPath("").toAbsolutePath().toString();
        final String directoryPath = currentWorkingDirectory.concat("/src/main/resources/" + ".ScriptBin");
        String ip = "127.0.0.1";

        // When
        Container container = platform.createContainer(scriptName, scriptParameters, directoryPath, ip);

        // Then
        Assert.assertTrue(container instanceof LocalDockerContainer);
        Assert.assertNotNull(container);
    }
}
