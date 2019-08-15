package uk.gov.gchq.gaffer.store.operation.handler;

import org.eclipse.jgit.api.Git;
import org.eclipse.jgit.api.errors.GitAPIException;
import org.eclipse.jgit.errors.RepositoryNotFoundException;

import java.io.File;
import java.io.IOException;

public class PullOrCloneRepo {
    public PullOrCloneRepo() {
    }

    /**
     * Pulls or clones repo of python scripts as needed
     */
    void pullOrClone(Git git, String pathAbsolutePythonRepo) {
        String repoURI = "https://github.com/g609bmsma/test";
        if (git == null) {
            try {
                git = Git.open(new File(pathAbsolutePythonRepo));
            } catch (final RepositoryNotFoundException e) {
                try {
                    git = Git.cloneRepository().setDirectory(new File(pathAbsolutePythonRepo)).setURI(repoURI).call();
                } catch (final GitAPIException e1) {
                    e1.printStackTrace();
                    git = null;
                }
            } catch (final IOException e) {
                e.printStackTrace();
                git = null;
            }
        }
        System.out.println("Fetching the repo...");
        File dir = new File(pathAbsolutePythonRepo);
        try {
            if (git != null) {
                System.out.println("Repo already cloned, pulling files...");
                git.pull().call();
                System.out.println("Pulled the latest files.");
            } else {
                System.out.println("Repo has not been cloned, cloning the repo...");
                Git.cloneRepository().setDirectory(dir).setURI(repoURI).call();
                System.out.println("Cloned the repo.");
            }
        } catch (final GitAPIException e) {
            e.printStackTrace();
        }
    }
}