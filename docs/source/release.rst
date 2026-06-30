Release Process
===============

This project uses CalVer for versioning.
Releases are generally kept in sync with the ``fsspec`` monthly release cycle.

Automated Release Workflow
--------------------------

The monthly release process is automated via GitHub Actions:

1. **Automated Preparation:**
   On the 5th of every month, the **Release Preparation** workflow runs automatically:
   - Calculates the next CalVer version.
   - Updates ``docs/source/changelog.rst`` with commit history since the last release.
   - Updates the minimum ``fsspec`` dependency in ``pyproject.toml``.
   - Creates a release branch and opens a Pull Request labeled ``autorelease`` (e.g., ``chore: release YYYY.M.PATCH``).

2. **Review and Merge:**
   Maintainers review the release PR and verify that CI and end-to-end tests pass. Merging the PR into ``main`` triggers the publishing phase.

3. **Automated Tagging & Publishing:**
   Upon merging the release PR, the **Release on Merge** workflow automatically:
   - Creates and pushes the git tag for the new version.
   - Runs CI and waits for Cloud Build end-to-end integration tests to pass.
   - Creates a GitHub Release with generated release notes.
   - Publishes the package to PyPI.

Manual Release Workflow
-----------------------

Maintainers can also execute a release manually:

1. **Pre-release steps:**
   - Verify that the CI pipeline is passing on the ``main`` branch.
   - Update ``docs/source/changelog.rst`` manually with the changes for the new version following the existing format.
   - Update the minimum ``fsspec`` dependency in ``pyproject.toml`` if necessary.
   - Commit the updates and merge them to the ``main`` branch.

2. **Execution:**
   - Create and push a git tag matching the CalVer version pattern (e.g., ``git tag 2026.6.0 && git push origin 2026.6.0``).
   - Pushing the tag directly triggers the **Release** workflow, which builds artifacts, runs end-to-end tests, creates a GitHub Release, and publishes to PyPI.

Verification
------------

1. Monitor the **Release on Merge** or **Release** workflow in the GitHub Actions tab.
2. Verify the new version is published on PyPI.

.. raw:: html

    <script data-goatcounter="https://gcsfs.goatcounter.com/count"
        async src="//gc.zgo.at/count.js"></script>
