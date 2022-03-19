#!/usr/bin/env bash
set -xe

# Clone master branch in /tmp and generate its documentation
cd /tmp
git clone https://gitlab-ci-token:"${CI_JOB_TOKEN}"@carvgit.ics.forth.gr/storage/tebis.git
cd tebis
doxygen Doxyfile

# Generate documentation for current branch
cd /builds/storage/tebis/
doxygen Doxyfile

pip install --no-cache-dir coverxygen
# Compare documentation coverage for the two branches
python3 scripts/CI/coverdocs.py "$CI_COMMIT_BRANCH" /builds/storage/tebis "$CI_DEFAULT_BRANCH" /tmp/tebis
