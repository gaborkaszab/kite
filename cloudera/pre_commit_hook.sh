#!/usr/bin/env bash
set -e

export JAVA8_BUILD=true
. /opt/toolchain/toolchain.sh
export PATH=$MAVEN_3_5_0_HOME/bin:$PATH

# instead of using mvn wrapper, we invoke mvn-gbn directly so it grabs the mvn
# from the path and picks up 3.5.0
mvn-gbn -s mvn_settings.xml --update-snapshots --batch-mode -Dmaven.test.failure.ignore=true -Dtests.nightly=true --fail-at-end clean verify
