#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#

# Abort unless the user answers "y".
confirm() {
  read -p "$1 (y to proceed) " answer
  if ! [[ "$answer" == "y" ]]; then
    echo "Aborted."
    exit 1
  fi
}

# Clear the screen, print a banner describing the step and the exact command
# it will run, wait for confirmation, then run it.
run_step() {
  local title="$1"
  shift
  clear
  echo "============================================================"
  echo "  $title"
  echo "============================================================"
  echo
  echo "\$ $*"
  echo
  confirm "Proceed?"
  echo
  "$@"
  echo
  read -p "Done. Press Enter to continue... " _
}

echo "Remember to change applications.properties!"
confirm "Did you change the applications.properties?"

echo 'IN POLARIS SHELL'
echo 'export SLACK_WEBHOOK_URL="https://hooks.slack.com/services/XXXX/XXXX/XXXX"'
confirm "Did you export the Slack Webhook URL?"

echo 'Install Apache Polaris PyPI Package'
pip install apache-polaris

export CATALOG_NAME=catalog1
export RESTRICTED_CATALOG_NAME=restricted-catalog1

run_step "Creating UNRESTRICTED catalog: $CATALOG_NAME" \
  polaris --client-id root --client-secret s3cr3t catalogs create "$CATALOG_NAME" --storage-type FILE --default-base-location "/var/tmp/$CATALOG_NAME/"

run_step "Creating RESTRICTED catalog: $RESTRICTED_CATALOG_NAME" \
  polaris --client-id root --client-secret s3cr3t catalogs create "$RESTRICTED_CATALOG_NAME" --storage-type FILE --default-base-location "/var/tmp/$RESTRICTED_CATALOG_NAME/"

run_step "Deleting UNRESTRICTED catalog: $CATALOG_NAME" \
  polaris --client-id root --client-secret s3cr3t catalogs delete "$CATALOG_NAME"

run_step "Deleting RESTRICTED catalog: $RESTRICTED_CATALOG_NAME" \
  polaris --client-id root --client-secret s3cr3t catalogs delete "$RESTRICTED_CATALOG_NAME"

clear
echo "Demo complete."