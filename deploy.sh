#!/usr/bin/env bash
# Deploys a release to Maven Central. Afterwards, click "Publish" at https://central.sonatype.com/publishing
set -euo pipefail
cd "$(dirname "$0")"

rm -rf javadoc
javadoc -quiet -Xdoclint:none -d javadoc -sourcepath src -subpackages io.lacuna.bifurcan

lein deploy releases

# the Portal only sees uploads to its OSSRH-compatible staging API once they're explicitly handed over
entry=$(gpg --quiet -d ~/.lein/credentials.clj.gpg | tr '\n' ' ' | sed -E 's/.*central[^{]*\{([^}]*)\}.*/\1/')
user=$(sed -E 's/.*:username *"([^"]*)".*/\1/' <<<"$entry")
pass=$(sed -E 's/.*:password *"([^"]*)".*/\1/' <<<"$entry")
curl -sS --fail-with-body -X POST \
  -H "Authorization: Bearer $(printf '%s:%s' "$user" "$pass" | base64)" \
  https://ossrh-staging-api.central.sonatype.com/manual/upload/defaultRepository/io.lacuna

echo
echo "Uploaded; now publish at https://central.sonatype.com/publishing"
