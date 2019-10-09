#! /bin/bash
# Adapted from optimus/package/scripts/release.sh
set -e

$(aws ecr get-login --no-include-email --region us-east-1)

additional_mvn_args=""
if [ "$1" = "--distribution-only" ]; then
    additional_mvn_args="$additional_mvn_args -pl distribution"
fi

mvn clean install -DskipTests -Dreduced-build -Pbundle-contrib-pinterest,dist -Ddocker.build -Dcheckstyle.skip ${additional_mvn_args}

IMAGE_ID=$(docker images 998131032990.dkr.ecr.us-east-1.amazonaws.com/druid:latest --format="{{.ID}}")

IMAGE_SHORT=${IMAGE_ID:0:7}

docker tag druid:latest pinregistry.pinadmin.com/druid:${IMAGE_ID}

docker push pinregistry.pinadmin.com/druid:${IMAGE_ID}

# grab dev token
if [[ -z "$TOKEN" ]]; then
  TOKEN=$(KNOX_MACHINE_AUTH=$(hostname) knox get teletraan:optimus_dev_token || true)
  if [[ -z "$TOKEN" ]]; then
    # probably running on local machine, so try using knox on devapp
    TOKEN=$(ssh devapp "KNOX_MACHINE_AUTH=$(hostname) knox get teletraan:optimus_dev_token")
  fi
fi

if [[ -z "$TOKEN" ]]; then
  echo "Error: No token found! Can't push local release!"
  exit 1
fi

# Add registry explicitly to all teletraan files
cp -r ./teletraan /tmp/
find /tmp/teletraan/ -name "serviceset*" -type f -exec sed -i '' 's/image: druid/image: druid\
        registry: pinregistry.pinadmin.com/g' {} +

tar czvf "druid-${IMAGE_SHORT}.tar.gz" -C /tmp teletraan/

aws s3 cp --no-progress druid-${IMAGE_SHORT}.tar.gz s3://pinterest-alameda/druid/druid-${IMAGE_SHORT}.tar.gz

echo -n "${IMAGE_SHORT}" > tmp.latest

echo "${IMAGE_SHORT}"

aws s3 cp --no-progress tmp.latest s3://pinterest-alameda/druid/druid.latest

rm tmp.latest

curl -s -k --retry 2 -X POST https://teletraan.pinadmin.com/v1/builds -H "Authorization: token ${TOKEN}" -H "Content-Type: application/json" -d @- << DATA
    {
      "name": "druid",
      "repo": "DRUID",
      "branch": "private",
      "commit": "${IMAGE_ID}",
      "artifactUrl": "https://devrepo.pinadmin.com/druid/druid-${IMAGE_SHORT}.tar.gz",
      "commitDate": "$(($(date +%s) * 1000))",
      "publishInfo": "$(whoami)@$(hostname)"
    }
DATA
