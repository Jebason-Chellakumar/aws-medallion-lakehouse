#!/usr/bin/env bash
# Deploy the stack, then upload Glue scripts, the lakehouse package, config and gold models.
#   STACK=medallion-lakehouse AWS_REGION=ap-south-1 ./scripts/deploy.sh
set -euo pipefail

STACK="${STACK:-medallion-lakehouse}"
REGION="${AWS_REGION:-us-east-1}"
cd "$(dirname "$0")/.."

aws cloudformation deploy \
  --region "$REGION" \
  --stack-name "$STACK" \
  --template-file infra/template.yaml \
  --capabilities CAPABILITY_IAM \
  --no-fail-on-empty-changeset

output() {
  aws cloudformation describe-stacks --region "$REGION" --stack-name "$STACK" \
    --query "Stacks[0].Outputs[?OutputKey=='$1'].OutputValue" --output text
}
ARTIFACTS="$(output ArtifactsBucketName)"

rm -rf build && mkdir -p build
zip -qr build/lakehouse.zip lakehouse -x '*__pycache__*'

aws s3 cp --region "$REGION" build/lakehouse.zip "s3://$ARTIFACTS/lakehouse.zip"
aws s3 cp --region "$REGION" conf/tables.json "s3://$ARTIFACTS/conf/tables.json"
aws s3 sync --region "$REGION" --delete glue_jobs/ "s3://$ARTIFACTS/glue_jobs/" --exclude '*' --include '*.py'
aws s3 sync --region "$REGION" --delete gold/ "s3://$ARTIFACTS/gold/" --exclude '*' --include '*.sql'

echo
echo "Deployed. Lake bucket:  $(output LakeBucketName)"
echo "Pipeline:               $(output PipelineArn)"
echo "Athena workgroup:       $(output AthenaWorkGroupName)"
