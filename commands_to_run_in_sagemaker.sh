cd ~/SageMaker/synthea/
git checkout v2.7.0
cp ../gnc-test/synthea.properties ../gnc-test/generate_synthetic_data.sh -t .
./generate_synthetic_data.sh
aws s3 sync ./output/csv/ s3://ac850692499868-gnc-test/data/csv/