# Script for Google Cloud Shell for preparing run solution on Dataproc

# Variables
export PROJECT_ID="<...Change me!!!...>"    # Change me!!!
export BUCKET_NAME="<...Change me!!!...>"   # Change me!!!
export CLUSTER_NAME="<...Change me!!!...>"  # Change me!!!
export REGION="<...Change me!!!...>"        # Change me!!!
gcloud config set project $PROJECT_ID

# Generate data for processing
git clone https://github.com/gridu/INTRO_SPARK-SCALA_FOR_STUDENTS.git
cd INTRO_SPARK-SCALA_FOR_STUDENTS
pip3 install -r bigdata-input-generator/requirements.txt
python3 bigdata-input-generator/main.py

# Prepare Google infrastructure
gsutil mb -p $PROJECT_ID $BUCKET_NAME
gsutil cp -r ./capstone-dataset $BUCKET_NAME
gcloud dataproc clusters create $CLUSTER_NAME \
  --region=$REGION \
  --image-version=1.5-centos8
