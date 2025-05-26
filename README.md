# aws-lakehouse

aws configure --profile glue-dev 

export PROFILE_NAME="glue-dev"

docker run -it `
  -v ${HOME}\.aws:/home/glue_user/.aws:ro `
  -v ${PWD}:/home/glue_user/workspace `
  -e AWS_PROFILE=glue-dev `
  -e DISABLE_SSL=true `
  --rm `
  public.ecr.aws/glue/aws-glue-libs:5  `
  pyspark



  