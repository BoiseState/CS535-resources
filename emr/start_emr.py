import boto3

client = boto3.client('emr')

client.run_job_flow(
    Name="my-cluster-name",
    # AWS will probably create a bucket like this for you. Change it to
    # your log bucket to view logs
    LogUri="s3://aws-logs-266669273183-us-west-2/elasticmapreduce/",
    ReleaseLabel="emr-7.2.0",
    Applications=[{"Name": "Spark"}, {"Name": "Hadoop"}],
    # Same as the interactive EMR
    Configurations=[
        {
            "Classification": "spark-env",
            "Configurations":[
                {
                    "Classification": "export",
                    "Properties": {
                        # Change this to an s3 bucket in your account to test your code
                        "PAGE_PAIRS_OUTPUT": "s3://bsu-c535-fall2024-commons/willy-results/"
                    },
                },
            ],
        },
        {
            "Classification": "spark",
            "Properties": {
            "maximizeResourceAllocation": "true"
            }
        },
        {
            "Classification": "spark-defaults",
            "Properties": {
            "spark.dynamicAllocation.executorIdleTimeout": "10800s",
            "spark.log4jHotPatch.enabled": "false",
            "spark.rdd.compress": "true",
            "spark.rpc.message.maxSize": "512",
            "spark.serializer": "org.apache.spark.serializer.KryoSerializer"
            }
        },
        {
            "Classification": "core-site",
            "Properties": {
            "fs.s3n.multipart.uploads.split.size": "5368709120"
            }
        },
    ],
    Instances={
        'InstanceGroups': [
            {
                'Name': 'Primary',
                'Market': 'ON_DEMAND',
                'InstanceRole': 'MASTER',
                'InstanceType': 'c6g.xlarge',
                # This should always be 1, you only need a single primary
                'InstanceCount': 1,
            },
            {
                'Name': 'Workers',
                'Market': 'ON_DEMAND',
                'InstanceRole': 'CORE',
                'InstanceType': 'c6gd.xlarge',
                'InstanceCount': 1,
            },
        ],
        'KeepJobFlowAliveWhenNoSteps': False,
        'TerminationProtected': False,
        'Ec2KeyName': 'teaching-linux-vm',
        # Replace these with security groups generated by EMR the first
        # time you started your cluster
        'AdditionalMasterSecurityGroups': ['sg-0e5f2b65f647ec19b'],
        'AdditionalSlaveSecurityGroups': ['sg-03853bfa26166ffc5'],

    },
    VisibleToAllUsers=True,
    # Replace these with your instance profile and your service role
    # that were automatically created by EMR
    JobFlowRole='arn:aws:iam::266669273183:instance-profile/AmazonEMR-InstanceProfile-20240816T100708',
    ServiceRole='arn:aws:iam::266669273183:role/service-role/AmazonEMR-ServiceRole-20240816T100724',
    Steps=[{
        "Name": "My Cool Spark Program",
        "ActionOnFailure": "TERMINATE_CLUSTER",
        "HadoopJarStep": {
            "Jar": "command-runner.jar",
            "Args": [
                "spark-submit",
                "--deploy-mode", "client",
                "--master", "yarn",
                "--py-files", 
                "s3://bsu-c535-fall2024-commons/code/willy.zip",
                "s3://bsu-c535-fall2024-commons/code/entrypoint.py",
            ],
        },
    }],
)