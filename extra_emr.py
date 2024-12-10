import boto3  # type: ignore

client = boto3.client('emr')

client.run_job_flow(
    Name="ClusterShar",
    LogUri="s3://aws-logs-064794820934-us-east-2/elasticmapreduce/",
    ReleaseLabel="emr-7.3.0",
    Applications=[{"Name": "Spark"}, {"Name": "Hadoop"}],
    Configurations=[
        {
            "Classification": "spark-env",
            "Configurations": [
                {
                    "Classification": "export",
                    "Properties": {
                        "OUTPUT": "s3://sharadhakasi/extracredit"
                    },
                },
            ],
        },
        {
            "Classification": "spark",
            "Properties": {"maximizeResourceAllocation": "true"},
        },
        {
            "Classification": "spark-defaults",
            "Properties": {
                "spark.dynamicAllocation.executorIdleTimeout": "10800s",
                "spark.jars": "s3://sharadhakasi/extracredit/modefrequencyudaf_2.12-0.1.jar",
                "spark.rdd.compress": "true",
            },
        },
    ],
    Instances={
        'InstanceGroups': [
            {
                'Name': 'Primary',
                'Market': 'ON_DEMAND',
                'InstanceRole': 'MASTER',
                'InstanceType': 'r6gd.2xlarge',
                'InstanceCount': 1,
            },
            {
                'Name': 'Workers',
                'Market': 'ON_DEMAND',
                'InstanceRole': 'CORE',
                'InstanceType': 'r6gd.2xlarge',
                'InstanceCount': 1,
            },
        ],
        'KeepJobFlowAliveWhenNoSteps': False,
        'TerminationProtected': False,
        'Ec2SubnetId': 'subnet-0d0ec0fdba8122c01',
        'Ec2KeyName': 'sharadhakasi_key',
        'AdditionalMasterSecurityGroups': ['sg-04db5623aae08de70'],
        'AdditionalSlaveSecurityGroups': ['sg-0c59184a743789fd2'],
    },
    VisibleToAllUsers=True,
    JobFlowRole='arn:aws:iam::064794820934:instance-profile/AmazonEMR-InstanceProfile-20240925T211027',
    ServiceRole='arn:aws:iam::064794820934:role/service-role/AmazonEMR-ServiceRole-20240925T211045',
    Steps=[{
        "Name": "Mode Frequency Calculation",
        "ActionOnFailure": "TERMINATE_CLUSTER",
        "HadoopJarStep": {
            "Jar": "command-runner.jar",
            "Args": [
                "spark-submit",
                "--deploy-mode", "client",
                "--master", "yarn",
                "s3://sharadhakasi/extracredit/run.py"
            ],
        },
    }],
)
