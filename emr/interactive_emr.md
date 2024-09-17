# Starting a cluster

Log in to AWS with your student credentials, and go to the EMR service.

Create a cluster.

You'll see a list of checkboxes for the software included. You can uncheck the "Livy" box.

Next you'll select your instance types for each instance group. Remove the "task" instance group.

For the primary node, use a `c6g.xlarge` instance.

For the core nodes, pick from these instance types depending on your needs:
* `c6gd.xlarge`
* `c6gd.2xlarge`
* `m6gd.xlarge`
* `m6gd.2xlarge`
* `r6gd.xlarge`
* `r6gd.2xlarge`

Talk to the instructor if you want to use instance types not in this list. Keep the on-demand price in mind, that's the price per instance per hour.

Under "cluster scaling and provisioning", set the cluster size manually, and pick how many instances you need. You usually only need one. Do not create more than 4 core nodes without permission from the instructor.

Under "software settings", enter this JSON configuration:
```json
[
  {
    "Classification": "spark",
    "Properties": {
      "maximizeResourceAllocation": "true"
    }
  },
  {
    "Classification": "spark-defaults",
    "Properties": {
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
  }
]
```

Under "security configuration and EC2 key pair", select your EC2 key pair so you can SSH into the cluster. If no key pair exists, create one. Use a "pem" type.

Under "identity and access management", create a service role if you've never started a cluster before. You can leave the VPC, Subnet and Secruity group options blank. If you've created a cluster before, you can use the same service role you created before. 

Next create an instance profile if you have never started a cluster. The only option you need to select here is "All S3 buckets in this account with read and write access" under "S3 bucket access". You can use the same instance profile after creating one for your first cluster, just like with the service role.

Now click "create cluster". It should take 4-7 minutes for your cluster to be available in the "waiting" state.

# Connecting to the cluster
Once your cluster is started, you'll need to connect to it. Under the "network and security" section of the "properties" tab for your newly created EMR cluster, go to the Primary node EMR managed security group. 

In this security group make sure you have the following ingress permissions set up:
* Port 22 (SSH) for `0.0.0.0/0`
* Port 20888 for your IP address
* Port 8088 for your IP address
* Port 4040 for your IP address

To SSH into the cluster and access the Spark UI, you need the private IP address of the primary node, as well as the public. In the "instances (hardware)" tab, expand the "primary" instance group, and it will have the public and private IP address of the primary.

Use this SSH command to enter the cluster. In addition to entering the cluster, this command also forwards port necessary to communicate with Spark Connect and the Spark UI:
```
ssh -i <key_file> hadoop@<primary_public_IP> -L 15002:localhost:15002 -L 4040:localhost:4040 -L 20888:<primary_private_IP>:20888 -L 8088:<primary_private_IP>:8088
```

The key file is the path on your local machine to the private key for the EC2 key pair you created earlier.

Once you're SSH'd into the primary node, start the Spark Connect server with this command:
```
sudo /usr/lib/spark/sbin/start-connect-server.sh --packages org.apache.spark:spark-connect_2.12:3.5.1
```

If successful, it will tell you the path of the log file for the Spark Connect server. If you lose track of the log file path, it's always somewhere in this directory: `/var/log/spark/`. I recommend you have a live feed of the log file on your terminal so you can diagnose issues you may have. You can use this command:
```
tail -f -n +1 <log_file>
```

To start a Spark Session on your local machine in PySpark, use this code snippet:
```python
import pyspark
spark = (
    SparkSession.builder
    .remote('sc://localhost:15002')
    .appName("My App")
    .getOrCreate()
)
```

To view the Spark UI, go to `localhost:8088` in your browser. You should see a "hadoop" page with an elephant. 

Under "cluster", click "applications". 

Click on the application with the name "Spark Connect Server". 

Now click on the "Tracking URL" labelled "ApplicationMaster". That will take you to an unreachable site. 

Replace the site in the URL with `localhost`. That will take you to the Spark UI. The original site is the private DNS address of the primary node.