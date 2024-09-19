# Install & Configure the AWS CLI
Install the AWS CLI on your machine. How you do this varies depending on your system.

Once you've installed the AWS CLI, go to the AWS console. On the upper-right corner, click your username to see a drop-down menu. Click "security credentials". Configure MFA if you haven't already.

In "my security credentials" under "access keys", create an access key.

It will warn you not to create an access key for the root account. Yes, we want to do this even though it doesn't conform to best practices. I'd rather keep this simple. Don't do this at your company, though. 

Once you get to the "retrieve access key" page, do not click "done" yet. Go to your terminal instead.

In your terminal, execute this command:
```
aws configure
```

You'll get a series of prompts that look like this:
```
AWS Access Key ID [None]: XXXX
AWS Secret Access Key [None]: XXXX
Default region name [None]: us-west-2
Default output format [None]:
```
For Access Key ID, use the "Access key" entry from the "retrieve access key" page.

Choose `us-west-2` for the region.

Now you should be able to execute shell commands for AWS! It will have the same permissions as the you console account.

# Starting an EMR cluster
You have to specify a lot when you do this, so we're going to do it from Python instead of the CLI.

So submit your code to run as a job on a non-interactive EMR cluster, you need an entrypoint Python file, and the rest of your code in a zip archive. The entrypoint acts as your "main" function, it's where the code starts executing. You need to upload the entrypoint and the rest of the code in a zip files before we start the cluster. 

Create the zip file with a command like this. The following example works for this directory:
```
zip -r willy.zip . -x entrypoint.py
```
Exclude the entrypoint file with the -x option.

Upload the entrypoint and the zip file to S3. The paths you copy them to have to match the paths submitted in the EMR step. 
```
aws s3 cp willy.zip s3://bsu-c535-fall2024-commons/code/
aws s3 cp entrypoint.py s3://bsu-c535-fall2024-commons/code/
```

To start the EMR cluster, use this command:
```
python3 start_emr.py
```

You'll have to change `start_emr.py` to match the infrastructure in your account.

Now go to the "Steps" tab in the AWS console on the page for the cluster you just created. You can see the status of the step you submitted to be run. 

After a while, the log files will become available for you to view. Log files can be viewed while the step is running, but keep in mind it will usually be a few minutes behind what is happening in real time. Log files contain the output of log and print statements you put in your code. Use them for debugging!

