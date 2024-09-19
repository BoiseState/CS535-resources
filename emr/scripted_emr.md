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

