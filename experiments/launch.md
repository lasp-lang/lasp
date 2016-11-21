## Configuring AWS CLI

Add the following to your __.bashrc__

```bash
export AWS_ACCESS_KEY_ID=your_key
export AWS_SECRET_ACCESS_KEY=your_secret
```

```bash
$ source .bashrc
$ aws configure set aws_access_key_id $AWS_ACCESS_KEY_ID
$ aws configure set aws_secret_access_key $AWS_SECRET_ACCESS_KEY
$ aws configure set default.region us-west-2
```

## Launching the cluster

```bash
$ cd bin/
$ ./launch-instances.sh
```
