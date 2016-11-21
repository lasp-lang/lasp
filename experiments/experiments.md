# Launching the cluster

1. Go to [this](https://downloads.dcos.io/dcos/stable/aws.html) link and select __US West (Oregon)__/__Single Master__

2. In __Select Template__ just click __Next__

3. In __Specify Details__:
   - Choose a __Stack name__
   - Pick one of your keys in __KeyName__
   - __PublicSlaveInstanceCount__: 25
   - __SlaveInstanceCount__: 0

4. In __Options__ just click __Next__

5. In __Review__ click in __I acknowledge that AWS CloudFormation might create IAM resources__ and then in __Create__

# Information about the cluster
Once the cluster is created, go to the __CloudFormation__ page in your AWS console, select the cluster and in the __Outputs__ tab you have:

- __DnsAddress__: _$DCOS_
- __ExhibitorS3Bucket__
- __PublicSlaveDnsAddress__: _$ELB_HOST_

# Installing DCOS CLI

- Open _$DCOS_ in the browser
- Login
- Click in the bottom left corner and then in __Install CLI__

# Authenticate to your DCOS cluster

```bash
$ dcos auth login
```

# Installing Marathon-lb

```bash
$ dcos package install marathon-lb
```

Now, if Lasp is running in the cluster and you open _$ELB_HOST_ in your browser, you should see the __Lasp web UI__.

# Starting the experiments

```bash
$ cd bin/
$ ID=1 \
  LASP_BRANCH=dcos_again \
  ELB_HOST=$ELB_HOST \
  AWS_ACCESS_KEY_ID=your_key \
  AWS_SECRET_ACCESS_KEY=your_secret \
  CLIENT_NUMBER=32 \
  PARTITION_PROBABILITY=0 ./start-dcos-runner.sh
```

# Stopping the cluster

You can stop the cluster by going to the __CloudFormation__ page in your AWS console.
