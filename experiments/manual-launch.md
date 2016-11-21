## Launching the cluster manually

1. Go to [this](https://downloads.dcos.io/dcos/stable/aws.html) link and select __US West (Oregon)__/__Single Master__

2. In __Select Template__ just click __Next__

3. In __Specify Details__:
   - Choose a __Stack name__
   - Pick one of your keys in __KeyName__
   - __PublicSlaveInstanceCount__: 25
   - __SlaveInstanceCount__: 0
   - __OAuthEnabled__: false

4. In __Options__ just click __Next__

5. In __Review__ click in __I acknowledge that AWS CloudFormation might create IAM resources__ and then in __Create__

Once the cluster is created, go to the __CloudFormation__ page in your AWS console, select the cluster and in the __Outputs__ tab you have:

- __DnsAddress__: _$DCOS_
- __ExhibitorS3Bucket__
- __PublicSlaveDnsAddress__: _$ELB_HOST_

## Installing DCOS CLI (if not yet installed)

- Open _$DCOS_ in the browser
- Click in __Install CLI__ (bottom-left corner)

## Configuring DCOS CLI (if already installed)

```bash
$ dcos config set core.dcos_url http://$DCOS
```

## Installing Marathon-lb

```bash
$ dcos package install marathon-lb
```

Now, if Lasp is running in the cluster and you open _$ELB_HOST_ in your browser, you should see the __Lasp web UI__.

## Stopping the cluster

You can stop the cluster by going to the __CloudFormation__ page in your AWS console.
