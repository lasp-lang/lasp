## Starting the experiments

This assumes that you have the AWS CLI configured. See [how to configure it](./launch.md).

```bash
$ cd bin/
$ ID=1 \
  LASP_BRANCH=dcos_again \
  CLIENT_NUMBER=32 \
  PARTITION_PROBABILITY=0 ./start-dcos-runner.sh
```
