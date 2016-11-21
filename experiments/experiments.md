## Starting the experiments

This assumes:
- AWS CLI ([how to configure it](./launch.md#configuring-aws-cli))
- DCOS CLI ([how to configure it])(./manual-launch.md#configuring-dcos-cli-if-already-installed))

```bash
$ cd bin/
$ ID=1 \
  LASP_BRANCH=dcos_again \
  CLIENT_NUMBER=32 \
  PARTITION_PROBABILITY=0 ./start-dcos-runner.sh
```
