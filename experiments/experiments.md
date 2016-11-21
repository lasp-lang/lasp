## Starting the experiments

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
