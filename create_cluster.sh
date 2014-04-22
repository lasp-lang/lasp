#!/bin/bash

for d in dev/dev*; do $d/bin/derflowdis start; done
for d in dev/dev{2,3,4}; do $d/bin/derflowdis-admin cluster join 'derflowdis1@127.0.0.1'; done
dev/dev1/bin/derflowdis-admin cluster plan
dev/dev1/bin/derflowdis-admin cluster commit

