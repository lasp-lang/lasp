FROM lasplang/erlang:19.3

MAINTAINER Christopher Meiklejohn <christopher.meiklejohn@gmail.com>

RUN cd /tmp && \
    apt-get update && \
    apt-get -y install wget build-essential make gcc ruby-dev git expect gnuplot

RUN cd /opt && \
    (git clone https://github.com/lasp-lang/lasp.git -b $LASP_BRANCH && cd lasp && make exp-stage);

CMD cd /opt/lasp && \
    /opt/lasp/_build/exp/rel/lasp/bin/env; cat erl_crash.dump