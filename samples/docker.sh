# Pull the docker image that contains Lasp
$ docker pull cmeiklejohn/lasp

# Start up the docker image with host networking and pseudo-tty
$ docker run -i -t --net=host cmeiklejohn/lasp
