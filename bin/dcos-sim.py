#!/usr/bin/python

from subprocess import Popen

commands = [
    'cd ../ ; CLIENT_NUMBER=1 INSTRUMENTATION=true AD_COUNTER_SIM_SERVER=true make shell',
    'cd ../ ; CLIENT_NUMBER=1 INSTRUMENTATION=true AD_COUNTER_SIM_CLIENT=true make shell'
]

# start all nodes
nodes = [Popen(cmd, shell=True) for cmd in commands]

# wait for simulation to end
for node in nodes: node.wait()
