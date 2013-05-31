#!/bin/sh

kill -9 `ps -ef | grep paxos.conf | grep -v grep | awk '{print $2}'`
