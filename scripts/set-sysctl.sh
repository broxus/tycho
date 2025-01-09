#!/usr/bin/env bash
set -ex

# 7_500_000 / 125_000_000
#
#    = 0.06
# should be enough to buffer 1gbit/s for 60ms
sysctl -w net.core.rmem_max=7500000
sysctl -w net.core.wmem_max=7500000