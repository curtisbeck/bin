#!/usr/bin/env python3

import boto3
import re
import sys

if re.match(r'^i-[0-9a-fA-F]{8,17}$', sys.argv[1]) is None:
  sys.exit()

try:
  ec2 = boto3.resource('ec2')
  instance = ec2.Instance(sys.argv[1])
  print(instance.private_ip_address)
except:
  sys.exit()

