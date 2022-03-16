import struct
import sys
import argparse
parser = argparse.ArgumentParser()
parser.add_argument('--prefix', type=str, nargs=1)
parser.add_argument('--stk', type=int, nargs=1)
args = parser.parse_args()

PREFIX = args.prefix[0]
STK = args.stk[0]
FILE = PREFIX + "/trade" + str(STK + 1)
OUT = "trade" + str(STK + 1) + ".txt"

arr = []
with open(FILE, 'rb') as f:
  while True:
    buf = f.read(24)
    if not buf:
      break
    arr.append(struct.unpack("=iiidi", buf))
with open(OUT, 'w') as f:
  print(len(arr),file = f)
  for i in arr:
    _, a, b, c, d = i
    print(STK, a, b, c, d, file = f)
