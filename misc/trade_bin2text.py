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
OUT = "trade" + str(STK) + ".txt"

with open(FILE, 'rb') as f:
  with open(OUT, 'w') as g:
    while True:
      buf = f.read(24)
      if not buf:
        break
      _, a, b, c, d = struct.unpack("=iiidi", buf)
      print("%d %d %d %.06f %d" % (STK, b, a, c, d), file = g)

