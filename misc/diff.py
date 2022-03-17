import struct
import sys
import argparse
parser = argparse.ArgumentParser()
parser.add_argument('--std', type=str, nargs=1)
parser.add_argument('--out', type=int, nargs=1)
args = parser.parse_args()
STD_PREFIX = args.std[0]
OUT_PREFIX = args.out[0]

STK = int(input("stk:"))
STD_FILE = STD_PREFIX + "/trade" + str(STK + 1)
OUT_FILE = OUT_PREFIX + "/trade" + str(STK + 1)

i = 1

with open(STD_FILE, 'rb') as std:
  with open(OUT_FILE, 'rb') as out:
    while True:
      buf1 = std.read(24)
      buf2 = out.read(24)
      if len(buf1) != 24 or len(buf2) != 24:
        break
      if buf1 != buf2:
        print("id %d diff" % i)
        print("std %d bid %d ask %d price %f vol %d" % struct.unpack("=iiidi", buf1))
        print("out %d bid %d ask %d price %f vol %d" % struct.unpack("=iiidi", buf2))
        break
      i = i + 1

print("compared %d trades" % i)
