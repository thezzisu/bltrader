import struct
import sys

PREFIX = "D:\\Downloads\\100x10x10"
STK = int(input("stk:"))
ID = int(input("id:"))
FILE = PREFIX + "\\trade" + str(STK + 1)

with open(FILE, 'rb') as f:
  for i in range(1, ID):
    f.read(24)
  buf = f.read(24)
  print("stk_code %d bid %d ask %d price %f vol %d" % struct.unpack("=iiidi", buf))
