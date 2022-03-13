import struct


FILE_prefix = "./100x10x10/trade"
OUT_prefix = "./trade"

for i in range(1,11):
	arr = []
	FILE = FILE_prefix + str(i)
	OUT = OUT_prefix + str(i)
	with open(FILE, 'rb') as f:
		while True:
			buf = f.read(24)
			if not buf:
				break
			arr.append(struct.unpack("=iiidi", buf))
	with open(OUT,'w') as f:
		print(len(arr),file = f)
		for i in arr:
			_, a, b, c, d = i
			print(a, b, c, d,file = f)
