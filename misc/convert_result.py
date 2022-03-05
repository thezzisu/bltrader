import struct


FILE = "D:\\Downloads\\100x10x10\\trade1"

arr = []

with open(FILE, 'rb') as f:
    while True:
        buf = f.read(24)
        if not buf:
            break
        arr.append(struct.unpack("=iiidi", buf))

print(len(arr))
for i in arr:
    _, a, b, c, d = i
    print(a, b, c, d)
