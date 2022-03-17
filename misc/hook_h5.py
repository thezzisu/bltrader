import sys
import h5py
import struct

# DATA_ROOT = "../.data/"
DATA_ROOT = "D:\\Downloads\\"


def read_order_from_file():
    hook_mtx = h5py.File(DATA_ROOT + 'hook.h5', 'r')['hook']
    N, M, K = hook_mtx.shape
    print("[io] size=", N, M, K)
    arr = []
    T = {}
    for i in range(M):
        arr.append((hook_mtx[0, i, 0], hook_mtx[0, i, 1],
                   hook_mtx[0, i, 2], hook_mtx[0, i, 3]))
        if hook_mtx[0, i, 0] not in T:
            T[hook_mtx[0, i, 0]] = True
        else:
            print("Fuck")
    arr.sort(key=lambda x: x[0])
    print(arr[0])
    with open('.data/hook-0', 'wb') as f:
        f.write(struct.pack('<L', len(arr)))
        for i in range(len(arr)):
            x, y, z, w = arr[i]
            f.write(struct.pack('<llll', x, y, z, w))


if __name__ == '__main__':
    read_order_from_file()
