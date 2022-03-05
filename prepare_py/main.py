import sys
import h5py
import struct

DATA_ROOT = "../.data/"
P_PRICE = "price"
P_ORDERID = "order_id"
P_VOLUME = "volume"
P_TYPE = "type"
P_DIRECTION = "direction"


def generate_path(prefix: str) -> str:
    return DATA_ROOT + prefix + '1' + '.h5'


def read_order_from_file():
    order_id_mtx = h5py.File(generate_path(P_ORDERID), 'r')['order_id']
    N, M, K = order_id_mtx.shape
    print("[io] size=", N, M, K)
    arr = [[] for _ in range(10)]
    for i in range(N):
        for j in range(M):
            for k in range(K):
                stk = i % 10
                arr[stk].append((order_id_mtx[i, j, k], (i, j, k)))
    for i in range(10):
        arr[i].sort(key=lambda x: x[0])
    for i in range(10):
        with open('.data/id-' + str(i), 'wb') as f:
            f.write(struct.pack('@L', len(arr[i])))
            for j in range(len(arr[i])):
                x = arr[i][j][0]
                f.write(struct.pack('@l', x))
    price_mtx = h5py.File(generate_path(P_PRICE), 'r')['price']
    for i in range(10):
        with open('.data/price-' + str(i), 'wb') as f:
            f.write(struct.pack('@L', len(arr[i])))
            for j in range(len(arr[i])):
                x, y, z = arr[i][j][1]
                f.write(struct.pack('@d', price_mtx[x, y, z]))


if __name__ == '__main__':
    read_order_from_file()
