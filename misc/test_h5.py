import sys
import h5py
import struct

# DATA_ROOT = "../.data/"
DATA_ROOT = "D:\\Downloads\\"
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
    X, Y, Z = None, None, None
    flag = True
    for i in range(N):
        if flag:
            for j in range(M):
                if flag:
                    for k in range(K):
                        if flag:
                            stk = i % 10
                            if stk == 9 and order_id_mtx[i, j, k] == 2:
                                X, Y, Z = i, j, k
                                flag = False
    price_mtx = h5py.File(generate_path(P_PRICE), 'r')['price']
    print("price=", price_mtx[X, Y, Z])
    volume_mtx = h5py.File(generate_path(P_VOLUME), 'r')['volume']
    print("volume=", volume_mtx[X, Y, Z])
    type_mtx = h5py.File(generate_path(P_TYPE), 'r')['type']
    print("type=", type_mtx[X, Y, Z])


if __name__ == '__main__':
    read_order_from_file()
