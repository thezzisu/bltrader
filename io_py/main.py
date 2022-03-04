import h5py
import struct

DATA_ROOT = ".data/"
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
    # result = [[] for _ in 10]
    print(order_id_mtx[5, 2, 7])
    R = {}
    for i in range(N):
        for j in range(M):
            for k in range(K):
                if i % 10 == 0:
                    t = order_id_mtx[i, j, k]
                    if t in R:
                        print("Fuck")
                        return
                    else:
                        R[t] = (i, j, k)


if __name__ == '__main__':
    read_order_from_file()
