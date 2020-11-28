import os
import argparse

import numpy as np

import h5py

from exetera.core.session import Session

def generate(vsize, vcount, rootdir):

    r = np.random.RandomState(12345678)
    for i in range(vcount):
        np.save(os.path.join(rootdir, "right_data_{}".format(i)), r.randint(100, size=vsize, dtype=np.int32))

    np.save(os.path.join(rootdir, "ids"), np.arange(vsize, dtype=np.int64))

    mapping = [0, 1, 1, 2]
    len_mapping = len(mapping)
    left_ids = np.zeros(vsize, dtype=np.int64)

    for i in range(vsize):
        base = i // len_mapping
        in_map = i % len_mapping
        left_ids[i] = mapping[in_map] + base * len_mapping

    np.save(os.path.join(rootdir, "fk_ids"), left_ids)


def build(input, vcount, output):
    s = Session()
    with h5py.File('/home/ben/covid/benchmarking.hdf5', 'w') as hf:
        for name in ('fk_ids', 'ids'):
            print('importing "{}"'.format(name))
            n = np.load('/home/ben/covid/{}.npy'.format(name))
            df = s.create_numeric(hf, name, 'int64')
            df.data.write(n)

        for v in range(vcount):
            print('importing "right_data_{}"'.format(v))
            n = np.load('/home/ben/covid/right_data_{}.npy'.format(v))
            df = s.create_numeric(hf, 'right_data_{}'.format(v), 'int32')
            df.data.write(n)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()

    subparsers = parser.add_subparsers(dest='command')
    subparsers.required = True

    build_command = subparsers.add_parser('generate')
    build_command.add_argument('-s', '--size', required=True,
                               help="the size of each field in number of elements")
    build_command.add_argument('-c', '--count', required=True,
                               help="the number of fields to be generated (excluding the keys)")
    build_command.add_argument('-o', '--output', required=True,
                               help="the directory to which the data should be written. This is"
                                    "created if it does not yet exist")
    args = parser.parse_args()

    if args.command == 'generate':
        generate(args.size, args.count, args.output)
    elif args.command == 'build':
        build(args.input, args.output)
    # else:
    #     print("command 'args.command' is not recognised")
