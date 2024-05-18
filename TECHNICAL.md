# Technical details

cubist is designed around content-addressable storage, where 256-bit hashes (generated via
[BLAKE3](https://github.com/BLAKE3-team/BLAKE3)) are used pervasively to reference objects in both the object
storage layer and internal data structures.

## Archives

An archive consists of:

- a file tree
- a hash map of block reference counts

## File trees

Each node of a file tree is one of:

- a file that is empty or references a block tree by its hash
- a symlink that references another file by its path
- a directory that contains zero or more child nodes

Each node contains the following metadata:

- inode
- mode
- group
- owner
- accessed
- created
- modified

## Block trees

Each node in a block tree is one of:

- a leaf block (level 0), containing data compressed using [Zstandard](https://github.com/facebook/zstd) and
referenced by the hash of this data
- a branch block (level N >= 1), containing hashes that reference nodes of level N-1 and referenced by the
hash of all its constituent hashes

## Files

Files are split up into leaf blocks in a streaming fashion using the
[FastCDC](https://github.com/nlfiedler/fastcdc-rs) v2020. Due to the nature of content-defined
chunking, block size is specified as a range instead of a single number, which allows the algorithm to split
the file at relatively consistent points, in contrast to fixed-size chunking in which any inserted or deleted
data results in a completely different stream of blocks. Specifying `--target-block-size=<N>` will result
block sizes in the range [N / 2, N * 4].

The default block size is 1 MiB, which is selected to compress well, work well with block storage systems,
and minimize the number of requests necessary to read and write large files. To keep block sizes consistent,
branch blocks are limited to the this size as well, meaning that with the default size of 1 MiB, a branch
block can store up to 32768 hashes of 256 bits (32 bytes) each.
