# Zeroskip

A zeroskip DB is a directory that has one or many DB files in it.
There is only one 'active' file at any point in time. The active file
is the one that new records are written into. The other files are
finalised files and are read-only.
Every zeroskip DB is associated with a uuid (128 bit). The uuid is
internal to the DB and is not known to the end user. The DB itself is
identified by the name of the directory.

A zeroskip DB will typically look like this on the filesystem:

```
$ ls -lRa foobar/
foobar/:
total 16
drwxr-xr-x 2 partha partha 4096 Dec 21 16:13 .
drwxr-xr-x 9 partha partha 4096 Dec 21 11:36 ..
-rw-r--r-- 1 partha partha 4524 Dec 21 11:36 zeroskip-53e8bca4-77c8-4a1f-b8a9-acbe503ae8dd-1-2
-rw-r--r-- 1 partha partha 5252 Dec 21 11:36 zeroskip-53e8bca4-77c8-4a1f-b8a9-acbe503ae8dd-3-3
-rw-r--r-- 1 partha partha  324 Dec 21 11:36 zeroskip-53e8bca4-77c8-4a1f-b8a9-acbe503ae8dd-4
-rw-r--r-- 1 partha partha   45 Dec 21 11:36 .zsdb
$
```

In the directory listing above, the Zeroskip DB is named `foobar` and
the uuid of the DB is `53e8bca4-77c8-4a1f-b8a9-acbe503ae8dd`. There
are 4 files listed, and each one of them is explained below:

 * .zsdb: This is an internal file that is used to store some metadata
   about the DB.
 * zeroskip-53e8bca4-77c8-4a1f-b8a9-acbe503ae8dd-4: The current active
   file on the Zeroskip DB. This is where all the records are written
   to.
 * zeroskip-53e8bca4-77c8-4a1f-b8a9-acbe503ae8dd-3-3 is a finalised
   file, that hasn't been packed yet. Files that are finalised are
   read-only. Multiple inalised files are packed together by a
   seperate process/command.
 * zeroskip-53e8bca4-77c8-4a1f-b8a9-acbe503ae8dd-1-2 is a packed file.
   The records in packed file are sorted based on the key. As with
   finalised files, packed files are read-only.

The file name format of each of the files in the DB is as follows:

 - zeroskip-<uuid>-<index> : represents an unpacked and unfinalised file
 - zeroskip-<uuid>-<index>-<index> : represents an unpacked finalised file
 - zeroskip-<uuid>-<startindex>-<endindex> : represents a packed file
                                             that includes data from
                                             files with `startindex`
                                             to `endindex`. 


.ZSDB file format:

On-Disk Format:

The on-disk format is explained in `zeroskip.txt`

In-Memory representation:

Reading:

Writing:

Locking:

API:

Tools:

