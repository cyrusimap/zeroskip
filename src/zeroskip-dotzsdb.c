/*
 * zeroskip-dotzsdb.c : zeroskip .zsdb manipulation functions
 *
 * This file is part of zeroskip.
 *
 * zeroskip is free software; you can redistribute it and/or modify
 * it under the terms of the MIT license. See LICENSE for details.
 *
 */

#include <libzeroskip/crc32c.h>
#include <libzeroskip/log.h>
#include <libzeroskip/mfile.h>
#include <libzeroskip/zeroskip.h>
#include "zeroskip-priv.h"

#include <errno.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <signal.h>
#include <unistd.h>

typedef void (*sigfunc)(int);

static struct file_lock dblock;
static struct mfile *zsdbfile;

/**
 * Internal/Private functions
 */
static void cleanup_lockfile(void)
{
        if (zsdbfile) {
                mfile_close(&zsdbfile);
                zsdbfile = NULL;
        }

        file_lock_release(&dblock);
}

static void cleanup_lockfile_on_exit(void)
{
        cleanup_lockfile();
}

static void cleanup_lockfile_on_signal(int signum)
{
        cleanup_lockfile();
        signal(signum, cleanup_lockfile_on_signal);
        raise(signum);
}

/**
 * Public functions
 */

/*
 * zs_dotzsdb_create():
 * Creates a .zsdb file in the DB directory. This function assumes the caller
 * has sanitised the input. This function also generates the UUID for the DB
 * since this function is called when the DB is created first.
 *
 * The .zsdb file in a DB directory has the following structure:
 *      ZSDB Signature       -  64 bits
 *      offset               -  64 bits (the last known good position in active file)
 *      Parsed uuid str      - 296 bits
 *      current index        -  32 bits
 *      CRC32                -  32 bits
 */
int zs_dotzsdb_create(struct zsdb_priv *priv)
{
        unsigned char stackbuf[DOTZSDB_SIZE];
        uuid_t uuid;
        unsigned char *sptr;
        struct mfile *mf;
        int ret = 1;
        size_t nbytes = 0;

        memset(&stackbuf, 0, DOTZSDB_SIZE);
        sptr = stackbuf;

        /* Generate a new uuid */
        uuid_generate(uuid);
        uuid_unparse_lower(uuid, priv->dotzsdb.uuidstr);

        /* Header */
        priv->dotzsdb.signature = ZS_SIGNATURE;
        memcpy(sptr, &priv->dotzsdb.signature, sizeof(uint64_t));
        sptr += sizeof(uint64_t);

        /* Offset */
        priv->dotzsdb.offset = ZS_HDR_SIZE;
        *((uint64_t *)sptr) = hton64(priv->dotzsdb.offset);
        sptr += sizeof(uint64_t);

        /* UUID */
        memcpy(sptr, &priv->dotzsdb.uuidstr, UUID_STRLEN);
        sptr += UUID_STRLEN;

        /* Index */
        priv->dotzsdb.curidx = 0;
        *((uint32_t *)sptr) = hton32(priv->dotzsdb.curidx);
        sptr += sizeof(uint32_t);

        /* CRC */
        priv->dotzsdb.crc = crc32c_hw(0, 0, 0);
        priv->dotzsdb.crc = crc32c_hw(priv->dotzsdb.crc,
                                      (void *)&priv->dotzsdb.signature,
                                      sizeof(uint64_t));
        priv->dotzsdb.crc = crc32c_hw(priv->dotzsdb.crc,
                                      (void *)&priv->dotzsdb.offset,
                                      sizeof(uint64_t));
        priv->dotzsdb.crc = crc32c_hw(priv->dotzsdb.crc,
                                      (void *)&priv->dotzsdb.uuidstr,
                                      UUID_STRLEN);
        priv->dotzsdb.crc = crc32c_hw(priv->dotzsdb.crc,
                                      (void *)&priv->dotzsdb.curidx,
                                      sizeof(uint32_t));
        *((uint32_t *)sptr) = hton32(priv->dotzsdb.crc);
        sptr += sizeof(uint32_t);

        /* Write to file */
        if (mfile_open(priv->dotzsdbfname.buf, MFILE_RW_CR, &mf) != 0) {
                zslog(LOGDEBUG, "Could not create %s!\n",
                      priv->dotzsdbfname.buf);
                ret = 0;
                goto fail1;
        }

        if (mfile_write(&mf, &stackbuf, DOTZSDB_SIZE, &nbytes) != 0) {
                zslog(LOGDEBUG, "Could not write to file %s!",
                      priv->dotzsdbfname.buf);
                ret = 0;
                goto fail2;
        }

        mfile_flush(&mf);

        zslog(LOGDEBUG, "Created db with UUID %s\n", priv->dotzsdb.uuidstr);

        /* stat() the .zsdb file to get the status. The .zsdb file is
         * used for synchronisation among various processes.
         */
        stat(priv->dotzsdbfname.buf, &priv->dotzsdb_st);
fail2:
        mfile_close(&mf);

fail1:
        return ret;
}

/*
 * zs_dotzsdb_validate():
 * Checks a the .zsdb file in a given dbdir and ensures that it is
 * valid. If it is valid, it sets the values in the priv->dotzsdb
 * structure.
 *
 * Returns 1 if it is valid and 0 otherwise.
 */
int zs_dotzsdb_validate(struct zsdb_priv *priv)
{
        struct mfile *mf;
        size_t mfsize;
        struct dotzsdb *dothdr;
        int ret = 1;

        if (mfile_open(priv->dotzsdbfname.buf, MFILE_RD, &mf) != 0) {
                zslog(LOGDEBUG, "Could not open %s!\n",
                      priv->dotzsdbfname.buf);
                ret = 0;
                goto fail1;
        }

        /* stat() the .zsdb file to get the status. The .zsdb file is
         * used for synchronisation among various processes.
         */
        stat(priv->dotzsdbfname.buf, &priv->dotzsdb_st);

        mfile_size(&mf, &mfsize);
        if (mfsize < DOTZSDB_SIZE) {
                zslog(LOGDEBUG, "File too small to be zeroskip DB: %zu.\n",
                        mfsize);
                ret = 0;
                goto fail2;
        }

        dothdr = (struct dotzsdb *)mf->ptr;
        if (dothdr->signature == ZS_SIGNATURE) {
                uint32_t crc;
                /* Signature */
                priv->dotzsdb.signature = dothdr->signature;

                /* Offset */
                priv->dotzsdb.offset = ntoh64(dothdr->offset);

                /* UUID str */
                memcpy(&priv->dotzsdb.uuidstr,dothdr->uuidstr,
                       UUID_STRLEN);
                uuid_parse(priv->dotzsdb.uuidstr, priv->uuid);

                /* Index */
                priv->dotzsdb.curidx = ntoh32(dothdr->curidx);

                /* Verify CRC */
                crc = crc32c_hw(0, 0, 0);
                crc = crc32c_hw(crc, (void *)&priv->dotzsdb.signature,
                                sizeof(uint64_t));
                crc = crc32c_hw(crc, (void *)&priv->dotzsdb.offset,
                                sizeof(uint64_t));
                crc = crc32c_hw(crc, (void *)&priv->dotzsdb.uuidstr,
                                UUID_STRLEN);
                crc = crc32c_hw(crc, (void *)&priv->dotzsdb.curidx,
                                sizeof(uint32_t));

                if (crc != ntoh32(dothdr->crc)) {
                        zslog(LOGWARNING, "Invalid zeroskip DB %s. CRC failed\n",
                              priv->dotzsdbfname.buf);
                        ret = 0;
                        goto fail2;
                }

                /* CRC */
                priv->dotzsdb.crc = ntoh32(dothdr->crc);

        } else {
                zslog(LOGDEBUG, "Invalid zeroskip DB %s.\n",
                        priv->dotzsdbfname.buf);
                ret = 0;
                goto fail2;
        }

        zslog(LOGDEBUG, "Opening DB with UUID %s\n", priv->dotzsdb.uuidstr);
fail2:
        mfile_close(&mf);
fail1:
        return ret;
}

/*
 * zs_dotzsdb_update_index_and_offset():
 * Updates the current index and offset in the .zsdb file.
 */
int zs_dotzsdb_update_index_and_offset(struct zsdb_priv *priv,
                                       uint32_t idx, uint64_t offset)
{
        int ret = 0;

        if (!zs_dotzsdb_update_begin(priv)) {
                zslog(LOGDEBUG, "Failed acquiring lock to update index!\n");
                ret = 1;
                goto done;
        }

        /* index */
        priv->dotzsdb.curidx = idx;
        /* offset */
        priv->dotzsdb.offset = offset;

        if (!zs_dotzsdb_update_end(priv)) {
                zslog(LOGDEBUG, "Failed acquiring lock to update index!\n");
                ret = 1;
                goto done;
        }

done:
        return ret;
}

ino_t zs_dotzsdb_get_ino(struct zsdb_priv *priv)
{
        cstring dotzsdbfname = CSTRING_INIT;
        struct stat sb;

        memset(&sb, 0, sizeof(struct stat));

        /* The filename */
        cstring_dup(&priv->dbdir, &dotzsdbfname);
        cstring_addch(&dotzsdbfname, '/');
        cstring_addstr(&dotzsdbfname, DOTZSDB_FNAME);

        /* stat() the .zsdb file to get the inode number. The .zsdb file is
         * used for synchronisation among various processes.
         */
        stat(dotzsdbfname.buf, &sb);

        cstring_release(&dotzsdbfname);

        return sb.st_ino;
}

int zs_dotzsdb_check_stat(struct zsdb_priv *priv)
{
        cstring dotzsdbfname = CSTRING_INIT;
        struct stat st;
        int status = 0;

        memset(&st, 0, sizeof(struct stat));

        /* The filename */
        cstring_dup(&priv->dbdir, &dotzsdbfname);
        cstring_addch(&dotzsdbfname, '/');
        cstring_addstr(&dotzsdbfname, DOTZSDB_FNAME);

        /* stat() the .zsdb file. The .zsdb file is
         * used for synchronisation among various processes.
         */
        if (stat(dotzsdbfname.buf, &st) != 0) {
                status = -1;
                goto done;
        }

        if (st.st_ino != priv->dotzsdb_st.st_ino)
                status |= ZSDB_FILE_INO_CHANGED;
        if (st.st_mode != priv->dotzsdb_st.st_mode)
                status |= ZSDB_FILE_MODE_CHANGED;
        if (st.st_uid != priv->dotzsdb_st.st_uid)
                status |= ZSDB_FILE_UID_CHANGED;
        if (st.st_gid != priv->dotzsdb_st.st_gid)
                status |= ZSDB_FILE_GID_CHANGED;
        if (st.st_size != priv->dotzsdb_st.st_size)
                status |= ZSDB_FILE_SIZE_CHANGED;
#ifdef MACOSX
        if (st.st_mtimensec != priv->dotzsdb_st.st_mtimensec)
                status |= ZSDB_FILE_MTIM_CHANGED;
        if (st.st_ctimensec != priv->dotzsdb_st.st_ctimensec)
                status |= ZSDB_FILE_CTIM_CHANGED;
#else  /* Linux */
        if (st.st_mtim.tv_nsec != priv->dotzsdb_st.st_mtim.tv_nsec)
                status |= ZSDB_FILE_MTIM_CHANGED;
        if (st.st_ctim.tv_nsec != priv->dotzsdb_st.st_ctim.tv_nsec)
                status |= ZSDB_FILE_CTIM_CHANGED;
#endif

        if (status) {
                priv->dotzsdb_st = st;
        }
done:
        cstring_release(&dotzsdbfname);
        return status;
}

/*
 * zs_dotzsdb_update_begin():
 * Acquires a lock for updating .dotzsdb file
 */
int zs_dotzsdb_update_begin(struct zsdb_priv *priv)
{
        size_t mfsize;
        struct dotzsdb *dothdr;
        int ret = 1;

        if (!priv->open) {
                zslog(LOGDEBUG, "DB not opened. Please open the db before updating\n");
                return 0;
        }

        if (dblock.active) {
                zslog(LOGDEBUG, "DB update already in progress\n");
                return 1;
        }

        /* Open .zsdb file in RO mode */
        if (mfile_open(priv->dotzsdbfname.buf, MFILE_RD,
                            &zsdbfile) != 0) {
                zslog(LOGDEBUG, "Could not open %s!\n", priv->dotzsdbfname.buf);
                ret = 0;
                goto fail1;

        }

        /* Read .dotzsdb file and update priv->dotzsdb */
        mfile_size(&zsdbfile, &mfsize);
        if (mfsize < DOTZSDB_SIZE) {
                /* XXX: This should *never* happen here, since the db must have
                 * been successfully opened by the time we've got here.
                 */
                zslog(LOGDEBUG, "File too small to be zeroskip DB: %zu.\n",
                        mfsize);
                ret = 0;
                goto fail1;
        }

        dothdr = (struct dotzsdb *)zsdbfile->ptr;
        if (dothdr->signature == ZS_SIGNATURE) {
                uint32_t crc;

                /* Signature */
                priv->dotzsdb.signature = dothdr->signature;

                /* Offset */
                priv->dotzsdb.offset = ntoh64(dothdr->offset);

                /* UUID str */
                memcpy(&priv->dotzsdb.uuidstr, dothdr->uuidstr, UUID_STRLEN);

                /* Index */
                priv->dotzsdb.curidx = ntoh32(dothdr->curidx);

                /* Verify CRC */
                crc = crc32c_hw(0, 0, 0);
                crc = crc32c_hw(crc, (void *)&priv->dotzsdb.signature,
                                sizeof(uint64_t));
                crc = crc32c_hw(crc, (void *)&priv->dotzsdb.offset,
                                sizeof(uint64_t));
                crc = crc32c_hw(crc, (void *)&priv->dotzsdb.uuidstr,
                                UUID_STRLEN);
                crc = crc32c_hw(crc, (void *)&priv->dotzsdb.curidx,
                                sizeof(uint32_t));

                if (crc != ntoh32(dothdr->crc)) {
                        zslog(LOGWARNING, "Invalid zeroskip DB %s. CRC failed\n",
                              priv->dotzsdbfname.buf);
                        ret = 0;
                        goto fail1;
                }

                /* CRC */
                priv->dotzsdb.crc = ntoh32(dothdr->crc);

        } else {
                /* XXX: This should *never* happen here, since the db must have
                 * been successfully opened by the time we've got here.
                 */
                zslog(LOGDEBUG, "Invalid zeroskip DB %s.\n",
                        priv->dotzsdbfname.buf);
                ret = 0;
                goto fail1;
        }

        /* Open .zsdb.lock file in RW and EXCL mode */
        file_lock_acquire(&dblock, priv->dbdir.buf, DOTZSDB_FNAME, 0);

        atexit(cleanup_lockfile_on_exit);
        goto done;

fail1:
        mfile_close(&zsdbfile);
        file_lock_release(&dblock);
done:
        return ret;
}

/*
 * zs_dotzsdb_update_end():
 * Releases the .dotzsdb file (locked)
 */
int zs_dotzsdb_update_end(struct zsdb_priv *priv)
{
        int ret = 1;
        unsigned char stackbuf[DOTZSDB_SIZE];
        unsigned char *sptr;
        ssize_t nr;

        if (!priv->open) {
                zslog(LOGDEBUG, "DB not opened. Please open the db before updating\n");
                return 0;
        }

        if (!dblock.active) {
                zslog(LOGDEBUG, "No active update to .zsdb\n");
                return 0;
        }

        /* Write data from priv->dotzsdb to .zsdb.lock file */
        memset(&stackbuf, 0, DOTZSDB_SIZE);
        sptr = stackbuf;

        /* Header */
        memcpy(sptr, &priv->dotzsdb.signature, sizeof(uint64_t));
        sptr += sizeof(uint64_t);

        /* Offset */
        *((uint64_t *)sptr) = hton64(priv->dotzsdb.offset);
        sptr += sizeof(uint64_t);

        /* UUID */
        memcpy(sptr, &priv->dotzsdb.uuidstr, UUID_STRLEN);
        sptr += UUID_STRLEN;

        /* Index */
        *((uint32_t *)sptr) = hton32(priv->dotzsdb.curidx);
        sptr += sizeof(uint32_t);

        /* CRC */
        priv->dotzsdb.crc = crc32c_hw(0, 0, 0);
        priv->dotzsdb.crc = crc32c_hw(priv->dotzsdb.crc,
                                      (void *)&priv->dotzsdb.signature,
                                      sizeof(uint64_t));
        priv->dotzsdb.crc = crc32c_hw(priv->dotzsdb.crc,
                                      (void *)&priv->dotzsdb.offset,
                                      sizeof(uint64_t));
        priv->dotzsdb.crc = crc32c_hw(priv->dotzsdb.crc,
                                      (void *)&priv->dotzsdb.uuidstr,
                                      UUID_STRLEN);
        priv->dotzsdb.crc = crc32c_hw(priv->dotzsdb.crc,
                                      (void *)&priv->dotzsdb.curidx,
                                      sizeof(uint32_t));

        *((uint32_t *)sptr) = hton32(priv->dotzsdb.crc);
        sptr += sizeof(uint32_t);

        sptr = stackbuf;
        while (1) {
                nr = write(dblock.fd, sptr, DOTZSDB_SIZE);
                if (nr < 0) {
                        if (errno == EINTR)
                                continue;
                }

                fsync(dblock.fd);
                break;
        }

        /* rename */
        if (file_lock_rename(&dblock, priv->dotzsdbfname.buf)) {
                zslog(LOGDEBUG, "Failed renaming %s to %s",
                      dblock.fname.buf, priv->dotzsdbfname.buf);
                ret = 0;
                goto done;
        }

done:
        /* close both .zsdb and .zsdb.lock files */
        cleanup_lockfile();

        return ret;
}
