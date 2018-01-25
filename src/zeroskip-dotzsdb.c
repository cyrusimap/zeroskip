/*
 * zeroskip-dotzsdb.c : zeroskip .zsdb manipulation functions
 *
 * This file is part of skiplistdb.
 *
 * skiplistdb is free software; you can redistribute it and/or modify
 * it under the terms of the MIT license. See LICENSE for details.
 *
 */

#include "zeroskip.h"
#include "zeroskip-priv.h"

/*
 * zs_dotzsdb_create():
 * Creates a .zsdb file in the DB directory. This function assumes the caller
 * has sanitised the input. This function also generates the UUID for the DB
 * since this function is called when the DB is created first.
 *
 * The .zsdb file in a DB directory has the following structure:
 *      ZSDB Signature  -  64 bits
 *      current index   -  32 bits
 *      Parsed uuid str - 288 bits
 */
int zs_dotzsdb_create(struct zsdb_priv *priv)
{
        unsigned char stackbuf[DOTZSDB_SIZE];
        uuid_t uuid;
        unsigned char *sptr;
        struct mappedfile *mf;
        int ret = ZS_OK;
        size_t nbytes = 0;
        cstring dotzsdbfname = CSTRING_INIT;

        memset(&stackbuf, 0, DOTZSDB_SIZE);
        sptr = stackbuf;

        /* Generate a new uuid */
        uuid_generate(uuid);
        uuid_unparse_lower(uuid, priv->dotzsdb.uuidstr);

        /* The filename */
        cstring_dup(&priv->dbdir, &dotzsdbfname);
        cstring_addch(&dotzsdbfname, '/');
        cstring_addstr(&dotzsdbfname, DOTZSDB_FNAME);

        /* Header */
        priv->dotzsdb.signature = ZS_SIGNATURE;
        memcpy(sptr, &priv->dotzsdb.signature, sizeof(uint64_t));
        sptr += sizeof(uint64_t);

        /* Index */
        priv->dotzsdb.curidx = 0;
        *((uint32_t *)sptr) = hton32(0);
        sptr += sizeof(uint32_t);

        /* UUID */
        memcpy(sptr, &priv->dotzsdb.uuidstr, UUID_STRLEN);
        sptr += UUID_STRLEN;


        /* Write to file */
        if (mappedfile_open(dotzsdbfname.buf, MAPPEDFILE_RW_CR, &mf) != 0) {
                fprintf(stderr, "Could not create %s!", dotzsdbfname.buf);
                ret = ZS_ERROR;
                goto fail1;
        }

        if (mappedfile_write(&mf, &stackbuf, DOTZSDB_SIZE, &nbytes) != 0) {
                fprintf(stderr, "Could not write to file %s!",
                        dotzsdbfname.buf);
                ret = ZS_ERROR;
                goto fail2;
        }

        mappedfile_flush(&mf);

fail2:
        mappedfile_close(&mf);

fail1:
        cstring_release(&dotzsdbfname);
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
        struct mappedfile *mf;
        size_t mfsize;
        struct dotzsdb *dothdr;
        int ret = 1;
        cstring dotzsdbfname = CSTRING_INIT;

        /* The filename */
        cstring_dup(&priv->dbdir, &dotzsdbfname);
        cstring_addch(&dotzsdbfname, '/');
        cstring_addstr(&dotzsdbfname, DOTZSDB_FNAME);

        if (mappedfile_open(dotzsdbfname.buf, MAPPEDFILE_RD, &mf) != 0) {
                fprintf(stderr, "Could not open %s!\n", dotzsdbfname.buf);
                ret = 0;
                goto fail1;
        }

        mappedfile_size(&mf, &mfsize);

        if (mfsize < DOTZSDB_SIZE) {
                fprintf(stderr, "File too small to be zeroskip DB: %zu.\n",
                        mfsize);
                ret = 0;
                goto fail2;
        }

        dothdr = (struct dotzsdb *)mf->ptr;
        if (dothdr->signature == ZS_SIGNATURE) {
                /* Signature */
                priv->dotzsdb.signature = dothdr->signature;

                /* Index */
                priv->dotzsdb.curidx = ntoh32(dothdr->curidx);

                /* UUID str */
                memcpy(&priv->dotzsdb.uuidstr,
                       mf->ptr + sizeof(priv->dotzsdb.signature) +
                       sizeof(priv->dotzsdb.curidx),
                       sizeof(priv->dotzsdb.uuidstr));
                uuid_parse(priv->dotzsdb.uuidstr, priv->uuid);
        } else {
                fprintf(stderr, "Invalid zeroskip DB %s.\n",
                        dotzsdbfname.buf);
                ret = 0;
                goto fail2;
        }

fail2:
        mappedfile_close(&mf);
fail1:
        cstring_release(&dotzsdbfname);
        return ret;
}

/*
 * zs_dotzsdb_update_index():
 * Updates the current index in the .zsdb file.
 */
int zs_dotzsdb_update_index(struct zsdb_priv *priv, uint32_t idx)
{
        struct mappedfile *mf;
        size_t mfsize;
        struct dotzsdb *dothdr;
        cstring dotzsdbfname = CSTRING_INIT;
        int ret = 1;

        /* The filename */
        cstring_dup(&priv->dbdir, &dotzsdbfname);
        cstring_addch(&dotzsdbfname, '/');
        cstring_addstr(&dotzsdbfname, DOTZSDB_FNAME);

        if (mappedfile_open(dotzsdbfname.buf, MAPPEDFILE_RD, &mf) != 0) {
                fprintf(stderr, "Could not open %s!\n", dotzsdbfname.buf);
                ret = 0;
                goto fail1;
        }

        mappedfile_size(&mf, &mfsize);

        if (mfsize < DOTZSDB_SIZE) {
                fprintf(stderr, "File too small to be zeroskip DB: %zu.\n",
                        mfsize);
                ret = 0;
                goto fail2;
       }

        dothdr = (struct dotzsdb *)mf->ptr;
        if (dothdr->signature == ZS_SIGNATURE) {
                /* Update the index */
                dothdr->curidx = hton32(idx);
                priv->dotzsdb.curidx = idx;
        } else {
                fprintf(stderr, "Invalid zeroskip DB %s Failed updating index.\n",
                        dotzsdbfname.buf);
                ret = 0;
                goto fail2;
        }

        mappedfile_flush(&mf);

fail2:
        mappedfile_close(&mf);
fail1:
        cstring_release(&dotzsdbfname);
        return ret;
}
