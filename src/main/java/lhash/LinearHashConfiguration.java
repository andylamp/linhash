package lhash;

@SuppressWarnings("unused")
class LinearHashConfiguration {

    private final int defaultVisiblePoolSize = 16;
    private final float defaultInsertionsBF = 0.8f;
    private final float defaultDeletionsBF = 0.5f;
    /* hash statistics */
    private final int init_pool;    // initial visible_pool size
    private final float bf_insert;  // balancing factor for insertions
    private final float bf_delete;  // balancing factor for deletions
    /* Default values */
    private final int defaultKeysPerPage = 64;          // default keys per page value
    private final int initial_blk_mgr_pool_size = 32;   // default block pool size
    private final int keyByteSize = 4;                  // handling only integers
    private final int defaultTickThresh = 100;          // per 100 ticks reset.
    private final String fileMode = "rw";               // default mode R/W
    private final int blk_hoffset = 2 * keyByteSize;    // header offset for each block (2 * base key size)


    /**
     * Block manager configuration
     */

    /* Properties */
    private final int keysPerBlock; // keys per block
    private final int bytesPerBlock;// bytes per block
    private final String blk_fname; // filename of the block storage


    /**
     * Hash configuration
     */


    /* Debug state */
    private boolean DEBUG_FLAG = false;
    /* Track performance */
    private boolean TRACK_PERF = true;
    /* tick threshold (for epochs) */
    private int tick_thresh;
    /* file override flag */
    private boolean overrideFileFlag = true;

    /**
     * Default constructor for {@link LinearHashConfiguration}
     *
     * @param blk_fname file which the keys will be stored.
     */
    LinearHashConfiguration(String blk_fname) {

        this.blk_fname = blk_fname;

        /* hash */
        this.init_pool = defaultVisiblePoolSize;
        this.bf_insert = defaultInsertionsBF;
        this.bf_delete = defaultDeletionsBF;

        /* block mgr parameters */
        this.keysPerBlock = defaultKeysPerPage;
        bytesPerBlock = calculateBlockByteSize(keyByteSize, keysPerBlock);
        this.tick_thresh = defaultTickThresh;
    }

    /**
     * Custom constructor for {@link LinearHashConfiguration}
     *
     * @param blk_fname        file which the keys will be stored.
     * @param keysPerBlock     keys allowed per block.
     * @param init_pool        initial pool size.
     * @param insert_bf        inserts balance factor
     * @param delete_bf        deletes balance factor
     * @param overrideFileFlag override file flag
     * @param epoch_thresh     epoch ticks
     */
    LinearHashConfiguration(String blk_fname,
                            int keysPerBlock, int init_pool,
                            float insert_bf, float delete_bf,
                            boolean overrideFileFlag, int epoch_thresh) {
        this.blk_fname = blk_fname;

        this.init_pool = init_pool;
        this.bf_insert = insert_bf;
        this.bf_delete = delete_bf;

        this.overrideFileFlag = overrideFileFlag;

        /* block manager parameters */
        this.keysPerBlock = keysPerBlock;
        this.bytesPerBlock = calculateBlockByteSize(keyByteSize, keysPerBlock);

        this.tick_thresh = epoch_thresh;
    }


    /**
     * Return the filename which we store our keys
     *
     * @return the finame of our binary file.
     */
    String getFilename() {
        return blk_fname;
    }

    /**
     * Returns the initial visible pool size.
     *
     * @return return the initial visible pool size.
     */
    int getInitialVisiblePoolSize() {
        return init_pool;
    }

    /**
     * Return the balance factor for inserts
     *
     * @return return the balance factor for inserts
     */
    float getBalanceFactorForInserts() {
        return bf_insert;
    }

    /**
     * Return the balance factor for deletes
     *
     * @return return the balance factor for deletes
     */
    float getBalanceFactorForDeletes() {
        return bf_delete;
    }


    /**
     * Get the debug flag
     *
     * @return true if debug is enabled, false otherwise.
     */
    boolean isDebugEnabled() {
        return (DEBUG_FLAG);
    }

    /**
     * Toggle the debug flag
     */
    void toggleDebugFlag() {
        DEBUG_FLAG = !DEBUG_FLAG;
    }

    /**
     * Get the value of the track perf. flag
     *
     * @return check if we have tracking of perf. enabled
     */
    boolean isTrackingEnabled() {
        return (TRACK_PERF);
    }

    /**
     * Toggle the tracking flag
     */
    void toggleTrackFlag() {
        TRACK_PERF = !TRACK_PERF;
    }

    /**
     * calculates the block size
     *
     * @param byteSize     byte size in our current setting (usually will be 8)
     * @param keysPerBlock stored keys per each block.
     * @return the block size in bytes.
     */
    private int calculateBlockByteSize(int byteSize, int keysPerBlock) {
        return ((byteSize * keysPerBlock) + blk_hoffset);
    }

    /**
     * Return the keys per block
     *
     * @return the keys per block
     */
    int getKeysPerBlock() {
        return keysPerBlock;
    }

    /**
     * Return the bytes per block
     *
     * @return the bytes per block
     */
    int getBytesPerBlock() {
        return bytesPerBlock;
    }

    /**
     * Return they key size in bytes
     *
     * @return the key size in bytes
     */
    int getKeyByteSize() {
        return keyByteSize;
    }

    /**
     * Return the value (in bytes) of the block header offset
     *
     * @return the value (in bytes) of the block header offset
     */
    int getBlk_hoffset() {
        return blk_hoffset;
    }

    /**
     * Return the file mode (for R/W perms)
     *
     * @return return the file mode flags
     */
    String getFileMode() {
        return fileMode;
    }

    /**
     * Return the override flag value
     *
     * @return the override flag value
     */
    boolean getOverrideFlag() {
        return overrideFileFlag;
    }

    /**
     * Return the initial Block manager pool size
     *
     * @return the initial block manager pool size
     */
    int getInitialBlockManagerPoolSize() {
        return initial_blk_mgr_pool_size;
    }

    /**
     * Return the number of ticks per epoch
     *
     * @return the number of ticks per epoch
     */
    int getTickThresh() {
        return tick_thresh;
    }

}
