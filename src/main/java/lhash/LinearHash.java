package lhash;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.UnsupportedEncodingException;

@SuppressWarnings("unused")
public class LinearHash {

    /**
     * Block manager instance
     */
    private final BlockManager blk_mgr;

    /**
     * Configuration instance
     */
    private LinearHashConfiguration lin_conf;

    /**
     * Properties
     */
    private int splitBlockPtr = 0;            // split block pointer
    private int visible_pool = 0;            // visible_pool size

    /**
     * Constructor that uses the default values for everything
     *
     * @param fname filename for our Key store.
     * @throws IOException is thrown when there is an I/O error during the operation.
     */
    public LinearHash(String fname) throws IOException {
        this.lin_conf = new LinearHashConfiguration(fname);
        this.visible_pool = lin_conf.getInitialVisiblePoolSize();
        blk_mgr = new BlockManager(lin_conf);
    }

    /**
     * Constructor that uses the user defined values for everything
     *
     * @param blk_fname        block filename
     * @param keysPerBlock     keys per stored block
     * @param init_pool        initial (visible) pool size
     * @param insert_bf        inserts load factor
     * @param delete_bf        deletes load factor
     * @param overrideFileFlag override file flag
     * @param epoch_thresh     epoch threshold
     * @throws IOException is thrown when there is an I/O error during the operation.
     */
    public LinearHash(String blk_fname,
                      int keysPerBlock, int init_pool,
                      float insert_bf, float delete_bf,
                      boolean overrideFileFlag, int epoch_thresh) throws IOException {

        argumentCheck(keysPerBlock, init_pool, insert_bf, delete_bf, epoch_thresh);

        this.lin_conf = new LinearHashConfiguration(blk_fname, keysPerBlock,
                init_pool, insert_bf, delete_bf, overrideFileFlag, epoch_thresh);
        this.visible_pool = lin_conf.getInitialVisiblePoolSize();
        blk_mgr = new BlockManager(lin_conf);
    }

    /**
     * Check the supplied arguments
     *
     * @param keysPerBlock keys per stored block
     * @param init_pool    initial (visible) pool size
     * @param insert_bf    inserts load factor
     * @param delete_bf    deletes load factor
     * @param epoch_thresh epoch threshold
     */
    private void argumentCheck(int keysPerBlock, int init_pool, float insert_bf,
                               float delete_bf, int epoch_thresh) {
        if (keysPerBlock < 16) {
            throw new IllegalArgumentException("We don't allow less than 16 keys per block");
        }

        if (insert_bf < 0.1 || delete_bf < 0.1) {
            throw new IllegalArgumentException("We don't allow less than 0.1 for balance factors");
        }


        if (insert_bf > .99 || delete_bf > .99) {
            throw new IllegalArgumentException("We don't allow more than .99 for balance factors");
        }

        if (init_pool < 8) {
            throw new IllegalArgumentException("We don't allow less than 8 blocks for the starting pool");
        }

        if (epoch_thresh < 10) {
            throw new IllegalArgumentException("We don't allow less than 10 ticks for each epoch");
        }
    }

    /**
     * Insert a key to the visible_pool
     *
     * @param val value to be inserted in our Key store.
     * @throws IOException is thrown when there is an I/O error during the operation.
     */
    public boolean insertKey(int val) throws IOException {
        // get the index
        int blk_idx = hf(val);
        // insert it
        if (blk_mgr.insertKey(val, blk_idx) == null) {
            return false;
        }
        // now let's check if we need to split anything
        while (blk_mgr.getBlockLF() > lin_conf.getBalanceFacotorForInserts()) {
            splitBlock(splitBlockPtr);
        }
        // issue a tick
        blk_mgr.tick();
        return true;
    }

    /**
     * Fetch the value from the table
     *
     * @param val value to be fetched.
     * @return the actual value
     * @throws IOException is thrown when there is an I/O error during the operation.
     */
    public Integer fetchKey(int val) throws IOException {
        // get the index
        int blk_idx = hf(val);
        // try to fetch the value
        Integer res = blk_mgr.fetchKey(val, blk_idx);
        // issue a tick
        blk_mgr.tick();
        if (res == null) {
            System.err.println("Key not found, returning 0 instead");
        }
        return (res);
    }

    /**
     * Delete a key from the visible_pool
     *
     * @param val value to be deleted.
     * @throws IOException is thrown when there is an I/O error during the operation.
     */
    public boolean deleteKey(int val) throws IOException {
        // get the index
        int blk_idx = hf(val);
        // delete it
        boolean ret = blk_mgr.deleteKey(val, blk_idx);
        // check if we need to merge something
        while (blk_mgr.getBlockLF() < lin_conf.getBalanceFactorForDeletes() &&
                !((visible_pool == lin_conf.getInitialVisiblePoolSize()) &&
                        (splitBlockPtr == 0))) {
            mergeBlock(splitBlockPtr);
        }
        blk_mgr.tick();
        return ret;
    }

    /**
     * Our hash function
     *
     * @param val value to be hashed
     * @return the mapped value based on our hash function.
     */
    private int hf(int val) {
        int blk_idx = Math.abs(val % visible_pool);
        // check if we need to use more hash function bits
        if (blk_idx < splitBlockPtr) {
            blk_idx = Math.abs(val % (2 * visible_pool));
        }
        return (blk_idx);
    }

    /**
     * Split the block given
     *
     * @param blk_num block number
     * @throws IOException is thrown when there is an I/O error during the operation.
     */
    private void splitBlock(int blk_num) throws IOException {
        int cblk_idx;
        // we need to add a block
        blk_mgr.addBlock();
        splitBlockPtr++;
        int blk_ent[] = blk_mgr.fetchBlock(blk_num);

        if (lin_conf.isDebugEnabled()) {
            System.err.println("Splitting block: " + blk_num);
        }

        // perform such actions only if we received some elements!
        if (blk_ent != null) {
            for (int aBlk_ent : blk_ent) {
                cblk_idx = hf(aBlk_ent);
                // check if we need to move the key
                if (cblk_idx > blk_num)
                // move the key to the split block
                {
                    moveKey(aBlk_ent, blk_num);
                }
            }
        }
        // check if we are maxed out; if so increase the visible_pool.
        if (splitBlockPtr > (visible_pool - 1)) {
            splitBlockPtr = 0;
            visible_pool = 2 * visible_pool;
        }
    }

    /**
     * Merge the given block
     *
     * @param blk_num block number
     * @throws IOException is thrown then there is an I/O error during the operation.
     */
    private void mergeBlock(int blk_num) throws IOException {
        int mblk_idx,    // merge block index
                blk_ent[];    // block entries
        // calculate merge index
        mblk_idx = (visible_pool - 1) + blk_num;

        if (lin_conf.isDebugEnabled()) {
            System.err.println("Merging block!");
        }

        // decrease the overflow block pointer
        if (splitBlockPtr > 0) {
            splitBlockPtr--;
        }
        // adjust the split pointer
        else {
            visible_pool = visible_pool / 2;
            splitBlockPtr = visible_pool - 1;
        }

        // get the block contents
        blk_ent = blk_mgr.fetchBlock(mblk_idx);
        if (blk_ent != null) {
            // move keys
            for (int aBlk_ent : blk_ent) {
                moveKey(aBlk_ent, mblk_idx);
            }
        }
        // delete the block
        blk_mgr.deleteBlock();
    }

    /**
     * Move a key from one block to another
     *
     * @param val   value to be moved
     * @param d_blk block to be placed
     * @throws IOException is thrown when there is an I/O error during the operation.
     */
    private void moveKey(int val, int d_blk) throws IOException {
        int c_idx = hf(val);
        if (lin_conf.isDebugEnabled()) {
            System.out.println("delete block is: " + d_blk +
                    " Insert block is: " + c_idx + " key is: " + val);
        }
        blk_mgr.insertKey(val, c_idx);
        blk_mgr.deleteKey(val, d_blk);
    }

    /**
     * Probe block manager to print quick statistics about the block file
     */
    public void printQuickStatReport() {
        blk_mgr.printBlockManagerHealth();
    }

    /**
     * Print block file contents
     *
     * @throws IOException is thrown when there is an error reading the binary file.
     */
    public void printBlockContents() throws IOException {
        blk_mgr.printContents();
    }

    /**
     * Print the stored epoch data from block manager
     */
    public void printEpochData() {
        if (!lin_conf.isTrackingEnabled()) {
            System.err.println(" -- Tracking is not enabled, printing aborted");
            return;
        }
        blk_mgr.getPerfTrackerInstance().printAllEpochStatistics();
    }

    /**
     * Dumps all the stored epoch data to a file.
     *
     * @param fname filename of epoch dump file.
     * @throws FileNotFoundException        is thrown then the file is not found
     * @throws UnsupportedEncodingException is thrown when the file is in unsupported encoding
     */
    public void dumpEpochDataToFile(String fname)
            throws FileNotFoundException, UnsupportedEncodingException {
        if (!lin_conf.isTrackingEnabled()) {
            System.err.println(" -- Tracking is not enabled, printing to file aborted");
            return;
        }
        blk_mgr.getPerfTrackerInstance().printAllEpochStatisticsToFile(fname);
    }

    /**
     * Get the debug flag
     *
     * @return true of debug is enabled false otherwise
     */
    public boolean getDebugFlag() {
        return (lin_conf.isDebugEnabled());
    }

    /**
     * Toggle the debug flag
     */
    public void toggleDebugFlag() {
        lin_conf.toggleDebugFlag();
    }

    /**
     * Close and commit the file.
     *
     * @throws IOException is thrown when we cannot close the file.
     */
    public void commitFile() throws IOException {
        blk_mgr.commitFile();
    }
}
