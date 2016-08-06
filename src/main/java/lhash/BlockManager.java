package lhash;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.text.SimpleDateFormat;

@SuppressWarnings("unused")
class BlockManager {

    /**
     * Properties
     */

    private int poolSize;        // size of our block pool (actual number of discrete blocks)
    private int ovf_blocks;        // current overflow blocks

    /* these stay */
    private long initFileSize;    // initial file size
    private long curFileSize;    // current file size

    private int key_num;            // total stored key number

    private LinearHashConfiguration lin_conf;    // configuration instance.
    private LinearHashPerfLog lin_perf;            // performance tracker instance.

    private RandomAccessFile blk_file;

    /**
     * Initialize the block manager using the specified configuration instance.
     *
     * @param lin_conf the {@link LinearHashConfiguration} instance.
     * @throws IOException is thrown when there is an I/O error during the operation.
     */
    BlockManager(LinearHashConfiguration lin_conf) throws IOException {
        this.lin_conf = lin_conf;
        poolSize = lin_conf.getInitialBlockManagerPoolSize();
        ovf_blocks = 0;
        key_num = 0;
        curFileSize = prepareBlockStorage(lin_conf.getFilename(),
                lin_conf.getOverrideFlag());
        if (lin_conf.isTrackingEnabled()) {
            this.lin_perf = new LinearHashPerfLog(lin_conf);
        }
    }

    /**
     * Prepares the file for writing
     *
     * @param fname    filename for our binary file.
     * @param override flag for truncating or keeping the binary file (if already present)
     * @return the initial file size.
     * @throws IOException is thrown when there is an I/O error during the operation.
     */
    private long prepareBlockStorage(String fname, boolean override) throws IOException {
        File f = new File(fname);
        // check if the file exists and depending on the override flag perform actions
        if (f.exists()) {
            if (override) {
                System.err.println("File " + fname + " already exists, erasing it.");
                if (f.delete()) {
                    System.err.println("File :" + fname + " was cleared successfully.");
                } else {
                    System.err.println("Encountered error while clearing contents of file: " + fname);
                    throw new IOException("Couldn't clear file contents");
                }
            } else {
                System.err.println("File " + fname + " already exists, opening.");
            }
        }
        // in any case, open it.
        blk_file = new RandomAccessFile(f, lin_conf.getFileMode());
        // now check if the file was created now and expand it to the necessary size.
        if (override) {
            blk_file.setLength(0);
            blk_file.setLength(blockOffset(poolSize));
        }
        // update file length
        initFileSize = blk_file.length();
        return (initFileSize);
    }

    /**
     * Get the current pool size
     *
     * @return block pool size
     */
    public int getPoolSize() {
        return poolSize;
    }

    /**
     * Calculate the relative file size growth
     *
     * @return calculate the file size of our binary file based on the current number of blocks.
     */
    private double calculateRelativeFileSize() {
        return (((double) curFileSize / (double) initFileSize));
    }

    /**
     * Get the (average) load factor of the blocks
     *
     * @return the load factor of the block.
     */
    double getBlockLF() {
        return (key_num / (poolSize * lin_conf.getKeysPerBlock() * 1.0));
    }

    /**
     * Calculate the pool size.
     *
     * @return the pool size.
     */

    private int calcPoolSize() {
        return (lin_conf.getBytesPerBlock() * poolSize);
    }

    /**
     * Calculate the position in the file based on offset and key byte size
     *
     * @param offset index offset that we calculate.
     * @return the actual index offset from the start of the file for the requested block.
     */
    private int offsetCalc(int offset) {
        return ((lin_conf.getBytesPerBlock() * offset) + lin_conf.getKeyByteSize());
    }

    /**
     * Calculate the position after the overflow block padding.
     *
     * @return the actual block offset of the overflow page.
     */
    private int ovfPadCalc() {
        return (lin_conf.getBytesPerBlock() * (poolSize + ovf_blocks));
    }

    /**
     * find the block header
     *
     * @param blk_num the block for which we calculate the actual index.
     * @return the actual block offset from the start of the file.
     */
    private int blockOffset(int blk_num) {
        return (lin_conf.getBytesPerBlock() * blk_num);
    }

    /**
     * Place a block in the file
     *
     * @throws IOException is thrown when there is an I/O error during the operation.
     */
    void addBlock() throws IOException {
        int ovf_ptr;
        int poolBytes = calcPoolSize();

        if (lin_conf.isDebugEnabled()) {
            System.err.println("Adding a block with number " + (poolSize));
        }

        blk_file.seek(poolBytes);
        // create an overflow array
        long shiftBytes = blk_file.length() - blk_file.getFilePointer();
        byte ovf_shift[] = new byte[(int) shiftBytes];

        if (lin_conf.isDebugEnabled()) {
            System.out.println("new block, shifting bytes: " + shiftBytes);
        }

        // read the overflow parts of the file
        blk_file.readFully(ovf_shift);
        blk_file.setLength(poolBytes);

        // increase blocks
        this.poolSize++;
        // recalculate the pool size
        poolBytes = calcPoolSize();
        // expand the file
        blk_file.setLength(poolBytes);
        blk_file.seek(poolBytes);
        // finally write the ovf block
        blk_file.write(ovf_shift);

        // forward ovf block pointer update
        for (int i = 0; i < ((poolSize + ovf_blocks) - 1); i++) {
            // seek overflow block, read if we have next
            blk_file.seek(offsetCalc(i));
            ovf_ptr = blk_file.readInt();
            // if we have a block, seek and write it.
            if (ovf_ptr != 0) {
                blk_file.seek(offsetCalc(i));
                blk_file.writeInt(ovf_ptr + 1);
            }
        }

        // reverse ovf block pointer update
        for (int i = poolSize; i < (poolSize + ovf_blocks); i++) {
            // seek again overflow block, read if we have next
            blk_file.seek(blockOffset(i));
            ovf_ptr = blk_file.readInt();
            // if the block mark exist write it.
            if (ovf_ptr >= (poolSize - 1)) {
                blk_file.seek(blockOffset(i));
                blk_file.writeInt(ovf_ptr + 1);
            }
        }
        // update current file-size (in bytes)
        curFileSize = blk_file.length();
    }

    /**
     * Add an overflow block to the file
     *
     * @param blk_num block number in which we add the overflow.
     * @throws IOException is thrown when there is an I/O error during the operation.
     */
    private void addOvfBlock(int blk_num) throws IOException {
        int ovf_blk,        // overflow block offset
                ovf_nptr;        // overflow block next

        // no overflow block present
        if ((poolSize - 1) < blk_num) {
            return;
        }

        // else advance ovf counter
        this.ovf_blocks++;
        int ovf_pad = ovfPadCalc();
        // now adjust the length of the file to account for the new ovf block
        blk_file.setLength(ovf_pad);
        blk_file.seek(blockOffset(blk_num));
        // parse the keys in current block
        blk_file.readInt();
        ovf_blk = blk_file.readInt();

        // check if we didn't have any overflow blocks before
        if (ovf_blk == 0) {
            // seek file in correct position and write correct value
            blk_file.seek(offsetCalc(blk_num));
            // update counter
            blk_file.writeInt((poolSize + ovf_blocks) - 1);
            // update previous block pointer
            blk_file.seek(blockOffset((poolSize + ovf_blocks) - 1));
            blk_file.writeInt(blk_num);
        } else {
            // seek to the correct block before adding
            while (true) {
                blk_file.seek(offsetCalc(ovf_blk));
                ovf_nptr = blk_file.readInt();
                if (ovf_nptr == 0) {
                    break;
                }
                ovf_blk = ovf_nptr;
            }
            // perform update and write operation on the file.
            blk_file.seek(offsetCalc(ovf_blk));
            blk_file.writeInt((poolSize + ovf_blocks) - 1);
            blk_file.seek(blockOffset((poolSize + ovf_blocks) - 1));
            blk_file.writeInt(ovf_blk);
        }
        // update current file size (in bytes)
        curFileSize = blk_file.length();
    }

    /**
     * Function that fetches a particular block
     *
     * @param blk_num block number that we fetch.
     * @return the block keys (in an array)
     * @throws IOException is thrown when there is an I/O error during the operation.
     */
    int[] fetchBlock(int blk_num) throws IOException {
        int blk_keys,    // number of keys in the block
                ovf_ptr;    // over flow block presence flag

        int blk_con[];    // block elements
        blk_file.seek(blockOffset(blk_num));
        // read number of keys and ovf blocks (if any)
        blk_keys = blk_file.readInt();
        ovf_ptr = blk_file.readInt();

        if (lin_conf.isDebugEnabled()) {
            System.err.println("fetching Block (" + blk_num + ") keys: " +
                    blk_keys + " ovf_ptr: " + ovf_ptr);
        }

        // nothing in this bucket
        if (blk_keys == 0) {
            return (null);
        }
        // allocate key array
        blk_con = new int[blk_keys];
        for (int i = 1; i < (blk_keys + 1); i++) {
            // read elements
            blk_con[i - 1] = blk_file.readInt();
            // if block end reached, check for overflow
            // block presence and go there
            if ((i % lin_conf.getKeysPerBlock()) == 0) {
                // update ovf pointer as well.
                blk_file.seek(offsetCalc(ovf_ptr));
                ovf_ptr = blk_file.readInt();
            }
        }
        // finally return the elements
        return (blk_con);
    }

    /**
     * Delete the specified overflow block
     *
     * @param blk_num block number that we delete (must be overflow).
     * @throws IOException is thrown when there is an I/O error during the operation.
     */
    private void deleteOvfBlock(int blk_num) throws IOException {
        int ovf_ptr,        // ovf block pointer (initial)
                ovf_nptr,        // ovf block pointer next
                ovf_pptr,        // ovf block pointer previous
                ovf_cptr;        // ovf block pointer iterator
        long shift_cnt;        // number of bytes to shift
        byte blk_shift[];    // shift file part
        // check if doesn't exist.
        if ((poolSize - 1) < blk_num) {
            return;
        }
        // seek to correct position
        blk_file.seek(offsetCalc(blk_num));
        // read the ovf block presence
        ovf_ptr = blk_file.readInt();
        // no ovf blocks, return
        if (ovf_ptr == 0) {
            return;
        }
        // traverse to the end of the ovf blocks while
        // keeping offset pointers for previous, next
        // and current ovf blocks
        while (true) {
            blk_file.seek(blockOffset(ovf_ptr));
            ovf_pptr = blk_file.readInt();
            ovf_nptr = blk_file.readInt();
            if (ovf_nptr == 0) {
                break;
            }
            ovf_ptr = ovf_nptr;
        }
        // to go the previous ovf block
        blk_file.seek(offsetCalc(ovf_pptr));
        // mark it as an end block
        blk_file.writeInt(0);
        // calculate shift-FROM position (end of all blocks)
        blk_file.seek(blockOffset(ovf_ptr + 1));
        // calculate shift length
        shift_cnt = blk_file.length() - blk_file.getFilePointer();
        // allocate shift bytes
        blk_shift = new byte[(int) shift_cnt];
        // read the rest of the file in memory before shifting
        blk_file.readFully(blk_shift);
        // decrement ovf blocks
        ovf_blocks--;
        // remove the block
        blk_file.setLength(blockOffset(ovf_ptr));
        // seek the correct position and write the buffered part
        blk_file.seek(blockOffset(ovf_ptr));
        blk_file.write(blk_shift);
        for (int i = 0; i < ((poolSize + ovf_blocks) - 1); i++) {
            blk_file.seek(offsetCalc(i));
            // update current the pointer
            ovf_cptr = blk_file.readInt();
            // this is to update the rest of the pointers
            if (ovf_cptr > ovf_ptr) {
                blk_file.seek(offsetCalc(i));
                blk_file.writeInt(ovf_cptr - 1);
            }
        }
        // update the reverse pointers of each block
        for (int i = poolSize; i < ((poolSize + ovf_blocks)); i++) {
            blk_file.seek(blockOffset(i));
            ovf_cptr = blk_file.readInt();
            // this is to update the rest of the pointers
            if (ovf_cptr > ovf_ptr) {
                blk_file.seek(blockOffset(i));
                blk_file.writeInt(ovf_cptr - 1);
            }
        }
        // update current file size (in bytes)
        curFileSize = blk_file.length();
    }

    /**
     * This function deletes a block from the file
     *
     * @throws IOException is thrown when there is an I/O error during the operation.
     */
    void deleteBlock() throws IOException {
        int ovf_ptr,        // ovf block pointer
                poolByteSz;        // pool size in bytes
        long shift_cnt;        // shift bytes count
        byte blk_shift[];    // shift file part
        // seek to end of blocks
        poolByteSz = calcPoolSize();
        blk_file.seek(poolByteSz);
        // calculate shift size
        shift_cnt = blk_file.length() - blk_file.getFilePointer();
        // allocate shift part
        blk_shift = new byte[(int) shift_cnt];
        // load it to memory
        blk_file.readFully(blk_shift);
        poolSize--;
        poolByteSz = calcPoolSize();
        blk_file.setLength(poolByteSz);
        // go to the end of the file
        blk_file.seek(poolByteSz);
        blk_file.write(blk_shift);

        // update the pointers
        for (int i = 0; i < ((poolSize + ovf_blocks) - 1); i++) {
            blk_file.seek(offsetCalc(i));
            ovf_ptr = blk_file.readInt();
            if (ovf_ptr != 0) {
                blk_file.seek(offsetCalc(i));
                blk_file.writeInt(ovf_ptr - 1);
            }
        }
        // update reverse pointers
        for (int i = poolSize; i < (poolSize + ovf_blocks); i++) {
            blk_file.seek(blockOffset(i));
            ovf_ptr = blk_file.readInt();
            if (ovf_ptr > poolSize) {
                blk_file.seek(blockOffset(i));
                blk_file.writeInt(ovf_ptr - 1);
            }
        }
        // update the current file size (in bytes)
        curFileSize = blk_file.length();
    }

    /**
     * Insert a value to a specific block
     *
     * @param val     value to insert in a block.
     * @param blk_num block where we will insert the provided value.
     * @throws IOException is thrown when there is an I/O error during the operation.
     */
    Integer insertKey(int val, int blk_num) throws IOException {
        int blk_keys,    // keys in block
                ovf_ptr,    // ovf block pointer
                c_key;        // current key

        // increment i/o counter
        lin_perf.incrementBothIO();
        // seek to the correct block
        blk_file.seek(blockOffset(blk_num));
        // get the key in block
        blk_keys = blk_file.readInt();
        // get the ovf block pointer
        ovf_ptr = blk_file.readInt();

        if (lin_conf.isDebugEnabled()) {
            System.out.println("Block (" + blk_num + ") keys: " +
                    blk_keys + " ovf_ptr: " + ovf_ptr +
                    " inserting value: " + val);
        }

        if (blk_keys == 0) {
            // write the actual key
            blk_file.writeInt(val);
            // seek back to the block header
            blk_file.seek(blockOffset(blk_num));
            // update the keys count in block
            blk_file.writeInt(blk_keys + 1);
            // update the total key count
            this.key_num++;
            // all OK
            return (val);
        }
        // now read the keys
        for (int i = 1; i < (blk_keys + 1); i++) {
            c_key = blk_file.readInt();
            // no duplicate keys (keys are singletons)
            if (c_key == val) {
                if (lin_conf.isDebugEnabled()) {
                    System.err.println("Block (" + blk_num + ") keys: " +
                            blk_keys + " Duplicate key: " + val);
                }
                return (null);
            }
            // advance ovf pointer if needed (EOF of current block)
            ovf_ptr = getOvf_ptr(ovf_ptr, blk_file, i);
        }


        // check if we need to add an overflow block
        if ((blk_keys % lin_conf.getKeysPerBlock()) == 0) {
            lin_perf.incrementBothIO();
            // we need to add an overflow block
            addOvfBlock(blk_num);
            // seek to the new overflow block, plus the block header offset
            blk_file.seek(blockOffset(poolSize + ovf_blocks - 1) + lin_conf.getBlk_hoffset());
            // finally write the value
            blk_file.writeInt(val);

        } else
        // no ovf needed, we can write the value as is.
        {
            blk_file.writeInt(val);
        }

        // seek back to the block header
        blk_file.seek(blockOffset(blk_num));
        // update the keys count in block
        //System.out.println("Writing block keys number:" + (blk_keys+1));
        blk_file.writeInt(blk_keys + 1);
        // update the total key count
        this.key_num++;
        // finally return
        return (val);
    }

    /**
     * Delete a value from a specified block
     *
     * @param val     value to delete from the block.
     * @param blk_num block number where we delete from.
     * @return if we were successful.
     */
    boolean deleteKey(int val, int blk_num) throws IOException {
        int blk_keys,        // keys in block
                ovf_ptr,        // ovf block pointer
                c_key,            // current key
                l_key;            // last key value

        long key_loc = 0L,        // key location
                key_ploc = 0L;        // past key location

        // given block is out of range...
        if (blk_num >= poolSize) {
            return (false);
        }
        lin_perf.incrementBothIO();
        // seek to the correct block
        blk_file.seek(blockOffset(blk_num));
        // read block header
        blk_keys = blk_file.readInt();
        ovf_ptr = blk_file.readInt();
        // check if have keys inside this block (if not just return)
        if (blk_keys == 0) {
            if (lin_conf.isDebugEnabled()) {
                System.out.println("Zero keys in block");
            }
            return (false);
        }

        //System.out.println("Block (" + blk_num + ") keys: " + blk_keys +
        // " ovf_ptr: " + ovf_ptr + " deleting value: " + val);

        // traverse the block
        for (int i = 1; i < (blk_keys + 1); i++) {
            key_ploc = blk_file.getFilePointer();
            c_key = blk_file.readInt();
            // check if we found our key and adjust the location
            if (c_key == val) {
                key_loc = blk_file.getFilePointer() - lin_conf.getKeyByteSize();
            }
            // let's check if we need to go to overflow pages
            ovf_ptr = getOvf_ptr(ovf_ptr, blk_file, i);
        }

        // check if we found the key
        if (key_loc == key_ploc) {
            blk_file.seek(key_loc);
            blk_file.writeInt(0);
        }
        // key was not found, at all.
        else if (key_loc == 0) {
            if (lin_conf.isDebugEnabled()) {
                System.err.println("Key (" + val + ") not found... to delete...");
            }
            return (false);
        } else {
            blk_file.seek(key_ploc);
            l_key = blk_file.readInt();
            blk_file.seek(key_ploc);
            blk_file.writeInt(0);
            blk_file.seek(key_loc);
            blk_file.writeInt(l_key);
        }
        // go back to block header and update it
        blk_file.seek(blockOffset(blk_num));
        blk_keys--;
        blk_file.writeInt(blk_keys);
        // do we need to delete this ovf block?
        if ((blk_keys > 0) && ((blk_keys % lin_conf.getKeysPerBlock()) == 0)) {
            if (lin_conf.isDebugEnabled()) {
                System.out.println("Deleting Ovf bucket: " + blk_num);
            }
            deleteOvfBlock(blk_num);
        }
        // decrease the total key count
        this.key_num--;
        // finally return
        return (true);
    }


    /**
     * Fetch the overflow pointer
     *
     * @param ovf_ptr    our current overflow pointer.
     * @param block_file our binary file.
     * @param key_count  key count
     * @return the overflow pointer index.
     * @throws IOException is thrown when there is an I/O error during the operation.
     */
    private int getOvf_ptr(int ovf_ptr, RandomAccessFile block_file, int key_count)
            throws IOException {
        if ((key_count % lin_conf.getKeysPerBlock()) == 0) {
            lin_perf.incrementBothIO();
            block_file.seek(offsetCalc(ovf_ptr));
            ovf_ptr = block_file.readInt();
        }
        return ovf_ptr;
    }

    /**
     * Fetches a key from our Key storage
     *
     * @param val     value to find
     * @param blk_num block to navigate
     * @return the Integer value corresponding to the value provided
     * @throws IOException is thrown when there is an I/O error during the operation.
     * @throws IOException is thrown when there is an I/O error during the operation.
     */
    Integer fetchKey(int val, int blk_num) throws IOException {
        int blk_keys,        // keys in block
                ovf_ptr,    // ovf block pointer
                c_key;        // current key

        // given block is out of range...
        if (blk_num >= poolSize) {
            return (null);
        }
        // open the file
        lin_perf.incrementBothIO();
        // seek to the correct block
        blk_file.seek(blockOffset(blk_num));
        // read block header
        blk_keys = blk_file.readInt();
        ovf_ptr = blk_file.readInt();
        // check if have keys inside this block (if not just return)
        if (blk_keys == 0) {
            if (lin_conf.isDebugEnabled()) {
                System.out.println("Zero keys in block");
            }
            return (null);
        }

        //System.out.println("Block (" + blk_num + ") keys: " + blk_keys +
        // " ovf_ptr: " + ovf_ptr + " deleting value: " + val);

        // traverse the block
        for (int i = 1; i < (blk_keys + 1); i++) {
            c_key = blk_file.readInt();
            // check if we found our key and adjust the location
            if (c_key == val) {
                return (c_key);
            }
            // let's check if we need to go to overflow pages
            if ((i % lin_conf.getKeysPerBlock()) == 0) {
                lin_perf.incrementIO();
                //io_cnt++;
                blk_file.seek(offsetCalc(ovf_ptr));
                ovf_ptr = blk_file.readInt();
            }
        }
        // return null
        return (null);
    }

    /**
     * Return the instance of the tracker
     *
     * @return the instance of the tracker
     */
    LinearHashPerfLog getPerfTrackerInstance() {
        return lin_perf;
    }

    /**
     * Beautification for time-stamp generation
     *
     * @return the last modified time of the file.
     */
    private String getLastModifiedTimeStamp() {
        File f = new File(lin_conf.getFilename());
        if (!f.exists()) {
            return (null);
        }

        SimpleDateFormat sdf = new SimpleDateFormat("dd/mm/yyyy HH:mm:ss");
        return (sdf.format(f.lastModified()));

    }

    /**
     * Prints out a quick stats report of the block file
     */
    void printBlockManagerHealth() {
        System.out.println("\nBlock Manager Report for file: " + lin_conf.getFilename() +
                " (last modified on: " + getLastModifiedTimeStamp() + ")\n");
        System.out.println("File statistics:");
        System.out.println("\tCurrent keys: " + key_num + " (total count)");
        System.out.println("\tCurrent blocks: " + poolSize);
        System.out.println("\tCurrent ovf blocks: " + ovf_blocks);
        System.out.println("\tCurrent load factor: " + getBlockLF() + " %\n");

        if (lin_conf.isTrackingEnabled()) {
            lin_perf.printEpochStats();
        }
    }

    /**
     * Initiates tracking of performance.
     */
    void tick() {
        if (lin_conf.isTrackingEnabled()) {
            lin_perf.tick(poolSize, ovf_blocks, calculateRelativeFileSize());
        }
    }

    /**
     * Print the contents of the blocks.
     *
     * @throws IOException is thrown when there is an I/O error during the operation.
     */
    void printContents()
            throws IOException {
        int t_i, t_j, t_blk;
        System.out.println("Blocks in file: " + poolSize);

        t_blk = lin_conf.getKeysPerBlock() + 2; // keys + header
        // normal buckets
        for (t_i = 0; t_i < poolSize; t_i++) {
            blk_file.seek(blockOffset(t_i));
            System.out.println("Bucket (" + t_i + ")");
            for (t_j = 1; t_j <= t_blk; t_j++) {
                System.out.println("\tValue: " + blk_file.readInt());
            }
        }
        // overflow buckets
        for (t_i = 0; t_i < ovf_blocks; t_i++) {
            blk_file.seek(blockOffset(poolSize + t_i));
            System.out.println("Ovf Bucket (" + t_i + ")");
            for (t_j = 1; t_j <= t_blk; t_j++) {
                System.out.println("\tValue: " + blk_file.readInt());
            }
        }
    }

    /**
     * Close and commit the file.
     *
     * @throws IOException is thrown when we cannot close the file.
     */
    void commitFile()
            throws IOException {
        blk_file.close();
    }
}
