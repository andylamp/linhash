package lhash;

import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.util.Vector;

@SuppressWarnings("unused")
class LinearHashPerfLog {
    private int itr_cnt;            // tick iterations (resets)
    private int tick_cnt;        // tick count (iterations)
    private int io_cnt;            // global i/o operation counter
    private int epoch_io_cnt;    // epoc i/o counter
    private long global_ticks;    // global ticks count

    private Vector<Integer> epochIO = new Vector<Integer>();            // I/O operations for each epoch
    private Vector<Double> epochAvgIO = new Vector<Double>();    // I/O operations for each epoch (average)
    private Vector<Integer> epochAccIO = new Vector<Integer>();        // I/O operations for each epoch (added)
    private Vector<Integer> epochBlocks = new Vector<Integer>();        // Blocks per Epoch
    private Vector<Double> epochFS = new Vector<Double>();            // Relative epoch file size

    private LinearHashConfiguration lin_conf;

    LinearHashPerfLog(LinearHashConfiguration lin_conf) {
        this.lin_conf = lin_conf;
        initBlockManagerCounters();
    }

    /**
     * Return the epoch count
     *
     * @return itr_cnt
     */
    int getEpochCount() {
        return (itr_cnt);
    }

    /**
     * Print the epoch stats if tracking is enabled
     */
    void printEpochStats() {
        System.out.println("\nEpoch statistics:");
        System.out.println("\tCurrent epoch: " + itr_cnt);
        System.out.println("\tEpoch ticks: " + tick_cnt);
        System.out.println("\tGlobal ticks: " + global_ticks);
        System.out.println("\tEpoch IO: " + epoch_io_cnt);
        System.out.println("\tGlobal IO: " + io_cnt);
    }

    /**
     * Print statistics
     */
    void printAllEpochStatistics() {
        System.out.println("\nBlock manager printing stored statistics for: " + itr_cnt + " epochs");
        for (int i = 0; i < epochFS.size(); i++) {
            // print data
            System.out.println("Epoch (" + (i + 1) + "): " +
                    "\n\tEpoch I/O: " + epochIO.get(i) +
                    "\n\tEpoch Acc. I/O: " + epochAccIO.get(i) +
                    "\n\tEpoch Avg. I/O: " + epochAvgIO.get(i) +
                    "\n\tEpoch Blocks (+ovf): " + epochBlocks.get(i) +
                    "\n\tEpoch Rel. filesize: " + epochFS.get(i));
        }
    }

    /**
     * Print all epoch statistics to a file
     *
     * @param fname filename where we put the metrics.
     * @throws FileNotFoundException        is thrown when the file is not located.
     * @throws UnsupportedEncodingException is thrown when the file has an unsupported encoding.
     */
    void printAllEpochStatisticsToFile(String fname)
            throws FileNotFoundException, UnsupportedEncodingException {
        PrintWriter pf = new PrintWriter(fname, "UTF-8");
        // write to the file.
        for (int i = 0; i < epochFS.size(); i++) {
            pf.write(i + " " + epochIO.get(i) + " " +
                    epochAccIO.get(i) + " " + epochAvgIO.get(i) + " " +
                    epochBlocks.get(i) + " " + epochFS.get(i) + "\n");
        }
        // close the file
        pf.close();
    }


    /**
     * Tick (for epoch statistics)
     */
    void tick(int poolSize, int ovf_blocks, double relFileSize) {
        // update counters
        global_ticks++;
        tick_cnt++;
        // check if we reached an epoch end.
        if (tick_cnt == lin_conf.getTickThresh()) {
            endEpoch(poolSize, ovf_blocks, relFileSize);
        }
    }

    /**
     * Initialize the block manager counters
     * .
     */
    private void initBlockManagerCounters() {
        itr_cnt = 0;
        io_cnt = 0;
        epoch_io_cnt = 0;
        tick_cnt = 0;
        global_ticks = 0;
    }

    /**
     * Increment single I/O
     */
    void incrementIO() {
        io_cnt++;
    }

    /**
     * Increment both I/O counters (reg. and epoch)
     */
    void incrementBothIO() {
        incrementIO();
        epoch_io_cnt++;
    }

    /**
     * Reset the counter epoch, update epoch vectors as well.
     */
    private void endEpoch(int poolSize, int ovf_blocks, double relFileSize) {
        epochFS.add(relFileSize);
        epochIO.add(epoch_io_cnt);
        epochBlocks.add(poolSize + ovf_blocks);
        epochAccIO.add(io_cnt);
        itr_cnt++;
        epochAvgIO.add((double) (epoch_io_cnt) / (double) (lin_conf.getTickThresh()));
        epoch_io_cnt = 0;
        tick_cnt = 0;
    }
}
