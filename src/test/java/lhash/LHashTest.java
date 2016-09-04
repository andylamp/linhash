package lhash;

import org.junit.Test;

import java.util.Random;

public class LHashTest {
    private String o_fname = "lin_hash_file.bin";

    // random number generator
    private Random r = new Random();

    private int key_cnt = 3000;
    private int rnd_range = key_cnt;

    private int keysPerBlock = 32,
            initial_pool = 10,
            epoch_thresh = 100;

    // table for random values
    private int rkey[] = new int[key_cnt];

    /**
     * Test a using an incremental key load.
     *
     * @throws Exception is thrown when an I/O error is detected.
     */
    @Test
    public void testLinearHash_LB1() throws Exception {
        float ilb_1 = 0.8f, dlb_1 = 0.5f;

        LinearHash f_file = new LinearHash(o_fname, keysPerBlock,
                initial_pool, ilb_1, dlb_1, true, epoch_thresh);

        // INSERTS

        for (int i = 0; i < key_cnt; i++) {
            f_file.insertKey(i);
        }

        // FETCHES

        for (int i = 0; i < key_cnt; i++) {
            f_file.fetchKey(i);
        }

        // DELETES

        for (int i = 0; i < key_cnt; i++) {
            f_file.deleteKey(i);
        }

        f_file.commitFile();
    }

    /**
     * Test using different settings for load factors as well as a
     * random key load.
     *
     * REMEMBER *only* unique keys are allowed.
     *
     * @throws Exception is thrown when an I/O error is detected.
     */
    @Test
    public void testLinearHash_LB2() throws Exception {

        float ilb_2 = 0.5f, dlb_2 = 0.5f;

        LinearHash s_file = new LinearHash(("s_" + o_fname), keysPerBlock,
                initial_pool, ilb_2, dlb_2, true, epoch_thresh);

        // INSERTS

        for (int i = 0; i < key_cnt; i++) {
            rkey[i] = rkey[i] = r.nextInt(rnd_range);
            s_file.insertKey(rkey[i]);
        }

        // FETCHES

        for (int i = 0; i < key_cnt; i++) {
            s_file.fetchKey(rkey[i]);
        }

        // DELETES

        for (int i = 0; i < key_cnt; i++) {
            s_file.deleteKey(rkey[i]);
        }

        s_file.commitFile();
    }
}