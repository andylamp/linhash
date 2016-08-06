import lhash.LinearHash;

import java.io.IOException;

public class MainStub {
    public static void main(String[] args) {
        // create the hashing table
        try {
            // first linear hash table
            int key_cnt = 3000;

            float ilb_2 = 0.5f,
                    dlb_2 = 0.5f;

            int keysPerBlock = 32,
                    initial_pool = 10,
                    epoch_thresh = 100;

            String o_fname = "fblock.dat",
                    s_fname = "stats.dat";


            // allocate the tables
            int key[] = new int[key_cnt];

            // DIFFERENT LB Factors

            LinearHash slh = new LinearHash(("s_" + o_fname), keysPerBlock,
                    initial_pool, ilb_2, dlb_2, true, epoch_thresh);

            // INSERTS

            for (int i = 0; i < key_cnt; i++) {
                key[i] = i;
                slh.insertKey(key[i]);
            }

            // FETCHES

            for (int i = 0; i < key_cnt; i++) {
                slh.fetchKey(key[i]);
            }

            // print an intermittent report before deletes
            slh.printQuickStatReport();

            // DELETES

            for (int i = 0; i < key_cnt; i++) {
                slh.deleteKey(key[i]);
            }

            // commit the file
            slh.commitFile();
            // print report after emptying it.
            slh.printQuickStatReport();
            // dump to file
            slh.dumpEpochDataToFile("s_" + s_fname);

        } catch (IOException e) {
            e.printStackTrace();
        }

    }

}
