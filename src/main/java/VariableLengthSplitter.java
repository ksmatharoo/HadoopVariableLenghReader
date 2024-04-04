import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;

public class VariableLengthSplitter {

    private static final Log LOG = LogFactory.getLog(VariableLengthSplitter.class);

    class Offsets {
        long previousStart;
        long fileSize;
        long numOfSplits;
        long splitSizeInByes;
        long numOfBytesReadSplit;
        Split currentSplit;
        ArrayList<Index> list;
        ArrayList<Split> splits;


        public Offsets(long fileSize, long numOfSplits) {
            this.splits = new ArrayList<Split>();
            this.list = new ArrayList<Index>();
            this.fileSize = fileSize;
            this.numOfSplits = numOfSplits;

            this.previousStart = 0;
            this.splitSizeInByes = fileSize / numOfSplits;
            this.numOfBytesReadSplit = 0;

            this.currentSplit = new Split(previousStart);
            this.splits.add(this.currentSplit);
        }

        public Split[] getSplits() {
            return splits.toArray(new Split[0]);
        }

        public Split getLastSplit() {
            return splits.get(splits.size() - 1);
        }

        public void addToList(long length) {
            if (SparkUtils.isDebugMode()) {
                list.add(new Index(previousStart, length));
            }
            previousStart += length;
            numOfBytesReadSplit += length;
            if (numOfBytesReadSplit > splitSizeInByes) {
                currentSplit.setEndOffset(previousStart);
                currentSplit = new Split(previousStart);
                splits.add(currentSplit);
                numOfBytesReadSplit = 0;
            }
        }

        class Split {
            long startOffset;
            long endOffset;

            public void setEndOffset(long endOffset) {
                this.endOffset = endOffset;
            }

            public Split(long startOffset) {
                this.startOffset = startOffset;
                this.endOffset = 0;
            }
        }

        class Index {
            long start;
            long end;

            public Index(long start, long end) {
                this.start = start;
                this.end = end;
            }
        }
    }


    private InputStream inputStream;
    private Offsets offsets;
    int numSplits;
    int lineNumber = 0;
    int rdwAdjust = 0;
    int totalRdwCount = 0;
    byte[] rdw = new byte[4];
    int nextRecord = 0;
    ArrayList<Integer> subLines = new ArrayList<>(5);
    long bytesRead = 0L;
    long fSize;

    public VariableLengthSplitter(JobConf job, Path inputFile, int numOfSplits, long fSize) throws IOException {
        this.numSplits = numOfSplits;
        FileSystem fs = inputFile.getFileSystem(job);
        this.inputStream = fs.open(inputFile);
        this.fSize = fSize;
        this.offsets = new Offsets(fSize, numOfSplits);
    }

    public Offsets.Split[] getSplits() throws IOException {
        if (this.numSplits == 1) {
            //do nothing
            LOG.info("split processing skipped because of no of split :" + numSplits);
        } else {
            while (true) {
                resetRdwCount();
                int length = read();
                if (length == 0)
                    break;
                offsets.addToList((long) length + getRdwByteCount());
            }
        }
        Offsets.Split lastSplit = offsets.getLastSplit();
        lastSplit.setEndOffset(fSize);
        return offsets.getSplits();
    }


    public int readLinePart() throws IOException {
        int ret = 0;
        if (readBuffer(inputStream, rdw, 0) > 0) {
            this.totalRdwCount++;
            int lineLength = ((rdw[0] & 0xFF) << 8) + (rdw[1] & 0xFF) - rdwAdjust;
            if (rdw[2] < 0 || rdw[2] > 3 || rdw[3] != 0) {
                throw new IOException(
                        "Invalid Record Descriptor word at line "
                                + lineNumber + " " + lineLength + "\tRDW="
                                + rdw[2] + rdw[3]);
            }
            if (lineLength < 0) {
                throw new IOException("Invalid Line Length: " + lineLength + " For line " + lineNumber);
            }
            return cursoryRead(inputStream,lineLength,0);
        }
        return ret;
    }

    protected final int readBuffer(InputStream in, final byte[] buf, int inTotal)
            throws IOException {
        int total = inTotal;
        int num = in.read(buf, total, buf.length - total);
        while (num >= 0 && total + num < buf.length) {
            total += num;
            num = in.read(buf, total, buf.length - total);
        }
        if (num > 0) {
            total += num;
        }
        incBytesRead((long) total - inTotal);
        return total;
    }

    protected final int cursoryRead(InputStream in, int length, int inTotal)
            throws IOException {
        int total = inTotal;
        long num = in.skip(length);
        while (num >= 0 && total + num < length) {
            total += num;
            num = in.skip((long) length - total);
        }
        if (num > 0) {
            total += num;
        }
        incBytesRead((long) total - inTotal);
        return total;
    }

    protected final void incBytesRead(long amount) {
        bytesRead += amount;
    }

    protected final int join(ArrayList<Integer> lineParts){
        int len = 0;
        for(Integer i : lineParts){
            len+=i;
        }
        return len;
    }

    public int read()  throws IOException {
        int ret = nextRecord;
        if (ret != 0) {
            nextRecord = 0;
            return ret;
        }
        if (inputStream == null) {
            throw new IOException("Error in RDW Record Reader");
        }
        lineNumber += 1;
        ret = readLinePart();
        if (rdw[2] != 0 && ret != 0) {
            int t = ret;
            subLines.clear();
            do {
                subLines.add(t);
                t = readLinePart();
                if (rdw[2] == 0) {
                    nextRecord = t;
                    return join(subLines);
                }

            } while (rdw[2] != 2);
            subLines.add(t);
            ret = join(subLines);
        }
        return ret;
    }
    public int getRdwByteCount() {
        return this.totalRdwCount*4;
    }

    public int resetRdwCount(){
        return this.totalRdwCount = 0;
    }
}
