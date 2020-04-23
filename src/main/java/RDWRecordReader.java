import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.Seekable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.compress.*;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;

public class RDWRecordReader
        extends RecordReader<LongWritable, BytesWrapper> {
    private static final Log LOG
            = LogFactory.getLog(RDWRecordReader.class);

    private int recordLength;
    private long start;
    private long pos;
    private long end;
    private long  numRecordsRemainingInSplit;
    private FSDataInputStream fileIn;
    private Seekable filePosition;
    private LongWritable key;
    private BytesWrapper value;
    private boolean isCompressedInput;
    private Decompressor decompressor;
    private InputStream inputStream;

    ///////
    long cnt;
    private int lineNumber = 0;
    private int rdwAdjust = 4;
    private long bytesRead = 0;
    /**
     * record descriptor word, it consists of
     * 2 bytes length
     * 2 bytes (hex zero)
     */
    private byte[] rdw = new byte[4];
    private byte[] nextRecord = null;
    private ArrayList<byte[]> subLines = new ArrayList<byte[]>(5);
    //////////////////


    @Override
    public void initialize(InputSplit genericSplit, TaskAttemptContext context) throws IOException, InterruptedException {
        FileSplit split = (FileSplit) genericSplit;
        Configuration job = context.getConfiguration();
        final Path file = split.getPath();
        initialize(job, split.getStart(), split.getLength(), file);
        cnt=0;
    }

    public void initialize(Configuration job, long splitStart, long splitLength,
                           Path file) throws IOException {
        start = splitStart;
        end = start + splitLength;

        long partialRecordLength = 0;
        long numBytesToSkip = 0;

        // open the file and seek to the start of the split
        final FileSystem fs = file.getFileSystem(job);
        fileIn = fs.open(file);

        CompressionCodec codec = new CompressionCodecFactory(job).getCodec(file);
        if (null != codec) {
            isCompressedInput = true;
            decompressor = CodecPool.getDecompressor(codec);
            CompressionInputStream cIn
                    = codec.createInputStream(fileIn, decompressor);
            filePosition = cIn;
            inputStream = cIn;
            numRecordsRemainingInSplit = Long.MAX_VALUE;
            LOG.info(
                    "Compressed input; cannot compute number of records in the split");
        } else {
            fileIn.seek(start);
            filePosition = fileIn;
            inputStream = fileIn;
            long splitSize = end - start - numBytesToSkip;
        }
        if (numBytesToSkip != 0) {
            start += inputStream.skip(numBytesToSkip);
        }
        this.pos = start;
    }

    public byte[] readLinePart() throws IOException {
        byte[] ret = null;
        if (readBuffer(inputStream, rdw,0) > 0) {
            int lineLength = ((rdw[0] & 0xFF) << 8) + (rdw[1] & 0xFF) - rdwAdjust;
            if (rdw[2] < 0 || rdw[2] > 3  || rdw[3] != 0) {
                throw new IOException(
                        "Invalid Record Descriptor word at line "
                                + lineNumber + " " + lineLength + "\tRDW=" + rdw[2] + ", " + rdw[3]
                );
            }

            if (lineLength < 0) {
                throw new IOException("Invalid Line Length: " + lineLength + " For line " + lineNumber);
            }
            byte[] inBytes = new byte[lineLength];

            if (readBuffer(inputStream, inBytes,0) >= 0) {
                ret = inBytes;
            }
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

        incBytesRead(total - inTotal);

        return total;
    }
    protected final void incBytesRead(long amount) {
        bytesRead += amount;
    }
    protected final byte[] join(ArrayList<byte[]> lineParts) {
        int len = 0;

        for (byte[] b : lineParts) {
            len += b.length;
        }

        byte[] ret = new byte[len];
        int pos = 0;

        for (byte[] b : lineParts) {
            System.arraycopy(b, 0, ret, pos, b.length);
            pos += b.length;
        }
        return ret;
    }

    public byte[] read()  throws IOException {
        byte[] ret = nextRecord;
        if (ret != null) {
            nextRecord = null;
            return ret;
        }
        if (inputStream == null) {
            throw new IOException("Error in RDW Record Reader");
        }
        lineNumber += 1;
        ret = readLinePart();
        if (rdw[2] != 0 && ret != null) {
            byte[] t = ret;
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

    @Override
    public boolean nextKeyValue() throws IOException {

        if (key == null) {
            key = new LongWritable();
        }
        if (value == null) {
            value = new BytesWrapper();
        }
        key.set(cnt++);

        byte[] b = read();
        if(b == null || (bytesRead > (end - start))) {
            return false;
        }
        value.setAllocated(b);
        return true;
    }

    @Override
    public LongWritable getCurrentKey() {
        return key;
    }

    @Override
    public BytesWrapper getCurrentValue() {
        return value;
    }

    @Override
    public synchronized float getProgress() throws IOException {
        if (start == end) {
            return 0.0f;
        } else {
            return Math.min(1.0f, (getFilePosition() - start) / (float)(end - start));
        }
    }

    @Override
    public synchronized void close() throws IOException {
        try {
            if (inputStream != null) {
                inputStream.close();
                inputStream = null;
            }
        } finally {
            if (decompressor != null) {
                CodecPool.returnDecompressor(decompressor);
                decompressor = null;
            }
        }
    }

    // This is called from the old FixedLengthRecordReader API implementation.
    public long getPos() {
        return pos;
    }

    private long getFilePosition() throws IOException {
        long retVal;
        if (isCompressedInput && null != filePosition) {
            retVal = filePosition.getPos();
        } else {
            retVal = pos;
        }
        return retVal;
    }
}
