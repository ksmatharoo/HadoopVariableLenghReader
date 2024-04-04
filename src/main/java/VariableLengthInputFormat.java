import com.google.common.base.Stopwatch;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapred.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class VariableLengthInputFormat extends FileInputFormat<LongWritable, BytesWrapper> implements
        JobConfigurable {

    @Override
    public RecordReader<LongWritable, BytesWrapper> getRecordReader(InputSplit inputSplit, JobConf jobConf, Reporter reporter)
            throws IOException {
        return new VariableLengthRecordReader(jobConf, (FileSplit) inputSplit);
    }

    @Override
    public void configure(JobConf jobConf) {
    }

    @Override
    public InputSplit[] getSplits(JobConf job, int numSplits) throws IOException {
        Stopwatch sw = new Stopwatch().start();
        FileStatus[] files = listStatus(job);

        // Save the number of input files for metrics/loadgen
        job.setLong(NUM_INPUT_FILES, files.length);
        ArrayList<Path> inputFilePath = new ArrayList<>();
        long totalSize = 0;                           // compute total size
        for (FileStatus file : files) {                // check we have valid files
            if (file.isDirectory()) {
                throw new IOException("Not a file: " + file.getPath());
            }
            totalSize += file.getLen();
            inputFilePath.add(file.getPath());
        }
        String fileName = inputFilePath.get(0).toString();
        ArrayList<FileSplit> splits = new ArrayList<>();

        Path path = new Path(fileName);
        String host[] = {"localhost"};
        //FileSplit fileSplit = makeSplit(p,0,5722,host,host);

        //test.bin
        /* FileSplit fileSplit = makeSplit(p,0,5722,host,host);
        FileSplit fileSplit1 = makeSplit(p,5722,(10664 - 5722),host,host);*/

        //test100Rec.bin
        //FileSplit fileSplit = makeSplit(path,0,4782,host,host);
        //FileSplit fileSplit1 = makeSplit(path,4782,(10502-4782),host,host);

        //test10Rec.bin
        /*FileSplit fileSplit = makeSplit(p,0,514,host,host);
        FileSplit fileSplit1 = makeSplit(p,514,(1002 - 514),host,host);*/

        //splits.add(fileSplit);
        //splits.add(fileSplit1);
        // todo need to fix this
        VariableLengthSplitter variableLengthSplitter = new VariableLengthSplitter(job, inputFilePath.get(0),
                numSplits, totalSize);
        VariableLengthSplitter.Offsets.Split[] splits1 = variableLengthSplitter.getSplits();

        for (VariableLengthSplitter.Offsets.Split s : splits1) {
            splits.add(makeSplit(path, s.startOffset, (s.endOffset - s.startOffset), host, host));
        }
        return splits.toArray(new FileSplit[0]);
    }
}
