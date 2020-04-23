import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.RecordReader;

import java.io.IOException;

public class VariableLengthRecordReader implements RecordReader<LongWritable, BytesWrapper> {

    private RDWRecordReader reader;

    public VariableLengthRecordReader(Configuration job, FileSplit split) throws IOException {
        this.reader = new RDWRecordReader();
        reader.initialize(job,split.getStart(),split.getLength(),split.getPath());
    }

    @Override
    public synchronized boolean next(LongWritable key, BytesWrapper value)
            throws IOException {

        boolean dataRead = reader.nextKeyValue();
        if(dataRead){
           LongWritable newKey = reader.getCurrentKey();
           BytesWrapper newValue = reader.getCurrentValue();
           key.set(newKey.get());
           value.setAllocated(newValue);
        }
        return dataRead;
    }

    @Override
    public LongWritable createKey() {
        return new LongWritable();
    }

    @Override
    public BytesWrapper createValue() {
        return new BytesWrapper();
    }

    @Override
    public long getPos() throws IOException {
        return reader.getPos();
    }

    @Override
    public void close() throws IOException {
        reader.close();
    }

    @Override
    public float getProgress() throws IOException {
        return reader.getProgress();
    }
}
