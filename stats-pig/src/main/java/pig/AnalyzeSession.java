package pig;

import java.io.IOException;

import org.apache.pig.Accumulator;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.logicalLayer.schema.Schema.FieldSchema;

/**
 * Sessionize input stream and out the number of elements each session and session time.
 */
public class AnalyzeSession extends EvalFunc<DataBag> implements Accumulator<DataBag>{

	public static final long MAX_QUERY_INTERVAL = 60 * 1000;

	private DataBag outputBag;
	private long sessionTime;
	private long recordCount;

    @Override
    public DataBag exec(Tuple input) throws IOException {
    	accumulate(input);
    	DataBag outputBag = getValue();
    	cleanup();
    	return outputBag;
    }
    
    @Override
    public void accumulate(Tuple input) throws IOException {
		if (input == null || input.size() != 1) return;
		long startTime = -1;
		long endTime = -1;
	    for (Tuple tuple : (DataBag)input.get(0)) {
	    	long currentTime = Long.parseLong(tuple.get(0).toString());
	    	
    		if(startTime == -1) {
	    		startTime = currentTime;
	    	} else if(endTime + MAX_QUERY_INTERVAL < currentTime) {
	    		this.sessionTime = endTime - startTime;
			    addOutputBag();
		        this.recordCount = 0;
	    		startTime = currentTime;
	    	} else if(currentTime < endTime) {
	            throw new IOException(String.format("input time series is not sorted (%s < %s)", currentTime, endTime));
	    	}
    		this.recordCount++;
	        endTime = currentTime;
	    }
		this.sessionTime = endTime - startTime;
	    addOutputBag();
    }

    private void addOutputBag() {
        Tuple tuple = TupleFactory.getInstance().newTuple();
        tuple.append(this.recordCount);
        tuple.append(this.sessionTime);
        outputBag.add(tuple);
    }
    
	@Override
	public DataBag getValue() {
	    return outputBag;
	}
	
	@Override
	public void cleanup() {
	    this.sessionTime = -1;
	    this.recordCount = 0;
	    this.outputBag = BagFactory.getInstance().newDefaultBag();
	}
	
    @Override
	public Schema outputSchema(Schema input) {
		try {
			Schema.FieldSchema inputFieldSchema = input.getField(0);
			if (inputFieldSchema.type != DataType.BAG) {
				throw new RuntimeException("Expected a BAG as input");
			}
			Schema outputTupleSchema = new Schema();
			outputTupleSchema.add(new FieldSchema("record_count", DataType.LONG));
			outputTupleSchema.add(new FieldSchema("session_time", DataType.LONG));
			
			return new Schema(new FieldSchema(getSchemaName(this.getClass().getName().toLowerCase(), input),
					outputTupleSchema, DataType.BAG));
		} catch (FrontendException e) {
			throw new RuntimeException(e);
		}
	}
}
