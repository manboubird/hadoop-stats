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
 * Calculate variance and median based on precomputed average.
 */
public class CalcStats extends EvalFunc<DataBag> implements Accumulator<DataBag>{

	public static final long MAX_QUERY_INTERVAL = 60 * 1000;

	private DataBag outputBag;

	private long count;
	private double average;
	private long median;
	private double variance;
	private double min;
	private double max;

    @Override
    public DataBag exec(Tuple input) throws IOException {
    	accumulate(input);
    	DataBag outputBag = getValue();
    	cleanup();
    	return outputBag;
    }
    
    @Override
    public void accumulate(Tuple input) throws IOException {
		if (input == null || input.size() != 2) {
			throw new IllegalStateException("Illegal input");
		}
	
		DataBag statsDataBag = (DataBag)input.get(0);
		if(statsDataBag == null) {
			throw new IllegalStateException("Illegal statsDataBag");
		}

		Tuple statTuple = statsDataBag.iterator().next();
		if (statTuple == null || statTuple.size() != 5) {
			throw new IllegalStateException("Illegal statTuple");
		}
		
		this.count = Long.parseLong(statTuple.get(1).toString());
		this.average = Double.parseDouble(statTuple.get(2).toString());
		this.min = Long.parseLong(statTuple.get(3).toString());
		this.max = Long.parseLong(statTuple.get(4).toString());

		boolean isCountOdd = this.count % 2 == 1;
		long meanTargetCount = isCountOdd 
									? (this.count + 1) / 2 
									: this.count / 2;

		double varianceAmount = 0.0D;
		int actualCount = 0;
	    for (Tuple tuple : (DataBag)input.get(1)) {
			actualCount++;
			long queryCount = Long.parseLong(tuple.get(0).toString());
			varianceAmount += Math.pow(((double)queryCount - this.average), 2);
			
			if(actualCount == meanTargetCount) {
				// get Xth value as median.
				this.median = queryCount;
			}else if(isCountOdd == false && actualCount == meanTargetCount + 1) {
				// when n is odd, add (X+1)th value to X th and divide by 2.
				this.median = (this.median + queryCount) / 2;
			}
		}
		if(this.count != actualCount) {
			throw new IllegalStateException("The count is not equal: count=" + this.count + ", actualCount=" + actualCount);
		}
		this.variance = varianceAmount / this.count;
	    addOutputBag();
    }

    private void addOutputBag() {
        Tuple tuple = TupleFactory.getInstance().newTuple();
        tuple.append(this.count);
        tuple.append(this.average);
        tuple.append(this.median);
        tuple.append(this.variance);
        tuple.append(this.min);
        tuple.append(this.max);
        outputBag.add(tuple);
    }
    
	@Override
	public DataBag getValue() {
	    return outputBag;
	}
	
	@Override
	public void cleanup() {
	    this.count = 0;
	    this.average = 0D;
	    this.median = 0;
	    this.variance = 0D;
	    this.min = Long.MAX_VALUE;
	    this.max = Long.MIN_VALUE;
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
			outputTupleSchema.add(new FieldSchema("count", DataType.LONG));
			outputTupleSchema.add(new FieldSchema("average", DataType.DOUBLE));
			outputTupleSchema.add(new FieldSchema("median", DataType.LONG));
			outputTupleSchema.add(new FieldSchema("variance", DataType.DOUBLE));
			outputTupleSchema.add(new FieldSchema("min", DataType.LONG));
			outputTupleSchema.add(new FieldSchema("max", DataType.LONG));

			return new Schema(new FieldSchema(getSchemaName(this.getClass().getName().toLowerCase(), input),
					outputTupleSchema, DataType.BAG));
		} catch (FrontendException e) {
			throw new RuntimeException(e);
		}
	}
}
