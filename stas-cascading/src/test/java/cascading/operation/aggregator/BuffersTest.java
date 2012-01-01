package cascading.operation.aggregator;

import java.util.Iterator;

import cascading.CascadingTestCase;
import cascading.operation.Buffer;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleListCollector;

public class BuffersTest extends CascadingTestCase {

	public BuffersTest() {
		super("buffers tests");
	}
	
	public void testCountTotalAndUnique() {
		Buffer buffer = new CountTotalAndUnique();
		
		Tuple[] arguments = new Tuple[] {new Tuple(new String("a")), new Tuple(new String("a")),
										 new Tuple(new String("b")), new Tuple(new String("c")),
										 new Tuple(new String("c"))};
		Fields resultFields = CountTotalAndUnique.OUTPUT_FIELDS;

		TupleListCollector resultEntryCollector = invokeBuffer(buffer, arguments, resultFields);

		Tuple tuple = resultEntryCollector.iterator().next();

		assertEquals("got expected value after buffer", 5L, tuple.getLong(0));
		assertEquals("got expected value after buffer", 3L, tuple.getLong(1));
	}

	public void testCountQueryPerSession() {
		Buffer buffer = new CountQueryPerSession();
		
		Tuple[] arguments = new Tuple[] {new Tuple(1999999L), 
										 new Tuple(2000000L),
										 new Tuple(2060001L), 
										 new Tuple(2061000L),
										 new Tuple(2062000L), 
										 new Tuple(3000000L)};
		Fields resultFields = CountQueryPerSession.OUTPUT_FIELDS;

		Iterator<Tuple> it = invokeBuffer(buffer, arguments, resultFields).iterator();

		Tuple tuple = it.next();
		assertEquals("got expected value after buffer", 2L, 	  tuple.getLong(0));
		assertEquals("got expected value after buffer", 1999999L, tuple.getLong(1));
		assertEquals("got expected value after buffer", 2000000L, tuple.getLong(2));
		
		tuple = it.next();
		assertEquals("got expected value after buffer", 3L, 	  tuple.getLong(0));
		assertEquals("got expected value after buffer", 2060001L, tuple.getLong(1));
		assertEquals("got expected value after buffer", 2062000L, tuple.getLong(2));
		
		tuple = it.next();
		assertEquals("got expected value after buffer", 1L, 	  tuple.getLong(0));
		assertEquals("got expected value after buffer", 3000000L, tuple.getLong(1));
		assertEquals("got expected value after buffer", 3000000L, tuple.getLong(2));
	}

	public void testCalcVarianceAndMedian() {
		Buffer buffer = new CalcVarianceAndMedian();
		
		Tuple[] arguments = new Tuple[] {new Tuple(0, null, 2L, 100D, 50L, 150L), 
										 new Tuple(1, 50L, null, null, null, null),
										 new Tuple(1, 150L, null, null, null, null)};
		Fields resultFields = CalcVarianceAndMedian.OUTPUT_FIELDS;

		TupleListCollector resultEntryCollector = invokeBuffer(buffer, arguments, resultFields);

		Tuple tuple = resultEntryCollector.iterator().next();

		assertEquals("got expected value after buffer", 2L, tuple.getLong(0));
		assertEquals("got expected value after buffer", 100D, tuple.getDouble(1));
		assertEquals("got expected value after buffer", 2500.0D, tuple.getDouble(2));
		assertEquals("got expected value after buffer", 100.0D, tuple.getDouble(3));
		assertEquals("got expected value after buffer", 50L, tuple.getLong(4));
		assertEquals("got expected value after buffer", 150L, tuple.getLong(5));
	}
}
