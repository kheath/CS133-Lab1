package simpledb;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Iterator;

import simpledb.TupleDesc.TDItem;

/**
 * Tuple maintains information about the contents of a tuple. Tuples have a
 * specified schema specified by a TupleDesc object and contain Field objects
 * with the data for each field.
 */
public class Tuple implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * Create a new tuple with the specified schema (type).
     * 
     * @param td
     *            the schema of this tuple. It must be a valid TupleDesc
     *            instance with at least one field.
     */
    
    private Field[] fields;
    private TupleDesc tupleD;
    private RecordId record;
    
    
    public Tuple(TupleDesc td) {
        this.fields = new Field[td.numFields()];
        this.tupleD = td;
    }

    /**
     * @return The TupleDesc representing the schema of this tuple.
     */
    public TupleDesc getTupleDesc() {
        return this.tupleD;
    }

    /**
     * @return The RecordId representing the location of this tuple on disk. May
     *         be null.
     */
    public RecordId getRecordId() {
        return record;
    }

    /**
     * Set the RecordId information for this tuple.
     * 
     * @param rid
     *            the new RecordId for this tuple.
     */
    public void setRecordId(RecordId rid) {
        this.record = rid;
    }

    /**
     * Change the value of the ith field of this tuple.
     * 
     * @param i
     *            index of the field to change. It must be a valid index.
     * @param f
     *            new value for the field.
     */
    public void setField(int i, Field f) {
    	//SHOULD WE ERROR CHECK?
        fields[i] = f;
    }

    /**
     * @return the value of the ith field, or null if it has not been set.
     * 
     * @param i
     *            field index to return. Must be a valid index.
     */
    public Field getField(int i) {
        return fields[i];
    }

    /**
     * Returns the contents of this Tuple as a string. Note that to pass the
     * system tests, the format needs to be as follows:
     * 
     * column1\tcolumn2\tcolumn3\t...\tcolumnN\n
     * 
     * where \t is any whitespace, except newline, and \n is a newline
     */
    public String toString() {
        StringBuilder builder = new StringBuilder();
        Iterator<Field> iter = fields();
        while (iter.hasNext()) {
        	builder.append(iter.next());
        }
        return builder.toString();
        
    }
    
    /**
     * @return
     *        An iterator which iterates over all the fields of this tuple
     * */
    public Iterator<Field> fields()
    {
    	Iterator<Field> iter = new Iterator<Field>() {

    		private int currentIndex = 0;

    		public boolean hasNext() {
    			return this.currentIndex < fields.length;
    		}

    		public Field next() {

    			return fields[currentIndex++];
    		}

    	};

    	return iter;
    }
    
    /**
     * Reset the TupleDesc of this tuple
     * Does not need to worry about the fields inside the Tuple
     * */
    public void resetTupleDesc(TupleDesc td)
    {
    	this.tupleD = td;
    }
}
