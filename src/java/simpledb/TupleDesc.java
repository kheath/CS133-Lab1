// Authors: Kevin Heath & Melissa Galonsky

package simpledb;

import java.io.Serializable;
import java.util.*;


/**
 * TupleDesc describes the schema of a tuple.
 */
public class TupleDesc implements Serializable {
	
	int STRING_LENGTH = 10;


    /**
     * A help class to facilitate organizing the information of each field
     * */
    public static class TDItem implements Serializable {

        private static final long serialVersionUID = 1L;

        /**
         * The type of the field
         * */
        public final Type fieldType;
        
        /**
         * The name of the field
         * */
        public final String fieldName;

        public TDItem(Type t, String n) {
            this.fieldName = n;
            this.fieldType = t;
        }

        public String toString() {
            return fieldName + "(" + fieldType + ")";
        }
    }
    
    /**
     * @return
     *        An iterator which iterates over all the field TDItems
     *        that are included in this TupleDesc
     * */
    public Iterator<TDItem> iterator() {   	
    	
    	Iterator<TDItem> iter = new Iterator<TDItem>() {
    		
    		private int currentIndex = 0;
    		
    		public boolean hasNext() {
    			return this.currentIndex < fields.length;
    		}
    		
    		public TDItem next() {
    			
    			return fields[currentIndex++];
    		}
    		
       	};
    	
        return iter;
    }

    private static final long serialVersionUID = 1L;

    
    /*
     * Array of TDItems
     * */
    
    private TDItem[] fields;
    
    /**
     * Create a new TupleDesc with typeAr.length fields with fields of the
     * specified types, with associated named fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     * @param fieldAr
     *            array specifying the names of the fields. Note that names may
     *            be null.
     */
    public TupleDesc(Type[] typeAr, String[] fieldAr) {
      	// For each type in typeAR and each string in fieldAr
    	fields = new TDItem[typeAr.length];
    	
    	for (int i = 0; i < typeAr.length; i++) {
    		fields[i] = new TDItem(typeAr[i], fieldAr[i]);
    	}
    	
    }

    /**
     * Constructor. Create a new tuple desc with typeAr.length fields with
     * fields of the specified types, with anonymous (unnamed) fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     */
    
    
    public TupleDesc(Type[] typeAr) {
    	// Creates an array of TDItems of the appropriate length and fills it
    	fields = new TDItem[typeAr.length];
    	
    	for (int i = 0; i < typeAr.length; i++) {
    		fields[i] = new TDItem(typeAr[i], null);
    	}
    }

    /**
     * @return the number of fields in this TupleDesc
     */
    public int numFields() {
    	
        return fields.length;
    }

    /**
     * Gets the (possibly null) field name of the ith field of this TupleDesc.
     * 
     * @param i
     *            index of the field name to return. It must be a valid index.
     * @return the name of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public String getFieldName(int i) throws NoSuchElementException {
    	if(i > numFields()-1) {
    		throw new NoSuchElementException();
    	}
        return fields[i].fieldName;
    }

    /**
     * Gets the type of the ith field of this TupleDesc.
     * 
     * @param i
     *            The index of the field to get the type of. It must be a valid
     *            index.
     * @return the type of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public Type getFieldType(int i) throws NoSuchElementException {
        return fields[i].fieldType;
    }

    /**
     * Find the index of the field with a given name.
     * 
     * @param name
     *            name of the field.
     * @return the index of the field that is first to have the given name.
     * @throws NoSuchElementException
     *             if no field with a matching name is found.
     */
    public int fieldNameToIndex(String name) throws NoSuchElementException {
        if (name == null) {
        	throw new NoSuchElementException();
        }
    	Iterator<TDItem> iter = this.iterator();
    	int i = 0;
    	while(iter.hasNext()){
    		TDItem temp = iter.next();
    		System.out.println(temp);
    		if(temp.fieldName.equals(name)){
    			return i;
    		}
    		i++;
    	}
    	throw new NoSuchElementException();
    	
    }

    /**
     * @return The size (in bytes) of tuples corresponding to this TupleDesc.
     *         Note that tuples from a given TupleDesc are of a fixed size.
     */
    public int getSize() {
        int size = 0;
        Iterator<TDItem> iter = this.iterator();
        while (iter.hasNext()) {
        	size += iter.next().fieldType.getLen();
        }
        return size;
    }

    /**
     * Merge two TupleDescs into one, with td1.numFields + td2.numFields fields,
     * with the first td1.numFields coming from td1 and the remaining from td2.
     * 
     * @param td1
     *            The TupleDesc with the first fields of the new TupleDesc
     * @param td2
     *            The TupleDesc with the last fields of the TupleDesc
     * @return the new TupleDesc
     */
    public static TupleDesc merge(TupleDesc td1, TupleDesc td2) {
        
    	int l = td1.numFields()+td2.numFields();
    	Type[] typeAr = new Type[l];
    	String[] stringAr = new String[l];
    	int count = 0;
    	Iterator<TDItem> iter1 = td1.iterator();
    	Iterator<TDItem> iter2 = td2.iterator();
    	
    	while(iter1.hasNext()){
    		TDItem tempItem = iter1.next();
    		typeAr[count] = tempItem.fieldType;
    		stringAr[count] = tempItem.fieldName;
    		count++;
    	}
    	while(iter2.hasNext()){
    		TDItem tempItem = iter2.next();
    		typeAr[count] = tempItem.fieldType;
    		stringAr[count] = tempItem.fieldName;
    		count++;
    	}
    	
        return new TupleDesc(typeAr, stringAr);
    }

    /**
     * Compares the specified object with this TupleDesc for equality. Two
     * TupleDescs are considered equal if they are the same size and if the n-th
     * type in this TupleDesc is equal to the n-th type in td.
     * 
     * @param o
     *            the Object to be compared for equality with this TupleDesc.
     * @return true if the object is equal to this TupleDesc.
     */
    public boolean equals(Object o) {
    	if (!(o instanceof TupleDesc) || o == null) {
    		return false;
    	}
        if (this.numFields() != ((TupleDesc)o).numFields() || this.getSize() != ((TupleDesc)o).getSize()) {
        	return false;
        }
        Iterator<TDItem> iter1 = this.iterator();
        Iterator<TDItem> iter2 = ((TupleDesc)o).iterator();
        while(iter1.hasNext()) {
        	if (!iter1.next().fieldType.equals(iter2.next().fieldType)) {
        		return false;
        	}
        }
        return true;
    }

    public int hashCode() {
        // If you want to use TupleDesc as keys for HashMap, implement this so
        // that equal objects have equals hashCode() results
        throw new UnsupportedOperationException("unimplemented");
    }

    /**
     * Returns a String describing this descriptor. It should be of the form
     * "fieldName[0](fieldType[0]), ..., fieldName[M](fieldType[M])", although
     * the exact format does not matter.
     * 
     * @return String describing this descriptor.
     */
    public String toString() {
    	StringBuilder output = new StringBuilder();
        Iterator<TDItem> iter = this.iterator();
        while(iter.hasNext()){
        	output.append(iter.next().toString());
        }
        return output.toString();
    }
}
