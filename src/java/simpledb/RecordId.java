package simpledb;

import java.io.Serializable;

/**
 * A RecordId is a reference to a specific tuple on a specific page of a
 * specific table.
 */
public class RecordId implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * Creates a new RecordId referring to the specified PageId and tuple
     * number.
     * 
     * @param pid
     *            the pageid of the page on which the tuple resides
     * @param tupleno
     *            the tuple number within the page.
     */
    
    private PageId pageId;
    private int tupleNum;
    
    public RecordId(PageId pid, int tupleno) {
        this.pageId = pid;
        this.tupleNum = tupleno;
    }

    /**
     * @return the tuple number this RecordId references.
     */
    public int tupleno() {
        return this.tupleNum;
    }

    /**
     * @return the page id this RecordId references.
     */
    public PageId getPageId() {
        return this.pageId;
    }

    /**
     * Two RecordId objects are considered equal if they represent the same
     * tuple.
     * 
     * @return True if this and o represent the same tuple
     */
    @Override
    public boolean equals(Object o) {
        RecordId oRecord = (RecordId) o;
        return (this.pageId.equals(oRecord.pageId) && this.tupleNum == oRecord.tupleno());
        
    }

    /**
     * You should implement the hashCode() so that two equal RecordId instances
     * (with respect to equals()) have the same hashCode().
     * 
     * @return An int that is the same for equal RecordId objects.
     */
    @Override
    public int hashCode() {
    	
    	int lengthOfTupleNm = (int) Math.floor(Math.log10(this.tupleNum) + 1);
        return (int) (this.pageId.hashCode() * Math.pow(10, lengthOfTupleNm) + this.tupleNum);
    }

}
