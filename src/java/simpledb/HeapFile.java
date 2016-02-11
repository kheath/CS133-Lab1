package simpledb;

import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 * 
 * @see simpledb.HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {

	
	private File f;
	private TupleDesc td;
	private RandomAccessFile fIO;
    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File f, TupleDesc td) {
        this.f = f;
        this.td = td;
        try {
			fIO = new RandomAccessFile(f, "rw");
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        return f;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     * 
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        return f.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        return td;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
    	int offset = pid.pageNumber();
    	try {
			fIO.seek(offset*BufferPool.getPageSize());
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    	byte[] ourArray = new byte[BufferPool.getPageSize()];
    	try {
			fIO.readFully(ourArray);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    	Page retVal;
		try {
			retVal = new HeapPage((HeapPageId) pid, ourArray);
			return retVal;
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return null;
		}
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for lab1
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        return (int) (this.f.length()/BufferPool.getPageSize());
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
    	
    	class HeapFileIterator implements DbFileIterator {
    		
    		private HeapPageId currentId;
    		private TransactionId tid;
    		private HeapPage currentPage;
    		private int currentPageNum;
    		private Iterator<Tuple> currentIterator;
    		private boolean isClosed;
    		
    		public HeapFileIterator(TransactionId tid) {
    			this.tid = tid;
    		}
    		
    		public void open() {
    			this.isClosed = false;
    			this.currentId = new HeapPageId(getId(), 0);
    			this.currentPageNum = 0;
    			try {
					this.currentPage = (HeapPage) Database.getBufferPool().getPage(tid, currentId, Permissions.READ_ONLY);
				} catch (NoSuchElementException | TransactionAbortedException
						| DbException | IOException e) {
					e.printStackTrace();
				}
    			this.currentIterator = this.currentPage.iterator();
    		}
    		
    		public boolean hasNext() {
    			if (this.isClosed) {
    				throw new NoSuchElementException();
    			} else {
    				return (this.currentIterator.hasNext() || this.currentPageNum < numPages()-1);	
    			}
    		}

			@Override
			public Tuple next() throws DbException,
					TransactionAbortedException, NoSuchElementException {
				if (this.isClosed) {
					throw new NoSuchElementException();
				} else if (this.currentIterator.hasNext()) {
					return this.currentIterator.next();
				} else {
	    			this.currentPageNum++;
	    			this.currentId = new HeapPageId(getId(), this.currentPageNum);
	    			try {
						this.currentPage = (HeapPage) Database.getBufferPool().getPage(tid, currentId, Permissions.READ_ONLY);
					} catch (IOException e) {
						e.printStackTrace();
					}
	    			this.currentIterator = this.currentPage.iterator();
	    			
	    			return this.currentIterator.next();
				}
			}

			@Override
			public void rewind() throws DbException,TransactionAbortedException {
				if(!this.isClosed){
					open();
				}
			}

			@Override
			public void close() {
				this.isClosed = true;
			}
    	}

    	return new HeapFileIterator(tid);
    }
    

}

