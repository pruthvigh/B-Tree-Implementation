/*
 *Pruthvi and Vamshi
 */

package btree;

import java.io.*;

import diskmgr.*;
import bufmgr.*;
import global.*;
import heap.*;
import btree.*;

/**
 * btfile.java This is the main definition of class BTreeFile, which derives
 * from abstract base class IndexFile. It provides an insert/delete interface.
 */
public class BTreeFile extends IndexFile implements GlobalConst {

	private final static int MAGIC0 = 1989;

	private final static String lineSep = System.getProperty("line.separator");

	private static FileOutputStream fos;
	private static DataOutputStream trace;

	/**
	 * It causes a structured trace to be written to a file. This output is used to
	 * drive a visualization tool that shows the inner workings of the b-tree during
	 * its operations.
	 *
	 * @param filename
	 *            input parameter. The trace file name
	 * @exception IOException
	 *                error from the lower layer
	 */
	public static void traceFilename(String filename) throws IOException {

		fos = new FileOutputStream(filename);
		trace = new DataOutputStream(fos);
	}

	/**
	 * Stop tracing. And close trace file.
	 *
	 * @exception IOException
	 *                error from the lower layer
	 */
	public static void destroyTrace() throws IOException {
		if (trace != null)
			trace.close();
		if (fos != null)
			fos.close();
		fos = null;
		trace = null;
	}

	private BTreeHeaderPage headerPage;
	private PageId headerPageId;
	private String dbname;

	/**
	 * Access method to data member.
	 * 
	 * @return Return a BTreeHeaderPage object that is the header page of this btree
	 *         file.
	 */
	public BTreeHeaderPage getHeaderPage() {
		return headerPage;
	}

	private PageId get_file_entry(String filename) throws GetFileEntryException {
		try {
			return SystemDefs.JavabaseDB.get_file_entry(filename);
		} catch (Exception e) {
			e.printStackTrace();
			throw new GetFileEntryException(e, "");
		}
	}

	private Page pinPage(PageId pageno) throws PinPageException {
		try {
			Page page = new Page();
			SystemDefs.JavabaseBM.pinPage(pageno, page, false/* Rdisk */);
			return page;
		} catch (Exception e) {
			e.printStackTrace();
			throw new PinPageException(e, "");
		}
	}

	private void add_file_entry(String fileName, PageId pageno) throws AddFileEntryException {
		try {
			SystemDefs.JavabaseDB.add_file_entry(fileName, pageno);
		} catch (Exception e) {
			e.printStackTrace();
			throw new AddFileEntryException(e, "");
		}
	}

	private void unpinPage(PageId pageno) throws UnpinPageException {
		try {
			SystemDefs.JavabaseBM.unpinPage(pageno, false /* = not DIRTY */);
		} catch (Exception e) {
			e.printStackTrace();
			throw new UnpinPageException(e, "");
		}
	}

	private void freePage(PageId pageno) throws FreePageException {
		try {
			SystemDefs.JavabaseBM.freePage(pageno);
		} catch (Exception e) {
			e.printStackTrace();
			throw new FreePageException(e, "");
		}

	}

	private void delete_file_entry(String filename) throws DeleteFileEntryException {
		try {
			SystemDefs.JavabaseDB.delete_file_entry(filename);
		} catch (Exception e) {
			e.printStackTrace();
			throw new DeleteFileEntryException(e, "");
		}
	}

	private void unpinPage(PageId pageno, boolean dirty) throws UnpinPageException {
		try {
			SystemDefs.JavabaseBM.unpinPage(pageno, dirty);
		} catch (Exception e) {
			e.printStackTrace();
			throw new UnpinPageException(e, "");
		}
	}

	/**
	 * BTreeFile class an index file with given filename should already exist; this
	 * opens it.
	 *
	 * @param filename
	 *            the B+ tree file name. Input parameter.
	 * @exception GetFileEntryException
	 *                can not ger the file from DB
	 * @exception PinPageException
	 *                failed when pin a page
	 * @exception ConstructPageException
	 *                BT page constructor failed
	 */
	public BTreeFile(String filename) throws GetFileEntryException, PinPageException, ConstructPageException {

		headerPageId = get_file_entry(filename);

		headerPage = new BTreeHeaderPage(headerPageId);
		dbname = new String(filename);
		/*
		 * 
		 * - headerPageId is the PageId of this BTreeFile's header page; - headerPage,
		 * headerPageId valid and pinned - dbname contains a copy of the name of the
		 * database
		 */
	}

	/**
	 * if index file exists, open it; else create it.
	 *
	 * @param filename
	 *            file name. Input parameter.
	 * @param keytype
	 *            the type of key. Input parameter.
	 * @param keysize
	 *            the maximum size of a key. Input parameter.
	 * @param delete_fashion
	 *            full delete or naive delete. Input parameter. It is either
	 *            DeleteFashion.NAIVE_DELETE or DeleteFashion.FULL_DELETE.
	 * @exception GetFileEntryException
	 *                can not get file
	 * @exception ConstructPageException
	 *                page constructor failed
	 * @exception IOException
	 *                error from lower layer
	 * @exception AddFileEntryException
	 *                can not add file into DB
	 */
	public BTreeFile(String filename, int keytype, int keysize, int delete_fashion)
			throws GetFileEntryException, ConstructPageException, IOException, AddFileEntryException {

		headerPageId = get_file_entry(filename);
		if (headerPageId == null) // file not exist
		{
			headerPage = new BTreeHeaderPage();
			headerPageId = headerPage.getPageId();
			add_file_entry(filename, headerPageId);
			headerPage.set_magic0(MAGIC0);
			headerPage.set_rootId(new PageId(INVALID_PAGE));
			headerPage.set_keyType((short) keytype);
			headerPage.set_maxKeySize(keysize);
			headerPage.set_deleteFashion(delete_fashion);
			headerPage.setType(NodeType.BTHEAD);
		} else {
			headerPage = new BTreeHeaderPage(headerPageId);
		}

		dbname = new String(filename);

	}

	/**
	 * Close the B+ tree file. Unpin header page.
	 *
	 * @exception PageUnpinnedException
	 *                error from the lower layer
	 * @exception InvalidFrameNumberException
	 *                error from the lower layer
	 * @exception HashEntryNotFoundException
	 *                error from the lower layer
	 * @exception ReplacerException
	 *                error from the lower layer
	 */
	public void close()
			throws PageUnpinnedException, InvalidFrameNumberException, HashEntryNotFoundException, ReplacerException {
		if (headerPage != null) {
			SystemDefs.JavabaseBM.unpinPage(headerPageId, true);
			headerPage = null;
		}
	}

	/**
	 * Destroy entire B+ tree file.
	 *
	 * @exception IOException
	 *                error from the lower layer
	 * @exception IteratorException
	 *                iterator error
	 * @exception UnpinPageException
	 *                error when unpin a page
	 * @exception FreePageException
	 *                error when free a page
	 * @exception DeleteFileEntryException
	 *                failed when delete a file from DM
	 * @exception ConstructPageException
	 *                error in BT page constructor
	 * @exception PinPageException
	 *                failed when pin a page
	 */
	public void destroyFile() throws IOException, IteratorException, UnpinPageException, FreePageException,
			DeleteFileEntryException, ConstructPageException, PinPageException {
		if (headerPage != null) {
			PageId pgId = headerPage.get_rootId();
			if (pgId.pid != INVALID_PAGE)
				_destroyFile(pgId);
			unpinPage(headerPageId);
			freePage(headerPageId);
			delete_file_entry(dbname);
			headerPage = null;
		}
	}

	private void _destroyFile(PageId pageno) throws IOException, IteratorException, PinPageException,
			ConstructPageException, UnpinPageException, FreePageException {

		BTSortedPage sortedPage;
		Page page = pinPage(pageno);
		sortedPage = new BTSortedPage(page, headerPage.get_keyType());

		if (sortedPage.getType() == NodeType.INDEX) {
			BTIndexPage indexPage = new BTIndexPage(page, headerPage.get_keyType());
			RID rid = new RID();
			PageId childId;
			KeyDataEntry entry;
			for (entry = indexPage.getFirst(rid); entry != null; entry = indexPage.getNext(rid)) {
				childId = ((IndexData) (entry.data)).getData();
				_destroyFile(childId);
			}
		} else { // BTLeafPage

			unpinPage(pageno);
			freePage(pageno);
		}

	}

	private void updateHeader(PageId newRoot) throws IOException, PinPageException, UnpinPageException {

		BTreeHeaderPage header;
		PageId old_data;

		header = new BTreeHeaderPage(pinPage(headerPageId));

		old_data = headerPage.get_rootId();
		header.set_rootId(newRoot);

		// clock in dirty bit to bm so our dtor needn't have to worry about it
		unpinPage(headerPageId, true /* = DIRTY */);

		// ASSERTIONS:
		// - headerPage, headerPageId valid, pinned and marked as dirty

	}

	/**
	 * insert record with the given key and rid
	 *
	 * @param key
	 *            the key of the record. Input parameter.
	 * @param rid
	 *            the rid of the record. Input parameter.
	 * @exception KeyTooLongException
	 *                key size exceeds the max keysize.
	 * @exception KeyNotMatchException
	 *                key is not integer key nor string key
	 * @exception IOException
	 *                error from the lower layer
	 * @exception LeafInsertRecException
	 *                insert error in leaf page
	 * @exception IndexInsertRecException
	 *                insert error in index page
	 * @exception ConstructPageException
	 *                error in BT page constructor
	 * @exception UnpinPageException
	 *                error when unpin a page
	 * @exception PinPageException
	 *                error when pin a page
	 * @exception NodeNotMatchException
	 *                node not match index page nor leaf page
	 * @exception ConvertException
	 *                error when convert between revord and byte array
	 * @exception DeleteRecException
	 *                error when delete in index page
	 * @exception IndexSearchException
	 *                error when search
	 * @exception IteratorException
	 *                iterator error
	 * @exception LeafDeleteException
	 *                error when delete in leaf page
	 * @exception InsertException
	 *                error when insert in index page
	 */
	public void insert(KeyClass key, RID rid) throws KeyTooLongException, KeyNotMatchException, LeafInsertRecException,
			IndexInsertRecException, ConstructPageException, UnpinPageException, PinPageException,
			NodeNotMatchException, ConvertException, DeleteRecException, IndexSearchException, IteratorException,
			LeafDeleteException, InsertException, IOException

	{
		/*
		 * Inserting key of type integer into the B+ tree If header page is pointing to
		 * invalid page create a new root leaf page and insert 1st element If header
		 * page pointing to a index/leaf page check whether _insert returns a null or
		 * key If null no split occured and key is inserted into the leaf/index page If
		 * key is returned split occured on the corresponding node/leaf and the root
		 * page is updated accordingly Header page is updated on the first creation of
		 * the root page It's a good practice to regularly flush the trace file as it
		 * may lead to unexpected errors.
		 */

		if (trace != null) {
			trace.flush();
		}
		KeyDataEntry rootPoint;
		// If headerpage is pointing to inavlid page then there is no root page
		if (headerPage.get_rootId().pid == INVALID_PAGE) {
			// creating a new leaf page to insert the first key
			BTLeafPage leafPage = new BTLeafPage(headerPage.get_keyType());
			// updating header to point to the new leaf page
			updateHeader(leafPage.getCurPage());
			//Flush the trace file
			if(trace != null)
            {
                trace.flush();
            }
			// setting the root leaf page's next and prev pointers
			leafPage.setNextPage(new PageId(INVALID_PAGE));
			leafPage.setPrevPage(new PageId(INVALID_PAGE));
			// inserting the first key into the leaf page
			leafPage.insertRecord(key, rid);
			// unpinning the page after the record inserted and marking it as dirty so that
			// it is written back to database
			unpinPage(leafPage.getCurPage(), true);
			//Flush the trace file
			if(trace != null)
            {
                trace.flush();
            }
			/*
			 * If the header page pointing to index/page calling _insert method to check if
			 * the record can be inserted or a split to make
			 */
		} else {
			// leafPage = new BTLeafPage(headerPage.get_rootId(), headerPage.get_keyType());
			// leafPage.insertRecord(key,rid);
			// unpinPage(leafPage.getCurPage(), true);

			//Flush the trace file
			if(trace != null)
            {
                trace.flush();
            }
			rootPoint = _insert(key, rid, headerPage.get_rootId());
			// If _insert returns null no split occurred
			if (rootPoint == null) {
				return;
				/*
				 * If keydataentry is returned then split occurred on index/leaf page updating
				 * the indexpage and headerpage acordingly
				 */
			} else {
				BTIndexPage newIndexPage = new BTIndexPage(headerPage.get_keyType());
				// inserting the returned key into the index page
				newIndexPage.insertKey(rootPoint.key, ((IndexData) rootPoint.data).getData());
				// setting index page point to the header page
				newIndexPage.setPrevPage(headerPage.get_rootId());
				// unpinning the index page after record in inserted
				unpinPage(newIndexPage.getCurPage());
				// updating the header page point to the index page
				updateHeader(newIndexPage.getCurPage());
				//Flush the trace file
				if(trace != null)
	            {
	                trace.flush();
	            }
			}
			//Flush the trace file
			if(trace != null)
            {
                trace.flush();
            }
			return;
		}
	}

	/**
	 * recursive method for inserting record and splitting index/leaf pages if
	 * necessary
	 * 
	 * @param key
	 * @param rid
	 * @param currentPageId
	 * @return
	 * @throws PinPageException
	 * @throws IOException
	 * @throws ConstructPageException
	 * @throws LeafDeleteException
	 * @throws ConstructPageException
	 * @throws DeleteRecException
	 * @throws IndexSearchException
	 * @throws UnpinPageException
	 * @throws LeafInsertRecException
	 * @throws ConvertException
	 * @throws IteratorException
	 * @throws IndexInsertRecException
	 * @throws KeyNotMatchException
	 * @throws NodeNotMatchException
	 * @throws InsertException
	 */
	private KeyDataEntry _insert(KeyClass key, RID rid, PageId currentPageId)
			throws PinPageException, IOException, ConstructPageException, LeafDeleteException, ConstructPageException,
			DeleteRecException, IndexSearchException, UnpinPageException, LeafInsertRecException, ConvertException,
			IteratorException, IndexInsertRecException, KeyNotMatchException, NodeNotMatchException, InsertException

	{
		/*
		 * First check the page type whether it is index or leaf If it is index page
		 * associate the passed index page to it and call _insert method accordingly If
		 * it returns null no split occurred then insert the record in the page Else
		 * split the page into two pages and redsitribute the elements accordingly Do
		 * the same for leaf pages too but now instead return duplicate key entry
		 */
		KeyDataEntry indexEntry = null, upEntry = null;
		// pinning the page passed and associating it with a BTSorted page instance
		BTSortedPage workingPage = new BTSortedPage(pinPage(currentPageId), headerPage.get_keyType());
		// checking whether the current page type is an index or leaf.
		// If it is an index page
		if (workingPage.getType() == NodeType.INDEX) {

			// Creating a new BTIndex page to associate it with the passed index page
			BTIndexPage workingIndexpage = new BTIndexPage(pinPage(currentPageId), headerPage.get_keyType());
			// varaible to store the page ID
			PageId insertingIndexPage = workingIndexpage.getPageNoByKey(key);
			// Unpinning the current index page and recursively calling _insert method and
			// pinning it again
			unpinPage(workingIndexpage.getCurPage());
			// Checking for whether there is any split on the index page or not by
			// recursively calling _insert method
			upEntry = _insert(key, rid, insertingIndexPage);

			// If upentry is null, then no split occurred. Return null.
			if (upEntry == null)
				return null;

			workingIndexpage = new BTIndexPage(currentPageId, headerPage.get_keyType());
			/*
			 * Checking whether the current index page have any space if so insert the
			 * record and unpin it as it is dirty.
			 */
			if (workingIndexpage.available_space() >= BT.getKeyDataLength(upEntry.key, NodeType.INDEX)) {
				workingIndexpage.insertKey(upEntry.key, ((IndexData) upEntry.data).getData());
				unpinPage(workingIndexpage.getCurPage(), true);
				return null;
			}
			// If there is not enough space available then split the records between current
			// index page and new index page
			BTIndexPage newIndexPage = new BTIndexPage(headerPage.get_keyType());
			KeyDataEntry temp;
			RID tempRid = new RID();
			int noOfRecords = 0;
			// Copy all the records from the current index page to the new index page
			// And delete entries from the existing index page incrementing the noOfRecords
			for (temp = workingIndexpage.getFirst(tempRid); temp != null; temp = workingIndexpage.getFirst(tempRid)) {
				noOfRecords = noOfRecords + 1;
				newIndexPage.insertKey(temp.key, ((IndexData) temp.data).getData());
				workingIndexpage.deleteSortedRecord(tempRid);
			}

			int count = 0;
			// Dividing by 2 to get the middle entry
			noOfRecords = noOfRecords / 2;
			KeyDataEntry lastKeyDataEntry = null;
			// Iterating through the new index page and transferring half of the records to
			// the
			// existing index page and deleting them on the new index page
			for (temp = newIndexPage.getFirst(tempRid); count < noOfRecords; temp = newIndexPage.getFirst(tempRid)) {
				lastKeyDataEntry = temp;
				if (count < noOfRecords) {
					workingIndexpage.insertKey(temp.key, ((IndexData) temp.data).getData());
					newIndexPage.deleteSortedRecord(tempRid);
					count++;
				} else {
					count++;
				}
			}
			// Undo the last entry and delete it from the existing index page by inserting a
			// empty RID
			if (workingIndexpage.available_space() < newIndexPage.available_space()) {
				newIndexPage.insertKey(lastKeyDataEntry.key, ((IndexData) lastKeyDataEntry.data).getData());
				workingIndexpage
						.deleteSortedRecord(new RID(workingIndexpage.getCurPage(), workingIndexpage.getSlotCnt() - 1));
			}
			// Compare the upentry key and last entry key and insert in the appropriate
			// index page
			temp = newIndexPage.getFirst(tempRid);
			// If the upentry key is bigger then insert the key in the new index page
			if (BT.keyCompare(upEntry.key, temp.key) >= 0) {
				newIndexPage.insertKey(upEntry.key, ((IndexData) upEntry.data).getData());
				// else insert in the existing index page
			} else {
				workingIndexpage.insertKey(upEntry.key, ((IndexData) upEntry.data).getData());
			}
			// Unpinning the current inde page and set dirty to true
			unpinPage(workingIndexpage.getCurPage(), true);
			// Fill up the upEntry with the first key in the new index page
			upEntry = newIndexPage.getFirst(tempRid);
			// Setting the prev page pointer of new link page with the upentry page id
			newIndexPage.setPrevPage(((IndexData) upEntry.data).getData());
			// Delete first record from the new index page
			newIndexPage.deleteSortedRecord(tempRid);

			// Unpin the new index page and set dirty to true
			unpinPage(newIndexPage.getCurPage(), true);
			// Pointing the new index page by setting the higher index page in the hierarchy
			((IndexData) upEntry.data).setData(newIndexPage.getCurPage());

			return upEntry;

		}
		/*
		 * If the current node type is leaf then follow the same procedure for index
		 * page and check for whether you have space to insert in the leaf node, else
		 * split it and pass upentry as the copy of the node isntead of moving up as in
		 * index page case
		 */
		else if (workingPage.getType() == NodeType.LEAF) {

			RID tempRid = new RID();
			// Asssociate current leaf page with the passed leaf page ID
			BTLeafPage workingLeafPage = new BTLeafPage(pinPage(currentPageId), headerPage.get_keyType());
			// Check if the current leaf page has space for new entries. If so enter the
			// data on the current leaf
			// page and unpin it and mark dirty as true
			if (workingLeafPage.available_space() >= BT.getKeyDataLength(key, NodeType.LEAF)) {

				
				workingLeafPage.insertRecord(key, rid);
				unpinPage(workingLeafPage.getCurPage(), true);
				//Flush the trace file
				if(trace != null)
	            {
	                trace.flush();
	            }
				return null;
			}
			/*
			 * If no space is available to insert data then create a new leaf page and set
			 * it's prev and next page pointers accordingly. Also set the current leaf
			 * page's next page pointer.
			 */
			else {

				BTLeafPage newLeafPage = new BTLeafPage(headerPage.get_keyType());
				newLeafPage.setNextPage(workingLeafPage.getNextPage());
				newLeafPage.setPrevPage(workingLeafPage.getCurPage());
				workingLeafPage.setNextPage(newLeafPage.getCurPage());

				// Cheking if there is a next leaf page to the new leaf page, if so setting the
				// prev and next page pointers for the new and leaf page next to it
				if (newLeafPage.getNextPage().pid != INVALID_PAGE) {
					BTLeafPage nextLeafPage = new BTLeafPage(newLeafPage.getNextPage(), headerPage.get_keyType());
					nextLeafPage.setPrevPage(newLeafPage.getCurPage());
					unpinPage(nextLeafPage.getCurPage(), true);
				}
				//Flush the trace file
				if(trace != null)
	            {
	                trace.flush();
	            }
				// variable for counting the no of records in the existing leaf page.
				int noOfRecords = 0;
				// Copy all the records from the existing leaf page to the new leaf page and
				// delete everything from the existing leaf page
				for (KeyDataEntry temp = workingLeafPage.getFirst(tempRid); temp != null; temp = workingLeafPage
						.getFirst(tempRid)) {
					noOfRecords = noOfRecords + 1;
					newLeafPage.insertRecord(temp.key, ((LeafData) (temp.data)).getData());
					workingLeafPage.deleteSortedRecord(tempRid);
				}

				// Getting split for copying half of the records from the new leaf page to the
				// existing leaf page and updating temp keydataentry for undoing the last entry
				int count = 0;
				noOfRecords = noOfRecords / 2;
				KeyDataEntry lastKeyDataEntry = null;
				for (KeyDataEntry temp = newLeafPage.getFirst(tempRid); count < noOfRecords; temp = newLeafPage
						.getFirst(tempRid)) {
					lastKeyDataEntry = temp;
					if (count < noOfRecords) {
						workingLeafPage.insertRecord(temp.key, ((LeafData) (temp.data)).getData());
						newLeafPage.deleteSortedRecord(tempRid);
						count++;
					} else {
						count++;
					}
				}
				//Flush the trace file
				if(trace != null)
	            {
	                trace.flush();
	            }
               /*
                * Comparing the key value to be inserted with the last entry key value
                * If the value is positive then key will be inserted in the new leaf page
                * Else it will be inserted into the existing leaf page.
                * Unpin the current leaf page after insertion and mark dirty as true.
                */
				if (BT.keyCompare(key, lastKeyDataEntry.key) >= 0) {
					newLeafPage.insertRecord(key, rid);
				} else {
					if (workingLeafPage.available_space() < newLeafPage.available_space()) {
						newLeafPage.insertRecord(lastKeyDataEntry.key, ((LeafData) (lastKeyDataEntry.data)).getData());
						workingLeafPage.deleteSortedRecord(
								new RID(workingLeafPage.getCurPage(), workingLeafPage.getSlotCnt() - 1));
					}
					workingLeafPage.insertRecord(key, rid);
					unpinPage(workingLeafPage.getCurPage());
				}

				//Get record data to be returned which will copied to the index page
				lastKeyDataEntry = newLeafPage.getFirst(tempRid);
				//Initialise index entry with the values of key and page id which will be copied to the index page
				indexEntry = new KeyDataEntry(lastKeyDataEntry.key, newLeafPage.getCurPage());
				//Unpin newleaf page
				unpinPage(newLeafPage.getCurPage());
				//Flush the trace file
				if(trace != null)
	            {
	                trace.flush();
	            }
				return indexEntry;
			}

		}
		
		return null;

	}

	/**
	 * delete leaf entry given its <key, rid> pair. `rid' is IN the data entry; it
	 * is not the id of the data entry)
	 *
	 * @param key
	 *            the key in pair <key, rid>. Input Parameter.
	 * @param rid
	 *            the rid in pair <key, rid>. Input Parameter.
	 * @return true if deleted. false if no such record.
	 * @exception DeleteFashionException
	 *                neither full delete nor naive delete
	 * @exception LeafRedistributeException
	 *                redistribution error in leaf pages
	 * @exception RedistributeException
	 *                redistribution error in index pages
	 * @exception InsertRecException
	 *                error when insert in index page
	 * @exception KeyNotMatchException
	 *                key is neither integer key nor string key
	 * @exception UnpinPageException
	 *                error when unpin a page
	 * @exception IndexInsertRecException
	 *                error when insert in index page
	 * @exception FreePageException
	 *                error in BT page constructor
	 * @exception RecordNotFoundException
	 *                error delete a record in a BT page
	 * @exception PinPageException
	 *                error when pin a page
	 * @exception IndexFullDeleteException
	 *                fill delete error
	 * @exception LeafDeleteException
	 *                delete error in leaf page
	 * @exception IteratorException
	 *                iterator error
	 * @exception ConstructPageException
	 *                error in BT page constructor
	 * @exception DeleteRecException
	 *                error when delete in index page
	 * @exception IndexSearchException
	 *                error in search in index pages
	 * @exception IOException
	 *                error from the lower layer
	 *
	 */
	public boolean Delete(KeyClass key, RID rid)
			throws DeleteFashionException, LeafRedistributeException, RedistributeException, InsertRecException,
			KeyNotMatchException, UnpinPageException, IndexInsertRecException, FreePageException,
			RecordNotFoundException, PinPageException, IndexFullDeleteException, LeafDeleteException, IteratorException,
			ConstructPageException, DeleteRecException, IndexSearchException, IOException {
		if (headerPage.get_deleteFashion() == DeleteFashion.NAIVE_DELETE)
			return NaiveDelete(key, rid);
		else
			throw new DeleteFashionException(null, "");
	}

	/*
	 * findRunStart. Status BTreeFile::findRunStart (const void lo_key, RID
	 * *pstartrid)
	 * 
	 * find left-most occurrence of `lo_key', going all the way left if lo_key is
	 * null.
	 * 
	 * Starting record returned in *pstartrid, on page *pppage, which is pinned.
	 * 
	 * Since we allow duplicates, this must "go left" as described in the text (for
	 * the search algorithm).
	 * 
	 * @param lo_key find left-most occurrence of `lo_key', going all the way left
	 * if lo_key is null.
	 * 
	 * @param startrid it will reurn the first rid =< lo_key
	 * 
	 * @return return a BTLeafPage instance which is pinned. null if no key was
	 * found.
	 */

	BTLeafPage findRunStart(KeyClass lo_key, RID startrid) throws IOException, IteratorException, KeyNotMatchException,
			ConstructPageException, PinPageException, UnpinPageException {
		BTLeafPage pageLeaf;
		BTIndexPage pageIndex;
		Page page;
		BTSortedPage sortPage;
		PageId pageno;
		PageId curpageno = null; // iterator
		PageId prevpageno;
		PageId nextpageno;
		RID curRid;
		KeyDataEntry curEntry;

		pageno = headerPage.get_rootId();

		if (pageno.pid == INVALID_PAGE) { // no pages in the BTREE
			pageLeaf = null; // should be handled by
			// startrid =INVALID_PAGEID ; // the caller
			return pageLeaf;
		}

		page = pinPage(pageno);
		sortPage = new BTSortedPage(page, headerPage.get_keyType());

		if (trace != null) {
			trace.writeBytes("VISIT node " + pageno + lineSep);
			trace.flush();
		}

		// ASSERTION
		// - pageno and sortPage is the root of the btree
		// - pageno and sortPage valid and pinned

		while (sortPage.getType() == NodeType.INDEX) {
			pageIndex = new BTIndexPage(page, headerPage.get_keyType());
			prevpageno = pageIndex.getPrevPage();
			curEntry = pageIndex.getFirst(startrid);
			while (curEntry != null && lo_key != null && BT.keyCompare(curEntry.key, lo_key) < 0) {

				prevpageno = ((IndexData) curEntry.data).getData();
				curEntry = pageIndex.getNext(startrid);
			}

			unpinPage(pageno);

			pageno = prevpageno;
			page = pinPage(pageno);
			sortPage = new BTSortedPage(page, headerPage.get_keyType());

			if (trace != null) {
				trace.writeBytes("VISIT node " + pageno + lineSep);
				trace.flush();
			}

		}

		pageLeaf = new BTLeafPage(page, headerPage.get_keyType());

		curEntry = pageLeaf.getFirst(startrid);
		while (curEntry == null) {
			// skip empty leaf pages off to left
			nextpageno = pageLeaf.getNextPage();
			unpinPage(pageno);
			if (nextpageno.pid == INVALID_PAGE) {
				// oops, no more records, so set this scan to indicate this.
				return null;
			}

			pageno = nextpageno;
			pageLeaf = new BTLeafPage(pinPage(pageno), headerPage.get_keyType());
			curEntry = pageLeaf.getFirst(startrid);
		}

		// ASSERTIONS:
		// - curkey, curRid: contain the first record on the
		// current leaf page (curkey its key, cur
		// - pageLeaf, pageno valid and pinned

		if (lo_key == null) {
			return pageLeaf;
			// note that pageno/pageLeaf is still pinned;
			// scan will unpin it when done
		}

		while (BT.keyCompare(curEntry.key, lo_key) < 0) {
			curEntry = pageLeaf.getNext(startrid);
			while (curEntry == null) { // have to go right
				nextpageno = pageLeaf.getNextPage();
				unpinPage(pageno);

				if (nextpageno.pid == INVALID_PAGE) {
					return null;
				}

				pageno = nextpageno;
				pageLeaf = new BTLeafPage(pinPage(pageno), headerPage.get_keyType());

				curEntry = pageLeaf.getFirst(startrid);
			}
		}

		return pageLeaf;
	}

	/*
	 * Status BTreeFile::NaiveDelete (const void *key, const RID rid)
	 * 
	 * Remove specified data entry (<key, rid>) from an index.
	 * 
	 * We don't do merging or redistribution, but do allow duplicates.
	 * 
	 * Page containing first occurrence of key `key' is found for us by
	 * findRunStart. We then iterate for (just a few) pages, if necesary, to find
	 * the one containing <key,rid>, which we then delete via
	 * BTLeafPage::delUserRid.
	 */

	private boolean NaiveDelete(KeyClass key, RID rid)
			throws LeafDeleteException, KeyNotMatchException, PinPageException, ConstructPageException, IOException,
			UnpinPageException, PinPageException, IndexSearchException, IteratorException {
		
		/**
		 * Search for the key value in pages and delete the entry if key found. No need
		 * to redistribute because this is a naive delete
		 */

		BTLeafPage currentLeafPage;
		RID curRid = new RID();
		KeyDataEntry entry;
		PageId nextpageId;

		// Finding the leaf page where the key is present.
		currentLeafPage = findRunStart(key, curRid);

		// if method returns null then key is not present in the leaf
		if (currentLeafPage == null)
			return false;

		// If it is not null, then key is present search for the record and delete the
		// record
		// corresponding to the key
		entry = currentLeafPage.getCurrent(curRid);

		// Iterating through the leaf page for the record associated with the key
		while (true) {
			// going right to the current page
			while (entry == null) {
				// getting page Id of next page
				nextpageId = currentLeafPage.getNextPage();
				// unpin the current leafpage
				unpinPage(currentLeafPage.getCurPage());
				// if there is no next page return false
				if (nextpageId.pid == INVALID_PAGE) {
					return false;
				}
				// make leafpage point to next leafpage
				currentLeafPage = new BTLeafPage(pinPage(nextpageId), headerPage.get_keyType());

				entry = currentLeafPage.getFirst(curRid);

			}

			if (BT.keyCompare(key, entry.key) > 0)
				break;

			// delEntry methods deletes record and returns TRUE if record found
			boolean keyFound = currentLeafPage.delEntry(new KeyDataEntry(key, rid));
			if (keyFound == true) {
				// key and record found and deleted
				unpinPage(currentLeafPage.getCurPage(), true);

				if (trace != null) {
					trace.flush();
				}

				return true;
			}
			// If key not found go over to the next/right leaf
			nextpageId = currentLeafPage.getNextPage();
			// unpinning the current page as search is done for the key
			unpinPage(currentLeafPage.getCurPage());
			// Making currentleaf to the next page ID
			currentLeafPage = new BTLeafPage(pinPage(nextpageId), headerPage.get_keyType());

			entry = currentLeafPage.getFirst(curRid);

		}

		// unpinning the page after searching for the key is done
		unpinPage(currentLeafPage.getCurPage());

		return false;
	}

	/**
	 * create a scan with given keys Cases: (1) lo_key = null, hi_key = null scan
	 * the whole index (2) lo_key = null, hi_key!= null range scan from min to the
	 * hi_key (3) lo_key!= null, hi_key = null range scan from the lo_key to max (4)
	 * lo_key!= null, hi_key!= null, lo_key = hi_key exact match ( might not unique)
	 * (5) lo_key!= null, hi_key!= null, lo_key < hi_key range scan from lo_key to
	 * hi_key
	 *
	 * @param lo_key
	 *            the key where we begin scanning. Input parameter.
	 * @param hi_key
	 *            the key where we stop scanning. Input parameter.
	 * @exception IOException
	 *                error from the lower layer
	 * @exception KeyNotMatchException
	 *                key is not integer key nor string key
	 * @exception IteratorException
	 *                iterator error
	 * @exception ConstructPageException
	 *                error in BT page constructor
	 * @exception PinPageException
	 *                error when pin a page
	 * @exception UnpinPageException
	 *                error when unpin a page
	 */
	public BTFileScan new_scan(KeyClass lo_key, KeyClass hi_key) throws IOException, KeyNotMatchException,
			IteratorException, ConstructPageException, PinPageException, UnpinPageException

	{
		BTFileScan scan = new BTFileScan();
		if (headerPage.get_rootId().pid == INVALID_PAGE) {
			scan.leafPage = null;
			return scan;
		}

		scan.treeFilename = dbname;
		scan.endkey = hi_key;
		scan.didfirst = false;
		scan.deletedcurrent = false;
		scan.curRid = new RID();
		scan.keyType = headerPage.get_keyType();
		scan.maxKeysize = headerPage.get_maxKeySize();
		scan.bfile = this;

		// this sets up scan at the starting position, ready for iteration
		scan.leafPage = findRunStart(lo_key, scan.curRid);
		return scan;
	}

	void trace_children(PageId id)
			throws IOException, IteratorException, ConstructPageException, PinPageException, UnpinPageException {

		if (trace != null) {

			BTSortedPage sortedPage;
			RID metaRid = new RID();
			PageId childPageId;
			KeyClass key;
			KeyDataEntry entry;
			sortedPage = new BTSortedPage(pinPage(id), headerPage.get_keyType());

			// Now print all the child nodes of the page.
			if (sortedPage.getType() == NodeType.INDEX) {
				BTIndexPage indexPage = new BTIndexPage(sortedPage, headerPage.get_keyType());
				trace.writeBytes("INDEX CHILDREN " + id + " nodes" + lineSep);
				trace.writeBytes(" " + indexPage.getPrevPage());
				for (entry = indexPage.getFirst(metaRid); entry != null; entry = indexPage.getNext(metaRid)) {
					trace.writeBytes("   " + ((IndexData) entry.data).getData());
				}
			} else if (sortedPage.getType() == NodeType.LEAF) {
				BTLeafPage leafPage = new BTLeafPage(sortedPage, headerPage.get_keyType());
				trace.writeBytes("LEAF CHILDREN " + id + " nodes" + lineSep);
				for (entry = leafPage.getFirst(metaRid); entry != null; entry = leafPage.getNext(metaRid)) {
					trace.writeBytes("   " + entry.key + " " + entry.data);
				}
			}
			unpinPage(id);
			trace.writeBytes(lineSep);
			trace.flush();
		}

	}

}
