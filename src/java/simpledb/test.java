package simpledb;
import java.io.*;

public class test {

    public static void main(String[] argv) {

        // construct a 3-column table schema
        Type types[] = new Type[]{ Type.INT_TYPE, Type.INT_TYPE, Type.INT_TYPE, Type.INT_TYPE };
        String names[] = new String[]{ "field0", "field1", "field2", "field3" };
        TupleDesc descriptor = new TupleDesc(types, names);
        
        // create the table, associate it with some_data_file.dat
        // and tell the catalog about the schema of this table.
        HeapFile table1 = new HeapFile(new File("some_data_file.dat"), descriptor);        
        Database.getCatalog().addTable(table1, "test");
        
        System.out.println("tuple size: "+descriptor.getSize());
        
        HeapPageId hip = new HeapPageId(table1.getId(),0);
        System.out.println("total pgs: "+table1.numPages());        
        
        HeapPage hpg = (HeapPage) table1.readPage(hip);
        System.out.println("hdr size: "+hpg.getHeaderSize());
        System.out.println("slot numbs: "+hpg.getNumSlots());
        for(int i=0; i<hpg.getNumSlots(); i++) {
        	if(hpg.isSlotUsed(i)) {
		System.out.println(i+"th slot is used");
        	}
        }
        
        HeapPageIterator hitr = (HeapPageIterator) hpg.iterator();
        System.out.println("heap page iterator...");
        while(hitr.hasNext()) {
        	Tuple tup2=hitr.next();
        	System.out.println(tup2);
        }
        
        // construct the query: we use a simple SeqScan, which spoonfeeds
        // tuples via its iterator.
        TransactionId tid = new TransactionId();
        SeqScan f = new SeqScan(tid, table1.getId());
        
        System.out.println("");
        System.out.println("seq scan...");
        try {
            // and run it
            f.open();
            while (f.hasNext()) {
                Tuple tup = f.next();
                System.out.println(tup);
            }
            f.close();
            Database.getBufferPool().transactionComplete(tid);
        } catch (Exception e) {
            System.out.println ("Exception : " + e);
        }
    }

}