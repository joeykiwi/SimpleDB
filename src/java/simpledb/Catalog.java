package simpledb;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.Serializable;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * The Catalog keeps track of all available tables in the database and their
 * associated schemas.
 * For now, this is a stub catalog that must be populated with tables by a
 * user program before it can be used -- eventually, this should be converted
 * to a catalog that reads a catalog table from disk.
 * 
 * @Threadsafe
 */
public class Catalog {
	
	private ArrayList<Integer> m_tableIDs;
	private ArrayList<Table> m_tables;

 
    private static class Table {
        public DbFile dbfile;
        public String name;
        public String pkey;
        
        public Table(DbFile dbf, String nm, String pk) {
        	dbfile = dbf;
        	name = nm;
        	pkey = pk;
        }
    }	
	
    public Catalog() {
        // some code goes here
    	m_tableIDs = new ArrayList<Integer>();
    	m_tables = new ArrayList<Table>();
    }

    /**
     * Add a new table to the catalog.
     * This table's contents are stored in the specified DbFile.
     * @param file the contents of the table to add;  file.getId() is the identfier of
     *    this file/tupledesc param for the calls getTupleDesc and getFile
     * @param name the name of the table -- may be an empty string.  May not be null.  If a name
     * @param pkeyField the name of the primary key field
     * conflict exists, use the last table to be added as the table for a given name.
     */
    public void addTable(DbFile file, String name, String pkeyField) {
        // some code goes here
    	//m_tableIDs.add(file.getId());
    	
    	System.out.println("add table name:"+name);
    	
    	Table newTable = new Table(file, name, pkeyField);
    	
    	for(int i=0; i<m_tables.size(); i++) { //search for the given table name
    		Table currTable = m_tables.get(i);
    		if(currTable.name.equals(name)) { //found it!
    			currTable.dbfile = file;
    			currTable.pkey = pkeyField;
    			m_tableIDs.set(i,  new Integer(file.getId())); //replace the corresponding entry in the table ID list
    			return;
    		}
    		else if(m_tableIDs.get(i).equals(new Integer(file.getId()))) { //same id found
    			currTable.dbfile = file;
    			currTable.pkey = pkeyField;
    			currTable.name = name;
    			return;
    		}
    	}
    	
    	//if it doesn't exist, add to the end
    	m_tableIDs.add(new Integer(file.getId()));
    	m_tables.add(newTable);
    }

    public void addTable(DbFile file, String name) {
    	//System.out.println("table name: "+name);
        addTable(file, name, "");
    }

    /**
     * Add a new table to the catalog.
     * This table has tuples formatted using the specified TupleDesc and its
     * contents are stored in the specified DbFile.
     * @param file the contents of the table to add;  file.getId() is the identfier of
     *    this file/tupledesc param for the calls getTupleDesc and getFile
     */
    public void addTable(DbFile file) {
    	//System.out.println("table name: random");
        addTable(file, (UUID.randomUUID()).toString());
    }
    
    

    /**
     * Return the id of the table with a specified name,
     * @throws NoSuchElementException if the table doesn't exist
     */
    public int getTableId(String name) throws NoSuchElementException {
        // some code goes here
    	for(int i=0; i<m_tables.size(); i++) { //iterate through the array list of tables
    		Table currTable = m_tables.get(i);
    		if(currTable.name.equals(name)) { //found it!
    			return currTable.dbfile.getId();
    		}
    	}
        throw new NoSuchElementException();
    }

    /**
     * Returns the tuple descriptor (schema) of the specified table
     * @param tableid The id of the table, as specified by the DbFile.getId()
     *     function passed to addTable
     * @throws NoSuchElementException if the table doesn't exist
     */
    public TupleDesc getTupleDesc(int tableid) throws NoSuchElementException {
        // some code goes here
    	for(int i=0; i<m_tables.size(); i++) { //search using the tableID list
    		//if(m_tableIDs.get(i).intValue()==tableid) {
    		//	return m_tables.get(i).dbfile.getTupleDesc(); //access ith table in the table list
    		//}
    		if(m_tables.get(i).dbfile.getId()==tableid) {
    			return m_tables.get(i).dbfile.getTupleDesc();
    		}
    	}
    	throw new NoSuchElementException();
    }

    /**
     * Returns the DbFile that can be used to read the contents of the
     * specified table.
     * @param tableid The id of the table, as specified by the DbFile.getId()
     *     function passed to addTable
     */
    public DbFile getDatabaseFile(int tableid) throws NoSuchElementException {
    	for(int i=0; i<m_tables.size(); i++) { //search using the tableID list
    		//if(m_tableIDs.get(i).intValue()==tableid) {
    		//	return m_tables.get(i).dbfile; //access ith table in the table list
    		//}
    		
    		if(m_tables.get(i).dbfile.getId()==tableid) {
    			return m_tables.get(i).dbfile;
    		}
    	}
    	throw new NoSuchElementException();
    }

    public String getPrimaryKey(int tableid) {
        // some code goes here
    	for(int i=0; i<m_tableIDs.size(); i++) { //search using the tableID list
    		if(m_tableIDs.get(i).intValue()==tableid) {
    			return m_tables.get(i).pkey;
    		}
    	}
    	
    	//if no table with the tableid exists
    	return null;
    }

    public Iterator<Integer> tableIdIterator() {
        // some code goes here
    	return m_tableIDs.iterator();
    }

    public String getTableName(int id) {
        // some code goes here
    	for(int i=0; i<m_tables.size(); i++) { //search using the tableID list
    		if(m_tables.get(i).dbfile.getId()==id) {
    			return m_tables.get(i).name;
    		}
    	}
    	return null;
    }
    
    /** Delete all tables from the catalog */
    public void clear() {
        // some code goes here
    	m_tables = new ArrayList<Table>();
    	m_tableIDs = new ArrayList<Integer>();
    }
    
    /**
     * Reads the schema from a file and creates the appropriate tables in the database.
     * @param catalogFile
     */
    public void loadSchema(String catalogFile) {
        String line = "";
        String baseFolder=new File(new File(catalogFile).getAbsolutePath()).getParent();
        try {
            BufferedReader br = new BufferedReader(new FileReader(new File(catalogFile)));
            
            while ((line = br.readLine()) != null) {
                //assume line is of the format name (field type, field type, ...)
                String name = line.substring(0, line.indexOf("(")).trim();
                //System.out.println("TABLE NAME: " + name);
                String fields = line.substring(line.indexOf("(") + 1, line.indexOf(")")).trim();
                String[] els = fields.split(",");
                ArrayList<String> names = new ArrayList<String>();
                ArrayList<Type> types = new ArrayList<Type>();
                String primaryKey = "";
                for (String e : els) {
                    String[] els2 = e.trim().split(" ");
                    names.add(els2[0].trim());
                    if (els2[1].trim().toLowerCase().equals("int"))
                        types.add(Type.INT_TYPE);
                    else if (els2[1].trim().toLowerCase().equals("string"))
                        types.add(Type.STRING_TYPE);
                    else {
                        System.out.println("Unknown type " + els2[1]);
                        System.exit(0);
                    }
                    if (els2.length == 3) {
                        if (els2[2].trim().equals("pk"))
                            primaryKey = els2[0].trim();
                        else {
                            System.out.println("Unknown annotation " + els2[2]);
                            System.exit(0);
                        }
                    }
                }
                Type[] typeAr = types.toArray(new Type[0]);
                String[] namesAr = names.toArray(new String[0]);
                TupleDesc t = new TupleDesc(typeAr, namesAr);
                HeapFile tabHf = new HeapFile(new File(baseFolder+"/"+name + ".dat"), t);
                addTable(tabHf,name,primaryKey);
                System.out.println("Added table : " + name + " with schema " + t);
            }
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(0);
        } catch (IndexOutOfBoundsException e) {
            System.out.println ("Invalid catalog entry : " + line);
            System.exit(0);
        }
    }
}

