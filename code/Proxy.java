/*
* Proxy.java
* 	Open-Close Semantics
* 		Open, update local copy and LRU
* 		Close, update server file  
*
* */

import java.io.*;
import java.nio.channels.*;
import java.util.*;
import java.net.MalformedURLException;
import java.rmi.Naming;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.Remote;
import java.rmi.server.UnicastRemoteObject;
import java.nio.file.*;
		

class Proxy {

	// root directory path	
	public static String rootDir;
	// capacity of the cache
	public static int capacity;
	// shared server instance
	public static ServerAbstract server;
	
	private static class FileHandler implements FileHandling {
		// offset of file descriptor
		public static int fd=10000;
		// Map fd to File instance
		public static HashMap<Integer, File> fileMap = new HashMap<Integer, File>();
		// Map fd to RandomAccessFile
		public static HashMap<Integer, RandomAccessFile> ptMap = new HashMap<Integer, RandomAccessFile>();
		// Map fd to OpenOption of a open action
		public static HashMap<Integer, OpenOption> rightMap = new HashMap<Integer, OpenOption>();
		// Map fd to original path
		public static HashMap<Integer, String> pathMap = new HashMap<Integer, String>();
		// Cache Instance
		public static LRUCache cache = new LRUCache(capacity);
		// Map original path to MasterCopy path
		public static HashMap<String, String> masterCopy = new HashMap<String, String>();
		// Map fd to real file path
		public static HashMap<Integer, String> realMap = new HashMap<Integer, String>();

        /* Function Name: fetchCopy
        * 		fetch copy from Server
        * */
        private int fetchCopy(String path, int ver, int size) {
			System.err.println("Fetch copy from Server\t"+path);
			byte[] buf = null;
            try {
				// file doesn't exist
                File temp = new File(masterCopy.get(path));
               	temp.getParentFile().mkdirs();
                temp.createNewFile();
                BufferedOutputStream output = new
                	BufferedOutputStream(new FileOutputStream(masterCopy.get(path)));
				while (size > 0) {	
                	buf = server.downloadFile(path.split(rootDir)[1]);
					size -= buf.length;
                    output.write(buf);
				}
                output.close();
                //update LRU and set its version
                int ret = cache.set(masterCopy.get(path), ver, 0);
                if (ret == -1) {
                	// cache failure
                   	// delete the file
                   	temp.delete();
                    return ret;
                }
               	System.err.println("cache file " + masterCopy.get(path));
                return 1;
            } catch (Exception e) {
                e.printStackTrace();
                System.err.println(e);
                return 1;
            }
        }
		/* Function Name: checkCache
 		*		maintain Master Copy on Proxy
 		* */
		private int checkCache(String path) {
			int[] stat = null;
			try {
				stat = server.getVersion(path.split(rootDir)[1]);
			} catch (Exception err) {
				System.err.println("file doesn't exist");
			}
			if (stat[0] == -1) {
			// it means the file doesn't exist on server
				if (masterCopy.containsKey(path)) {
					//proxy exist old obsolete file
					cache.deleteObsolete(masterCopy.get(path));
					masterCopy.remove(path);
				}
				return 1;
			}
			if (!cache.containsKey(path+"."+String.valueOf(stat[0]))) {
				// latest file doesn't exist in cache
				// update the cache and masterCopy record
				if (masterCopy.containsKey(path)&&cache.containsKey(masterCopy.get(path)))
					cache.deleteObsolete(masterCopy.get(path));
				masterCopy.put(path, path+"."+String.valueOf(stat[0]));
				System.err.println("Put " + "." + path + String.valueOf(stat[0])  + " into HashMap");
				return fetchCopy(path, stat[0], stat[1]);
			} else {
				// latest file exists in cache
				return 1;
			}
		}
		
		/* Function Name: duplicateCopy
		*		Duplicate Private Copy for one client
 		* */
		public int duplicateCopy(int fd, String path, File file) {
			try{	
				File temp = new File(path+"-"+String.valueOf(fd));
				temp.getParentFile().mkdirs();
				FileChannel fcin = new FileInputStream(file).getChannel();
				FileChannel fcout = new FileOutputStream(temp).getChannel();
				fcin.transferTo(0, fcin.size(), fcout);
				fcin.close();
				fcout.close();	
				int ret = cache.set(path+"-"+String.valueOf(fd), -2, 1);
				if (ret == -1) {
					temp.delete();
					return -1;
				}
			} catch(Exception e) {
				e.printStackTrace();
			}
			return 1;
		}
        /* Function Name: checkPathValid
        * 		check path valid or not
        *   	return the simplified path string
        * */
        private String checkPathValid(String path) {
            String ret = FileSystems.getDefault().getPath(rootDir+path).toAbsolutePath().normalize().toString();
            if (ret.startsWith(rootDir)){
				return ret;
			}
            else
                return null;
        }
		/* Function Name: open
 		*		Each time only one client can enter this method
 		* */
		public synchronized int open( String path, OpenOption o ) {
            // check path
            System.err.println("OPEN\t"+path+"\t"+String.valueOf(fd));
            path = checkPathValid(path);
            if (path==null)
                return Errors.EPERM;
            // check cache
			int ret = checkCache(path);
			if (ret==-1)
				return Errors.ENOMEM;
			File file;
			// check the option
            // for read only use the master copy
            // for other option use private copy
			if (o == OpenOption.CREATE) {
				// create if not exists
				if (!masterCopy.containsKey(path)) {
					// file doesn't exist
					file = new File(path);
					try{
						// fake masterCopy which is empty
						cache.set(path, 0, 0);
						masterCopy.put(path, path);
						server.createNewFile(path.split(rootDir)[1]);
						file.createNewFile();
					} catch(IOException err){}
				} else {
					file = new File(masterCopy.get(path));
				}
                // duplicate file to get a private copy
                if (duplicateCopy(fd, path, file) == -1)
					return Errors.ENOMEM;
                File copy = new File(path+"-"+String.valueOf(fd));
				fileMap.put(fd, copy);
				rightMap.put(fd, o);
				pathMap.put(fd, path);
				realMap.put(fd, path+"-"+String.valueOf(fd));
				return fd++;
			} else if (o == OpenOption.CREATE_NEW) {
				System.err.println("Create New");
				// create new file, if exists return error
				if (masterCopy.containsKey(path)) {
					return Errors.EEXIST;
				}
				file = new File(path);
				try{
					cache.set(path, 0, 0);
					masterCopy.put(path, path);
					boolean flg = server.createNewFile(path.split(rootDir)[1]);
					if (!flg)
						return Errors.EEXIST;
					file.createNewFile();
				} catch(IOException err) {}
				// duplicate file to get a private copy
                if (duplicateCopy(fd, path, file) == -1)
					return Errors.ENOMEM;
                File copy = new File(path+"-"+String.valueOf(fd));
				fileMap.put(fd, copy);
				rightMap.put(fd, o);
				pathMap.put(fd, path);
				realMap.put(fd, path+"-"+String.valueOf(fd));
				return fd++;
			} else if (o == OpenOption.READ) {
				// read a file
				if (!masterCopy.containsKey(path))
					return Errors.ENOENT;
				// change master copy into read situation
				file = new File(masterCopy.get(path));
				try {
					cache.set(masterCopy.get(path), cache.get(masterCopy.get(path)), cache.getUsed(masterCopy.get(path))+1);
				} catch (Exception e){}
				fileMap.put(fd, file);
				rightMap.put(fd, o);
				pathMap.put(fd, path);
				realMap.put(fd, masterCopy.get(path));
				return fd++;
			} else {
				// write a file
				if (!masterCopy.containsKey(path))
					return Errors.ENOENT;
				file = new File(masterCopy.get(path));
				if (file.isDirectory())
					return Errors.EISDIR;
                if (duplicateCopy(fd, path, file) == -1)
					return Errors.ENOMEM;
                File copy = new File(path+"-"+String.valueOf(fd));
				// cache the private copy
				fileMap.put(fd, copy);
				rightMap.put(fd, o);
				pathMap.put(fd, path);
				realMap.put(fd, path+"-"+String.valueOf(fd));
				return fd++;
			}
		}
        /* Function Name: updateCopy
        * 		Update the copy on server and proxy from the private copy
        * */
        private int updateCopy( int fd ) {
            try {
                // read the private copy
                String path = pathMap.get(fd);
				System.err.println("UpdateCopy:"+path);
				RandomAccessFile raf = new RandomAccessFile(fileMap.get(fd), "rw");
				long remain = fileMap.get(fd).length();
				while (remain > 0) {
					long len = 1024*1024;
					if (remain<len) 
						len = remain;
                	byte[] buf = new byte[(int)len];
					raf.read(buf, 0, buf.length);
					remain = remain-len;
                	server.uploadFile(path.split(rootDir)[1], buf, remain);
				}
                System.err.println("Version update:\t"+cache.get(masterCopy.get(path)));
                // update the copy on server
                // update the LRU and set the used 0
                //cache.close(path);
            } catch(Exception e) {
				//e.printStackTrace();
			}
			return 1;
        }
        /* Function Name: close
        * 		close the fd
        * */
		public synchronized int close( int fd ) {
			if (fileMap.containsKey(fd)){
				// close file successfully
				cache.status();
				if (rightMap.get(fd) != OpenOption.READ){
					updateCopy(fd);
					cache.remove(realMap.get(fd));
				}
				try {
					cache.close(realMap.get(fd));
				} catch(Exception e){}
				fileMap.remove(fd);
				rightMap.remove(fd);
				ptMap.remove(fd);
				pathMap.remove(fd);
				realMap.remove(fd);
				cache.status();
				return 0;
			}
			return Errors.EBADF;
		}
		/* Function Name: write
 		*		write file
 		* */
		public long write( int fd, byte[] buf ){
			if (fileMap.containsKey(fd)) {
				OpenOption right = rightMap.get(fd);
				int len = 0;
				File f = fileMap.get(fd);
				if (right == OpenOption.READ)
					return Errors.EBADF;
				if (f.isDirectory())
					return Errors.EISDIR;
				try {
					RandomAccessFile raf;
					if (ptMap.containsKey(fd))
						raf = ptMap.get(fd);	
					else {
						raf = new RandomAccessFile(f,"rw");
						ptMap.put(fd, raf);
					}
					raf.write(buf);
				} catch (IOException err) {
					return Errors.ENOMEM;
				}
				return buf.length;	
			}
			return Errors.EBADF;
		}
		/* Function Name: read
 		*		read file
 		* */
		public long read( int fd, byte[] buf ){
			if (fileMap.containsKey(fd)) {
				OpenOption right = rightMap.get(fd);
				File f = fileMap.get(fd);
				int len = 0;
				if (right == OpenOption.WRITE) 
					return Errors.EBADF;
				if (f.isDirectory())
					return Errors.EISDIR;
				try {
					RandomAccessFile raf;
					if (ptMap.containsKey(fd))
						raf = ptMap.get(fd);
					else {
						raf = new RandomAccessFile(f, "rw");
						ptMap.put(fd, raf);	
					}
					len = raf.read(buf);
				} catch (IOException err) {
					return Errors.ENOMEM;
				}
				if (len==-1)
					return 0;
				return len;
			}
			return Errors.EBADF;
		}
		/* Function Name: lseek 
 		*		Use RandomAccessFile to change file pointer
 		* */
		public long lseek(int fd, long pos, LseekOption o ) {
			if (fileMap.containsKey(fd)) {
				File f = fileMap.get(fd);
				if (f.isDirectory())
					return Errors.EISDIR;
				try {
					RandomAccessFile raf;
					if (ptMap.containsKey(fd))
						raf = ptMap.get(fd);
					else {
						raf = new RandomAccessFile(f, "rw");
						ptMap.put(fd, raf);
					}
					if (o == LseekOption.FROM_CURRENT) {
						long current = raf.getFilePointer();
						raf.seek(current+pos);
						return current+pos;
					} else if (o == LseekOption.FROM_END) {
						long end = raf.length();
						raf.seek(end-pos);
						return end-pos;
					} else {
						raf.seek(pos);
						return pos;
					}
				} catch (IOException err) {
					//to be done	
				}
			}	
			return Errors.EBADF;
		}
		/* Function Name: unlink
 		*		Unlink File on server and update LRU
 		* */
		public int unlink( String path ) {
            String ret = FileSystems.getDefault().getPath(rootDir+path).toAbsolutePath().normalize().toString();
			cache.remove(ret);
			System.err.println("Unlink");
			cache.status();
			try {
				return server.unlink(path);
			} catch(Exception e) {
				// maybe need to be updated
				return -1;
			}	
		}

		public void clientdone() {
			return;
		}

	}
	
	private static class FileHandlingFactory implements FileHandlingMaking {
		public FileHandling newclient() {
			return new FileHandler();
		}
	}
	public static ServerAbstract getServerInstance(String ip, int port) {
		String url = String.format("//%s:%d/Server", ip, port);
		try {
			return (ServerAbstract) Naming.lookup(url);
		} catch(Exception e) {
			e.printStackTrace();
			return null;	
		}
	}

	public static void main(String[] args) throws IOException {
		String ip = args[0];
		int port = Integer.parseInt(args[1]);	
		rootDir = FileSystems.getDefault().getPath(args[2]).toAbsolutePath().normalize().toString()+"/";
		capacity = Integer.parseInt(args[3]);
		server = null;
		try {
			server = getServerInstance(ip, port);	
		} catch(Exception e) {
			e.printStackTrace();
		}
		if (server == null) 
			System.exit(1);
		(new RPCreceiver(new FileHandlingFactory())).run();
	}
}
/*
 *	LRU cache
 * */
class LRUCache {
	private class Node{
        Node prev;
        Node next;
        String key;
		// version number
        int value;
		// number of users currently using this file
		int used;
		// file size
		int size;
        public Node(String key, int value) {
            this.key = key;
            this.value = value;
            this.prev = null;
            this.next = null;
			this.used=0;
			File t = new File(key);
			this.size = (int)t.length();
        }
    }
	// capacity of LRU
    private int capacity;
	// current occupied size
	private int curCap;
    private HashMap<String, Node> hs = new HashMap<String, Node>();
    private Node head = new Node("-1", -1);
    private Node tail = new Node("-1", -1);

	public LRUCache(int capacity) {
        this.capacity = capacity;
		this.curCap = 0;
        tail.prev = head;
        head.next = tail;
    }

	public boolean containsKey(String key) { 
		return hs.containsKey(key);
	}

    // return the version number
	public int get(String key) {
        if(!hs.containsKey(key)) {
            return -1;
        }
		Node current = hs.get(key);
        return hs.get(key).value;
    }

	// return the number of used
	public int getUsed(String key) {
		if (!hs.containsKey(key))
			return -1;
		Node current = hs.get(key);
		return hs.get(key).used;
	}

    // update the version number and set it as used
	public int set(String key, int value, int used) {
		// if file doesn't exist, add it to LRU
        if( get(key) != -1) {
            Node n = hs.get(key);
            n.value = value;
			n.used = used;
			File copy = new File(key);
			if (curCap+copy.length()-n.size > capacity)
				return -1;	
            n.prev.next = n.next;
			n.next.prev = n.prev;
			curCap = curCap-n.size+(int)copy.length();
			n.size = (int)copy.length();
            move_to_tail(n);
            return 1;
        }
		File f = new File(key);
		int len = (int)f.length();

        Node cur = head.next;
		System.err.println("Check Cache Size\t"+String.valueOf(curCap)+"+"+String.valueOf(len));
		// evict the unused file according to LRU rules
        while (len+curCap > capacity && cur != tail) {
            if (cur.used!=0) {
                cur = cur.next;
                continue;
            }
            hs.remove(cur.key);
			File del = new File(cur.key);
			System.err.println("File delete in Loop\t"+cur.key);
			curCap -= del.length();
			del.delete();
            cur.prev.next = cur.next;
            cur.next.prev = cur.prev;
			cur = cur.next;
        }
        if (len+curCap>capacity)
            return -1;
		curCap += len;
        Node insert = new Node(key, value);
		insert.used = used;
		insert.size = len;
        hs.put(key, insert);
        move_to_tail(insert);
        return 1;
    }
	// One client finish using this file and update LRU
    public void close(String key) throws Exception {
        Node n = hs.get(key);
        n.used -= 1;
    }
	// Evict file from LRU
	public void remove(String key) {
		System.err.println("Remove LRU\t"+key);
		if (get(key) != -1) {
			Node cur = hs.get(key);
			curCap -= cur.size;
			File del = new File(key);
			del.delete();
			cur.prev.next = cur.next;
			cur.next.prev = cur.prev;
			hs.remove(key);	
		}
	}
	// Delete obsolete master copy on Proxy
	public void deleteObsolete(String path) {
		System.err.println("Delete old master copy\t"+path);
		Node n = hs.get(path);
		if (n.used == 0)
			remove(path);
	}
	// Move used node to tail
    private void move_to_tail(Node current) {
        current.prev = tail.prev;
        tail.prev = current;
        current.prev.next = current;
        current.next = tail;
    }
	// Used for debugging
	public void status() {
		System.err.println("-------------------");
		System.err.println("File cached in LRU:" + String.valueOf(curCap));
		Node cur = head.next;
		while (cur != tail) {
			System.err.println("filename:\t"+cur.key);
			System.err.println("used:\t"+String.valueOf(cur.used));
			System.err.println("version:\t"+String.valueOf(cur.value));
			cur = cur.next;
		}
		System.err.println("-------------------");
	}

}

