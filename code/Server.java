import java.rmi.server.UnicastRemoteObject;
import java.rmi.Remote;
import java.rmi.RemoteException;
import java.rmi.Naming;
import java.io.*;
import java.util.*;
import java.rmi.registry.LocateRegistry;
import java.nio.file.*;

public class Server extends UnicastRemoteObject implements ServerAbstract {
	public Server() throws RemoteException{
		super();
	}
	
	//use a hashmap to record the version of the file
	//when a proxy upload a new file the version autoincrements
	static HashMap<String, Integer> versionRec = new HashMap<String, Integer>();
	static HashMap<String, RandomAccessFile> chunkRead = new HashMap<String, RandomAccessFile>();
	static HashMap<String, RandomAccessFile> chunkWrite = new HashMap<String, RandomAccessFile>();
	static String rootDir;
	/*	Function Name: downloadFile
 	*		chunk download file from server 
 	* */
	public synchronized byte[] downloadFile(String path) {
		try{
			File file = new File(rootDir+path);
			if (!file.exists())
				return null;
			if (!chunkRead.containsKey(path))
				chunkRead.put(path, new RandomAccessFile(file, "rw")); 
			if (!versionRec.containsKey(path))
				versionRec.put(path, 1);
			RandomAccessFile raf = chunkRead.get(path);
			long cur = raf.getFilePointer();
			System.err.println("Read file start at\t"+String.valueOf(cur));
			System.err.println("File length\t"+String.valueOf(file.length()));
			long len = 1024*1024;
			boolean flag = false;
			if (file.length()-cur <= len) {
				len = file.length()-cur;
				flag=true;
			}
			byte[] buffer = new byte[(int)len];
			System.err.println("Download File");
         	raf.read(buffer);
			if (flag)
				chunkRead.remove(path);
         	return buffer;
		} catch(Exception e) {
			System.out.println(e.getMessage());
         	e.printStackTrace();
         	return null;
		}
	}
	/*	Function Name: uploadFile
 	*		chunk upload file from server
 	* */
	public synchronized boolean uploadFile(String path, byte[] bytes, long remain) {
		System.err.println("upload\t"+path);
		try {
			if (versionRec.containsKey(path))
				versionRec.put(path, versionRec.get(path)+1);
			else
				versionRec.put(path, 1);
			System.err.println("Version updated:\t"+versionRec.get(path));
			File temp = new File(rootDir+path);
			if (!temp.exists()) {
				temp.getParentFile().mkdirs();
				temp.createNewFile();
			}	
			if (!chunkWrite.containsKey(path))
				chunkWrite.put(path, new RandomAccessFile(temp, "rw"));
			RandomAccessFile raf = chunkWrite.get(path);
			raf.write(bytes);
			if (remain == 0)
				chunkWrite.remove(path);
			return true;
		} catch(Exception e) {
			System.err.println("upload error");
			e.printStackTrace();
			return false;
		}	
	}
	/* Function Name: getVersion
 	*		return version number and file size
 	* */
	public synchronized int[] getVersion(String path) {
		int[] ret = new int[2];
		if (versionRec.containsKey(path)) {
			ret[0] = versionRec.get(path);	
		}		
		else {
			File temp = new File(rootDir+path);
			if (temp.exists()) {
				versionRec.put(path, 1);
				ret[0] = 1;
			} else 
				ret[0] = -1;
		}
		try {
			File file = new File(rootDir+path);
			ret[1] = (int)file.length();
		} catch(Exception err){
			ret[1] = -1;
		}
		return ret;
		
	}
	/*	Function Name: updateVersion
 	*		update the version number of a file
 	* */
	public synchronized void updateVersion(String path, int ver) {
		versionRec.put(path, ver);
	}
	/*	Function Name: unlink
 	*		unlink file
 	* */
	public synchronized int unlink(String path) {
		File file = new File(rootDir+path);
		if (file.delete()) {
			if (versionRec.containsKey(path))
				versionRec.remove(path);	
			return 1;
		}
		return FileHandling.Errors.ENOENT;
	}
	/*	Function Name: createNewFile
 	*		create new file while the OpenOption is CreateNew
 	* */
	public synchronized boolean createNewFile(String path) {
		File file = new File(rootDir+path);
		try { 
			if (!file.exists()) { 
				file.createNewFile();
				System.err.println("create new File");
				return true;
			} else
				return false;
		} catch (IOException e) {};
		return false;
	}

	

	public static void main(String[] args) {
		int port = Integer.parseInt(args[0]);
		rootDir = FileSystems.getDefault().getPath(args[1]).toAbsolutePath().normalize().toString()+"/";
		try {
			String name = "Server";
			Server server = new Server();
			LocateRegistry.createRegistry(port);
			Naming.rebind(String.format("//127.0.0.1:%d/%s", port, name), server);
		} catch (Exception err) {
			System.err.println(err);
			err.printStackTrace();
		}
	}
}


