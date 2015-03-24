import java.rmi.*;
import java.rmi.RemoteException;
import java.io.*;

public interface ServerAbstract extends Remote {
	int unlink(String path) throws RemoteException;		
	byte[] downloadFile(String path) throws RemoteException;
	boolean uploadFile(String path, byte[] bytes, long remain) throws RemoteException; 
	int[] getVersion(String path) throws RemoteException;
    void updateVersion(String path, int version) throws RemoteException;
	boolean createNewFile(String path) throws RemoteException;
}
