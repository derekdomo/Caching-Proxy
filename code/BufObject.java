import java.io.Serializable;

class BufObject implements Serializable {
	byte[] buf;
	boolean flag;
	public BufObject(byte[] buf, boolean flag) {
		this.buf = buf;
		this.flag = flag;
	}
}
