package baidu.cae.utils;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.HashSet;
import java.util.Set;

public class LogTail implements Runnable {

	private final Set<TailNotify> listeners = new HashSet<TailNotify>();
	private long sampleInterval = 1;
	private File logfile;
	private boolean startAtBeginning = false;
	private boolean tailing = true;

	public LogTail(long sampleInterval, File logfile, boolean startAtBeginning) {
		super();
		this.sampleInterval = sampleInterval;
		this.logfile = logfile;
		this.startAtBeginning = startAtBeginning;
	}

	public void add(TailNotify tailListener) {
		listeners.add(tailListener);
	}

	protected void notify(String line) {
		for (TailNotify tail : listeners) {
			tail.notifyMsg(line);
		}
	}

	@Override
	public void run() {
		long filePointer = 0;
		if (this.startAtBeginning) { // 判断是否从头开始读文件
			filePointer = 0;
		} else {
			filePointer = this.logfile.length(); // 指针标识从文件的当前长度开始。
		}
		try {
			RandomAccessFile file = new RandomAccessFile(logfile, "r"); // 创建随机读写文件
			while (this.tailing) {
				long fileLength = this.logfile.length();
				if (fileLength < filePointer) {
					file = new RandomAccessFile(logfile, "r");
					filePointer = 0;
				}
				if (fileLength > filePointer) {
					file.seek(filePointer);
					String line = file.readLine();
					while (line != null) {
						this.notify(line);
						line = file.readLine();
					}
					filePointer = file.getFilePointer();
				}
				Thread.sleep(this.sampleInterval);
			}
			file.close();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();

		}

	}

	public void stop() {
		this.tailing = false;
	}

}
