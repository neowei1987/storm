import java.io.File;

import baidu.cae.utils.*;

public class App {
	
	public static void main(String[] args) throws InterruptedException
	{	
		class TailNotifyImpl implements TailNotify {
		    public void notifyMsg(String msg) {  
		        if (msg != null && !"".equals(msg)) {  
		            System.out.println(msg);  
		        }  
		    }   
		}
		
		File file = new File(args[0]);
		LogTail tailer = new LogTail(1, file, true);
		tailer.add(new TailNotifyImpl());
		
		new Thread(tailer).start();
		
		Thread.sleep(100 * 1000);
	}
}
