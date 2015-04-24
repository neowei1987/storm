import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.concurrent.BlockingQueue;

public class LogReceiveServer
{
	private int _port;
	BlockingQueue<String> _dataQueue;
	ServerSocket _serverSocket;
	
	LogReceiveServer(int port, BlockingQueue<String> dataQueue)
	{
		_port = port;
		_dataQueue = dataQueue;
		try {
			_serverSocket = new ServerSocket(_port);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public void startUp()
	{
		try
		{			
			new Thread(new Runnable(){
				public void run() {
					while(true)
					{
						Socket clientSocket = null;
						try {
							clientSocket = _serverSocket.accept();
						} catch (IOException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
						if (clientSocket != null)
						{
							Thread t = new Thread(new ClientHandler(clientSocket, _dataQueue));
							t.start();		
						}
					}
				}
				
			}).start();
			
		
		}catch(Exception e)
		{
			e.printStackTrace();
		}
	}
	
	public class ClientHandler implements Runnable
	{
		private BufferedReader reader;
		private Socket socket;
		BlockingQueue<String> _dataQueue;
		
		public ClientHandler(Socket clientSocket, BlockingQueue<String> dataQueue)
		{
			try
			{
				socket = clientSocket;
				InputStreamReader isReader = new InputStreamReader(socket.getInputStream());
				reader = new BufferedReader(isReader);
				_dataQueue = dataQueue;
			} catch (IOException e)
			{
				e.printStackTrace();
			}
		}
		
		public void run()
		{
			String message;
			try
			{
				while((message = reader.readLine()) != null)
				{
					_dataQueue.offer(message);
				}
			} catch (IOException e)
			{
				e.printStackTrace();
			}
		}
	}
}
