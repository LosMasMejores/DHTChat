package classes;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.DatagramPacket;
import java.net.InetAddress;
import java.net.MulticastSocket;
import java.util.Base64;

public class Chat {

	static String GROUP = "228.5.6.7";
	static Integer PORT = 6789;

	public static void main(String[] args) {

		if (args.length != 1) {
			return;
		}

		String user = args[0];
		String pwd = "";
		String sala = "";

		Peer peer = new Peer(user, GROUP, PORT);
		new Thread(peer).start();

		try {
			Thread.sleep(1000);
			System.out.println("Nombre de la sala?: ");
			BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
			sala = br.readLine();
			System.out.println("Password(opcional)?: ");
			pwd = br.readLine();
		} catch (IOException | InterruptedException e) {
			e.printStackTrace();
			return;
		}

		String message = peer.get(Peer.sha1(sala + pwd));

		if (message.equals("")) {
			try {
				BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
				System.out.println("Introduzca IP chat: ");
				String ip = br.readLine();
				InetAddress group = InetAddress.getByName(ip);

				System.out.println("Introduzca PUERTO chat: ");
				String port = br.readLine();
				MulticastSocket socket = new MulticastSocket(Integer.parseInt(port));
				
				if (ip.contains(GROUP) && port.contains(PORT.toString())) {
					socket.close();
					return;
				}

				peer.put(Peer.sha1(sala + pwd), ip + "_" + port);
				lanzarChat(Peer.sha1(sala + pwd), user, group, Integer.parseInt(port), socket);
			} catch (NumberFormatException | IOException e) {
				e.printStackTrace();
				return;
			}
			return;
		}

		String[] campos = message.split("_");
		if (campos.length != 2) {
			return;
		}
		try {
			InetAddress group = InetAddress.getByName(campos[0]);
			MulticastSocket socket = new MulticastSocket(Integer.parseInt(campos[1]));
			lanzarChat(Peer.sha1(sala + pwd), user, group, Integer.parseInt(campos[1]), socket);
		} catch (NumberFormatException | IOException e) {
			e.printStackTrace();
			return;
		}
	}

	public static void lanzarChat(byte[] hash, String user, InetAddress group, int port, MulticastSocket socket) {
		try {
			socket.joinGroup(group);
		} catch (IOException e) {
			e.printStackTrace();
			return;
		}

		System.out.println("Bienvenido al chat " + user);

		new Thread(new Runnable() {
			public void run() {
				while (true) {
					byte[] buf = new byte[1024];
					DatagramPacket recv = new DatagramPacket(buf, buf.length);
					try {
						socket.receive(recv);
						String[] msg = new String(recv.getData()).split(":");
						if (!msg[0].equals(Base64.getEncoder().encodeToString(hash))) {
							continue;
						}
						System.out.println(msg[1]);
					} catch (IOException e) {
						e.printStackTrace();
						return;
					}
				}
			}
		}).start();

		new Thread(new Runnable() {
			public void run() {
				while (true) {
					BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
					String line;
					try {
						line = Base64.getEncoder().encodeToString(hash) + ":" + user + "-" + br.readLine() + ":end";
						socket.send(new DatagramPacket(line.getBytes(), line.getBytes().length, group, port));
					} catch (IOException e) {
						e.printStackTrace();
						return;
					}
				}
			}
		}).start();
	}

}
