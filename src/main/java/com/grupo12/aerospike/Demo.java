package com.grupo12.aerospike;

import java.util.Scanner;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.AerospikeException;
import com.aerospike.client.policy.ClientPolicy;
import com.grupo12.aerospike.servicios.ServicioUsuario;

public class Demo {
	private String seedHost;
	private int port;
	private AerospikeClient cliente;
	
	public Demo () {
		ClientPolicy clientPolicy = new ClientPolicy();
		this.cliente = new AerospikeClient (clientPolicy);
	}
	
	public Demo (String seedHost, int port) {
		ClientPolicy clientPolicy = new ClientPolicy();
		this.seedHost = seedHost;
		this.port = port;
		this.cliente = new AerospikeClient (clientPolicy, seedHost, port);
	}
	
	protected void finalize() throws Throwable{
		if (this.cliente != null) {
			this.cliente.close();
		}
	}
	
	protected void work () {
		try {
			System.out.println("INFO: Conectándose a Aerospike");
			
			// Estableciendo conexión con Aerospike
			if (cliente == null || !cliente.isConnected()) {
				System.out.println("\nERROR: Fallo en la conexión");
			} else {
				Scanner input = new Scanner (System.in);
				System.out.println("\nINFO: Conexión a Aerospike exitosa");
				
				//Inicializar servicios
				ServicioUsuario su = new ServicioUsuario(cliente);
				
				// Opciones
				System.out.println("\nIngrese la acción que desea realizar: \n");
				System.out.println("1> Cargar usuario manualmente\n");
				System.out.println("2> Cargar usuarios desde un archivo .csv\n");
				System.out.println("2> Leer el registro de un usuario\n");
				System.out.println("3> Consulta interesante 1\n");
				System.out.println("4> Consulta interesante 2\n");
				System.out.println("5> Consulta interesante 3\n");
				System.out.println("6> Consulta interesante 4\n");
				System.out.println("7> Consulta interesante 5\n");
				System.out.println("0> Salir\n");
				System.out.println("\nSeleccione 0-5 y presione Enter\n");
				int seleccion = input.nextInt();
				
				if (seleccion != 0) {
					switch (seleccion){
						case 1:
							System.out.println("\n**********Ha seleccionado: cargar usuario manualmente**********\n"); 
							//su.crearUsuario();
							break;
						case 2:
							System.out.println("\n**********Ha seleccionado: leer el registro de un usuario**********\n");
							//su.leerUsuario();
							break;
						case 3:
							System.out.println("\n**********Ha seleccionado: consulta interesante 1**********\n");
							break;
						case 4:
							System.out.println("\n**********Ha seleccionado: consulta interesante 2**********\n");
							break;
						case 5:
							System.out.println("\n**********Ha seleccionado: consulta interesante 3**********\n");
							break;
						case 6:
							System.out.println("\n**********Ha seleccionado: consulta interesante 4**********\n");
							break;
						case 7:
							System.out.println("\n**********Ha seleccionado: consulta interesante 5**********\n");
							break;
						default:
							break;
					}
				}
			}
		} catch (AerospikeException e) {
			System.out.println("Aerospike Exception - Mensaje: "+e.getMessage()+ "\n");
			System.out.println("Aerospike Exception - Stack Trace: "+e.getStackTrace()+ "\n");
		} catch (Exception e) {
			System.out.println("Exception - Mensaje: "+e.getMessage()+ "\n");
			System.out.println("Exception - Stack Trace: "+e.getStackTrace()+ "\n");
		} finally {
			if (cliente != null && cliente.isConnected()) {
				cliente.close();
			}
			System.out.println("\nINFO: Presione cualquier tecla para salir");
		}
	}

	public String getSeedHost() {
		return seedHost;
	}

	public void setSeedHost(String seedHost) {
		this.seedHost = seedHost;
	}

	public int getPort() {
		return port;
	}

	public void setPort(int port) {
		this.port = port;
	}

	public AerospikeClient getCliente() {
		return cliente;
	}

	public void setCliente(AerospikeClient cliente) {
		this.cliente = cliente;
	}

	public static void main(String[] args) {
		Demo demo = new Demo("172.28.128.4", 3000);
		ServicioUsuario su = new ServicioUsuario(demo.getCliente());
		su.insertarUsuariosDesdeCsv("src/main/resources/usuarios.csv");

	}

}
