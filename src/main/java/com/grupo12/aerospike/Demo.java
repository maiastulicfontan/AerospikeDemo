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
		this.cliente = new AerospikeClient (clientPolicy, this.seedHost, this.port);
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
				System.out.println("1> Crear un usuario y un tweet\n");
				System.out.println("2> Leer el registro de un usuario\n");
				System.out.println("0> Salir\n");
				System.out.println("\nSeleccione 0-5 y presione Enter\n");
				int seleccion = input.nextInt();
				
				if (seleccion != 0) {
					switch (seleccion){
						case 1:
							System.out.println("\n**********Ha seleccionado: crear un usuario y un tweet**********\n"); 
							//su.crearUsuario();
							//st.crearTweet();
							break;
						case 2:
							System.out.println("\n**********Ha seleccionado: leer el registro de un usuario**********\n");
							//su.leerUsuario();
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

	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}

}
