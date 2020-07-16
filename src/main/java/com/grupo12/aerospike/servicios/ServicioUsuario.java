package com.grupo12.aerospike.servicios;

import java.util.Arrays;
import java.util.Scanner;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.AerospikeException;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.policy.RecordExistsAction;
import com.aerospike.client.policy.WritePolicy;

public class ServicioUsuario {
	private AerospikeClient cliente;
	
	public ServicioUsuario (AerospikeClient cliente) {
		this.cliente = cliente;
	}
	
	public void crearUsuario() throws AerospikeException {
		System.out.println("\n**************Crear Usuario**************\n");
		
		String nombreUsuario;
		String password;
		String genero;
		String intereses;
		
		Scanner input = new Scanner(System.in);
		
		System.out.println("Ingrese nombre de usuario: ");
		nombreUsuario = input.nextLine();
		
		if (nombreUsuario != null && nombreUsuario.length() > 0) {
			System.out.println("Ingrese contraseña para el usuario "+ nombreUsuario + ":");
			password = input.nextLine();
			
			System.out.println("Ingrese genero (M-Masculino, F-Femenino, O-Otro): ");
			genero = input.nextLine().substring(0, 1);
			
			System.out.println("Ingrese intereses separados por comas: ");
			intereses = input.nextLine();
			
			WritePolicy writePolicy = new WritePolicy();
			writePolicy.recordExistsAction = RecordExistsAction.UPDATE;
			
			Key key = new Key("test", "usuarios", nombreUsuario);
			Bin bin1 = new Bin("nombreusuario", nombreUsuario);
			Bin bin2 = new Bin("password", password);
			Bin bin3 = new Bin("genero", genero);
			Bin bin4 = new Bin("contadortweets", 0);
			Bin bin5 = new Bin("intereses", Arrays.asList(intereses.split(",")));
			
			cliente.put(writePolicy, key, bin1, bin2, bin3, bin4, bin5);
			
			System.out.println("\nINFO: Registro de usuario creado");
		}
	}
	
	public void insertarUsuario(String nombreUsuario, String password, String genero, int contadortweets, ) {
		
	}
}
