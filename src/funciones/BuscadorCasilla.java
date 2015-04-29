package funciones;

import org.apache.hadoop.io.IntWritable;

import entities.CeldaValueWritable;
import entities.CoordenadaWritable;

public class BuscadorCasilla {
	double anchoCelda;
	// double latitudMin;
	// double longitudMin;
	double latitud;
	double longitud;

	public BuscadorCasilla(CoordenadaWritable coordenada, double anchoCelda) {
		this.latitud = coordenada.getLatitud().get();
		this.longitud = coordenada.getLongitud().get();
		this.anchoCelda = anchoCelda;
	}

	public CeldaValueWritable celda() {
		CeldaValueWritable celda = new CeldaValueWritable();
		int columna = (int) (this.latitud / this.anchoCelda);
		int fila = (int) (this.longitud / this.anchoCelda);
		// System.out.println("Fila: " + this.latitud + " | " + fila);
		// System.out.println("Columna: " + this.longitud + " | " + columna);
		celda.getCeldaKey().setFila(new IntWritable(fila));
		celda.getCeldaKey().setColumna(new IntWritable(columna));
		return celda;
	}

}
