# create_table_births.py
import happybase
import pandas as pd

# Establecer conexión con HBase
connection = happybase.Connection('localhost')
print("Conexión establecida con HBase")
table_name = 'births_data'
families = {
    'info': dict(),
    'location': dict(),
    'birth': dict(),
    'parent': dict()
}

# Eliminar la tabla si ya existe
if table_name.encode() in connection.tables():
    print(f"Eliminando tabla existente - {table_name}")
    connection.delete_table(table_name, disable=True)

# Crear nueva tabla
connection.create_table(table_name, families)
table = connection.table(table_name)
print("Tabla 'births_data' creada exitosamente")

# Leer el archivo CSV
csv_file = 'Nacimeintos.csv'
df = pd.read_csv(csv_file)

# Cargar los datos en la tabla HBase
for index, row in df.iterrows():
    row_key = f'birth_{index}'.encode()
    data = {
        b'info:periodo': str(row['periodo']).encode(),
        b'location:departamento_nacimiento': str(row['departamento_nacimiento']).encode(),
        b'location:municipio': str(row['municipio']).encode(),
        b'location:area': str(row['area']).encode(),
        b'birth:sexo': str(row['sexo']).encode(),
        b'birth:peso': str(row['peso']).encode(),
        b'birth:talla': str(row['talla']).encode(),
        b'birth:fecha_nacimiento': str(row['fecha_nacimiento']).encode(),
        b'birth:hora_nacimiento': str(row['hora_nacimiento']).encode(),
        b'birth:tiempo_gestaci_n': str(row['tiempo_gestaci_n']).encode(),
        b'birth:n_mero_consultas_prenatales': str(row['n_mero_consultas_prenatales']).encode(),
        b'birth:tipo_parto': str(row['tipo_parto']).encode(),
        b'birth:multiplicidad_embarazo': str(row['multiplicidad_embarazo']).encode(),
        b'parent:edad_madre': str(row['edad_madre']).encode(),
        b'parent:r_gimen_seguridad_social': str(row['r_gimen_seguridad_social']).encode(),
        b'parent:eps': str(row['eps']).encode(),
        b'parent:edad_padre': str(row['edad_padre']).encode()
    }
    table.put(row_key, data)

print("Datos cargados exitosamente")
connection.close()