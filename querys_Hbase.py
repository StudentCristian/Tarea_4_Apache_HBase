# querys_Hbase.py 
import happybase
import pandas as pd

# Establecer conexión con HBase
connection = happybase.Connection('localhost')
print("Conexión establecida con HBase")
table_name = 'births_data'
table = connection.table(table_name)

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

# Consultas de selección, filtrado y recorrido
print("\n=== Primeros 5 registros ===")
for key, data in table.scan(limit=5):
    print(f"Row key: {key.decode()}")
    for column, value in data.items():
        print(f"{column.decode()}: {value.decode()}")
    print()

# Encontrar nacimientos con peso menor a 3000
print("\n=== Nacimientos con peso menor a 3000 ===")
for key, data in table.scan():
    if int(data[b'birth:peso'].decode()) < 3000:
        print(f"\nBirth ID: {key.decode()}")
        print(f"Peso: {data[b'birth:peso'].decode()}")
        print(f"Sexo: {data[b'birth:sexo'].decode()}")

# Análisis de nacimientos por tipo de parto
print("\n=== Nacimientos por tipo de parto ===")
parto_counts = {}

for key, data in table.scan():
    tipo_parto = data[b'birth:tipo_parto'].decode()
    parto_counts[tipo_parto] = parto_counts.get(tipo_parto, 0) + 1

for tipo_parto, count in parto_counts.items():
    print(f"Tipo de parto {tipo_parto}: {count}")

# Operaciones de escritura (inserción, actualización y eliminación)
# Inserción
new_row_key = b'birth_new'
new_data = {
    b'info:periodo': b'2023',
    b'location:departamento_nacimiento': b'SANTANDER',
    b'location:municipio': b'SAN GIL',
    b'location:area': b'CABECERA MUNICIPAL',
    b'birth:sexo': b'MASCULINO',
    b'birth:peso': b'3200',
    b'birth:talla': b'50',
    b'birth:fecha_nacimiento': b'2023-04-01T00:00:00.000',
    b'birth:hora_nacimiento': b'12:00',
    b'birth:tiempo_gestaci_n': b'39',
    b'birth:n_mero_consultas_prenatales': b'8',
    b'birth:tipo_parto': b'ESPONTANEO',
    b'birth:multiplicidad_embarazo': b'SIMPLE',
    b'parent:edad_madre': b'30',
    b'parent:r_gimen_seguridad_social': b'SUBSIDIADO',
    b'parent:eps': b'NUEVA EPS S.A.',
    b'parent:edad_padre': b'32'
}
table.put(new_row_key, new_data)
print("Nuevo registro insertado")

# Actualización
update_row_key = b'birth_0'
update_data = {b'birth:peso': b'3500'}
table.put(update_row_key, update_data)
print("Registro actualizado")

# Eliminación
delete_row_key = b'birth_new'
table.delete(delete_row_key)
print("Registro eliminado")

connection.close()