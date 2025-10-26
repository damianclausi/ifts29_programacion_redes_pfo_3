import socket
import threading
import json
import time
import sqlite3

# --- Configuración del Servidor ---
SERVIDOR_HOST = '127.0.0.1'  # localhost
SERVIDOR_PUERTO = 9999
NOMBRE_DB = 'tareas_pfo3.db'

def inicializar_db():
    """Crea la tabla de registro de tareas si no existe."""
    try:
        conn = sqlite3.connect(NOMBRE_DB)
        cursor = conn.cursor()
        
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS registro_tareas (
            id_tarea TEXT PRIMARY KEY,
            nombre_tarea TEXT NOT NULL,
            payload_recibido TEXT,
            resultado_worker TEXT,
            estado TEXT,
            timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
        )
        ''')
        
        conn.commit()
        conn.close()
        print(f"[SERVIDOR] Base de datos '{NOMBRE_DB}' inicializada.")
    except Exception as e:
        print(f"[ERROR SERVIDOR] No se pudo inicializar la base de datos: {e}")
        exit(1)

def ejecutar_tarea_y_registrar(datos_tarea):
    """
    Esta función es la lógica del WORKER.
    Procesa la tarea y registra el resultado en la DB.
    """
    id_tarea = datos_tarea.get('id', 'ID_DESCONOCIDO')
    print(f"[WORKER/HILO] Procesando tarea ID: {id_tarea}")
    
    # 1. Simular trabajo pesado
    time.sleep(2) 
    
    # 2. Generar un resultado
    nombre_tarea = datos_tarea.get('nombre_tarea', 'desconocida')
    resultado_final = f"Resultado exitoso para '{nombre_tarea}' (Usuario: {datos_tarea.get('usuario')})"
    
    # 3. Registrar en SQLite
    try:
        # Cada hilo worker abre su propia conexión a la DB
        conn = sqlite3.connect(NOMBRE_DB)
        cursor = conn.cursor()
        
        cursor.execute(
            "INSERT INTO registro_tareas (id_tarea, nombre_tarea, payload_recibido, resultado_worker, estado) VALUES (?, ?, ?, ?, ?)",
            (
                id_tarea,
                nombre_tarea,
                json.dumps(datos_tarea), 
                resultado_final,
                'COMPLETADO'
            )
        )
        conn.commit()
        conn.close()
        
    except sqlite3.IntegrityError:
        print(f"[ERROR DB] La tarea con ID {id_tarea} ya existe en el registro.")
    except Exception as e:
        print(f"[ERROR DB] Hilo worker no pudo registrar en SQLite: {e}")
        return {"status": "ERROR_DB", "result": "Error al registrar resultado"}

    # 4. Preparar respuesta para el cliente
    return {"status": "completado", "result": resultado_final}

def manejar_cliente_en_hilo(conexion_cliente, direccion_cliente):
    """
    Esta función se ejecuta en un HILO (worker) separado
    por cada cliente conectado.
    """
    print(f"[SERVIDOR] Asignando nuevo hilo (worker) del pool para {direccion_cliente}")
    
    try:
        # 1. Recibir el payload de la tarea
        payload_recibido = conexion_cliente.recv(1024)
        if not payload_recibido:
            raise ConnectionError("Cliente se desconectó")
            
        datos_tarea = json.loads(payload_recibido.decode('utf-8'))
        
        # 2. El worker ejecuta la lógica de procesamiento
        datos_resultado = ejecutar_tarea_y_registrar(datos_tarea)

        # 3. El worker envía el resultado de vuelta al cliente
        respuesta_json = json.dumps(datos_resultado)
        conexion_cliente.sendall(respuesta_json.encode('utf-8'))

    except (json.JSONDecodeError, ConnectionError) as e:
        print(f"[ERROR HILO] Error de datos o conexión con {direccion_cliente}: {e}")
    except Exception as e:
        print(f"[ERROR HILO INESPERADO] {direccion_cliente}: {e}")
    finally:
        # 4. El hilo (worker) finaliza y cierra la conexión
        print(f"[SERVIDOR] Hilo (worker) para {direccion_cliente} finalizó. Conexión cerrada.")
        conexion_cliente.close()

def iniciar_servidor():
    """
    Inicia el SERVIDOR principal que escucha conexiones
    y las distribuye al pool de hilos (workers). 
    """
    
    inicializar_db()
    
    socket_servidor = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    socket_servidor.bind((SERVIDOR_HOST, SERVIDOR_PUERTO))
    socket_servidor.listen(5)  
    
    print(f"[*] Servidor PFO3 escuchando en {SERVIDOR_HOST}:{SERVIDOR_PUERTO}")

    while True:
        try:
            # Espera por un cliente que envíe una tarea
            conexion_cliente, direccion_cliente = socket_servidor.accept()
            
            # Crea un nuevo HILO (Worker) para manejar esta tarea
            worker_hilo = threading.Thread(
                target=manejar_cliente_en_hilo, 
                args=(conexion_cliente, direccion_cliente)
            )
            worker_hilo.start()
        
        except KeyboardInterrupt:
            print("\n[APAGANDO] Servidor detenido por el usuario.")
            break
        except Exception as e:
            print(f"[ERROR SERVIDOR] Error en el loop principal: {e}")

    socket_servidor.close()

if __name__ == "__main__":
    iniciar_servidor()