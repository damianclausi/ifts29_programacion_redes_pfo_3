import socket
import json
import random
import time
import uuid

# --- Configuración del Cliente ---
SERVIDOR_HOST = '127.0.0.1' # Host del Servidor
SERVIDOR_PUERTO = 9999      # Puerto del Servidor

# --- Datos de Ejemplo (Arreglos) ---
TIPOS_TAREA = [
    "generar_reporte_ventas", 
    "procesar_imagen_perfil", 
    "calcular_facturacion_mensual", 
    "enviar_notificacion_push",
    "actualizar_stock_inventario"
]

USUARIOS_EJEMPLO = ["ana.perez", "juan.lopez", "maria.garcia", "carlos.diaz", "laura.martin"]

def enviar_tarea_al_servidor(carga_util_tarea):
    """
    Se conecta al Servidor, envía una única tarea y espera el resultado. 
    """
    id_tarea = carga_util_tarea['id']
    print(f"Enviando Tarea ID: {id_tarea[:8]}... (Tipo: {carga_util_tarea['nombre_tarea']})")
    
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as socket_cliente:
            
            # 1. Conectarse al Servidor
            socket_cliente.connect((SERVIDOR_HOST, SERVIDOR_PUERTO))
            
            # 2. Enviar el payload de la tarea
            tarea_json = json.dumps(carga_util_tarea)
            socket_cliente.sendall(tarea_json.encode('utf-8'))
            
            # 3. Esperar y recibir resultado del worker
            datos_recibidos = socket_cliente.recv(1024) 
            
            # 4. Decodificar e imprimir
            resultado = json.loads(datos_recibidos.decode('utf-8'))
            print(f"    -> Respuesta del Servidor: {resultado['status']}")
            
    except ConnectionRefusedError:
        print(f"[ERROR] No se pudo conectar al Servidor en {SERVIDOR_HOST}:{SERVIDOR_PUERTO}")
    except Exception as e:
        print(f"[ERROR] Ocurrió un error inesperado: {e}")

if __name__ == "__main__":
    print(f"Iniciando envío de 10 tareas al Servidor en {SERVIDOR_HOST}:{SERVIDOR_PUERTO}...")
    
    for i in range(10):
        # 1. Crear un payload de tarea random
        tarea_actual = {
            "id": str(uuid.uuid4()), # ID único global para la tarea
            "nombre_tarea": random.choice(TIPOS_TAREA),
            "usuario": random.choice(USUARIOS_EJEMPLO),
            "prioridad": random.randint(1, 5)
        }
        
        # 2. Enviar la tarea al Servidor
        enviar_tarea_al_servidor(tarea_actual)
        
        # 3. Esperar un poco
        time.sleep(random.uniform(0.3, 1.0))

    print("\n¡Las 10 tareas han sido enviadas al Servidor!")