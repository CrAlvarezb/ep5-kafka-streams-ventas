import os

from flask import Flask, jsonify, request

app = Flask(__name__)

# ============================================
# DATOS EN MEMORIA (sin base de datos)
# ============================================
usuarios = [
    {"id": 1, "nombre": "Ana García", "email": "ana@ejemplo.com"},
    {"id": 2, "nombre": "Carlos López", "email": "carlos@ejemplo.com"},
    {"id": 3, "nombre": "María Rodríguez", "email": "maria@ejemplo.com"},
]

# Contador para generar IDs nuevos
siguiente_id = 4


# ============================================
# RUTA: GET /api/saludo
# ============================================
@app.route("/api/saludo", methods=["GET"])
def saludo():
    return jsonify({"mensaje": "Hola desde el Backend"})


# ============================================
# RUTA: GET /api/usuarios
# ============================================
@app.route("/api/usuarios", methods=["GET"])
def obtener_usuarios():
    return jsonify(usuarios)


# ============================================
# RUTA: GET /api/usuarios/<id>
# ============================================
@app.route("/api/usuarios/<int:id>", methods=["GET"])
def obtener_usuario(id):
    usuario = next((u for u in usuarios if u["id"] == id), None)
    if usuario is None:
        return jsonify({"error": "Usuario no encontrado"}), 404
    return jsonify(usuario)


# ============================================
# RUTA: POST /api/usuarios
# ============================================
@app.route("/api/usuarios", methods=["POST"])
def crear_usuario():
    global siguiente_id
    data = request.get_json()

    if not data or "nombre" not in data or "email" not in data:
        return jsonify({"error": "Se requieren 'nombre' y 'email'"}), 400

    nuevo_usuario = {
        "id": siguiente_id,
        "nombre": data["nombre"],
        "email": data["email"],
    }
    siguiente_id += 1
    usuarios.append(nuevo_usuario)
    return jsonify(nuevo_usuario), 201


# ============================================
# RUTA: PUT /api/usuarios/<id>
# ============================================
@app.route("/api/usuarios/<int:id>", methods=["PUT"])
def actualizar_usuario(id):
    usuario = next((u for u in usuarios if u["id"] == id), None)
    if usuario is None:
        return jsonify({"error": "Usuario no encontrado"}), 404

    data = request.get_json()
    if not data:
        return jsonify({"error": "Cuerpo de petición requerido"}), 400

    usuario["nombre"] = data.get("nombre", usuario["nombre"])
    usuario["email"] = data.get("email", usuario["email"])
    return jsonify(usuario)


# ============================================
# RUTA: DELETE /api/usuarios/<id>
# ============================================
@app.route("/api/usuarios/<int:id>", methods=["DELETE"])
def eliminar_usuario(id):
    global usuarios
    usuario = next((u for u in usuarios if u["id"] == id), None)
    if usuario is None:
        return jsonify({"error": "Usuario no encontrado"}), 404

    usuarios = [u for u in usuarios if u["id"] != id]
    return jsonify({"mensaje": f"Usuario {id} eliminado"})


# ============================================
# EJECUTAR SERVIDOR
# ============================================
# GET /health: usado por el healthcheck de Docker y por ./verificar.sh api
@app.route("/health", methods=["GET"])
def health():
    return jsonify({"estado": "ok", "usuarios": len(usuarios)})


# En la EC2 se ejecuta con gunicorn (ver Dockerfile). Esto queda para correrla a mano en local.
# host/puerto/debug salen de variables de entorno: en un servidor debe escuchar en 0.0.0.0 y
# NUNCA con debug=True (el depurador de Flask permite ejecutar codigo remoto).
if __name__ == "__main__":
    app.run(host=os.getenv("API_HOST", "127.0.0.1"),
            port=int(os.getenv("API_PORT", "8081")),
            debug=os.getenv("API_DEBUG", "false").lower() == "true")
