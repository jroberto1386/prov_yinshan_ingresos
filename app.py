# ─────────────────────────────────────────────
#  app.py  —  Servidor Flask
#  Pólizas de Provisión de Ingresos CFDI XML
# ─────────────────────────────────────────────

import os
import uuid
import threading
import tempfile
from flask import Flask, request, jsonify, send_file, render_template

from motor import procesar_zip, generar_excel

app = Flask(__name__)
app.config["MAX_CONTENT_LENGTH"] = 500 * 1024 * 1024  # 500 MB

# ── Estado en memoria por sesión ──────────────
JOBS = {}   # job_id → dict con estado, progreso, resultado


# ── Rutas ─────────────────────────────────────

@app.route("/")
def index():
    return render_template("index.html")


@app.route("/procesar", methods=["POST"])
def procesar():
    """Recibe el ZIP y el número de póliza inicial, lanza procesamiento en background."""
    if "archivo" not in request.files:
        return jsonify({"error": "No se recibió ningún archivo"}), 400

    archivo = request.files["archivo"]
    if not archivo.filename.lower().endswith(".zip"):
        return jsonify({"error": "El archivo debe ser un ZIP con XMLs"}), 400

    try:
        num_pol = int(request.form.get("num_poliza", 1))
    except ValueError:
        return jsonify({"error": "Número de póliza inválido"}), 400

    zip_bytes = archivo.read()
    job_id    = str(uuid.uuid4())

    JOBS[job_id] = {
        "estado":    "procesando",
        "progreso":  0,
        "total":     0,
        "archivo":   None,   # ruta al archivo temporal en disco
        "error":     None,
        "stats":     None,
    }

    def run():
        tmp_path = None
        try:
            def cb(done, total):
                JOBS[job_id]["progreso"] = done
                JOBS[job_id]["total"]    = total

            facturas, omitidos, sin_catalogo = procesar_zip(zip_bytes, num_pol, cb)
            excel_bytes = generar_excel(facturas, num_pol)

            # Guardar en archivo temporal en disco (sobrevive entre requests)
            tmp = tempfile.NamedTemporaryFile(
                delete=False,
                suffix=".xlsx",
                prefix=f"job_{job_id}_"
            )
            tmp.write(excel_bytes)
            tmp.close()
            tmp_path = tmp.name

            JOBS[job_id]["archivo"] = tmp_path
            JOBS[job_id]["stats"]   = {
                "procesadas":    len(facturas),
                "omitidas":      len(omitidos),
                "sin_catalogo":  len(sin_catalogo),
                "polizas_desde": num_pol,
                "polizas_hasta": num_pol + len(facturas) - 1 if facturas else num_pol,
            }
            JOBS[job_id]["estado"] = "listo"

        except Exception as e:
            if tmp_path and os.path.exists(tmp_path):
                os.unlink(tmp_path)
            JOBS[job_id]["estado"] = "error"
            JOBS[job_id]["error"]  = str(e)

    threading.Thread(target=run, daemon=True).start()
    return jsonify({"job_id": job_id})


@app.route("/progreso/<job_id>")
def progreso(job_id):
    job = JOBS.get(job_id)
    if not job:
        return jsonify({"error": "Job no encontrado"}), 404
    return jsonify({
        "estado":   job["estado"],
        "progreso": job["progreso"],
        "total":    job["total"],
        "stats":    job["stats"],
        "error":    job["error"],
    })


@app.route("/descargar/<job_id>")
def descargar(job_id):
    job = JOBS.get(job_id)
    if not job or job["estado"] != "listo":
        return jsonify({"error": "Resultado no disponible"}), 404

    ruta = job.get("archivo")
    if not ruta or not os.path.exists(ruta):
        return jsonify({"error": "Archivo temporal no encontrado. Vuelve a procesar."}), 404

    return send_file(
        ruta,
        mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        as_attachment=True,
        download_name="Polizas_Provision_Ingresos.xlsx",
    )


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5050))
    app.run(host="0.0.0.0", port=port, debug=False)
