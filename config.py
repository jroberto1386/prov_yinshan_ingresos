# ─────────────────────────────────────────────
#  config.py  —  Parámetros globales del sistema
# ─────────────────────────────────────────────

RFC_EMISOR = "YIN080808FT6"   # RFC de la empresa emisora

# Cuentas contables fijas
CTA_INGRESOS = 401010000
CTA_IVA_TRAS = 209010000

# Tipo de póliza CONTPAq: 3 = Diario
TIPO_POL = 3

# Diario CONTPAq
ID_DIARIO = 0

# Catálogo RFC → Cuenta CXC
# Fuente: Clientes_Mzo_26.xls exportado de CONTPAq
CATALOGO_CLIENTES = {
    "LME080530BP0": {"cuenta": 105010001, "nombre": "LEASEPLAN MEXICO S.A. DE C.V"},
    "EFM150724B12": {"cuenta": 105010002, "nombre": "ELEMENT FLEET MANAGEMENT CORPORATION MEXICO S.A. DE C.V."},
    "AAU070531BI4": {"cuenta": 105010003, "nombre": "ALD AUTOMOTIVE SA DE CV"},
    "KIA070115M20": {"cuenta": 105010009, "nombre": "AUTOMOVILES VALLEJO GKA S DE R L DE CV"},
    "SSE160513RZ3": {"cuenta": 105010004, "nombre": "SERVICIOS Y SOLUCIONES EMPRESARIALES TICKET EDENRED SA DE CV"},
    "ASE930924SS7": {"cuenta": 105010007, "nombre": "EDENRED MEXICO"},
    "SOF220808SS5": {"cuenta": 105010008, "nombre": "SOFALSAM"},
    "SER210922CX9": {"cuenta": 105010010, "nombre": "SERVCAR"},
}
