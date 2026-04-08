# ─────────────────────────────────────────────
#  motor.py  —  Parseo de XMLs CFDI 4.0 y
#               generación de layout CONTPAq
#  v2 — Streaming: procesa un XML a la vez,
#       escribe al Excel en vuelo, sin acumular
#       todos los bytes en RAM simultáneamente.
# ─────────────────────────────────────────────

import zipfile
import io
from datetime import datetime
from xml.etree import ElementTree as ET

import openpyxl
from openpyxl.styles import Font, PatternFill

from config import (
    RFC_EMISOR, CTA_INGRESOS, CTA_IVA_TRAS,
    TIPO_POL, ID_DIARIO, CATALOGO_CLIENTES
)

# ── Namespaces CFDI 4.0 ──────────────────────
NS = {
    "cfdi": "http://www.sat.gob.mx/cfd/4",
    "tfd":  "http://www.sat.gob.mx/TimbreFiscalDigital",
}

# ── Encabezados fijos del layout CONTPAq (22 filas) ──
HEADERS = [
    ["Egreso(EG)", "IdDocumentoDe", "TipoDocumento", "Folio", "Fecha", "FechaAplicacion",
     "CodigoPersona", "BeneficiarioPagador", "IdCuentaCheques", "CodigoMoneda",
     "Total", "Referencia", "Origen", "BancoDestino", "CuentaDestino",
     "OtroMetodoDePago", "Guid", None, None, "TipoCambio",
     "UUIDRep", "NodoPago", "CodigoMonedaTipoCambio", "NumAsoc"],
    ["deposito.1(DE)", "IdDocumentoDe", "TipoDocumento", "Folio", "Fecha", "Ejercicio",
     "Periodo", "FechaAplicacion", "EjercicioAp", "PeriodoAp",
     "IdCuentaCheques", "NatBancaria", "Naturaleza", "Total", "Referencia",
     "Concepto", "EsConciliado", "IdMovEdoCta", "EjercicioPol", "PeriodoPol",
     "TipoPol", "NumPol", "FormaDeposito", "IdPoliza", "Origen",
     "IdDocumento", "PolizaAgrupada", "UsuarioCrea", "UsuarioModifica", "tieneCFD", "Guid"],
    ["ingreso.1(IN)", "IdDocumentoDe", "TipoDocumento", "Folio", "Fecha", "FechaAplicacion",
     "CodigoPersona", "BeneficiarioPagador", "IdCuentaCheques", "CodigoMoneda",
     "Total", "Referencia", "Origen", "BancoOrigen", "CuentaOrigen",
     "OtroMetodoDePago", "Guid", None, None, "TipoCambio",
     "NumeroCheque", "UUIDRep", "NodoPago", "CodigoMonedaTipoCambio", "NumAsoc"],
    ["Datos para CONTPAQi Factura Electrónica®(FE)", "RutaAnexo", "ArchivoAnexo"],
    ["Movimiento de póliza(M1)", "IdCuenta", "Referencia", "TipoMovto", "Importe",
     "IdDiario", "ImporteME", "Concepto", "IdSegNeg", "Guid", "FechaAplicacion"],
    ["Devolución de IVA (IETU)(W)", "IETUDeducible", "IETUModificado"],
    ["Devolución de IVA(V)", "IdProveedor", "ImpTotal", "PorIVA", "ImpBase",
     "ImpIVA", "CausaIVA", "ExentoIVA", "Serie", "Folio", "Referencia",
     "OtrosImptos", "ImpSinRet", "IVARetenido", "ISRRetenido", "GranTotal",
     "EjercicioAsignado", "PeriodoAsignado", "IdCuenta", "IVAPagNoAcred",
     "UUID", None, "IEPS"],
    ["Asociación de nodo de pago(AP)", "UUIDRep", "NumNodoPago", "GuidReferencia", "AplicationType"],
    ["Periodo de causación de IVA(R)", "EjercicioAsignado", "PeriodoAsignado"],
    ["Póliza(P)", "Fecha", "TipoPol", "Folio", "Clase", "IdDiario",
     "Concepto", "SistOrig", "Impresa", "Ajuste", "Guid"],
    ["Asociación movimiento(AM)", "UUID"],
    ["Comprobantes(MC)", "IdCuentaFlujoEfectivo", "IdSegmentoNegCtaFlujo", "Fecha",
     "Serie", "Folio", "UUID", "ClaveRastreo", "Referencia", "IdProveedor",
     "CodigoConceptoIETU", "ImpNeto", "ImpNetoME", "IdCuentaNeto",
     "IdSegmentoNegNeto", "PorIVA", "ImporteIVA", "ImporteIVAME",
     "IVATasaExcenta", "IdCuentaIVA", "IdSegmentoNegIVA", "NombreImpuesto",
     "ImpImpuesto", "ImpImpuestoME", "IdCuentaImpuesto", "IdSegmentoNegImp",
     "ImpOtrosGastos", "ImpOtrosGastosME", "IdCuentaOtrosGastos",
     "IdSegmentoNegOtrosGastos", "IVARetenido", "IVARetenidoME",
     "IdCuentaRetIVA", "IdSegmentoNegRetIVA", "ISRRetenido", "ISRRetenidoME",
     "IdCuentaRetISR", "IdSegmentoNegRetISR", "NombreOtrasRetenciones",
     "ImpOtrasRetenciones", "ImpOtrasRetencionesME", "IdCuentaOtrasRetenciones",
     "IdSegmentoNegOtrasRet", "BaseIVADIOT", "BaseIETU", "IVANoAcreditable",
     "ImpTotalErogacion", "IVAAcreditable", "ImpExtra1", "ImpExtra2",
     "IdCategoria", "IdSubCategoria", "TipoCambio", "IdDocGastos",
     "EsCapturaCompleta", "FolioStr"],
    ["Movimiento de póliza(M)", "IdCuenta", "Referencia", "TipoMovto", "Importe",
     "IdDiario", "ImporteME", "Concepto", "IdSegNeg"],
    ["Dispersiones de pago(DP)", "UUID", "UUIDRep", "GuidRef", "NumNodoPago",
     "FechaPago", "TotalPago", "TipoCambio", "TotalPagoComprobante"],
    ["Devolución de IVA (IETU)(W2)", "IETUDeducible", "IETUAcreditable",
     "IETUModificado", "IdConceptoIETU"],
    ["Movimientos de impuestos(I)", "IdPersona", "EjercicioAsignado",
     "PeriodoAsignado", "IdCuenta", "AplicaImpuesto", "Serie", "Folio",
     "Referencia", "UUID", "Origen", "Computable", "TipoMovimiento",
     "TipoFactor", "Impuesto", "ObjetoImpuesto", "NombreImpLocal",
     "TasaOCuota", "ImpBase", "ImpImpuesto", "ImpTotal", "Desglosado",
     "IVANoAcred", "AcumulaIETU", "IdConceptoIETU", "IETUDeducible",
     "IETUModificado", "IETUAcreditable", "GuidMov", "GuidMovPadre",
     "Migrado", "ConceptoIVA", "SubconceptoIVA", "ClasificadorIVA",
     "ProporcionDIOT", "DeducibleDIOT"],
    ["Asociación documento(AD)", "UUID"],
    ["Cheque(CH)", "IdDocumentoDe", "TipoDocumento", "Folio", "Fecha",
     "FechaAplicacion", "CodigoPersona", "BeneficiarioPagador",
     "IdCuentaCheques", "CodigoMoneda", "Total", "Referencia", "Origen",
     "CuentaDestino", "BancoDestino", "Guid", None, "OtroMetodoDePago",
     "TipoCambio", "UUIDRep", "NodoPago", "CodigoMonedaTipoCambio", "NumAsoc"],
    ["IngresosNoDepositados.1(DI)", "IdDocumentoDe", "TipoDocumento", "Folio",
     "Fecha", "Ejercicio", "Periodo", "FechaAplicacion", "EjercicioAp",
     "PeriodoAp", "CodigoPersona", "BeneficiarioPagador", "NatBancaria",
     "Naturaleza", "CodigoMoneda", "CodigoMonedaTipoCambio", "TipoCambio",
     "Total", "Referencia", "Concepto", "EsAsociado", "UsuAutorizaPresupuesto",
     "PosibilidadPago", "EsProyectado", "Origen", "IdChequeOrigen",
     "TipoCambioDeposito", "IdDocumento", "EsAnticipo", "EsTraspasado",
     "UsuarioCrea", "UsuarioModifica", "tieneCFD", "Guid",
     "CuentaOrigen", "BancoOrigen", "OtroMetodoDePago", "NumeroCheque", "NumAsoc"],
    ["Causación de IVA (Concepto de IETU)(E)", "IdConceptoIETU"],
    ["Causación de IVA (IETU)(D)", "IVATasa15NoAcred", "IVATasa10NoAcred",
     "IETU", "Modificado", "Origen", "TotTasa16", "BaseTasa16", "IVATasa16",
     "IVATasa16NoAcred", "TotTasa11", "BaseTasa11", "IVATasa11",
     "IVATasa11NoAcred", "TotTasa8", "BaseTasa8", "IVATasa8", "IVATasa8NoAcred"],
    ["Causación de IVA(C)", "Tipo", "TotTasa15", "BaseTasa15", "IVATasa15",
     "TotTasa10", "BaseTasa10", "IVATasa10", "TotTasa0", "BaseTasa0",
     "TotTasaExento", "BaseTasaExento", "TotOtraTasa", "BaseOtraTasa",
     "IVAOtraTasa", "ISRRetenido", "TotOtros", "IVARetenido",
     "Captado", "NoCausar", "IEPS"],
]


# ── Parseo de un XML (bytes) ──────────────────
def _parsear(xml_bytes):
    """
    Devuelve un dict con los datos de la factura,
    la cadena 'sin_catalogo', o None si se omite.
    Libera el árbol XML de inmediato tras extraer los datos.
    """
    try:
        root = ET.fromstring(xml_bytes)
    except ET.ParseError:
        return None

    if root.get("TipoDeComprobante", "") != "I":
        return None

    emisor = root.find("cfdi:Emisor", NS)
    if emisor is None or emisor.get("Rfc", "") != RFC_EMISOR:
        return None

    receptor = root.find("cfdi:Receptor", NS)
    if receptor is None:
        return None
    rfc_receptor = receptor.get("Rfc", "")

    if rfc_receptor not in CATALOGO_CLIENTES:
        return "sin_catalogo"

    cliente  = CATALOGO_CLIENTES[rfc_receptor]
    fecha_str = root.get("Fecha", "")
    folio     = root.get("Folio", "")
    serie     = root.get("Serie", "")
    subtotal  = float(root.get("SubTotal", "0") or 0)
    total     = float(root.get("Total",    "0") or 0)

    iva = 0.0
    impuestos = root.find("cfdi:Impuestos", NS)
    if impuestos is not None:
        traslados = impuestos.find("cfdi:Traslados", NS)
        if traslados is not None:
            for t in traslados.findall("cfdi:Traslado", NS):
                iva += float(t.get("Importe", "0") or 0)

    uuid = ""
    complemento = root.find("cfdi:Complemento", NS)
    if complemento is not None:
        tfd = complemento.find("tfd:TimbreFiscalDigital", NS)
        if tfd is not None:
            uuid = tfd.get("UUID", "").upper()

    try:
        fecha = datetime.strptime(fecha_str[:10], "%Y-%m-%d")
    except ValueError:
        fecha = None

    ref_folio = f"{serie}{folio}".strip() if serie else folio

    # Liberar el árbol explícitamente
    root.clear()

    return {
        "fecha":          fecha,
        "folio":          ref_folio,
        "rfc_receptor":   rfc_receptor,
        "nombre_cliente": cliente["nombre"],
        "cta_cxc":        cliente["cuenta"],
        "subtotal":       round(subtotal, 2),
        "iva":            round(iva, 2),
        "total":          round(total, 2),
        "uuid":           uuid,
    }


# ── Procesamiento ZIP + generación Excel en streaming ──
def procesar_zip(zip_bytes, num_poliza_inicio, callback_progreso=None):
    """
    Lee el ZIP entrada por entrada (streaming).
    Parsea cada XML, lo descarta de RAM inmediatamente,
    y acumula solo los dicts ligeros de facturas válidas.
    Devuelve (facturas, omitidos, sin_catalogo).
    """
    facturas     = []
    omitidos     = []
    sin_catalogo = []

    # Leer el índice del ZIP sin descomprimir todo de golpe
    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
        nombres = [n for n in zf.namelist() if n.lower().endswith(".xml")]
        total   = len(nombres)

        for i, nombre in enumerate(nombres):
            if callback_progreso:
                callback_progreso(i + 1, total)

            # Leer un XML a la vez y descartarlo tras parsear
            with zf.open(nombre) as f:
                xml_bytes_entry = f.read()

            resultado = _parsear(xml_bytes_entry)
            del xml_bytes_entry          # liberar RAM inmediatamente

            if resultado is None:
                omitidos.append(nombre)
            elif resultado == "sin_catalogo":
                sin_catalogo.append(nombre)
            else:
                facturas.append(resultado)

    # Ordenar por fecha y folio
    facturas.sort(key=lambda x: (x["fecha"] or datetime.min, x["folio"]))

    return facturas, omitidos, sin_catalogo


# ── Generación del Excel CONTPAq ──────────────
def generar_excel(facturas, num_poliza_inicio):
    """
    Escribe el Excel fila por fila (write-only mode de openpyxl)
    para minimizar el uso de RAM durante la generación.
    Devuelve bytes del xlsx.
    """
    # write_only=True: openpyxl no mantiene el workbook en RAM,
    # escribe cada fila directo al buffer interno.
    wb = openpyxl.Workbook(write_only=True)
    ws = wb.create_sheet("Pólizas")

    header_fill = PatternFill("solid", fgColor="D9D9D9")
    header_font = Font(bold=True, size=9)

    # ── 1. Escribir las 22 filas de encabezado ──
    for header_row in HEADERS:
        row_cells = []
        for valor in header_row:
            cell = openpyxl.cell.WriteOnlyCell(ws, value=valor)
            cell.font  = header_font
            cell.fill  = header_fill
            row_cells.append(cell)
        ws.append(row_cells)

    # ── 2. Escribir pólizas factura por factura ──
    num_pol = num_poliza_inicio

    for fact in facturas:
        fecha_pol = fact["fecha"]
        folio_ref = fact["folio"]
        cta_cxc   = fact["cta_cxc"]
        total     = fact["total"]
        subtotal  = fact["subtotal"]
        iva       = fact["iva"]
        nombre    = fact["nombre_cliente"]
        uuid      = fact["uuid"]
        concepto  = f"PROVISION VENTAS {folio_ref}"

        # Fila P
        p_row = [None] * 11
        p_row[0] = "P"
        p_row[1] = fecha_pol
        p_row[2] = int(TIPO_POL)
        p_row[3] = int(num_pol)
        p_row[4] = 1
        p_row[5] = int(ID_DIARIO)
        p_row[6] = concepto
        p_row[7] = 12
        p_row[8] = 0
        p_row[9] = 0

        # Fecha como WriteOnlyCell con formato
        p_cells = []
        for col_idx, val in enumerate(p_row):
            cell = openpyxl.cell.WriteOnlyCell(ws, value=val)
            if col_idx == 1 and isinstance(val, datetime):
                cell.number_format = "mm/dd/yyyy"
            p_cells.append(cell)
        ws.append(p_cells)

        # Fila M1 — CXC (Debe)
        ws.append(["M1", int(cta_cxc), f"F-{folio_ref}", 0,
                   total, int(ID_DIARIO), 0, nombre, None])

        # Fila M1 — Ingresos (Haber)
        ws.append(["M1", int(CTA_INGRESOS), f"F-{folio_ref}", 1,
                   subtotal, int(ID_DIARIO), 0, nombre, None])

        # Fila M1 — IVA Trasladado (Haber)
        if iva > 0:
            ws.append(["M1", int(CTA_IVA_TRAS), f"F-{folio_ref}", 1,
                       iva, int(ID_DIARIO), 0, nombre, None])

        # Fila AD — UUID
        if uuid:
            ws.append(["AD", uuid])

        num_pol += 1

    # Guardar en buffer de memoria
    buf = io.BytesIO()
    wb.save(buf)
    buf.seek(0)
    return buf.getvalue()
