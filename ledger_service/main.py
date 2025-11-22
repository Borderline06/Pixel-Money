"""Servicio FastAPI para gestionar el registro de transacciones (Ledger) usando SQL (MariaDB)."""

import os
import httpx
import uuid
import json
import logging
from datetime import datetime, timedelta, timezone
from typing import Optional, List
from collections import defaultdict
from decimal import Decimal

from fastapi import FastAPI, Depends, HTTPException, status, Header, Request, Response
from sqlalchemy.orm import Session
from sqlalchemy import text # <--- CAMBIO: Usamos Session de SQLAlchemy
from prometheus_client import Counter, Histogram, generate_latest, CONTENT_TYPE_LATEST
import time

# Importaciones locales (absolutas)
# <--- CAMBIO: Importamos la configuración de base de datos SQL
from db import engine, Base, get_db 
import models # <--- Importante importar models para que create_all detecte las tablas
import schemas
from dotenv import load_dotenv

load_dotenv()

# Configura logger
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# URLs de Servicios
BALANCE_SERVICE_URL = os.getenv("BALANCE_SERVICE_URL")
INTERBANK_SERVICE_URL = os.getenv("INTERBANK_SERVICE_URL")
INTERBANK_API_KEY = os.getenv("INTERBANK_API_KEY")
AUTH_SERVICE_URL = os.getenv("AUTH_SERVICE_URL")
GROUP_SERVICE_URL = os.getenv("GROUP_SERVICE_URL")

# --- Inicialización de la App y Base de Datos ---
app = FastAPI(
    title="Ledger Service - Pixel Money",
    description="Registra todas las transacciones financieras en SQL (MariaDB).",
    version="2.0.0" # Actualizamos versión a 2.0 (SQL Migration)
)

# Evento de Inicio: Crear tablas SQL si no existen
@app.on_event("startup")
def startup_event():
    try:
        logger.info("Iniciando Ledger Service (SQL Mode)...")
        # Esto crea las tablas definidas en models.py en MariaDB
        Base.metadata.create_all(bind=engine)
        logger.info("✅ Tablas de base de datos verificadas/creadas exitosamente.")
    except Exception as e:
        logger.critical(f"FATAL: Error al configurar schema de Base de Datos: {e}", exc_info=True)

# Evento de Apagado
@app.on_event("shutdown")
def shutdown_event():
    logger.info("Ledger Service deteniéndose.")

# --- Métricas Prometheus ---
REQUEST_COUNT = Counter(
    "ledger_requests_total", 
    "Total requests", 
    ["method", "endpoint", "status_code"]
)
REQUEST_LATENCY = Histogram(
    "ledger_request_latency_seconds", 
    "Request latency", 
    ["endpoint"]
)

# Métricas de Negocio
DEPOSIT_COUNT = Counter(
    "ledger_deposits_total", 
    "Número total de depósitos procesados"
)
TRANSFER_COUNT = Counter(
    "ledger_transfers_total", 
    "Número total de transferencias procesadas"
)
CONTRIBUTION_COUNT = Counter(
    "ledger_contributions_total", 
    "Número total de aportes a grupos"
)
LEDGER_P2P_TRANSFERS_TOTAL = Counter(
    "ledger_p2p_transfers_total",
    "Total de transferencias P2P (BDI -> BDI) procesadas"
)
LEDGER_WITHDRAWALS_TOTAL = Counter(
    "ledger_withdrawals_total",
    "Total de retiros (BDI -> Banco Externo) procesados"
)

# --- Middleware para Métricas ---
@app.middleware("http")
async def metrics_middleware(request: Request, call_next):
    start_time = time.time()
    response = None
    status_code = 500
    try:
        response = await call_next(request)
        status_code = response.status_code
    except HTTPException as http_exc:
        status_code = http_exc.status_code
        raise http_exc
    except Exception as exc:
        logger.error(f"Middleware error: {exc}", exc_info=True)
        return Response("Internal Server Error", status_code=500)
    finally:
        latency = time.time() - start_time
        endpoint = request.url.path
        final_status_code = getattr(response, 'status_code', status_code)
        REQUEST_LATENCY.labels(endpoint=endpoint).observe(latency)
        REQUEST_COUNT.labels(method=request.method, endpoint=endpoint, status_code=final_status_code).inc()
    return response

# --- Funciones de Utilidad (Adaptadas a SQL) ---

def check_idempotency(db: Session, key: str) -> Optional[str]:
    """
    Verifica si una clave de idempotencia ya existe en la tabla SQL.
    Retorna el transaction_id si existe, o None.
    """
    if not key:
        return None
    try:
        # Consulta SQL usando SQLAlchemy
        record = db.query(models.IdempotencyKey).filter(models.IdempotencyKey.key == key).first()
        return record.transaction_id if record else None
    except Exception as e:
        logger.error(f"Error al verificar idempotencia para key {key}: {e}", exc_info=True)
        # En caso de error de BD, mejor fallar seguro que duplicar
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Error interno de base de datos (Idempotencia)")

def get_transaction_by_id(db: Session, tx_id: str) -> Optional[models.Transaction]:
    """
    Busca una transacción por ID en la tabla SQL.
    Retorna el objeto modelo SQLAlchemy.
    """
    try:
        tx = db.query(models.Transaction).filter(models.Transaction.id == tx_id).first()
        return tx
    except Exception as e:
        logger.error(f"Error al obtener transacción {tx_id}: {e}", exc_info=True)
        return None

# --- Endpoints de la API ---

@app.post("/deposit", response_model=schemas.Transaction, status_code=status.HTTP_201_CREATED, tags=["Transactions"])
async def deposit(
    req: schemas.DepositRequest,
    idempotency_key: Optional[str] = Header(None, description="Clave única (UUID v4) para idempotencia"),
    db: Session = Depends(get_db)
):
    if idempotency_key is None:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "Cabecera Idempotency-Key es requerida")

    # 1. Idempotencia (SQL)
    if existing_tx_id := check_idempotency(db, idempotency_key):
        logger.info(f"Depósito duplicado (Key: {idempotency_key}). Devolviendo tx: {existing_tx_id}")
        # Usamos la nueva función get_transaction_by_id que busca en SQL
        tx_data = get_transaction_by_id(db, existing_tx_id)
        if tx_data: return tx_data
        raise HTTPException(status.HTTP_500_INTERNAL_SERVER_ERROR, "Error de idempotencia: Tx original no encontrada")

    tx_id = str(uuid.uuid4())
    now = datetime.now(timezone.utc)
    metadata_json = json.dumps({"description": "Depósito en BDI"})
    status_final = "PENDING"
    currency = "PEN"

    try:
        # 2. Guardar PENDING (SQL - SQLAlchemy)
        new_tx = models.Transaction(
            id=tx_id,
            user_id=req.user_id,
            source_wallet_type="EXTERNAL",
            source_wallet_id="N/A",
            destination_wallet_type="BDI",
            destination_wallet_id=str(req.user_id),
            type="DEPOSIT",
            amount=req.amount,
            currency=currency,
            status=status_final,
            metadata_info=metadata_json,
            created_at=now,
            updated_at=now
        )
        db.add(new_tx)
        db.commit() # Guardamos el estado PENDING
        
    except Exception as e:
        db.rollback()
        logger.error(f"Error al insertar PENDING (depósito) {tx_id}: {e}", exc_info=True)
        raise HTTPException(status.HTTP_500_INTERNAL_SERVER_ERROR, "Error al registrar la transacción inicial")

    try:
        # 3. Llamar a Balance Service
        async with httpx.AsyncClient() as client:
            response = await client.post(
                f"{BALANCE_SERVICE_URL}/balance/credit",
                json={"user_id": req.user_id, "amount": req.amount}
            )
            response.raise_for_status()
        
        status_final = "COMPLETED"
        
    except (httpx.RequestError, httpx.HTTPStatusError) as e:
        # Manejo de Error (Actualizar SQL a FAILED)
        status_final = "FAILED_BALANCE_SVC"
        detail = f"Balance Service falló al acreditar: {e}"
        status_code = status.HTTP_503_SERVICE_UNAVAILABLE
        
        if isinstance(e, httpx.HTTPStatusError):
            try: 
                detail = e.response.json().get("detail", str(e))
            except json.JSONDecodeError: 
                detail = e.response.text
            status_code = e.response.status_code
            
        logger.error(f"Fallo en tx {tx_id} (depósito): {detail}")
        
        # Actualizamos el estado en SQL
        new_tx.status = status_final
        new_tx.updated_at = datetime.now(timezone.utc)
        db.commit() 
        
        raise HTTPException(status_code=status_code, detail=detail)

    try:
        # 4. Éxito: Guardar Idempotencia y Actualizar a COMPLETED (SQL)
        # Guardamos la llave para evitar duplicados futuros
        idempotency_record = models.IdempotencyKey(key=idempotency_key, transaction_id=tx_id)
        db.add(idempotency_record)
        
        # Actualizamos la transacción
        new_tx.status = status_final
        new_tx.updated_at = datetime.now(timezone.utc)
        
        db.commit() # Confirmamos todo junto
        db.refresh(new_tx) # Refrescamos el objeto para devolverlo

        DEPOSIT_COUNT.inc() # Métrica Prometeus

        logger.info(f"Depósito {status_final} para user_id {req.user_id}, tx_id {tx_id}")
        
    except Exception as final_e:
        # Error Crítico en el commit final
        status_final = "PENDING_CONFIRMATION"
        new_tx.status = status_final
        db.commit()
        logger.critical(f"¡FALLO CRÍTICO post-crédito en tx {tx_id}! Estado: {status_final}. Error: {final_e}. Requiere reconciliación manual.")

    # Devolvemos el objeto Transacción actualizado
    return new_tx

@app.post("/transfer", response_model=schemas.Transaction, status_code=status.HTTP_201_CREATED, tags=["Transactions"])
async def transfer(
    req: schemas.TransferRequest, 
    idempotency_key: Optional[str] = Header(None, description="Clave única (UUID v4) para idempotencia"),
    db: Session = Depends(get_db)
):
    """Procesa una transferencia BDI -> Banco Externo (ej. Happy Money)."""
    if idempotency_key is None:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "Cabecera Idempotency-Key es requerida")
    
    # Validación simple de banco destino (Simulación)
    if req.to_bank.upper() not in ["HAPPY_MONEY", "INTERBANK", "BCP"]:
         # Nota: Ajusta esta lista según los bancos que quieras soportar
         pass 

    # 1. Verificar Idempotencia (SQL)
    if existing_tx_id := check_idempotency(db, idempotency_key):
        logger.info(f"Transferencia duplicada (Key: {idempotency_key}). Devolviendo tx: {existing_tx_id}")
        tx_data = get_transaction_by_id(db, existing_tx_id)
        if tx_data: return tx_data
        raise HTTPException(status.HTTP_500_INTERNAL_SERVER_ERROR, "Error de idempotencia: Transacción original no encontrada")

    tx_id = str(uuid.uuid4())
    now = datetime.now(timezone.utc)
    
    metadata = {"to_bank": req.to_bank, "destination_phone_number": req.destination_phone_number}
    status_final = "PENDING"
    currency = "PEN"

    # 2. Guardar PENDING (SQL)
    try:
        new_tx = models.Transaction(
            id=tx_id,
            user_id=req.user_id,
            source_wallet_type="BDI",
            source_wallet_id=str(req.user_id),
            destination_wallet_type="EXTERNAL_BANK",
            destination_wallet_id=req.destination_phone_number, # O la cuenta externa
            type="TRANSFER",
            amount=req.amount,
            currency=currency,
            status=status_final,
            metadata_info=json.dumps(metadata),
            created_at=now,
            updated_at=now
        )
        db.add(new_tx)
        db.commit()

    except Exception as e:
        db.rollback()
        logger.error(f"Error al insertar PENDING (transfer) {tx_id}: {e}", exc_info=True)
        raise HTTPException(status.HTTP_500_INTERNAL_SERVER_ERROR, "Error al registrar la transacción inicial")

    try:
        async with httpx.AsyncClient(timeout=15.0) as client:
            # A. Verificar Fondos en BDI origen
            logger.debug(f"Tx {tx_id}: Verificando fondos para user_id {req.user_id}")
            check_res = await client.post(
                f"{BALANCE_SERVICE_URL}/balance/check",
                json={"user_id": req.user_id, "amount": req.amount}
            )
            check_res.raise_for_status() 

            # B. Llamar al Servicio Interbancario (Happy Money / Otro Grupo)
            # NOTA: Aquí usamos la URL del otro grupo que configuraste en .env
            logger.debug(f"Tx {tx_id}: Llamando a Interbank Service ({req.to_bank})...")
            
            interbank_payload = {
                "origin_bank": "PIXEL_MONEY",
                "origin_account_id": str(req.user_id),
                "destination_bank": req.to_bank.upper(),
                "destination_phone_number": req.destination_phone_number,
                "amount": req.amount,
                "currency": currency,
                "transaction_id": str(tx_id),
                "description": "Transferencia desde Pixel Money"
            }
            # Headers requeridos por el otro grupo
            interbank_headers = {"X-API-KEY": INTERBANK_API_KEY}

            response_bank_b = await client.post(
                f"{INTERBANK_SERVICE_URL}/interbank/transfers", # Ajustar ruta según el otro grupo
                json=interbank_payload,
                headers=interbank_headers
            )
            response_bank_b.raise_for_status() 

            bank_b_response = response_bank_b.json()
            remote_tx_id = bank_b_response.get("remote_transaction_id", "N/A")
            
            # Actualizamos metadata con el ID remoto
            metadata["remote_tx_id"] = remote_tx_id
            new_tx.metadata_info = json.dumps(metadata)
            
            logger.info(f"Banco externo aceptó tx {tx_id}. ID remoto: {remote_tx_id}")

            # C. Debitar Saldo en BDI origen (Confirmación)
            logger.debug(f"Tx {tx_id}: Debitando saldo de user_id {req.user_id}")
            debit_res = await client.post(
                f"{BALANCE_SERVICE_URL}/balance/debit",
                json={"user_id": req.user_id, "amount": req.amount}
            )
            debit_res.raise_for_status()

            # D. Todo OK
            status_final = "COMPLETED"

    except httpx.HTTPStatusError as e:
        status_code = e.response.status_code
        detail = "Error en transferencia externa"
        try: detail = e.response.json().get("detail", e.response.text)
        except: detail = e.response.text

        if status_code == 400: status_final = "FAILED_FUNDS"
        elif status_code == 404: status_final = "FAILED_ACCOUNT"
        else: status_final = f"FAILED_HTTP_{status_code}"

        logger.warning(f"Transferencia fallida {tx_id}: {detail}")
        
        # Actualizar estado FAILED
        new_tx.status = status_final
        new_tx.updated_at = datetime.now(timezone.utc)
        new_tx.metadata_info = json.dumps(metadata)
        db.commit()
        
        raise HTTPException(status_code=status_code, detail=detail)

    except Exception as e:
        status_final = "FAILED_UNKNOWN"
        new_tx.status = status_final
        new_tx.updated_at = datetime.now(timezone.utc)
        db.commit()
        logger.error(f"Error inesperado en tx {tx_id}: {e}", exc_info=True)
        raise HTTPException(status.HTTP_500_INTERNAL_SERVER_ERROR, "Error interno procesando transferencia")

    # Si todo fue exitoso
    try:
        # Guardar llave de idempotencia
        db.add(models.IdempotencyKey(key=idempotency_key, transaction_id=tx_id))
        
        # Actualizar estado COMPLETED
        new_tx.status = status_final
        new_tx.updated_at = datetime.now(timezone.utc)
        new_tx.metadata_info = json.dumps(metadata)
        
        db.commit()
        db.refresh(new_tx)
        
        TRANSFER_COUNT.inc() # Métrica
        logger.info(f"Transferencia {status_final} user {req.user_id} -> {req.to_bank}")
        
        return new_tx

    except Exception as final_e:
        # Error al guardar el estado final (El dinero ya se movió)
        new_tx.status = "PENDING_CONFIRMATION"
        db.commit()
        logger.critical(f"¡FALLO CRÍTICO post-débito en tx {tx_id}! Error: {final_e}. Requiere reconciliación.")
        # Devolvemos la tx aunque haya fallado el paso final de log, porque el dinero ya se envió
        return new_tx

@app.post("/contribute", response_model=schemas.Transaction, status_code=status.HTTP_201_CREATED, tags=["Transactions"])
async def contribute_to_group(
    req: schemas.ContributionRequest,
    idempotency_key: Optional[str] = Header(None, description="Clave única (UUID v4) para idempotencia"),
    db: Session = Depends(get_db)
):
    """
    Procesa un aporte desde una BDI (individual) a una BDG (grupal).
    SAGA: Debita usuario -> Acredita grupo -> Actualiza deuda interna.
    """
    if idempotency_key is None:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "Cabecera Idempotency-Key es requerida")

    sender_id = req.user_id
    group_id = req.group_id
    amount = req.amount

    # 1. Idempotencia (SQL)
    if existing_tx_id := check_idempotency(db, idempotency_key):
        logger.info(f"Aporte duplicado (Key: {idempotency_key}).")
        tx = get_transaction_by_id(db, existing_tx_id)
        if tx: return tx
        raise HTTPException(status.HTTP_500_INTERNAL_SERVER_ERROR, "Error de idempotencia")

    tx_id_sent = str(uuid.uuid4())     # ID para el usuario
    tx_id_received = str(uuid.uuid4()) # ID para el grupo
    now = datetime.now(timezone.utc)
    currency = "PEN"
    metadata_json = json.dumps({"contribution_to_group_id": group_id})
    
    # 2. Guardar PENDING (SQL)
    new_tx = models.Transaction(
        id=tx_id_sent,
        user_id=sender_id,
        source_wallet_type="BDI",
        source_wallet_id=str(sender_id),
        destination_wallet_type="BDG",
        destination_wallet_id=str(group_id),
        type="CONTRIBUTION_SENT",
        amount=amount,
        status="PENDING",
        metadata_info=metadata_json,
        created_at=now,
        updated_at=now
    )
    db.add(new_tx)
    db.commit()

    # 3. Ejecutar SAGA (Movimiento de dinero)
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            # A. Debitar BDI origen
            debit_res = await client.post(
                f"{BALANCE_SERVICE_URL}/balance/debit",
                json={"user_id": sender_id, "amount": amount}
            )
            debit_res.raise_for_status()

            try:
                # B. Acreditar BDG destino
                credit_res = await client.post(
                    f"{BALANCE_SERVICE_URL}/group_balance/credit",
                    json={"group_id": group_id, "amount": amount}
                )
                credit_res.raise_for_status() 

                # C. Actualizar Saldo Interno (Group Service)
                internal_res = await client.post(
                    f"{GROUP_SERVICE_URL}/groups/{group_id}/member_balance",
                    json={"user_id_to_update": sender_id, "amount": amount}
                )
                internal_res.raise_for_status()

            except Exception as credit_error:
                # ROLLBACK: Revertir el débito si falla el grupo
                logger.error(f"¡FALLO DE SAGA! Revertiendo débito {tx_id_sent}...")
                await client.post(
                    f"{BALANCE_SERVICE_URL}/balance/credit",
                    json={"user_id": sender_id, "amount": amount}
                )
                raise Exception(f"Fallo en paso de grupo: {credit_error}")

        # 4. Éxito: Guardar todo en SQL
        
        # Actualizar Tx del Usuario
        new_tx.status = "COMPLETED"
        new_tx.updated_at = datetime.now(timezone.utc)
        
        # Crear Tx del Grupo (Para que aparezca en el historial del grupo)
        # Nota: Usamos 'destination_wallet_id' para filtrar luego por grupo
        group_tx = models.Transaction(
            id=tx_id_received,
            user_id=None, # No tiene usuario dueño, es del grupo
            source_wallet_type="BDI",
            source_wallet_id=str(sender_id),
            destination_wallet_type="BDG",
            destination_wallet_id=str(group_id), # Clave para filtrar historial de grupo
            type="CONTRIBUTION_RECEIVED",
            amount=amount,
            status="COMPLETED",
            metadata_info=json.dumps({"from_user_id": sender_id}),
            created_at=now,
            updated_at=now
        )
        db.add(group_tx)
        
        # Guardar Idempotencia
        db.add(models.IdempotencyKey(key=idempotency_key, transaction_id=tx_id_sent))
        
        db.commit()
        db.refresh(new_tx)
        
        CONTRIBUTION_COUNT.inc()
        return new_tx

    except httpx.HTTPStatusError as e:
        status_code = e.response.status_code
        status_final = "FAILED_FUNDS" if status_code == 400 else "FAILED_BALANCE_SVC"
        
        new_tx.status = status_final
        db.commit()
        
        detail = e.response.json().get("detail", "Error en servicios internos")
        raise HTTPException(status_code=status_code, detail=detail)

    except Exception as e:
        new_tx.status = "FAILED_UNKNOWN"
        db.commit()
        logger.error(f"Error inesperado en aporte: {e}", exc_info=True)
        raise HTTPException(status.HTTP_500_INTERNAL_SERVER_ERROR, "Error interno procesando el aporte")

# --- Historial de Grupo (SQL) ---

@app.get("/transactions/group/{group_id}", response_model=List[schemas.Transaction], tags=["Ledger"])
async def get_group_transactions(
    group_id: int,
    db: Session = Depends(get_db)
):
    """Obtiene las transacciones asociadas a un grupo (BDG)."""
    # Buscamos transacciones donde el destino O el origen sea este grupo
    txs = db.query(models.Transaction).filter(
        (models.Transaction.destination_wallet_type == "BDG") & 
        (models.Transaction.destination_wallet_id == str(group_id))
    ).order_by(models.Transaction.created_at.desc()).limit(100).all()
    
    return txs

# --- Saga Interna: Retiro de Grupo ---
@app.post("/group-withdrawal", response_model=schemas.Transaction, status_code=status.HTTP_201_CREATED, tags=["Internal SAGA"])
async def execute_group_withdrawal(
    req: schemas.GroupWithdrawalRequest,
    db: Session = Depends(get_db)
):
    """
    EJECUTA la saga de retiro de grupo.
    Debita BDG -> Acredita BDI -> Actualiza Deuda.
    """
    tx_id_debit = str(uuid.uuid4())
    now = datetime.now(timezone.utc)
    
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            # A. Debitar Grupo
            await client.post(
                f"{BALANCE_SERVICE_URL}/group_balance/debit",
                json={"group_id": req.group_id, "amount": req.amount}
            )

            try:
                # B. Acreditar Miembro
                await client.post(
                    f"{BALANCE_SERVICE_URL}/balance/credit",
                    json={"user_id": req.member_user_id, "amount": req.amount}
                )
                
                # C. Actualizar Deuda (Resta)
                await client.post(
                    f"{GROUP_SERVICE_URL}/groups/{req.group_id}/member_balance",
                    json={"user_id_to_update": req.member_user_id, "amount": -req.amount}
                )
            except Exception:
                # Rollback: Devolver al grupo
                await client.post(
                    f"{BALANCE_SERVICE_URL}/group_balance/credit", 
                    json={"group_id": req.group_id, "amount": req.amount}
                )
                raise Exception("Fallo al acreditar al miembro, retiro revertido.")

        # Guardar Logs SQL
        # 1. Log para el Grupo (Salida)
        tx_group = models.Transaction(
            id=tx_id_debit,
            user_id=None,
            source_wallet_type="BDG",
            source_wallet_id=str(req.group_id),
            destination_wallet_type="BDI",
            destination_wallet_id=str(req.member_user_id),
            type="GROUP_WITHDRAWAL",
            amount=req.amount,
            status="COMPLETED",
            created_at=now,
            updated_at=now
        )
        db.add(tx_group)
        
        # 2. Log para el Usuario (Entrada)
        tx_user = models.Transaction(
            id=str(uuid.uuid4()),
            user_id=req.member_user_id,
            source_wallet_type="BDG",
            source_wallet_id=str(req.group_id),
            destination_wallet_type="BDI",
            destination_wallet_id=str(req.member_user_id),
            type="DEPOSIT", # O GROUP_DEPOSIT
            amount=req.amount,
            status="COMPLETED",
            created_at=now,
            updated_at=now
        )
        db.add(tx_user)
        
        db.commit()
        return tx_user # Devolvemos la parte del usuario

    except httpx.HTTPStatusError as e:
        raise HTTPException(e.response.status_code, detail=e.response.json().get('detail'))
    except Exception as e:
        logger.error(f"Error en retiro grupal: {e}")
        raise HTTPException(500, "Error procesando retiro grupal")



@app.get("/transactions/me", response_model=List[schemas.Transaction], tags=["Ledger"])
async def get_my_transactions(
    x_user_id: int = Header(..., alias="X-User-ID"),
    db: Session = Depends(get_db)
):
    """Obtiene el historial de transacciones del usuario (SQL)."""
    logger.info(f"Obteniendo historial SQL para user_id: {x_user_id}")
    
    # En SQL, buscamos transacciones donde el usuario sea el remitente O el destinatario
    # Usamos el operador OR (|) de SQLAlchemy
    txs = db.query(models.Transaction).filter(
        (models.Transaction.user_id == x_user_id) | 
        (models.Transaction.destination_wallet_id == str(x_user_id))
    ).order_by(models.Transaction.created_at.desc()).limit(50).all()
    
    return txs

# ... (después de 'get_my_transactions')

@app.get("/transactions/group/{group_id}", response_model=List[schemas.Transaction], tags=["Ledger"])
async def get_group_transactions(
    group_id: int,
    db: Session = Depends(get_db)
):
    """Obtiene las transacciones asociadas a un grupo (BDG)."""
    logger.info(f"Obteniendo historial SQL para group_id: {group_id}")
    
    # Buscamos transacciones donde el destino sea este grupo (Aportes)
    # O donde el origen sea este grupo (Retiros/Transferencias)
    txs = db.query(models.Transaction).filter(
        ((models.Transaction.destination_wallet_type == "BDG") & (models.Transaction.destination_wallet_id == str(group_id))) |
        ((models.Transaction.source_wallet_type == "BDG") & (models.Transaction.source_wallet_id == str(group_id)))
    ).order_by(models.Transaction.created_at.desc()).limit(100).all()
    
    return txs

@app.get("/analytics/daily_balance/{user_id}", tags=["Analytics"])
def get_daily_balance(user_id: int, db: Session = Depends(get_db)):
    """Calcula el balance diario de los últimos 30 días (SQL)."""
    logger.info(f"Calculando saldo diario SQL para user_id: {user_id}")
    
    # 1. Definir rango de fecha (últimos 30 días)
    thirty_days_ago = datetime.now() - timedelta(days=30)
    
    # 2. Consultar transacciones en ese rango (Origen o Destino = user_id)
    txs = db.query(models.Transaction).filter(
        (models.Transaction.user_id == user_id) | 
        (models.Transaction.destination_wallet_id == str(user_id))
    ).filter(
        models.Transaction.created_at >= thirty_days_ago
    ).order_by(models.Transaction.created_at.asc()).all()

    # 3. Procesar en memoria (Python)
    daily_balance = defaultdict(Decimal)
    running_balance = Decimal('0.0')

    # Tipos de transacción que suman (Entradas)
    INCOMING_TYPES = ["DEPOSIT", "P2P_RECEIVED", "LOAN_DISBURSEMENT", "CONTRIBUTION_RECEIVED", "GROUP_WITHDRAWAL"]
    # Tipos de transacción que restan (Salidas)
    OUTGOING_TYPES = ["P2P_SENT", "LOAN_PAYMENT", "TRANSFER", "CONTRIBUTION_SENT"]

    for tx in txs:
        date_key = tx.created_at.date()
        
        # Lógica de suma/resta
        if tx.type in INCOMING_TYPES:
            # Caso especial: Retiro de grupo. Solo suma si yo soy el destinatario final (BDI)
            if tx.type == "GROUP_WITHDRAWAL" and tx.destination_wallet_id != str(user_id):
                 continue # No es para mí
            running_balance += tx.amount
            
        elif tx.type in OUTGOING_TYPES:
            running_balance -= tx.amount
        
        daily_balance[date_key] = running_balance

    # 4. Rellenar días vacíos para que el gráfico no tenga huecos
    result = []
    balance = Decimal('0.0')
    now = datetime.now().date()
    
    for i in range(30, -1, -1):
        day = now - timedelta(days=i)
        # Si hubo movimiento ese día, actualizamos el balance acumulado
        if day in daily_balance:
            balance = daily_balance[day]
        
        result.append({
            "date": day.isoformat(),
            "balance": float(balance)
        })
        
    return result



    
@app.post("/transfer/p2p", response_model=schemas.Transaction, status_code=status.HTTP_201_CREATED, tags=["Transactions"])
async def transfer_p2p(
    req: schemas.P2PTransferRequest,
    idempotency_key: Optional[str] = Header(None, description="Clave única (UUID v4) para idempotencia"),
    db: Session = Depends(get_db)
):
    """
    Procesa una transferencia P2P (BDI -> BDI).
    SAGA: Debita remitente -> Acredita destinatario -> Registra historial doble.
    """
    if idempotency_key is None:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "Cabecera Idempotency-Key es requerida")

    sender_id = req.user_id
    amount = req.amount

    # 1. Idempotencia (SQL)
    if existing_tx_id := check_idempotency(db, idempotency_key):
        logger.info(f"Transferencia P2P duplicada: {existing_tx_id}")
        tx = get_transaction_by_id(db, existing_tx_id)
        if tx: return tx
        raise HTTPException(status.HTTP_500_INTERNAL_SERVER_ERROR, "Error de idempotencia")

    tx_id_debit = str(uuid.uuid4())  # ID para el remitente (Salida)
    tx_id_credit = str(uuid.uuid4()) # ID para el destinatario (Entrada)
    now = datetime.now(timezone.utc)
    currency = "PEN"
    recipient_id = None

    # 2. Resolver Destinatario (Auth Service)
    async with httpx.AsyncClient(timeout=10.0) as client:
        try:
            auth_res = await client.get(f"{AUTH_SERVICE_URL}/users/by-phone/{req.destination_phone_number}")
            auth_res.raise_for_status()
            recipient_id = int(auth_res.json()["id"])
            
            if recipient_id == sender_id:
                raise HTTPException(status.HTTP_400_BAD_REQUEST, "No puedes transferirte a ti mismo.")
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 404:
                raise HTTPException(404, "Destinatario no encontrado.")
            raise e

    # 3. Guardar PENDING (SQL)
    # Creamos la transacción del remitente (la principal)
    tx_sender = models.Transaction(
        id=tx_id_debit,
        user_id=sender_id,
        source_wallet_type="BDI",
        source_wallet_id=str(sender_id),
        destination_wallet_type="BDI",
        destination_wallet_id=str(recipient_id),
        type="P2P_SENT",
        amount=amount,
        currency=currency,
        status="PENDING",
        created_at=now,
        updated_at=now,
        metadata_info=json.dumps({"recipient_phone": req.destination_phone_number})
    )
    db.add(tx_sender)
    db.commit()

    # 4. Ejecutar SAGA (Balance)
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            # A. Verificar y Debitar Remitente
            debit_res = await client.post(
                f"{BALANCE_SERVICE_URL}/balance/debit", 
                json={"user_id": sender_id, "amount": amount}
            )
            debit_res.raise_for_status()

            # B. Acreditar Destinatario (Si falla, revertir A)
            try:
                credit_res = await client.post(
                    f"{BALANCE_SERVICE_URL}/balance/credit", 
                    json={"user_id": recipient_id, "amount": amount}
                )
                credit_res.raise_for_status()
            except Exception:
                # ROLLBACK: Devolver dinero al remitente
                await client.post(
                    f"{BALANCE_SERVICE_URL}/balance/credit", 
                    json={"user_id": sender_id, "amount": amount}
                )
                raise Exception("Fallo al acreditar al destinatario. Transacción revertida.")

        # 5. Éxito: Guardar TODO en SQL
        
        # Actualizar Tx del Remitente
        tx_sender.status = "COMPLETED"
        tx_sender.updated_at = datetime.now(timezone.utc)

        # Crear Tx del Destinatario (Para su historial)
        tx_recipient = models.Transaction(
            id=tx_id_credit,
            user_id=recipient_id,
            source_wallet_type="BDI",
            source_wallet_id=str(sender_id),
            destination_wallet_type="BDI",
            destination_wallet_id=str(recipient_id),
            type="P2P_RECEIVED",
            amount=amount,
            currency=currency,
            status="COMPLETED",
            created_at=now,
            updated_at=now,
            metadata_info=json.dumps({"sender_id": sender_id})
        )
        db.add(tx_recipient)

        # Guardar Idempotencia (Ligada al tx del remitente)
        db.add(models.IdempotencyKey(key=idempotency_key, transaction_id=tx_id_debit))

        db.commit()
        db.refresh(tx_sender)
        
        LEDGER_P2P_TRANSFERS_TOTAL.inc()
        return tx_sender

    except httpx.HTTPStatusError as e:
        status_code = e.response.status_code
        detail = e.response.json().get("detail", "Error en servicios internos")
        status_final = "FAILED_FUNDS" if status_code == 400 else "FAILED_BALANCE_SVC"
        
        # Actualizar estado a FAILED
        tx_sender.status = status_final
        tx_sender.updated_at = datetime.now(timezone.utc)
        db.commit()
        
        raise HTTPException(status_code=status_code, detail=detail)

    except Exception as e:
        tx_sender.status = "FAILED_UNKNOWN"
        tx_sender.updated_at = datetime.now(timezone.utc)
        db.commit()
        logger.error(f"Error inesperado en P2P: {e}", exc_info=True)
        raise HTTPException(status.HTTP_500_INTERNAL_SERVER_ERROR, "Error interno procesando la transferencia")


# ... (después de tu función 'get_daily_balance'...)

@app.post("/transfers/inbound", response_model=schemas.Transaction, status_code=status.HTTP_201_CREATED, tags=["Internal API"])
async def receive_inbound_transfer(
    req: schemas.InboundTransferRequest,
    db: Session = Depends(get_db)
):
    """
    Recibe una transferencia desde un servicio externo (ej. JavaBank).
    Busca al usuario por celular y le acredita el saldo.
    """
    logger.info(f"Recibiendo transferencia entrante para celular: {req.destination_phone_number}")
    
    # 1. Verificar si ya procesamos esta transacción externa (Idempotencia básica)
    # Buscamos en metadata si ya existe ese external_transaction_id
    # Nota: En SQL esto podría ser lento si hay muchos datos, para prod se recomienda una columna dedicada o tabla aparte.
    # Por ahora, asumimos que el 'happy path' es lo principal para la demo.
    
    tx_id = str(uuid.uuid4())
    now = datetime.now(timezone.utc)
    currency = "PEN"
    recipient_id = None

    async with httpx.AsyncClient(timeout=10.0) as client:
        try:
            # PASO 1: Resolver Destinatario (AUTH SERVICE)
            logger.debug(f"Tx {tx_id}: Buscando destinatario por celular: {req.destination_phone_number}")
            auth_res = await client.get(f"{AUTH_SERVICE_URL}/users/by-phone/{req.destination_phone_number}")
            
            if auth_res.status_code == 404:
                logger.warning(f"Celular {req.destination_phone_number} no encontrado.")
                raise HTTPException(status.HTTP_404_NOT_FOUND, "El número de celular no existe en Pixel Money.")
                
            auth_res.raise_for_status()
            recipient_id = int(auth_res.json()["id"])

            # PASO 2: Acreditar Destinatario (BALANCE SERVICE)
            logger.debug(f"Tx {tx_id}: Acreditando {req.amount} a user_id {recipient_id}")
            credit_res = await client.post(
                f"{BALANCE_SERVICE_URL}/balance/credit", 
                json={"user_id": recipient_id, "amount": req.amount}
            )
            credit_res.raise_for_status()
            logger.info(f"Tx {tx_id}: Crédito a {recipient_id} exitoso.")

        except httpx.HTTPStatusError as e:
            status_code = e.response.status_code
            detail = "Error en servicios internos."
            try: detail = e.response.json().get("detail", detail)
            except: pass
            
            logger.error(f"Fallo en transferencia entrante: {detail} (Status: {status_code})")
            raise HTTPException(status_code=status_code, detail=detail)
            
        except httpx.RequestError as e:
            logger.error(f"Error de red en transferencia entrante: {e}", exc_info=True)
            raise HTTPException(status.HTTP_503_SERVICE_UNAVAILABLE, "Error de comunicación entre servicios.")

    # PASO 3: Escribir en SQL (¡ÉXITO!)
    try:
        status_final = "COMPLETED"
        decimal_amount = Decimal(str(req.amount))
        
        metadata = {
            "external_tx_id": req.external_transaction_id, 
            "sender_bank": "EXTERNAL_BANK" # Podrías recibir el nombre del banco en el futuro
        }
        
        new_tx = models.Transaction(
            id=tx_id,
            user_id=recipient_id,
            source_wallet_type="EXTERNAL_BANK",
            source_wallet_id="JavaBank", # O el nombre del banco origen
            destination_wallet_type="BDI",
            destination_wallet_id=str(recipient_id),
            type="DEPOSIT", # Lo registramos como depósito
            amount=decimal_amount,
            currency=currency,
            status=status_final,
            metadata_info=json.dumps(metadata),
            created_at=now,
            updated_at=now
        )
        
        db.add(new_tx)
        db.commit()
        db.refresh(new_tx)

        DEPOSIT_COUNT.inc() # Usamos la misma métrica de depósito
        
        return new_tx

    except Exception as e:
        db.rollback()
        logger.critical(f"¡FALLO CRÍTICO POST-SAGA! Tx {tx_id} (Entrante) tuvo éxito en Balance pero SQL falló: {e}", exc_info=True)
        # Aunque falle el registro, el dinero ya está. Devolvemos 500 pero el usuario tiene la plata.
        raise HTTPException(status.HTTP_500_INTERNAL_SERVER_ERROR, "La transferencia se completó pero falló al registrarse.")

# ... (después de 'receive_inbound_transfer')

@app.post("/group-withdrawal", response_model=schemas.Transaction, status_code=status.HTTP_201_CREATED, tags=["Internal SAGA"])
async def execute_group_withdrawal(
    req: schemas.GroupWithdrawalRequest,
    db: Session = Depends(get_db)
):
    """
    EJECUTA la saga de retiro de grupo.
    Debita BDG -> Acredita BDI -> Actualiza Deuda Interna.
    """
    # URLs de servicios (deben estar definidas al inicio del archivo)
    if not BALANCE_SERVICE_URL or not GROUP_SERVICE_URL:
         logger.error("URLs de servicio internas no configuradas en Ledger!")
         raise HTTPException(status.HTTP_500_INTERNAL_SERVER_ERROR, "Error de configuración interna.")

    tx_id_debit = str(uuid.uuid4()) # ID para el grupo (Salida)
    tx_id_credit = str(uuid.uuid4()) # ID para el miembro (Entrada)
    now = datetime.now(timezone.utc)
    currency = "PEN"
    
    # Metadata para rastrear qué solicitud de retiro originó esto
    metadata_json = json.dumps({"withdrawal_request_id": req.request_id})
    
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            # A. Debitar Grupo (BDG)
            # Si falla aquí (400), es porque el grupo no tiene fondos -> abortamos sin rollback
            debit_res = await client.post(
                f"{BALANCE_SERVICE_URL}/group_balance/debit",
                json={"group_id": req.group_id, "amount": req.amount}
            )
            debit_res.raise_for_status()

            try:
                # B. Acreditar Miembro (BDI)
                credit_res = await client.post(
                    f"{BALANCE_SERVICE_URL}/balance/credit",
                    json={"user_id": req.member_user_id, "amount": req.amount}
                )
                credit_res.raise_for_status()
                
                # C. Actualizar Deuda Interna (Group Service)
                # Restamos el monto (generamos deuda)
                await client.post(
                    f"{GROUP_SERVICE_URL}/groups/{req.group_id}/member_balance",
                    json={"user_id_to_update": req.member_user_id, "amount": -req.amount}
                )
                
            except Exception as e:
                # ROLLBACK: Si falla acreditar al miembro o actualizar la deuda, devolvemos el dinero al grupo
                logger.error(f"Fallo en paso B/C de retiro grupal. Revertiendo débito al grupo... Error: {e}")
                await client.post(
                    f"{BALANCE_SERVICE_URL}/group_balance/credit", 
                    json={"group_id": req.group_id, "amount": req.amount}
                )
                raise Exception("Fallo al acreditar al miembro, retiro revertido.")

        # --- GUARDAR EN SQL ---
        
        # 1. Log para el Grupo (Salida - GROUP_WITHDRAWAL)
        tx_group = models.Transaction(
            id=tx_id_debit,
            user_id=None, # Es una cuenta de grupo, no de usuario personal
            source_wallet_type="BDG",
            source_wallet_id=str(req.group_id),
            destination_wallet_type="BDI",
            destination_wallet_id=str(req.member_user_id),
            type="GROUP_WITHDRAWAL",
            amount=req.amount,
            currency=currency,
            status="COMPLETED",
            metadata_info=metadata_json,
            created_at=now,
            updated_at=now
        )
        db.add(tx_group)
        
        # 2. Log para el Usuario (Entrada - DEPOSIT)
        # Esto es para que el usuario vea "+50.00" en su historial personal
        tx_user = models.Transaction(
            id=tx_id_credit,
            user_id=req.member_user_id,
            source_wallet_type="BDG",
            source_wallet_id=str(req.group_id),
            destination_wallet_type="BDI",
            destination_wallet_id=str(req.member_user_id),
            type="DEPOSIT", 
            amount=req.amount,
            currency=currency,
            status="COMPLETED",
            metadata_info=metadata_json,
            created_at=now,
            updated_at=now
        )
        db.add(tx_user)
        
        db.commit()
        
        LEDGER_WITHDRAWALS_TOTAL.inc() # Métrica
        
        # Devolvemos la transacción del usuario para que el front vea el éxito
        db.refresh(tx_user)
        return tx_user 

    except httpx.HTTPStatusError as e:
        # Errores de lógica (fondos insuficientes, etc.)
        detail = e.response.json().get('detail', e.response.text)
        logger.warning(f"Retiro grupal fallido (HTTP): {detail}")
        raise HTTPException(e.response.status_code, detail=detail)
        
    except Exception as e:
        # Errores inesperados
        db.rollback()
        logger.error(f"Error crítico en retiro grupal: {e}", exc_info=True)
        raise HTTPException(status.HTTP_500_INTERNAL_SERVER_ERROR, "Error procesando retiro grupal")

# --- SAGA DE PRÉSTAMOS (Loans) ---

@app.post("/loans/disbursement", response_model=schemas.Transaction, status_code=status.HTTP_201_CREATED, tags=["Internal SAGA"])
async def process_loan_disbursement(
    req: schemas.LoanEventRequest,
    db: Session = Depends(get_db)
):
    """
    SAGA: Desembolso de Préstamo via Balance Service.
    1. Ledger recibe orden.
    2. Ledger llama a Balance Service (/credit).
    3. Ledger registra LOAN_DISBURSEMENT en SQL.
    """
    tx_id = str(uuid.uuid4())
    now = datetime.now(timezone.utc)
    currency = "PEN"
    
    # Metadata para rastrear el préstamo
    metadata = {"loan_id": req.loan_id, "description": "Préstamo aprobado"}
    
    # 1. Mover el dinero (Llamar a Balance Service)
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            # Acreditamos la cuenta del usuario (BDI)
            response = await client.post(
                f"{BALANCE_SERVICE_URL}/balance/credit",
                json={"user_id": req.user_id, "amount": req.amount}
            )
            response.raise_for_status()
            
    except httpx.HTTPStatusError as e:
        # Si el balance falla, no guardamos nada en el ledger
        logger.error(f"Fallo al desembolsar en Balance: {e.response.text}")
        raise HTTPException(status.HTTP_503_SERVICE_UNAVAILABLE, f"Error en Balance Service: {e.response.json().get('detail')}")
    except Exception as e:
        logger.error(f"Error de conexión en desembolso: {e}")
        raise HTTPException(status.HTTP_500_INTERNAL_SERVER_ERROR, "Error interno al abonar el préstamo.")

    # 2. Registrar en SQL (Solo si el paso 1 funcionó)
    try:
        new_tx = models.Transaction(
            id=tx_id,
            user_id=req.user_id,
            # Usamos 'PIXEL_BANK' como origen para indicar que el dinero viene del banco
            source_wallet_type="PIXEL_BANK", 
            source_wallet_id="MAIN_VAULT",
            destination_wallet_type="BDI",
            destination_wallet_id=str(req.user_id),
            type="LOAN_DISBURSEMENT",
            amount=req.amount,
            currency=currency,
            status="COMPLETED",
            metadata_info=json.dumps(metadata),
            created_at=now,
            updated_at=now
        )
        
        db.add(new_tx)
        db.commit()
        db.refresh(new_tx)
        
        return new_tx

    except Exception as e:
        db.rollback()
        # Esto es un error crítico: El usuario tiene el dinero pero no hay registro en el Ledger.
        # En un sistema real, esto dispararía una alerta a los desarrolladores.
        logger.critical(f"¡FALLO CRÍTICO! Dinero entregado pero error SQL en Loan Disbursement: {e}", exc_info=True)
        raise HTTPException(status.HTTP_500_INTERNAL_SERVER_ERROR, "Préstamo entregado pero error al registrar historial.")


@app.post("/loans/payment", response_model=schemas.Transaction, tags=["Internal SAGA"])
async def process_loan_payment(
    req: schemas.LoanEventRequest,
    db: Session = Depends(get_db)
):
    """
    SAGA: Pago de Préstamo via Balance Service.
    1. Ledger recibe orden.
    2. Ledger llama a Balance Service (/debit) para cobrar.
    3. Ledger registra LOAN_PAYMENT en SQL.
    """
    tx_id = str(uuid.uuid4())
    now = datetime.now(timezone.utc)
    currency = "PEN"
    status_final = "COMPLETED"
    
    metadata = {"loan_id": req.loan_id, "description": "Pago de préstamo"}
    
    # 1. Cobrar el dinero (Llamar a Balance Service)
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            # Debitamos la cuenta del usuario (BDI)
            response = await client.post(
                f"{BALANCE_SERVICE_URL}/balance/debit",
                json={"user_id": req.user_id, "amount": req.amount}
            )
            response.raise_for_status() # Esto lanzará error 400 si no hay fondos

    except httpx.HTTPStatusError as e:
        detail = f"Fallo el cobro: {e.response.json().get('detail', 'Error desconocido')}"
        logger.warning(f"Intento de pago fallido: {detail}")
        raise HTTPException(status_code=e.response.status_code, detail=detail)
    except Exception as e:
        logger.error(f"Fallo al cobrar préstamo en Balance Service: {e}")
        raise HTTPException(status.HTTP_500_INTERNAL_SERVER_ERROR, "Error al procesar el cobro del préstamo.")

    # 2. Registrar en SQL (Solo si el cobro fue exitoso)
    try:
        new_tx = models.Transaction(
            id=tx_id,
            user_id=req.user_id,
            # Origen: Usuario (BDI) -> Destino: Banco (PIXEL_BANK)
            source_wallet_type="BDI",
            source_wallet_id=str(req.user_id),
            destination_wallet_type="PIXEL_BANK", 
            destination_wallet_id="MAIN_VAULT",
            type="LOAN_PAYMENT",
            amount=req.amount,
            currency=currency,
            status=status_final,
            metadata_info=json.dumps(metadata),
            created_at=now,
            updated_at=now
        )
        
        db.add(new_tx)
        db.commit()
        db.refresh(new_tx)
        
        return new_tx

    except Exception as e:
        db.rollback()
        # Error crítico: Se cobró el dinero pero no hay registro
        logger.critical(f"¡FALLO CRÍTICO! Dinero cobrado pero error SQL en Loan Payment: {e}", exc_info=True)
        raise HTTPException(status.HTTP_500_INTERNAL_SERVER_ERROR, "Préstamo cobrado pero error al registrar historial.")


@app.get("/health", tags=["Monitoring"])
def health_check(db: Session = Depends(get_db)):
    """
    Verifica la salud básica del servicio y la conexión a la Base de Datos (SQL).
    Usado por Docker Healthcheck.
    """
    db_status = "ok"
    try:
        # Ejecutamos una consulta trivial para verificar que la DB responde
        db.execute(text("SELECT 1")) # Usamos 'text' para query cruda en SQLAlchemy
    except Exception as e:
        logger.error(f"Health check fallido - Error de SQL: {e}", exc_info=True)
        db_status = "error"
        # Devolvemos 503 para que el healthcheck de Docker falle y reinicie el contenedor si es necesario
        raise HTTPException(status_code=503, detail=f"Database (SQL) connection error: {e}")

    return {"status": "ok", "service": "ledger_service", "database": db_status}

@app.get("/metrics", tags=["Monitoring"])
def metrics():
    """Expone métricas de la aplicación para Prometheus."""
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)
