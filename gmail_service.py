"""
Gmail Service — OAuth2 multi-conta para envio de emails
ProspectAdv
"""

import os
import json
import base64
import pickle
import logging
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow, Flow
from googleapiclient.discovery import build

logger = logging.getLogger("ProspectAdv.Gmail")

SCOPES = [
    "https://www.googleapis.com/auth/gmail.send",
    "https://www.googleapis.com/auth/gmail.readonly",
]

CREDENTIALS_FILE = "credentials.json"
REDIRECT_URI = os.getenv("GMAIL_REDIRECT_URI", "http://localhost:5000/oauth2callback")

# Armazena flows em andamento
_flows_pendentes = {}


def _token_path(conta_id):
    """Retorna caminho do token para uma conta."""
    return f"token_{conta_id}.json"


def _carregar_credenciais(conta_id):
    """Carrega credenciais salvas para uma conta."""
    path = _token_path(conta_id)
    pickle_path = f"token_{conta_id}.pickle"

    creds = None

    # Tentar JSON primeiro
    if os.path.exists(path):
        try:
            creds = Credentials.from_authorized_user_file(path, SCOPES)
        except Exception as e:
            logger.warning(f"Erro ao carregar token JSON {path}: {e}")

    # Tentar pickle como fallback
    if not creds and os.path.exists(pickle_path):
        try:
            with open(pickle_path, "rb") as f:
                creds = pickle.load(f)
        except Exception as e:
            logger.warning(f"Erro ao carregar token pickle {pickle_path}: {e}")

    if creds and creds.expired and creds.refresh_token:
        try:
            creds.refresh(Request())
            _salvar_credenciais(conta_id, creds)
        except Exception as e:
            logger.error(f"Erro ao atualizar token: {e}")
            return None

    return creds


def _salvar_credenciais(conta_id, creds):
    """Salva credenciais no formato JSON."""
    path = _token_path(conta_id)
    try:
        with open(path, "w") as f:
            f.write(creds.to_json())
    except Exception as e:
        logger.error(f"Erro ao salvar token: {e}")


def iniciar_oauth(conta_id=1):
    """Inicia fluxo OAuth2 e retorna URL de autorização."""
    if not os.path.exists(CREDENTIALS_FILE):
        raise FileNotFoundError(
            f"Arquivo {CREDENTIALS_FILE} não encontrado. "
            "Baixe-o no Google Cloud Console (APIs & Services > Credentials)."
        )

    flow = Flow.from_client_secrets_file(
        CREDENTIALS_FILE,
        scopes=SCOPES,
        redirect_uri=REDIRECT_URI,
    )

    auth_url, state = flow.authorization_url(
        access_type="offline",
        include_granted_scopes="true",
        prompt="consent",
    )

    _flows_pendentes[state] = {"flow": flow, "conta_id": conta_id}

    return auth_url


def processar_callback(url_completa):
    """Processa callback do OAuth2 e salva token."""
    from urllib.parse import urlparse, parse_qs

    parsed = urlparse(url_completa)
    params = parse_qs(parsed.query)

    state = params.get("state", [None])[0]
    if not state or state not in _flows_pendentes:
        raise ValueError("State inválido no callback OAuth2.")

    flow_info = _flows_pendentes.pop(state)
    flow = flow_info["flow"]
    conta_id = flow_info["conta_id"]

    flow.fetch_token(authorization_response=url_completa)
    creds = flow.credentials

    _salvar_credenciais(conta_id, creds)

    # Obter email da conta
    service = build("gmail", "v1", credentials=creds)
    profile = service.users().getProfile(userId="me").execute()
    email = profile.get("emailAddress", "")

    logger.info(f"Gmail conectado: {email} (conta {conta_id})")

    return {"email": email, "conta_id": conta_id}


def _get_service(conta_id=1):
    """Retorna serviço Gmail autenticado."""
    creds = _carregar_credenciais(conta_id)
    if not creds or not creds.valid:
        raise RuntimeError(
            f"Conta Gmail {conta_id} não autenticada. "
            "Acesse /gmail/conectar/{conta_id} para autenticar."
        )
    return build("gmail", "v1", credentials=creds)


def enviar_email(destinatario, assunto, corpo_html, corpo_texto=None, conta_id=1):
    """
    Envia email via Gmail API.
    
    Args:
        destinatario: Email do destinatário
        assunto: Assunto do email
        corpo_html: Corpo em HTML
        corpo_texto: Corpo em texto plano (opcional)
        conta_id: ID da conta Gmail
    
    Returns:
        dict com 'id' e 'threadId' da mensagem enviada
    """
    service = _get_service(conta_id)

    msg = MIMEMultipart("alternative")
    msg["to"] = destinatario
    msg["subject"] = assunto

    if corpo_texto:
        msg.attach(MIMEText(corpo_texto, "plain", "utf-8"))

    msg.attach(MIMEText(corpo_html, "html", "utf-8"))

    raw = base64.urlsafe_b64encode(msg.as_bytes()).decode("utf-8")

    result = service.users().messages().send(
        userId="me",
        body={"raw": raw},
    ).execute()

    logger.info(f"Email enviado para {destinatario} — ID: {result.get('id')}")

    return result


def enviar_email_reply(destinatario, assunto, corpo_html, thread_id, message_id, corpo_texto=None, conta_id=1):
    """Envia resposta em uma thread existente."""
    service = _get_service(conta_id)

    msg = MIMEMultipart("alternative")
    msg["to"] = destinatario
    msg["subject"] = assunto
    msg["In-Reply-To"] = message_id
    msg["References"] = message_id

    if corpo_texto:
        msg.attach(MIMEText(corpo_texto, "plain", "utf-8"))

    msg.attach(MIMEText(corpo_html, "html", "utf-8"))

    raw = base64.urlsafe_b64encode(msg.as_bytes()).decode("utf-8")

    result = service.users().messages().send(
        userId="me",
        body={"raw": raw, "threadId": thread_id},
    ).execute()

    logger.info(f"Reply enviado para {destinatario} na thread {thread_id}")

    return result


def listar_contas_conectadas():
    """Lista todas as contas Gmail conectadas."""
    contas = []
    for i in range(1, 6):  # Suporta até 5 contas
        path = _token_path(i)
        if os.path.exists(path):
            try:
                creds = _carregar_credenciais(i)
                if creds and creds.valid:
                    service = build("gmail", "v1", credentials=creds)
                    profile = service.users().getProfile(userId="me").execute()
                    contas.append({
                        "conta_id": i,
                        "email": profile.get("emailAddress"),
                        "ativo": True,
                    })
            except Exception as e:
                logger.warning(f"Conta {i} com erro: {e}")
                contas.append({"conta_id": i, "email": "Erro", "ativo": False})
    return contas


def verificar_respostas(conta_id=1, horas=24):
    """Verifica respostas recebidas nas últimas N horas."""
    try:
        service = _get_service(conta_id)
    except RuntimeError:
        return []

    import time
    after = int(time.time()) - (horas * 3600)

    try:
        results = service.users().messages().list(
            userId="me",
            q=f"is:inbox after:{after}",
            maxResults=50,
        ).execute()

        messages = results.get("messages", [])
        respostas = []

        for msg_info in messages:
            msg = service.users().messages().get(
                userId="me",
                id=msg_info["id"],
                format="metadata",
                metadataHeaders=["From", "Subject", "Date"],
            ).execute()

            headers = {h["name"]: h["value"] for h in msg.get("payload", {}).get("headers", [])}
            respostas.append({
                "id": msg_info["id"],
                "thread_id": msg.get("threadId"),
                "de": headers.get("From", ""),
                "assunto": headers.get("Subject", ""),
                "data": headers.get("Date", ""),
                "snippet": msg.get("snippet", ""),
            })

        return respostas

    except Exception as e:
        logger.error(f"Erro ao verificar respostas: {e}")
        return []
