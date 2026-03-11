"""
WhatsApp Service — Integração WPPConnect para envio automatizado
ProspectAdv
"""

import os
import json
import time
import logging
import requests

logger = logging.getLogger("ProspectAdv.WhatsApp")

# Configuração WPPConnect
WPPCONNECT_URL = os.getenv("WPPCONNECT_URL", "http://localhost:21465")
WPPCONNECT_SECRET = os.getenv("WPPCONNECT_SECRET_KEY", "")
SESSION_NAME = "prospectadv"

# Cache do token
_token_cache = {"token": None, "expires": 0}


def _get_headers():
    """Retorna headers com token de autenticação."""
    return {
        "Content-Type": "application/json",
        "Accept": "application/json",
        "Authorization": f"Bearer {_token_cache.get('token', '')}",
    }


def gerar_token():
    """Gera token de autenticação no WPPConnect."""
    try:
        response = requests.post(
            f"{WPPCONNECT_URL}/api/{SESSION_NAME}/{WPPCONNECT_SECRET}/generate-token",
            timeout=10,
        )
        data = response.json()

        if data.get("status") == "success":
            _token_cache["token"] = data.get("token")
            _token_cache["expires"] = time.time() + 3600
            logger.info("Token WPPConnect gerado com sucesso")
            return True
        else:
            logger.error(f"Erro ao gerar token: {data}")
            return False
    except requests.exceptions.ConnectionError:
        logger.error(f"WPPConnect não disponível em {WPPCONNECT_URL}")
        return False
    except Exception as e:
        logger.error(f"Erro ao gerar token: {e}")
        return False


def _ensure_token():
    """Garante que o token está válido."""
    if not _token_cache.get("token") or time.time() > _token_cache.get("expires", 0):
        if not gerar_token():
            raise ConnectionError("Não foi possível autenticar no WPPConnect")


def iniciar_sessao():
    """Inicia sessão do WhatsApp no WPPConnect."""
    _ensure_token()

    try:
        response = requests.post(
            f"{WPPCONNECT_URL}/api/{SESSION_NAME}/start-session",
            headers=_get_headers(),
            json={
                "webhook": None,
                "waitQrCode": True,
            },
            timeout=30,
        )
        data = response.json()
        logger.info(f"Sessão iniciada: {data.get('status')}")
        return data
    except Exception as e:
        logger.error(f"Erro ao iniciar sessão: {e}")
        return {"status": "error", "message": str(e)}


def obter_qrcode():
    """Obtém QR Code para conectar o WhatsApp."""
    _ensure_token()

    try:
        response = requests.get(
            f"{WPPCONNECT_URL}/api/{SESSION_NAME}/qrcode-session",
            headers=_get_headers(),
            timeout=15,
        )

        if response.headers.get("Content-Type", "").startswith("image"):
            return {"status": "qrcode", "qrcode": response.content}

        data = response.json()
        return data
    except Exception as e:
        logger.error(f"Erro ao obter QR Code: {e}")
        return {"status": "error", "message": str(e)}


def verificar_status():
    """Verifica status da sessão WhatsApp."""
    _ensure_token()

    try:
        response = requests.get(
            f"{WPPCONNECT_URL}/api/{SESSION_NAME}/check-connection-session",
            headers=_get_headers(),
            timeout=10,
        )
        data = response.json()
        return data
    except requests.exceptions.ConnectionError:
        return {"status": "disconnected", "message": "WPPConnect não disponível"}
    except Exception as e:
        logger.error(f"Erro ao verificar status: {e}")
        return {"status": "error", "message": str(e)}


def _formatar_numero(numero):
    """Formata número para padrão WhatsApp (55DDDNUMERO@c.us)."""
    # Remover caracteres não numéricos
    num = "".join(filter(str.isdigit, str(numero)))

    # Adicionar código do Brasil se necessário
    if not num.startswith("55"):
        num = "55" + num

    # Remover dígito extra do DDD se tiver 13 dígitos
    if len(num) == 13:
        pass  # 55 + DDD(2) + 9 + numero(8) = 13 ✓
    elif len(num) == 12:
        # Adicionar 9 depois do DDD
        num = num[:4] + "9" + num[4:]

    return f"{num}@c.us"


def enviar_mensagem(numero, mensagem):
    """
    Envia mensagem de texto via WhatsApp.
    
    Args:
        numero: Número do destinatário (com ou sem formatação)
        mensagem: Texto da mensagem
    
    Returns:
        True se enviado com sucesso, False caso contrário
    """
    _ensure_token()

    numero_formatado = _formatar_numero(numero)

    try:
        response = requests.post(
            f"{WPPCONNECT_URL}/api/{SESSION_NAME}/send-message",
            headers=_get_headers(),
            json={
                "phone": numero_formatado,
                "message": mensagem,
                "isGroup": False,
            },
            timeout=15,
        )

        data = response.json()

        if data.get("status") == "success" or response.status_code == 200:
            logger.info(f"WhatsApp enviado para {numero_formatado}")
            return True
        else:
            logger.warning(f"Falha ao enviar WhatsApp: {data}")
            return False

    except requests.exceptions.ConnectionError:
        logger.error("WPPConnect não disponível")
        return False
    except Exception as e:
        logger.error(f"Erro ao enviar WhatsApp: {e}")
        return False


def enviar_mensagem_com_link(numero, mensagem, link_url, link_titulo=""):
    """Envia mensagem com preview de link."""
    _ensure_token()

    numero_formatado = _formatar_numero(numero)

    try:
        response = requests.post(
            f"{WPPCONNECT_URL}/api/{SESSION_NAME}/send-link-preview",
            headers=_get_headers(),
            json={
                "phone": numero_formatado,
                "url": link_url,
                "caption": mensagem,
            },
            timeout=15,
        )

        data = response.json()
        return data.get("status") == "success" or response.status_code == 200

    except Exception as e:
        logger.error(f"Erro ao enviar link: {e}")
        return False


def verificar_numero_existe(numero):
    """Verifica se um número está registrado no WhatsApp."""
    _ensure_token()

    numero_formatado = _formatar_numero(numero)

    try:
        response = requests.get(
            f"{WPPCONNECT_URL}/api/{SESSION_NAME}/check-number-status/{numero_formatado}",
            headers=_get_headers(),
            timeout=10,
        )

        data = response.json()
        return data.get("numberExists", False)

    except Exception as e:
        logger.error(f"Erro ao verificar número: {e}")
        return None


def obter_mensagens_recebidas(limite=50):
    """Obtém mensagens recebidas recentes."""
    _ensure_token()

    try:
        response = requests.get(
            f"{WPPCONNECT_URL}/api/{SESSION_NAME}/all-unread-messages",
            headers=_get_headers(),
            timeout=15,
        )

        data = response.json()
        mensagens = []

        for msg in data.get("response", []):
            mensagens.append({
                "de": msg.get("from", "").replace("@c.us", ""),
                "mensagem": msg.get("body", ""),
                "timestamp": msg.get("timestamp", 0),
                "tipo": msg.get("type", "chat"),
                "nome": msg.get("sender", {}).get("pushname", ""),
            })

        return mensagens[:limite]

    except Exception as e:
        logger.error(f"Erro ao obter mensagens: {e}")
        return []


def desconectar():
    """Desconecta sessão do WhatsApp."""
    _ensure_token()

    try:
        response = requests.post(
            f"{WPPCONNECT_URL}/api/{SESSION_NAME}/logout-session",
            headers=_get_headers(),
            timeout=10,
        )
        data = response.json()
        logger.info(f"Sessão WhatsApp encerrada: {data.get('status')}")
        return data
    except Exception as e:
        logger.error(f"Erro ao desconectar: {e}")
        return {"status": "error", "message": str(e)}


def get_status_resumo():
    """Retorna resumo do status do WhatsApp para o dashboard."""
    status = verificar_status()

    connected = status.get("status") in ("CONNECTED", "isLogged")

    return {
        "conectado": connected,
        "status": status.get("status", "unknown"),
        "mensagem": status.get("message", ""),
        "url_wppconnect": WPPCONNECT_URL,
        "sessao": SESSION_NAME,
    }
