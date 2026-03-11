"""
Anti-Detection Module — ProspectAdv Pipeline
Tecnicas anti-bloqueio para scraping em escala.

Principios:
- Nunca mais de 5 abas headless no mesmo IP
- Accept-Language deve bater com geolocalizacao do proxy
- Ruido entre queries (buscar termos aleatorios)
- Rotacao de User-Agent realista
- Fingerprint de navegador consistente
- Delays humanos (nao-uniformes)
"""

import random
import time
import logging
from datetime import datetime

logger = logging.getLogger("ProspectAdv.AntiDetect")

# ============================================================
# 1. USER-AGENTS REAIS (Chrome 120-126, Firefox 123-125, Edge 122-124)
# ============================================================

CHROME_UAS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_4) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
]

FIREFOX_UAS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:125.0) Gecko/20100101 Firefox/125.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:124.0) Gecko/20100101 Firefox/124.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14.4; rv:125.0) Gecko/20100101 Firefox/125.0",
    "Mozilla/5.0 (X11; Linux x86_64; rv:125.0) Gecko/20100101 Firefox/125.0",
]

EDGE_UAS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36 Edg/124.0.0.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36 Edg/122.0.0.0",
]

ALL_UAS = CHROME_UAS + FIREFOX_UAS + EDGE_UAS

# ============================================================
# 2. HEADERS GEOLOCALIZADOS (BR)
# ============================================================

ACCEPT_LANGUAGES_BR = [
    "pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7",
    "pt-BR,pt;q=0.9,en;q=0.8",
    "pt-BR,pt;q=0.95,en-US;q=0.5,en;q=0.3",
    "pt-BR,pt;q=0.9",
]

ACCEPT_ENCODINGS = [
    "gzip, deflate, br",
    "gzip, deflate, br, zstd",
    "gzip, deflate",
]

SEC_CH_UA_PLATFORMS = [
    '"Windows"',
    '"macOS"',
    '"Linux"',
]

# ============================================================
# 3. QUERIES DE RUIDO (entropia alta, evita padrao de scraping)
# ============================================================

NOISE_QUERIES = [
    "clima hoje", "horoscopo sagitario", "resultado lotofacil",
    "cotacao dolar", "receita bolo de chocolate", "horario dos correios",
    "tabela fipe", "campeonato brasileiro", "previsao do tempo curitiba",
    "noticias de hoje", "valor do bitcoin", "tabela irpf 2026",
    "como fazer arroz", "filmes em cartaz", "agenda feriados 2026",
    "classificacao serie a", "resultado mega sena", "dolar hoje",
    "tabela campeonato paranaense", "farmacia aberta perto de mim",
    "passagem aerea", "ingresso cinema", "supermercado oferta",
    "receita pao caseiro", "emprego curitiba", "imovel aluguel",
    "telefone samu", "cep curitiba centro", "mapa parana",
]


# ============================================================
# 4. CLASSE PRINCIPAL: SessionManager
# ============================================================

class SessionManager:
    """
    Gerencia sessoes de scraping com anti-deteccao.
    Controla rate-limit, rotacao de UA, delays humanos e ruido.
    """

    def __init__(self, max_concurrent=5, base_delay=2.0, noise_ratio=0.15):
        """
        Args:
            max_concurrent: Max abas/threads no mesmo IP (recomendado <= 5)
            base_delay: Delay base entre requests (segundos)
            noise_ratio: Proporcao de queries de ruido (0.15 = 15%)
        """
        self.max_concurrent = max_concurrent
        self.base_delay = base_delay
        self.noise_ratio = noise_ratio
        self.request_count = 0
        self.noise_count = 0
        self.start_time = datetime.now()
        self._current_ua = random.choice(ALL_UAS)
        self._ua_change_interval = random.randint(15, 30)
        self._last_query_time = 0

    def get_headers(self, referer=None):
        """Retorna headers realistas com geolocalizacao BR."""
        # Rotacionar UA periodicamente (nao a cada request)
        self.request_count += 1
        if self.request_count % self._ua_change_interval == 0:
            self._current_ua = random.choice(ALL_UAS)
            self._ua_change_interval = random.randint(15, 30)

        ua = self._current_ua

        headers = {
            "User-Agent": ua,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
            "Accept-Language": random.choice(ACCEPT_LANGUAGES_BR),
            "Accept-Encoding": random.choice(ACCEPT_ENCODINGS),
            "DNT": "1",
            "Connection": "keep-alive",
            "Upgrade-Insecure-Requests": "1",
        }

        # Sec-CH-UA headers (Chrome/Edge only)
        if "Chrome" in ua or "Edg" in ua:
            headers["Sec-Fetch-Dest"] = "document"
            headers["Sec-Fetch-Mode"] = "navigate"
            headers["Sec-Fetch-Site"] = "none" if not referer else "same-origin"
            headers["Sec-Fetch-User"] = "?1"
            headers["Sec-CH-UA-Platform"] = random.choice(SEC_CH_UA_PLATFORMS)

            # Extrair versao do Chrome do UA
            import re
            chrome_ver = re.search(r"Chrome/(\d+)", ua)
            if chrome_ver:
                ver = chrome_ver.group(1)
                headers["Sec-CH-UA"] = f'"Chromium";v="{ver}", "Google Chrome";v="{ver}", "Not.A/Brand";v="8"'
                headers["Sec-CH-UA-Mobile"] = "?0"

        if referer:
            headers["Referer"] = referer

        return headers

    def human_delay(self, min_factor=0.5, max_factor=2.5):
        """Delay nao-uniforme que simula comportamento humano."""
        # Distribuicao log-normal — mais delays curtos, poucos longos
        delay = self.base_delay * random.uniform(min_factor, max_factor)

        # Adicionar micro-variacao (humanos nao tem precisao de ms)
        delay += random.uniform(0.1, 0.8)

        # Delay extra a cada ~20 requests (simula pausa para ler)
        if self.request_count % random.randint(18, 25) == 0:
            delay += random.uniform(5, 15)
            logger.debug(f"Pausa longa: {delay:.1f}s (simulando leitura)")

        time.sleep(delay)
        return delay

    def should_noise(self):
        """Decide se deve inserir query de ruido."""
        return random.random() < self.noise_ratio

    def get_noise_query(self):
        """Retorna query de ruido aleatoria."""
        self.noise_count += 1
        return random.choice(NOISE_QUERIES)

    def execute_noise(self, session=None):
        """Executa query de ruido no Google (mantem entropia alta)."""
        import requests as req
        query = self.get_noise_query()
        logger.debug(f"Ruido #{self.noise_count}: {query}")

        try:
            if session:
                session.get(
                    f"https://www.google.com.br/search?q={query}&hl=pt-BR",
                    headers=self.get_headers(referer="https://www.google.com.br/"),
                    timeout=(5, 10),
                    allow_redirects=True,
                )
            else:
                req.get(
                    f"https://www.google.com.br/search?q={query}&hl=pt-BR",
                    headers=self.get_headers(referer="https://www.google.com.br/"),
                    timeout=(5, 10),
                    allow_redirects=True,
                )
            time.sleep(random.uniform(1, 3))
        except Exception:
            pass

    def stats(self):
        """Retorna estatisticas da sessao."""
        elapsed = (datetime.now() - self.start_time).total_seconds()
        return {
            "requests": self.request_count,
            "noise_queries": self.noise_count,
            "elapsed_seconds": round(elapsed, 1),
            "requests_per_minute": round(self.request_count / max(elapsed / 60, 0.1), 1),
        }


# ============================================================
# 5. SELENIUM STEALTH SETUP
# ============================================================

def criar_driver_stealth(headless=True, proxy=None):
    """
    Cria WebDriver Chrome com selenium-stealth configurado.
    Patches anti-deteccao: WebGL, navigator, canvas, etc.

    Args:
        headless: Rodar sem janela visivel
        proxy: Proxy HTTP (ex: "http://user:pass@host:port")

    Returns:
        WebDriver configurado ou None se Chrome nao instalado
    """
    try:
        from selenium import webdriver
        from selenium.webdriver.chrome.options import Options
        from selenium.webdriver.chrome.service import Service
        from selenium_stealth import stealth
    except ImportError:
        logger.warning("selenium/selenium-stealth nao instalado. Instale: pip install selenium selenium-stealth")
        return None

    options = Options()

    if headless:
        options.add_argument("--headless=new")

    # Anti-deteccao flags
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument("--disable-infobars")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-gpu")
    options.add_argument("--window-size=1920,1080")
    options.add_argument("--start-maximized")
    options.add_argument("--disable-extensions")
    options.add_argument("--disable-popup-blocking")
    options.add_argument("--lang=pt-BR")

    # Proxy
    if proxy:
        options.add_argument(f"--proxy-server={proxy}")

    # Remover flag de automacao
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option("useAutomationExtension", False)

    try:
        driver = webdriver.Chrome(options=options)
    except Exception as e:
        logger.warning(f"Chrome WebDriver nao encontrado: {e}")
        logger.info("Instale chromedriver ou use: pip install webdriver-manager")
        return None

    # Aplicar stealth
    stealth(
        driver,
        languages=["pt-BR", "pt", "en-US", "en"],
        vendor="Google Inc.",
        platform="Win32",
        webgl_vendor="Intel Inc.",
        renderer="Intel Iris OpenGL Engine",
        fix_hairline=True,
    )

    # Patches extras
    driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
    driver.execute_script("Object.defineProperty(navigator, 'plugins', {get: () => [1, 2, 3, 4, 5]})")
    driver.execute_script(
        "Object.defineProperty(navigator, 'languages', {get: () => ['pt-BR', 'pt', 'en-US', 'en']})"
    )

    return driver


def scrape_com_selenium(url, driver=None, wait_seconds=3):
    """
    Raspa pagina com Selenium (para sites que precisam JS).
    Retorna HTML renderizado.

    Se driver nao fornecido, cria temporario.
    """
    fechar_driver = False
    if driver is None:
        driver = criar_driver_stealth()
        if driver is None:
            return None
        fechar_driver = True

    try:
        driver.get(url)
        time.sleep(wait_seconds + random.uniform(0.5, 2))

        # Scroll para trigger lazy load
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight / 3)")
        time.sleep(1)
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight * 2 / 3)")
        time.sleep(1)

        html = driver.page_source
        return html

    except Exception as e:
        logger.error(f"Erro Selenium em {url}: {e}")
        return None

    finally:
        if fechar_driver:
            try:
                driver.quit()
            except Exception:
                pass


# ============================================================
# 6. TABELA DE EXCLUSAO LGPD
# ============================================================

# Art. 7o, I — dados publicos de acesso irrestrito
CAMPOS_PERMITIDOS = {
    "nome",           # Nome profissional — dado publico (OAB)
    "oab_num",        # Numero OAB — dado publico
    "cnpj",           # CNPJ — dado publico (Receita Federal)
    "endereco",       # Endereco comercial — dado publico
    "telefone",       # Telefone profissional — dado publico
    "email",          # Email profissional — dado publico
    "site",           # Site — dado publico
}

# NUNCA coletar estes campos
CAMPOS_PROIBIDOS = {
    "cpf",
    "rg",
    "data_nascimento",
    "filiacao",
    "partido_politico",
    "religiao",
    "orientacao_sexual",
    "origem_racial",
    "dados_saude",
    "biometria",
    "endereco_residencial",
    "telefone_pessoal",
}


def filtrar_dados_lgpd(dados):
    """Remove campos proibidos pela LGPD antes de armazenar."""
    limpo = {}
    for key, value in dados.items():
        key_lower = key.lower().replace(" ", "_")
        if key_lower not in CAMPOS_PROIBIDOS:
            limpo[key] = value
    return limpo
