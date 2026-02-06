// bot.cjs
global.crypto = require('crypto');
require('dotenv').config();

const {
  default: makeWASocket,
  useMultiFileAuthState,
  DisconnectReason,
  fetchLatestBaileysVersion
} = require('@whiskeysockets/baileys');

const axios = require('axios');
const fs = require('fs');
const fsp = fs.promises;
const path = require('path');
const XLSX = require('xlsx');
const P = require('pino');
const qrcode = require('qrcode-terminal');
const https = require('https');
const { HttpsProxyAgent } = require('https-proxy-agent');
const { boomify, isBoom } = require('@hapi/boom');

// -----------------------------
// CONFIGURAÇÃO AVANÇADA
// -----------------------------
const CONFIG = {
  MAX_RETRIES: 15,
  RECONNECT_BASE_DELAY: 7000,
  PRESENCE_INTERVAL: 45000,
  API_TIMEOUT: 25000,
  AUTH_PATH: path.join(__dirname, 'auth_info'),
  API_TOKEN: process.env.API_TOKEN,
  ADMIN_NUMBERS: (process.env.ADMIN_NUMBERS || '').split(',').map(n => n.trim()).filter(n => n),
  LOGS_DIR: path.join(__dirname, 'logs'),
  QUERY_CSV: path.join(__dirname, 'logs', 'consultas.csv'),
  MESSAGE_TIMEOUT: 30000, // 30 segundos para resposta
  RATE_LIMIT_MAX: 3, // Máximo 3 mensagens por segundo
  
  // Configurações de rede corporativa
  USE_PROXY: process.env.USE_PROXY === 'true',
  PROXY_CONFIG: process.env.PROXY_HOST ? {
    host: process.env.PROXY_HOST,
    port: process.env.PROXY_PORT || 8080,
    auth: process.env.PROXY_USER ? {
      username: process.env.PROXY_USER,
      password: process.env.PROXY_PASS
    } : undefined
  } : null,
  
  // Timeouts adaptativos
  CONNECT_TIMEOUT: 30000,
  KEEP_ALIVE_INTERVAL: 20000,
  
  // Configurações específicas WhatsApp
  WS_ORIGIN: 'https://web.whatsapp.com',
  USER_AGENT: 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
};

// Validar API Token
if (!CONFIG.API_TOKEN) {
  console.error('❌ API_TOKEN não configurada. Defina API_TOKEN no arquivo .env');
  process.exit(1);
}

// -----------------------------
// SISTEMA DE LOGGING AVANÇADO
// -----------------------------
if (!fs.existsSync(CONFIG.LOGS_DIR)) {
  fs.mkdirSync(CONFIG.LOGS_DIR, { recursive: true });
}

function dailyLogPath() {
  const d = new Date();
  const name = `bot-${d.getFullYear()}-${String(d.getMonth() + 1).padStart(2, '0')}-${String(d.getDate()).padStart(2, '0')}.log`;
  return path.join(CONFIG.LOGS_DIR, name);
}

const pinoDest = P.destination({ dest: dailyLogPath(), sync: false });
const logger = P({ level: process.env.LOG_LEVEL || 'info' }, pinoDest);

// Logger para console com cores
function clog(level, ...args) {
  const timestamp = new Date().toISOString();
  const colors = { info: '📘', warn: '📒', error: '📕', debug: '📗' };
  console.log(`${colors[level] || '📘'} ${timestamp}`, ...args);
}

function logInfo(...args) { 
  logger.info(...args); 
  clog('info', ...args); 
}

function logWarn(...args) { 
  logger.warn(...args); 
  clog('warn', ...args); 
}

function logError(...args) { 
  logger.error(...args); 
  clog('error', ...args); 
}

function logDebug(...args) {
  if (process.env.LOG_LEVEL === 'debug') {
    logger.debug(...args);
    clog('debug', ...args);
  }
}

// -----------------------------
// ESTATÍSTICAS
// -----------------------------
let statistics = {
  totalQueries: 0,
  successfulQueries: 0,
  failedQueries: 0,
  apiCalls: 0
};

// Controle de mensagens processadas (evita duplicatas)
const processedMessages = new Set();

// Planilhas em memória (carregadas 1x na inicialização)
let planilhasMemoria = {
  PTPC: [],
  GTPC: [],
  lastLoaded: null
};

// Limpar mensagens antigas a cada 5 minutos
setInterval(() => {
  if (processedMessages.size > 1000) {
    processedMessages.clear();
    logDebug('🧹 Cache de mensagens processadas limpo');
  }
}, 5 * 60 * 1000);

// Função para verificar se é admin
function isAdmin(jid) {
  if (CONFIG.ADMIN_NUMBERS.length === 0) {
    // Se não há admins configurados, qualquer DM é considerado admin
    return jid.endsWith('@s.whatsapp.net');
  }
  // Extrair número do JID e verificar se está na lista
  const number = jid.split('@')[0];
  return CONFIG.ADMIN_NUMBERS.some(admin => number.includes(admin) || admin.includes(number));
}

// -----------------------------
// SISTEMA DE REDE CORPORATIVA
// -----------------------------
async function createNetworkAgent() {
  try {
    // Se proxy está configurado e habilitado
    if (CONFIG.USE_PROXY && CONFIG.PROXY_CONFIG) {
      logInfo('🔌 Usando proxy corporativo:', CONFIG.PROXY_CONFIG.host);
      return new HttpsProxyAgent(CONFIG.PROXY_CONFIG);
    }

    // Agent direto com configurações para rede corporativa
    logDebug('🌐 Usando conexão direta (configuração corporativa)');
    return new https.Agent({
      rejectUnauthorized: false,
      keepAlive: true,
      timeout: CONFIG.API_TIMEOUT,
      maxFreeSockets: 10,
      keepAliveMsecs: 10000,
      // Configurações para contornar firewalls restritivos
      secureOptions: require('constants').SSL_OP_LEGACY_SERVER_CONNECT,
      checkServerIdentity: (host, cert) => {
        // Aceitar certificados com nomes diferentes (útil para proxies corporativos)
        return undefined;
      }
    });
  } catch (err) {
    logWarn('⚠️ Falha ao criar agent de rede, usando fallback:', err.message);
    return new https.Agent({ rejectUnauthorized: false });
  }
}

// -----------------------------
// DIAGNÓSTICO DE REDE
// -----------------------------
async function testNetworkConnectivity() {
  logInfo('🔍 Iniciando diagnóstico de rede...');
  
  const testUrls = [
    { name: 'Google', url: 'https://google.com' },
    { name: 'UTE Pecém', url: 'https://utepecem.com' },
    { name: 'WhatsApp Web', url: 'https://web.whatsapp.com' }
  ];

  const results = [];
  
  for (const test of testUrls) {
    try {
      const agent = await createNetworkAgent();
      const start = Date.now();
      
      const response = await axios.get(test.url, {
        httpsAgent: agent,
        timeout: 10000,
        validateStatus: () => true // Aceitar qualquer status
      });
      
      const duration = Date.now() - start;
      results.push(`✅ ${test.name}: ${response.status} (${duration}ms)`);
      logInfo(`✅ ${test.name}: ${response.status} (${duration}ms)`);
    } catch (err) {
      results.push(`❌ ${test.name}: ${err.message}`);
      logWarn(`❌ ${test.name}: ${err.message}`);
    }
  }
  
  return results.join('\n');
}

// -----------------------------
// BACKUP E GERENCIAMENTO DE AUTH
// -----------------------------
async function backupAuthInfo() {
  try {
    if (!fs.existsSync(CONFIG.AUTH_PATH)) return;
    
    const timestamp = new Date().toISOString().replace(/[:.]/g, '-');
    const backupDir = path.join(__dirname, 'backup');
    const dest = path.join(backupDir, `auth_info_backup_${timestamp}`);
    
    await fsp.mkdir(backupDir, { recursive: true });
    await fsp.cp(CONFIG.AUTH_PATH, dest, { recursive: true });
    
    logInfo('📦 Backup auth_info criado em', dest);
    return dest;
  } catch (err) {
    logWarn('⚠️ Falha ao criar backup auth_info:', err.message);
    return null;
  }
}

async function deleteAuthInfoWithBackup() {
  try {
    await backupAuthInfo();
    
    if (fs.existsSync(CONFIG.AUTH_PATH)) {
      await fsp.rm(CONFIG.AUTH_PATH, { recursive: true, force: true });
      logInfo('🗑️ auth_info removida com backup');
      return true;
    }
    return false;
  } catch (err) {
    logError('❌ Erro ao remover auth_info:', err.message);
    return false;
  }
}

// -----------------------------
// CARREGAMENTO DE PLANILHAS (OTIMIZADO - CARREGA 1X NA MEMÓRIA)
// -----------------------------
async function carregarPlanilhasMemoria() {
  try {
    logInfo('📊 Carregando planilhas na memória...');
    
    const planilhas = {
      PTPC: { path: 'Estoque Segurança PPTM.xlsx', dados: [] },
      GTPC: { path: 'Estoque de segurança - Energia Pecém.xlsx', dados: [] }
    };
    
    for (const [empresa, config] of Object.entries(planilhas)) {
      const filePath = path.join(__dirname, config.path);
      
      if (!fs.existsSync(filePath)) {
        logWarn(`⚠️ Planilha ${empresa} não encontrada: ${filePath}`);
        continue;
      }
      
      try {
        const workbook = XLSX.readFile(filePath);
        const sheet = workbook.Sheets[workbook.SheetNames[0]];
        const dados = XLSX.utils.sheet_to_json(sheet);
        
        planilhasMemoria[empresa] = dados;
        logInfo(`✅ Planilha ${empresa} carregada: ${dados.length} registros`);
      } catch (err) {
        logError(`❌ Erro ao carregar planilha ${empresa}:`, err.message);
      }
    }
    
    planilhasMemoria.lastLoaded = new Date();
    logInfo('✅ Todas as planilhas carregadas com sucesso');
  } catch (err) {
    logError('❌ Erro crítico ao carregar planilhas:', err.message);
  }
}

async function recarregarPlanilhas() {
  logInfo('🔄 Recarregando planilhas...');
  await carregarPlanilhasMemoria();
  return planilhasMemoria.lastLoaded;
}

async function obterEstoqueSeguranca(codigoProduto, empresa) {
  try {
    const dados = planilhasMemoria[empresa] || [];
    
    if (dados.length === 0) {
      logWarn(`⚠️ Planilha ${empresa} não disponível em memória`);
      return 0;
    }
    
    const coluna = empresa === 'PTPC' ? 'EstSeg-PPTM' : 'EstSeg-GTPC';
    const row = dados.find(r => 
      String(r['Codigo']).trim().toLowerCase() === String(codigoProduto).trim().toLowerCase()
    );
    
    return row?.[coluna] ?? 0;
  } catch (err) {
    logWarn(`⚠️ Erro ao obter estoque segurança ${empresa}:`, err.message);
    return 0;
  }
}

// -----------------------------
// CONSULTA DE PRODUTOS COM RESILIÊNCIA
// -----------------------------
async function consultarProdutoAPI(codigoProduto, retryCount = 0) {
  const MAX_RETRIES = 3;
  const RETRY_DELAY = 1000;
  
  statistics.totalQueries++;

  try {
    statistics.apiCalls++;
    const agent = await createNetworkAgent();
    
    logDebug(`🌐 Consultando API (tentativa ${retryCount + 1}/${MAX_RETRIES + 1}):`, codigoProduto);
    
    const response = await axios.get(
      `https://utepecem.com/sigma/api/getProduto?produto=${codigoProduto}`,
      {
        httpsAgent: agent,
        timeout: CONFIG.API_TIMEOUT,
        headers: {
          'X-API-Token': CONFIG.API_TOKEN,
          'User-Agent': CONFIG.USER_AGENT,
          'Accept': 'application/json',
          'Cache-Control': 'no-cache'
        },
        validateStatus: (status) => status < 500 // Não lançar erro em 4xx
      }
    );

    // Validar resposta
    if (response.status === 401 || response.status === 403) {
      logError('🔐 Erro de autenticação na API - Token inválido ou expirado');
      return { success: false, error: 'Token de autenticação inválido. Contate o administrador.' };
    }
    
    if (response.status === 404) {
      logDebug('❌ Produto não encontrado:', codigoProduto);
      return { success: false, error: 'Produto não encontrado' };
    }
    
    if (response.status >= 400) {
      logWarn(`⚠️ API retornou status ${response.status}:`, response.data);
      return { success: false, error: `Erro no servidor (${response.status})` };
    }

    // Validar estrutura da resposta
    if (!response.data || typeof response.data !== 'object') {
      logWarn('⚠️ Resposta da API em formato inesperado:', response.data);
      return { success: false, error: 'Resposta do servidor em formato inválido' };
    }

    statistics.successfulQueries++;
    logDebug('✅ Produto consultado com sucesso:', codigoProduto);
    return { success: true, data: response.data, source: 'api' };
    
  } catch (err) {
    statistics.failedQueries++;
    
    logWarn(`❌ Erro na consulta API (tentativa ${retryCount + 1}):`, err.code || 'NO_CODE', err.message);
    
    // Retry logic para erros temporários
    const retryableErrors = ['ECONNRESET', 'ETIMEDOUT', 'ECONNABORTED', 'ENOTFOUND', 'EAI_AGAIN', 'ENETUNREACH'];
    
    if (retryCount < MAX_RETRIES && (retryableErrors.includes(err.code) || err.code?.startsWith('E'))) {
      const delay = RETRY_DELAY * Math.pow(2, retryCount); // Backoff exponencial
      logInfo(`🔄 Tentando novamente em ${delay}ms...`);
      
      await new Promise(resolve => setTimeout(resolve, delay));
      return consultarProdutoAPI(codigoProduto, retryCount + 1);
    }
    
    // Mensagens de erro específicas
    if (err.code === 'ECONNABORTED' || err.code === 'ETIMEDOUT') {
      return { success: false, error: 'Timeout na consulta ao sistema. Tente novamente.' };
    }
    if (err.code === 'ENOTFOUND' || err.code === 'EAI_AGAIN') {
      return { success: false, error: 'Erro de conexão - DNS não resolvido. Verifique sua rede.' };
    }
    if (err.code === 'ECONNREFUSED') {
      return { success: false, error: 'Servidor indisponível. Tente novamente mais tarde.' };
    }
    
    // Erro genérico
    logError('🚨 Erro não recuperável na API:', err.stack);
    return { success: false, error: 'Erro de comunicação com o sistema. Tente novamente.' };
  }
}

// -----------------------------
// LOGGING DE CONSULTAS EM CSV
// -----------------------------
async function ensureCSV() {
  try {
    if (!fs.existsSync(CONFIG.LOGS_DIR)) {
      await fsp.mkdir(CONFIG.LOGS_DIR, { recursive: true });
    }
    
    if (!fs.existsSync(CONFIG.QUERY_CSV)) {
      const header = 'data,hora,usuario,codigo,status,origem\n';
      await fsp.writeFile(CONFIG.QUERY_CSV, header, 'utf8');
    }
  } catch (err) {
    logWarn('⚠️ Falha ao garantir CSV de consultas:', err.message);
  }
}

async function registrarConsultaCSV(usuario, codigo, status, origem = 'api') {
  try {
    const d = new Date();
    const linha = `${d.toISOString().split('T')[0]},${d.toISOString().split('T')[1].split('.')[0]},${usuario},${codigo},${status},${origem}\n`;
    await fsp.appendFile(CONFIG.QUERY_CSV, linha, 'utf8');
  } catch (err) {
    logWarn('⚠️ Falha ao registrar consulta CSV:', err.message);
  }
}

// -----------------------------
// NÚCLEO DO BOT WHATSAPP
// -----------------------------
let globalSock = null;
let isStarting = false;
let reconnectAttempts = 0;
let lastBaileysVersion = null;
let presenceInterval = null;

// Controle de rate limiting global
const rateLimiter = new Map();

function getBackoffDelay(attempts) {
  const cap = 8;
  const mult = Math.min(attempts, cap);
  return CONFIG.RECONNECT_BASE_DELAY * Math.pow(2, mult - 1);
}

async function safeStopSock() {
  try {
    if (!globalSock) return;
    
    // Limpar intervals
    if (presenceInterval) {
      clearInterval(presenceInterval);
      presenceInterval = null;
    }
    
    // Tentar logout graceful
    try { 
      await globalSock.logout().catch(() => {}); 
    } catch {}
    
    // Limpar event listeners
    try { 
      globalSock.ev.removeAllListeners(); 
    } catch {}
    
    // Fechar WebSocket
    try { 
      if (globalSock.ws && globalSock.ws.close) {
        globalSock.ws.close();
      }
    } catch {}
    
  } catch (err) {
    logWarn('⚠️ Erro no safeStopSock:', err.message);
  } finally {
    globalSock = null;
  }
}

function createWASocketCorporate(state, version) {
  const socketOptions = {
    version: version,
    auth: state,
    logger: P({ level: process.env.LOG_LEVEL || 'warn' }),
    printQRInTerminal: false,
    browser: ['Ubuntu', 'Chrome', '120.0.0.0'],
    
    // Configurações otimizadas para rede corporativa
    markOnlineOnConnect: true,
    generateHighQualityLinkPreview: false,
    syncFullHistory: false,
    linkPreviewImageThumbnailWidth: 192,
    
    // Timeouts aumentados para rede corporativa
    connectTimeoutMs: CONFIG.CONNECT_TIMEOUT,
    keepAliveIntervalMs: CONFIG.KEEP_ALIVE_INTERVAL,
    maxIdleTimeMs: 90000,
    
    // Configurações WebSocket para firewall corporativo
    wsOptions: {
      origin: CONFIG.WS_ORIGIN,
      headers: {
        'User-Agent': CONFIG.USER_AGENT,
        'Accept-Encoding': 'gzip, deflate, br',
        'Accept-Language': 'pt-BR,pt;q=0.9,en;q=0.8'
      },
      // Agent para WebSocket (se proxy estiver configurado)
      agent: CONFIG.USE_PROXY && CONFIG.PROXY_CONFIG ? 
        new HttpsProxyAgent(CONFIG.PROXY_CONFIG) : undefined
    },
    
    // Retry policies
    retryRequestDelayMs: 2000,
    maxRetryCount: 3,
    emitOwnEvents: true,
    defaultQueryTimeoutMs: 60000
  };

  return makeWASocket(socketOptions);
}

async function startBot() {
  if (isStarting) {
    logWarn('🔁 startBot já em progresso, ignorando chamada duplicada');
    return;
  }
  
  isStarting = true;
  logInfo('🚀 Iniciando bot WhatsApp...');

  try {
    // Obter versão do Baileys
    let versionObj;
    try {
      versionObj = await fetchLatestBaileysVersion();
      const verStr = versionObj.version.join('.');
      
      if (lastBaileysVersion && lastBaileysVersion !== verStr) {
        logWarn('🔔 Nova versão do Baileys:', verStr, '- Considere atualizar!');
      }
      lastBaileysVersion = verStr;
      logInfo('📦 Versão Baileys:', verStr);
    } catch (err) {
      logWarn('⚠️ Não foi possível obter versão do Baileys, usando fallback');
      versionObj = { version: [2, 2412, 10] };
    }

    // Estado de autenticação
    const { state, saveCreds } = await useMultiFileAuthState(CONFIG.AUTH_PATH);
    
    // Criar socket com configurações corporativas
    const sock = createWASocketCorporate(state, versionObj.version);
    globalSock = sock;
    reconnectAttempts = 0;

    // Gerenciar credenciais
    sock.ev.on('creds.update', saveCreds);

    // Handler de conexão
    sock.ev.on('connection.update', async (update) => {
      try {
        const { connection, lastDisconnect, qr } = update;
        
        if (qr) {
          logInfo('📲 QR Code para autenticação:');
          qrcode.generate(qr, { small: true });
        }

        if (connection === 'open') {
          logInfo('✅ Bot conectado com sucesso ao WhatsApp!');
          reconnectAttempts = 0;
          
          // Iniciar heartbeat de presença
          if (presenceInterval) clearInterval(presenceInterval);
          presenceInterval = setInterval(async () => {
            try {
              if (globalSock?.user) {
                await globalSock.sendPresenceUpdate('available');
                logDebug('💓 Presença atualizada');
              }
            } catch (err) {
              logDebug('⚠️ Falha na presença:', err.message);
            }
          }, CONFIG.PRESENCE_INTERVAL);
        }

        if (connection === 'close') {
          let reason = 0;
          try {
            if (lastDisconnect?.error) {
              if (lastDisconnect.error.output?.statusCode) {
                reason = lastDisconnect.error.output.statusCode;
              } else if (isBoom(lastDisconnect.error)) {
                reason = lastDisconnect.error.output?.statusCode;
              } else {
                const boomified = boomify(lastDisconnect.error);
                reason = boomified.output?.statusCode || 0;
              }
            }
          } catch (e) {
            reason = 0;
          }

          const errorMsg = lastDisconnect?.error?.message || 'Desconhecido';
          logWarn('🔌 Conexão fechada. Código:', reason, 'Motivo:', errorMsg);

          // Logout detectado - reiniciar com novo QR
          if (reason === DisconnectReason.loggedOut) {
            logWarn('🔄 Sessão expirada. Reiniciando para novo QR...');
            await deleteAuthInfoWithBackup();
            await safeStopSock();
            setTimeout(() => startBot(), 3000);
            return;
          }

          // Reconexão com backoff
          reconnectAttempts++;
          const delay = getBackoffDelay(reconnectAttempts);
          logWarn(`🔄 Reconexão #${reconnectAttempts} em ${delay}ms`);
          
          await safeStopSock();
          setTimeout(() => startBot(), delay);
        }
      } catch (err) {
        logError('❌ Erro no connection.update:', err.message);
      }
    });

    // Handler de mensagens
    sock.ev.on('messages.upsert', async ({ messages, type }) => {
      if (type !== 'notify') return;
      
      let currentMsg = null;
      let presenceSent = false;
      
      try {
        currentMsg = messages[0];
        if (!currentMsg?.message) return;
        
        // Ignorar mensagens do próprio bot
        if (currentMsg.key.fromMe) return;
        
        // Verificar se já processou esta mensagem (evitar duplicatas)
        const messageId = currentMsg.key.id;
        if (processedMessages.has(messageId)) {
          logDebug('⏭️ Mensagem já processada, ignorando:', messageId);
          return;
        }
        
        // Marcar mensagem como processada
        processedMessages.add(messageId);

        const messageTypes = {
          conversation: currentMsg.message.conversation,
          extendedTextMessage: currentMsg.message.extendedTextMessage?.text,
          imageMessage: currentMsg.message.imageMessage?.caption,
          videoMessage: currentMsg.message.videoMessage?.caption,
          documentMessage: currentMsg.message.documentMessage?.caption
        };

        const text = Object.values(messageTypes).find(t => t) || "";
        const userMessage = String(text).trim();
        
        // Apenas comandos com !
        if (!userMessage.startsWith('!')) return;

        const remetente = currentMsg.key.remoteJid;
        logInfo('📨 Comando recebido:', userMessage, 'de', remetente);

        // Rate limiting - 3 mensagens por segundo
        const now = Date.now();
        const userLimit = rateLimiter.get(remetente) || { count: 0, lastTime: 0, blocked: false };
        
        // Reset se passou mais de 1 segundo
        if (now - userLimit.lastTime > 1000) {
          userLimit.count = 0;
          userLimit.lastTime = now;
          userLimit.blocked = false;
        }
        
        userLimit.count++;
        rateLimiter.set(remetente, userLimit);
        
        if (userLimit.count > CONFIG.RATE_LIMIT_MAX || userLimit.blocked) {
          userLimit.blocked = true;
          await sock.sendMessage(remetente, { 
            text: `⏳ *LIMITE DE CONSULTAS*\n\nMuitas consultas rápidas!\nAguarde 1 segundo entre as consultas.\n\nLimite: ${CONFIG.RATE_LIMIT_MAX} mensagens/segundo` 
          });
          await registrarConsultaCSV(remetente, userMessage.slice(1), 'RATE_LIMIT');
          return;
        }

        // Indicar "digitando"
        await sock.sendPresenceUpdate('composing', remetente);
        presenceSent = true;

        // Processar comandos
        if (userMessage === '!ajuda' || userMessage === '!help') {
          const isUserAdmin = isAdmin(remetente);
          const ajuda = `📚 *COMANDOS DO BOT DE ESTOQUE*

👤 *Comandos de Usuário:*
━━━━━━━━━━━━━━━━━━━
!12345678 - Consulta produto (8 dígitos)
!ajuda - Exibe esta mensagem
!status - Mostra status e estatísticas

📝 *Exemplo de uso:*
!00012345

⚠️ *Importante:*
• Código deve ter exatamente 8 dígitos
• Limite: ${CONFIG.RATE_LIMIT_MAX} consultas/segundo
• Timeout: ${CONFIG.MESSAGE_TIMEOUT/1000} segundos${isUserAdmin ? `

🔐 *Comandos de Admin:*
━━━━━━━━━━━━━━━━━━━
!diagnostico - Testa conectividade
!limparstats - Zera estatísticas
!recarregar - Recarrega planilhas` : ''}
`;
          await sock.sendMessage(remetente, { text: ajuda });
          return;
        }
        
        if (userMessage === '!status') {
          const memoryUsage = (process.memoryUsage().heapUsed / 1024 / 1024).toFixed(2);
          const uptime = Math.floor(process.uptime() / 60);
          const successRate = statistics.totalQueries > 0 
            ? ((statistics.successfulQueries / statistics.totalQueries) * 100).toFixed(1) 
            : 0;
          
          const status = `🤖 *STATUS DO BOT*

✅ Conectado: ${sock.user ? 'Sim' : 'Não'}
⏱️ Tempo ativo: ${uptime} minutos
🔄 Tentativas de reconexão: ${reconnectAttempts}
📊 Consultas totais: ${statistics.totalQueries}
✅ Consultas bem-sucedidas: ${statistics.successfulQueries}
❌ Consultas com erro: ${statistics.failedQueries}
📈 Taxa de sucesso: ${successRate}%
📡 Chamadas API: ${statistics.apiCalls}
🧠 Memória: ${memoryUsage}MB
🔌 Proxy: ${CONFIG.USE_PROXY ? '✅' : '❌'}`;

          await sock.sendMessage(remetente, { text: status });
          return;
        }

        // Comando de diagnóstico (apenas admin)
        if (userMessage === '!diagnostico') {
          if (!isAdmin(remetente)) {
            await sock.sendMessage(remetente, { text: '🔒 Comando restrito a administradores.' });
            return;
          }
          const diagnosis = await testNetworkConnectivity();
          await sock.sendMessage(remetente, { 
            text: `🔍 *DIAGNÓSTICO DE REDE*\n\n${diagnosis}` 
          });
          return;
        }

        // Limpar estatísticas (apenas admin)
        if (userMessage === '!limparstats') {
          if (!isAdmin(remetente)) {
            await sock.sendMessage(remetente, { text: '🔒 Comando restrito a administradores.' });
            return;
          }
          statistics.totalQueries = 0;
          statistics.successfulQueries = 0;
          statistics.failedQueries = 0;
          statistics.apiCalls = 0;
          
          await sock.sendMessage(remetente, { 
            text: "🔄 Estatísticas zeradas!" 
          });
          return;
        }

        // Recarregar planilhas (apenas admin)
        if (userMessage === '!recarregar') {
          if (!isAdmin(remetente)) {
            await sock.sendMessage(remetente, { text: '🔒 Comando restrito a administradores.' });
            return;
          }
          const lastLoad = await recarregarPlanilhas();
          await sock.sendMessage(remetente, { 
            text: `✅ Planilhas recarregadas!\n\n🕐 Última atualização: ${lastLoad.toLocaleString('pt-BR')}` 
          });
          return;
        }

        // Consulta de produto
        const codigoProduto = userMessage.slice(1);
        if (!/^\d{8}$/.test(codigoProduto)) {
          await sock.sendMessage(remetente, { 
            text: "⚠️ *FORMATO INVÁLIDO!*\n\nUse: !12345678 (8 dígitos numéricos)\nExemplo: !00012345" 
          });
          await registrarConsultaCSV(remetente, codigoProduto, 'INVALID_FORMAT');
          return;
        }

        // Consultar produto com timeout
        const startTime = Date.now();
        const consulta = await Promise.race([
          consultarProdutoAPI(codigoProduto),
          new Promise((_, reject) => 
            setTimeout(() => reject(new Error('Timeout na consulta')), CONFIG.MESSAGE_TIMEOUT)
          )
        ]).catch(err => {
          logError('❌ Timeout ou erro na consulta:', err.message);
          return { success: false, error: 'Consulta excedeu o tempo limite (30s). Tente novamente.' };
        });
        const responseTime = ((Date.now() - startTime) / 1000).toFixed(2);
        
        if (!consulta.success) {
          await sock.sendMessage(remetente, { 
            text: `❌ *ERRO NA CONSULTA*\n\n${consulta.error}` 
          });
          await registrarConsultaCSV(remetente, codigoProduto, 'API_ERROR', consulta.source);
          return;
        }

        if (consulta.data.success && consulta.data.data) {
          const produto = consulta.data.data;
          const unidade = produto.unidade;
          
          // Processar estoques
          const estoques = { PTPC: 0, GTPC: 0 };
          produto.estoques.forEach(e => {
            const qtd = parseFloat(e.qAtual) || 0;
            if (e.empresa === 'PTPC') estoques.PTPC += qtd;
            if (e.empresa === 'GTPC') estoques.GTPC += qtd;
          });

          // Obter estoque de segurança
          const [estoqueSegPTPC, estoqueSegGTPC] = await Promise.all([
            obterEstoqueSeguranca(produto.id, 'PTPC'),
            obterEstoqueSeguranca(produto.id, 'GTPC')
          ]);

          // Construir resposta
          const resposta = `📦 *Produto Encontrado!* ⏱️ ${responseTime}s

📌 *Código:* ${produto.id}
📃 *Texto breve:* ${produto.texto_breve}
📝 *Descrição completa:* ${produto.texto_completo}

📍 *Estoque por Empresa:*
🏭 *PPTM:* ${estoques.PTPC > 0 ? `${estoques.PTPC} ${unidade}` : "❌"}
🏭 *EP:* ${estoques.GTPC > 0 ? `${estoques.GTPC} ${unidade}` : "❌"}

⚠️ *Estoque de Segurança:*
🏭 *PPTM:* ${estoqueSegPTPC > 0 ? `${estoqueSegPTPC} ${unidade}` : "❌"}
🏭 *EP:* ${estoqueSegGTPC > 0 ? `${estoqueSegGTPC} ${unidade}` : "❌"}`;

          await sock.sendMessage(remetente, { text: resposta });
          await registrarConsultaCSV(remetente, codigoProduto, 'SUCCESS', 'api');
        } else {
          const erroApi = consulta.data?.message || 'Produto não encontrado no sistema.';
          await sock.sendMessage(remetente, { 
            text: `❌ *PRODUTO NÃO ENCONTRADO*\n\nCódigo: ${codigoProduto}\nMotivo: ${erroApi}` 
          });
          await registrarConsultaCSV(remetente, codigoProduto, 'NOT_FOUND', 'api');
        }

      } catch (err) {
        logError('❌ Erro no processamento da mensagem:', err.message, err.stack);
        
        // Garantir resposta ao usuário em caso de erro
        try {
          if (currentMsg && currentMsg.key.remoteJid) {
            await sock.sendMessage(currentMsg.key.remoteJid, { 
              text: '❌ *ERRO INTERNO*\n\nOcorreu um erro ao processar sua mensagem.\nTente novamente em alguns instantes.\n\nSe o problema persistir, contate o administrador.' 
            });
          }
        } catch (sendErr) {
          logError('❌ Falha ao enviar mensagem de erro:', sendErr.message);
        }
      } finally {
        // Parar indicador de "digitando"
        if (presenceSent && currentMsg) {
          try {
            await sock.sendPresenceUpdate('paused', currentMsg.key.remoteJid);
          } catch (err) {
            logDebug('⚠️ Erro ao pausar presença:', err.message);
          }
        }
      }
    });

    isStarting = false;
    logInfo('✅ Bot WhatsApp inicializado com sucesso');
    return sock;

  } catch (err) {
    isStarting = false;
    reconnectAttempts++;
    const delay = getBackoffDelay(reconnectAttempts);
    
    logError('❌ Falha crítica ao iniciar bot:', err.message);
    logError('🔧 Stack trace:', err.stack);
    
    await safeStopSock();
    setTimeout(() => startBot(), delay);
  }
}

// -----------------------------
// HEALTH CHECK SIMPLIFICADO
// -----------------------------
function startHealthCheck() {
  setInterval(() => {
    try {
      if (!globalSock || !globalSock.user) {
        logWarn('⚠️ Health Check: Socket não autenticado');
        return;
      }
      
      logDebug('💚 Health Check: Conexão saudável');
      
      // Log estatísticas periódicas
      logInfo(`📊 Estatísticas - Total: ${statistics.totalQueries}, Sucesso: ${statistics.successfulQueries}, Erro: ${statistics.failedQueries}, API: ${statistics.apiCalls}`);
      
    } catch (e) {
      logWarn('⚠️ Health Check erro:', e.message);
    }
  }, 10 * 60 * 1000); // A cada 10 minutos
}

// -----------------------------
// INICIALIZAÇÃO
// -----------------------------
(async () => {
  try {
    logInfo('🔧 Inicializando sistema...');
    
    // Inicializar componentes
    await ensureCSV();
    await carregarPlanilhasMemoria();
    
    // Diagnóstico inicial de rede
    if (process.env.NETWORK_DIAGNOSIS !== 'false') {
      await testNetworkConnectivity();
    }
    
    // Iniciar bot
    await startBot();
    startHealthCheck();
    
    logInfo('🎉 Sistema totalmente inicializado e operacional');
    logInfo('🔌 Configuração de rede:', CONFIG.USE_PROXY ? 'Proxy corporativo' : 'Conexão direta');
    
  } catch (err) {
    logError('💥 Erro fatal na inicialização:', err.message);
    process.exit(1);
  }
})();

// -----------------------------
// GRACEFUL SHUTDOWN
// -----------------------------
let isShuttingDown = false;

async function gracefulShutdown(signal) {
  if (isShuttingDown) {
    logWarn('⚠️ Shutdown já em andamento...');
    return;
  }
  
  isShuttingDown = true;
  logInfo(`\n🛑 Sinal ${signal} recebido. Encerrando gracefully...`);
  
  try {
    // Parar de aceitar novas mensagens
    if (globalSock) {
      logInfo('📴 Desconectando WhatsApp...');
      await safeStopSock();
    }
    
    // Fechar logs
    logInfo('📝 Fechando logs...');
    logger.flush();
    
    logInfo('✅ Shutdown concluído com sucesso');
    process.exit(0);
  } catch (err) {
    logError('❌ Erro durante shutdown:', err.message);
    process.exit(1);
  }
}

// Capturar sinais de encerramento
process.on('SIGINT', () => gracefulShutdown('SIGINT'));
process.on('SIGTERM', () => gracefulShutdown('SIGTERM'));
process.on('SIGQUIT', () => gracefulShutdown('SIGQUIT'));

// Prevenir crash por erros não tratados (último recurso)
process.on('unhandledRejection', (reason, promise) => {
  logError('🚨 Promise rejeitada não tratada:', reason);
});

process.on('uncaughtException', (error) => {
  logError('🚨 Exceção não tratada:', error);
  if (error.message?.includes('FATAL') || error.code === 'ERR_UNHANDLED_ERROR') {
    gracefulShutdown('UNCAUGHT_EXCEPTION');
  }
});