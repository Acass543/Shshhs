const express = require('express');
const multer = require('multer');
const qrcode = require('qrcode');
const fs = require('fs');
const path = require('path');
const pino = require('pino');
// Importando 'Browsers' para garantir compatibilidade do Pairing Code
const { default: makeWASocket, useMultiFileAuthState, DisconnectReason, Browsers } = require('@whiskeysockets/baileys');

const app = express();
const PORT = process.env.PORT || 3000;

const upload = multer({ storage: multer.memoryStorage() });

app.use(express.json());
app.use(express.static(path.join(__dirname)));

// --- ESTADO GLOBAL ---
let sock = null;
let waStatus = { status: 'desconectado', phone: '' };
let waQr = '';
let clients = []; 

// Bloqueio / Opt-out
const BLOCKLIST_FILE = path.join(__dirname, 'blocklist.json');
let blocklist = new Set();
if (fs.existsSync(BLOCKLIST_FILE)) {
    try {
        const data = JSON.parse(fs.readFileSync(BLOCKLIST_FILE));
        blocklist = new Set(data);
    } catch (e) {}
}

function saveBlocklist() {
    fs.writeFileSync(BLOCKLIST_FILE, JSON.stringify(Array.from(blocklist)));
}

// Fila de Envios
let queue = {
    items: [], message: '', image: null, interval: 10,
    currentIndex: 0, status: 'ocioso',
    stats: { enviados: 0, falharam: 0, pendentes: 0, total: 0 }
};
let isProcessing = false;

function broadcast(event, data) {
    clients.forEach(client => client.write(`event: ${event}\ndata: ${JSON.stringify(data)}\n\n`));
}

// --- CONEXÃO BAILEYS ---
async function connectToWhatsApp() {
    // Limpeza de conexão morta antes de tentar iniciar uma nova
    if (sock) {
        try { sock.ws.close(); } catch(e) {}
        try { sock.ev.removeAllListeners(); } catch(e) {}
    }

    const { state, saveCreds } = await useMultiFileAuthState('baileys_auth_info');
    
    sock = makeWASocket({
        auth: state,
        printQRInTerminal: false,
        logger: pino({ level: 'silent' }),
        // Utilizando identificador nativo do Ubuntu Chrome aprovado pelo WhatsApp
        browser: Browsers.ubuntu('Chrome') 
    });

    sock.ev.on('connection.update', async (update) => {
        const { connection, lastDisconnect, qr } = update;
        
        if (qr) {
            waQr = await qrcode.toDataURL(qr);
            waStatus = { status: 'desconectado', phone: '' };
            broadcast('qr', waQr);
            broadcast('status', waStatus);
        }
        
        if (connection === 'close') {
            const shouldReconnect = (lastDisconnect.error?.output?.statusCode !== DisconnectReason.loggedOut);
            waQr = ''; 
            if (shouldReconnect) {
                setTimeout(connectToWhatsApp, 5000);
            } else {
                waStatus = { status: 'desconectado', phone: '' };
                broadcast('status', waStatus);
            }
        } else if (connection === 'open') {
            waQr = ''; 
            const phone = sock.user.id.split(':')[0];
            waStatus = { status: 'conectado', phone: phone };
            broadcast('status', waStatus);
            broadcast('pairing-success', true);
        }
    });

    sock.ev.on('creds.update', saveCreds);

    sock.ev.on('messages.upsert', async ({ messages }) => {
        const msg = messages[0];
        if (!msg.message || msg.key.fromMe) return;
        
        const text = msg.message.conversation || msg.message.extendedTextMessage?.text || "";
        const body = text.toLowerCase().trim();
        
        if (['parar', 'sair', 'cancelar', 'stop', 'optout'].includes(body)) {
            const sender = msg.key.remoteJid.split('@')[0];
            blocklist.add(sender);
            saveBlocklist();
        }
    });
}

// --- MOTOR DA FILA DE ENVIOS ---
async function processQueue() {
    if (isProcessing) return;
    isProcessing = true;

    while (queue.status === 'executando' && queue.currentIndex < queue.items.length) {
        const item = queue.items[queue.currentIndex];
        
        if (item.status === 'pendente') {
            const parts = item.number.split(',');
            const numOnly = parts[0];
            const nameOnly = parts.length > 1 ? parts[1].trim() : '';
            const jid = numOnly + '@s.whatsapp.net';

            if (blocklist.has(numOnly)) {
                item.status = 'falhou';
                queue.stats.falharam++;
                queue.stats.pendentes--;
            } else {
                try {
                    let msgText = queue.message.replace(/{{numero}}/g, numOnly).replace(/{{nome}}/g, nameOnly);
                    if (queue.image) {
                        await sock.sendMessage(jid, { image: queue.image, caption: msgText });
                    } else {
                        await sock.sendMessage(jid, { text: msgText });
                    }
                    item.status = 'enviado';
                    queue.stats.enviados++;
                    queue.stats.pendentes--;
                } catch (error) {
                    item.status = 'falhou';
                    queue.stats.falharam++;
                    queue.stats.pendentes--;
                }
            }
            
            broadcast('queue-update', { index: queue.currentIndex, status: item.status });
            broadcast('queue-stats', queue.stats);
        }

        queue.currentIndex++;

        if (queue.currentIndex < queue.items.length && queue.status === 'executando') {
            if (queue.interval > 0) {
                await new Promise(resolve => setTimeout(resolve, queue.interval * 1000));
            } else {
                // Modo Turbo
                await new Promise(resolve => setTimeout(resolve, 50)); 
            }
        }
    }

    if (queue.currentIndex >= queue.items.length) {
        queue.status = 'concluido';
        broadcast('queue-state', { status: queue.status });
    }

    isProcessing = false;
}

// --- ENDPOINTS (API) ---
app.get('/', (req, res) => res.sendFile(path.join(__dirname, 'index.html')));

app.get('/api/events', (req, res) => {
    res.setHeader('Content-Type', 'text/event-stream');
    res.setHeader('Cache-Control', 'no-cache');
    res.setHeader('Connection', 'keep-alive');
    
    clients.push(res);
    
    res.write(`event: status\ndata: ${JSON.stringify(waStatus)}\n\n`);
    if(waQr) res.write(`event: qr\ndata: ${JSON.stringify(waQr)}\n\n`);
    
    req.on('close', () => {
        clients = clients.filter(c => c !== res);
    });
});

app.post('/api/connect', async (req, res) => {
    if (waStatus.status === 'desconectado') connectToWhatsApp();
    res.json({ success: true });
});

app.post('/api/disconnect', async (req, res) => {
    if (sock) {
        await sock.logout();
        waStatus = { status: 'desconectado', phone: '' };
        waQr = '';
        broadcast('status', waStatus);
    }
    res.json({ success: true });
});

// Endpoint com auto-recuperação de Erro 428 (Connection Closed)
app.post('/api/pair', async (req, res) => {
    try {
        const phone = req.body.phone;
        if (!sock) return res.status(400).json({error: "Sistema iniciando, aguarde..."});
        if (sock.authState.creds.registered) return res.status(400).json({error: "O WhatsApp já está conectado."});
        
        // Verifica se a conexão WebSocket está fisicamente aberta
        // 1 significa OPEN. Se for diferente, vai causar o erro 428.
        if (!sock.ws || sock.ws.readyState !== 1) {
            connectToWhatsApp(); // Reinicia silenciosamente
            return res.status(400).json({error: "A conexão estava inativa. Reiniciando o sistema, aguarde 5 segundos e clique em Gerar novamente."});
        }

        // Aguarda 1 segundo por garantia para o handshake terminar
        await new Promise(r => setTimeout(r, 1000));
        
        const code = await sock.requestPairingCode(phone);
        const formattedCode = code?.match(/.{1,4}/g)?.join('-') || code;
        res.json({ success: true, code: formattedCode });

    } catch(e) {
        // Se ainda assim o Baileys cuspir o erro 428, nós matamos e iniciamos uma nova conexão limpa
        if (e.message === 'Connection Closed' || e?.output?.statusCode === 428) {
            console.log("[AVISO] WebSocket expirado. Reiniciando a conexão via Baileys...");
            connectToWhatsApp();
            return res.status(400).json({error: "A conexão inspirou e foi resetada. Aguarde 5 segundos e tente novamente."});
        }

        console.error(e);
        res.status(500).json({error: "Erro interno ao gerar o código. Verifique se o formato do número está correto."});
    }
});

app.post('/api/send', upload.single('image'), (req, res) => {
    const numbers = JSON.parse(req.body.numbers);
    queue.items = numbers.map(num => ({ number: num, status: 'pendente' }));
    queue.message = req.body.message || '';
    queue.interval = parseInt(req.body.interval) || 0;
    queue.image = req.file ? req.file.buffer : null;
    queue.currentIndex = 0;
    queue.stats = { enviados: 0, falharam: 0, pendentes: numbers.length, total: numbers.length };
    queue.status = 'executando';

    broadcast('queue-start', queue.items);
    broadcast('queue-stats', queue.stats);
    broadcast('queue-state', { status: queue.status });
    
    processQueue();
    res.json({ success: true });
});

app.post('/api/queue/pause', (req, res) => {
    if(queue.status === 'executando') queue.status = 'pausado';
    broadcast('queue-state', { status: queue.status });
    res.json({ success: true });
});

app.post('/api/queue/resume', (req, res) => {
    if(queue.status === 'pausado') {
        queue.status = 'executando';
        processQueue();
    }
    broadcast('queue-state', { status: queue.status });
    res.json({ success: true });
});

app.post('/api/queue/cancel', (req, res) => {
    queue.status = 'cancelado';
    broadcast('queue-state', { status: queue.status });
    res.json({ success: true });
});

// Inicialização
connectToWhatsApp();
app.listen(PORT, '0.0.0.0', () => console.log(`Servidor rodando na porta ${PORT}`));